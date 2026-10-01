//! `ClaudeHarness`: the `AgentHarness` façade over one `claude` child per primary thread.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::ids::{ApprovalId, ServerRequestId, ThreadId, TurnId};
use giskard_core::model::{Effort, ModelDescriptor, ModelRef};
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::turn::TurnOverrides;
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, EventLog, HarnessCapabilities, HarnessNotice, HarnessProvider,
    OpenThreadOptions, ProviderHttpHeaders, ThreadHandle, ThreadUpdate, ThreadUpdateSendError,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::attachments::user_message_line;
use crate::catalog::{ANTHROPIC_PROVIDER_ID, CatalogSnapshot, descriptors, parse_entries};
use crate::frame::Frame;
use crate::ids::is_task_native_id;
use crate::log_fields::display_opt;
use crate::mapper::ClaudeMapper;
use crate::process::{
    ChildExit, ChildLogContext, ClaudeChild, ClaudeLaunchOptions, ExitKind, SessionArgs,
    SessionFlag, classify_exit, probe_argv, resume_missing_sentence, session_argv, spawn_child,
};
use crate::session::{
    ChildCommand, ChildHandle, Children, Pending, PendingRequests, STOP_EXIT_GRACE,
    SupervisorParts, control_line, control_outcome, lock, new_request_id, spawn_supervisor,
};

/// How long the CLI has to answer `initialize`.
pub(crate) const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a control request (and a `start_turn` hand-off) may take.
pub(crate) const CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
/// How long stopping one child may take; the registry's own shutdown budget is the same 15 s.
pub(crate) const STOP_TIMEOUT: Duration = Duration::from_secs(15);
/// How long the catalog probe has to answer `initialize`.
pub(crate) const PROBE_TIMEOUT: Duration = Duration::from_secs(30);
/// Commands queued to one supervisor before a sender waits.
const COMMAND_QUEUE: usize = 16;

/// Starts children. The production spawner runs `claude`; tests install a scripted one.
#[async_trait]
pub(crate) trait ChildSpawner: Send + Sync {
    async fn spawn(
        &self,
        argv: &[String],
        cwd: &Path,
        context: &ChildLogContext,
    ) -> Result<Box<dyn ClaudeChild>, HarnessError>;
}

struct ProcessSpawner {
    options: ClaudeLaunchOptions,
}

#[async_trait]
impl ChildSpawner for ProcessSpawner {
    async fn spawn(
        &self,
        argv: &[String],
        cwd: &Path,
        context: &ChildLogContext,
    ) -> Result<Box<dyn ClaudeChild>, HarnessError> {
        let child = spawn_child(&self.options, argv, cwd, context).await?;
        Ok(Box::new(child))
    }
}

/// One Claude Code harness instance: one `claude` process per open primary thread.
pub struct ClaudeHarness {
    workspace_root: PathBuf,
    launch: ClaudeLaunchOptions,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Reach each live child's supervisor task and retained log from the trait methods.
    // Source of truth: `open_thread` inserts an entry after the handshake; the supervisor removes
    //   it when the child exits.
    // Structural reason: The harness crate cannot depend on the server's thread authority.
    // Synchronization: A std mutex guards insert, lookup and removal; nothing awaits under it.
    // Invalidation/removal: Child exit, `delete_thread`, `set_thread_archived(true)` and
    //   `shutdown` remove entries; dropping the harness drops the map.
    children: Children,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Remember which thread and CLI request id a published approval or server request
    //   belongs to, for milestone 3's `respond_*`.
    // Source of truth: The supervisor records an entry when the mapper publishes the request.
    // Structural reason: The responses carry no thread (trait doc: ids are instance-unique).
    // Synchronization: A std mutex.
    // Invalidation/removal: Milestone 3 removes an entry when it is answered; the supervisor
    //   removes a thread's entries when its child exits; `shutdown` clears the map.
    pending: Pending,
    catalog: Arc<Mutex<Option<CatalogSnapshot>>>,
    /// Serializes probe children so concurrent `list_models` calls share one.
    probe: tokio::sync::Mutex<()>,
    shutdown_tx: watch::Sender<bool>,
    spawner: Arc<dyn ChildSpawner>,
    generations: AtomicU64,
}

impl ClaudeHarness {
    /// Build an instance. Spawns nothing: the first child is the first `open_thread` or the
    /// first `list_models`.
    pub fn new(workspace_root: PathBuf, launch: ClaudeLaunchOptions) -> Arc<Self> {
        let spawner = Arc::new(ProcessSpawner {
            options: launch.clone(),
        });
        Arc::new(Self::with_parts(workspace_root, launch, spawner))
    }

    fn with_parts(
        workspace_root: PathBuf,
        launch: ClaudeLaunchOptions,
        spawner: Arc<dyn ChildSpawner>,
    ) -> Self {
        Self {
            workspace_root,
            launch,
            children: Arc::new(Mutex::new(HashMap::new())),
            pending: Arc::new(Mutex::new(PendingRequests::default())),
            catalog: Arc::new(Mutex::new(None)),
            probe: tokio::sync::Mutex::new(()),
            shutdown_tx: watch::channel(false).0,
            spawner,
            generations: AtomicU64::new(0),
        }
    }

    /// An instance whose children come from `spawner` instead of `claude`.
    #[cfg(test)]
    pub(crate) fn with_spawner(
        workspace_root: PathBuf,
        launch: ClaudeLaunchOptions,
        spawner: Arc<dyn ChildSpawner>,
    ) -> Arc<Self> {
        Arc::new(Self::with_parts(workspace_root, launch, spawner))
    }

    fn ensure_running(&self) -> Result<(), HarnessError> {
        if *self.shutdown_tx.borrow() {
            return Err(HarnessError::Transport(
                "Claude Code harness is shut down".into(),
            ));
        }
        Ok(())
    }

    fn context(&self, thread: Option<ThreadId>, session: Option<&str>) -> ChildLogContext {
        ChildLogContext {
            project_id: self.launch.project_id,
            harness: self.launch.declaration.clone(),
            thread_id: thread,
            harness_thread_id: session.map(str::to_owned),
            resume: false,
            live_children: self.live_children(),
        }
    }

    fn catalog_snapshot(&self) -> Option<CatalogSnapshot> {
        lock(&self.catalog).clone()
    }

    fn live_children(&self) -> usize {
        lock(&self.children).len()
    }

    /// The command channel and open model of a thread's live child.
    fn live(&self, thread: ThreadId) -> Option<(mpsc::Sender<ChildCommand>, ModelRef)> {
        lock(&self.children)
            .get(&thread)
            .map(|handle| (handle.commands.clone(), handle.model.clone()))
    }

    /// Spawn one session child and run its handshake. A child that fails the handshake has been
    /// reaped before this returns.
    async fn spawn_and_handshake(
        &self,
        opts: &OpenThreadOptions,
        session: SessionFlag,
    ) -> Result<(Box<dyn ClaudeChild>, Handshake), HandshakeFailure> {
        let resume = matches!(session, SessionFlag::Resume(_));
        let session_id = match &session {
            SessionFlag::Fresh(id) | SessionFlag::Resume(id) => id.clone(),
        };
        let argv = session_argv(
            &self.launch,
            &SessionArgs {
                model: opts.initial_model.clone(),
                session,
            },
        );
        let mut context = self.context(Some(opts.thread), Some(&session_id));
        context.resume = resume;
        let mut child = self
            .spawner
            .spawn(&argv, &opts.workspace_root, &context)
            .await
            .map_err(HandshakeFailure::Error)?;
        let started = Instant::now();
        let handshake = handshake(child.as_mut(), &context, resume).await?;
        debug!(
            thread_id = %opts.thread,
            harness_thread_id = %session_id,
            action = "handshake",
            resume,
            elapsed_ms = started.elapsed().as_millis() as u64,
            pid = display_opt(handshake.pid),
            permission_mode = display_opt(handshake.permission_mode.as_deref()),
            "Claude Code answered the handshake"
        );
        Ok((child, handshake))
    }

    /// Replace the catalog snapshot with what a handshake or probe reported.
    fn store_catalog(&self, models: &[Value], source: &'static str) -> CatalogSnapshot {
        let snapshot = CatalogSnapshot::new(parse_entries(models), source);
        debug!(
            action = "catalog",
            source,
            models = snapshot.entries.len(),
            "model catalog refreshed"
        );
        *lock(&self.catalog) = Some(snapshot.clone());
        snapshot
    }

    /// The model the CLI says it applied, compared with the one requested.
    fn resumed_model(
        &self,
        opts: &OpenThreadOptions,
        session_id: &str,
        applied: Option<AppliedSettings>,
    ) -> Option<ModelRef> {
        let requested = &opts.initial_model;
        let applied = applied?;
        let resolved = self
            .catalog_snapshot()
            .and_then(|snapshot| snapshot.resolved_model(&requested.model).map(str::to_owned));
        if applied.model == requested.model || resolved.as_deref() == Some(applied.model.as_str()) {
            return Some(requested.clone());
        }
        warn!(
            project_id = display_opt(self.launch.project_id),
            thread_id = %opts.thread,
            harness_thread_id = %session_id,
            action = "model_not_applied",
            requested = %requested.model,
            applied = %applied.model,
            "Claude Code applied a different model than the one requested"
        );
        Some(ModelRef {
            provider: ANTHROPIC_PROVIDER_ID.into(),
            model: applied.model,
            reasoning_effort: applied.effort.map(Effort),
        })
    }

    /// Stop one child that is no longer in `children`, bounded by `STOP_TIMEOUT`.
    async fn stop_handle(thread: ThreadId, handle: ChildHandle, action: &'static str) {
        let ChildHandle {
            commands,
            mut task,
            harness_thread_id,
            log,
            ..
        } = handle;
        let started = Instant::now();
        let stopped = tokio::time::timeout(STOP_TIMEOUT, async {
            let (reply, done) = oneshot::channel();
            // A supervisor already stopping on its own (shutdown) drops the command; its task
            // ending is the answer either way.
            if commands.send(ChildCommand::Stop { reply }).await.is_ok() {
                let _ = done.await;
            }
            if let Err(error) = (&mut task).await {
                // The supervisor did not reach its exit handling, which closes the log; readers
                // hold the log alive, so without this the thread's stream would never end.
                log.close();
                warn!(
                    thread_id = %thread,
                    harness_thread_id = %harness_thread_id,
                    action,
                    error = %error,
                    "the claude supervisor task ended abnormally; closed its event stream"
                );
            }
        })
        .await;
        match stopped {
            Ok(()) => info!(
                thread_id = %thread,
                harness_thread_id = %harness_thread_id,
                action = "thread_stopped",
                reason = action,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "claude child stopped"
            ),
            Err(_) => {
                // Aborting drops the child, and `kill_on_drop` reaps the process. The aborted
                // supervisor never closes the log, and readers keep it alive, so close it here:
                // the thread's stream must end when its session does.
                task.abort();
                log.close();
                warn!(
                    thread_id = %thread,
                    harness_thread_id = %harness_thread_id,
                    action = "thread_stopped",
                    reason = action,
                    timeout_ms = STOP_TIMEOUT.as_millis() as u64,
                    "claude child did not stop in time; aborted its supervisor and closed its \
                     event stream"
                );
            }
        }
    }

    /// Stop a thread's child if it has one (archive, delete).
    async fn stop_thread(&self, thread: &ThreadHandle, action: &'static str) {
        if is_task_native_id(&thread.harness_thread_id) {
            debug!(
                thread_id = %thread.thread,
                harness_thread_id = %thread.harness_thread_id,
                action,
                "a sub-agent thread has no child to stop"
            );
            return;
        }
        let Some(handle) = lock(&self.children).remove(&thread.thread) else {
            debug!(
                thread_id = %thread.thread,
                harness_thread_id = %thread.harness_thread_id,
                action,
                "no live claude child for this thread"
            );
            return;
        };
        Self::stop_handle(thread.thread, handle, action).await;
        let dropped = lock(&self.pending).remove_thread(thread.thread);
        debug!(
            thread_id = %thread.thread,
            action,
            pending_dropped = dropped,
            live_children = self.live_children(),
            "thread's claude child removed"
        );
    }

    /// Send one command to a live child and await its reply, both under `limit`.
    async fn call<T>(
        &self,
        thread: ThreadId,
        commands: mpsc::Sender<ChildCommand>,
        what: &'static str,
        limit: Duration,
        make: impl FnOnce(oneshot::Sender<Result<T, HarnessError>>) -> ChildCommand,
    ) -> Result<T, HarnessError> {
        let (reply, answer) = oneshot::channel();
        let outcome = tokio::time::timeout(limit, async {
            commands
                .send(make(reply))
                .await
                .map_err(|_| child_stopped())?;
            answer.await.map_err(|_| child_stopped())?
        })
        .await;
        match outcome {
            Ok(result) => result,
            Err(_) => {
                warn!(
                    thread_id = %thread,
                    action = what,
                    timeout_ms = limit.as_millis() as u64,
                    "claude did not answer in time"
                );
                Err(HarnessError::Timeout(format!(
                    "claude did not answer {what} within {} s",
                    limit.as_secs()
                )))
            }
        }
    }

    /// Spawn a probe child, read its catalog, and let it exit. Leaves no transcript.
    async fn probe_catalog(&self) -> Result<CatalogSnapshot, HarnessError> {
        let started = Instant::now();
        let context = self.context(None, None);
        let argv = probe_argv(&self.launch);
        let mut child = self
            .spawner
            .spawn(&argv, &self.workspace_root, &context)
            .await?;
        let mut early = Vec::new();
        let mut result_errors = Vec::new();
        let reply = tokio::time::timeout(
            PROBE_TIMEOUT,
            request(
                child.as_mut(),
                &new_request_id(),
                &json!({"subtype": "initialize"}),
                &mut early,
                &mut result_errors,
            ),
        )
        .await;
        let models = match reply {
            Ok(Ok(Ok(reply))) => reply,
            Ok(Ok(Err(message))) => {
                reap(child.as_mut()).await;
                return Err(HarnessError::Spawn(format!(
                    "claude refused initialize: {message}"
                )));
            }
            Ok(Err(failure)) => return Err(failure.into_error(&context)),
            Err(_) => {
                child.start_kill();
                child.wait().await;
                warn!(
                    action = "catalog_probe",
                    timeout_ms = PROBE_TIMEOUT.as_millis() as u64,
                    "the catalog probe did not answer initialize; killed it"
                );
                return Err(HarnessError::Timeout(format!(
                    "claude did not answer initialize within {} s",
                    PROBE_TIMEOUT.as_secs()
                )));
            }
        };
        let reply = InitializeReply::from_value(&models);
        reap(child.as_mut()).await;
        let snapshot = self.store_catalog(reply.models.as_deref().unwrap_or_default(), "probe");
        info!(
            project_id = display_opt(self.launch.project_id),
            harness = display_opt(self.launch.declaration.as_deref()),
            action = "catalog_probe",
            elapsed_ms = started.elapsed().as_millis() as u64,
            models = snapshot.entries.len(),
            "read the Claude Code model catalog from a probe child"
        );
        Ok(snapshot)
    }
}

/// The descriptors of a stored snapshot, noting where and when it came from.
fn served(snapshot: &CatalogSnapshot) -> Vec<ModelDescriptor> {
    debug!(
        action = "list_models",
        source = snapshot.source,
        age_ms = snapshot.taken_at.elapsed().as_millis() as u64,
        models = snapshot.entries.len(),
        "answering from the stored model catalog"
    );
    descriptors(snapshot)
}

fn child_stopped() -> HarnessError {
    HarnessError::Transport("claude child stopped".into())
}

/// Close stdin and wait for the exit, killing the child after `STOP_EXIT_GRACE`.
async fn reap(child: &mut dyn ClaudeChild) -> ChildExit {
    child.close_stdin();
    let drained = tokio::time::timeout(STOP_EXIT_GRACE, async {
        while let Ok(Some(_)) = child.next_line().await {}
    })
    .await;
    if drained.is_err() {
        warn!(
            action = "stop_kill",
            pid = display_opt(child.pid()),
            grace_ms = STOP_EXIT_GRACE.as_millis() as u64,
            "claude did not exit after stdin closed; killing it"
        );
        child.start_kill();
    }
    child.wait().await
}

/// The parts of the `initialize` response the adapter reads; unknown keys are ignored.
#[derive(Debug, Default, Deserialize)]
struct InitializeReply {
    models: Option<Vec<Value>>,
    current_permission_mode: Option<String>,
    pid: Option<u32>,
}

impl InitializeReply {
    fn from_value(value: &Value) -> Self {
        Self::deserialize(value).unwrap_or_else(|error| {
            warn!(
                action = "handshake",
                error = %crate::frame::redact_serde_error(&error),
                "the initialize response did not match its expected shape"
            );
            Self::default()
        })
    }
}

/// `get_settings.applied`.
#[derive(Debug, Clone)]
struct AppliedSettings {
    model: String,
    effort: Option<String>,
}

/// What a successful handshake learned.
struct Handshake {
    models: Option<Vec<Value>>,
    permission_mode: Option<String>,
    pid: Option<u32>,
    applied: Option<AppliedSettings>,
    /// `get_context_usage.maxTokens`, on resume only.
    context_window: Option<u32>,
    /// Frames read during the handshake that were not its responses.
    early_lines: Vec<String>,
    /// Ids of handshake requests that timed out; their late answers are expected.
    abandoned_requests: Vec<String>,
}

enum HandshakeFailure {
    /// The child exited before answering the handshake request named by `stage`.
    Exited {
        exit: ChildExit,
        result_errors: Vec<String>,
        /// The control request subtype left unanswered (`initialize`, `get_settings`, …).
        stage: String,
    },
    /// Anything else: a timeout, a write failure, a refused `initialize`. The child is reaped.
    Error(HarnessError),
}

impl HandshakeFailure {
    fn into_error(self, context: &ChildLogContext) -> HarnessError {
        let error = match self {
            HandshakeFailure::Error(error) => error,
            HandshakeFailure::Exited {
                exit,
                result_errors,
                stage,
            } => {
                if classify_exit(&exit, &result_errors) == ExitKind::Unauthenticated {
                    HarnessError::Unauthenticated
                } else {
                    let said = if exit.stderr_tail.is_empty() {
                        result_errors.join(" | ")
                    } else {
                        exit.stderr_tail.join(" | ")
                    };
                    HarnessError::Spawn(format!(
                        "claude exited with {} before answering {stage}: {said}",
                        exit.describe()
                    ))
                }
            }
        };
        warn!(
            project_id = display_opt(context.project_id),
            harness = display_opt(context.harness.as_deref()),
            thread_id = display_opt(context.thread_id),
            harness_thread_id = display_opt(context.harness_thread_id.as_deref()),
            resume = context.resume,
            action = "handshake",
            error = %error,
            "Claude Code failed its handshake"
        );
        error
    }
}

/// Write one control request and read until its response. Lines that are not the response are
/// kept in `early` for the mapper; a `result` among them leaves its `errors` in `result_errors`.
///
/// `Ok(Ok(payload))` is a success, `Ok(Err(message))` the CLI's refusal.
async fn request(
    child: &mut dyn ClaudeChild,
    request_id: &str,
    request: &Value,
    early: &mut Vec<String>,
    result_errors: &mut Vec<String>,
) -> Result<Result<Value, String>, HandshakeFailure> {
    let stage = request
        .get("subtype")
        .and_then(Value::as_str)
        .unwrap_or("a control request")
        .to_owned();
    if let Err(error) = child.write_line(&control_line(request_id, request)).await {
        // A child whose stdin is already gone has most likely exited: report its exit.
        debug!(action = "handshake", error = %error, "handshake write failed");
        while let Ok(Some(line)) = child.next_line().await {
            note_result_errors(&line, result_errors);
        }
        let exit = child.wait().await;
        return Err(HandshakeFailure::Exited {
            exit,
            result_errors: std::mem::take(result_errors),
            stage,
        });
    }
    loop {
        match child.next_line().await {
            Ok(Some(line)) => {
                if let Ok(Frame::ControlResponse {
                    request_id: answered,
                    raw,
                }) = Frame::parse(&line)
                    && answered == request_id
                {
                    let payload = raw.get("response").cloned().unwrap_or(Value::Null);
                    return Ok(control_outcome(&payload).map_err(|error| match error {
                        HarnessError::Protocol(message) => message,
                        other => other.to_string(),
                    }));
                }
                note_result_errors(&line, result_errors);
                debug!(
                    action = "handshake",
                    bytes = line.len(),
                    "a frame arrived before the handshake response; it is mapped after the open"
                );
                early.push(line);
            }
            Ok(None) => {
                let exit = child.wait().await;
                return Err(HandshakeFailure::Exited {
                    exit,
                    result_errors: std::mem::take(result_errors),
                    stage,
                });
            }
            Err(error) => {
                child.start_kill();
                child.wait().await;
                return Err(HandshakeFailure::Error(error));
            }
        }
    }
}

fn note_result_errors(line: &str, result_errors: &mut Vec<String>) {
    if let Ok(Frame::Result(result)) = Frame::parse(line) {
        *result_errors = result.errors.clone();
    }
}

/// `initialize`, then `get_settings`, then on resume `get_context_usage`.
async fn handshake(
    child: &mut dyn ClaudeChild,
    context: &ChildLogContext,
    resume: bool,
) -> Result<Handshake, HandshakeFailure> {
    let mut early = Vec::new();
    let mut result_errors = Vec::new();
    let mut abandoned = Vec::new();
    let initialize = tokio::time::timeout(
        INITIALIZE_TIMEOUT,
        request(
            child,
            &new_request_id(),
            &json!({"subtype": "initialize"}),
            &mut early,
            &mut result_errors,
        ),
    )
    .await;
    let reply = match initialize {
        Ok(Ok(Ok(reply))) => InitializeReply::from_value(&reply),
        Ok(Ok(Err(message))) => {
            reap(child).await;
            return Err(HandshakeFailure::Error(HarnessError::Spawn(format!(
                "claude refused initialize: {message}"
            ))));
        }
        Ok(Err(failure)) => return Err(failure),
        Err(_) => {
            child.start_kill();
            child.wait().await;
            return Err(HandshakeFailure::Error(HarnessError::Timeout(format!(
                "claude did not answer initialize within {} s",
                INITIALIZE_TIMEOUT.as_secs()
            ))));
        }
    };

    let settings = optional_request(
        child,
        context,
        &json!({"subtype": "get_settings"}),
        &mut early,
        &mut result_errors,
        &mut abandoned,
    )
    .await?;
    let applied = settings.and_then(|settings| {
        let applied = settings.get("applied");
        let model = applied
            .and_then(|applied| applied.get("model"))
            .and_then(Value::as_str);
        match model {
            Some(model) => Some(AppliedSettings {
                model: model.to_owned(),
                effort: applied
                    .and_then(|applied| applied.get("effort"))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            }),
            None => {
                warn!(
                    thread_id = display_opt(context.thread_id),
                    harness_thread_id = display_opt(context.harness_thread_id.as_deref()),
                    action = "handshake",
                    "get_settings named no applied model"
                );
                None
            }
        }
    });

    let mut context_window = None;
    if resume {
        let usage = optional_request(
            child,
            context,
            &json!({"subtype": "get_context_usage"}),
            &mut early,
            &mut result_errors,
            &mut abandoned,
        )
        .await?;
        context_window = usage
            .as_ref()
            .and_then(|usage| usage.get("maxTokens"))
            .and_then(Value::as_u64)
            .and_then(|tokens| u32::try_from(tokens).ok())
            .filter(|tokens| *tokens > 0);
        if usage.is_some() && context_window.is_none() {
            warn!(
                thread_id = display_opt(context.thread_id),
                harness_thread_id = display_opt(context.harness_thread_id.as_deref()),
                action = "handshake",
                "get_context_usage carried no usable maxTokens"
            );
        }
    }

    Ok(Handshake {
        models: reply.models,
        permission_mode: reply.current_permission_mode,
        pid: reply.pid,
        applied,
        context_window,
        early_lines: early,
        abandoned_requests: abandoned,
    })
}

/// A handshake request whose failure degrades the open instead of failing it. A child that exits
/// meanwhile still fails the open. A request that times out has its id pushed to `abandoned`, so
/// the supervisor recognises the CLI's late answer.
async fn optional_request(
    child: &mut dyn ClaudeChild,
    context: &ChildLogContext,
    body: &Value,
    early: &mut Vec<String>,
    result_errors: &mut Vec<String>,
    abandoned: &mut Vec<String>,
) -> Result<Option<Value>, HandshakeFailure> {
    let subtype = body.get("subtype").and_then(Value::as_str).unwrap_or("?");
    let request_id = new_request_id();
    let answer = tokio::time::timeout(
        CONTROL_TIMEOUT,
        request(child, &request_id, body, early, result_errors),
    )
    .await;
    match answer {
        Ok(Ok(Ok(payload))) => Ok(Some(payload)),
        Ok(Ok(Err(message))) => {
            warn!(
                thread_id = display_opt(context.thread_id),
                harness_thread_id = display_opt(context.harness_thread_id.as_deref()),
                action = "handshake",
                subtype,
                error = %message,
                "Claude Code refused a handshake request; continuing without it"
            );
            Ok(None)
        }
        Ok(Err(failure)) => Err(failure),
        Err(_) => {
            abandoned.push(request_id.clone());
            warn!(
                thread_id = display_opt(context.thread_id),
                harness_thread_id = display_opt(context.harness_thread_id.as_deref()),
                action = "handshake",
                subtype,
                request_id = %request_id,
                timeout_ms = CONTROL_TIMEOUT.as_millis() as u64,
                "Claude Code did not answer a handshake request; continuing without it"
            );
            Ok(None)
        }
    }
}

#[async_trait]
impl AgentHarness for ClaudeHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        crate::capabilities()
    }

    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError> {
        self.ensure_running()?;
        if let Some(snapshot) = self.catalog_snapshot() {
            return Ok(served(&snapshot));
        }
        let _probe = self.probe.lock().await;
        // A caller that waited on the probe uses what it found.
        if let Some(snapshot) = self.catalog_snapshot() {
            return Ok(served(&snapshot));
        }
        self.ensure_running()?;
        let snapshot = self.probe_catalog().await?;
        Ok(descriptors(&snapshot))
    }

    async fn list_providers(&self) -> Result<Vec<HarnessProvider>, HarnessError> {
        Ok(vec![HarnessProvider {
            id: ANTHROPIC_PROVIDER_ID.into(),
            name: Some("Anthropic (Claude Code)".into()),
            // No base URL keeps Giskard's own `/v1/models` discovery off.
            base_url: None,
            auth: None,
            http_headers: ProviderHttpHeaders::default(),
            env: self.launch.env.clone(),
        }])
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        self.shutdown_tx.send_replace(true);
        let handles: Vec<(ThreadId, ChildHandle)> = lock(&self.children).drain().collect();
        let children_stopped = handles.len();
        futures::future::join_all(
            handles
                .into_iter()
                .map(|(thread, handle)| Self::stop_handle(thread, handle, "shutdown")),
        )
        .await;
        let pending_dropped = lock(&self.pending).clear();
        info!(
            project_id = display_opt(self.launch.project_id),
            harness = display_opt(self.launch.declaration.as_deref()),
            action = "shutdown",
            children_stopped,
            pending_dropped,
            "Claude Code harness shut down"
        );
        Ok(())
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        self.ensure_running()?;
        let resume = match opts.resume.as_deref() {
            Some(id) if is_task_native_id(id) => {
                return Err(HarnessError::Unsupported(
                    "a Claude Code sub-agent thread has no session to resume".into(),
                ));
            }
            Some(id) => {
                uuid::Uuid::parse_str(id).map_err(|_| {
                    HarnessError::Protocol(format!(
                        "thread {} has native id {id:?}, which is not a Claude Code session id",
                        opts.thread
                    ))
                })?;
                Some(id.to_owned())
            }
            None => None,
        };

        if let Some(handle) = lock(&self.children).get(&opts.thread) {
            debug!(
                thread_id = %opts.thread,
                harness_thread_id = %handle.harness_thread_id,
                action = "open_thread",
                "thread already has a live claude child; returning its handle"
            );
            let mut existing = ThreadHandle::opened(
                opts.thread,
                handle.harness_thread_id.clone(),
                opts.workspace_root.clone(),
            );
            existing.resumed_model = Some(handle.model.clone());
            return Ok(existing);
        }

        let (session_id, first) = match resume {
            Some(id) => (id.clone(), SessionFlag::Resume(id)),
            None => {
                let id = uuid::Uuid::new_v4().to_string();
                (id.clone(), SessionFlag::Fresh(id))
            }
        };
        let resuming = matches!(first, SessionFlag::Resume(_));
        let mut context = self.context(Some(opts.thread), Some(&session_id));
        context.resume = resuming;

        let (child, handshake, warning) = match self.spawn_and_handshake(&opts, first).await {
            Ok((child, handshake)) => (child, handshake, None),
            Err(HandshakeFailure::Exited {
                exit,
                result_errors,
                ..
            }) if resuming && classify_exit(&exit, &result_errors) == ExitKind::ResumeMissing => {
                let detail = resume_missing_sentence(&exit, &result_errors);
                warn!(
                    project_id = display_opt(self.launch.project_id),
                    harness = display_opt(self.launch.declaration.as_deref()),
                    thread_id = %opts.thread,
                    harness_thread_id = %session_id,
                    action = "claude_resume_failed",
                    exit_code = display_opt(exit.code),
                    stderr_tail = ?exit.stderr_tail,
                    "the Claude Code transcript is gone; starting a fresh session with the \
                     same id"
                );
                let mut fresh_context = context.clone();
                fresh_context.resume = false;
                let (child, handshake) = self
                    .spawn_and_handshake(&opts, SessionFlag::Fresh(session_id.clone()))
                    .await
                    .map_err(|failure| failure.into_error(&fresh_context))?;
                let notice = HarnessNotice {
                    code: "claude_resume_failed".into(),
                    message: "Agent context was lost; started a fresh Claude Code session. \
                                  History is intact."
                        .into(),
                    detail,
                };
                (child, handshake, Some(notice))
            }
            Err(failure) => return Err(failure.into_error(&context)),
        };

        if let Some(models) = handshake.models.as_deref() {
            self.store_catalog(models, "handshake");
        }
        let resumed_model = self.resumed_model(&opts, &session_id, handshake.applied.clone());
        let mut mapper =
            ClaudeMapper::new(opts.thread, session_id.clone(), opts.workspace_root.clone());
        if let Some(window) = handshake.context_window {
            let update = ThreadUpdate::ContextWindowRestored {
                model: resumed_model
                    .clone()
                    .unwrap_or_else(|| opts.initial_model.clone()),
                context_window: window,
            };
            if let Err(error) = opts.updates.send(update) {
                let why = match error {
                    ThreadUpdateSendError::Full(_) => "full",
                    ThreadUpdateSendError::Closed(_) => "closed",
                };
                debug!(
                    thread_id = %opts.thread,
                    action = "context_window_restored",
                    why,
                    "the thread update sink did not take the restored window"
                );
            }
            mapper.note_context_window(window);
        }

        let log = Arc::new(EventLog::new());
        let (commands, receiver) = mpsc::channel(COMMAND_QUEUE);
        let generation = self.generations.fetch_add(1, Ordering::Relaxed);
        let open_model = resumed_model
            .clone()
            .unwrap_or_else(|| opts.initial_model.clone());
        // Decided under the lock so a concurrent `shutdown` either sees this child or refuses it.
        let registered: Result<usize, (HarnessError, Box<dyn ClaudeChild>)> = {
            let mut children = lock(&self.children);
            if *self.shutdown_tx.borrow() {
                Err((
                    HarnessError::Transport("Claude Code harness is shut down".into()),
                    child,
                ))
            } else {
                match children.entry(opts.thread) {
                    Entry::Occupied(_) => Err((
                        HarnessError::Protocol(format!(
                            "thread {} was opened concurrently",
                            opts.thread
                        )),
                        child,
                    )),
                    Entry::Vacant(slot) => {
                        let task = spawn_supervisor(SupervisorParts {
                            child,
                            mapper,
                            log: log.clone(),
                            commands: receiver,
                            shutdown: self.shutdown_tx.subscribe(),
                            children: self.children.clone(),
                            pending: self.pending.clone(),
                            generation,
                            thread: opts.thread,
                            context: context.clone(),
                            early_lines: handshake.early_lines,
                            abandoned_requests: handshake.abandoned_requests,
                        });
                        slot.insert(ChildHandle {
                            harness_thread_id: session_id.clone(),
                            log,
                            commands,
                            task,
                            model: open_model,
                            generation,
                        });
                        Ok(children.len())
                    }
                }
            }
        };
        let live_children = match registered {
            Ok(live_children) => live_children,
            Err((error, mut child)) => {
                warn!(
                    thread_id = %opts.thread,
                    harness_thread_id = %session_id,
                    action = "thread_opened",
                    error = %error,
                    "discarding a freshly opened claude child"
                );
                reap(child.as_mut()).await;
                return Err(error);
            }
        };
        info!(
            project_id = display_opt(self.launch.project_id),
            harness = display_opt(self.launch.declaration.as_deref()),
            thread_id = %opts.thread,
            harness_thread_id = %session_id,
            action = "thread_opened",
            resume = resuming,
            resume_fallback = warning.is_some(),
            live_children,
            "Claude Code thread opened"
        );
        let mut handle = ThreadHandle::opened(opts.thread, session_id, opts.workspace_root);
        handle.warning = warning;
        handle.resumed_model = resumed_model;
        Ok(handle)
    }

    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream {
        match lock(&self.children).get(&thread.thread) {
            Some(handle) => AgentEventStream::new(handle.log.reader()),
            None => AgentEventStream::closed(),
        }
    }

    async fn set_thread_name(&self, thread: &ThreadHandle, name: &str) -> Result<(), HarnessError> {
        if is_task_native_id(&thread.harness_thread_id) {
            debug!(
                thread_id = %thread.thread,
                action = "rename_session",
                "a sub-agent thread has no session to rename"
            );
            return Ok(());
        }
        let Some((commands, _)) = self.live(thread.thread) else {
            debug!(
                thread_id = %thread.thread,
                harness_thread_id = %thread.harness_thread_id,
                action = "rename_session",
                "no live claude child; Giskard keeps the name"
            );
            return Ok(());
        };
        let request = json!({"subtype": "rename_session", "title": name, "source": "host"});
        self.call(
            thread.thread,
            commands,
            "rename_session",
            CONTROL_TIMEOUT,
            |reply| ChildCommand::Control { request, reply },
        )
        .await
        .map(|_| ())
    }

    async fn set_thread_archived(
        &self,
        thread: &ThreadHandle,
        archived: bool,
    ) -> Result<(), HarnessError> {
        if archived {
            self.stop_thread(thread, "archive").await;
        }
        Ok(())
    }

    /// Stops the child. The session's transcript under `~/.claude` is left alone.
    async fn delete_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        self.stop_thread(thread, "delete").await;
        Ok(())
    }

    async fn interrupt(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        let (commands, _) = self
            .live(thread.thread)
            .ok_or(HarnessError::ThreadNotFound(thread.thread))?;
        self.call(
            thread.thread,
            commands,
            "interrupt",
            CONTROL_TIMEOUT,
            |reply| ChildCommand::Interrupt { reply },
        )
        .await
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        input: UserInput,
        overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let (commands, model) = self
            .live(thread.thread)
            .ok_or(HarnessError::ThreadNotFound(thread.thread))?;
        // Provider and model only: the same model at another effort is not a different model
        // (per-turn effort is milestone 3's, with the rest of the overrides).
        if let Some(requested) = overrides
            .model
            .as_ref()
            .filter(|m| m.provider != model.provider || m.model != model.model)
        {
            warn!(
                thread_id = %thread.thread,
                action = "turn_model_override_ignored",
                requested_provider = %requested.provider,
                requested = %requested.model,
                open_provider = %model.provider,
                open_model = %model.model,
                "per-turn models arrive in milestone 3; the turn runs on the thread's model"
            );
        }
        debug!(
            thread_id = %thread.thread,
            action = "start_turn",
            mode = ?overrides.mode,
            permission_preset = ?overrides.permission_preset,
            "per-turn mode and permission preset are not applied until milestone 3"
        );
        let UserInput::Text { text, attachments } = input;
        let line = user_message_line(&text, &attachments)?;
        let turn = TurnId::new();
        self.call(
            thread.thread,
            commands,
            "start_turn",
            CONTROL_TIMEOUT,
            |reply| ChildCommand::StartTurn {
                line,
                turn,
                model,
                reply,
            },
        )
        .await?;
        Ok(turn)
    }

    async fn respond_approval(
        &self,
        req: ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<(), HarnessError> {
        let known = lock(&self.pending).approval(&req).is_some();
        debug!(
            request_id = %req,
            action = "respond_approval",
            known,
            decision = ?decision,
            "approval answers arrive in milestone 3"
        );
        Err(HarnessError::Unsupported(
            "Claude Code approvals are answered from milestone 3".into(),
        ))
    }

    async fn respond_server_request(
        &self,
        req: ServerRequestId,
        response: ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        let known = lock(&self.pending).server_request(&req).is_some();
        let _ = response;
        debug!(
            request_id = %req,
            action = "respond_server_request",
            known,
            "server request answers arrive in milestone 3"
        );
        Err(HarnessError::Unsupported(
            "Claude Code approvals are answered from milestone 3".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io::Write;

    use giskard_core::event::AgentEvent;
    use giskard_core::turn::{Mode, PermissionPreset, TurnStatusKind};
    use giskard_harness::{
        EnvOverlay, EventStreamError, ThreadUpdateStream, thread_update_channel,
    };

    use super::*;
    use crate::process::MAX_STDOUT_LINE_BYTES;
    use crate::process::tests::fake_claude;
    use crate::session::tests::{
        Action, ScriptRecord, ScriptedChild, Step, control, fixture_lines, user,
    };

    const WORKSPACE: &str = "/work/project";
    const RESUME_ID: &str = "d9bfe887-699b-4c3d-9d6f-b7001e75764b";

    // ---- scaffolding ---------------------------------------------------------------------------

    #[derive(Default)]
    struct ScriptedSpawner {
        children: Mutex<VecDeque<ScriptedChild>>,
        argv: Mutex<Vec<Vec<String>>>,
    }

    #[async_trait]
    impl ChildSpawner for ScriptedSpawner {
        async fn spawn(
            &self,
            argv: &[String],
            _cwd: &Path,
            _context: &ChildLogContext,
        ) -> Result<Box<dyn ClaudeChild>, HarnessError> {
            lock(&self.argv).push(argv.to_vec());
            match lock(&self.children).pop_front() {
                Some(child) => Ok(Box::new(child)),
                None => Err(HarnessError::Spawn("no scripted child left".into())),
            }
        }
    }

    impl ScriptedSpawner {
        fn spawns(&self) -> Vec<Vec<String>> {
            lock(&self.argv).clone()
        }
    }

    fn harness(children: Vec<ScriptedChild>) -> (Arc<ClaudeHarness>, Arc<ScriptedSpawner>) {
        let spawner = Arc::new(ScriptedSpawner {
            children: Mutex::new(children.into()),
            argv: Mutex::default(),
        });
        let harness = ClaudeHarness::with_spawner(
            PathBuf::from(WORKSPACE),
            ClaudeLaunchOptions::default(),
            spawner.clone(),
        );
        (harness, spawner)
    }

    fn model(name: &str) -> ModelRef {
        ModelRef {
            provider: ANTHROPIC_PROVIDER_ID.into(),
            model: name.into(),
            reasoning_effort: None,
        }
    }

    fn open_options(
        thread: ThreadId,
        resume: Option<&str>,
        model_name: &str,
    ) -> (OpenThreadOptions, ThreadUpdateStream) {
        let (updates, stream) = thread_update_channel();
        let options = OpenThreadOptions {
            project: giskard_core::ids::ProjectId::new(),
            thread,
            workspace_root: PathBuf::from(WORKSPACE),
            resume: resume.map(str::to_owned),
            initial_model: model(model_name),
            updates,
        };
        (options, stream)
    }

    fn overrides() -> TurnOverrides {
        TurnOverrides {
            model: None,
            mode: Mode::Build,
            permission_preset: PermissionPreset::AskFirst,
        }
    }

    fn text(value: &str) -> UserInput {
        UserInput::Text {
            text: value.into(),
            attachments: Vec::new(),
        }
    }

    /// The `initialize` fixture's own response payload (its `models` populate the catalog).
    fn initialize_payload() -> Value {
        let path = format!(
            "{}/tests/fixtures/initialize.out.jsonl",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(path).unwrap();
        let first: Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
        first["response"]["response"].clone()
    }

    fn settings(applied: &str) -> Value {
        json!({"applied": {"model": applied, "effort": null}, "effective": {}, "sources": []})
    }

    fn handshake_steps(applied: &str) -> Vec<Step> {
        vec![
            Step::OnStdin(
                control("initialize"),
                vec![Action::Respond(initialize_payload())],
            ),
            Step::OnStdin(
                control("get_settings"),
                vec![Action::Respond(settings(applied))],
            ),
        ]
    }

    fn scripted(
        mut steps: Vec<Step>,
        rest: Vec<Step>,
    ) -> (ScriptedChild, Arc<Mutex<ScriptRecord>>) {
        steps.extend(rest);
        ScriptedChild::new(steps)
    }

    fn written(record: &Arc<Mutex<ScriptRecord>>) -> Vec<Value> {
        lock(record)
            .written
            .iter()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn missing_transcript_exit() -> Action {
        let result = fixture_lines("resume-missing")
            .into_iter()
            .find(|line| line.contains("\"type\": \"result\""))
            .unwrap();
        Action::Emit(vec![result])
    }

    /// Events until `TurnCompleted` (inclusive), bounded so a broken test fails instead of hangs.
    async fn until_completed(stream: &mut AgentEventStream) -> Vec<AgentEvent> {
        let mut events = Vec::new();
        loop {
            let event = tokio::time::timeout(Duration::from_secs(10), stream.recv())
                .await
                .expect("timed out waiting for TurnCompleted")
                .expect("stream ended before TurnCompleted");
            let done = matches!(event, AgentEvent::TurnCompleted { .. });
            events.push(event);
            if done {
                return events;
            }
        }
    }

    /// Events until the stream closes.
    async fn until_closed(stream: &mut AgentEventStream) -> Vec<AgentEvent> {
        let mut events = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(10), stream.recv())
                .await
                .expect("timed out waiting for the stream to close")
            {
                Ok(event) => events.push(event),
                Err(EventStreamError::Closed) => return events,
                Err(other) => panic!("unexpected stream error {other:?}"),
            }
        }
    }

    fn completion(events: &[AgentEvent]) -> (TurnId, TurnStatusKind, Option<String>) {
        events
            .iter()
            .find_map(|event| match event {
                AgentEvent::TurnCompleted { turn, status, .. } => {
                    Some((*turn, status.kind, status.message.clone()))
                }
                _ => None,
            })
            .expect("no TurnCompleted")
    }

    async fn until_no_children(harness: &ClaudeHarness) {
        for _ in 0..1000 {
            if harness.live_children() == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("children never went away");
    }

    #[derive(Clone)]
    struct LogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for LogWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            lock(&self.0).extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Capture this thread's logs (the test runtime is single-threaded, so the supervisor's too).
    fn capture_logs() -> (Arc<Mutex<Vec<u8>>>, tracing::subscriber::DefaultGuard) {
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || LogWriter(writer.clone()))
            .finish();
        (output, tracing::subscriber::set_default(subscriber))
    }

    fn logs(output: &Arc<Mutex<Vec<u8>>>) -> String {
        String::from_utf8(lock(output).clone()).unwrap()
    }

    // ---- open ----------------------------------------------------------------------------------

    #[tokio::test]
    async fn open_thread_handshakes_and_returns_a_subscribable_handle() {
        let (child, record) = scripted(handshake_steps("sonnet"), Vec::new());
        let (harness, spawner) = harness(vec![child]);
        let thread = ThreadId::new();
        let (options, _updates) = open_options(thread, None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();

        assert_eq!(handle.thread, thread);
        assert!(uuid::Uuid::parse_str(&handle.harness_thread_id).is_ok());
        assert_eq!(handle.workspace_root, PathBuf::from(WORKSPACE));
        assert_eq!(handle.resumed_model, Some(model("sonnet")));
        assert!(handle.warning.is_none());
        let argv = &spawner.spawns()[0];
        let at = argv.iter().position(|arg| arg == "--session-id").unwrap();
        assert_eq!(argv[at + 1], handle.harness_thread_id);

        let subtypes: Vec<_> = written(&record)
            .iter()
            .map(|line| line["request"]["subtype"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(subtypes, ["initialize", "get_settings"]);

        // A reader exists before the child wrote a frame, and nothing is in it yet.
        let mut stream = harness.subscribe(&handle);
        assert!(stream.try_recv().is_none());

        // The handshake's catalog answers `list_models` without a probe.
        let models = harness.list_models().await.unwrap();
        assert!(models.iter().any(|m| m.model == "sonnet"));
        assert_eq!(spawner.spawns().len(), 1);

        // A second open of the same thread returns the same child.
        let (options, _updates) = open_options(thread, None, "sonnet");
        let again = harness.open_thread(options).await.unwrap();
        assert_eq!(again.harness_thread_id, handle.harness_thread_id);
        assert_eq!(spawner.spawns().len(), 1);
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn open_thread_reports_the_applied_model_when_it_differs() {
        let (resolved, _) = scripted(handshake_steps("claude-sonnet-5-5"), Vec::new());
        let (other, _) = scripted(handshake_steps("claude-opus-5-5"), Vec::new());
        let (harness, _) = harness(vec![resolved, other]);
        let (output, _guard) = capture_logs();

        // The catalog's resolved id for the requested alias counts as applied.
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        assert_eq!(handle.resumed_model, Some(model("sonnet")));
        assert!(!logs(&output).contains("model_not_applied"));

        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        assert_eq!(handle.resumed_model, Some(model("claude-opus-5-5")));
        let logs = logs(&output);
        assert!(logs.contains("action=\"model_not_applied\""), "{logs}");
        assert!(logs.contains("requested=sonnet"), "{logs}");
        assert!(logs.contains("applied=claude-opus-5-5"), "{logs}");
        harness.shutdown().await.unwrap();
    }

    // ---- turns ---------------------------------------------------------------------------------

    #[tokio::test]
    async fn a_text_turn_streams_to_the_log_and_completes() {
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                user(),
                vec![Action::EmitFixture {
                    name: "text-turn",
                    skip_types: &[],
                }],
            )],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);

        let turn = harness
            .start_turn(
                &handle,
                text("Reply with exactly one word: pong"),
                overrides(),
            )
            .await
            .unwrap();
        let events = until_completed(&mut stream).await;

        assert!(matches!(events[0], AgentEvent::TurnStarted { turn: t, .. } if t == turn));
        assert!(
            events
                .iter()
                .any(|event| matches!(event, AgentEvent::ItemDelta { .. }))
        );
        let usage_models: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::TurnUsageUpdated { model, .. } => Some(model.clone()),
                _ => None,
            })
            .collect();
        assert!(!usage_models.is_empty());
        assert!(usage_models.iter().all(|m| *m == Some(model("sonnet"))));
        assert_eq!(completion(&events), (turn, TurnStatusKind::Completed, None));

        let message = written(&record).pop().unwrap();
        assert_eq!(
            message,
            json!({"type": "user", "message": {"role": "user", "content": [
                {"type": "text", "text": "Reply with exactly one word: pong"}
            ]}})
        );
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn start_turn_while_a_turn_is_active_is_thread_busy() {
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                user(),
                vec![Action::EmitFixturePrefix {
                    name: "text-turn",
                    count: 3,
                }],
            )],
        );
        let (harness, _) = harness(vec![child]);
        let thread = ThreadId::new();
        let (options, _updates) = open_options(thread, None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();

        harness
            .start_turn(&handle, text("first"), overrides())
            .await
            .unwrap();
        let error = harness
            .start_turn(&handle, text("second"), overrides())
            .await
            .unwrap_err();
        assert!(matches!(error, HarnessError::ThreadBusy { thread: t } if t == thread));
        let users = written(&record)
            .iter()
            .filter(|line| line["type"] == "user")
            .count();
        assert_eq!(users, 1, "a user message is never queued behind another");
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn only_another_model_is_an_ignored_override() {
        let (child, _) = scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(
                    user(),
                    vec![Action::EmitFixture {
                        name: "text-turn",
                        skip_types: &[],
                    }],
                ),
                Step::OnStdin(
                    user(),
                    vec![Action::EmitFixture {
                        name: "text-turn",
                        skip_types: &[],
                    }],
                ),
            ],
        );
        let (harness, _) = harness(vec![child]);
        let (output, _guard) = capture_logs();
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);

        let mut same_model = overrides();
        same_model.model = Some(ModelRef {
            reasoning_effort: Some(Effort("high".into())),
            ..model("sonnet")
        });
        harness
            .start_turn(&handle, text("one"), same_model)
            .await
            .unwrap();
        until_completed(&mut stream).await;
        assert!(!logs(&output).contains("turn_model_override_ignored"));

        let mut other_model = overrides();
        other_model.model = Some(model("opus"));
        harness
            .start_turn(&handle, text("two"), other_model)
            .await
            .unwrap();
        until_completed(&mut stream).await;
        let logs = logs(&output);
        let line = logs
            .lines()
            .find(|line| line.contains("action=\"turn_model_override_ignored\""))
            .unwrap_or_else(|| panic!("{logs}"));
        assert!(line.contains("requested=opus"), "{line}");
        assert!(line.contains("open_model=sonnet"), "{line}");
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn interrupt_resolves_on_the_control_response_and_the_turn_ends_interrupted() {
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(user(), Vec::new()),
                Step::OnStdin(
                    control("interrupt"),
                    vec![
                        Action::Respond(json!({"still_queued": []})),
                        Action::EmitFixture {
                            name: "cancel",
                            skip_types: &["control_request"],
                        },
                    ],
                ),
            ],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);

        let turn = harness
            .start_turn(&handle, text("run something"), overrides())
            .await
            .unwrap();
        harness.interrupt(&handle).await.unwrap();
        let events = until_completed(&mut stream).await;
        let (completed, status, _) = completion(&events);
        assert_eq!(completed, turn);
        assert_eq!(status, TurnStatusKind::Interrupted);

        let last = written(&record).pop().unwrap();
        assert_eq!(last["type"], "control_request");
        assert_eq!(last["request"], json!({"subtype": "interrupt"}));
        assert!(uuid::Uuid::parse_str(last["request_id"].as_str().unwrap()).is_ok());
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn interrupt_while_idle_is_ok() {
        let (child, _) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                control("interrupt"),
                vec![Action::Respond(json!({"still_queued": []}))],
            )],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        harness.interrupt(&handle).await.unwrap();
        assert!(
            stream.try_recv().is_none(),
            "an idle interrupt emits nothing"
        );

        let unknown = ThreadHandle::detached(ThreadId::new(), RESUME_ID.into());
        assert!(matches!(
            harness.interrupt(&unknown).await,
            Err(HarnessError::ThreadNotFound(_))
        ));
        harness.shutdown().await.unwrap();
    }

    // ---- resume --------------------------------------------------------------------------------

    #[tokio::test]
    async fn a_resume_that_finds_no_transcript_respawns_with_the_same_id() {
        let sentence = format!("No conversation found with session ID: {RESUME_ID}");
        let (missing, _) = ScriptedChild::new(vec![Step::OnStdin(
            control("initialize"),
            vec![
                missing_transcript_exit(),
                Action::Exit {
                    code: 1,
                    stderr: vec![sentence.clone()],
                },
            ],
        )]);
        let (fresh, _) = scripted(handshake_steps("sonnet"), Vec::new());
        let (harness, spawner) = harness(vec![missing, fresh]);
        let (output, _guard) = capture_logs();
        let (options, _updates) = open_options(ThreadId::new(), Some(RESUME_ID), "sonnet");
        let handle = harness.open_thread(options).await.unwrap();

        assert_eq!(handle.harness_thread_id, RESUME_ID);
        let warning = handle.warning.as_ref().unwrap();
        assert_eq!(warning.code, "claude_resume_failed");
        assert_eq!(
            warning.message,
            "Agent context was lost; started a fresh Claude Code session. History is intact."
        );
        assert_eq!(warning.detail.as_deref(), Some(sentence.as_str()));
        let spawns = spawner.spawns();
        assert_eq!(spawns.len(), 2);
        assert!(spawns[0].windows(2).any(|w| w == ["--resume", RESUME_ID]));
        assert!(
            spawns[1]
                .windows(2)
                .any(|w| w == ["--session-id", RESUME_ID])
        );
        assert!(!spawns[1].iter().any(|arg| arg == "--resume"));
        assert!(logs(&output).contains("action=\"claude_resume_failed\""));
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_resume_failure_that_is_not_a_missing_transcript_is_an_error() {
        let in_use = format!("Error: Session ID {RESUME_ID} is already in use.");
        let (child, _) = ScriptedChild::new(vec![Step::OnStdin(
            control("initialize"),
            vec![Action::Exit {
                code: 1,
                stderr: vec![in_use.clone()],
            }],
        )]);
        let (harness, spawner) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), Some(RESUME_ID), "sonnet");
        let error = harness.open_thread(options).await.unwrap_err();
        assert!(
            matches!(&error, HarnessError::Spawn(message) if message.contains(&in_use) && message.contains("code 1")),
            "{error}"
        );
        assert_eq!(spawner.spawns().len(), 1, "no same-id respawn");
    }

    #[tokio::test]
    async fn a_second_failed_respawn_returns_the_second_error() {
        let (missing, _) = ScriptedChild::new(vec![Step::OnStdin(
            control("initialize"),
            vec![
                missing_transcript_exit(),
                Action::Exit {
                    code: 1,
                    stderr: vec![format!(
                        "No conversation found with session ID: {RESUME_ID}"
                    )],
                },
            ],
        )]);
        let (in_use, _) = ScriptedChild::new(vec![Step::OnStdin(
            control("initialize"),
            vec![Action::Exit {
                code: 1,
                stderr: vec![format!("Error: Session ID {RESUME_ID} is already in use.")],
            }],
        )]);
        let (harness, spawner) = harness(vec![missing, in_use]);
        let (options, _updates) = open_options(ThreadId::new(), Some(RESUME_ID), "sonnet");
        let error = harness.open_thread(options).await.unwrap_err();
        assert!(
            matches!(&error, HarnessError::Spawn(message) if message.contains("already in use")),
            "{error}"
        );
        assert_eq!(spawner.spawns().len(), 2);
        assert_eq!(harness.live_children(), 0);
    }

    #[tokio::test]
    async fn an_exit_after_initialize_names_the_unanswered_request() {
        let (child, _) = ScriptedChild::new(vec![
            Step::OnStdin(
                control("initialize"),
                vec![Action::Respond(initialize_payload())],
            ),
            Step::OnStdin(
                control("get_settings"),
                vec![Action::Exit {
                    code: 2,
                    stderr: vec!["fatal: settings unreadable".into()],
                }],
            ),
        ]);
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let error = harness.open_thread(options).await.unwrap_err();
        assert!(
            matches!(&error, HarnessError::Spawn(message)
                if message == "claude exited with code 2 before answering get_settings: \
                               fatal: settings unreadable"),
            "{error}"
        );
        assert_eq!(harness.live_children(), 0);
    }

    #[tokio::test]
    async fn an_unauthenticated_exit_is_reported_as_such() {
        let (child, _) = ScriptedChild::new(vec![Step::OnStdin(
            control("initialize"),
            vec![Action::Exit {
                code: 1,
                stderr: vec!["Invalid API key · Please run /login".into()],
            }],
        )]);
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        assert!(matches!(
            harness.open_thread(options).await,
            Err(HarnessError::Unauthenticated)
        ));
    }

    #[tokio::test]
    async fn a_task_id_is_never_resumed() {
        let (harness, spawner) = harness(Vec::new());
        let (options, _updates) = open_options(
            ThreadId::new(),
            Some("task:toolu_01DSgcYLdZqTSvfAwdnE2njN"),
            "sonnet",
        );
        assert!(matches!(
            harness.open_thread(options).await,
            Err(HarnessError::Unsupported(_))
        ));
        let (options, _updates) = open_options(ThreadId::new(), Some("not-a-uuid"), "sonnet");
        assert!(matches!(
            harness.open_thread(options).await,
            Err(HarnessError::Protocol(message)) if message.contains("not-a-uuid")
        ));
        assert!(spawner.spawns().is_empty());
    }

    #[tokio::test]
    async fn a_resumed_thread_restores_its_context_window() {
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(
                    control("get_context_usage"),
                    vec![Action::Respond(json!({
                        "totalTokens": 12000, "maxTokens": 200000, "rawMaxTokens": 200000,
                        "percentage": 6, "categories": []
                    }))],
                ),
                Step::OnStdin(
                    user(),
                    vec![Action::EmitFixture {
                        name: "text-turn",
                        skip_types: &[],
                    }],
                ),
            ],
        );
        let (harness, _) = harness(vec![child]);
        let (options, mut updates) = open_options(ThreadId::new(), Some(RESUME_ID), "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        assert!(handle.warning.is_none());
        assert_eq!(
            updates.recv().await,
            Some(ThreadUpdate::ContextWindowRestored {
                model: model("sonnet"),
                context_window: 200_000,
            })
        );
        let subtypes: Vec<_> = written(&record)
            .iter()
            .map(|line| line["request"]["subtype"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            subtypes,
            ["initialize", "get_settings", "get_context_usage"]
        );

        let mut stream = harness.subscribe(&handle);
        harness
            .start_turn(&handle, text("pong?"), overrides())
            .await
            .unwrap();
        let events = until_completed(&mut stream).await;
        let first_window = events.iter().find_map(|event| match event {
            AgentEvent::TurnUsageUpdated { context_window, .. } => Some(*context_window),
            _ => None,
        });
        assert_eq!(first_window, Some(Some(200_000)));
        harness.shutdown().await.unwrap();
    }

    // ---- child exit ----------------------------------------------------------------------------

    #[tokio::test]
    async fn child_exit_mid_turn_fails_the_turn_and_closes_the_stream() {
        let (child, _) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                user(),
                vec![
                    Action::EmitFixturePrefix {
                        name: "text-turn",
                        count: 6,
                    },
                    Action::Exit {
                        code: 3,
                        stderr: vec!["fatal: out of cheese".into()],
                    },
                ],
            )],
        );
        let (harness, _) = harness(vec![child]);
        let (output, _guard) = capture_logs();
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        let turn = harness
            .start_turn(&handle, text("go"), overrides())
            .await
            .unwrap();
        let events = until_closed(&mut stream).await;
        let (completed, status, message) = completion(&events);
        assert_eq!(completed, turn);
        assert_eq!(status, TurnStatusKind::Failed);
        assert!(message.unwrap().contains("(code 3)"));
        assert!(matches!(
            events.last(),
            Some(AgentEvent::TurnCompleted { .. })
        ));

        until_no_children(&harness).await;
        assert!(matches!(
            harness.subscribe(&handle).recv().await,
            Err(EventStreamError::Closed)
        ));
        assert!(matches!(
            harness
                .start_turn(&handle, text("again"), overrides())
                .await,
            Err(HarnessError::ThreadNotFound(_))
        ));
        let logs = logs(&output);
        let line = logs
            .lines()
            .find(|line| line.contains("claude child exited unexpectedly"))
            .unwrap_or_else(|| panic!("{logs}"));
        assert!(line.contains("WARN"), "{line}");
        assert!(line.contains("exit_code=3"), "{line}");
        assert!(line.contains("out of cheese"), "{line}");
        assert!(line.contains("live_children=0"), "{line}");
    }

    #[tokio::test]
    async fn a_closed_log_is_reported_not_ignored() {
        let (child, _) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                user(),
                vec![Action::EmitFixture {
                    name: "text-turn",
                    skip_types: &[],
                }],
            )],
        );
        let (harness, _) = harness(vec![child]);
        let (output, _guard) = capture_logs();
        let thread = ThreadId::new();
        let (options, _updates) = open_options(thread, None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        lock(&harness.children).get(&thread).unwrap().log.close();
        harness
            .start_turn(&handle, text("go"), overrides())
            .await
            .unwrap();
        harness.shutdown().await.unwrap();
        let logs = logs(&output);
        assert_eq!(
            logs.matches("the thread's event log is closed").count(),
            1,
            "{logs}"
        );
        let exit = logs
            .lines()
            .find(|line| line.contains("action=\"child_exited\""))
            .unwrap();
        assert!(!exit.contains("dropped_events=0"), "{exit}");
    }

    // ---- rename, archive, delete, shutdown -----------------------------------------------------

    #[tokio::test]
    async fn set_thread_name_renames_a_live_session_and_is_a_no_op_without_one() {
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                control("rename_session"),
                vec![Action::Respond(Value::Null)],
            )],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        harness
            .set_thread_name(&handle, "Fix the build")
            .await
            .unwrap();
        let last = written(&record).pop().unwrap();
        assert_eq!(
            last["request"],
            json!({"subtype": "rename_session", "title": "Fix the build", "source": "host"})
        );

        let cold = ThreadHandle::detached(ThreadId::new(), RESUME_ID.into());
        harness.set_thread_name(&cold, "x").await.unwrap();
        let task = ThreadHandle::detached(ThreadId::new(), "task:toolu_1".into());
        harness.set_thread_name(&task, "x").await.unwrap();
        assert_eq!(written(&record).len(), 3);
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_refused_rename_is_an_error() {
        let (child, _) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                control("rename_session"),
                vec![Action::RespondError("no session")],
            )],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        assert!(matches!(
            harness.set_thread_name(&handle, "x").await,
            Err(HarnessError::Protocol(message)) if message == "no session"
        ));
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn archiving_and_deleting_stop_the_child() {
        let (busy, busy_record) = scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(
                    user(),
                    vec![Action::EmitFixturePrefix {
                        name: "text-turn",
                        count: 3,
                    }],
                ),
                Step::OnStdin(
                    control("interrupt"),
                    vec![
                        Action::Respond(json!({"still_queued": []})),
                        Action::EmitFixture {
                            name: "cancel",
                            skip_types: &["control_request"],
                        },
                    ],
                ),
            ],
        );
        let (idle, idle_record) = scripted(handshake_steps("sonnet"), Vec::new());
        let (harness, _) = harness(vec![busy, idle]);
        let (output, _guard) = capture_logs();

        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let busy = harness.open_thread(options).await.unwrap();
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let idle = harness.open_thread(options).await.unwrap();
        let mut busy_stream = harness.subscribe(&busy);
        harness
            .start_turn(&busy, text("go"), overrides())
            .await
            .unwrap();

        harness.set_thread_archived(&busy, true).await.unwrap();
        let events = until_closed(&mut busy_stream).await;
        assert_eq!(completion(&events).1, TurnStatusKind::Interrupted);
        {
            let record = lock(&busy_record);
            let interrupt = record
                .written
                .iter()
                .position(|line| line.contains("\"interrupt\""))
                .expect("the live turn was interrupted");
            assert_eq!(interrupt, record.written.len() - 1);
            assert!(record.stdin_closed);
            assert!(!record.killed);
        }
        assert_eq!(harness.live_children(), 1);

        // Unarchiving writes nothing; deleting stops an idle child without an interrupt.
        let before = lock(&idle_record).written.len();
        harness.set_thread_archived(&idle, false).await.unwrap();
        assert_eq!(lock(&idle_record).written.len(), before);
        harness.delete_thread(&idle).await.unwrap();
        {
            let record = lock(&idle_record);
            assert_eq!(record.written.len(), before);
            assert!(record.stdin_closed);
        }
        assert_eq!(harness.live_children(), 0);

        // A cold thread and a sub-agent thread have nothing to stop.
        harness
            .delete_thread(&ThreadHandle::detached(ThreadId::new(), RESUME_ID.into()))
            .await
            .unwrap();
        harness
            .set_thread_archived(
                &ThreadHandle::detached(ThreadId::new(), "task:toolu_1".into()),
                true,
            )
            .await
            .unwrap();
        let logs = logs(&output);
        assert!(logs.contains("action=\"stop_interrupt\""), "{logs}");
        assert_eq!(
            logs.matches("action=\"thread_stopped\"").count(),
            2,
            "{logs}"
        );
        assert!(!logs.contains("stop_kill"), "{logs}");
    }

    #[tokio::test(start_paused = true)]
    async fn stop_kills_a_child_that_ignores_eof() {
        let (child, record) = scripted(handshake_steps("sonnet"), Vec::new());
        let (harness, _) = harness(vec![child.ignoring_eof()]);
        let (output, _guard) = capture_logs();
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let started = Instant::now();
        harness.set_thread_archived(&handle, true).await.unwrap();
        let elapsed = started.elapsed();
        assert!(elapsed >= crate::session::STOP_EXIT_GRACE, "{elapsed:?}");
        assert!(elapsed < STOP_TIMEOUT, "{elapsed:?}");
        let record = lock(&record);
        assert!(record.stdin_closed);
        assert!(record.killed);
        let logs = logs(&output);
        assert!(logs.contains("action=\"stop_kill\""), "{logs}");
        assert!(logs.contains("signal=9"), "{logs}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_stop_that_times_out_still_closes_the_stream() {
        // The child is killed on the exit grace but its exit is never collected, so the
        // supervisor never reaches its own exit handling and the stop times out.
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                user(),
                vec![Action::EmitFixturePrefix {
                    name: "text-turn",
                    count: 3,
                }],
            )],
        );
        let (harness, _) = harness(vec![child.ignoring_eof().hanging_on_wait()]);
        let (output, _guard) = capture_logs();
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        harness
            .start_turn(&handle, text("go"), overrides())
            .await
            .unwrap();

        let started = Instant::now();
        harness.delete_thread(&handle).await.unwrap();
        assert!(started.elapsed() >= STOP_TIMEOUT);
        assert!(lock(&record).killed);
        assert_eq!(harness.live_children(), 0);
        let events = until_closed(&mut stream).await;
        assert!(matches!(
            events.first(),
            Some(AgentEvent::TurnStarted { .. })
        ));
        let logs = logs(&output);
        assert!(
            logs.contains("aborted its supervisor and closed its event stream"),
            "{logs}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_start_turn_whose_caller_timed_out_is_never_written() {
        let (child, record) = scripted(handshake_steps("sonnet"), Vec::new());
        let (child, gate) = child.gated();
        let (harness, _) = harness(vec![child]);
        let (output, _guard) = capture_logs();
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);

        // The CLI stops reading stdin: the supervisor blocks writing the interrupt, so the
        // `StartTurn` queued behind it outlives its caller's timeout.
        gate.send_replace(false);
        let (interrupt, turn) = tokio::join!(
            harness.interrupt(&handle),
            harness.start_turn(&handle, text("late"), overrides())
        );
        assert!(matches!(interrupt, Err(HarnessError::Timeout(_))));
        assert!(matches!(turn, Err(HarnessError::Timeout(_))));

        // The CLI reads again; the supervisor drains its queue.
        gate.send_replace(true);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let lines = written(&record);
        assert_eq!(lines.last().unwrap()["request"]["subtype"], "interrupt");
        assert!(lines.iter().all(|line| line["type"] != "user"));
        assert!(
            stream.try_recv().is_none(),
            "no TurnStarted for a turn nobody admitted"
        );
        let logs = logs(&output);
        assert!(
            logs.contains("the caller gave up on this turn before it was started"),
            "{logs}"
        );
        harness.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_late_answer_to_a_timed_out_handshake_request_is_expected() {
        let (child, _) = ScriptedChild::new(vec![
            Step::OnStdin(
                control("initialize"),
                vec![Action::Respond(initialize_payload())],
            ),
            Step::OnStdin(
                control("get_settings"),
                vec![Action::RespondOnNextWrite(settings("sonnet"))],
            ),
            Step::OnStdin(
                control("rename_session"),
                vec![Action::Respond(Value::Null)],
            ),
        ]);
        let (harness, _) = harness(vec![child]);
        let (output, _guard) = capture_logs();
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        assert_eq!(handle.resumed_model, None, "get_settings timed out");

        // The rename's write releases the late `get_settings` answer ahead of its own.
        harness.set_thread_name(&handle, "named").await.unwrap();
        let logs = logs(&output);
        assert!(
            logs.contains("late answer to a handshake request that timed out"),
            "{logs}"
        );
        assert!(
            !logs.contains("control response for a request nobody is waiting on"),
            "{logs}"
        );
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_stops_every_child_and_is_idempotent() {
        let (first, first_record) = scripted(handshake_steps("sonnet"), Vec::new());
        let (second, second_record) = scripted(handshake_steps("sonnet"), Vec::new());
        let (harness, _) = harness(vec![first, second]);
        let (output, _guard) = capture_logs();
        for _ in 0..2 {
            let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
            harness.open_thread(options).await.unwrap();
        }
        assert_eq!(harness.live_children(), 2);
        harness.shutdown().await.unwrap();
        assert_eq!(harness.live_children(), 0);
        assert!(lock(&first_record).stdin_closed);
        assert!(lock(&second_record).stdin_closed);
        harness.shutdown().await.unwrap();

        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        assert!(matches!(
            harness.open_thread(options).await,
            Err(HarnessError::Transport(message)) if message.contains("shut down")
        ));
        assert!(matches!(
            harness.list_models().await,
            Err(HarnessError::Transport(_))
        ));
        let logs = logs(&output);
        assert!(logs.contains("children_stopped=2"), "{logs}");
        assert!(logs.contains("children_stopped=0"), "{logs}");
    }

    // ---- asks ----------------------------------------------------------------------------------

    #[tokio::test]
    async fn a_pending_ask_is_recorded_and_respond_approval_is_unsupported() {
        let ask_line = fixture_lines("tool-allowed")
            .into_iter()
            .position(|line| line.contains("\"control_request\""))
            .unwrap();
        let request_id: String = {
            let line = &fixture_lines("tool-allowed")[ask_line];
            serde_json::from_str::<Value>(line).unwrap()["request_id"]
                .as_str()
                .unwrap()
                .to_owned()
        };
        let (child, _) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                user(),
                vec![Action::EmitFixturePrefix {
                    name: "tool-allowed",
                    count: ask_line + 1,
                }],
            )],
        );
        let (harness, _) = harness(vec![child]);
        let thread = ThreadId::new();
        let (options, _updates) = open_options(thread, None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        harness
            .start_turn(&handle, text("touch probe.txt"), overrides())
            .await
            .unwrap();
        let approval = loop {
            let event = tokio::time::timeout(Duration::from_secs(10), stream.recv())
                .await
                .unwrap()
                .unwrap();
            if let AgentEvent::ApprovalRequested { request, .. } = event {
                break request.id;
            }
        };
        assert_eq!(approval.0, request_id);
        assert_eq!(
            lock(&harness.pending)
                .approval(&approval)
                .map(|ask| ask.thread),
            Some(thread)
        );
        assert!(matches!(
            harness
                .respond_approval(approval.clone(), ApprovalDecision::Accept)
                .await,
            Err(HarnessError::Unsupported(_))
        ));
        assert!(
            lock(&harness.pending).approval(&approval).is_some(),
            "kept for milestone 3"
        );
        harness.delete_thread(&handle).await.unwrap();
        assert_eq!(lock(&harness.pending).len(), 0);
    }

    // ---- isolation and timeouts ----------------------------------------------------------------

    #[tokio::test]
    async fn an_overlong_stdout_line_is_fatal_for_that_child_only() {
        let (broken, broken_record) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                user(),
                vec![Action::Emit(vec!["x".repeat(MAX_STDOUT_LINE_BYTES + 1)])],
            )],
        );
        let (healthy, _) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                user(),
                vec![Action::EmitFixture {
                    name: "text-turn",
                    skip_types: &[],
                }],
            )],
        );
        let (harness, _) = harness(vec![broken, healthy]);
        let (output, _guard) = capture_logs();
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let broken = harness.open_thread(options).await.unwrap();
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let healthy = harness.open_thread(options).await.unwrap();
        let mut broken_stream = harness.subscribe(&broken);
        let mut healthy_stream = harness.subscribe(&healthy);

        harness
            .start_turn(&broken, text("go"), overrides())
            .await
            .unwrap();
        let events = until_closed(&mut broken_stream).await;
        assert_eq!(completion(&events).1, TurnStatusKind::Failed);
        assert!(lock(&broken_record).killed);
        assert!(logs(&output).contains("action=\"read_stdout\""));

        // The sibling is untouched and still runs turns.
        assert!(healthy_stream.try_recv().is_none());
        harness
            .start_turn(&healthy, text("go"), overrides())
            .await
            .unwrap();
        let events = until_completed(&mut healthy_stream).await;
        assert_eq!(completion(&events).1, TurnStatusKind::Completed);
        harness.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn the_handshake_times_out() {
        let (child, record) = ScriptedChild::new(Vec::new());
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let error = harness.open_thread(options).await.unwrap_err();
        assert!(
            matches!(&error, HarnessError::Timeout(message) if message.contains("initialize")),
            "{error}"
        );
        assert!(lock(&record).killed);
        assert_eq!(harness.live_children(), 0);
    }

    // ---- catalog and providers -----------------------------------------------------------------

    fn probe_child() -> (ScriptedChild, Arc<Mutex<ScriptRecord>>) {
        ScriptedChild::new(vec![Step::OnStdin(
            control("initialize"),
            vec![Action::Respond(initialize_payload())],
        )])
    }

    #[tokio::test]
    async fn list_models_uses_the_freshest_handshake_then_probes() {
        let (probe, probe_record) = probe_child();
        let mut narrow = initialize_payload();
        narrow["models"] = json!([
            {"value": "default", "resolvedModel": "claude-sonnet-5-5"},
            {"value": "sonnet", "resolvedModel": "claude-sonnet-5-5", "displayName": "Sonnet"},
        ]);
        let (session, _) = ScriptedChild::new(vec![
            Step::OnStdin(control("initialize"), vec![Action::Respond(narrow)]),
            Step::OnStdin(
                control("get_settings"),
                vec![Action::Respond(settings("sonnet"))],
            ),
        ]);
        let (harness, spawner) = harness(vec![probe, session]);

        let models = harness.list_models().await.unwrap();
        let probe_argv = &spawner.spawns()[0];
        assert_eq!(
            *probe_argv,
            crate::process::probe_argv(&ClaudeLaunchOptions::default())
        );
        assert!(
            !probe_argv
                .iter()
                .any(|arg| arg == "--session-id" || arg == "--model")
        );
        assert!(lock(&probe_record).stdin_closed, "the probe exits");
        assert_eq!(harness.live_children(), 0, "a probe is not a live child");
        assert!(models.iter().all(|m| m.model != "default"));
        let defaults: Vec<_> = models.iter().filter(|m| m.is_default).collect();
        assert_eq!(defaults.len(), 1);
        assert_eq!(defaults[0].model, "opus");

        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        harness.open_thread(options).await.unwrap();
        let models = harness.list_models().await.unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].model, "sonnet");
        assert!(models[0].is_default);
        assert_eq!(spawner.spawns().len(), 2);
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn list_models_shares_one_probe_between_concurrent_callers() {
        let (probe, _) = probe_child();
        let (harness, spawner) = harness(vec![probe]);
        let (first, second) = tokio::join!(harness.list_models(), harness.list_models());
        assert_eq!(first.unwrap(), second.unwrap());
        assert_eq!(spawner.spawns().len(), 1);
    }

    #[tokio::test]
    async fn a_failed_probe_is_an_error() {
        let (harness, _) = harness(Vec::new());
        assert!(matches!(
            harness.list_models().await,
            Err(HarnessError::Spawn(_))
        ));
    }

    #[tokio::test]
    async fn list_providers_reports_anthropic_with_the_overlay() {
        let env = EnvOverlay::new([("ANTHROPIC_BASE_URL".into(), "http://proxy".into())]);
        let harness = ClaudeHarness::new(
            PathBuf::from(WORKSPACE),
            ClaudeLaunchOptions {
                env: env.clone(),
                ..ClaudeLaunchOptions::default()
            },
        );
        let providers = harness.list_providers().await.unwrap();
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, ANTHROPIC_PROVIDER_ID);
        assert_eq!(
            providers[0].name.as_deref(),
            Some("Anthropic (Claude Code)")
        );
        assert_eq!(providers[0].base_url, None);
        assert_eq!(providers[0].auth, None);
        assert_eq!(providers[0].env, env);
        assert_eq!(harness.capabilities(), crate::capabilities());
    }

    // ---- the real process path (tests/fake-claude.sh) ------------------------------------------

    fn real_harness(env: &[(&str, &str)]) -> (Arc<ClaudeHarness>, tempfile::TempDir) {
        let workspace = tempfile::tempdir().unwrap();
        let harness = ClaudeHarness::new(
            workspace.path().to_path_buf(),
            ClaudeLaunchOptions {
                command: Some(fake_claude()),
                env: EnvOverlay::new(
                    env.iter()
                        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned())),
                ),
                ..ClaudeLaunchOptions::default()
            },
        );
        (harness, workspace)
    }

    fn real_options(
        workspace: &tempfile::TempDir,
        resume: Option<&str>,
    ) -> (OpenThreadOptions, ThreadUpdateStream) {
        let (mut options, updates) = open_options(ThreadId::new(), resume, "sonnet");
        options.workspace_root = workspace.path().to_path_buf();
        (options, updates)
    }

    #[tokio::test]
    async fn a_real_child_handshakes_and_completes_a_turn() {
        let (harness, workspace) = real_harness(&[]);
        let (options, _updates) = real_options(&workspace, None);
        let handle = harness.open_thread(options).await.unwrap();
        assert_eq!(handle.resumed_model, Some(model("sonnet")));
        let mut stream = harness.subscribe(&handle);
        let turn = harness
            .start_turn(&handle, text("pong?"), overrides())
            .await
            .unwrap();
        let events = until_completed(&mut stream).await;
        assert_eq!(completion(&events), (turn, TurnStatusKind::Completed, None));
        harness.interrupt(&handle).await.unwrap();
        harness.set_thread_name(&handle, "named").await.unwrap();
        harness.shutdown().await.unwrap();
        assert!(matches!(stream.recv().await, Err(EventStreamError::Closed)));
    }

    #[tokio::test]
    async fn a_real_resume_failure_respawns_and_warns() {
        let (harness, workspace) = real_harness(&[]);
        let (options, _updates) = real_options(&workspace, Some(RESUME_ID));
        let handle = harness.open_thread(options).await.unwrap();
        assert_eq!(handle.harness_thread_id, RESUME_ID);
        let warning = handle.warning.unwrap();
        assert_eq!(warning.code, "claude_resume_failed");
        assert_eq!(
            warning.detail,
            Some(format!(
                "No conversation found with session ID: {RESUME_ID}"
            ))
        );
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_real_child_that_exits_mid_turn_fails_the_turn_with_its_exit_code() {
        let (harness, workspace) = real_harness(&[("FAKE_CLAUDE_EXIT_MID_TURN", "5")]);
        let (options, _updates) = real_options(&workspace, None);
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        harness
            .start_turn(&handle, text("go"), overrides())
            .await
            .unwrap();
        let events = until_closed(&mut stream).await;
        let (_, status, message) = completion(&events);
        assert_eq!(status, TurnStatusKind::Failed);
        assert!(message.unwrap().contains("(code 3)"));
        until_no_children(&harness).await;
    }

    #[tokio::test]
    async fn a_real_spawn_failure_quotes_stderr() {
        let (harness, workspace) = real_harness(&[]);
        let harness = ClaudeHarness::new(
            workspace.path().to_path_buf(),
            ClaudeLaunchOptions {
                command: Some(fake_claude()),
                args: vec!["--permission-mode".into(), "bogus".into()],
                ..harness.launch.clone()
            },
        );
        let (options, _updates) = real_options(&workspace, None);
        let error = harness.open_thread(options).await.unwrap_err();
        assert!(
            matches!(&error, HarnessError::Spawn(message) if message.contains("'bogus' is invalid")),
            "{error}"
        );
    }

    #[tokio::test]
    async fn stop_kills_a_real_child_that_ignores_eof() {
        let (harness, workspace) = real_harness(&[("FAKE_CLAUDE_IGNORE_EOF", "1")]);
        let (options, _updates) = real_options(&workspace, None);
        let handle = harness.open_thread(options).await.unwrap();
        let started = std::time::Instant::now();
        harness.delete_thread(&handle).await.unwrap();
        let elapsed = started.elapsed();
        assert!(elapsed >= crate::session::STOP_EXIT_GRACE, "{elapsed:?}");
        assert!(elapsed < STOP_TIMEOUT, "{elapsed:?}");
        assert_eq!(harness.live_children(), 0);
    }
}
