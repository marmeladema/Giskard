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
use giskard_core::mcp::McpServerStatus;
use giskard_core::model::{Effort, ModelDescriptor, ModelRef};
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::turn::{Mode, PermissionPreset, TurnOverrides};
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
use crate::mcp::mcp_servers;
use crate::process::{
    ChildExit, ChildLogContext, ClaudeChild, ClaudeLaunchOptions, ExitKind, LaunchMode,
    SessionArgs, SessionFlag, bypass_refused_sentence, classify_exit, probe_argv,
    resume_missing_sentence, session_argv, spawn_child,
};
pub(crate) use crate::session::CONTROL_TIMEOUT;
use crate::session::{
    ChildCommand, ChildHandle, Children, Pending, PendingRequests, STOP_EXIT_GRACE,
    SupervisorParts, TurnSettings, control_line, control_outcome, lock, new_request_id,
    spawn_supervisor,
};

/// How long the CLI has to answer `initialize`.
pub(crate) const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a `start_turn` hand-off may take: up to four control requests (mode, model, effort,
/// read-back) plus the write.
pub(crate) const START_TURN_TIMEOUT: Duration = Duration::from_secs(30);
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
    //   belongs to, and what its answer must echo, for `respond_*`.
    // Source of truth: The supervisor records an entry when the mapper publishes the request.
    // Structural reason: The responses carry no thread (trait doc: ids are instance-unique).
    // Synchronization: A std mutex.
    // Invalidation/removal: An answer or a `control_cancel_request` removes one entry; the
    //   supervisor removes a thread's entries when its child exits; `shutdown` clears the map.
    pending: Pending,
    catalog: Arc<Mutex<Option<CatalogSnapshot>>>,
    /// Serializes probe children so concurrent `list_models` calls share one.
    probe: tokio::sync::Mutex<()>,
    /// The sentence in which the CLI refused a `bypassPermissions` launch, once one was refused.
    /// Later opens launch standard children directly, and `full_access` turns quote it.
    bypass_refused: Mutex<Option<String>>,
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
            bypass_refused: Mutex::new(None),
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

    /// The command channel, open model and launch mode of a thread's live child.
    fn live(&self, thread: ThreadId) -> Option<LiveChild> {
        lock(&self.children).get(&thread).map(|handle| LiveChild {
            commands: handle.commands.clone(),
            model: handle.model.clone(),
            launch_mode: handle.launch_mode,
        })
    }

    /// Spawn a session child in bypass mode unless the CLI already refused one, falling back to a
    /// standard launch with the same session flag when it refuses now.
    async fn spawn_child_for(
        &self,
        opts: &OpenThreadOptions,
        session: SessionFlag,
    ) -> Result<(Box<dyn ClaudeChild>, Handshake, LaunchMode), (HandshakeFailure, LaunchMode)> {
        let refused = lock(&self.bypass_refused).is_some();
        if refused {
            return self
                .spawn_and_handshake(opts, session, LaunchMode::Standard)
                .await
                .map(|(child, handshake)| (child, handshake, LaunchMode::Standard))
                .map_err(|failure| (failure, LaunchMode::Standard));
        }
        match self
            .spawn_and_handshake(opts, session.clone(), LaunchMode::Bypass)
            .await
        {
            Ok((child, handshake)) => Ok((child, handshake, LaunchMode::Bypass)),
            Err(HandshakeFailure::Exited {
                exit,
                result_errors,
                ..
            }) if classify_exit(&exit, &result_errors) == ExitKind::BypassRefused => {
                let sentence = bypass_refused_sentence(&exit, &result_errors);
                let first = {
                    let mut refused = lock(&self.bypass_refused);
                    let first = refused.is_none();
                    if first {
                        *refused = Some(sentence.clone());
                    }
                    first
                };
                if first {
                    warn!(
                        project_id = display_opt(self.launch.project_id),
                        harness = display_opt(self.launch.declaration.as_deref()),
                        thread_id = %opts.thread,
                        action = "bypass_refused",
                        exit_code = display_opt(exit.code),
                        sentence = %sentence,
                        "Claude Code refused a bypassPermissions launch; children launch in \
                         standard mode and full_access turns are refused"
                    );
                } else {
                    debug!(
                        thread_id = %opts.thread,
                        action = "bypass_refused",
                        "Claude Code refused a bypassPermissions launch again"
                    );
                }
                self.spawn_and_handshake(opts, session, LaunchMode::Standard)
                    .await
                    .map(|(child, handshake)| (child, handshake, LaunchMode::Standard))
                    .map_err(|failure| (failure, LaunchMode::Standard))
            }
            Err(failure) => Err((failure, LaunchMode::Bypass)),
        }
    }

    /// Spawn one session child and run its handshake. A child that fails the handshake has been
    /// reaped before this returns.
    async fn spawn_and_handshake(
        &self,
        opts: &OpenThreadOptions,
        session: SessionFlag,
        launch_mode: LaunchMode,
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
                launch_mode,
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
        let handshake = handshake(child.as_mut(), &context, resume, launch_mode).await?;
        debug!(
            thread_id = %opts.thread,
            harness_thread_id = %session_id,
            action = "handshake",
            resume,
            launch_mode = launch_mode.as_str(),
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
        let (snapshot, _) = self.probe(None).await?;
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

    /// Spawn a probe child, run `initialize` and then `follow_up` when given, and let it exit.
    /// The catalog snapshot is stored from `initialize` either way; the second value is
    /// `follow_up`'s success payload. Leaves no transcript.
    async fn probe(
        &self,
        follow_up: Option<&Value>,
    ) -> Result<(CatalogSnapshot, Option<Value>), HarnessError> {
        let context = self.context(None, None);
        let argv = probe_argv(&self.launch);
        let mut child = self
            .spawner
            .spawn(&argv, &self.workspace_root, &context)
            .await?;
        let mut early = Vec::new();
        let mut result_errors = Vec::new();
        let initialize = json!({"subtype": "initialize"});
        let reply = probe_request(
            child.as_mut(),
            &context,
            &initialize,
            &mut early,
            &mut result_errors,
        )
        .await?;
        let reply = InitializeReply::from_value(&reply);
        let snapshot = self.store_catalog(reply.models.as_deref().unwrap_or_default(), "probe");
        let answer = match follow_up {
            Some(body) => Some(
                probe_request(
                    child.as_mut(),
                    &context,
                    body,
                    &mut early,
                    &mut result_errors,
                )
                .await?,
            ),
            None => None,
        };
        reap(child.as_mut()).await;
        Ok((snapshot, answer))
    }
}

/// One probe request under `PROBE_TIMEOUT`. A refusal reaps the child and is `HarnessError::Spawn`
/// for `initialize` (the CLI cannot be used at all), `HarnessError::Protocol` otherwise; an exit
/// before the answer is the handshake error; a timeout kills the child. Any other error (such as an
/// overlong stdout line) is returned as it is.
async fn probe_request(
    child: &mut dyn ClaudeChild,
    context: &ChildLogContext,
    body: &Value,
    early: &mut Vec<String>,
    result_errors: &mut Vec<String>,
) -> Result<Value, HarnessError> {
    let subtype = body.get("subtype").and_then(Value::as_str).unwrap_or("?");
    let reply = tokio::time::timeout(
        PROBE_TIMEOUT,
        request(child, &new_request_id(), body, early, result_errors),
    )
    .await;
    match reply {
        Ok(Ok(Ok(reply))) => Ok(reply),
        Ok(Ok(Err(message))) => {
            reap(child).await;
            warn!(
                project_id = display_opt(context.project_id),
                harness = display_opt(context.harness.as_deref()),
                action = "probe",
                subtype,
                error = %message,
                "Claude Code refused a probe request"
            );
            if subtype == "initialize" {
                Err(HarnessError::Spawn(format!(
                    "claude refused initialize: {message}"
                )))
            } else {
                Err(HarnessError::Protocol(message))
            }
        }
        Ok(Err(failure)) => Err(failure.into_error(context)),
        Err(_) => {
            child.start_kill();
            child.wait().await;
            warn!(
                project_id = display_opt(context.project_id),
                harness = display_opt(context.harness.as_deref()),
                action = "probe",
                subtype,
                timeout_ms = PROBE_TIMEOUT.as_millis() as u64,
                "a probe child did not answer in time; killed it"
            );
            Err(HarnessError::Timeout(format!(
                "claude did not answer {subtype} within {} s",
                PROBE_TIMEOUT.as_secs()
            )))
        }
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

/// What the trait methods need of a live child.
struct LiveChild {
    commands: mpsc::Sender<ChildCommand>,
    model: ModelRef,
    launch_mode: LaunchMode,
}

/// The CLI's name for a turn's permission mode (plan §8.2: Plan wins over the preset). `auto` and
/// `dontAsk` are never sent (plan §8.1).
pub(crate) fn turn_mode(overrides: &TurnOverrides) -> &'static str {
    if overrides.mode == Mode::Plan {
        return "plan";
    }
    match overrides.permission_preset {
        PermissionPreset::AskFirst => "default",
        PermissionPreset::AutoApprove => "acceptEdits",
        PermissionPreset::FullAccess => "bypassPermissions",
    }
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

/// `initialize`, then `set_permission_mode default`, then `get_settings`, then on resume
/// `get_context_usage`.
async fn handshake(
    child: &mut dyn ClaudeChild,
    context: &ChildLogContext,
    resume: bool,
    launch_mode: LaunchMode,
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

    // No turn runs before this: nothing reaches stdout before the first user message, and every
    // turn sets its own mode. A bypass-launched child that cannot leave bypass is never used.
    let mut permission_mode = reply.current_permission_mode;
    let set_default = json!({"subtype": "set_permission_mode", "mode": "default"});
    match launch_mode {
        LaunchMode::Bypass => {
            let answer = tokio::time::timeout(
                CONTROL_TIMEOUT,
                request(
                    child,
                    &new_request_id(),
                    &set_default,
                    &mut early,
                    &mut result_errors,
                ),
            )
            .await;
            let why = match answer {
                Ok(Ok(Ok(_))) => None,
                Ok(Ok(Err(message))) => Some(message),
                // The child is gone, so it cannot be used in bypass mode: report its exit like any
                // other handshake exit (stderr tail, unanswered stage, authentication and bypass
                // refusal classification).
                Ok(Err(failure @ HandshakeFailure::Exited { .. })) => {
                    debug!(
                        thread_id = display_opt(context.thread_id),
                        action = "set_permission_mode",
                        launch_mode = launch_mode.as_str(),
                        "a bypass-launched child exited before leaving bypassPermissions"
                    );
                    return Err(failure);
                }
                Ok(Err(HandshakeFailure::Error(error))) => Some(error.to_string()),
                Err(_) => Some(format!("no answer within {} s", CONTROL_TIMEOUT.as_secs())),
            };
            if let Some(why) = why {
                child.start_kill();
                child.wait().await;
                warn!(
                    thread_id = display_opt(context.thread_id),
                    harness_thread_id = display_opt(context.harness_thread_id.as_deref()),
                    action = "set_permission_mode",
                    mode = "default",
                    launch_mode = launch_mode.as_str(),
                    error = %why,
                    "a bypass-launched child did not leave bypassPermissions; killed it"
                );
                return Err(HandshakeFailure::Error(HarnessError::Spawn(format!(
                    "claude did not leave bypassPermissions: {why}"
                ))));
            }
            permission_mode = Some("default".into());
        }
        LaunchMode::Standard => {
            // `manual` already is `default`; the request makes it explicit and costs nothing.
            if optional_request(
                child,
                context,
                &set_default,
                &mut early,
                &mut result_errors,
                &mut abandoned,
            )
            .await?
            .is_some()
            {
                permission_mode = Some("default".into());
            }
        }
    }

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
        permission_mode,
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

    /// A live child's servers when one runs (what the user's session connected), else a probe's.
    async fn list_mcp_servers(&self) -> Result<Vec<McpServerStatus>, HarnessError> {
        self.ensure_running()?;
        let request = json!({"subtype": "mcp_status"});
        let live = lock(&self.children)
            .iter()
            .next()
            .map(|(thread, handle)| (*thread, handle.commands.clone()));
        if let Some((thread, commands)) = live {
            let payload = self
                .call(thread, commands, "mcp_status", CONTROL_TIMEOUT, |reply| {
                    ChildCommand::Control { request, reply }
                })
                .await
                .inspect_err(|error| {
                    warn!(
                        project_id = display_opt(self.launch.project_id),
                        harness = display_opt(self.launch.declaration.as_deref()),
                        thread_id = %thread,
                        action = "mcp_status",
                        error = %error,
                        "a live claude child did not report its MCP servers"
                    );
                })?;
            let servers = mcp_servers(&payload);
            info!(
                project_id = display_opt(self.launch.project_id),
                harness = display_opt(self.launch.declaration.as_deref()),
                thread_id = %thread,
                action = "mcp_status",
                servers = servers.len(),
                "read the MCP servers of a live claude child"
            );
            return Ok(servers);
        }
        let _probe = self.probe.lock().await;
        self.ensure_running()?;
        let started = Instant::now();
        let (_, payload) = self.probe(Some(&request)).await.inspect_err(|error| {
            warn!(
                project_id = display_opt(self.launch.project_id),
                harness = display_opt(self.launch.declaration.as_deref()),
                action = "mcp_probe",
                error = %error,
                "the MCP probe failed"
            );
        })?;
        let servers = mcp_servers(&payload.unwrap_or(Value::Null));
        info!(
            project_id = display_opt(self.launch.project_id),
            harness = display_opt(self.launch.declaration.as_deref()),
            action = "mcp_probe",
            elapsed_ms = started.elapsed().as_millis() as u64,
            servers = servers.len(),
            "read the Claude Code MCP servers from a probe child"
        );
        Ok(servers)
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

        let (child, handshake, launch_mode, warning) = match self
            .spawn_child_for(&opts, first)
            .await
        {
            Ok((child, handshake, launch_mode)) => (child, handshake, launch_mode, None),
            Err((
                HandshakeFailure::Exited {
                    exit,
                    result_errors,
                    ..
                },
                launch_mode,
            )) if resuming && classify_exit(&exit, &result_errors) == ExitKind::ResumeMissing => {
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
                // The same launch mode; a bypass refusal on the respawn still falls back.
                let (child, handshake, launch_mode) = if launch_mode == LaunchMode::Bypass {
                    self.spawn_child_for(&opts, SessionFlag::Fresh(session_id.clone()))
                        .await
                        .map_err(|(failure, _)| failure.into_error(&fresh_context))?
                } else {
                    self.spawn_and_handshake(
                        &opts,
                        SessionFlag::Fresh(session_id.clone()),
                        launch_mode,
                    )
                    .await
                    .map(|(child, handshake)| (child, handshake, launch_mode))
                    .map_err(|failure| failure.into_error(&fresh_context))?
                };
                let notice = HarnessNotice {
                    code: "claude_resume_failed".into(),
                    message: "Agent context was lost; started a fresh Claude Code session. \
                                  History is intact."
                        .into(),
                    detail,
                };
                (child, handshake, launch_mode, Some(notice))
            }
            Err((failure, _)) => return Err(failure.into_error(&context)),
        };

        if let Some(models) = handshake.models.as_deref() {
            self.store_catalog(models, "handshake");
        }
        let resumed_model = self.resumed_model(&opts, &session_id, handshake.applied.clone());
        let mut mapper =
            ClaudeMapper::new(opts.thread, session_id.clone(), opts.workspace_root.clone());
        // The handshake set `default`; a `status` it emitted (mapped from `early_lines`) is not
        // drift.
        mapper.set_expected_mode("default");
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
                            model: open_model.clone(),
                        });
                        slot.insert(ChildHandle {
                            harness_thread_id: session_id.clone(),
                            log,
                            commands,
                            task,
                            model: open_model,
                            launch_mode,
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
            launch_mode = launch_mode.as_str(),
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
        let Some(LiveChild { commands, .. }) = self.live(thread.thread) else {
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
        let LiveChild { commands, .. } = self
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
        let live = self
            .live(thread.thread)
            .ok_or(HarnessError::ThreadNotFound(thread.thread))?;
        let mode = turn_mode(&overrides);
        if mode == "bypassPermissions" && live.launch_mode == LaunchMode::Standard {
            // `bypassPermissions` is a launch-time capability: the CLI refuses to set it on a
            // child launched without it.
            let why = lock(&self.bypass_refused).clone().map_or_else(
                || "the child was not launched in that mode".to_owned(),
                |sentence| {
                    format!("Claude Code refused to start in bypassPermissions mode ({sentence})")
                },
            );
            warn!(
                thread_id = %thread.thread,
                harness_thread_id = %thread.harness_thread_id,
                action = "start_turn",
                mode,
                launch_mode = live.launch_mode.as_str(),
                "refusing a full_access turn on a child that cannot bypass permissions"
            );
            return Err(HarnessError::Unsupported(format!(
                "full_access is not available: {why}"
            )));
        }
        let model = overrides.model.clone().unwrap_or(live.model);
        let resolved_model = self
            .catalog_snapshot()
            .and_then(|snapshot| snapshot.resolved_model(&model.model).map(str::to_owned));
        let settings = TurnSettings {
            mode: mode.to_owned(),
            model: Some(model.model.clone()),
            effort: model
                .reasoning_effort
                .as_ref()
                .map(|effort| effort.0.clone()),
            resolved_model,
        };
        let UserInput::Text { text, attachments } = input;
        let line = user_message_line(&text, &attachments)?;
        let turn = TurnId::new();
        self.call(
            thread.thread,
            live.commands,
            "start_turn",
            START_TURN_TIMEOUT,
            |reply| ChildCommand::StartTurn {
                line,
                turn,
                model,
                settings,
                reply,
            },
        )
        .await?;
        Ok(turn)
    }

    async fn compact_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        let LiveChild { commands, .. } = self
            .live(thread.thread)
            .ok_or(HarnessError::ThreadNotFound(thread.thread))?;
        let turn = TurnId::new();
        self.call(
            thread.thread,
            commands,
            "compact",
            CONTROL_TIMEOUT,
            |reply| ChildCommand::Compact { turn, reply },
        )
        .await
    }

    async fn respond_approval(
        &self,
        req: ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<(), HarnessError> {
        let Some(ask) = lock(&self.pending).remove_approval(&req) else {
            warn!(
                request_id = %req,
                action = "respond_approval",
                "approval is not pending: answered, withdrawn by an interrupt, or its child is gone"
            );
            return Err(HarnessError::Protocol(format!(
                "approval {req} is not pending"
            )));
        };
        if let ApprovalDecision::AcceptWithExecPolicyAmendment { .. } = decision {
            // Not advertised (plan §9.3), so this is a client bug; the ask stays answerable.
            warn!(
                thread_id = %ask.thread,
                request_id = %req,
                action = "respond_approval",
                "exec-policy amendments are not offered for Claude Code approvals"
            );
            lock(&self.pending).insert_approval(req, ask);
            return Err(HarnessError::Unsupported(
                "Claude Code approvals offer no exec-policy amendment".into(),
            ));
        }
        let thread = ask.thread;
        let LiveChild { commands, .. } = self
            .live(thread)
            .ok_or(HarnessError::ThreadNotFound(thread))?;
        self.call(
            thread,
            commands,
            "respond_approval",
            CONTROL_TIMEOUT,
            |reply| ChildCommand::RespondApproval {
                id: req,
                ask,
                decision,
                reply,
            },
        )
        .await
    }

    async fn respond_server_request(
        &self,
        req: ServerRequestId,
        response: ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        let Some(ask) = lock(&self.pending).remove_server_request(&req) else {
            warn!(
                request_id = %req,
                action = "respond_server_request",
                "server request is not pending: answered, withdrawn, or its child is gone"
            );
            return Err(HarnessError::Protocol(format!(
                "server request {req} is not pending"
            )));
        };
        let thread = ask.thread;
        let LiveChild { commands, .. } = self
            .live(thread)
            .ok_or(HarnessError::ThreadNotFound(thread))?;
        self.call(
            thread,
            commands,
            "respond_server_request",
            CONTROL_TIMEOUT,
            |reply| ChildCommand::RespondServerRequest {
                id: req,
                ask,
                response,
                reply,
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use giskard_core::event::AgentEvent;
    use giskard_core::turn::{Mode, PermissionPreset, TurnStatusKind};
    use giskard_harness::{
        EnvOverlay, EventStreamError, ThreadUpdateStream, thread_update_channel,
    };

    use tracing_test::traced_test;

    use super::*;
    use crate::log_checks::{a_line_with, lines_with, no_line_with};
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
        assert_eq!(
            subtypes,
            ["initialize", "set_permission_mode", "get_settings"]
        );

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
    #[traced_test]
    async fn open_thread_reports_the_applied_model_when_it_differs() {
        let (resolved, _) = scripted(handshake_steps("claude-sonnet-5-5"), Vec::new());
        let (other, _) = scripted(handshake_steps("claude-opus-5-5"), Vec::new());
        let (harness, _) = harness(vec![resolved, other]);

        // The catalog's resolved id for the requested alias counts as applied.
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        assert_eq!(handle.resumed_model, Some(model("sonnet")));
        logs_assert(no_line_with("model_not_applied"));

        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        assert_eq!(handle.resumed_model, Some(model("claude-opus-5-5")));
        logs_assert(a_line_with(&[
            "action=\"model_not_applied\"",
            "requested=sonnet",
            "applied=claude-opus-5-5",
        ]));
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
    #[traced_test]
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
        for spawn in &spawns {
            assert!(
                spawn
                    .windows(2)
                    .any(|w| w == ["--permission-mode", "bypassPermissions"])
            );
        }
        assert!(!spawns[1].iter().any(|arg| arg == "--resume"));
        assert!(logs_contain("action=\"claude_resume_failed\""));
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
            [
                "initialize",
                "set_permission_mode",
                "get_settings",
                "get_context_usage"
            ]
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
    #[traced_test]
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
        logs_assert(a_line_with(&[
            "WARN",
            "claude child exited unexpectedly",
            "exit_code=3",
            "out of cheese",
            "live_children=0",
        ]));
    }

    #[tokio::test]
    #[traced_test]
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
        let thread = ThreadId::new();
        let (options, _updates) = open_options(thread, None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        lock(&harness.children).get(&thread).unwrap().log.close();
        harness
            .start_turn(&handle, text("go"), overrides())
            .await
            .unwrap();
        harness.shutdown().await.unwrap();
        logs_assert(lines_with(1, &["the thread's event log is closed"]));
        logs_assert(lines_with(1, &["action=\"child_exited\""]));
        logs_assert(lines_with(
            0,
            &["action=\"child_exited\"", "dropped_events=0"],
        ));
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
        assert_eq!(written(&record).len(), 4);
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
    #[traced_test]
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
        assert!(logs_contain("action=\"stop_interrupt\""));
        logs_assert(lines_with(2, &["action=\"thread_stopped\""]));
        logs_assert(no_line_with("stop_kill"));
    }

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn stop_kills_a_child_that_ignores_eof() {
        let (child, record) = scripted(handshake_steps("sonnet"), Vec::new());
        let (harness, _) = harness(vec![child.ignoring_eof()]);
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
        assert!(logs_contain("action=\"stop_kill\""));
        assert!(logs_contain("signal=9"));
    }

    #[tokio::test(start_paused = true)]
    #[traced_test]
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
        assert!(logs_contain(
            "aborted its supervisor and closed its event stream"
        ));
    }

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn a_start_turn_whose_caller_timed_out_is_never_written() {
        let (child, record) = scripted(handshake_steps("sonnet"), Vec::new());
        let (child, gate) = child.gated();
        let (harness, _) = harness(vec![child]);
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
        assert!(logs_contain(
            "the caller gave up on this turn before it was started"
        ));
        harness.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    #[traced_test]
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
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        assert_eq!(handle.resumed_model, None, "get_settings timed out");

        // The rename's write releases the late `get_settings` answer ahead of its own.
        harness.set_thread_name(&handle, "named").await.unwrap();
        assert!(logs_contain(
            "late answer to a handshake request that timed out"
        ));
        logs_assert(no_line_with(
            "control response for a request nobody is waiting on",
        ));
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn shutdown_stops_every_child_and_is_idempotent() {
        let (first, first_record) = scripted(handshake_steps("sonnet"), Vec::new());
        let (second, second_record) = scripted(handshake_steps("sonnet"), Vec::new());
        let (harness, _) = harness(vec![first, second]);
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
        assert!(logs_contain("children_stopped=2"));
        assert!(logs_contain("children_stopped=0"));
    }

    // ---- asks ----------------------------------------------------------------------------------

    // ---- milestone 3: launch mode and handshake -------------------------------------------------

    const ROOT_REFUSAL: &str = "--dangerously-skip-permissions cannot be used with root/sudo \
                                privileges for security reasons";

    fn root_refused_child() -> (ScriptedChild, Arc<Mutex<ScriptRecord>>) {
        ScriptedChild::new(vec![Step::OnStdin(
            control("initialize"),
            vec![Action::Exit {
                code: 1,
                stderr: vec![ROOT_REFUSAL.into()],
            }],
        )])
    }

    fn launch_mode_of(argv: &[String]) -> &str {
        let at = argv
            .iter()
            .position(|arg| arg == "--permission-mode")
            .unwrap();
        &argv[at + 1]
    }

    fn turn_overrides(
        preset: PermissionPreset,
        mode: Mode,
        model: Option<ModelRef>,
    ) -> TurnOverrides {
        TurnOverrides {
            model,
            mode,
            permission_preset: preset,
        }
    }

    fn with_effort(name: &str, effort: &str) -> ModelRef {
        ModelRef {
            reasoning_effort: Some(Effort(effort.into())),
            ..model(name)
        }
    }

    /// The modes of every `set_permission_mode` written, in order.
    fn modes_written(record: &Arc<Mutex<ScriptRecord>>) -> Vec<String> {
        written(record)
            .iter()
            .filter(|line| line["request"]["subtype"] == "set_permission_mode")
            .map(|line| line["request"]["mode"].as_str().unwrap().to_owned())
            .collect()
    }

    fn subtypes_written(record: &Arc<Mutex<ScriptRecord>>) -> Vec<String> {
        written(record)
            .iter()
            .filter_map(|line| line["request"]["subtype"].as_str().map(str::to_owned))
            .collect()
    }

    /// The control responses the adapter wrote, in order.
    fn answers_written(record: &Arc<Mutex<ScriptRecord>>) -> Vec<Value> {
        written(record)
            .into_iter()
            .filter(|line| line["type"] == "control_response")
            .collect()
    }

    /// The text-turn frames with the `init` frame's `permissionMode` rewritten.
    fn text_turn_reporting_mode(mode: &str) -> Vec<String> {
        fixture_lines("text-turn")
            .into_iter()
            .map(|line| {
                let mut frame: Value = serde_json::from_str(&line).unwrap();
                if frame["type"] == "system" && frame["subtype"] == "init" {
                    frame["permissionMode"] = json!(mode);
                    return frame.to_string();
                }
                line
            })
            .collect()
    }

    fn text_turn() -> Action {
        Action::EmitFixture {
            name: "text-turn",
            skip_types: &[],
        }
    }

    /// The next event matching `select`, bounded so a broken test fails instead of hangs.
    async fn next_matching<T>(
        stream: &mut AgentEventStream,
        mut select: impl FnMut(&AgentEvent) -> Option<T>,
    ) -> T {
        loop {
            let event = tokio::time::timeout(Duration::from_secs(10), stream.recv())
                .await
                .expect("timed out waiting for an event")
                .expect("stream ended");
            if let Some(found) = select(&event) {
                return found;
            }
        }
    }

    #[tokio::test]
    async fn a_bypass_launch_sets_default_in_the_handshake() {
        let (child, record) = scripted(handshake_steps("sonnet"), Vec::new());
        let (harness, spawner) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        harness.open_thread(options).await.unwrap();
        assert_eq!(launch_mode_of(&spawner.spawns()[0]), "bypassPermissions");
        assert_eq!(
            subtypes_written(&record),
            ["initialize", "set_permission_mode", "get_settings"]
        );
        assert_eq!(modes_written(&record), ["default"]);
        harness.shutdown().await.unwrap();

        // The handshake reports `default` though the CLI said it was launched in bypass.
        let mut payload = initialize_payload();
        payload["current_permission_mode"] = json!("bypassPermissions");
        let (mut child, _) = ScriptedChild::new(vec![
            Step::OnStdin(control("initialize"), vec![Action::Respond(payload)]),
            Step::OnStdin(
                control("get_settings"),
                vec![Action::Respond(settings("sonnet"))],
            ),
        ]);
        let shaken = handshake(
            &mut child,
            &ChildLogContext::default(),
            false,
            LaunchMode::Bypass,
        )
        .await
        .ok()
        .unwrap();
        assert_eq!(shaken.permission_mode.as_deref(), Some("default"));

        // A standard launch reports `default` too, once its explicit request succeeded.
        let (mut child, record) = ScriptedChild::new(vec![
            Step::OnStdin(
                control("initialize"),
                vec![Action::Respond(initialize_payload())],
            ),
            Step::OnStdin(
                control("get_settings"),
                vec![Action::Respond(settings("sonnet"))],
            ),
        ]);
        let shaken = handshake(
            &mut child,
            &ChildLogContext::default(),
            false,
            LaunchMode::Standard,
        )
        .await
        .ok()
        .unwrap();
        assert_eq!(shaken.permission_mode.as_deref(), Some("default"));
        assert_eq!(modes_written(&record), ["default"]);
    }

    #[tokio::test]
    #[traced_test]
    async fn a_refused_bypass_launch_falls_back_to_a_standard_launch() {
        let (refused, _) = root_refused_child();
        let (first, first_record) = scripted(handshake_steps("sonnet"), Vec::new());
        let (second, _) = scripted(handshake_steps("sonnet"), Vec::new());
        let (harness, spawner) = harness(vec![refused, first, second]);
        for _ in 0..2 {
            let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
            harness.open_thread(options).await.unwrap();
        }
        let spawns = spawner.spawns();
        let modes: Vec<&str> = spawns.iter().map(|argv| launch_mode_of(argv)).collect();
        assert_eq!(modes, ["bypassPermissions", "manual", "manual"]);
        // The same session flag on the fallback.
        let session = |argv: &[String]| {
            let at = argv.iter().position(|arg| arg == "--session-id").unwrap();
            argv[at + 1].clone()
        };
        assert_eq!(session(&spawns[0]), session(&spawns[1]));
        assert_eq!(lock(&harness.bypass_refused).as_deref(), Some(ROOT_REFUSAL));
        // A standard child still sets `default` explicitly.
        assert_eq!(modes_written(&first_record), ["default"]);
        logs_assert(lines_with(1, &["WARN", "action=\"bypass_refused\""]));
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_handshake_that_cannot_leave_bypass_fails_the_open() {
        let (child, record) = ScriptedChild::new(vec![
            Step::OnStdin(
                control("initialize"),
                vec![Action::Respond(initialize_payload())],
            ),
            Step::OnStdin(
                control("set_permission_mode"),
                vec![Action::RespondError("Cannot set permission mode")],
            ),
        ]);
        let (harness, _) = harness(vec![child.without_mode_echo()]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let error = harness.open_thread(options).await.unwrap_err();
        assert!(
            matches!(&error, HarnessError::Spawn(message)
                if message == "claude did not leave bypassPermissions: Cannot set permission mode"),
            "{error}"
        );
        assert!(lock(&record).killed);
        assert_eq!(harness.live_children(), 0);
    }

    #[tokio::test]
    async fn a_resume_missing_fallback_keeps_the_launch_mode() {
        // Bypass refused, then the transcript is missing: the fresh respawn stays standard.
        let (refused, _) = root_refused_child();
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
        let (fresh, _) = scripted(handshake_steps("sonnet"), Vec::new());
        let (harness, spawner) = harness(vec![refused, missing, fresh]);
        let (options, _updates) = open_options(ThreadId::new(), Some(RESUME_ID), "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        assert_eq!(handle.warning.unwrap().code, "claude_resume_failed");
        let spawns = spawner.spawns();
        let modes: Vec<&str> = spawns.iter().map(|argv| launch_mode_of(argv)).collect();
        assert_eq!(modes, ["bypassPermissions", "manual", "manual"]);
        assert!(spawns[0].windows(2).any(|w| w == ["--resume", RESUME_ID]));
        assert!(spawns[1].windows(2).any(|w| w == ["--resume", RESUME_ID]));
        assert!(
            spawns[2]
                .windows(2)
                .any(|w| w == ["--session-id", RESUME_ID])
        );
        harness.shutdown().await.unwrap();
    }

    // ---- milestone 3: per-turn settings ---------------------------------------------------------

    #[tokio::test]
    async fn every_turn_sets_its_permission_mode() {
        let turns = vec![
            turn_overrides(PermissionPreset::AskFirst, Mode::Build, None),
            turn_overrides(PermissionPreset::AskFirst, Mode::Build, None),
            turn_overrides(PermissionPreset::AutoApprove, Mode::Build, None),
            turn_overrides(PermissionPreset::FullAccess, Mode::Plan, None),
            turn_overrides(PermissionPreset::AskFirst, Mode::Plan, None),
        ];
        let steps = turns
            .iter()
            .map(|_| Step::OnStdin(user(), vec![text_turn()]))
            .collect();
        let (child, record) = scripted(handshake_steps("sonnet"), steps);
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        for overrides in turns {
            harness
                .start_turn(&handle, text("go"), overrides)
                .await
                .unwrap();
            until_completed(&mut stream).await;
        }
        assert_eq!(
            modes_written(&record),
            [
                "default",
                "default",
                "default",
                "acceptEdits",
                "plan",
                "plan"
            ]
        );
        // Each mode is set before its turn's message.
        let lines = written(&record);
        for (index, line) in lines.iter().enumerate() {
            if line["type"] == "user" {
                assert_eq!(
                    lines[index - 1]["request"]["subtype"],
                    "set_permission_mode"
                );
            }
        }
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn full_access_needs_a_bypass_launch() {
        let full_access = turn_overrides(PermissionPreset::FullAccess, Mode::Build, None);

        // A standard child: refused with the CLI's sentence, nothing written.
        let (refused, _) = root_refused_child();
        let (standard, standard_record) = scripted(handshake_steps("sonnet"), Vec::new());
        let (harness_standard, _) = harness(vec![refused, standard]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness_standard.open_thread(options).await.unwrap();
        let mut stream = harness_standard.subscribe(&handle);
        let before = lock(&standard_record).written.len();
        let error = harness_standard
            .start_turn(&handle, text("go"), full_access.clone())
            .await
            .unwrap_err();
        assert!(
            matches!(&error, HarnessError::Unsupported(message)
                if message.starts_with("full_access is not available: Claude Code refused to \
                                        start in bypassPermissions mode")
                    && message.contains(ROOT_REFUSAL)),
            "{error}"
        );
        assert_eq!(lock(&standard_record).written.len(), before);
        assert!(stream.try_recv().is_none());
        harness_standard.shutdown().await.unwrap();

        // A bypass child: the mode is set and the turn runs.
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(user(), vec![text_turn()])],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        harness
            .start_turn(&handle, text("go"), full_access)
            .await
            .unwrap();
        until_completed(&mut stream).await;
        assert_eq!(modes_written(&record), ["default", "bypassPermissions"]);
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn a_refused_mode_fails_the_turn_start() {
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                control("set_permission_mode"),
                vec![
                    Action::RespondErrorCode {
                        error: "Cannot set permission mode to bypassPermissions because the \
                                session was not launched with --dangerously-skip-permissions",
                        code: "bypass_not_launched",
                    },
                    // The CLI still holds `default`; reporting it is not drift.
                    Action::Emit(vec![
                        r#"{"type":"system","subtype":"status","status":null,"permissionMode":"default","session_id":"s"}"#.into(),
                    ]),
                ],
            )],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        let error = harness
            .start_turn(
                &handle,
                text("go"),
                turn_overrides(PermissionPreset::FullAccess, Mode::Build, None),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&error, HarnessError::Protocol(message) if message.contains("not launched")),
            "{error}"
        );
        assert!(stream.try_recv().is_none(), "no TurnStarted");
        assert!(written(&record).iter().all(|line| line["type"] != "user"));
        // Shutdown reads stdout to EOF, so the status frame has been mapped once it returns.
        harness.shutdown().await.unwrap();
        logs_assert(a_line_with(&[
            "WARN",
            "action=\"set_permission_mode\"",
            "error_code=bypass_not_launched",
            "mode=bypassPermissions",
        ]));
        logs_assert(no_line_with("permission_mode_drift"));
        assert!(
            !until_closed(&mut stream)
                .await
                .iter()
                .any(|event| matches!(event, AgentEvent::Notice { .. })),
            "no drift notice"
        );
    }

    #[tokio::test]
    #[traced_test]
    async fn the_mode_status_after_a_set_is_not_drift() {
        let status = r#"{"type":"system","subtype":"status","status":null,"permissionMode":"acceptEdits","session_id":"f18693ff-2d11-4f87-9556-2b527e19e081"}"#;
        let (child, _) = scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(
                    control("set_permission_mode"),
                    vec![
                        Action::Respond(json!({"mode": "acceptEdits"})),
                        Action::Emit(vec![status.into()]),
                    ],
                ),
                Step::OnStdin(
                    user(),
                    vec![Action::Emit(text_turn_reporting_mode("acceptEdits"))],
                ),
            ],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        harness
            .start_turn(
                &handle,
                text("go"),
                turn_overrides(PermissionPreset::AutoApprove, Mode::Build, None),
            )
            .await
            .unwrap();
        let events = until_completed(&mut stream).await;
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, AgentEvent::Notice { .. }))
        );
        logs_assert(no_line_with("permission_mode_drift"));
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn a_mode_the_adapter_did_not_set_is_drift_on_init_too() {
        let (child, _) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                user(),
                vec![Action::Emit(text_turn_reporting_mode("plan"))],
            )],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        harness
            .start_turn(&handle, text("go"), overrides())
            .await
            .unwrap();
        let events = until_completed(&mut stream).await;
        let notices: Vec<&str> = events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::Notice { message, .. } => Some(message.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            notices,
            ["Claude Code switched its permission mode to plan; Giskard set default"]
        );
        assert!(logs_contain("action=\"permission_mode_drift\""));
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn a_model_change_is_sent_and_read_back() {
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(control("set_model"), vec![Action::Respond(Value::Null)]),
                // The CLI answers with the catalog's resolved id for `opus`.
                Step::OnStdin(
                    control("get_settings"),
                    vec![Action::Respond(settings("claude-opus-5-5"))],
                ),
                Step::OnStdin(user(), vec![text_turn()]),
                Step::OnStdin(user(), vec![text_turn()]),
            ],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        let opus = turn_overrides(PermissionPreset::AskFirst, Mode::Build, Some(model("opus")));
        harness
            .start_turn(&handle, text("one"), opus.clone())
            .await
            .unwrap();
        let events = until_completed(&mut stream).await;
        let usage_models: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::TurnUsageUpdated { model, .. } => Some(model.clone()),
                _ => None,
            })
            .collect();
        assert!(!usage_models.is_empty());
        assert!(usage_models.iter().all(|m| *m == Some(model("opus"))));
        let set_model = written(&record)
            .into_iter()
            .find(|line| line["request"]["subtype"] == "set_model")
            .unwrap();
        assert_eq!(set_model["request"]["model"], "opus");
        logs_assert(a_line_with(&[
            "action=\"turn_settings\"",
            "model=opus",
            "mode=default",
        ]));

        // The same model on the next turn sends nothing but the mode.
        let before = subtypes_written(&record).len();
        harness
            .start_turn(&handle, text("two"), opus)
            .await
            .unwrap();
        until_completed(&mut stream).await;
        assert_eq!(
            &subtypes_written(&record)[before..],
            ["set_permission_mode"]
        );
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn an_unknown_model_fails_the_turn_start() {
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                control("set_model"),
                vec![Action::RespondErrorCode {
                    error: "Model 'gpt-5' not found",
                    code: "catalog_unknown",
                }],
            )],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        let error = harness
            .start_turn(
                &handle,
                text("go"),
                turn_overrides(
                    PermissionPreset::AskFirst,
                    Mode::Build,
                    Some(model("gpt-5")),
                ),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&error, HarnessError::Unsupported(message) if message == "Model 'gpt-5' not found"),
            "{error}"
        );
        assert!(stream.try_recv().is_none(), "no TurnStarted");
        let subtypes = subtypes_written(&record);
        assert_eq!(subtypes.last().map(String::as_str), Some("set_model"));
        assert!(written(&record).iter().all(|line| line["type"] != "user"));
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn an_effort_change_is_sent_and_read_back() {
        let applied = json!({"applied": {"model": "sonnet", "effort": "high"}, "effective": {}});
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(
                    control("apply_flag_settings"),
                    vec![Action::Respond(Value::Null)],
                ),
                Step::OnStdin(control("get_settings"), vec![Action::Respond(applied)]),
                Step::OnStdin(user(), vec![text_turn()]),
                Step::OnStdin(user(), vec![text_turn()]),
            ],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        let high = turn_overrides(
            PermissionPreset::AskFirst,
            Mode::Build,
            Some(with_effort("sonnet", "high")),
        );
        harness
            .start_turn(&handle, text("one"), high.clone())
            .await
            .unwrap();
        until_completed(&mut stream).await;
        let apply = written(&record)
            .into_iter()
            .find(|line| line["request"]["subtype"] == "apply_flag_settings")
            .unwrap();
        assert_eq!(apply["request"]["settings"], json!({"effortLevel": "high"}));
        assert!(
            !subtypes_written(&record).contains(&"set_model".to_owned()),
            "same model: no set_model"
        );

        // The CLI now holds `high`: nothing more is sent.
        let before = subtypes_written(&record).len();
        harness
            .start_turn(&handle, text("two"), high)
            .await
            .unwrap();
        until_completed(&mut stream).await;
        assert_eq!(
            &subtypes_written(&record)[before..],
            ["set_permission_mode"]
        );
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_refused_effort_fails_the_turn_start() {
        // An invalid level leaves the previous one in `applied.effort`.
        let (child, _) = scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(
                    control("apply_flag_settings"),
                    vec![Action::Respond(Value::Null)],
                ),
                Step::OnStdin(
                    control("get_settings"),
                    vec![Action::Respond(settings("sonnet"))],
                ),
            ],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        let error = harness
            .start_turn(
                &handle,
                text("go"),
                turn_overrides(
                    PermissionPreset::AskFirst,
                    Mode::Build,
                    Some(with_effort("sonnet", "banana")),
                ),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&error, HarnessError::Unsupported(message)
                if message == "Claude Code did not accept effort banana for sonnet"),
            "{error}"
        );
        assert!(stream.try_recv().is_none(), "no TurnStarted");
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_read_back_mismatch_is_a_protocol_error() {
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(control("set_model"), vec![Action::Respond(Value::Null)]),
                Step::OnStdin(
                    control("get_settings"),
                    vec![Action::Respond(settings("claude-fable-5-1"))],
                ),
            ],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        let error = harness
            .start_turn(
                &handle,
                text("go"),
                turn_overrides(PermissionPreset::AskFirst, Mode::Build, Some(model("opus"))),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&error, HarnessError::Protocol(message)
                if message == "Claude Code applied model claude-fable-5-1 instead of opus"),
            "{error}"
        );
        assert!(stream.try_recv().is_none(), "no TurnStarted");
        assert!(written(&record).iter().all(|line| line["type"] != "user"));
        harness.shutdown().await.unwrap();
    }

    // ---- milestone 3: approvals -----------------------------------------------------------------

    /// The index of a fixture's `can_use_tool` ask among `fixture_lines`, and its request id.
    fn ask_of(name: &str) -> (usize, String) {
        let lines = fixture_lines(name);
        let index = lines
            .iter()
            .position(|line| line.contains("\"control_request\""))
            .unwrap();
        let value: Value = serde_json::from_str(&lines[index]).unwrap();
        (index, value["request_id"].as_str().unwrap().to_owned())
    }

    /// A control response the adapter writes; the rest of the fixture follows it.
    fn answered() -> crate::session::tests::Matcher {
        Box::new(|value| value["type"] == "control_response")
    }

    /// A child that emits `name` up to its ask on the user message, and the rest once answered.
    fn asking(name: &'static str) -> (ScriptedChild, Arc<Mutex<ScriptRecord>>) {
        let (index, _) = ask_of(name);
        scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(
                    user(),
                    vec![Action::EmitFixturePrefix {
                        name,
                        count: index + 1,
                    }],
                ),
                Step::OnStdin(
                    answered(),
                    vec![Action::EmitFixtureFrom {
                        name,
                        from: index + 1,
                    }],
                ),
            ],
        )
    }

    async fn approval_requested(stream: &mut AgentEventStream) -> ApprovalId {
        next_matching(stream, |event| match event {
            AgentEvent::ApprovalRequested { request, .. } => Some(request.id.clone()),
            _ => None,
        })
        .await
    }

    /// Open a thread on `child`, start a turn, and wait for its ask.
    async fn ask_pending(
        child: ScriptedChild,
    ) -> (
        Arc<ClaudeHarness>,
        ThreadHandle,
        AgentEventStream,
        ApprovalId,
    ) {
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        harness
            .start_turn(&handle, text("touch probe.txt"), overrides())
            .await
            .unwrap();
        let approval = approval_requested(&mut stream).await;
        (harness, handle, stream, approval)
    }

    fn command_status(events: &[AgentEvent]) -> Option<String> {
        events.iter().find_map(|event| match event {
            AgentEvent::ItemCompleted { item, .. } => match &item.payload {
                giskard_core::item::ItemPayload::CommandExecution { status, .. } => status.clone(),
                _ => None,
            },
            _ => None,
        })
    }

    #[tokio::test]
    #[traced_test]
    async fn accept_writes_a_bare_allow() {
        let (child, record) = asking("tool-allowed");
        let (harness, _, mut stream, approval) = ask_pending(child).await;
        let (_, request_id) = ask_of("tool-allowed");
        assert_eq!(approval.0, request_id);
        harness
            .respond_approval(approval.clone(), ApprovalDecision::Accept)
            .await
            .unwrap();
        let events = until_completed(&mut stream).await;
        assert_eq!(completion(&events).1, TurnStatusKind::Completed);
        assert_eq!(command_status(&events).as_deref(), Some("completed"));
        assert_eq!(
            answers_written(&record),
            [json!({"type": "control_response", "response": {
                "subtype": "success",
                "request_id": request_id,
                "response": {"behavior": "allow"},
            }})]
        );
        assert!(lock(&harness.pending).approval(&approval).is_none());
        logs_assert(a_line_with(&[
            "action=\"respond_approval\"",
            "decision=\"accept\"",
            "tool_name=\"Bash\"",
        ]));
        logs_assert(no_line_with("touch probe.txt"));
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn accept_for_session_echoes_the_rule_with_the_session_destination() {
        let (child, record) = asking("accept-for-session");
        let (harness, _, mut stream, approval) = ask_pending(child).await;
        harness
            .respond_approval(approval, ApprovalDecision::AcceptForSession)
            .await
            .unwrap();
        let events = until_completed(&mut stream).await;
        assert_eq!(completion(&events).1, TurnStatusKind::Completed);

        let recorded = std::fs::read_to_string(format!(
            "{}/tests/fixtures/accept-for-session.in.jsonl",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        let recorded: Value = recorded
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .find(|line| line["type"] == "control_response")
            .unwrap();
        let answers = answers_written(&record);
        assert_eq!(answers.len(), 1);
        assert_eq!(
            answers[0]["response"]["response"],
            recorded["response"]["response"]
        );
        let rule = &answers[0]["response"]["response"]["updatedPermissions"][0];
        assert_eq!(rule["destination"], "session");
        assert_eq!(rule["rules"][0]["ruleContent"], "python3 -c \"print(1)\"");
        let line = lock(&record)
            .written
            .iter()
            .find(|line| line.contains("control_response"))
            .unwrap()
            .clone();
        assert!(!line.contains("localSettings"), "{line}");
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn accept_for_session_without_a_rule_suggestion_degrades_to_accept() {
        let (index, request_id) = ask_of("tool-allowed");
        let lines = fixture_lines("tool-allowed");
        let mut ask: Value = serde_json::from_str(&lines[index]).unwrap();
        ask["request"]["permission_suggestions"] = json!([
            {"type": "addDirectories", "directories": ["/work/project"], "destination": "session"}
        ]);
        let mut prefix = lines[..index].to_vec();
        prefix.push(ask.to_string());
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(user(), vec![Action::Emit(prefix)]),
                Step::OnStdin(
                    answered(),
                    vec![Action::EmitFixtureFrom {
                        name: "tool-allowed",
                        from: index + 1,
                    }],
                ),
            ],
        );
        let (harness, _, mut stream, approval) = ask_pending(child).await;
        harness
            .respond_approval(approval, ApprovalDecision::AcceptForSession)
            .await
            .unwrap();
        until_completed(&mut stream).await;
        assert_eq!(
            answers_written(&record)[0]["response"],
            json!({"subtype": "success", "request_id": request_id, "response": {"behavior": "allow"}})
        );
        logs_assert(a_line_with(&[
            "WARN",
            "action=\"accept_for_session_degraded\"",
            "suggestions=1",
        ]));
        harness.shutdown().await.unwrap();
    }

    /// A fixture's lines with `key` removed from every frame of `frame_type`.
    fn fixture_without(name: &str, frame_type: &str, key: &str) -> Vec<String> {
        fixture_lines(name)
            .into_iter()
            .map(|line| {
                let mut frame: Value = serde_json::from_str(&line).unwrap();
                if frame["type"] == frame_type
                    && let Some(object) = frame.as_object_mut()
                {
                    object.remove(key);
                    return frame.to_string();
                }
                line
            })
            .collect()
    }

    /// Like `asking`, from rewritten lines.
    fn asking_lines(
        name: &'static str,
        lines: Vec<String>,
    ) -> (ScriptedChild, Arc<Mutex<ScriptRecord>>) {
        let (index, _) = ask_of(name);
        scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(user(), vec![Action::Emit(lines[..=index].to_vec())]),
                Step::OnStdin(answered(), vec![Action::Emit(lines[index + 1..].to_vec())]),
            ],
        )
    }

    #[tokio::test]
    async fn decline_blocks_the_tool_and_the_turn_completes() {
        // Without `tool_result_meta`, only the adapter's own denial note makes the item
        // `declined` (the result alone reads as a failed tool).
        let (child, record) = asking_lines(
            "tool-denied",
            fixture_without("tool-denied", "user", "tool_result_meta"),
        );
        let (harness, _, mut stream, approval) = ask_pending(child).await;
        harness
            .respond_approval(approval, ApprovalDecision::Decline)
            .await
            .unwrap();
        let events = until_completed(&mut stream).await;
        assert_eq!(completion(&events).1, TurnStatusKind::Completed);
        assert_eq!(command_status(&events).as_deref(), Some("declined"));
        assert_eq!(
            answers_written(&record)[0]["response"]["response"],
            json!({"behavior": "deny", "message": "Declined by the user in Giskard"})
        );
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn cancel_interrupts_and_the_turn_is_interrupted() {
        // Without `terminal_reason`, only the adapter's interrupt note makes the error result an
        // interruption; the exit 1 after it must then read as expected.
        let (child, record) = asking_lines(
            "cancel",
            fixture_without("cancel", "result", "terminal_reason"),
        );
        let (harness, handle, mut stream, approval) =
            ask_pending(child.exiting_on_eof_with(1)).await;
        harness
            .respond_approval(approval, ApprovalDecision::Cancel)
            .await
            .unwrap();
        let events = until_completed(&mut stream).await;
        assert_eq!(completion(&events).1, TurnStatusKind::Interrupted);
        assert_eq!(
            answers_written(&record)[0]["response"]["response"],
            json!({
                "behavior": "deny",
                "message": "Cancelled by the user in Giskard",
                "interrupt": true,
            })
        );
        harness.delete_thread(&handle).await.unwrap();
        logs_assert(a_line_with(&[
            "INFO",
            "action=\"child_exited\"",
            "exit_code=1",
        ]));
    }

    #[tokio::test]
    async fn an_unknown_or_already_answered_approval_is_a_protocol_error() {
        let (child, _) = asking("tool-allowed");
        let (harness, _, mut stream, approval) = ask_pending(child).await;
        assert!(matches!(
            harness
                .respond_approval(ApprovalId::new("nope"), ApprovalDecision::Accept)
                .await,
            Err(HarnessError::Protocol(message)) if message == "approval nope is not pending"
        ));
        harness
            .respond_approval(approval.clone(), ApprovalDecision::Accept)
            .await
            .unwrap();
        until_completed(&mut stream).await;
        assert!(matches!(
            harness
                .respond_approval(approval, ApprovalDecision::Accept)
                .await,
            Err(HarnessError::Protocol(message)) if message.contains("is not pending")
        ));
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn exec_policy_amendments_are_unsupported() {
        let (child, record) = asking("tool-allowed");
        let (harness, handle, _stream, approval) = ask_pending(child).await;
        assert!(matches!(
            harness
                .respond_approval(
                    approval.clone(),
                    ApprovalDecision::AcceptWithExecPolicyAmendment {
                        amendment: vec!["touch".into()],
                    },
                )
                .await,
            Err(HarnessError::Unsupported(_))
        ));
        assert!(answers_written(&record).is_empty());
        assert!(
            lock(&harness.pending).approval(&approval).is_some(),
            "the ask stays answerable"
        );
        harness.delete_thread(&handle).await.unwrap();
        assert_eq!(lock(&harness.pending).len(), 0);
    }

    #[tokio::test]
    #[traced_test]
    async fn a_cancelled_ask_is_dropped_and_a_late_answer_is_refused() {
        let (index, ask_id) = ask_of("cancel");
        let dialog = json!({"type": "control_request", "request_id": "dialog-1", "request": {
            "subtype": "request_user_dialog", "title": "t"
        }})
        .to_string();
        let cancel =
            |id: &str| json!({"type": "control_cancel_request", "request_id": id}).to_string();
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(
                    user(),
                    vec![
                        Action::EmitFixturePrefix {
                            name: "cancel",
                            count: index + 1,
                        },
                        Action::Emit(vec![dialog]),
                    ],
                ),
                // The CLI withdraws its asks before it answers the interrupt.
                Step::OnStdin(
                    control("interrupt"),
                    vec![
                        Action::Emit(vec![cancel(&ask_id), cancel("dialog-1")]),
                        Action::Respond(json!({"still_queued": []})),
                        Action::EmitFixtureFrom {
                            name: "cancel",
                            from: index + 1,
                        },
                    ],
                ),
            ],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        harness
            .start_turn(&handle, text("touch probe.txt"), overrides())
            .await
            .unwrap();
        let approval = approval_requested(&mut stream).await;
        next_matching(&mut stream, |event| {
            matches!(event, AgentEvent::ServerRequestReceived { .. }).then_some(())
        })
        .await;
        harness.interrupt(&handle).await.unwrap();
        let events = until_completed(&mut stream).await;
        assert!(events.iter().any(|event| matches!(
            event,
            AgentEvent::ServerRequestResolved { request_id, .. } if request_id.0 == "dialog-1"
        )));
        assert_eq!(completion(&events).1, TurnStatusKind::Interrupted);
        assert!(matches!(
            harness
                .respond_approval(approval, ApprovalDecision::Accept)
                .await,
            Err(HarnessError::Protocol(_))
        ));
        assert!(matches!(
            harness
                .respond_server_request(
                    ServerRequestId::new("dialog-1"),
                    ServerRequestResponse::result(json!({}))
                )
                .await,
            Err(HarnessError::Protocol(_))
        ));
        assert!(answers_written(&record).is_empty(), "nothing late is sent");
        assert_eq!(lock(&harness.pending).len(), 0);
        // The mapper's and the supervisor's line per withdrawn ask.
        logs_assert(lines_with(4, &["action=\"control_cancel_request\""]));
        harness.shutdown().await.unwrap();
    }

    // ---- milestone 3: server requests -----------------------------------------------------------

    fn ask_user_question(request_id: &str) -> (String, Value) {
        let input = json!({"questions": [
            {
                "question": "Do you prefer cats or dogs?",
                "header": "Pet",
                "options": [
                    {"label": "Cats", "description": "Independent"},
                    {"label": "Dogs", "description": "Loyal"},
                ],
                "multiSelect": false,
            },
            {
                "question": "Which fruits do you like?",
                "header": "Fruit",
                "options": [
                    {"label": "Apple", "description": "Crisp"},
                    {"label": "Cherry", "description": "Sweet"},
                ],
                "multiSelect": true,
            },
        ]});
        let line = json!({"type": "control_request", "request_id": request_id, "request": {
            "subtype": "can_use_tool",
            "tool_name": "AskUserQuestion",
            "display_name": "AskUserQuestion",
            "input": input,
            "requires_user_interaction": true,
            "tool_use_id": format!("toolu_{request_id}"),
        }})
        .to_string();
        (line, input)
    }

    async fn server_request_received(
        stream: &mut AgentEventStream,
    ) -> (ServerRequestId, String, Value) {
        next_matching(stream, |event| match event {
            AgentEvent::ServerRequestReceived { request, .. } => Some((
                request.id.clone(),
                request.method.clone(),
                request.params.clone(),
            )),
            _ => None,
        })
        .await
    }

    async fn server_request_resolved(stream: &mut AgentEventStream) -> ServerRequestId {
        next_matching(stream, |event| match event {
            AgentEvent::ServerRequestResolved { request_id, .. } => Some(request_id.clone()),
            _ => None,
        })
        .await
    }

    #[tokio::test]
    #[traced_test]
    async fn ask_user_question_round_trips() {
        let (first, input) = ask_user_question("q1");
        let (second, _) = ask_user_question("q2");
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(user(), vec![Action::Emit(vec![first])]),
                Step::OnStdin(answered(), vec![Action::Emit(vec![second])]),
                Step::OnStdin(answered(), vec![text_turn()]),
            ],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        harness
            .start_turn(&handle, text("ask me"), overrides())
            .await
            .unwrap();

        let (id, method, params) = server_request_received(&mut stream).await;
        assert_eq!(id.0, "q1");
        assert_eq!(method, "claude/ask_user_question");
        assert_eq!(params["questions"][0]["id"], "0");
        assert_eq!(params["questions"][1]["id"], "1");
        assert_eq!(
            params["questions"][0]["question"],
            "Do you prefer cats or dogs?"
        );
        harness
            .respond_server_request(
                id.clone(),
                ServerRequestResponse::result(json!({"answers": {
                    "0": {"answers": ["Cats"]},
                    "1": {"answers": ["Apple", "Cherry"]},
                }})),
            )
            .await
            .unwrap();
        assert_eq!(server_request_resolved(&mut stream).await, id);
        let answer = &answers_written(&record)[0];
        assert_eq!(answer["response"]["request_id"], "q1");
        assert_eq!(
            answer["response"]["response"],
            json!({"behavior": "allow", "updatedInput": {
                "questions": input["questions"],
                "answers": {
                    "Do you prefer cats or dogs?": "Cats",
                    "Which fruits do you like?": "Apple, Cherry",
                },
            }})
        );

        // A browser cancel is a deny with its message; an unanswered question is left out.
        let (id, _, _) = server_request_received(&mut stream).await;
        assert!(matches!(
            harness
                .respond_server_request(id.clone(), ServerRequestResponse::result(json!("x")))
                .await,
            Err(HarnessError::Protocol(_))
        ));
        assert!(
            lock(&harness.pending).server_request(&id).is_some(),
            "a malformed answer keeps the request pending"
        );
        harness
            .respond_server_request(
                id.clone(),
                ServerRequestResponse::error(-32000, "User input request cancelled."),
            )
            .await
            .unwrap();
        assert_eq!(server_request_resolved(&mut stream).await, id);
        assert_eq!(
            answers_written(&record)[1]["response"]["response"],
            json!({"behavior": "deny", "message": "User input request cancelled."})
        );
        until_completed(&mut stream).await;
        assert!(logs_contain("action=\"respond_server_request\""));
        logs_assert(no_line_with("Cats"));
        logs_assert(no_line_with("cats or dogs"));
        harness.shutdown().await.unwrap();
    }

    #[test]
    fn ask_user_question_answers_skip_unanswered_questions() {
        let (_, input) = ask_user_question("q");
        let ask = crate::session::PendingAsk {
            thread: ThreadId::new(),
            request_id: "q".into(),
            tool_use_id: None,
            tool_name: None,
            suggestions: Vec::new(),
            subtype: "can_use_tool".into(),
            input,
        };
        let line = crate::session::server_request_line(
            &ask,
            &ServerRequestResponse::result(json!({"answers": {
                "0": {"answers": []},
                "1": {"answers": ["Cherry"]},
            }})),
        )
        .unwrap();
        let line: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(
            line["response"]["response"]["updatedInput"]["answers"],
            json!({"Which fruits do you like?": "Cherry"})
        );
        // An id that names no question is not that shape.
        assert!(
            crate::session::server_request_line(
                &ask,
                &ServerRequestResponse::result(json!({"answers": {"7": {"answers": ["x"]}}})),
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn other_control_requests_round_trip() {
        let request = |id: &str| {
            json!({"type": "control_request", "request_id": id, "request": {
                "subtype": "rename_session", "title": "From the CLI"
            }})
            .to_string()
        };
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(user(), vec![Action::Emit(vec![request("r7")])]),
                Step::OnStdin(answered(), vec![Action::Emit(vec![request("r8")])]),
                Step::OnStdin(answered(), vec![text_turn()]),
            ],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        harness
            .start_turn(&handle, text("go"), overrides())
            .await
            .unwrap();

        let (id, method, params) = server_request_received(&mut stream).await;
        assert_eq!(method, "claude/rename_session");
        assert_eq!(params["title"], "From the CLI");
        harness
            .respond_server_request(
                id.clone(),
                ServerRequestResponse::result(json!({"ok": true})),
            )
            .await
            .unwrap();
        assert_eq!(server_request_resolved(&mut stream).await, id);

        let (id, _, _) = server_request_received(&mut stream).await;
        harness
            .respond_server_request(id.clone(), ServerRequestResponse::error(-32000, "nope"))
            .await
            .unwrap();
        assert_eq!(server_request_resolved(&mut stream).await, id);
        until_completed(&mut stream).await;

        assert_eq!(
            answers_written(&record),
            [
                json!({"type": "control_response", "response": {
                    "subtype": "success", "request_id": "r7", "response": {"ok": true}
                }}),
                json!({"type": "control_response", "response": {
                    "subtype": "error", "request_id": "r8", "error": "nope"
                }}),
            ]
        );
        harness.shutdown().await.unwrap();
    }

    // ---- milestone 3: compaction ----------------------------------------------------------------

    #[tokio::test]
    #[traced_test]
    async fn compact_runs_a_compaction_turn() {
        // The `compact` fixture's frames after its first turn's `result`.
        let first_result = fixture_lines("compact")
            .iter()
            .position(|line| line.contains("\"type\": \"result\""))
            .unwrap();
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                user(),
                vec![Action::EmitFixtureFrom {
                    name: "compact",
                    from: first_result + 1,
                }],
            )],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        let before = subtypes_written(&record).len();
        harness.compact_thread(&handle).await.unwrap();
        let events = until_completed(&mut stream).await;

        let turn = match events.first() {
            Some(AgentEvent::TurnStarted { turn, .. }) => *turn,
            other => panic!("expected TurnStarted, got {other:?}"),
        };
        let items: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::ItemCompleted { item, .. } => Some(item),
                _ => None,
            })
            .collect();
        assert_eq!(items.len(), 1, "{items:?}");
        assert!(matches!(
            items[0].payload,
            giskard_core::item::ItemPayload::Activity { .. }
        ));
        assert_eq!(completion(&events), (turn, TurnStatusKind::Completed, None));
        logs_assert(no_line_with("permission_mode_drift"));
        // No per-turn settings for a compaction turn: only the message.
        assert_eq!(subtypes_written(&record).len(), before);
        assert_eq!(
            written(&record).pop().unwrap(),
            json!({"type": "user", "message": {"role": "user", "content": [
                {"type": "text", "text": "/compact"}
            ]}})
        );
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn compact_during_a_turn_is_thread_busy() {
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
            .start_turn(&handle, text("go"), overrides())
            .await
            .unwrap();
        assert!(matches!(
            harness.compact_thread(&handle).await,
            Err(HarnessError::ThreadBusy { thread: t }) if t == thread
        ));
        assert_eq!(
            written(&record)
                .iter()
                .filter(|line| line["type"] == "user")
                .count(),
            1
        );
        let cold = ThreadHandle::detached(ThreadId::new(), RESUME_ID.into());
        assert!(matches!(
            harness.compact_thread(&cold).await,
            Err(HarnessError::ThreadNotFound(_))
        ));
        harness.shutdown().await.unwrap();
    }

    // ---- review follow-ups ---------------------------------------------------------------------

    #[tokio::test]
    async fn a_model_switch_sends_and_checks_the_requested_effort() {
        // Open at `high`; switching to a model without effort must not pass silently.
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(control("set_model"), vec![Action::Respond(Value::Null)]),
                Step::OnStdin(
                    control("apply_flag_settings"),
                    vec![Action::Respond(Value::Null)],
                ),
                Step::OnStdin(
                    control("get_settings"),
                    vec![Action::Respond(settings("haiku"))],
                ),
            ],
        );
        let (harness, _) = harness(vec![child]);
        let (mut options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        options.initial_model = with_effort("sonnet", "high");
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        let error = harness
            .start_turn(
                &handle,
                text("go"),
                turn_overrides(
                    PermissionPreset::AskFirst,
                    Mode::Build,
                    Some(with_effort("haiku", "high")),
                ),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&error, HarnessError::Unsupported(message)
                if message == "Claude Code did not accept effort high for haiku"),
            "{error}"
        );
        let subtypes = subtypes_written(&record);
        assert!(subtypes.contains(&"set_model".to_owned()), "{subtypes:?}");
        assert!(
            subtypes.contains(&"apply_flag_settings".to_owned()),
            "{subtypes:?}"
        );
        assert!(stream.try_recv().is_none(), "no TurnStarted");
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn a_reply_that_cannot_be_written_while_waiting_breaks_the_child() {
        // While the turn's mode request is pending, the CLI asks for `ExitPlanMode` (which the
        // mapper denies itself) and its stdin breaks: the deny cannot be written.
        let exit_plan = json!({"type": "control_request", "request_id": "plan-1", "request": {
            "subtype": "can_use_tool", "tool_name": "ExitPlanMode", "input": {},
            "permission_suggestions": null, "tool_use_id": "toolu_plan"
        }})
        .to_string();
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                control("set_permission_mode"),
                vec![Action::BreakStdin, Action::Emit(vec![exit_plan])],
            )],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let error = harness
            .start_turn(&handle, text("go"), overrides())
            .await
            .unwrap_err();
        assert!(matches!(error, HarnessError::Transport(_)), "{error}");
        assert!(lock(&record).killed);
        until_no_children(&harness).await;
        logs_assert(a_line_with(&["ERROR", "action=\"write_stdin\""]));
    }

    #[tokio::test]
    #[traced_test]
    async fn an_answer_overtaken_by_a_withdrawal_is_not_sent() {
        let (index, ask_id) = ask_of("tool-allowed");
        let dialog = json!({"type": "control_request", "request_id": "dialog-1", "request": {
            "subtype": "request_user_dialog", "title": "t"
        }})
        .to_string();
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                user(),
                vec![
                    Action::EmitFixturePrefix {
                        name: "tool-allowed",
                        count: index + 1,
                    },
                    Action::Emit(vec![dialog]),
                ],
            )],
        );
        let injector = child.injector();
        let (harness, _handle, mut stream, approval) = ask_pending(child).await;
        next_matching(&mut stream, |event| {
            matches!(event, AgentEvent::ServerRequestReceived { .. }).then_some(())
        })
        .await;
        let cancel =
            |id: &str| json!({"type": "control_cancel_request", "request_id": id}).to_string();

        // The withdrawal is already on stdout when the façade takes the ask: the supervisor reads
        // it first (stdout wins its select), so the answer arrives for an ask no longer asked.
        injector.emit(cancel(&ask_id));
        assert!(matches!(
            harness.respond_approval(approval, ApprovalDecision::Accept).await,
            Err(HarnessError::Protocol(message)) if message.contains("withdrawn")
        ));
        injector.emit(cancel("dialog-1"));
        assert!(matches!(
            harness
                .respond_server_request(
                    ServerRequestId::new("dialog-1"),
                    ServerRequestResponse::result(json!({}))
                )
                .await,
            Err(HarnessError::Protocol(message)) if message.contains("withdrawn")
        ));
        assert_eq!(
            server_request_resolved(&mut stream).await,
            ServerRequestId::new("dialog-1")
        );
        assert!(answers_written(&record).is_empty(), "nothing late is sent");
        assert_eq!(lock(&harness.pending).len(), 0);
        assert!(logs_contain(
            "late answer to an approval Claude Code withdrew"
        ));
        harness.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn the_turn_settings_share_one_budget() {
        // Each answer comes after 9 s, under its own 10 s: after two of them only 7 s of the
        // supervisor's 25 s budget are left for the third, which ends the hand-off before the
        // façade's 30 s.
        let late = |payload: Value| Action::RespondAfter {
            delay: Duration::from_secs(9),
            payload,
        };
        let (child, _) = scripted(
            handshake_steps("sonnet"),
            vec![
                Step::OnStdin(
                    control("set_permission_mode"),
                    vec![late(json!({"mode": "default"}))],
                ),
                Step::OnStdin(control("set_model"), vec![late(Value::Null)]),
                Step::OnStdin(control("apply_flag_settings"), vec![late(Value::Null)]),
            ],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let started = Instant::now();
        let error = harness
            .start_turn(
                &handle,
                text("go"),
                turn_overrides(
                    PermissionPreset::AskFirst,
                    Mode::Build,
                    Some(with_effort("opus", "high")),
                ),
            )
            .await
            .unwrap_err();
        let elapsed = started.elapsed();
        assert!(
            matches!(&error, HarnessError::Timeout(message)
                if message == "claude did not answer apply_flag_settings within 7.0 s"),
            "{error}"
        );
        assert!(
            elapsed >= crate::session::TURN_SETTINGS_BUDGET,
            "{elapsed:?}"
        );
        assert!(elapsed < START_TURN_TIMEOUT, "{elapsed:?}");
        harness.shutdown().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_stop_during_the_turn_settings_is_not_delayed() {
        // The mode request is never answered.
        let (child, record) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(control("set_permission_mode"), Vec::new())],
        );
        let (harness, _) = harness(vec![child]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let handle = harness.open_thread(options).await.unwrap();
        let started = Instant::now();
        let (turn, ()) = tokio::join!(
            harness.start_turn(&handle, text("go"), overrides()),
            async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                harness.delete_thread(&handle).await.unwrap();
            }
        );
        assert!(
            matches!(&turn, Err(HarnessError::Transport(message)) if message.contains("stopping")),
            "{turn:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
        assert!(lock(&record).stdin_closed);
        assert!(written(&record).iter().all(|line| line["type"] != "user"));
        assert_eq!(harness.live_children(), 0);
    }

    #[tokio::test]
    async fn a_bypass_child_that_exits_on_set_permission_mode_reports_its_exit() {
        let (child, _) = ScriptedChild::new(vec![
            Step::OnStdin(
                control("initialize"),
                vec![Action::Respond(initialize_payload())],
            ),
            Step::OnStdin(
                control("set_permission_mode"),
                vec![Action::Exit {
                    code: 2,
                    stderr: vec!["fatal: permissions unreadable".into()],
                }],
            ),
        ]);
        let (harness, _) = harness(vec![child.without_mode_echo()]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        let error = harness.open_thread(options).await.unwrap_err();
        assert!(
            matches!(&error, HarnessError::Spawn(message)
                if message == "claude exited with code 2 before answering set_permission_mode: \
                               fatal: permissions unreadable"),
            "{error}"
        );
        assert_eq!(harness.live_children(), 0);
    }

    // ---- isolation and timeouts ----------------------------------------------------------------

    #[tokio::test]
    #[traced_test]
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
        assert!(logs_contain("action=\"read_stdout\""));

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

    // ---- MCP servers ---------------------------------------------------------------------------

    fn mcp_probe_child(answer: Action) -> (ScriptedChild, Arc<Mutex<ScriptRecord>>) {
        ScriptedChild::new(vec![
            Step::OnStdin(
                control("initialize"),
                vec![Action::Respond(initialize_payload())],
            ),
            Step::OnStdin(control("mcp_status"), vec![answer]),
        ])
    }

    #[traced_test]
    #[tokio::test]
    async fn list_mcp_servers_probes_when_no_child_is_live() {
        let (empty, empty_record) = mcp_probe_child(Action::Respond(json!({"mcpServers": []})));
        let (two, _) = mcp_probe_child(Action::Respond(crate::mcp::tests::failed_and_pending()));
        let (harness, spawner) = harness(vec![empty, two]);

        assert!(harness.list_mcp_servers().await.unwrap().is_empty());
        assert!(lock(&empty_record).stdin_closed, "the probe exits");
        assert_eq!(harness.live_children(), 0, "a probe is not a live child");
        assert!(
            harness.catalog_snapshot().is_some(),
            "the probe's initialize stores the catalog"
        );

        let servers = harness.list_mcp_servers().await.unwrap();
        let names: Vec<_> = servers.iter().map(|server| server.name.as_str()).collect();
        assert_eq!(names, ["broken", "echo"]);
        assert!(servers.iter().all(|server| server.auth_status
            == giskard_core::mcp::McpAuthStatus::Unknown
            && server.tools.is_empty()));
        assert_eq!(
            servers[0]
                .server_info
                .as_ref()
                .and_then(|info| info.description.as_deref()),
            Some("failed: ENOENT: no such file or directory, posix_spawn 'stdio'")
        );
        assert_eq!(
            spawner.spawns()[0],
            crate::process::probe_argv(&ClaudeLaunchOptions::default())
        );
        assert!(logs_contain("action=\"mcp_probe\""));
        assert!(logs_contain("servers=2"));
    }

    #[tokio::test]
    async fn list_mcp_servers_maps_a_needs_auth_status() {
        let (probe, _) = mcp_probe_child(Action::Respond(json!({"mcpServers": [
            {"name": "remote", "status": "needs-auth", "scope": "user", "source": "user"}
        ]})));
        let (harness, _) = harness(vec![probe]);
        let servers = harness.list_mcp_servers().await.unwrap();
        assert_eq!(
            servers[0].auth_status,
            giskard_core::mcp::McpAuthStatus::NotLoggedIn
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn list_mcp_servers_asks_a_live_child_without_probing() {
        let (session, record) = scripted(
            handshake_steps("sonnet"),
            vec![Step::OnStdin(
                control("mcp_status"),
                vec![Action::Respond(crate::mcp::tests::failed_and_pending())],
            )],
        );
        let (harness, spawner) = harness(vec![session]);
        let (options, _updates) = open_options(ThreadId::new(), None, "sonnet");
        harness.open_thread(options).await.unwrap();

        let servers = harness.list_mcp_servers().await.unwrap();
        assert_eq!(servers.len(), 2);
        assert_eq!(spawner.spawns().len(), 1, "no probe was spawned");
        assert!(
            written(&record)
                .iter()
                .any(|line| line["request"]["subtype"] == "mcp_status")
        );
        assert!(logs_contain("read the MCP servers of a live claude child"));
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_refused_mcp_status_is_a_protocol_error() {
        let (probe, record) = mcp_probe_child(Action::RespondError("mcp unavailable"));
        let (harness, _) = harness(vec![probe]);
        let error = harness.list_mcp_servers().await.unwrap_err();
        assert!(
            matches!(&error, HarnessError::Protocol(message) if message == "mcp unavailable"),
            "{error}"
        );
        assert!(lock(&record).stdin_closed, "the refused probe is reaped");
    }

    #[tokio::test]
    async fn an_mcp_probe_that_exits_before_answering_is_the_handshake_error() {
        let (probe, _) = mcp_probe_child(Action::Exit {
            code: 1,
            stderr: vec!["boom".into()],
        });
        let (harness, _) = harness(vec![probe]);
        let error = harness.list_mcp_servers().await.unwrap_err();
        assert!(
            matches!(&error, HarnessError::Spawn(message)
                if message.contains("before answering mcp_status") && message.contains("boom")),
            "{error}"
        );
    }

    #[tokio::test]
    async fn list_mcp_servers_after_shutdown_is_refused() {
        let (harness, spawner) = harness(Vec::new());
        harness.shutdown().await.unwrap();
        assert!(matches!(
            harness.list_mcp_servers().await,
            Err(HarnessError::Transport(_))
        ));
        assert!(spawner.spawns().is_empty());
    }

    #[tokio::test]
    async fn a_probe_whose_initialize_is_refused_is_a_spawn_error() {
        let (probe, record) = ScriptedChild::new(vec![Step::OnStdin(
            control("initialize"),
            vec![Action::RespondError("not now")],
        )]);
        let (harness, _) = harness(vec![probe]);
        let error = harness.list_mcp_servers().await.unwrap_err();
        assert!(
            matches!(&error, HarnessError::Spawn(message)
                if message == "claude refused initialize: not now"),
            "{error}"
        );
        assert!(lock(&record).stdin_closed, "the refused probe is reaped");
        assert!(harness.catalog_snapshot().is_none());
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
    async fn a_real_probe_lists_no_mcp_servers() {
        let (harness, _workspace) = real_harness(&[]);
        assert!(harness.list_mcp_servers().await.unwrap().is_empty());
        assert_eq!(harness.live_children(), 0);
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
    async fn a_real_child_sets_mode_model_and_effort_per_turn() {
        // Asserted on events, not log lines: tracing's per-thread capture is not reliable across
        // parallel real-process tests. The fake's `init` always reports `default`, so every turn
        // set to another mode yields a drift notice naming the mode Giskard set.
        let (harness, workspace) = real_harness(&[]);
        let (options, _updates) = real_options(&workspace, None);
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        for (overrides, mode) in [
            (
                turn_overrides(
                    PermissionPreset::AutoApprove,
                    Mode::Build,
                    Some(with_effort("opus", "high")),
                ),
                "acceptEdits",
            ),
            (
                turn_overrides(
                    PermissionPreset::FullAccess,
                    Mode::Build,
                    Some(with_effort("opus", "high")),
                ),
                "bypassPermissions",
            ),
            (
                turn_overrides(
                    PermissionPreset::AskFirst,
                    Mode::Plan,
                    Some(model("sonnet")),
                ),
                "plan",
            ),
        ] {
            let requested = overrides.model.clone();
            harness
                .start_turn(&handle, text("pong?"), overrides)
                .await
                .unwrap();
            let events = until_completed(&mut stream).await;
            assert_eq!(completion(&events).1, TurnStatusKind::Completed);
            let notices: Vec<&str> = events
                .iter()
                .filter_map(|event| match event {
                    AgentEvent::Notice { message, .. } => Some(message.as_str()),
                    _ => None,
                })
                .collect();
            assert_eq!(
                notices,
                [format!(
                    "Claude Code switched its permission mode to default; Giskard set {mode}"
                )]
            );
            assert!(events.iter().any(|event| matches!(
                event,
                AgentEvent::TurnUsageUpdated { model, .. } if *model == requested
            )));
        }
        let error = harness
            .start_turn(
                &handle,
                text("pong?"),
                turn_overrides(
                    PermissionPreset::AskFirst,
                    Mode::Build,
                    Some(model("gpt-5")),
                ),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&error, HarnessError::Unsupported(message) if message == "Model 'gpt-5' not found"),
            "{error}"
        );
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_real_child_answers_an_ask() {
        let (harness, workspace) = real_harness(&[]);
        let (options, _updates) = real_options(&workspace, None);
        let handle = harness.open_thread(options).await.unwrap();
        let mut stream = harness.subscribe(&handle);
        harness
            .start_turn(&handle, text("touch probe.txt"), overrides())
            .await
            .unwrap();
        let approval = approval_requested(&mut stream).await;
        harness
            .respond_approval(approval, ApprovalDecision::Accept)
            .await
            .unwrap();
        let events = until_completed(&mut stream).await;
        assert_eq!(completion(&events).1, TurnStatusKind::Completed);
        assert_eq!(command_status(&events).as_deref(), Some("completed"));
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_real_refused_bypass_launch_refuses_full_access() {
        let (harness, workspace) = real_harness(&[("FAKE_CLAUDE_REFUSE_BYPASS", "1")]);
        let (options, _updates) = real_options(&workspace, None);
        let handle = harness.open_thread(options).await.unwrap();
        let error = harness
            .start_turn(
                &handle,
                text("pong?"),
                turn_overrides(PermissionPreset::FullAccess, Mode::Build, None),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&error, HarnessError::Unsupported(message) if message.contains(ROOT_REFUSAL)),
            "{error}"
        );
        let mut stream = harness.subscribe(&handle);
        harness
            .start_turn(&handle, text("pong?"), overrides())
            .await
            .unwrap();
        assert_eq!(
            completion(&until_completed(&mut stream).await).1,
            TurnStatusKind::Completed
        );
        harness.shutdown().await.unwrap();
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
