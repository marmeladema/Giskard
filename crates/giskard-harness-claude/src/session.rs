//! The supervisor: one task per `claude` child.
//!
//! The task is the single owner of the child process, the [`ClaudeMapper`], the pending
//! control-request waiters and the thread's retained [`EventLog`]. Nothing else touches them: the
//! façade reaches the task only through its command channel, so there is no lock around the
//! mapper or the stdin handle (the `CodexInstance` rule of `AGENTS.md`, applied per child).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ServerRequestId, ThreadId, TurnId};
use giskard_core::model::ModelRef;
use giskard_core::server_request::ServerRequestResponse;
use giskard_harness::EventLog;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{Instrument, debug, error, info, warn};

use crate::log_fields::display_opt;
use crate::mapper::{ClaudeMapper, MapperOutput, TurnKind};
use crate::process::{ChildExit, ChildLogContext, ClaudeChild, LaunchMode};

/// How long one control request the supervisor awaits itself may take (the per-turn settings).
pub(crate) const CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a turn's settings (mode, model, effort, read-back) may take in all. It stays under the
/// façade's 30 s `start_turn` budget, so the supervisor's own timeout, naming the request left
/// unanswered, is what the caller sees.
pub(crate) const TURN_SETTINGS_BUDGET: Duration = Duration::from_secs(25);
/// How often `await_control` stops reading to look for a stop, a shutdown or another command.
const AWAIT_POLL_SLICE: Duration = Duration::from_millis(50);
/// What every deny the adapter writes for `Decline` says; the model and the transcript see it.
pub(crate) const DECLINE_MESSAGE: &str = "Declined by the user in Giskard";
/// What the deny the adapter writes for `Cancel` says.
pub(crate) const CANCEL_MESSAGE: &str = "Cancelled by the user in Giskard";
/// How long the stop sequence waits for an interrupted turn's `result`.
pub(crate) const STOP_INTERRUPT_GRACE: Duration = Duration::from_secs(5);
/// How long the stop sequence waits for EOF after closing stdin before it kills the child.
pub(crate) const STOP_EXIT_GRACE: Duration = Duration::from_secs(5);

/// The settings a turn asks for. The supervisor compares them with what the CLI holds and sends
/// only what differs, except the mode, which is sent on every turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnSettings {
    /// The CLI's name for the mode (`default`, `acceptEdits`, `bypassPermissions`, `plan`).
    pub mode: String,
    /// The model selector the turn asks for; `None` keeps the one the CLI holds.
    pub model: Option<String>,
    /// The effort level the turn asks for; `None` leaves the CLI's alone (clearing it is
    /// unverified).
    pub effort: Option<String>,
    /// The catalog's `resolvedModel` for `model`, which the read-back also accepts.
    pub resolved_model: Option<String>,
}

/// What the façade asks a supervisor to do.
pub(crate) enum ChildCommand {
    StartTurn {
        line: String,
        turn: TurnId,
        /// The model reported on the turn's usage events.
        model: ModelRef,
        settings: TurnSettings,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    },
    Interrupt {
        reply: oneshot::Sender<Result<(), HarnessError>>,
    },
    /// A control request whose success payload the caller wants (`rename_session`).
    Control {
        request: Value,
        reply: oneshot::Sender<Result<Value, HarnessError>>,
    },
    /// Answer a `can_use_tool` ask. The façade already removed `ask` from the pending map.
    RespondApproval {
        id: ApprovalId,
        ask: PendingAsk,
        decision: ApprovalDecision,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    },
    /// Answer an `AskUserQuestion` or another inbound control request. The façade already
    /// removed `ask` from the pending map.
    RespondServerRequest {
        id: ServerRequestId,
        ask: PendingAsk,
        response: ServerRequestResponse,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    },
    /// Run `/compact` as a compaction turn.
    Compact {
        turn: TurnId,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    },
    Stop {
        reply: oneshot::Sender<()>,
    },
}

/// The façade's view of one live child.
pub(crate) struct ChildHandle {
    pub harness_thread_id: String,
    pub log: Arc<EventLog>,
    pub commands: mpsc::Sender<ChildCommand>,
    pub task: JoinHandle<()>,
    /// The model the thread was opened on; a turn without a model override runs on it.
    pub model: ModelRef,
    /// The mode the child was launched with: `full_access` needs `Bypass`.
    pub launch_mode: LaunchMode,
    /// Distinguishes this child from a later one for the same thread, so a supervisor that ends
    /// after its thread was reopened never removes the new entry.
    pub generation: u64,
}

pub(crate) type Children = Arc<Mutex<HashMap<ThreadId, ChildHandle>>>;
pub(crate) type Pending = Arc<Mutex<PendingRequests>>;

/// Lock a std mutex whose guarded maps stay consistent even if a holder panicked.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One ask the CLI published and nothing has answered yet.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PendingAsk {
    pub thread: ThreadId,
    /// The CLI's `request_id` the answer must carry.
    pub request_id: String,
    pub tool_use_id: Option<String>,
    /// The asked tool, for an approval; `None` for a server request.
    pub tool_name: Option<String>,
    /// An approval's raw `permission_suggestions`, echoed by `AcceptForSession`.
    pub suggestions: Vec<Value>,
    /// `can_use_tool` (an approval or an `AskUserQuestion`), else the control request's subtype.
    pub subtype: String,
    /// An `AskUserQuestion`'s `input`, whose `questions` the answer echoes; `Null` otherwise.
    pub input: Value,
}

/// Which map a pending ask lives in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestKind {
    Approval,
    ServerRequest,
}

impl RequestKind {
    fn as_str(self) -> &'static str {
        match self {
            RequestKind::Approval => "approval",
            RequestKind::ServerRequest => "server_request",
        }
    }
}

/// Approvals and server requests awaiting `respond_*`. The ids are the CLI's own `request_id`
/// UUIDs, which is what makes them unique across this instance's children.
#[derive(Debug, Default)]
pub(crate) struct PendingRequests {
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: The approval half of `ClaudeHarness::pending` (see that field).
    // Source of truth: A supervisor inserts on the mapper's `PendingApproval`.
    // Structural reason: `respond_approval` carries no thread.
    // Synchronization: The façade's std mutex around this struct.
    // Invalidation/removal: Answer, `control_cancel_request`, child exit, thread stop, `shutdown`.
    approvals: HashMap<ApprovalId, PendingAsk>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: The server-request half of `ClaudeHarness::pending` (see that field).
    // Source of truth: A supervisor inserts on the mapper's `PendingServerRequest`.
    // Structural reason: `respond_server_request` carries no thread.
    // Synchronization: The façade's std mutex around this struct.
    // Invalidation/removal: Answer, `control_cancel_request`, child exit, thread stop, `shutdown`.
    server_requests: HashMap<ServerRequestId, PendingAsk>,
}

impl PendingRequests {
    pub fn insert_approval(&mut self, id: ApprovalId, ask: PendingAsk) {
        self.approvals.insert(id, ask);
    }

    pub fn insert_server_request(&mut self, id: ServerRequestId, ask: PendingAsk) {
        self.server_requests.insert(id, ask);
    }

    #[cfg(test)]
    pub fn approval(&self, id: &ApprovalId) -> Option<&PendingAsk> {
        self.approvals.get(id)
    }

    #[cfg(test)]
    pub fn server_request(&self, id: &ServerRequestId) -> Option<&PendingAsk> {
        self.server_requests.get(id)
    }

    pub fn remove_approval(&mut self, id: &ApprovalId) -> Option<PendingAsk> {
        self.approvals.remove(id)
    }

    pub fn remove_server_request(&mut self, id: &ServerRequestId) -> Option<PendingAsk> {
        self.server_requests.remove(id)
    }

    /// Remove the ask of `thread` whose CLI request id is `request_id`, whichever map holds it.
    pub fn remove_by_request_id(
        &mut self,
        thread: ThreadId,
        request_id: &str,
    ) -> Option<(RequestKind, PendingAsk)> {
        let approval = self
            .approvals
            .iter()
            .find(|(_, ask)| ask.thread == thread && ask.request_id == request_id)
            .map(|(id, _)| id.clone());
        if let Some(ask) = approval.and_then(|id| self.approvals.remove(&id)) {
            return Some((RequestKind::Approval, ask));
        }
        let request = self
            .server_requests
            .iter()
            .find(|(_, ask)| ask.thread == thread && ask.request_id == request_id)
            .map(|(id, _)| id.clone());
        request
            .and_then(|id| self.server_requests.remove(&id))
            .map(|ask| (RequestKind::ServerRequest, ask))
    }

    /// Drop every ask of one thread; returns how many there were.
    pub fn remove_thread(&mut self, thread: ThreadId) -> usize {
        let before = self.len();
        self.approvals.retain(|_, ask| ask.thread != thread);
        self.server_requests.retain(|_, ask| ask.thread != thread);
        before - self.len()
    }

    pub fn clear(&mut self) -> usize {
        let count = self.len();
        self.approvals.clear();
        self.server_requests.clear();
        count
    }

    pub fn len(&self) -> usize {
        self.approvals.len() + self.server_requests.len()
    }
}

/// A fresh control-request id. The CLI echoes it on the response.
pub(crate) fn new_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// One stdin control request line.
pub(crate) fn control_line(request_id: &str, request: &Value) -> String {
    json!({"type": "control_request", "request_id": request_id, "request": request}).to_string()
}

/// A control response's outcome: the `response` payload of a success, the `error` of a failure.
pub(crate) fn control_outcome(payload: &Value) -> Result<Value, HarnessError> {
    match payload.get("subtype").and_then(Value::as_str) {
        Some("success") => Ok(payload.get("response").cloned().unwrap_or(Value::Null)),
        Some("error") => Err(HarnessError::Protocol(
            payload
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("Claude Code rejected the control request")
                .to_owned(),
        )),
        other => Err(HarnessError::Protocol(format!(
            "unexpected control response subtype {other:?}"
        ))),
    }
}

fn child_stopped() -> HarnessError {
    HarnessError::Transport("claude child stopped".into())
}

/// One control response line answering an inbound control request.
fn control_response_line(request_id: &str, response: Value) -> String {
    json!({"type": "control_response", "response": {
        "subtype": "success",
        "request_id": request_id,
        "response": response,
    }})
    .to_string()
}

/// The `can_use_tool` answer for one approval decision (plan §9.3), and how many rules it echoes.
/// `Err` for a decision the adapter does not offer.
pub(crate) fn approval_response(
    ask: &PendingAsk,
    decision: &ApprovalDecision,
) -> Result<(Value, usize), HarnessError> {
    match decision {
        ApprovalDecision::Accept => Ok((json!({"behavior": "allow"}), 0)),
        ApprovalDecision::AcceptForSession => {
            // The CLI's own rule objects, cloned raw (its `ruleContent` is never rewritten), with
            // the destination narrowed to the session: the grant dies with this child.
            let rules: Vec<Value> = ask
                .suggestions
                .iter()
                .filter(|suggestion| {
                    suggestion.get("type").and_then(Value::as_str) == Some("addRules")
                })
                .map(|suggestion| {
                    let mut suggestion = suggestion.clone();
                    if let Some(object) = suggestion.as_object_mut() {
                        object.insert("destination".into(), Value::String("session".into()));
                    }
                    suggestion
                })
                .collect();
            if rules.is_empty() {
                return Ok((json!({"behavior": "allow"}), 0));
            }
            let count = rules.len();
            Ok((
                json!({"behavior": "allow", "updatedPermissions": rules}),
                count,
            ))
        }
        ApprovalDecision::Decline => {
            Ok((json!({"behavior": "deny", "message": DECLINE_MESSAGE}), 0))
        }
        ApprovalDecision::Cancel => Ok((
            json!({"behavior": "deny", "message": CANCEL_MESSAGE, "interrupt": true}),
            0,
        )),
        ApprovalDecision::AcceptWithExecPolicyAmendment { .. } => Err(HarnessError::Unsupported(
            "Claude Code has no exec-policy amendments; answer the approval another way".into(),
        )),
    }
}

/// The control response line answering one server request.
pub(crate) fn server_request_line(
    ask: &PendingAsk,
    response: &ServerRequestResponse,
) -> Result<String, HarnessError> {
    if ask.subtype == "can_use_tool" {
        // `AskUserQuestion`: the answer is a `can_use_tool` permission result.
        let answer = match response {
            ServerRequestResponse::Result { value } => {
                let questions = ask.input.get("questions").cloned().unwrap_or(json!([]));
                let answers = ask_user_question_answers(&questions, value).ok_or_else(|| {
                    HarnessError::Protocol(format!(
                        "server request {} expects an answer of the form \
                         {{\"answers\": {{<question id>: {{\"answers\": [<label>]}}}}}}",
                        ask.request_id
                    ))
                })?;
                json!({"behavior": "allow", "updatedInput": {
                    "questions": questions,
                    "answers": answers,
                }})
            }
            ServerRequestResponse::Error { message, .. } => {
                json!({"behavior": "deny", "message": message})
            }
        };
        return Ok(control_response_line(&ask.request_id, answer));
    }
    Ok(match response {
        ServerRequestResponse::Result { value } => {
            control_response_line(&ask.request_id, value.clone())
        }
        // The control protocol's error shape; the browser's numeric code has no counterpart.
        ServerRequestResponse::Error { message, .. } => {
            json!({"type": "control_response", "response": {
                "subtype": "error",
                "request_id": ask.request_id,
                "error": message,
            }})
            .to_string()
        }
    })
}

/// Translate the browser's `{answers: {<id>: {answers: [<label>…]}}}` into the CLI's map keyed
/// by **question text** (keyed by `header`, the CLI reports the questions unanswered), several
/// labels joined with `", "`. A question with no answer is left out. `None` for another shape.
fn ask_user_question_answers(questions: &Value, value: &Value) -> Option<Value> {
    let questions = questions.as_array()?;
    let by_id = value.get("answers")?.as_object()?;
    let mut answers = serde_json::Map::new();
    for (id, answer) in by_id {
        let index: usize = id.parse().ok()?;
        let text = questions.get(index)?.get("question")?.as_str()?;
        let labels = answer
            .get("answers")?
            .as_array()?
            .iter()
            .map(Value::as_str)
            .collect::<Option<Vec<&str>>>()?;
        if labels.is_empty() {
            continue;
        }
        answers.insert(text.to_owned(), Value::String(labels.join(", ")));
    }
    Some(Value::Object(answers))
}

/// Who waits for a control response.
enum Waiter {
    Unit(oneshot::Sender<Result<(), HarnessError>>),
    Value(oneshot::Sender<Result<Value, HarnessError>>),
    /// The supervisor's own `await_control`: it wants the raw payload, refusals included, so it
    /// can read the CLI's `error_code`.
    Raw(oneshot::Sender<Result<Value, HarnessError>>),
    /// The stop sequence's own interrupt: nobody awaits the response.
    Stop,
}

impl Waiter {
    /// `payload` is the control response's `response` object, or why none will come.
    fn resolve(self, payload: Result<Value, HarnessError>) {
        // A caller that gave up (its timeout fired) has dropped the receiver; nothing to tell.
        match self {
            Waiter::Unit(reply) => {
                let _ = reply.send(
                    payload
                        .and_then(|payload| control_outcome(&payload))
                        .map(|_| ()),
                );
            }
            Waiter::Value(reply) => {
                let _ = reply.send(payload.and_then(|payload| control_outcome(&payload)));
            }
            Waiter::Raw(reply) => {
                let _ = reply.send(payload);
            }
            Waiter::Stop => {}
        }
    }
}

/// Why `await_control` returned no success payload.
#[derive(Debug)]
enum ControlFailure {
    /// The CLI answered with an error response.
    Refused {
        message: String,
        code: Option<String>,
    },
    /// No answer: a timeout, or the child stopped or broke.
    Failed(HarnessError),
}

impl ControlFailure {
    fn into_error(self) -> HarnessError {
        match self {
            ControlFailure::Refused { message, .. } => HarnessError::Protocol(message),
            ControlFailure::Failed(error) => error,
        }
    }

    fn code(&self) -> Option<&str> {
        match self {
            ControlFailure::Refused { code, .. } => code.as_deref(),
            ControlFailure::Failed(_) => None,
        }
    }
}

impl std::fmt::Display for ControlFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlFailure::Refused { message, .. } => formatter.write_str(message),
            ControlFailure::Failed(error) => write!(formatter, "{error}"),
        }
    }
}

/// Everything one supervisor task owns, handed over at spawn.
pub(crate) struct SupervisorParts {
    pub child: Box<dyn ClaudeChild>,
    pub mapper: ClaudeMapper,
    pub log: Arc<EventLog>,
    pub commands: mpsc::Receiver<ChildCommand>,
    pub shutdown: watch::Receiver<bool>,
    pub children: Children,
    pub pending: Pending,
    pub generation: u64,
    pub thread: ThreadId,
    pub context: ChildLogContext,
    /// Lines the handshake read that were not its own responses, mapped first.
    pub early_lines: Vec<String>,
    /// Handshake requests that timed out; the CLI may still answer them.
    pub abandoned_requests: Vec<String>,
    /// The model the child was opened on, which the CLI holds until a turn changes it.
    pub model: ModelRef,
}

pub(crate) fn spawn_supervisor(parts: SupervisorParts) -> JoinHandle<()> {
    let supervisor = Supervisor {
        pid: parts.child.pid(),
        child: parts.child,
        mapper: parts.mapper,
        log: parts.log,
        commands: parts.commands,
        shutdown: parts.shutdown,
        children: parts.children,
        pending: parts.pending,
        generation: parts.generation,
        thread: parts.thread,
        context: parts.context,
        waiters: HashMap::new(),
        early_lines: parts.early_lines,
        abandoned: parts.abandoned_requests.into_iter().collect(),
        eof: false,
        interrupt_sent: false,
        dropped_events: 0,
        failure: None,
        withdrawn: HashSet::new(),
        deferred: VecDeque::new(),
        stop_request: None,
        // The handshake set `default` on both launch modes.
        current_mode: "default".into(),
        current_effort: parts
            .model
            .reasoning_effort
            .as_ref()
            .map(|effort| effort.0.clone()),
        current_model: parts.model,
    };
    // The supervisor logs for the child's whole life: it runs in the span `open_thread` ran in.
    tokio::spawn(supervisor.run().in_current_span())
}

/// How the main loop ended.
enum Ending {
    /// The child closed stdout on its own.
    Eof,
    /// A read or write failed; the child was killed.
    Broken,
    /// `Stop`, shutdown, or a dropped façade; the stop sequence ran.
    Stopped(Option<oneshot::Sender<()>>),
}

struct Supervisor {
    child: Box<dyn ClaudeChild>,
    mapper: ClaudeMapper,
    log: Arc<EventLog>,
    commands: mpsc::Receiver<ChildCommand>,
    shutdown: watch::Receiver<bool>,
    children: Children,
    pending: Pending,
    generation: u64,
    thread: ThreadId,
    context: ChildLogContext,
    pid: Option<u32>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Correlate the control requests this child was sent with their responses.
    // Source of truth: Writing a control request inserts its fresh `request_id`.
    // Structural reason: The CLI answers asynchronously, interleaved with other frames.
    // Synchronization: Owned by this supervisor task alone.
    // Invalidation/removal: The response removes its entry; child exit fails the rest.
    waiters: HashMap<String, Waiter>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Recognise late answers to handshake requests that timed out before open returned.
    // Source of truth: The handshake's `optional_request` timeouts, handed over at spawn.
    // Structural reason: The CLI still answers them, interleaved with session frames.
    // Synchronization: Owned by this supervisor task alone.
    // Invalidation/removal: The late answer removes its entry; the rest drop with the task.
    abandoned: HashSet<String>,
    early_lines: Vec<String>,
    /// stdout reached EOF.
    eof: bool,
    /// An interrupt was written during this child's life (an exit 1 after it is expected).
    interrupt_sent: bool,
    /// Events the closed log refused.
    dropped_events: u64,
    /// Why the supervisor killed the child, for the failed turn's message.
    failure: Option<&'static str>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Recognise an answer to an ask the CLI withdrew after the façade took it from `pending`.
    // Source of truth: A `control_cancel_request` for a request id no longer pending.
    // Structural reason: The façade removes the ask before the supervisor writes its answer, so
    //   the withdrawal can overtake an answer already in flight.
    // Synchronization: Owned by this supervisor task alone.
    // Invalidation/removal: The late answer removes its entry; the next turn clears the rest.
    withdrawn: HashSet<String>,
    /// Commands received while `await_control` was reading; handled before the next `select!`.
    deferred: VecDeque<ChildCommand>,
    /// A stop (its reason and reply) that arrived while `await_control` was reading.
    stop_request: Option<(&'static str, Option<oneshot::Sender<()>>)>,
    /// The permission mode the CLI holds, by the CLI's name: `default` after the handshake, then
    /// whatever the last successful `set_permission_mode` set. The mapper's session mode stays
    /// what the CLI *reports*.
    current_mode: String,
    /// The model the CLI holds: the open model, then the last one a read-back confirmed.
    current_model: ModelRef,
    /// The effort level the CLI holds, as the last read-back reported it.
    current_effort: Option<String>,
}

async fn shutdown_signal(shutdown: &mut watch::Receiver<bool>) {
    // A dropped sender (the façade is gone) is a shutdown too.
    let _ = shutdown.wait_for(|stopped| *stopped).await;
}

impl Supervisor {
    async fn run(mut self) {
        let mut ending = None;
        for line in std::mem::take(&mut self.early_lines) {
            if let Err(error) = self.dispatch_line(&line).await {
                self.broken("write_stdin", "a stdin write failed", &error);
                ending = Some(Ending::Broken);
                break;
            }
        }
        let ending = match ending {
            Some(ending) => ending,
            None => self.main_loop().await,
        };
        let (requested, stop_reply) = match ending {
            Ending::Eof | Ending::Broken => (false, None),
            Ending::Stopped(reply) => (true, reply),
        };
        let exit = self.child.wait().await;
        self.on_exit(&exit, requested);
        if let Some(reply) = stop_reply {
            let _ = reply.send(());
        }
    }

    async fn main_loop(&mut self) -> Ending {
        loop {
            if let Some((reason, reply)) = self.stop_request.take() {
                self.stop(reason).await;
                return Ending::Stopped(reply);
            }
            if let Some(command) = self.deferred.pop_front() {
                if let Err(error) = self.handle_command(command).await {
                    self.broken("write_stdin", "a stdin write failed", &error);
                    return Ending::Broken;
                }
                continue;
            }
            tokio::select! {
                biased;
                line = self.child.next_line() => match line {
                    Ok(Some(line)) => {
                        if let Err(error) = self.dispatch_line(&line).await {
                            self.broken("write_stdin", "a stdin write failed", &error);
                            return Ending::Broken;
                        }
                    }
                    Ok(None) => {
                        self.eof = true;
                        return Ending::Eof;
                    }
                    Err(error) => {
                        self.broken("read_stdout", "a stdout read failed", &error);
                        return Ending::Broken;
                    }
                },
                command = self.commands.recv() => match command {
                    Some(ChildCommand::Stop { reply }) => {
                        self.stop("stop").await;
                        return Ending::Stopped(Some(reply));
                    }
                    Some(command) => {
                        if let Err(error) = self.handle_command(command).await {
                            self.broken("write_stdin", "a stdin write failed", &error);
                            return Ending::Broken;
                        }
                    }
                    None => {
                        debug!(
                            thread_id = %self.thread,
                            harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                            action = "stop",
                            "the harness dropped this child's command channel; stopping it"
                        );
                        self.stop("harness_dropped").await;
                        return Ending::Stopped(None);
                    }
                },
                () = shutdown_signal(&mut self.shutdown) => {
                    self.stop("shutdown").await;
                    return Ending::Stopped(None);
                }
            }
        }
    }

    /// A read or write failed: the child cannot be trusted with another frame.
    fn broken(&mut self, action: &'static str, failure: &'static str, error: &HarnessError) {
        error!(
            project_id = display_opt(self.context.project_id),
            harness = display_opt(self.context.harness.as_deref()),
            thread_id = %self.thread,
            harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
            turn_id = display_opt(self.mapper.active_turn()),
            pid = display_opt(self.pid),
            action,
            error = %error,
            "claude child I/O failed; killing it"
        );
        self.failure = Some(failure);
        self.child.start_kill();
    }

    async fn dispatch_line(&mut self, line: &str) -> Result<(), HarnessError> {
        let outputs = self.mapper.map_line(line);
        self.dispatch_all(outputs).await
    }

    async fn dispatch_all(&mut self, outputs: Vec<MapperOutput>) -> Result<(), HarnessError> {
        for output in outputs {
            self.dispatch(output).await?;
        }
        Ok(())
    }

    async fn dispatch(&mut self, output: MapperOutput) -> Result<(), HarnessError> {
        match output {
            MapperOutput::Event(event) => self.append(event),
            MapperOutput::Reply(value) => self.child.write_line(&value.to_string()).await?,
            MapperOutput::ControlResponse {
                request_id,
                payload,
            } => {
                let outcome = control_outcome(&payload);
                match self.waiters.remove(&request_id) {
                    Some(waiter) => {
                        debug!(
                            thread_id = %self.thread,
                            request_id = %request_id,
                            action = "control_response",
                            success = outcome.is_ok(),
                            "control response received"
                        );
                        waiter.resolve(Ok(payload));
                    }
                    None if self.abandoned.remove(&request_id) => debug!(
                        thread_id = %self.thread,
                        request_id = %request_id,
                        action = "control_response",
                        success = outcome.is_ok(),
                        "late answer to a handshake request that timed out; ignored"
                    ),
                    None => warn!(
                        thread_id = %self.thread,
                        harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                        request_id = %request_id,
                        action = "control_response",
                        "control response for a request nobody is waiting on"
                    ),
                }
            }
            MapperOutput::PendingApproval {
                id,
                request_id,
                tool_use_id,
                tool_name,
                suggestions,
            } => {
                debug!(
                    thread_id = %self.thread,
                    request_id = %request_id,
                    tool_call_id = display_opt(tool_use_id.as_deref()),
                    tool_name = %tool_name,
                    suggestions = suggestions.len(),
                    action = "pending_approval",
                    "approval recorded until the browser answers it"
                );
                lock(&self.pending).insert_approval(
                    id,
                    PendingAsk {
                        thread: self.thread,
                        request_id,
                        tool_use_id,
                        tool_name: Some(tool_name),
                        suggestions,
                        subtype: "can_use_tool".into(),
                        input: Value::Null,
                    },
                );
            }
            MapperOutput::PendingServerRequest {
                id,
                request_id,
                subtype,
                input,
            } => {
                debug!(
                    thread_id = %self.thread,
                    request_id = %request_id,
                    subtype = %subtype,
                    action = "pending_server_request",
                    "server request recorded until the browser answers it"
                );
                lock(&self.pending).insert_server_request(
                    id,
                    PendingAsk {
                        thread: self.thread,
                        request_id,
                        tool_use_id: None,
                        tool_name: None,
                        suggestions: Vec::new(),
                        subtype,
                        input,
                    },
                );
            }
            MapperOutput::CancelRequest { request_id } => self.on_cancel_request(&request_id),
        }
        Ok(())
    }

    /// The CLI withdrew an ask (`control_cancel_request`): drop it, so a late answer is refused.
    /// A server request's card is cleared by `ServerRequestResolved`; an approval's vanishes with
    /// its turn.
    fn on_cancel_request(&mut self, request_id: &str) {
        let removed = lock(&self.pending).remove_by_request_id(self.thread, request_id);
        match removed {
            Some((kind, _)) => {
                debug!(
                    thread_id = %self.thread,
                    turn_id = display_opt(self.mapper.active_turn()),
                    request_id = %request_id,
                    action = "control_cancel_request",
                    kind = kind.as_str(),
                    "dropped the ask Claude Code withdrew"
                );
                if kind == RequestKind::ServerRequest {
                    self.append(AgentEvent::ServerRequestResolved {
                        thread: self.thread,
                        turn: self.mapper.active_turn(),
                        request_id: ServerRequestId::new(request_id.to_owned()),
                    });
                }
            }
            None => {
                // Answered already, or its answer is in flight from the façade: remembered, so an
                // answer that arrives later is logged and not written.
                debug!(
                    thread_id = %self.thread,
                    request_id = %request_id,
                    action = "control_cancel_request",
                    kind = "none",
                    "Claude Code withdrew an ask that is no longer pending"
                );
                self.withdrawn.insert(request_id.to_owned());
            }
        }
    }

    /// Append to the retained log; a closed log is reported once and counted, never ignored.
    fn append(&mut self, event: AgentEvent) {
        if self.log.append(event) {
            return;
        }
        self.dropped_events += 1;
        if self.dropped_events == 1 {
            warn!(
                thread_id = %self.thread,
                harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                turn_id = display_opt(self.mapper.active_turn()),
                action = "append_event",
                "the thread's event log is closed; dropping this child's further events"
            );
        }
    }

    /// Handle one non-`Stop` command. `Err` is a stdin write failure: the child is broken.
    async fn handle_command(&mut self, command: ChildCommand) -> Result<(), HarnessError> {
        match command {
            ChildCommand::StartTurn {
                line,
                turn,
                model,
                settings,
                reply,
            } => self.start_turn(line, turn, model, settings, reply).await,
            ChildCommand::Interrupt { reply } => {
                let request_id = new_request_id();
                if let Err(error) = self
                    .child
                    .write_line(&control_line(&request_id, &json!({"subtype": "interrupt"})))
                    .await
                {
                    let _ = reply.send(Err(error.clone()));
                    return Err(error);
                }
                self.waiters.insert(request_id.clone(), Waiter::Unit(reply));
                self.interrupt_sent = true;
                let turn = self.mapper.active_turn();
                if turn.is_some() {
                    self.mapper.note_interrupt_sent();
                }
                info!(
                    thread_id = %self.thread,
                    turn_id = display_opt(turn),
                    request_id = %request_id,
                    action = "interrupt",
                    "interrupt sent"
                );
                Ok(())
            }
            ChildCommand::Control { request, reply } => {
                let request_id = new_request_id();
                if let Err(error) = self
                    .child
                    .write_line(&control_line(&request_id, &request))
                    .await
                {
                    let _ = reply.send(Err(error.clone()));
                    return Err(error);
                }
                let subtype = request.get("subtype").and_then(Value::as_str);
                debug!(
                    thread_id = %self.thread,
                    request_id = %request_id,
                    action = "control_request",
                    subtype = display_opt(subtype),
                    "control request sent"
                );
                self.waiters.insert(request_id, Waiter::Value(reply));
                Ok(())
            }
            ChildCommand::RespondApproval {
                id,
                ask,
                decision,
                reply,
            } => self.respond_approval(id, ask, decision, reply).await,
            ChildCommand::RespondServerRequest {
                id,
                ask,
                response,
                reply,
            } => self.respond_server_request(id, ask, response, reply).await,
            ChildCommand::Compact { turn, reply } => self.compact(turn, reply).await,
            ChildCommand::Stop { reply } => {
                // The main loop intercepts `Stop`; reaching here would be a routing bug.
                warn!(
                    thread_id = %self.thread,
                    action = "stop",
                    "a stop command reached the command handler"
                );
                let _ = reply.send(());
                Ok(())
            }
        }
    }

    /// The caller of a hand-off gave up (its timeout fired) before the supervisor reached it.
    fn log_caller_gave_up(&self, turn: TurnId, action: &'static str) {
        warn!(
            project_id = display_opt(self.context.project_id),
            thread_id = %self.thread,
            harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
            turn_id = %turn,
            action,
            "the caller gave up on this turn before it was started; not writing it"
        );
    }

    /// `ThreadBusy` while a turn is active: a user message is never queued behind another.
    fn busy(&self, action: &'static str) -> Option<HarnessError> {
        let active = self.mapper.active_turn()?;
        debug!(
            thread_id = %self.thread,
            turn_id = %active,
            action,
            "refusing a turn while one is active"
        );
        Some(HarnessError::ThreadBusy {
            thread: self.thread,
        })
    }

    /// The `StartTurn` hand-off: the turn's settings, then `TurnStarted`, then the user message.
    /// `Err` is a stdin write failure: the child is broken.
    async fn start_turn(
        &mut self,
        line: String,
        turn: TurnId,
        model: ModelRef,
        settings: TurnSettings,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    ) -> Result<(), HarnessError> {
        // The caller's receiver is dropped the moment its timeout fires. Starting the turn
        // anyway would run the user's message under a turn the server never admitted, so a
        // hand-off that arrives after its caller gave up is dropped unwritten.
        if reply.is_closed() {
            self.log_caller_gave_up(turn, "start_turn");
            return Ok(());
        }
        if let Some(busy) = self.busy("start_turn") {
            let _ = reply.send(Err(busy));
            return Ok(());
        }
        if let Err(error) = self.apply_turn_settings(turn, &model, &settings).await {
            debug!(
                thread_id = %self.thread,
                turn_id = %turn,
                action = "start_turn",
                error = %error,
                "the turn's settings were not applied; the turn does not start"
            );
            let _ = reply.send(Err(error));
            return Ok(());
        }
        // Frames read while the settings were applied may have opened a turn of the CLI's own
        // (a background task's continuation), and the caller may have given up meanwhile.
        if reply.is_closed() {
            self.log_caller_gave_up(turn, "start_turn");
            return Ok(());
        }
        if let Some(busy) = self.busy("start_turn") {
            let _ = reply.send(Err(busy));
            return Ok(());
        }
        // Asks belong to a turn: a withdrawal of an earlier turn's ask can match nothing now.
        self.withdrawn.clear();
        // `TurnStarted` reaches the log before the line is written, so the server sees the turn
        // before its first frame.
        let outputs = self.mapper.begin_turn(turn, TurnKind::User);
        self.mapper.note_turn_model(model);
        for output in outputs {
            if let MapperOutput::Event(event) = output {
                self.append(event);
            }
        }
        info!(
            project_id = display_opt(self.context.project_id),
            thread_id = %self.thread,
            harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
            turn_id = %turn,
            pid = display_opt(self.pid),
            action = "start_turn",
            bytes = line.len(),
            "writing a user message"
        );
        match self.child.write_line(&line).await {
            Ok(()) => {
                let _ = reply.send(Ok(()));
                Ok(())
            }
            Err(error) => {
                let _ = reply.send(Err(error.clone()));
                Err(error)
            }
        }
    }

    /// Plan §8.2: set the turn's permission mode (every turn), then its model and effort when they
    /// differ from what the CLI holds, then read them back, all within `TURN_SETTINGS_BUDGET`.
    /// Any failure fails the hand-off; a mode already set stays set (the next turn sets its own).
    async fn apply_turn_settings(
        &mut self,
        turn: TurnId,
        model: &ModelRef,
        settings: &TurnSettings,
    ) -> Result<(), HarnessError> {
        let budget = Instant::now() + TURN_SETTINGS_BUDGET;
        let deadline = || (Instant::now() + CONTROL_TIMEOUT).min(budget);
        // Expected before the write, so the `status` the change emits is not drift.
        self.mapper.set_expected_mode(settings.mode.clone());
        let request = json!({"subtype": "set_permission_mode", "mode": settings.mode});
        match self.await_control(&request, deadline()).await {
            Ok(_) => {
                debug!(
                    thread_id = %self.thread,
                    turn_id = %turn,
                    action = "set_permission_mode",
                    mode = %settings.mode,
                    previous_mode = %self.current_mode,
                    "permission mode set for the turn"
                );
                self.current_mode = settings.mode.clone();
            }
            Err(failure) => {
                // The CLI kept its mode: a later frame reporting it is not drift.
                self.mapper.set_expected_mode(self.current_mode.clone());
                warn!(
                    project_id = display_opt(self.context.project_id),
                    thread_id = %self.thread,
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    turn_id = %turn,
                    action = "set_permission_mode",
                    mode = %settings.mode,
                    error_code = display_opt(failure.code()),
                    error = %failure,
                    "Claude Code did not set the turn's permission mode"
                );
                return Err(failure.into_error());
            }
        }

        let model_change = settings
            .model
            .as_deref()
            .filter(|requested| *requested != self.current_model.model);
        // A model switch can change the effort the CLI holds (a model without effort reports
        // none), so a requested level is sent and checked again whenever the model changes.
        let effort_change = settings.effort.as_deref().filter(|requested| {
            model_change.is_some() || Some(*requested) != self.current_effort.as_deref()
        });
        if model_change.is_none() && effort_change.is_none() {
            self.log_turn_settings(turn, false);
            return Ok(());
        }
        if let Some(requested) = model_change {
            let request = json!({"subtype": "set_model", "model": requested});
            if let Err(failure) = self.await_control(&request, deadline()).await {
                warn!(
                    thread_id = %self.thread,
                    turn_id = %turn,
                    action = "set_model",
                    model = %requested,
                    error_code = display_opt(failure.code()),
                    error = %failure,
                    "Claude Code did not switch the model"
                );
                // The picker offered a model the CLI does not know: surface its sentence.
                return Err(match failure {
                    ControlFailure::Refused { message, code }
                        if code.as_deref() == Some("catalog_unknown") =>
                    {
                        HarnessError::Unsupported(message)
                    }
                    other => other.into_error(),
                });
            }
        }
        if let Some(level) = effort_change {
            let request =
                json!({"subtype": "apply_flag_settings", "settings": {"effortLevel": level}});
            if let Err(failure) = self.await_control(&request, deadline()).await {
                warn!(
                    thread_id = %self.thread,
                    turn_id = %turn,
                    action = "apply_flag_settings",
                    effort = %level,
                    error_code = display_opt(failure.code()),
                    error = %failure,
                    "Claude Code did not take the effort level"
                );
                return Err(failure.into_error());
            }
        }

        let settings_now = match self
            .await_control(&json!({"subtype": "get_settings"}), deadline())
            .await
        {
            Ok(payload) => payload,
            Err(failure) => {
                warn!(
                    thread_id = %self.thread,
                    turn_id = %turn,
                    action = "get_settings",
                    error = %failure,
                    "could not read back the turn's model and effort"
                );
                return Err(failure.into_error());
            }
        };
        let applied = settings_now.get("applied");
        let applied_model = applied
            .and_then(|applied| applied.get("model"))
            .and_then(Value::as_str);
        // `applied.effort` is the only read-back: `effective.effortLevel` is cleared by a valid
        // `max`, and an invalid level leaves the previous one in `applied`.
        let applied_effort = applied
            .and_then(|applied| applied.get("effort"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let wanted_model = settings
            .model
            .clone()
            .unwrap_or_else(|| self.current_model.model.clone());
        let model_applied = applied_model.is_some_and(|applied| {
            applied == wanted_model || settings.resolved_model.as_deref() == Some(applied)
        });
        if !model_applied {
            let applied = applied_model.unwrap_or("no model");
            warn!(
                thread_id = %self.thread,
                turn_id = %turn,
                action = "get_settings",
                requested = %wanted_model,
                applied = %applied,
                "Claude Code applied another model than the one requested"
            );
            return Err(HarnessError::Protocol(format!(
                "Claude Code applied model {applied} instead of {wanted_model}"
            )));
        }
        if model_change.is_some() {
            // The switch went through, whatever happens to the effort below.
            self.current_model = model.clone();
        }
        // What the CLI holds now, refused or not, so the next turn compares against the truth.
        self.current_effort = applied_effort.clone();
        self.current_model.reasoning_effort =
            applied_effort.clone().map(giskard_core::model::Effort);
        if let Some(level) = effort_change
            && applied_effort.as_deref() != Some(level)
        {
            warn!(
                thread_id = %self.thread,
                turn_id = %turn,
                action = "get_settings",
                requested_effort = %level,
                applied_effort = display_opt(applied_effort.as_deref()),
                model = %wanted_model,
                "Claude Code did not take the effort level"
            );
            return Err(HarnessError::Unsupported(format!(
                "Claude Code did not accept effort {level} for {wanted_model}"
            )));
        }
        self.log_turn_settings(turn, true);
        Ok(())
    }

    /// One `turn_settings` line per started turn: the mode, model and effort the CLI holds.
    fn log_turn_settings(&self, turn: TurnId, changed: bool) {
        info!(
            project_id = display_opt(self.context.project_id),
            thread_id = %self.thread,
            harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
            turn_id = %turn,
            action = "turn_settings",
            mode = %self.current_mode,
            model = %self.current_model.model,
            effort = display_opt(self.current_effort.as_deref()),
            model_or_effort_changed = changed,
            "the turn's settings are applied"
        );
    }

    /// Write one control request and pump frames until its response or `deadline`. Every frame
    /// read meanwhile is dispatched normally (the way the stop sequence pumps), so a `status` or a
    /// late answer is never lost. On a timeout the waiter is dropped; a write failure breaks the
    /// child.
    ///
    /// The response is checked between pumps rather than raced against them in a `select!`: a
    /// pump that resolves the waiter may still be writing a mapper reply, and dropping that
    /// future would lose the write. Reads are cut into `AWAIT_POLL_SLICE`s (`next_line` is
    /// cancel-safe) so a shutdown or a `Stop` ends the wait at once, and any other command is
    /// kept for the main loop.
    async fn await_control(
        &mut self,
        request: &Value,
        deadline: Instant,
    ) -> Result<Value, ControlFailure> {
        let started = Instant::now();
        let subtype = request
            .get("subtype")
            .and_then(Value::as_str)
            .unwrap_or("control_request");
        let request_id = new_request_id();
        if let Err(error) = self
            .child
            .write_line(&control_line(&request_id, request))
            .await
        {
            self.broken("write_stdin", "a stdin write failed", &error);
            return Err(ControlFailure::Failed(error));
        }
        let (tx, mut rx) = oneshot::channel();
        self.waiters.insert(request_id.clone(), Waiter::Raw(tx));
        debug!(
            thread_id = %self.thread,
            request_id = %request_id,
            action = "control_request",
            subtype,
            "control request sent; awaiting its response"
        );
        loop {
            match rx.try_recv() {
                Ok(Ok(payload)) => {
                    return match control_outcome(&payload) {
                        Ok(response) => Ok(response),
                        Err(error) => Err(ControlFailure::Refused {
                            message: match error {
                                HarnessError::Protocol(message) => message,
                                other => other.to_string(),
                            },
                            code: payload
                                .get("error_code")
                                .and_then(Value::as_str)
                                .map(str::to_owned),
                        }),
                    };
                }
                Ok(Err(error)) => return Err(ControlFailure::Failed(error)),
                Err(oneshot::error::TryRecvError::Closed) => {
                    return Err(ControlFailure::Failed(child_stopped()));
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
            }
            if self.eof {
                self.waiters.remove(&request_id);
                return Err(ControlFailure::Failed(child_stopped()));
            }
            if self.stop_requested_while_waiting() {
                self.waiters.remove(&request_id);
                debug!(
                    thread_id = %self.thread,
                    request_id = %request_id,
                    action = subtype,
                    "stopping the child; no longer waiting for this control request"
                );
                return Err(ControlFailure::Failed(HarnessError::Transport(
                    "claude child is stopping".into(),
                )));
            }
            if Instant::now() >= deadline {
                self.waiters.remove(&request_id);
                let waited = deadline.saturating_duration_since(started);
                warn!(
                    thread_id = %self.thread,
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    request_id = %request_id,
                    action = subtype,
                    timeout_ms = waited.as_millis() as u64,
                    "Claude Code did not answer a control request in time"
                );
                return Err(ControlFailure::Failed(HarnessError::Timeout(format!(
                    "claude did not answer {subtype} within {:.1} s",
                    waited.as_secs_f64()
                ))));
            }
            let slice = (Instant::now() + AWAIT_POLL_SLICE).min(deadline);
            self.pump_until(slice, "await_control").await;
        }
    }

    /// While `await_control` waits: take a shutdown or a `Stop` as a stop request for the main
    /// loop (`true`), and keep any other command for after the wait.
    fn stop_requested_while_waiting(&mut self) -> bool {
        if self.stop_request.is_some() {
            return true;
        }
        if *self.shutdown.borrow() {
            self.stop_request = Some(("shutdown", None));
            return true;
        }
        loop {
            match self.commands.try_recv() {
                Ok(ChildCommand::Stop { reply }) => {
                    self.stop_request = Some(("stop", Some(reply)));
                    return true;
                }
                Ok(command) => self.deferred.push_back(command),
                Err(mpsc::error::TryRecvError::Empty) => return false,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.stop_request = Some(("harness_dropped", None));
                    return true;
                }
            }
        }
    }

    /// Answer a `can_use_tool` ask (plan §9.3). `Err` is a stdin write failure.
    async fn respond_approval(
        &mut self,
        id: ApprovalId,
        ask: PendingAsk,
        decision: ApprovalDecision,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    ) -> Result<(), HarnessError> {
        if self.withdrawn.remove(&ask.request_id) {
            info!(
                thread_id = %self.thread,
                request_id = %ask.request_id,
                action = "respond_approval",
                "late answer to an approval Claude Code withdrew; not sent"
            );
            let _ = reply.send(Err(HarnessError::Protocol(format!(
                "approval {id} was withdrawn by Claude Code"
            ))));
            return Ok(());
        }
        if reply.is_closed() {
            // Nobody learns whether this answer was written: keep the ask answerable instead.
            warn!(
                thread_id = %self.thread,
                request_id = %ask.request_id,
                action = "respond_approval",
                "the caller gave up before the answer was written; the approval stays pending"
            );
            lock(&self.pending).insert_approval(id, ask);
            return Ok(());
        }
        let (response, rules) = match approval_response(&ask, &decision) {
            Ok(built) => built,
            Err(error) => {
                lock(&self.pending).insert_approval(id, ask);
                let _ = reply.send(Err(error));
                return Ok(());
            }
        };
        let decision_name = match &decision {
            ApprovalDecision::Accept => "accept",
            ApprovalDecision::AcceptForSession => "accept_for_session",
            ApprovalDecision::Decline => "decline",
            ApprovalDecision::Cancel => "cancel",
            ApprovalDecision::AcceptWithExecPolicyAmendment { .. } => {
                "accept_with_exec_policy_amendment"
            }
        };
        let tool_name = ask.tool_name.as_deref().unwrap_or("?");
        if matches!(decision, ApprovalDecision::AcceptForSession) && rules == 0 {
            // Plan §9.3 degradation: no rule to grant for the session, so this is a plain allow.
            warn!(
                thread_id = %self.thread,
                request_id = %ask.request_id,
                action = "accept_for_session_degraded",
                tool_name,
                suggestions = ask.suggestions.len(),
                "the ask carried no addRules suggestion; allowing this use only"
            );
        }
        let turn = self.mapper.active_turn();
        if matches!(
            decision,
            ApprovalDecision::Decline | ApprovalDecision::Cancel
        ) && let Some(tool_use_id) = &ask.tool_use_id
        {
            self.mapper.note_denied(tool_use_id);
        }
        if matches!(decision, ApprovalDecision::Cancel) {
            // The deny carries `interrupt: true`: the turn ends `aborted_tools`, which must
            // persist as `Interrupted`, and the exit after it is expected.
            self.interrupt_sent = true;
            if turn.is_some() {
                self.mapper.note_interrupt_sent();
            }
        }
        if let Err(error) = self
            .child
            .write_line(&control_response_line(&ask.request_id, response))
            .await
        {
            let _ = reply.send(Err(error.clone()));
            return Err(error);
        }
        info!(
            project_id = display_opt(self.context.project_id),
            thread_id = %self.thread,
            turn_id = display_opt(turn),
            request_id = %ask.request_id,
            action = "respond_approval",
            tool_name,
            decision = decision_name,
            rules,
            "approval answered"
        );
        let _ = reply.send(Ok(()));
        Ok(())
    }

    /// Answer an `AskUserQuestion` or another inbound control request, then clear its card with
    /// `ServerRequestResolved`. `Err` is a stdin write failure.
    async fn respond_server_request(
        &mut self,
        id: ServerRequestId,
        ask: PendingAsk,
        response: ServerRequestResponse,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    ) -> Result<(), HarnessError> {
        if self.withdrawn.remove(&ask.request_id) {
            info!(
                thread_id = %self.thread,
                request_id = %ask.request_id,
                action = "respond_server_request",
                "late answer to a server request Claude Code withdrew; not sent"
            );
            // The withdrawal found nothing pending, so its card is cleared here.
            self.append(AgentEvent::ServerRequestResolved {
                thread: self.thread,
                turn: self.mapper.active_turn(),
                request_id: id.clone(),
            });
            let _ = reply.send(Err(HarnessError::Protocol(format!(
                "server request {id} was withdrawn by Claude Code"
            ))));
            return Ok(());
        }
        if reply.is_closed() {
            warn!(
                thread_id = %self.thread,
                request_id = %ask.request_id,
                action = "respond_server_request",
                "the caller gave up before the answer was written; the request stays pending"
            );
            lock(&self.pending).insert_server_request(id, ask);
            return Ok(());
        }
        let line = match server_request_line(&ask, &response) {
            Ok(line) => line,
            Err(error) => {
                warn!(
                    thread_id = %self.thread,
                    request_id = %ask.request_id,
                    action = "respond_server_request",
                    subtype = %ask.subtype,
                    error = %error,
                    "the browser's answer does not fit the request; it stays pending"
                );
                lock(&self.pending).insert_server_request(id, ask);
                let _ = reply.send(Err(error));
                return Ok(());
            }
        };
        if let Err(error) = self.child.write_line(&line).await {
            let _ = reply.send(Err(error.clone()));
            return Err(error);
        }
        let turn = self.mapper.active_turn();
        info!(
            project_id = display_opt(self.context.project_id),
            thread_id = %self.thread,
            turn_id = display_opt(turn),
            request_id = %ask.request_id,
            action = "respond_server_request",
            subtype = %ask.subtype,
            outcome = match response {
                ServerRequestResponse::Result { .. } => "result",
                ServerRequestResponse::Error { .. } => "error",
            },
            "server request answered"
        );
        self.append(AgentEvent::ServerRequestResolved {
            thread: self.thread,
            turn,
            request_id: id,
        });
        let _ = reply.send(Ok(()));
        Ok(())
    }

    /// `/compact` as a compaction turn. No per-turn settings are sent: compaction runs no tool,
    /// and the mode in force is the previous turn's. `Err` is a stdin write failure.
    async fn compact(
        &mut self,
        turn: TurnId,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    ) -> Result<(), HarnessError> {
        if reply.is_closed() {
            self.log_caller_gave_up(turn, "compact");
            return Ok(());
        }
        if let Some(busy) = self.busy("compact") {
            let _ = reply.send(Err(busy));
            return Ok(());
        }
        self.withdrawn.clear();
        for output in self.mapper.begin_turn(turn, TurnKind::Compaction) {
            if let MapperOutput::Event(event) = output {
                self.append(event);
            }
        }
        info!(
            project_id = display_opt(self.context.project_id),
            thread_id = %self.thread,
            harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
            turn_id = %turn,
            pid = display_opt(self.pid),
            action = "compact",
            "writing /compact"
        );
        let line = json!({"type": "user", "message": {
            "role": "user",
            "content": [{"type": "text", "text": "/compact"}],
        }})
        .to_string();
        match self.child.write_line(&line).await {
            Ok(()) => {
                let _ = reply.send(Ok(()));
                Ok(())
            }
            Err(error) => {
                let _ = reply.send(Err(error.clone()));
                Err(error)
            }
        }
    }

    /// Read and dispatch one line before `deadline`. `false` when the deadline passed first.
    async fn pump_until(&mut self, deadline: Instant, action: &'static str) -> bool {
        match tokio::time::timeout_at(deadline, self.child.next_line()).await {
            Err(_) => false,
            Ok(Ok(Some(line))) => {
                if let Err(error) = self.dispatch_line(&line).await {
                    debug!(
                        thread_id = %self.thread,
                        action,
                        "a reply could not be written while pumping frames"
                    );
                    self.broken("write_stdin", "a stdin write failed", &error);
                }
                true
            }
            Ok(Ok(None)) => {
                self.eof = true;
                true
            }
            Ok(Err(error)) => {
                self.broken("read_stdout", "a stdout read failed", &error);
                self.eof = true;
                true
            }
        }
    }

    /// The stop sequence: interrupt a live turn, close stdin, kill on the grace timeout.
    ///
    /// SIGTERM is deliberately not used: it leaves the turn without a `result`.
    async fn stop(&mut self, reason: &'static str) {
        if self.eof {
            return;
        }
        if let Some(turn) = self.mapper.active_turn() {
            let started = Instant::now();
            let request_id = new_request_id();
            match self
                .child
                .write_line(&control_line(&request_id, &json!({"subtype": "interrupt"})))
                .await
            {
                Ok(()) => {
                    self.waiters.insert(request_id.clone(), Waiter::Stop);
                    self.interrupt_sent = true;
                    self.mapper.note_interrupt_sent();
                    let deadline = started + STOP_INTERRUPT_GRACE;
                    while self.mapper.active_turn().is_some() && !self.eof {
                        if !self.pump_until(deadline, "stop").await {
                            break;
                        }
                    }
                    info!(
                        thread_id = %self.thread,
                        turn_id = %turn,
                        request_id = %request_id,
                        action = "stop_interrupt",
                        reason,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        turn_closed = self.mapper.active_turn().is_none(),
                        "interrupted the live turn before stopping"
                    );
                }
                Err(error) => warn!(
                    thread_id = %self.thread,
                    turn_id = %turn,
                    action = "stop_interrupt",
                    reason,
                    error = %error,
                    "could not interrupt the live turn before stopping"
                ),
            }
        }
        if self.eof {
            return;
        }
        self.child.close_stdin();
        let deadline = Instant::now() + STOP_EXIT_GRACE;
        while !self.eof {
            if !self.pump_until(deadline, "stop").await {
                warn!(
                    project_id = display_opt(self.context.project_id),
                    thread_id = %self.thread,
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    pid = display_opt(self.pid),
                    action = "stop_kill",
                    reason,
                    grace_ms = STOP_EXIT_GRACE.as_millis() as u64,
                    "claude did not exit after stdin closed; killing it"
                );
                self.child.start_kill();
                break;
            }
        }
    }

    /// Child-exit handling, whatever ended the child.
    fn on_exit(&mut self, exit: &ChildExit, requested: bool) {
        let mut described = exit.describe();
        if let Some(failure) = self.failure {
            described = format!("{described}, after {failure}");
        }
        if self.mapper.active_turn().is_some() {
            for output in self.mapper.child_exited(&described) {
                if let MapperOutput::Event(event) = output {
                    self.append(event);
                }
            }
        }
        self.log.close();
        for (request_id, waiter) in self.waiters.drain() {
            if !matches!(waiter, Waiter::Stop) {
                warn!(
                    thread_id = %self.thread,
                    request_id = %request_id,
                    action = "control_request",
                    "control request unanswered when the child stopped"
                );
            }
            waiter.resolve(Err(child_stopped()));
        }
        let live_children = {
            let mut children = lock(&self.children);
            if children
                .get(&self.thread)
                .is_some_and(|handle| handle.generation == self.generation)
            {
                children.remove(&self.thread);
            }
            children.len()
        };
        let pending_dropped = lock(&self.pending).remove_thread(self.thread);
        let expected =
            requested && (exit.code == Some(0) || (exit.code == Some(1) && self.interrupt_sent));
        macro_rules! exit_line {
            ($level:ident, $message:literal) => {
                $level!(
                    project_id = display_opt(self.context.project_id),
                    harness = display_opt(self.context.harness.as_deref()),
                    thread_id = %self.thread,
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    pid = display_opt(self.pid),
                    action = "child_exited",
                    exit_code = display_opt(exit.code),
                    signal = display_opt(exit.signal),
                    stderr_tail = ?exit.stderr_tail,
                    requested,
                    live_children,
                    pending_dropped,
                    dropped_events = self.dropped_events,
                    $message
                )
            };
        }
        if expected {
            exit_line!(info, "claude child exited");
        } else {
            exit_line!(warn, "claude child exited unexpectedly");
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    //! `ScriptedChild`: an in-process `ClaudeChild` driven by a script of steps.

    use std::collections::VecDeque;

    use async_trait::async_trait;
    use tokio::sync::mpsc;

    use super::*;
    use crate::process::{MAX_STDOUT_LINE_BYTES, overlong_line};

    /// What a script does when its trigger fires.
    pub(crate) enum Action {
        /// Emit these stdout lines.
        Emit(Vec<String>),
        /// Emit a fixture's stdout lines, without its `control_response` lines (those belong to
        /// the scripted replies) and without `skip_types`.
        EmitFixture {
            name: &'static str,
            skip_types: &'static [&'static str],
        },
        /// Emit the first `count` lines `EmitFixture` would.
        EmitFixturePrefix { name: &'static str, count: usize },
        /// Emit the lines `EmitFixture` would, from index `from` on.
        EmitFixtureFrom { name: &'static str, from: usize },
        /// Answer the triggering control request with this success payload.
        Respond(Value),
        /// Answer the triggering control request with an error.
        RespondError(&'static str),
        /// Answer the triggering control request with an error carrying the CLI's `error_code`.
        RespondErrorCode {
            error: &'static str,
            code: &'static str,
        },
        /// Answer the triggering control request with this success payload, but only when the
        /// next stdin line arrives: a late answer.
        RespondOnNextWrite(Value),
        /// Answer the triggering control request with this success payload after `delay`.
        RespondAfter { delay: Duration, payload: Value },
        /// Every later stdin write fails, as for a CLI whose stdin pipe broke.
        BreakStdin,
        /// Close stdout and exit with this code and stderr.
        Exit { code: i32, stderr: Vec<String> },
    }

    /// Puts stdout lines into a scripted child from the test itself.
    pub(crate) struct Injector(mpsc::UnboundedSender<Out>);

    impl Injector {
        pub(crate) fn emit(&self, line: String) {
            let _ = self.0.send(Out::Line(line));
        }
    }

    pub(crate) type Matcher = Box<dyn Fn(&Value) -> bool + Send>;

    pub(crate) enum Step {
        /// Wait for a stdin line matching the predicate, then run the actions.
        OnStdin(Matcher, Vec<Action>),
    }

    /// What a test can inspect after the child moved into the harness.
    #[derive(Default)]
    pub(crate) struct ScriptRecord {
        pub written: Vec<String>,
        pub stdin_closed: bool,
        pub killed: bool,
    }

    enum Out {
        Line(String),
        Eof,
    }

    pub(crate) struct ScriptedChild {
        steps: VecDeque<Step>,
        tx: mpsc::UnboundedSender<Out>,
        rx: mpsc::UnboundedReceiver<Out>,
        record: Arc<Mutex<ScriptRecord>>,
        exit: Option<ChildExit>,
        closed: bool,
        exit_on_eof: bool,
        /// `wait` never returns, as for a process the kernel cannot reap.
        hang_on_wait: bool,
        /// `write_line` waits while this is `false`, as for a CLI that stopped reading stdin.
        gate: watch::Receiver<bool>,
        /// A late answer `RespondOnNextWrite` holds back until the next stdin line.
        deferred: Option<String>,
        /// Answer every `set_permission_mode` the script does not handle itself by echoing the
        /// mode, as the CLI does: the handshake and every turn send one.
        echo_modes: bool,
        /// The exit code once stdin closes.
        eof_exit_code: i32,
    }

    pub(crate) fn fixture_lines(name: &str) -> Vec<String> {
        let path = format!(
            "{}/tests/fixtures/{name}.out.jsonl",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("no fixture {path}"))
            .lines()
            .filter(|line| {
                serde_json::from_str::<Value>(line).unwrap()["type"] != "control_response"
            })
            .map(str::to_owned)
            .collect()
    }

    /// A stdin line that is a control request of this subtype.
    pub(crate) fn control(subtype: &'static str) -> Matcher {
        Box::new(move |value| {
            value["type"] == "control_request" && value["request"]["subtype"] == subtype
        })
    }

    /// A stdin line that is a user message.
    pub(crate) fn user() -> Matcher {
        Box::new(|value| value["type"] == "user")
    }

    impl ScriptedChild {
        pub(crate) fn new(steps: Vec<Step>) -> (Self, Arc<Mutex<ScriptRecord>>) {
            let (tx, rx) = mpsc::unbounded_channel();
            let record = Arc::new(Mutex::new(ScriptRecord::default()));
            let child = Self {
                steps: steps.into(),
                tx,
                rx,
                record: record.clone(),
                exit: None,
                closed: false,
                exit_on_eof: true,
                hang_on_wait: false,
                gate: watch::channel(true).1,
                deferred: None,
                echo_modes: true,
                eof_exit_code: 0,
            };
            (child, record)
        }

        /// Exit with `code` once stdin closes, as the CLI does after an interrupted turn.
        pub(crate) fn exiting_on_eof_with(mut self, code: i32) -> Self {
            self.eof_exit_code = code;
            self
        }

        /// A handle that emits stdout lines without a stdin trigger.
        pub(crate) fn injector(&self) -> Injector {
            Injector(self.tx.clone())
        }

        /// `set_permission_mode` requests are only answered by the script's own steps.
        pub(crate) fn without_mode_echo(mut self) -> Self {
            self.echo_modes = false;
            self
        }

        /// Writes block while the returned sender holds `false`.
        pub(crate) fn gated(mut self) -> (Self, watch::Sender<bool>) {
            let (open, gate) = watch::channel(true);
            self.gate = gate;
            (self, open)
        }

        /// The child ignores EOF on stdin, as a wedged CLI would.
        pub(crate) fn ignoring_eof(mut self) -> Self {
            self.exit_on_eof = false;
            self
        }

        /// `wait` never returns, so the supervisor cannot finish its exit handling.
        pub(crate) fn hanging_on_wait(mut self) -> Self {
            self.hang_on_wait = true;
            self
        }

        fn finish(&mut self, exit: ChildExit) {
            if self.exit.is_none() {
                self.exit = Some(exit);
                let _ = self.tx.send(Out::Eof);
            }
        }

        fn run(&mut self, actions: Vec<Action>, trigger: &Value) {
            for action in actions {
                match action {
                    Action::Emit(lines) => {
                        for line in lines {
                            let _ = self.tx.send(Out::Line(line));
                        }
                    }
                    Action::EmitFixture { name, skip_types } => {
                        for line in fixture_lines(name) {
                            let value: Value = serde_json::from_str(&line).unwrap();
                            if skip_types.iter().any(|skip| value["type"] == *skip) {
                                continue;
                            }
                            let _ = self.tx.send(Out::Line(line));
                        }
                    }
                    Action::EmitFixturePrefix { name, count } => {
                        for line in fixture_lines(name).into_iter().take(count) {
                            let _ = self.tx.send(Out::Line(line));
                        }
                    }
                    Action::EmitFixtureFrom { name, from } => {
                        for line in fixture_lines(name).into_iter().skip(from) {
                            let _ = self.tx.send(Out::Line(line));
                        }
                    }
                    Action::Respond(payload) => {
                        let line = json!({"type": "control_response", "response": {
                            "subtype": "success",
                            "request_id": trigger["request_id"],
                            "response": payload,
                        }});
                        let _ = self.tx.send(Out::Line(line.to_string()));
                    }
                    Action::RespondError(message) => {
                        let line = json!({"type": "control_response", "response": {
                            "subtype": "error",
                            "request_id": trigger["request_id"],
                            "error": message,
                        }});
                        let _ = self.tx.send(Out::Line(line.to_string()));
                    }
                    Action::RespondErrorCode { error, code } => {
                        let line = json!({"type": "control_response", "response": {
                            "subtype": "error",
                            "request_id": trigger["request_id"],
                            "error": error,
                            "error_code": code,
                        }});
                        let _ = self.tx.send(Out::Line(line.to_string()));
                    }
                    Action::RespondOnNextWrite(payload) => {
                        let line = json!({"type": "control_response", "response": {
                            "subtype": "success",
                            "request_id": trigger["request_id"],
                            "response": payload,
                        }});
                        self.deferred = Some(line.to_string());
                    }
                    Action::RespondAfter { delay, payload } => {
                        let line = json!({"type": "control_response", "response": {
                            "subtype": "success",
                            "request_id": trigger["request_id"],
                            "response": payload,
                        }});
                        let tx = self.tx.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(delay).await;
                            let _ = tx.send(Out::Line(line.to_string()));
                        });
                    }
                    Action::BreakStdin => self.closed = true,
                    Action::Exit { code, stderr } => self.finish(ChildExit {
                        code: Some(code),
                        signal: None,
                        stderr_tail: stderr,
                    }),
                }
            }
        }
    }

    #[async_trait]
    impl ClaudeChild for ScriptedChild {
        async fn write_line(&mut self, line: &str) -> Result<(), HarnessError> {
            let _ = self.gate.wait_for(|open| *open).await;
            if self.closed || self.exit.is_some() {
                return Err(HarnessError::Transport("claude stdin is closed".into()));
            }
            if let Some(late) = self.deferred.take() {
                let _ = self.tx.send(Out::Line(late));
            }
            lock(&self.record).written.push(line.to_owned());
            let value: Value = serde_json::from_str(line).unwrap();
            let matched = match self.steps.front() {
                Some(Step::OnStdin(matcher, _)) => matcher(&value),
                None => false,
            };
            if matched && let Some(Step::OnStdin(_, actions)) = self.steps.pop_front() {
                self.run(actions, &value);
            } else if self.echo_modes && control("set_permission_mode")(&value) {
                let mode = value["request"]["mode"].clone();
                self.run(vec![Action::Respond(json!({ "mode": mode }))], &value);
            }
            Ok(())
        }

        async fn next_line(&mut self) -> Result<Option<String>, HarnessError> {
            match self.rx.recv().await {
                Some(Out::Line(line)) if line.len() > MAX_STDOUT_LINE_BYTES => {
                    Err(overlong_line(line.len()))
                }
                Some(Out::Line(line)) => Ok(Some(line)),
                Some(Out::Eof) | None => {
                    // EOF is sticky, like a real pipe.
                    let _ = self.tx.send(Out::Eof);
                    Ok(None)
                }
            }
        }

        fn close_stdin(&mut self) {
            if self.closed {
                return;
            }
            self.closed = true;
            lock(&self.record).stdin_closed = true;
            if self.exit_on_eof {
                self.finish(ChildExit {
                    code: Some(self.eof_exit_code),
                    signal: None,
                    stderr_tail: Vec::new(),
                });
            }
        }

        async fn wait(&mut self) -> ChildExit {
            if self.hang_on_wait {
                std::future::pending::<()>().await;
            }
            self.exit.clone().unwrap_or_default()
        }

        fn start_kill(&mut self) {
            lock(&self.record).killed = true;
            self.finish(ChildExit {
                code: None,
                signal: Some(9),
                stderr_tail: Vec::new(),
            });
        }

        fn pid(&self) -> Option<u32> {
            Some(4242)
        }
    }
}
