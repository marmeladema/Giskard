//! The supervisor: one task per `claude` child.
//!
//! The task is the single owner of the child process, the [`ClaudeMapper`] and the pending
//! control-request waiters, and the only writer of the thread's retained [`EventLog`] while its
//! child lives (a reaped child's respawn appends to the same log). Nothing else touches them: the
//! façade reaches the task only through its command channel, so there is no lock around the
//! mapper or the stdin handle (the `CodexInstance` rule of `AGENTS.md`, applied per child).

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
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
use crate::mapper::{ClaudeMapper, MapperOutput, RouteLookup, TurnKind};
use crate::process::{ChildExit, ChildLogContext, ClaudeChild, LaunchMode};

/// How long a background command's terminal `task_updated` may wait for the `task_notification`
/// that carries its output file before the mapper completes it from the update alone. The
/// notification followed within ~70 ms in every recording.
pub(crate) const NOTIFICATION_GRACE: Duration = Duration::from_secs(2);
/// How long one request of a turn's settings may take (each stage of the hand-off).
pub(crate) const CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a turn's settings (mode, model, effort, read-back) may take in all. It stays under the
/// façade's 30 s `start_turn` budget, so the supervisor's own timeout, naming the request left
/// unanswered, is what the caller sees.
pub(crate) const TURN_SETTINGS_BUDGET: Duration = Duration::from_secs(25);
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
        /// A sentence appended as `AgentEvent::Notice` right after `TurnStarted` (a respawn that
        /// lost its transcript).
        notice: Option<String>,
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
    /// Stop the sub-agent of route `thread` (`stop_task`): the sub-agent thread's `interrupt`.
    StopTask {
        thread: ThreadId,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    },
    /// Stop the background command of `local_bash` task `task_id` (`stop_task`):
    /// `terminate_command`.
    StopBackgroundCommand {
        task_id: String,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    },
    /// Run `/compact` as a compaction turn.
    Compact {
        turn: TurnId,
        /// As `StartTurn`'s.
        notice: Option<String>,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    },
    Stop {
        reply: oneshot::Sender<()>,
    },
}

impl ChildCommand {
    fn name(&self) -> &'static str {
        match self {
            ChildCommand::StartTurn { .. } => "start_turn",
            ChildCommand::Interrupt { .. } => "interrupt",
            ChildCommand::Control { .. } => "control",
            ChildCommand::RespondApproval { .. } => "respond_approval",
            ChildCommand::RespondServerRequest { .. } => "respond_server_request",
            ChildCommand::StopTask { .. } => "stop_task",
            ChildCommand::StopBackgroundCommand { .. } => "terminate_command",
            ChildCommand::Compact { .. } => "compact",
            ChildCommand::Stop { .. } => "stop",
        }
    }
}

/// One primary thread this instance holds: its session, its retained log, and its child while
/// one runs. The entry outlives a reaped child; only delete, archive, an unexpected child exit and
/// shutdown remove it.
pub(crate) struct ThreadEntry {
    /// The session id: `--session-id` at the first spawn, `--resume` on every respawn.
    pub harness_thread_id: String,
    /// One log for the thread's whole life here: a respawned child appends to it.
    pub log: Arc<EventLog>,
    pub workspace_root: PathBuf,
    /// The model the CLI holds or held: the open model, then what the reaped supervisor held. A
    /// turn without a model override runs on it, and a respawn launches with it.
    pub model: ModelRef,
    pub child: Option<ChildHandle>,
    /// Held by the one caller respawning the thread's child, so two callers never start two
    /// `claude --resume` of one session.
    pub respawn: Arc<tokio::sync::Mutex<()>>,
    /// The `apiKeySource` notice the thread already showed; a respawned mapper is seeded with it
    /// so the notice is not repeated after every reap.
    pub api_key_source_noticed: Option<String>,
}

/// The façade's view of one live child.
pub(crate) struct ChildHandle {
    pub commands: mpsc::Sender<ChildCommand>,
    pub task: JoinHandle<()>,
    /// The mode the child was launched with: `full_access` needs `Bypass`.
    pub launch_mode: LaunchMode,
    /// Distinguishes this child from a later one for the same thread, so a supervisor that ends
    /// after its thread was reopened or respawned never touches the new child.
    pub generation: u64,
}

pub(crate) type Threads = Arc<Mutex<HashMap<ThreadId, ThreadEntry>>>;
pub(crate) type Routes = Arc<Mutex<HashMap<ThreadId, RouteHandle>>>;

/// The façade's view of one sub-agent route: a live one, published by its child's supervisor, or
/// a cold one, bound by `claim_native_thread` for a route whose session is gone or left behind by
/// its child's exit. Its log stays open until the sub-agent thread's delete or archive, or
/// shutdown.
pub(crate) struct RouteHandle {
    /// `task:<tool_use_id>`.
    pub harness_thread_id: String,
    pub log: Arc<EventLog>,
    /// The primary thread whose child carries this route; `None` for a cold route.
    /// It is also the child `list_mcp_servers` asks for a sub-agent hint.
    pub owner: Option<ThreadId>,
    pub commands: Option<mpsc::Sender<ChildCommand>>,
    pub parent_harness_thread_id: Option<String>,
    pub agent_name: Option<String>,
    pub model: Option<ModelRef>,
    /// The publishing child's generation, so a supervisor never removes a route it did not
    /// publish; `None` for a cold route.
    pub generation: Option<u64>,
}
pub(crate) type Pending = Arc<Mutex<PendingRequests>>;

/// Lock a std mutex whose guarded maps stay consistent even if a holder panicked.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One ask the CLI published and nothing has answered yet.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PendingAsk {
    /// The thread the ask was published on: the primary's, or one of its sub-agent routes'.
    pub thread: ThreadId,
    /// The primary thread whose child answers the ask.
    pub owner: ThreadId,
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

    /// Remove the ask of `owner`'s child whose CLI request id is `request_id`, whichever map
    /// holds it.
    pub fn remove_by_request_id(
        &mut self,
        owner: ThreadId,
        request_id: &str,
    ) -> Option<(RequestKind, PendingAsk)> {
        let approval = self
            .approvals
            .iter()
            .find(|(_, ask)| ask.owner == owner && ask.request_id == request_id)
            .map(|(id, _)| id.clone());
        if let Some(ask) = approval.and_then(|id| self.approvals.remove(&id)) {
            return Some((RequestKind::Approval, ask));
        }
        let request = self
            .server_requests
            .iter()
            .find(|(_, ask)| ask.owner == owner && ask.request_id == request_id)
            .map(|(id, _)| id.clone());
        request
            .and_then(|id| self.server_requests.remove(&id))
            .map(|ask| (RequestKind::ServerRequest, ask))
    }

    /// How many asks `owner`'s child published that nothing answered yet, its routes' included.
    pub fn count_owner(&self, owner: ThreadId) -> usize {
        self.approvals
            .values()
            .chain(self.server_requests.values())
            .filter(|ask| ask.owner == owner)
            .count()
    }

    /// Drop every ask `owner`'s child published, its routes' included; returns how many.
    pub fn remove_owner(&mut self, owner: ThreadId) -> usize {
        let before = self.len();
        self.approvals.retain(|_, ask| ask.owner != owner);
        self.server_requests.retain(|_, ask| ask.owner != owner);
        before - self.len()
    }

    /// Drop every ask published on one thread (a sub-agent route); returns how many.
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

/// A mapper output's name, for logs.
fn output_kind(output: &MapperOutput) -> &'static str {
    match output {
        MapperOutput::Event(_) => "event",
        MapperOutput::Reply(_) => "reply",
        MapperOutput::ControlResponse { .. } => "control_response",
        MapperOutput::PendingApproval { .. } => "pending_approval",
        MapperOutput::PendingServerRequest { .. } => "pending_server_request",
        MapperOutput::CancelRequest { .. } => "cancel_request",
        MapperOutput::RouteOpened { .. } => "route_opened",
        MapperOutput::RouteClosed { .. } => "route_closed",
    }
}

/// The thread an event belongs to. `giskard-core` has no accessor for it.
pub(crate) fn event_thread(event: &AgentEvent) -> ThreadId {
    match event {
        AgentEvent::ThreadOpened { thread, .. }
        | AgentEvent::TurnStarted { thread, .. }
        | AgentEvent::TurnUsageUpdated { thread, .. }
        | AgentEvent::ItemStarted { thread, .. }
        | AgentEvent::ItemDelta { thread, .. }
        | AgentEvent::ItemCompleted { thread, .. }
        | AgentEvent::DiffUpdated { thread, .. }
        | AgentEvent::ApprovalRequested { thread, .. }
        | AgentEvent::ServerRequestReceived { thread, .. }
        | AgentEvent::ServerRequestResolved { thread, .. }
        | AgentEvent::TurnCompleted { thread, .. }
        | AgentEvent::Error { thread, .. }
        | AgentEvent::Notice { thread, .. } => *thread,
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

/// `terminate_command` for a task id that names no running background command. The server reads
/// this wording as "unmanaged" (`harness_error_means_command_unmanaged`), as it does Codex's.
pub(crate) fn no_background_command(task_id: &str) -> HarnessError {
    HarnessError::Transport(format!("no background command with task id {task_id}"))
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
    /// A sub-agent's `stop_task`: its answer is logged, then replied.
    StopTask(StopTaskWaiter),
    /// The stop sequence's own interrupt: nobody awaits the response.
    Stop,
}

/// What the answer to a `stop_task` is logged with and replied to.
struct StopTaskWaiter {
    /// The sub-agent thread.
    thread: ThreadId,
    task_id: String,
    harness_thread_id: Option<String>,
    reply: oneshot::Sender<Result<(), HarnessError>>,
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
            Waiter::StopTask(stop) => {
                let _ = stop.reply.send(
                    payload
                        .and_then(|payload| control_outcome(&payload))
                        .map(|_| ()),
                );
            }
            Waiter::Stop => {}
        }
    }
}

/// Why one stage of a turn's settings got no success payload.
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

/// One stage's response: its success payload, or the CLI's refusal with its `error_code`.
fn setup_outcome(payload: &Value) -> Result<Value, ControlFailure> {
    control_outcome(payload).map_err(|error| ControlFailure::Refused {
        message: match error {
            HarnessError::Protocol(message) => message,
            other => other.to_string(),
        },
        code: payload
            .get("error_code")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

/// The `StartTurn` hand-off between its first settings request and the user line: the main loop
/// advances it on each control response, fails it on its stage deadline, and runs its last step
/// (the write) when the read-back is in. At most one is in flight; a second `StartTurn` or a
/// `Compact` meanwhile is `ThreadBusy`.
struct TurnSetup {
    line: String,
    turn: TurnId,
    model: ModelRef,
    settings: TurnSettings,
    reply: oneshot::Sender<Result<(), HarnessError>>,
    /// A sentence to append as `AgentEvent::Notice` right after `TurnStarted` (a respawn that
    /// lost its transcript).
    notice: Option<String>,
    stage: SetupStage,
    /// The outstanding request's id; checked before `waiters` when a response arrives.
    request_id: String,
    /// `started + TURN_SETTINGS_BUDGET`.
    budget: Instant,
    /// When the outstanding request was written.
    sent: Instant,
    /// The outstanding request's deadline: `min(sent + CONTROL_TIMEOUT, budget)`.
    deadline: Instant,
    /// Decided when the mode is in: the `set_model` to send, and the effort to send.
    model_change: Option<String>,
    effort_change: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetupStage {
    /// `set_permission_mode`, sent on every turn.
    Mode,
    /// `set_model`, when the requested model differs from the one the CLI holds.
    Model,
    /// `apply_flag_settings {effortLevel}`, when the effort differs or the model changed.
    Effort,
    /// `get_settings`, the read-back of `applied.model` and `applied.effort`.
    ReadBack,
}

impl SetupStage {
    fn subtype(self) -> &'static str {
        match self {
            SetupStage::Mode => "set_permission_mode",
            SetupStage::Model => "set_model",
            SetupStage::Effort => "apply_flag_settings",
            SetupStage::ReadBack => "get_settings",
        }
    }
}

/// What the main loop is doing besides reading frames.
enum Phase {
    /// Reading frames and serving commands.
    Serving,
    /// The stop sequence. No command is served: each is refused at once with `child_stopped`.
    Stopping(Stopping),
}

struct Stopping {
    /// `stop`, `shutdown`, `harness_dropped` or `idle`.
    reason: &'static str,
    /// Every `Stop` that arrived; all are answered when the child has exited.
    replies: Vec<oneshot::Sender<()>>,
    stage: StopStage,
    /// The child is reaped for idleness: its thread keeps its entry and its log stays open.
    reaped: bool,
}

enum StopStage {
    /// `interrupt` was written for the live turn; waiting for its `result`.
    Interrupting {
        turn: TurnId,
        request_id: String,
        started: Instant,
        deadline: Instant,
    },
    /// stdin is closed; waiting for EOF.
    Draining { deadline: Instant },
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
    /// The façade's thread entries: the reap clears this child's slot, another exit removes the
    /// entry.
    pub threads: Threads,
    pub pending: Pending,
    /// The façade's sub-agent routes, where this supervisor publishes its own.
    pub routes: Routes,
    /// The sender half of `commands`, handed to the façade with each published route.
    pub commands_sender: mpsc::WeakSender<ChildCommand>,
    pub generation: u64,
    pub thread: ThreadId,
    pub context: ChildLogContext,
    /// Lines the handshake read that were not its own responses, mapped first.
    pub early_lines: Vec<String>,
    /// Handshake requests that timed out; the CLI may still answer them.
    pub abandoned_requests: Vec<String>,
    /// The model the child was opened on, which the CLI holds until a turn changes it.
    pub model: ModelRef,
    /// Reap the child once it was idle this long; `None` never reaps.
    pub idle_timeout: Option<Duration>,
}

pub(crate) fn spawn_supervisor(parts: SupervisorParts) -> JoinHandle<()> {
    let supervisor = Supervisor {
        pid: parts.child.pid(),
        child: parts.child,
        mapper: parts.mapper,
        log: parts.log,
        commands: parts.commands,
        shutdown: parts.shutdown,
        threads: parts.threads,
        pending: parts.pending,
        routes: parts.routes,
        commands_sender: parts.commands_sender,
        route_logs: HashMap::new(),
        route_logs_missing: HashSet::new(),
        generation: parts.generation,
        thread: parts.thread,
        context: parts.context,
        waiters: HashMap::new(),
        early_lines: parts.early_lines,
        abandoned: parts.abandoned_requests.into_iter().collect(),
        interrupt_sent: false,
        dropped_events: 0,
        failure: None,
        withdrawn: HashSet::new(),
        answered_asks: HashSet::new(),
        turn_setup: None,
        phase: Phase::Serving,
        commands_closed: false,
        ending: None,
        idle_timeout: parts.idle_timeout,
        idle_since: None,
        busy_reason: None,
        last_line: Instant::now(),
        awaiting_notification_since: None,
        reaped: false,
        reaped_outputs: 0,
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
    /// `Stop`, shutdown, a dropped façade or idleness; the stop sequence ran.
    Stopped {
        replies: Vec<oneshot::Sender<()>>,
        /// Reaped for idleness: the thread keeps its entry and its log stays open.
        reaped: bool,
    },
}

struct Supervisor {
    child: Box<dyn ClaudeChild>,
    mapper: ClaudeMapper,
    log: Arc<EventLog>,
    commands: mpsc::Receiver<ChildCommand>,
    shutdown: watch::Receiver<bool>,
    threads: Threads,
    pending: Pending,
    routes: Routes,
    commands_sender: mpsc::WeakSender<ChildCommand>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: The retained event log of each sub-agent route of this child.
    // Source of truth: The mapper's `RouteOpened` creates the log.
    // Structural reason: A route's events are its own thread's, read through the route's handle.
    // Synchronization: Owned by this supervisor task alone.
    // Invalidation/removal: The mapper's `RouteClosed` removes one and child exit the rest; the
    //   log itself stays open, published in the façade's routes until the sub-agent thread's
    //   delete or archive, or shutdown.
    route_logs: HashMap<ThreadId, Arc<EventLog>>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Report an event for a thread with no log once per thread, not once per event.
    // Source of truth: `append` inserts the thread the first time it finds no log for it.
    // Structural reason: A lost route log is an invariant breach worth one warning, not a flood.
    // Synchronization: Owned by this supervisor task alone.
    // Invalidation/removal: Drops with the task.
    route_logs_missing: HashSet<ThreadId>,
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
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Recognise the CLI's echo of an answer this supervisor wrote to one of its asks.
    // Source of truth: Every `control_response` the supervisor writes for a CLI ask
    //   (`respond_approval`, `respond_server_request`, the mapper's own `Reply`).
    // Structural reason: `--replay-user-messages` echoes each such line back on stdout, verbatim
    //   and unmarked, where it reads as a response to a request of the adapter's own.
    // Synchronization: Owned by this supervisor task alone.
    // Invalidation/removal: The echo removes its entry; the next turn clears the rest.
    answered_asks: HashSet<String>,
    /// The `StartTurn` hand-off whose settings are in flight. Not keyed: one at a time, cleared
    /// when it replies.
    turn_setup: Option<TurnSetup>,
    phase: Phase,
    /// The command channel closed (the façade dropped the child); its arm is disabled.
    commands_closed: bool,
    /// Set by a loop arm that ends the loop (the stop sequence's kill).
    ending: Option<Ending>,
    idle_timeout: Option<Duration>,
    /// When the child last became idle; `None` while it has something to do.
    idle_since: Option<Instant>,
    /// What last kept the child busy, so only a change is logged.
    busy_reason: Option<&'static str>,
    /// When stdout last produced a line; the idle timer also waits for it to go quiet.
    last_line: Instant,
    /// Since when a background command's terminal update has waited for its notification.
    awaiting_notification_since: Option<Instant>,
    /// The child was reaped: whatever it still says is dropped, never published on the thread
    /// a respawned child now serves.
    reaped: bool,
    /// Outputs of a reaped child dropped.
    reaped_outputs: u64,
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
        let (requested, replies, reaped) = match ending {
            Ending::Eof | Ending::Broken => (false, Vec::new(), false),
            Ending::Stopped { replies, reaped } => (true, replies, reaped),
        };
        let exit = self.child.wait().await;
        self.on_exit(&exit, requested, reaped);
        for reply in replies {
            let _ = reply.send(());
        }
    }

    /// One loop: every input the supervisor waits on (a frame, a command, the shutdown flag, the
    /// in-flight stage's deadline, the idle timer) is an arm of one `select!`, and every arm body runs to
    /// completion, so a write made from a body is never cancelled.
    async fn main_loop(&mut self) -> Ending {
        loop {
            self.track_idle();
            let stage_deadline = self.stage_deadline();
            let idle_deadline = self.idle_deadline();
            let notification_deadline = self.notification_deadline();
            let stopping = self.stopping();
            tokio::select! {
                biased;
                line = self.child.next_line() => match line {
                    Ok(Some(line)) => {
                        self.last_line = Instant::now();
                        if let Err(error) = self.dispatch_line(&line).await {
                            self.broken("write_stdin", "a stdin write failed", &error);
                            return self.end_broken();
                        }
                        self.after_line();
                    }
                    Ok(None) => return self.end_at_eof(),
                    Err(error) => {
                        self.broken("read_stdout", "a stdout read failed", &error);
                        return self.end_broken();
                    }
                },
                // A closed channel resolves on every poll: the arm is disabled once it fired.
                command = self.commands.recv(), if !self.commands_closed => {
                    if let Err(error) = self.on_command(command).await {
                        self.broken("write_stdin", "a stdin write failed", &error);
                        return self.end_broken();
                    }
                }
                // So does a set flag: the arm is disabled while the stop sequence runs.
                () = shutdown_signal(&mut self.shutdown), if !stopping => {
                    self.enter_stopping("shutdown", None, false).await;
                }
                () = tokio::time::sleep_until(stage_deadline.unwrap_or_else(Instant::now)),
                    if stage_deadline.is_some() => self.on_deadline(),
                () = tokio::time::sleep_until(notification_deadline.unwrap_or_else(Instant::now)),
                    if notification_deadline.is_some() => {
                    self.awaiting_notification_since = None;
                    let outputs = self.mapper.settle_background_commands("grace");
                    if let Err(error) = self.dispatch_all(outputs).await {
                        self.broken("write_stdin", "a stdin write failed", &error);
                        return self.end_broken();
                    }
                }
                () = tokio::time::sleep_until(idle_deadline.unwrap_or_else(Instant::now)),
                    if idle_deadline.is_some() => {
                    if let Err(error) = self.reap().await {
                        self.broken("write_stdin", "a stdin write failed", &error);
                        return self.end_broken();
                    }
                }
            }
            if let Some(ending) = self.ending.take() {
                return ending;
            }
        }
    }

    /// One command, or the channel's end. `Err` is a stdin write failure: the child is broken.
    async fn on_command(&mut self, command: Option<ChildCommand>) -> Result<(), HarnessError> {
        match command {
            Some(ChildCommand::Stop { reply }) => {
                self.enter_stopping("stop", Some(reply), false).await;
                Ok(())
            }
            Some(command) => match &self.phase {
                Phase::Stopping(stopping) => {
                    let reason = stopping.reason;
                    self.refuse(command, reason);
                    Ok(())
                }
                Phase::Serving => self.handle_command(command).await,
            },
            None => {
                self.commands_closed = true;
                if self.stopping() {
                    // A reaped child's façade handle, its last sender, is gone: nothing new.
                    return Ok(());
                }
                debug!(
                    thread_id = %self.thread,
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    action = "stop",
                    "the harness dropped this child's command channel; stopping it"
                );
                self.enter_stopping("harness_dropped", None, false).await;
                Ok(())
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
        if self.reaped
            && matches!(
                output,
                MapperOutput::Event(_)
                    | MapperOutput::Reply(_)
                    | MapperOutput::PendingApproval { .. }
                    | MapperOutput::PendingServerRequest { .. }
                    | MapperOutput::RouteOpened { .. }
            )
        {
            // Mapped all the same, so the mapper stays consistent; nothing reaches the thread.
            self.drop_reaped_output(output_kind(&output));
            return Ok(());
        }
        match output {
            MapperOutput::Event(event) => self.append(event),
            MapperOutput::Reply(value) => {
                // The mapper's own answer to an ask (an `ExitPlanMode` deny): the CLI echoes it.
                if let Some(request_id) = value["response"]["request_id"].as_str() {
                    self.answered_asks.insert(request_id.to_owned());
                }
                self.child.write_line(&value.to_string()).await?
            }
            MapperOutput::ControlResponse {
                request_id,
                payload,
            } if self
                .turn_setup
                .as_ref()
                .is_some_and(|setup| setup.request_id == request_id) =>
            {
                self.on_setup_response(payload).await?;
            }
            MapperOutput::ControlResponse {
                request_id,
                payload,
            } if self.answered_asks.remove(&request_id) => {
                // Checked before the waiters, so "abandoned" and "nobody is waiting" keep meaning
                // what they say; the adapter's own request ids are fresh UUIDs, never an ask's.
                debug!(
                    thread_id = %self.thread,
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    request_id = %request_id,
                    action = "control_response",
                    echo = true,
                    success = control_outcome(&payload).is_ok(),
                    "the CLI echoed the adapter's own answer; ignored"
                );
            }
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
                        if let Waiter::StopTask(stop) = &waiter {
                            self.log_stop_task_answer(stop, &outcome);
                        }
                        waiter.resolve(Ok(payload));
                    }
                    None if self.abandoned.remove(&request_id) => debug!(
                        thread_id = %self.thread,
                        request_id = %request_id,
                        action = "control_response",
                        success = outcome.is_ok(),
                        "late answer to a request that timed out; ignored"
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
                thread,
            } => {
                debug!(
                    thread_id = %thread,
                    owner_thread_id = %self.thread,
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
                        thread,
                        owner: self.thread,
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
                thread,
                request_id,
                subtype,
                input,
            } => {
                debug!(
                    thread_id = %thread,
                    owner_thread_id = %self.thread,
                    request_id = %request_id,
                    subtype = %subtype,
                    action = "pending_server_request",
                    "server request recorded until the browser answers it"
                );
                lock(&self.pending).insert_server_request(
                    id,
                    PendingAsk {
                        thread,
                        owner: self.thread,
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
            MapperOutput::RouteOpened {
                thread,
                harness_thread_id,
                parent_harness_thread_id,
                agent_name,
            } => self.open_route(
                thread,
                harness_thread_id,
                parent_harness_thread_id,
                agent_name,
            ),
            MapperOutput::RouteClosed { thread } => self.close_route(thread),
        }
        Ok(())
    }

    /// The mapper minted a route: give it a retained log and publish it to the façade, so the
    /// server's claim adopts it and its reader starts at the route's first event.
    fn open_route(
        &mut self,
        thread: ThreadId,
        harness_thread_id: String,
        parent_harness_thread_id: String,
        agent_name: Option<String>,
    ) {
        let log = Arc::new(EventLog::new());
        self.route_logs.insert(thread, log.clone());
        let commands = self.commands_sender.upgrade();
        let live_routes = {
            let mut routes = lock(&self.routes);
            routes.insert(
                thread,
                RouteHandle {
                    harness_thread_id: harness_thread_id.clone(),
                    log,
                    owner: Some(self.thread),
                    commands,
                    parent_harness_thread_id: Some(parent_harness_thread_id.clone()),
                    agent_name,
                    model: Some(self.current_model.clone()),
                    generation: Some(self.generation),
                },
            );
            routes.len()
        };
        info!(
            project_id = display_opt(self.context.project_id),
            thread_id = %thread,
            owner_thread_id = %self.thread,
            harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
            route = %harness_thread_id,
            parent_harness_thread_id = %parent_harness_thread_id,
            live_routes,
            action = "route_opened",
            "sub-agent route published"
        );
    }

    /// The mapper dropped a route: this supervisor stops appending to its log and drops its asks.
    ///
    /// The log stays **open and published**: the sub-agent thread's owner keeps reading it like
    /// any thread's stream (a closed stream would end that owner as failed), and a claim that
    /// lands after the parent's turn ended still adopts the route and reads its retained events.
    /// The route ends only with the sub-agent thread's own delete or archive, or at shutdown.
    fn close_route(&mut self, thread: ThreadId) {
        let had_log = self.route_logs.remove(&thread).is_some();
        let pending_dropped = lock(&self.pending).remove_thread(thread);
        info!(
            thread_id = %thread,
            owner_thread_id = %self.thread,
            harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
            had_log,
            pending_dropped,
            action = "route_closed",
            "sub-agent route closed; its log stays open for the sub-agent thread"
        );
    }

    /// The child exited: every route it published becomes a cold route (no owner, no command
    /// sender), its log left open, so the sub-agent threads' owners keep their silent streams.
    /// Returns how many routes were turned cold.
    fn cool_routes(&self) -> usize {
        let mut routes = lock(&self.routes);
        let mut cooled = 0;
        for route in routes.values_mut() {
            if route.owner == Some(self.thread) && route.generation == Some(self.generation) {
                route.owner = None;
                route.commands = None;
                route.generation = None;
                cooled += 1;
            }
        }
        cooled
    }

    /// The CLI withdrew an ask (`control_cancel_request`): drop it, so a late answer is refused.
    /// A server request's card is cleared by `ServerRequestResolved`; an approval's vanishes with
    /// its turn.
    fn on_cancel_request(&mut self, request_id: &str) {
        let removed = lock(&self.pending).remove_by_request_id(self.thread, request_id);
        match removed {
            Some((kind, ask)) => {
                debug!(
                    thread_id = %ask.thread,
                    owner_thread_id = %self.thread,
                    turn_id = display_opt(self.mapper.active_turn()),
                    request_id = %request_id,
                    action = "control_cancel_request",
                    kind = kind.as_str(),
                    "dropped the ask Claude Code withdrew"
                );
                if kind == RequestKind::ServerRequest {
                    let turn = self.mapper.active_turn_of(ask.thread);
                    self.append(AgentEvent::ServerRequestResolved {
                        thread: ask.thread,
                        turn,
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

    /// A reaped child still produced something: dropped, reported once and counted.
    fn drop_reaped_output(&mut self, kind: &'static str) {
        self.reaped_outputs += 1;
        if self.reaped_outputs == 1 {
            warn!(
                thread_id = %self.thread,
                harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                pid = display_opt(self.pid),
                action = "reaped_frame",
                output = kind,
                "a reaped claude child is still writing; dropping what it says"
            );
        }
    }

    /// Append to the retained log of the event's thread (the primary's or a route's); a closed or
    /// missing log is reported once and counted, never ignored. A reaped child appends nothing:
    /// the log is the thread's, and a respawned child may already be writing to it.
    fn append(&mut self, event: AgentEvent) {
        if self.reaped {
            self.drop_reaped_output(AgentEvent::kind(&event));
            return;
        }
        let thread = event_thread(&event);
        if thread != self.thread {
            let appended = self
                .route_logs
                .get(&thread)
                .is_some_and(|log| log.append(event));
            if appended {
                return;
            }
            self.dropped_events += 1;
            if self.route_logs_missing.insert(thread) {
                warn!(
                    thread_id = %thread,
                    owner_thread_id = %self.thread,
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    action = "route_log_missing",
                    "no open event log for this sub-agent thread; dropping its events"
                );
            }
            return;
        }
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
                notice,
                reply,
            } => {
                self.begin_turn_setup(line, turn, model, settings, notice, reply)
                    .await
            }
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
            ChildCommand::StopTask { thread, reply } => self.stop_task(thread, reply).await,
            ChildCommand::StopBackgroundCommand { task_id, reply } => {
                self.stop_background_command(task_id, reply).await
            }
            ChildCommand::Compact {
                turn,
                notice,
                reply,
            } => self.compact(turn, notice, reply).await,
            ChildCommand::Stop { reply } => {
                // A defensive duplicate: `on_command` handles every `Stop` itself, so this is
                // reached only if a caller bypasses it.
                self.enter_stopping("stop", Some(reply), false).await;
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

    /// `ThreadBusy` while another turn's settings are in flight.
    fn setup_in_flight(&self, action: &'static str) -> Option<HarnessError> {
        let setup = self.turn_setup.as_ref()?;
        debug!(
            thread_id = %self.thread,
            turn_id = %setup.turn,
            action,
            "refusing a turn while another's settings are in flight"
        );
        Some(HarnessError::ThreadBusy {
            thread: self.thread,
        })
    }

    /// The `StartTurn` hand-off, first step: the turn's settings, then `TurnStarted`, then the
    /// user message.
    ///
    /// Plan §8.2: the turn's permission mode is set on every turn, then its model and effort when
    /// they differ from what the CLI holds, then both are read back, all within
    /// `TURN_SETTINGS_BUDGET`. This writes the mode request and leaves the hand-off in
    /// `turn_setup`; the main loop advances it on each response (`on_setup_response`) and fails
    /// it on its stage deadline. Any failure fails the hand-off; a mode already set stays set
    /// (the next turn sets its own). `Err` is a stdin write failure: the child is broken.
    async fn begin_turn_setup(
        &mut self,
        line: String,
        turn: TurnId,
        model: ModelRef,
        settings: TurnSettings,
        notice: Option<String>,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    ) -> Result<(), HarnessError> {
        // The caller's receiver is dropped the moment its timeout fires. Starting the turn
        // anyway would run the user's message under a turn the server never admitted, so a
        // hand-off that arrives after its caller gave up is dropped unwritten.
        if reply.is_closed() {
            self.log_caller_gave_up(turn, "start_turn");
            return Ok(());
        }
        if let Some(busy) = self
            .busy("start_turn")
            .or_else(|| self.setup_in_flight("start_turn"))
        {
            let _ = reply.send(Err(busy));
            return Ok(());
        }
        // Expected before the write, so the `status` the change emits is not drift.
        self.mapper.set_expected_mode(settings.mode.clone());
        let now = Instant::now();
        let setup = TurnSetup {
            line,
            turn,
            model,
            settings,
            reply,
            notice,
            stage: SetupStage::Mode,
            request_id: String::new(),
            budget: now + TURN_SETTINGS_BUDGET,
            sent: now,
            deadline: now,
            model_change: None,
            effort_change: None,
        };
        self.send_setup_request(setup, SetupStage::Mode).await
    }

    /// Write the request of `stage` and keep the hand-off in flight until its answer or its
    /// deadline. `Err` is a stdin write failure: the hand-off fails with it and the child is
    /// broken.
    async fn send_setup_request(
        &mut self,
        mut setup: TurnSetup,
        stage: SetupStage,
    ) -> Result<(), HarnessError> {
        let request = match stage {
            SetupStage::Mode => {
                json!({"subtype": "set_permission_mode", "mode": setup.settings.mode})
            }
            SetupStage::Model => json!({"subtype": "set_model", "model": setup.model_change}),
            SetupStage::Effort => {
                json!({"subtype": "apply_flag_settings", "settings": {"effortLevel": setup.effort_change}})
            }
            SetupStage::ReadBack => json!({"subtype": "get_settings"}),
        };
        let request_id = new_request_id();
        if let Err(error) = self
            .child
            .write_line(&control_line(&request_id, &request))
            .await
        {
            let _ = setup.reply.send(Err(error.clone()));
            return Err(error);
        }
        debug!(
            thread_id = %self.thread,
            turn_id = %setup.turn,
            request_id = %request_id,
            action = "control_request",
            subtype = stage.subtype(),
            "control request sent; awaiting its response"
        );
        let now = Instant::now();
        setup.stage = stage;
        setup.request_id = request_id;
        setup.sent = now;
        setup.deadline = (now + CONTROL_TIMEOUT).min(setup.budget);
        self.turn_setup = Some(setup);
        Ok(())
    }

    /// The CLI answered the in-flight hand-off's request: write the next one, start the turn, or
    /// fail the hand-off. `Err` is a stdin write failure: the child is broken.
    async fn on_setup_response(&mut self, payload: Value) -> Result<(), HarnessError> {
        let Some(mut setup) = self.turn_setup.take() else {
            return Ok(());
        };
        let outcome = setup_outcome(&payload);
        debug!(
            thread_id = %self.thread,
            turn_id = %setup.turn,
            request_id = %setup.request_id,
            action = "control_response",
            subtype = setup.stage.subtype(),
            success = outcome.is_ok(),
            "control response received"
        );
        let next = match outcome {
            Ok(response) => self.setup_stage_done(&mut setup, &response),
            Err(failure) => Err(self.setup_stage_failed(&setup, failure)),
        };
        match next {
            Ok(Some(stage)) => self.send_setup_request(setup, stage).await,
            Ok(None) => self.finish_turn_setup(setup).await,
            Err(error) => {
                self.fail_turn_setup(setup, error);
                Ok(())
            }
        }
    }

    /// One stage succeeded: the next stage, `None` when the settings are applied, or the error
    /// the read-back found.
    fn setup_stage_done(
        &mut self,
        setup: &mut TurnSetup,
        response: &Value,
    ) -> Result<Option<SetupStage>, HarnessError> {
        let turn = setup.turn;
        match setup.stage {
            SetupStage::Mode => {
                debug!(
                    thread_id = %self.thread,
                    turn_id = %turn,
                    action = "set_permission_mode",
                    mode = %setup.settings.mode,
                    previous_mode = %self.current_mode,
                    "permission mode set for the turn"
                );
                self.current_mode = setup.settings.mode.clone();
                setup.model_change = setup
                    .settings
                    .model
                    .clone()
                    .filter(|requested| *requested != self.current_model.model);
                // A model switch can change the effort the CLI holds (a model without effort
                // reports none), so a requested level is sent and checked again whenever the
                // model changes.
                let model_changes = setup.model_change.is_some();
                setup.effort_change = setup.settings.effort.clone().filter(|requested| {
                    model_changes || Some(requested.as_str()) != self.current_effort.as_deref()
                });
                if !model_changes && setup.effort_change.is_none() {
                    self.log_turn_settings(turn, false);
                    return Ok(None);
                }
                Ok(Some(if model_changes {
                    SetupStage::Model
                } else {
                    SetupStage::Effort
                }))
            }
            SetupStage::Model => Ok(Some(if setup.effort_change.is_some() {
                SetupStage::Effort
            } else {
                SetupStage::ReadBack
            })),
            SetupStage::Effort => Ok(Some(SetupStage::ReadBack)),
            SetupStage::ReadBack => self.check_read_back(setup, response).map(|()| None),
        }
    }

    /// The read-back: the CLI must hold the requested model (or its catalog resolution) and
    /// effort.
    fn check_read_back(
        &mut self,
        setup: &TurnSetup,
        settings_now: &Value,
    ) -> Result<(), HarnessError> {
        let turn = setup.turn;
        let settings = &setup.settings;
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
        if setup.model_change.is_some() {
            // The switch went through, whatever happens to the effort below.
            self.current_model = setup.model.clone();
        }
        // What the CLI holds now, refused or not, so the next turn compares against the truth.
        self.current_effort = applied_effort.clone();
        self.current_model.reasoning_effort =
            applied_effort.clone().map(giskard_core::model::Effort);
        self.sync_entry_model();
        if let Some(level) = setup.effort_change.as_deref()
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

    /// One stage failed (refused, timed out, or the child is stopping): its log line, and the
    /// error the hand-off fails with.
    fn setup_stage_failed(&mut self, setup: &TurnSetup, failure: ControlFailure) -> HarnessError {
        let turn = setup.turn;
        match setup.stage {
            SetupStage::Mode => {
                // The CLI kept its mode: a later frame reporting it is not drift.
                self.mapper.set_expected_mode(self.current_mode.clone());
                warn!(
                    project_id = display_opt(self.context.project_id),
                    thread_id = %self.thread,
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    turn_id = %turn,
                    action = "set_permission_mode",
                    mode = %setup.settings.mode,
                    error_code = display_opt(failure.code()),
                    error = %failure,
                    "Claude Code did not set the turn's permission mode"
                );
                failure.into_error()
            }
            SetupStage::Model => {
                warn!(
                    thread_id = %self.thread,
                    turn_id = %turn,
                    action = "set_model",
                    model = display_opt(setup.model_change.as_deref()),
                    error_code = display_opt(failure.code()),
                    error = %failure,
                    "Claude Code did not switch the model"
                );
                // The picker offered a model the CLI does not know: surface its sentence.
                match failure {
                    ControlFailure::Refused { message, code }
                        if code.as_deref() == Some("catalog_unknown") =>
                    {
                        HarnessError::Unsupported(message)
                    }
                    other => other.into_error(),
                }
            }
            SetupStage::Effort => {
                warn!(
                    thread_id = %self.thread,
                    turn_id = %turn,
                    action = "apply_flag_settings",
                    effort = display_opt(setup.effort_change.as_deref()),
                    error_code = display_opt(failure.code()),
                    error = %failure,
                    "Claude Code did not take the effort level"
                );
                failure.into_error()
            }
            SetupStage::ReadBack => {
                warn!(
                    thread_id = %self.thread,
                    turn_id = %turn,
                    action = "get_settings",
                    error = %failure,
                    "could not read back the turn's model and effort"
                );
                failure.into_error()
            }
        }
    }

    /// The in-flight request's deadline passed. The CLI may still answer it: its late answer is
    /// expected.
    fn on_setup_deadline(&mut self) {
        let Some(setup) = self.turn_setup.take() else {
            return;
        };
        let subtype = setup.stage.subtype();
        let waited = setup.deadline.saturating_duration_since(setup.sent);
        warn!(
            thread_id = %self.thread,
            harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
            turn_id = %setup.turn,
            request_id = %setup.request_id,
            action = subtype,
            timeout_ms = waited.as_millis() as u64,
            "Claude Code did not answer a control request in time"
        );
        self.abandoned.insert(setup.request_id.clone());
        let error = self.setup_stage_failed(
            &setup,
            ControlFailure::Failed(HarnessError::Timeout(format!(
                "claude did not answer {subtype} within {:.1} s",
                waited.as_secs_f64()
            ))),
        );
        self.fail_turn_setup(setup, error);
    }

    /// The hand-off failed: the turn does not start.
    fn fail_turn_setup(&self, setup: TurnSetup, error: HarnessError) {
        debug!(
            thread_id = %self.thread,
            turn_id = %setup.turn,
            action = "start_turn",
            error = %error,
            "the turn's settings were not applied; the turn does not start"
        );
        let _ = setup.reply.send(Err(error));
    }

    /// The settings are applied: `TurnStarted`, then the user message. `Err` is a stdin write
    /// failure: the child is broken.
    async fn finish_turn_setup(&mut self, setup: TurnSetup) -> Result<(), HarnessError> {
        let TurnSetup {
            line,
            turn,
            model,
            reply,
            notice,
            ..
        } = setup;
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
        // An echo follows its answer within milliseconds: none is still on its way.
        self.answered_asks.clear();
        // `TurnStarted` reaches the log before the line is written, so the server sees the turn
        // before its first frame.
        let outputs = self.mapper.begin_turn(turn, TurnKind::User);
        self.mapper.note_turn_model(model);
        for output in outputs {
            if let MapperOutput::Event(event) = output {
                self.append(event);
            }
        }
        if let Some(message) = notice {
            self.append(AgentEvent::Notice {
                thread: self.thread,
                turn: Some(turn),
                message,
            });
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

    /// The thread's entry follows what the CLI holds, so a turn without a model override falls
    /// back to it, before a reap as after.
    fn sync_entry_model(&self) {
        let mut threads = lock(&self.threads);
        if self.holds_entry(&threads)
            && let Some(entry) = threads.get_mut(&self.thread)
        {
            entry.model = self.current_model.clone();
        }
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
        let turn = self.mapper.active_turn_of(ask.thread);
        if matches!(
            decision,
            ApprovalDecision::Decline | ApprovalDecision::Cancel
        ) && let Some(tool_use_id) = &ask.tool_use_id
        {
            self.mapper.note_denied(ask.thread, tool_use_id);
        }
        if matches!(decision, ApprovalDecision::Cancel) {
            // The deny carries `interrupt: true`: the turn ends `aborted_tools`, which must
            // persist as `Interrupted`, and the exit after it is expected.
            self.interrupt_sent = true;
            if self.mapper.active_turn().is_some() {
                self.mapper.note_interrupt_sent();
            }
        }
        self.answered_asks.insert(ask.request_id.clone());
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
            thread_id = %ask.thread,
            owner_thread_id = %self.thread,
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
            let turn = self.mapper.active_turn_of(ask.thread);
            self.append(AgentEvent::ServerRequestResolved {
                thread: ask.thread,
                turn,
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
        self.answered_asks.insert(ask.request_id.clone());
        if let Err(error) = self.child.write_line(&line).await {
            let _ = reply.send(Err(error.clone()));
            return Err(error);
        }
        let turn = self.mapper.active_turn_of(ask.thread);
        info!(
            project_id = display_opt(self.context.project_id),
            thread_id = %ask.thread,
            owner_thread_id = %self.thread,
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
            thread: ask.thread,
            turn,
            request_id: id,
        });
        let _ = reply.send(Ok(()));
        Ok(())
    }

    /// `stop_task` for the sub-agent of route `thread`: the sub-agent thread's interrupt. The
    /// sub-agent is killed, its pending ask is withdrawn (`control_cancel_request`), and the
    /// parent's turn continues. The answer is a waiter the main loop resolves; the façade bounds
    /// the call. `Err` is a stdin write failure.
    async fn stop_task(
        &mut self,
        thread: ThreadId,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    ) -> Result<(), HarnessError> {
        let task_id = match self.mapper.route_task_id(thread) {
            Ok(Some(task_id)) => task_id,
            Ok(None) => {
                debug!(
                    thread_id = %thread,
                    owner_thread_id = %self.thread,
                    action = "stop_task",
                    "the sub-agent has not started yet; nothing to stop"
                );
                let _ = reply.send(Err(HarnessError::Protocol(
                    "the sub-agent has not started yet".into(),
                )));
                return Ok(());
            }
            Err(RouteLookup::Ended) => {
                debug!(
                    thread_id = %thread,
                    owner_thread_id = %self.thread,
                    action = "stop_task",
                    "the sub-agent already ended; nothing to stop"
                );
                let _ = reply.send(Ok(()));
                return Ok(());
            }
            Err(RouteLookup::NotARoute) => {
                warn!(
                    thread_id = %thread,
                    owner_thread_id = %self.thread,
                    action = "stop_task",
                    "stop_task for a thread that is no sub-agent of this child"
                );
                let _ = reply.send(Err(HarnessError::Protocol(format!(
                    "thread {thread} is not a running sub-agent of this child"
                ))));
                return Ok(());
            }
        };
        let harness_thread_id = lock(&self.routes)
            .get(&thread)
            .map(|route| route.harness_thread_id.clone());
        // Noted before the write: the CLI emits the task's `killed` update before it answers, and
        // that update must read as an interruption. A refused stop leaves the task running, and
        // only another stop (or the parent's interrupt) can kill it later.
        self.mapper.note_stop_sent(thread);
        let request_id = new_request_id();
        let request = json!({"subtype": "stop_task", "task_id": task_id});
        if let Err(error) = self
            .child
            .write_line(&control_line(&request_id, &request))
            .await
        {
            let _ = reply.send(Err(error.clone()));
            return Err(error);
        }
        debug!(
            thread_id = %thread,
            owner_thread_id = %self.thread,
            request_id = %request_id,
            task_id = %task_id,
            action = "control_request",
            subtype = "stop_task",
            "control request sent; awaiting its response"
        );
        self.waiters.insert(
            request_id,
            Waiter::StopTask(StopTaskWaiter {
                thread,
                task_id,
                harness_thread_id,
                reply,
            }),
        );
        Ok(())
    }

    /// `terminate_command`: `stop_task` for the `local_bash` task of a background command. The
    /// `{}` answer resolves the call; the real signal is the task's `killed` update, which the
    /// mapper turns into the item's terminal completion. `Err` is a stdin write failure.
    async fn stop_background_command(
        &mut self,
        task_id: String,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    ) -> Result<(), HarnessError> {
        let Some((thread, command)) = self.mapper.background_command(&task_id) else {
            warn!(
                thread_id = %self.thread,
                task_id = %task_id,
                action = "terminate_command",
                "terminate_command for no background command of this child"
            );
            let _ = reply.send(Err(no_background_command(&task_id)));
            return Ok(());
        };
        let command = command.to_owned();
        let request_id = new_request_id();
        let request = json!({"subtype": "stop_task", "task_id": task_id});
        if let Err(error) = self
            .child
            .write_line(&control_line(&request_id, &request))
            .await
        {
            let _ = reply.send(Err(error.clone()));
            return Err(error);
        }
        info!(
            project_id = display_opt(self.context.project_id),
            thread_id = %thread,
            owner_thread_id = %self.thread,
            request_id = %request_id,
            task_id = %task_id,
            command = %command,
            action = "terminate_command",
            "stopping a background command"
        );
        self.waiters.insert(request_id, Waiter::Unit(reply));
        Ok(())
    }

    /// The CLI answered a `stop_task`.
    fn log_stop_task_answer(&self, stop: &StopTaskWaiter, outcome: &Result<Value, HarnessError>) {
        match outcome {
            Ok(_) => info!(
                project_id = display_opt(self.context.project_id),
                thread_id = %stop.thread,
                owner_thread_id = %self.thread,
                harness_thread_id = display_opt(stop.harness_thread_id.as_deref()),
                task_id = %stop.task_id,
                action = "stop_task",
                "sub-agent stopped"
            ),
            Err(error) => warn!(
                project_id = display_opt(self.context.project_id),
                thread_id = %stop.thread,
                owner_thread_id = %self.thread,
                harness_thread_id = display_opt(stop.harness_thread_id.as_deref()),
                task_id = %stop.task_id,
                action = "stop_task",
                error = %error,
                "Claude Code did not stop the sub-agent"
            ),
        }
    }

    /// `/compact` as a compaction turn. No per-turn settings are sent: compaction runs no tool,
    /// and the mode in force is the previous turn's. `Err` is a stdin write failure.
    async fn compact(
        &mut self,
        turn: TurnId,
        notice: Option<String>,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    ) -> Result<(), HarnessError> {
        if reply.is_closed() {
            self.log_caller_gave_up(turn, "compact");
            return Ok(());
        }
        if let Some(busy) = self
            .busy("compact")
            .or_else(|| self.setup_in_flight("compact"))
        {
            let _ = reply.send(Err(busy));
            return Ok(());
        }
        self.withdrawn.clear();
        self.answered_asks.clear();
        for output in self.mapper.begin_turn(turn, TurnKind::Compaction) {
            if let MapperOutput::Event(event) = output {
                self.append(event);
            }
        }
        if let Some(message) = notice {
            self.append(AgentEvent::Notice {
                thread: self.thread,
                turn: Some(turn),
                message,
            });
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

    fn stopping(&self) -> bool {
        matches!(self.phase, Phase::Stopping(_))
    }

    /// What keeps the child busy, by name, or `None` when it is idle: no turn hand-off in flight,
    /// no ask awaiting the user, no live sub-agent route, no open task (a background shell
    /// outlives its turn and can still ask), no control request awaiting its answer, and no
    /// active turn. The most specific reason comes first: an ask or a task usually comes with a
    /// turn, and is the one that can outlast it.
    fn busy_with(&self) -> Option<&'static str> {
        if self.turn_setup.is_some() {
            Some("turn_setup")
        } else if lock(&self.pending).count_owner(self.thread) > 0 {
            Some("asks")
        } else if self.mapper.has_routes() {
            Some("routes")
        } else if self.mapper.has_tasks() {
            Some("tasks")
        } else if !self.waiters.is_empty() {
            Some("control_request")
        } else if self.mapper.active_turn().is_some() {
            Some("turn")
        } else {
            None
        }
    }

    /// Start or stop the idle clock, logging each transition and each change of what keeps the
    /// child busy. Runs at the top of every loop iteration while serving; the clock is tracked
    /// even when reaping is off.
    fn track_idle(&mut self) {
        if self.stopping() {
            return;
        }
        let busy = self.busy_with();
        let unchanged = match busy {
            None => self.idle_since.is_some(),
            Some(_) => self.idle_since.is_none() && busy == self.busy_reason,
        };
        if unchanged {
            return;
        }
        self.busy_reason = busy;
        match busy {
            None => {
                self.idle_since = Some(Instant::now());
                debug!(
                    thread_id = %self.thread,
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    pid = display_opt(self.pid),
                    action = "idle",
                    idle = true,
                    timeout_ms = display_opt(self.idle_timeout.map(|timeout| timeout.as_millis())),
                    "claude child is idle"
                );
            }
            Some(reason) => {
                self.idle_since = None;
                debug!(
                    thread_id = %self.thread,
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    pid = display_opt(self.pid),
                    action = "idle",
                    idle = false,
                    reason,
                    open_tasks = self.mapper.open_tasks(),
                    "claude child is busy"
                );
            }
        }
    }

    /// When a background command's terminal update stops waiting for its notification: the
    /// grace after the update was first seen waiting, while one is.
    fn notification_deadline(&mut self) -> Option<Instant> {
        if !self.mapper.awaiting_notification() {
            self.awaiting_notification_since = None;
            return None;
        }
        self.awaiting_notification_since
            .get_or_insert_with(Instant::now)
            .checked_add(NOTIFICATION_GRACE)
    }

    /// When the idle child is reaped: only while serving, with reaping on, once it has been idle
    /// **and** silent on stdout for the timeout. A frame the mapper does not model (a future
    /// background mechanism) postpones the reap: a child that keeps talking is never reaped, the
    /// safe direction. A timeout too large for the clock never reaps.
    fn idle_deadline(&self) -> Option<Instant> {
        if self.stopping() {
            return None;
        }
        self.idle_since?
            .max(self.last_line)
            .checked_add(self.idle_timeout?)
    }

    /// Whether the thread's entry still holds this child.
    fn holds_entry(&self, threads: &HashMap<ThreadId, ThreadEntry>) -> bool {
        threads
            .get(&self.thread)
            .and_then(|entry| entry.child.as_ref())
            .is_some_and(|child| child.generation == self.generation)
    }

    /// The idle timer fired: take this child out of its thread's entry, then stop it. The thread
    /// stays bound and its log open; the next turn respawns the child with `--resume`.
    ///
    /// The façade looks a child up and enqueues to it in one critical section of `threads`, and
    /// this takes the child and drains the queue in one critical section of the same lock. So a
    /// command either is in the queue drained here, which cancels the reap (the child goes back
    /// into its entry and serves it), or was never sent to this child: once the child is out of
    /// its entry no command can reach it. Its routes turn cold first, under the `routes` lock
    /// their senders are reached through. `Err` is a stdin write failure serving that command.
    async fn reap(&mut self) -> Result<(), HarnessError> {
        let idle_ms = self
            .idle_since
            .map_or(0, |since| since.elapsed().as_millis() as u64);
        let timeout_ms = self
            .idle_timeout
            .map_or(0, |timeout| timeout.as_millis() as u64);
        let routes_cooled = self.cool_routes();
        let (kept, queued, live_children, loaded_threads) = {
            let mut threads = lock(&self.threads);
            let kept = self.holds_entry(&threads);
            let mut queued = None;
            if let Some(entry) = threads.get_mut(&self.thread).filter(|_| kept) {
                let child = entry.child.take();
                match self.commands.try_recv() {
                    Ok(command) => {
                        entry.child = child;
                        queued = Some(command);
                    }
                    Err(_) => {
                        // What the CLI held, so the respawn launches with it, and the notice
                        // the thread already showed.
                        entry.model = self.current_model.clone();
                        entry.api_key_source_noticed =
                            self.mapper.api_key_source_noticed().map(str::to_owned);
                    }
                }
            }
            let (live_children, loaded_threads) = thread_counts(&threads);
            (kept, queued, live_children, loaded_threads)
        };
        if let Some(command) = queued {
            // The routes stay cold. That is benign: idle means every route already ended in the
            // mapper, so a `stop_task` on one now answers "no longer running" instead of "already
            // ended", and an MCP read hinting one of them goes to the probe instead of this child.
            debug!(
                thread_id = %self.thread,
                harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                action = "reap_cancelled",
                command = command.name(),
                routes_cooled,
                "a command reached the child before the reap took it; the reap is cancelled"
            );
            return self.on_command(Some(command)).await;
        }
        if kept {
            self.reaped = true;
            info!(
                project_id = display_opt(self.context.project_id),
                harness = display_opt(self.context.harness.as_deref()),
                thread_id = %self.thread,
                harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                pid = display_opt(self.pid),
                action = "child_reaped",
                idle_ms,
                timeout_ms,
                live_children,
                loaded_threads,
                routes_cooled,
                "claude child idle too long; stopping it, the thread stays open"
            );
        } else {
            warn!(
                project_id = display_opt(self.context.project_id),
                thread_id = %self.thread,
                harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                pid = display_opt(self.pid),
                action = "child_reaped",
                idle_ms,
                timeout_ms,
                "the thread's entry no longer holds this child; stopping it as for any exit"
            );
        }
        self.enter_stopping(if kept { "idle" } else { "stop" }, None, kept)
            .await;
        Ok(())
    }

    /// The deadline the main loop waits on besides frames and commands: the in-flight turn
    /// hand-off's while serving, the stop stage's while stopping.
    fn stage_deadline(&self) -> Option<Instant> {
        match &self.phase {
            Phase::Serving => self.turn_setup.as_ref().map(|setup| setup.deadline),
            Phase::Stopping(stopping) => Some(match &stopping.stage {
                StopStage::Interrupting { deadline, .. } | StopStage::Draining { deadline } => {
                    *deadline
                }
            }),
        }
    }

    /// Enter the stop sequence: interrupt a live turn, then close stdin and wait for EOF, killing
    /// the child on the grace timeout. A stop that arrives while one runs joins it.
    ///
    /// SIGTERM is deliberately not used: it leaves the turn without a `result`.
    async fn enter_stopping(
        &mut self,
        reason: &'static str,
        reply: Option<oneshot::Sender<()>>,
        reaped: bool,
    ) {
        if let Phase::Stopping(stopping) = &mut self.phase {
            stopping.replies.extend(reply);
            debug!(
                thread_id = %self.thread,
                harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                action = "stop",
                reason,
                running = stopping.reason,
                "a second stop joined the stop sequence"
            );
            return;
        }
        if let Some(setup) = self.turn_setup.take() {
            debug!(
                thread_id = %self.thread,
                turn_id = %setup.turn,
                request_id = %setup.request_id,
                action = setup.stage.subtype(),
                "stopping the child; no longer waiting for this control request"
            );
            // The CLI may still answer it while it drains.
            self.abandoned.insert(setup.request_id.clone());
            let error = self.setup_stage_failed(
                &setup,
                ControlFailure::Failed(HarnessError::Transport("claude child is stopping".into())),
            );
            self.fail_turn_setup(setup, error);
        }
        let stage = match self.mapper.active_turn() {
            Some(turn) => {
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
                        StopStage::Interrupting {
                            turn,
                            request_id,
                            started,
                            deadline: started + STOP_INTERRUPT_GRACE,
                        }
                    }
                    Err(error) => {
                        warn!(
                            thread_id = %self.thread,
                            turn_id = %turn,
                            action = "stop_interrupt",
                            reason,
                            error = %error,
                            "could not interrupt the live turn before stopping"
                        );
                        self.begin_draining()
                    }
                }
            }
            None => self.begin_draining(),
        };
        self.idle_since = None;
        self.phase = Phase::Stopping(Stopping {
            reason,
            replies: reply.into_iter().collect(),
            stage,
            reaped,
        });
    }

    /// Close stdin: an idle CLI exits 0 at EOF.
    fn begin_draining(&mut self) -> StopStage {
        self.child.close_stdin();
        StopStage::Draining {
            deadline: Instant::now() + STOP_EXIT_GRACE,
        }
    }

    /// Run after every dispatched line: an interrupted turn whose `result` came in moves the stop
    /// sequence on to draining.
    fn after_line(&mut self) {
        let interrupting = matches!(
            &self.phase,
            Phase::Stopping(Stopping {
                stage: StopStage::Interrupting { .. },
                ..
            })
        );
        if interrupting && self.mapper.active_turn().is_none() {
            self.finish_interrupting(true);
        }
    }

    /// The interrupt stage is over, its turn closed or its grace spent: close stdin.
    fn finish_interrupting(&mut self, turn_closed: bool) {
        let Phase::Stopping(stopping) = &self.phase else {
            return;
        };
        let StopStage::Interrupting {
            turn,
            request_id,
            started,
            ..
        } = &stopping.stage
        else {
            return;
        };
        info!(
            thread_id = %self.thread,
            turn_id = %turn,
            request_id = %request_id,
            action = "stop_interrupt",
            reason = stopping.reason,
            elapsed_ms = started.elapsed().as_millis() as u64,
            turn_closed,
            "interrupted the live turn before stopping"
        );
        let stage = self.begin_draining();
        if let Phase::Stopping(stopping) = &mut self.phase {
            stopping.stage = stage;
        }
    }

    /// The stage deadline passed: the turn hand-off's request timed out, the interrupted turn
    /// never closed, or the child ignored EOF.
    fn on_deadline(&mut self) {
        match &self.phase {
            Phase::Serving => self.on_setup_deadline(),
            Phase::Stopping(Stopping {
                stage: StopStage::Interrupting { .. },
                ..
            }) => self.finish_interrupting(false),
            Phase::Stopping(Stopping {
                stage: StopStage::Draining { .. },
                reason,
                ..
            }) => {
                warn!(
                    project_id = display_opt(self.context.project_id),
                    thread_id = %self.thread,
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    pid = display_opt(self.pid),
                    action = "stop_kill",
                    reason = *reason,
                    grace_ms = STOP_EXIT_GRACE.as_millis() as u64,
                    "claude did not exit after stdin closed; killing it"
                );
                self.child.start_kill();
                self.ending = Some(self.stopped());
            }
        }
    }

    /// The ending of a stop sequence, with every reply it collected.
    fn stopped(&mut self) -> Ending {
        match std::mem::replace(&mut self.phase, Phase::Serving) {
            Phase::Stopping(stopping) => Ending::Stopped {
                replies: stopping.replies,
                reaped: stopping.reaped,
            },
            Phase::Serving => Ending::Stopped {
                replies: Vec::new(),
                reaped: false,
            },
        }
    }

    /// stdout reached EOF: the end of a stop sequence, or a child that exited on its own.
    fn end_at_eof(&mut self) -> Ending {
        if self.stopping() {
            self.stopped()
        } else {
            Ending::Eof
        }
    }

    /// A read or write failed and the child was killed. During a stop sequence the stop still
    /// answers its callers.
    fn end_broken(&mut self) -> Ending {
        if self.stopping() {
            self.stopped()
        } else {
            Ending::Broken
        }
    }

    /// A command that arrived during the stop sequence is answered at once: the child will not
    /// serve it. A `Stop` joins the sequence.
    fn refuse(&mut self, command: ChildCommand, reason: &'static str) {
        debug!(
            thread_id = %self.thread,
            harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
            action = "stop_refused",
            command = command.name(),
            reason,
            "the child is stopping; refusing the command"
        );
        match command {
            ChildCommand::StartTurn { reply, .. }
            | ChildCommand::Interrupt { reply }
            | ChildCommand::RespondApproval { reply, .. }
            | ChildCommand::RespondServerRequest { reply, .. }
            | ChildCommand::StopTask { reply, .. }
            | ChildCommand::StopBackgroundCommand { reply, .. }
            | ChildCommand::Compact { reply, .. } => {
                let _ = reply.send(Err(child_stopped()));
            }
            ChildCommand::Control { reply, .. } => {
                let _ = reply.send(Err(child_stopped()));
            }
            ChildCommand::Stop { reply } => {
                if let Phase::Stopping(stopping) = &mut self.phase {
                    stopping.replies.push(reply);
                }
            }
        }
    }

    /// Child-exit handling, whatever ended the child.
    ///
    /// A reaped child's thread stays: its log is not closed, its entry is not touched (the reap
    /// took the child out of it) and the pending map is left alone (idle means none, and a
    /// respawned child may already own asks under the same thread id).
    fn on_exit(&mut self, exit: &ChildExit, requested: bool, reaped: bool) {
        let mut described = exit.describe();
        if let Some(failure) = self.failure {
            described = format!("{described}, after {failure}");
        }
        // A hand-off whose answer never came (the child exited or broke meanwhile).
        if let Some(setup) = self.turn_setup.take() {
            self.fail_turn_setup(setup, child_stopped());
        }
        // Counted before the routes close, so the exit line reports every ask of this child.
        let pending_dropped = if reaped {
            0
        } else {
            lock(&self.pending).remove_owner(self.thread)
        };
        let outputs = if self.mapper.active_turn().is_some() || self.mapper.has_routes() {
            self.mapper.child_exited(&described)
        } else {
            Vec::new()
        };
        for output in outputs {
            match output {
                MapperOutput::Event(event) => self.append(event),
                MapperOutput::RouteClosed { thread } => self.close_route(thread),
                other => debug!(
                    thread_id = %self.thread,
                    action = "child_exited",
                    output = ?other,
                    "ignoring a mapper output produced at child exit"
                ),
            }
        }
        // Routes the mapper never closed (none, normally) end with the child.
        let rest: Vec<ThreadId> = self.route_logs.keys().copied().collect();
        for thread in rest {
            self.close_route(thread);
        }
        let routes_cooled = self.cool_routes();
        if !reaped {
            self.log.close();
        }
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
        let (live_children, loaded_threads) = {
            let mut threads = lock(&self.threads);
            if !reaped && self.holds_entry(&threads) {
                threads.remove(&self.thread);
            }
            thread_counts(&threads)
        };
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
                    reaped,
                    live_children,
                    loaded_threads,
                    pending_dropped,
                    routes_cooled,
                    dropped_events = self.dropped_events,
                    reaped_outputs = self.reaped_outputs,
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

/// `(live_children, loaded_threads)`: entries with a child, and entries.
pub(crate) fn thread_counts(threads: &HashMap<ThreadId, ThreadEntry>) -> (usize, usize) {
    let live = threads
        .values()
        .filter(|entry| entry.child.is_some())
        .count();
    (live, threads.len())
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
        /// Emit the triggering stdin line back verbatim, as `--replay-user-messages` does with
        /// each `control_response` the adapter writes.
        Echo,
        /// Emit the triggering user line back with `isReplay` and a `uuid`, as
        /// `--replay-user-messages` acknowledges a prompt.
        Replay,
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

        /// Close stdout, as a child that exits on its own does.
        pub(crate) fn eof(&self) {
            let _ = self.0.send(Out::Eof);
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
                    Action::Echo => {
                        let _ = self.tx.send(Out::Line(trigger.to_string()));
                    }
                    Action::Replay => {
                        let mut line = trigger.clone();
                        line["isReplay"] = json!(true);
                        line["uuid"] = json!("00000000-0000-4000-8000-00000000beef");
                        line["parent_tool_use_id"] = Value::Null;
                        line["session_id"] = json!("f18693ff-2d11-4f87-9556-2b527e19e081");
                        let _ = self.tx.send(Out::Line(line.to_string()));
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

    fn ask(thread: ThreadId, owner: ThreadId, request_id: &str) -> PendingAsk {
        PendingAsk {
            thread,
            owner,
            request_id: request_id.into(),
            tool_use_id: None,
            tool_name: None,
            suggestions: Vec::new(),
            subtype: "can_use_tool".into(),
            input: Value::Null,
        }
    }

    #[test]
    fn count_owner_counts_approvals_and_server_requests_of_one_owner() {
        let owner = ThreadId::new();
        let route = ThreadId::new();
        let other = ThreadId::new();
        let mut pending = PendingRequests::default();
        assert_eq!(pending.count_owner(owner), 0);
        pending.insert_approval(ApprovalId::new("a1"), ask(owner, owner, "a1"));
        // A sub-agent route's ask is its owner's too.
        pending.insert_approval(ApprovalId::new("a2"), ask(route, owner, "a2"));
        pending.insert_server_request(ServerRequestId::new("s1"), ask(owner, owner, "s1"));
        pending.insert_approval(ApprovalId::new("a3"), ask(other, other, "a3"));
        assert_eq!(pending.count_owner(owner), 3);
        assert_eq!(pending.count_owner(other), 1);
        pending.remove_approval(&ApprovalId::new("a1"));
        assert_eq!(pending.count_owner(owner), 2);
        assert_eq!(pending.remove_owner(owner), 2);
        assert_eq!(pending.count_owner(owner), 0);
    }
}
