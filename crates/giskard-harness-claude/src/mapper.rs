//! The pure mapper from Claude Code stream-json frames to Giskard events and control replies.
//!
//! One [`ClaudeMapper`] serves one `claude` child process: one primary thread, plus the `task:`
//! sub-agent routes milestone 5 adds. Frames in, [`MapperOutput`]s out; no I/O, and no clock except
//! `Utc::now()` for timestamps.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use claude_codes::io::{AssistantMessage, AssistantUsage, ResultMessage, UserMessage};
use claude_codes::{
    ApiKeySource, ApiRetryMessage, CompactBoundaryMessage, ContentBlock, InitMessage,
    PermissionDeniedMessage, RateLimitEvent, RateLimitStatus, StatusMessage,
    TaskNotificationMessage, TaskStartedMessage, TaskStatus, TaskType, TaskUpdatedMessage,
    ToolPermissionRequest, ToolResultBlock, ToolResultContent, ToolResultMeta, ToolUseBlock,
    UsageInfo,
};
use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalMetadata, ApprovalRequest};
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{
    CommandExecutionStart, FileChangeEntry, FileChangeKind, Item, ItemDelta, ItemKind, ItemPayload,
    ItemStart, SubagentAction, SubagentLink, SubagentStatus, ToolCallStart, command_description,
};
use giskard_core::model::ModelRef;
use giskard_core::server_request::ServerRequest;
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
use serde_json::{Value, json};
use tracing::{debug, error, info, warn};

use crate::catalog::{DEFAULT_ALIAS, anthropic_model};
use crate::frame::{
    BlockStart, Delta, Frame, FrameError, StreamEvent, StreamEventKind, redact_serde_error,
};
use crate::ids::{NativeItemKey, TASK_ID_PREFIX};
use crate::log_fields::display_opt;

/// What the mapper answers an `ExitPlanMode` / `EnterPlanMode` ask with: Giskard owns the mode.
const PLAN_MODE_DENIAL: &str =
    "Giskard chooses the mode per turn; present the plan as this turn's answer.";

/// A rate-limit window used at least this much is surfaced as a notice.
const RATE_LIMIT_NOTICE_UTILIZATION: f64 = 0.9;

/// The tool whose `tool_use` block delegates to a sub-agent and mints a route.
const AGENT_TOOL: &str = "Agent";
/// The `Agent` call's `model` that asks for the spawning conversation's model; it names none.
const INHERIT_MODEL: &str = "inherit";
/// The `message.model` of an `assistant` message the CLI wrote itself (an API error's text), with
/// zero usage: no model ran it.
const SYNTHETIC_MODEL: &str = "<synthetic>";
/// `result.origin.kind` of the turn the CLI runs on its own once a background task ended.
const TASK_NOTIFICATION_ORIGIN: &str = "task-notification";

/// Where a frame's items belong: the child process's own thread, or a sub-agent route.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Route {
    Primary,
    /// A sub-agent route, keyed by its `Agent` call's tool-use id.
    Task(String),
}

impl Route {
    /// The `route` log field: `task:<id>` for a sub-agent route, absent for the primary.
    fn label(&self) -> Option<String> {
        match self {
            Route::Primary => None,
            Route::Task(id) => Some(format!("{TASK_ID_PREFIX}{id}")),
        }
    }
}

/// Why `route_task_id` found no running sub-agent for a thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteLookup {
    /// The thread is not one of this mapper's sub-agent routes.
    NotARoute,
    /// The route's turn already ended: there is nothing left to stop.
    Ended,
}

/// Why a turn was opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnKind {
    /// A user message the adapter wrote.
    User,
    /// A `/compact` the adapter wrote for `compact_thread`.
    Compaction,
}

/// What the adapter must do with one mapped frame.
// Almost every output is an `Event`; boxing it would allocate per event to shrink the rare rest.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum MapperOutput {
    Event(AgentEvent),
    /// A control_response the adapter must write, needing no user: a denied `ExitPlanMode`.
    Reply(Value),
    /// A control_response to a request the adapter sent; it correlates the waiter itself.
    ControlResponse {
        request_id: String,
        payload: Value,
    },
    /// An approval the adapter must remember until `respond_approval` answers it.
    PendingApproval {
        id: ApprovalId,
        request_id: String,
        tool_use_id: Option<String>,
        tool_name: String,
        /// The ask's raw `permission_suggestions` (`[]` when absent or `null`), kept verbatim so
        /// `AcceptForSession` can echo its `addRules` entries without retyping them.
        suggestions: Vec<Value>,
        /// The thread the ask was published on: the primary's, or a sub-agent route's.
        thread: ThreadId,
    },
    /// A server request the adapter must remember until `respond_server_request` answers it.
    PendingServerRequest {
        id: ServerRequestId,
        /// The thread the request was published on: the primary's, or a sub-agent route's.
        thread: ThreadId,
        request_id: String,
        /// `can_use_tool` for an `AskUserQuestion` ask, else the control request's own subtype;
        /// it selects the answer's shape.
        subtype: String,
        /// The ask's `input` (an `AskUserQuestion`'s answer echoes its `questions`); `Null` for
        /// every other control request.
        input: Value,
    },
    /// The CLI withdrew one of its asks (`control_cancel_request`). The mapper holds no pending
    /// state, so the adapter looks the request id up.
    CancelRequest {
        request_id: String,
    },
    /// A sub-agent route was minted: the supervisor creates its retained log and publishes it.
    RouteOpened {
        thread: ThreadId,
        harness_thread_id: String,
        parent_harness_thread_id: String,
        /// `input.description`, the thread's name.
        agent_name: Option<String>,
        /// `input.model` of the `Agent` call: the model the delegation asked for, which the
        /// thread shows until the sub-agent's first frame names the one it runs on. `None` when
        /// the call named none (or `inherit`): an agent definition or the CLI's default decides.
        model: Option<ModelRef>,
    },
    /// A dropped route runs again (the CLI resumed its sub-agent): the supervisor appends to the
    /// route's retained log again, which `RouteClosed` had left open.
    RouteReopened {
        thread: ThreadId,
    },
    /// A route was dropped: no further event is mapped onto it and its asks are gone. Its log stays
    /// open for the sub-agent thread.
    RouteClosed {
        thread: ThreadId,
    },
}

/// Maps one child's frames onto `giskard-core` events.
///
/// State is grouped by lifetime, not by key type:
/// - one session, as long as the mapper lives: [`SessionState`] (the session model and permission
///   mode, the effective window, tasks, the notices and unknown frames already reported, the
///   sub-agent routes and the routes already dropped);
/// - one sub-agent route, from its `Agent` call (or a forked skill's `task_started`) until its
///   task has ended and its spawning turn has ended, or the child exits:
///   [`RouteState`] (its minted thread, its task, its link status, its own [`TurnState`]);
/// - one turn, dropped when the turn completes: [`TurnState`] (the turn id and kind, its model,
///   usage, the interrupt flag, denied tool uses);
/// - one item within the turn, dropped with the turn: [`TurnItems`] (item ids, open text and
///   thinking blocks, open tool calls).
///
/// A new field joins the struct whose cleanup site matches its lifetime.
/// The thread a sub-agent native id (`task:<tool_use_id>`) is already bound to in this harness
/// instance, if any: a route an earlier child of the session opened, kept by the façade.
pub type RouteThreads = Arc<dyn Fn(&str) -> Option<ThreadId> + Send + Sync>;

pub struct ClaudeMapper {
    thread: ThreadId,
    harness_thread_id: String,
    workspace_root: PathBuf,
    turn: Option<TurnState>,
    session: SessionState,
    /// Set by the façade (`set_route_threads`); `None` binds every route to a new thread.
    route_threads: Option<RouteThreads>,
}

/// Session-lifetime state: lives as long as the mapper.
#[derive(Default)]
struct SessionState {
    /// `system/init.model`, refreshed by every re-emitted `init`.
    model: Option<String>,
    /// The permission mode the CLI last reported, from `init` or `status`.
    permission_mode: Option<String>,
    /// The permission mode the adapter last set, for drift detection.
    expected_mode: Option<String>,
    /// `autocompact_state.value.effective_window`, when the session reported one.
    effective_window: Option<u32>,
    /// `result.modelUsage[<session model>].contextWindow` from the latest `result`.
    model_usage_window: Option<u32>,
    /// The non-`none` `apiKeySource` already surfaced, so a re-emitted `init` does not repeat it.
    api_key_source_noticed: Option<String>,
    /// `(value, resolvedModel)` of the catalog entries the façade seeded (`note_catalog`), so a
    /// sub-agent's frame model id reads as the picker's selector (`claude-haiku-…` → `haiku`).
    catalog: Vec<(String, String)>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Remember each Claude Code task's type, which only `task_started` carries, and the
    //   sub-agent route of a `local_agent` task, so an ask's `agent_id` or a `task_updated`
    //   resolves to its route.
    // Source of truth: `system/task_started` establishes the entry.
    // Structural reason: `task_updated` and `can_use_tool` name the task id alone; its type says
    //   how its end is handled and its route receives the ask.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: A terminal `task_updated` removes the entry (a background command's
    //   entry waits for its `task_notification`, or child exit), and so does the terminal
    //   `task_notification` of an agent task that got no update (a forked skill that completed);
    //   dropping a route clears the entry's route; shutdown drops the rest.
    tasks: HashMap<String, TaskEntry>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: One sub-agent route per delegating call of this session (an `Agent` call, or the
    //   `Skill` call of a forked skill), keyed by the call's tool-use id.
    // Source of truth: The `assistant` frame carrying the `Agent` tool_use block mints the entry;
    //   for any other call, the `local_agent` task's `task_started` naming it.
    // Structural reason: Forwarded frames name the call (`parent_tool_use_id`), asks name the task
    //   (`agent_id`), and the server names the thread; the entry joins the three.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: Lives from its delegating call until its task has ended and its
    //   spawning turn has ended, or the child exits: dropped by whichever comes last, `end_route`
    //   (a backgrounded route that outlived its turn) or the spawning route's `finish_turn`;
    //   `child_exited` completes and drops the rest.
    routes: HashMap<String, RouteState>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Hold a `SendMessage` run of a sub-agent this process never ran (an earlier child of
    //   the session did), until its first frame names the call its route must be keyed by.
    // Source of truth: `task_started` of an unknown task, named by an open `SendMessage` call.
    // Structural reason: The run's frames name the original `Agent` call (`parent_tool_use_id`),
    //   which no frame of this process carried before.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: The run's first frame opens the route and removes the entry; a
    //   terminal `task_updated` of the task removes it; `child_exited` clears the rest.
    pending_resumes: HashMap<String, PendingResume>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Recognise the frames that trail a dropped route (a killed sub-agent's rejection and
    //   interruption marker), so they are dropped as trailing frames, with the route's thread;
    //   and revive the route when the CLI resumes its sub-agent (`resume_route`).
    // Source of truth: Dropping a route records it under its tool-use id.
    // Structural reason: The CLI emits those frames after the terminal `task_updated`, which may
    //   be after the spawning turn ended and dropped the route; a resumed run's frames still name
    //   the route's call.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: Session lifetime (one entry per delegation); a resume moves the entry
    //   back to `routes`; `child_exited` clears it.
    ended_routes: HashMap<String, RouteState>,
    /// `(type, subtype)` pairs of unknown frames already logged at `warn`.
    unknown_seen: HashSet<(String, Option<String>)>,
}

/// A resumed sub-agent run waiting for its first frame (`SessionState::pending_resumes`).
struct PendingResume {
    /// The route the `SendMessage` call was made on.
    parent: Route,
    /// The call's tool name, for the parent's resume row.
    call: String,
    /// The call's turn on `parent`, which the run's rows report on.
    turn: Option<TurnId>,
    task: TaskStartedMessage,
}

/// One Claude Code task the session reported.
struct TaskEntry {
    kind: TaskType,
    /// The sub-agent route of a `local_agent` task: the route its `tool_use_id` names.
    route: Option<String>,
    /// The command item of a `local_bash` task behind a background `Bash` call.
    command: Option<BackgroundCommand>,
}

/// A `local_bash` task behind a background `Bash` call: the command item it completes later.
struct BackgroundCommand {
    route: Route,
    /// The route's thread, kept because a sub-agent route may be dropped before the task ends.
    thread: ThreadId,
    turn: TurnId,
    item_id: ItemId,
    /// The `tool_use` id, the item's `harness_item_id`.
    tool_use_id: String,
    command: String,
    description: Option<String>,
    /// When the item started, for its duration.
    started_at_ms: Option<i64>,
    /// The output file the `tool_result` text names ("Output is being written to: …"), for a
    /// completion that no `task_notification` carries a path for.
    output_file: Option<PathBuf>,
    /// Set by the terminal `task_updated`; the item completes on the `task_notification` that
    /// follows, else when the supervisor's grace runs out (`settle_background_commands`), or on
    /// child exit.
    terminal: Option<TaskStatus>,
    /// `task_updated.patch.end_time` of the terminal update.
    end_time_ms: Option<u64>,
}

/// What a new route takes from the call or task that delegates.
struct RouteSeed {
    prompt: Option<String>,
    subagent_type: Option<String>,
    description: Option<String>,
    is_backgrounded: bool,
    /// `input.model` of the `Agent` call, `inherit` excluded.
    model: Option<String>,
}

/// One sub-agent route: an `Agent` call (or a forked skill's `Skill` call) of this session and
/// the thread its frames map onto.
struct RouteState {
    /// Minted here; the server adopts it through `claim_native_thread`.
    thread: ThreadId,
    /// `task:<tool_use_id>`.
    harness_thread_id: String,
    /// The route the `Agent` call was made on: `Primary`, or `Task(outer)` when nested.
    parent: Route,
    /// `input.prompt` of the `Agent` call (or `task_started.prompt`), the child's first item.
    prompt: Option<String>,
    /// The delegated prompt was already mapped as the route's `UserMessage`.
    prompt_delivered: bool,
    /// `input.subagent_type` of the `Agent` call, for logs.
    subagent_type: Option<String>,
    /// From `task_started`; `None` until it arrives.
    task_id: Option<String>,
    is_backgrounded: bool,
    /// The adapter sent `stop_task` for this route (`note_stop_sent`).
    stop_sent: bool,
    /// The parent's `Agent` tool call is still open (its `tool_result` has not arrived).
    call_open: bool,
    /// The route's current status as the parent's link reports it.
    status: SubagentStatus,
    /// The turn of the delegating call, on `parent`; `None` when the parent had no turn.
    spawned_turn: Option<TurnId>,
    /// The spawning turn ended while the route still ran (a backgrounded delegation): the route
    /// is dropped at its own end.
    turn_ended: bool,
    /// The `run_id` of a resumed run (`resume_route`, `adopt_pending_resume`): its rows on the
    /// parent are keyed by it, since one turn may see several runs of one task. `None` for the
    /// first run.
    run: Option<String>,
    /// `input.description` of the `Agent` call, the outcome activity's title.
    description: Option<String>,
    /// The child's turn: the same `TurnState` the primary uses, items included.
    turn: Option<TurnState>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Count a child API message's usage once, though every one-block frame repeats it.
    // Source of truth: The route's `assistant` frames, by `message.id`.
    // Structural reason: Forwarded frames carry per-message usage and no stream events.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: Dropped with the route.
    counted_messages: HashSet<String>,
}

impl RouteState {
    /// The route's turn ended (terminal task, child exit, or a foreground route's spawning turn
    /// ended).
    fn ended(&self) -> bool {
        matches!(
            self.status,
            SubagentStatus::Completed | SubagentStatus::Interrupted | SubagentStatus::Failed
        )
    }

    /// The link the parent's `Agent` item carries now.
    fn link(&self, action: SubagentAction) -> SubagentLink {
        SubagentLink {
            harness_thread_id: self.harness_thread_id.clone(),
            path: None,
            initial_prompt: self.prompt.clone(),
            action,
            status: Some(self.status),
            message: None,
        }
    }

    /// The action a link reporting the route's current status names.
    fn outcome_action(&self) -> SubagentAction {
        match self.status {
            SubagentStatus::Interrupted => SubagentAction::Interrupted,
            _ if self.ended() => SubagentAction::Completed,
            _ => SubagentAction::Started,
        }
    }
}

/// Turn-lifetime state: created by `begin_turn` or an external turn, dropped at completion.
struct TurnState {
    id: TurnId,
    kind: TurnKind,
    /// The model the adapter asked for on this turn, through `note_turn_model`; on a route's
    /// turn, the model its latest `assistant` frame named.
    model: Option<ModelRef>,
    /// A route turn's `message.model` as the frame spelled it, so an unchanged model is not looked
    /// up again on every frame.
    native_model: Option<String>,
    /// The latest single API request's usage, which is what occupies the context window now: a
    /// `message_delta`'s, then the last `result.usage.iterations` entry. `TurnUsageUpdated`
    /// carries it.
    window_usage: TokenUsage,
    /// The turn's `result.usage` (on a route's turn, the sum of its messages' usage), which
    /// `TurnCompleted` carries.
    result_usage: Option<TokenUsage>,
    /// Last `(usage, context_window, model)` emitted as `TurnUsageUpdated`, for dedup.
    emitted_usage: Option<(TokenUsage, Option<u32>, Option<ModelRef>)>,
    /// The adapter sent `interrupt` (or a deny with `interrupt: true`) during this turn.
    interrupt_sent: bool,
    /// The CLI started this turn on its own (`ensure_turn`): no prompt of the adapter's opened it.
    external: bool,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Mark tool uses whose ask was answered deny, so their completion reads `declined`.
    // Source of truth: The mapper's own `ExitPlanMode` denial and `system/permission_denied`.
    // Structural reason: The `tool_result` of a denial does not always say it was one.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: Turn completion drops the set.
    denied_tool_use_ids: HashSet<String>,
    /// The turn's `UserMessage` item was emitted from `prompt`: at the turn's `system/init`, or
    /// at its first answer frame when no `init` came first.
    prompt_acknowledged: bool,
    /// The text the adapter wrote for this turn (`note_prompt`), which the `UserMessage` says.
    prompt: Option<String>,
    items: TurnItems,
}

/// Item-lifetime state within one turn.
#[derive(Default)]
struct TurnItems {
    /// The `message.id` of the stream's current API message, from `message_start`.
    stream_message_id: Option<String>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Number the blocks of one API message across its one-block `assistant` frames.
    // Source of truth: Each `assistant` frame carries the next block(s) of its `message.id`.
    // Structural reason: Frames repeat the envelope with one block each and no block index.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: Turn completion drops the map.
    message_blocks: HashMap<String, u32>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Translate native tool-use ids and message blocks to stable Giskard item ids.
    // Source of truth: First observation mints the item identity.
    // Structural reason: Start, delta and completion of one item arrive in separate frames.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: Turn completion drops the map.
    item_ids: HashMap<NativeItemKey, ItemId>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Track open text and thinking blocks between their stream start and their frame.
    // Source of truth: `content_block_start` / deltas open an entry, the `assistant` frame closes it.
    // Structural reason: A thinking block becomes an item only once it has text.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: The block's `assistant` frame removes it; turn completion drops the rest.
    blocks: HashMap<NativeItemKey, OpenBlock>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Keep what a tool call's completion needs until its `tool_result` arrives.
    // Source of truth: The `assistant` frame's `tool_use` block opens the entry.
    // Structural reason: `tool_result` names only the tool-use id, not the call it answers.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: The `tool_result` removes it; turn completion drops the rest.
    tools: HashMap<String, OpenTool>,
}

struct OpenBlock {
    kind: ItemKind,
    started: bool,
    /// Delta text received so far, used when the block's frame carries none.
    streamed: String,
}

struct OpenTool {
    item_id: ItemId,
    call: ToolKind,
    /// The item's human-readable title (`tool_description`), for every item the call produces.
    description: Option<String>,
    /// When its `ItemStarted` was emitted.
    started_at_ms: Option<i64>,
}

enum ToolKind {
    Command {
        command: String,
    },
    FileChange {
        path: PathBuf,
    },
    Call {
        name: String,
        server: Option<String>,
        input: Value,
    },
}

impl TurnState {
    fn new(id: TurnId, kind: TurnKind) -> Self {
        Self {
            id,
            kind,
            model: None,
            native_model: None,
            window_usage: TokenUsage::default(),
            result_usage: None,
            emitted_usage: None,
            interrupt_sent: false,
            external: false,
            denied_tool_use_ids: HashSet::new(),
            prompt_acknowledged: false,
            prompt: None,
            items: TurnItems::default(),
        }
    }
}

impl TurnState {
    /// The turn's usage for `TurnCompleted`: the summed results, or the last request's usage for a
    /// turn that ended without any `result` (one superseded by a new turn).
    fn completed_usage(&self) -> TokenUsage {
        self.result_usage.unwrap_or(self.window_usage)
    }
}

impl SessionState {
    /// The context window to report: the session's effective window, else the model's window from
    /// the latest `result.modelUsage`, else unknown.
    fn context_window(&self) -> Option<u32> {
        self.effective_window.or(self.model_usage_window)
    }
}

type Out = Vec<MapperOutput>;

impl ClaudeMapper {
    pub fn new(thread: ThreadId, harness_thread_id: String, workspace_root: PathBuf) -> Self {
        Self {
            thread,
            harness_thread_id,
            workspace_root,
            turn: None,
            session: SessionState::default(),
            route_threads: None,
        }
    }

    /// How a route's native id finds the thread an earlier child of this session bound it to, so
    /// a sub-agent this process resumes runs on that thread (`adopt_pending_resume`).
    pub fn set_route_threads(&mut self, route_threads: RouteThreads) {
        self.route_threads = Some(route_threads);
    }

    /// The adapter opened a turn for a user message it just wrote. Returns the `TurnStarted`.
    ///
    /// A turn already active is a caller bug: it is completed as `Failed` first.
    pub fn begin_turn(&mut self, turn: TurnId, kind: TurnKind) -> Vec<MapperOutput> {
        let mut out = Vec::new();
        if let Some(previous) = self.turn.as_ref().map(|state| state.id) {
            error!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = %turn,
                previous_turn_id = %previous,
                action = "begin_turn",
                "a turn began while another was active; failing the previous turn"
            );
            self.finish_turn(
                TurnStatus {
                    kind: TurnStatusKind::Failed,
                    message: Some("superseded by a new turn".into()),
                },
                None,
                &mut out,
            );
        }
        self.turn = Some(TurnState::new(turn, kind));
        debug!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = %turn,
            turn_kind = ?kind,
            action = "begin_turn",
            "turn started"
        );
        out.push(self.event(AgentEvent::TurnStarted {
            thread: self.thread,
            turn,
        }));
        out
    }

    /// The adapter sent `interrupt`; the next error-shaped result is `Interrupted`, not `Failed`.
    pub fn note_interrupt_sent(&mut self) {
        match self.turn.as_mut() {
            Some(turn) => turn.interrupt_sent = true,
            None => warn!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                action = "note_interrupt_sent",
                "interrupt noted with no active turn"
            ),
        }
    }

    /// The model the adapter asked for on this turn, so `TurnUsageUpdated.model` can be set.
    pub fn note_turn_model(&mut self, model: ModelRef) {
        match self.turn.as_mut() {
            Some(turn) => turn.model = Some(model),
            None => warn!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                action = "note_turn_model",
                "turn model noted with no active turn"
            ),
        }
    }

    /// The adapter answered this tool use's ask with a deny, so its `tool_result` completes the
    /// item as `declined`. `thread` is the thread the ask was published on, which names the route
    /// whose turn holds the tool use.
    pub fn note_denied(&mut self, thread: ThreadId, tool_use_id: &str) {
        let route = self.route_of_thread(thread).unwrap_or(Route::Primary);
        match self.turn_of_mut(&route) {
            Some(turn) => {
                turn.denied_tool_use_ids.insert(tool_use_id.to_owned());
            }
            None => warn!(
                thread_id = %thread,
                harness_thread_id = %self.harness_thread_id,
                route = display_opt(route.label()),
                action = "note_denied",
                native_item_id = %tool_use_id,
                "a denial noted with no active turn"
            ),
        }
    }

    /// The task id of the live sub-agent route `thread`, for `stop_task`: `None` before its
    /// `task_started`, an error for a thread that is not one of this mapper's routes or whose
    /// turn already ended.
    pub fn route_task_id(&self, thread: ThreadId) -> Result<Option<String>, RouteLookup> {
        if let Some(route) = self.session.routes.values().find(|r| r.thread == thread) {
            if route.ended() {
                return Err(RouteLookup::Ended);
            }
            return Ok(route.task_id.clone());
        }
        if self
            .session
            .ended_routes
            .values()
            .any(|route| route.thread == thread)
        {
            return Err(RouteLookup::Ended);
        }
        Err(RouteLookup::NotARoute)
    }

    /// The adapter sent `stop_task` for route `thread`: its `killed` update is an interruption.
    pub fn note_stop_sent(&mut self, thread: ThreadId) {
        match self
            .session
            .routes
            .values_mut()
            .find(|route| route.thread == thread)
        {
            Some(route) => route.stop_sent = true,
            None => warn!(
                thread_id = %thread,
                harness_thread_id = %self.harness_thread_id,
                action = "note_stop_sent",
                "stop_task noted for a thread that is no sub-agent route"
            ),
        }
    }

    /// The permission mode the adapter just set; an `init` or `status` reporting another one is
    /// drift.
    pub fn set_expected_mode(&mut self, mode: impl Into<String>) {
        self.session.expected_mode = Some(mode.into());
    }

    pub fn active_turn(&self) -> Option<TurnId> {
        self.turn.as_ref().map(|turn| turn.id)
    }

    /// Whether any sub-agent route is live.
    pub fn has_routes(&self) -> bool {
        !self.session.routes.is_empty()
    }

    /// The non-`none` `apiKeySource` this thread already surfaced.
    pub fn api_key_source_noticed(&self) -> Option<&str> {
        self.session.api_key_source_noticed.as_deref()
    }

    /// Seed the catalog a sub-agent's frame model id is read against: `(value, resolvedModel)` of
    /// each entry. The `default` alias is skipped, so an id resolves to the entry the picker shows.
    pub fn note_catalog(&mut self, entries: impl IntoIterator<Item = (String, String)>) {
        self.session.catalog = entries
            .into_iter()
            .filter(|(value, _)| value != DEFAULT_ALIAS)
            .collect();
    }

    /// The `ModelRef` a sub-agent frame's `message.model` names: the first catalog entry
    /// resolving to it, else the id itself.
    fn frame_model(&self, native: &str) -> ModelRef {
        let model = self
            .session
            .catalog
            .iter()
            .find(|(_, resolved)| resolved == native)
            .map_or(native, |(value, _)| value.as_str());
        anthropic_model(model)
    }

    /// Seed a respawned child's mapper with the `apiKeySource` its thread already surfaced, so
    /// the notice is not repeated after every respawn.
    pub fn note_api_key_source_noticed(&mut self, source: Option<String>) {
        self.session.api_key_source_noticed = source;
    }

    /// Whether any Claude Code task (a sub-agent or a background shell) has started and not yet
    /// reached a terminal `task_updated`. A `local_bash` task outlives its turn.
    pub fn has_tasks(&self) -> bool {
        !self.session.tasks.is_empty()
    }

    /// How many tasks are open.
    pub fn open_tasks(&self) -> usize {
        self.session.tasks.len()
    }

    /// The background command behind task `task_id`: the thread its item is on and its command
    /// line. `None` for an unknown task, or one that is no background `Bash` call.
    pub fn background_command(&self, task_id: &str) -> Option<(ThreadId, &str)> {
        let command = self.session.tasks.get(task_id)?.command.as_ref()?;
        Some((command.thread, command.command.as_str()))
    }

    /// The active turn of `thread`: the primary's, or a sub-agent route's.
    pub fn active_turn_of(&self, thread: ThreadId) -> Option<TurnId> {
        let route = self.route_of_thread(thread)?;
        self.turn_of(&route).map(|turn| turn.id)
    }

    /// The child process ended (`exit` reads `code 1` or `signal 9`) while a turn may be active.
    ///
    /// No `result` will ever arrive for that turn, so it completes here: `Interrupted` when the
    /// adapter sent an interrupt, else `Failed`, with a message naming the exit. With no active
    /// turn there is nothing to complete.
    pub fn child_exited(&mut self, exit: &str) -> Vec<MapperOutput> {
        let mut out = Vec::new();
        // A background command dies with the child: no notification will come for it.
        let lost: Vec<String> = self
            .session
            .tasks
            .iter()
            .filter(|(_, entry)| entry.command.is_some())
            .map(|(id, _)| id.clone())
            .collect();
        for task_id in lost {
            let Some(command) = self
                .session
                .tasks
                .remove(&task_id)
                .and_then(|entry| entry.command)
            else {
                continue;
            };
            warn!(
                thread_id = %command.thread,
                harness_thread_id = %self.harness_thread_id,
                route = display_opt(command.route.label()),
                turn_id = %command.turn,
                task_id = %task_id,
                tool_call_id = %command.tool_use_id,
                item_id = %command.item_id,
                action = "background_command_lost",
                exit,
                "Claude Code exited while a background command ran; completing it as terminated"
            );
            // What it wrote before the CLI went; with no marker line, no exit code.
            let output =
                self.read_background_output(&task_id, &command, command.output_file.as_deref());
            self.complete_background_command(&task_id, command, "terminated", output, &mut out);
        }
        // Every open route turn ends with the child: no `task_updated` will come for it.
        let primary_interrupted = self.turn.as_ref().is_some_and(|turn| turn.interrupt_sent);
        let open: Vec<String> = self
            .session
            .routes
            .iter()
            .filter(|(_, route)| route.turn.is_some())
            .map(|(id, _)| id.clone())
            .collect();
        for id in open {
            let Some(route) = self.session.routes.get(&id) else {
                continue;
            };
            let (kind, status) = if primary_interrupted || route.stop_sent {
                (TurnStatusKind::Interrupted, SubagentStatus::Interrupted)
            } else {
                (TurnStatusKind::Failed, SubagentStatus::Failed)
            };
            warn!(
                thread_id = %route.thread,
                harness_thread_id = %self.harness_thread_id,
                route = %route.harness_thread_id,
                turn_id = display_opt(route.turn.as_ref().map(|turn| turn.id)),
                task_id = display_opt(route.task_id.as_deref()),
                action = "child_exited",
                exit,
                status = ?kind,
                "Claude Code exited before the sub-agent's turn completed"
            );
            self.finish_route_turn(
                &id,
                status,
                TurnStatus {
                    kind,
                    message: (kind == TurnStatusKind::Failed).then(|| {
                        format!("Claude Code exited ({exit}) before the sub-agent completed")
                    }),
                },
                &mut out,
            );
        }
        self.child_exited_primary(exit, &mut out);
        // What the primary's completion did not drop (routes of an earlier turn, or none active).
        let rest: Vec<String> = self.session.routes.keys().cloned().collect();
        for id in rest {
            self.drop_route(&id, &mut out);
        }
        self.session.ended_routes.clear();
        self.session.pending_resumes.clear();
        out
    }

    fn child_exited_primary(&mut self, exit: &str, out: &mut Out) {
        let Some(turn) = self.turn.as_ref() else {
            debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                action = "child_exited",
                exit,
                "Claude Code exited with no active turn"
            );
            return;
        };
        let kind = if turn.interrupt_sent {
            TurnStatusKind::Interrupted
        } else {
            TurnStatusKind::Failed
        };
        warn!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = %turn.id,
            action = "child_exited",
            exit,
            status = ?kind,
            "Claude Code exited before the turn completed"
        );
        self.finish_turn(
            TurnStatus {
                kind,
                message: Some(format!(
                    "Claude Code exited ({exit}) before the turn completed"
                )),
            },
            None,
            out,
        );
    }

    /// Seed the session's context window before any `result` reported one: a resumed thread's
    /// `get_context_usage.maxTokens`. An `autocompact_state` window and a later
    /// `result.modelUsage` window still take precedence.
    pub fn note_context_window(&mut self, window: u32) {
        if self.session.model_usage_window.is_none() {
            self.session.model_usage_window = Some(window);
        }
    }

    /// Parse and map one stdout line. A line that is not a frame is logged and yields nothing.
    pub fn map_line(&mut self, line: &str) -> Vec<MapperOutput> {
        // The CLI writes stray blank lines between some frames; they carry nothing to diagnose.
        if line.trim().is_empty() {
            debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                bytes = line.len(),
                "skipping a blank stdout line"
            );
            return Vec::new();
        }
        match Frame::parse(line) {
            Ok(frame) => self.map(frame),
            Err(FrameError::NotJson { bytes, error }) => {
                warn!(
                    thread_id = %self.thread,
                    harness_thread_id = %self.harness_thread_id,
                    action = "parse_frame",
                    bytes,
                    line = error.line(),
                    column = error.column(),
                    "skipping a stdout line that is not JSON"
                );
                Vec::new()
            }
            Err(FrameError::NoType) => {
                warn!(
                    thread_id = %self.thread,
                    harness_thread_id = %self.harness_thread_id,
                    action = "parse_frame",
                    bytes = line.len(),
                    "skipping a stdout frame with no type"
                );
                Vec::new()
            }
            Err(FrameError::Untyped {
                r#type,
                subtype,
                error,
            }) => {
                warn!(
                    thread_id = %self.thread,
                    harness_thread_id = %self.harness_thread_id,
                    turn_id = display_opt(self.active_turn()),
                    action = "parse_frame",
                    frame_type = %r#type,
                    frame_subtype = display_opt(subtype.as_deref()),
                    error = %redact_serde_error(&error),
                    "skipping a frame that did not match its type"
                );
                Vec::new()
            }
        }
    }

    pub fn map(&mut self, frame: Frame) -> Vec<MapperOutput> {
        let mut out = Vec::new();
        // The backstop: `system/init` acknowledges the prompt, and an answer that comes without one
        // still must.
        if let Some(frame_type) = answers_the_prompt(&frame) {
            self.acknowledge_prompt(frame_type, frame_type, &mut out);
        }
        match frame {
            Frame::Init(init, _raw) => self.on_init(&init, &mut out),
            Frame::Status(status) => self.on_status(&status, &mut out),
            Frame::TaskStarted(task) => self.on_task_started(task, &mut out),
            Frame::TaskUpdated(task) => self.on_task_updated(task, &mut out),
            Frame::CompactBoundary(boundary) => self.on_compact_boundary(&boundary, &mut out),
            Frame::ApiRetry(retry) => self.on_api_retry(&retry, &mut out),
            Frame::PermissionDenied(denied) => self.on_permission_denied(denied, &mut out),
            Frame::Assistant(message) => self.on_assistant(*message, &mut out),
            Frame::User(message) => self.on_user(*message, &mut out),
            Frame::Stream(stream) => self.on_stream(stream, &mut out),
            Frame::Result(result) => self.on_result(result, &mut out),
            Frame::RateLimit(event) => self.on_rate_limit(&event, &mut out),
            Frame::AutocompactState {
                effective_window,
                threshold,
            } => self.on_autocompact_state(effective_window, threshold),
            Frame::CanUseTool {
                request_id,
                request,
                agent_id,
                raw,
            } => self.on_can_use_tool(request_id, *request, agent_id, &raw, &mut out),
            Frame::ControlRequest {
                request_id,
                subtype,
                raw,
            } => self.on_control_request(request_id, &subtype, raw, &mut out),
            Frame::ControlCancelRequest { request_id } => {
                info!(
                    thread_id = %self.thread,
                    harness_thread_id = %self.harness_thread_id,
                    turn_id = display_opt(self.active_turn()),
                    action = "control_cancel_request",
                    request_id = %request_id,
                    "Claude Code withdrew one of its asks"
                );
                out.push(MapperOutput::CancelRequest { request_id });
            }
            Frame::ControlResponse { request_id, raw } => {
                let payload = match raw {
                    Value::Object(mut object) => object.remove("response").unwrap_or_default(),
                    other => other,
                };
                out.push(MapperOutput::ControlResponse {
                    request_id,
                    payload,
                });
            }
            Frame::TaskNotification(task) => self.on_task_notification(task, &mut out),
            frame @ (Frame::SessionTitleChanged(_) | Frame::Ignored { .. }) => {
                let (frame_type, subtype) = frame.kind();
                debug!(
                    thread_id = %self.thread,
                    harness_thread_id = %self.harness_thread_id,
                    turn_id = display_opt(self.active_turn()),
                    frame_type,
                    frame_subtype = display_opt(subtype),
                    "frame read; no event in this milestone"
                );
            }
            Frame::Unknown { r#type, subtype } => self.on_unknown(r#type, subtype),
        }
        out
    }

    // ---- session-level frames ------------------------------------------------------------------

    fn on_init(&mut self, init: &InitMessage, out: &mut Out) {
        self.session.model = init.model.clone();
        debug!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            model = display_opt(init.model.as_deref()),
            permission_mode = display_opt(init.permission_mode.as_ref().map(|mode| mode.as_str())),
            claude_code_version = display_opt(init.claude_code_version.as_deref()),
            "session initialized"
        );
        // The CLI emits `init` at the top of every prompt it takes, before it decides whether a
        // model call is needed: the prompt is in. A re-emitted `init` mid-turn, a compaction
        // turn's and one outside any turn acknowledge nothing (`acknowledge_prompt`'s gate).
        self.acknowledge_prompt("init", "system", out);
        // A re-emitted `init` (after a backgrounded task, after `/compact`) is the frame most
        // likely to show a mode Giskard did not set.
        if let Some(mode) = &init.permission_mode {
            self.check_mode(mode.as_str(), out);
        }
        let Some(source) = init
            .api_key_source
            .as_ref()
            .filter(|source| !matches!(source, ApiKeySource::None))
        else {
            return;
        };
        let source = source.as_str().to_owned();
        if self.session.api_key_source_noticed.as_deref() == Some(source.as_str()) {
            return;
        }
        warn!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            action = "api_key_source",
            api_key_source = %source,
            "Claude Code authenticated with an API key source; usage is billed to that credential"
        );
        out.push(self.event(AgentEvent::Notice {
            thread: self.thread,
            turn: None,
            message: format!(
                "Claude Code authenticated with {source}; usage is billed to that credential, \
                 not to the subscription"
            ),
        }));
        self.session.api_key_source_noticed = Some(source);
    }

    fn on_status(&mut self, status: &StatusMessage, out: &mut Out) {
        debug!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = display_opt(self.active_turn()),
            status = display_opt(status.status.as_ref().map(|status| status.as_str())),
            compact_result = display_opt(status.compact_result.as_deref()),
            "status"
        );
        if let Some(mode) = &status.permission_mode {
            self.check_mode(mode.as_str(), out);
        }
    }

    /// Record the mode the CLI reports and compare it with the one the adapter last set.
    fn check_mode(&mut self, mode: &str, out: &mut Out) {
        let mode = mode.to_owned();
        self.session.permission_mode = Some(mode.clone());
        let Some(expected) = self.session.expected_mode.clone() else {
            return;
        };
        if expected == mode {
            return;
        }
        warn!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = display_opt(self.active_turn()),
            action = "permission_mode_drift",
            permission_mode = %mode,
            expected_mode = %expected,
            "Claude Code reports a permission mode Giskard did not set"
        );
        out.push(self.event(AgentEvent::Notice {
            thread: self.thread,
            turn: self.active_turn(),
            message: format!(
                "Claude Code switched its permission mode to {mode}; Giskard set {expected}"
            ),
        }));
    }

    fn on_autocompact_state(&mut self, effective_window: u64, threshold: Option<u64>) {
        match u32::try_from(effective_window) {
            Ok(window) => self.session.effective_window = Some(window),
            Err(_) => warn!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                effective_window,
                "ignoring an effective context window that does not fit a u32"
            ),
        }
        debug!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            effective_window,
            threshold = display_opt(threshold),
            "autocompact state"
        );
    }

    fn on_rate_limit(&mut self, event: &RateLimitEvent, out: &mut Out) {
        let info = &event.rate_limit_info;
        let windows = info.unified_windows.as_ref();
        let hot: Vec<(&str, f64)> = [
            ("five-hour", windows.and_then(|w| w.five_hour.as_ref())),
            ("seven-day", windows.and_then(|w| w.seven_day.as_ref())),
            (
                "seven-day overage",
                windows.and_then(|w| w.seven_day_overage_included.as_ref()),
            ),
        ]
        .into_iter()
        .filter_map(|(name, window)| Some((name, window?.utilization)))
        .filter(|(_, utilization)| *utilization >= RATE_LIMIT_NOTICE_UTILIZATION)
        .collect();
        let allowed = matches!(info.status, RateLimitStatus::Allowed);
        if allowed && hot.is_empty() {
            debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                status = %info.status,
                utilization = display_opt(info.utilization),
                "rate limit event"
            );
            return;
        }
        warn!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = display_opt(self.active_turn()),
            action = "rate_limit",
            status = %info.status,
            "Claude Code is near or at a rate limit"
        );
        let mut message = format!("Claude Code rate limit status: {}", info.status);
        for (name, utilization) in hot {
            message.push_str(&format!("; {name} window {:.0}% used", utilization * 100.0));
        }
        out.push(self.event(AgentEvent::Notice {
            thread: self.thread,
            turn: self.active_turn(),
            message,
        }));
    }

    fn on_api_retry(&mut self, retry: &ApiRetryMessage, out: &mut Out) {
        warn!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = display_opt(self.active_turn()),
            action = "api_retry",
            attempt = retry.attempt,
            max_retries = retry.max_retries,
            error_status = display_opt(retry.error_status),
            "Claude Code is retrying an API request"
        );
        out.push(self.event(AgentEvent::Notice {
            thread: self.thread,
            turn: self.active_turn(),
            message: format!(
                "retrying after {} (attempt {} of {})",
                retry.error, retry.attempt, retry.max_retries
            ),
        }));
    }

    fn on_permission_denied(&mut self, denied: PermissionDeniedMessage, out: &mut Out) {
        info!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = display_opt(self.active_turn()),
            action = "permission_denied",
            tool_name = %denied.tool_name,
            native_item_id = %denied.tool_use_id,
            "Claude Code denied a tool use"
        );
        let reason = denied.decision_reason.as_deref().unwrap_or(&denied.message);
        let message = format!("Claude Code denied {}: {reason}", denied.tool_name);
        // A sub-agent's tool use is denied on the route whose turn holds it.
        let route = self
            .route_with_open_tool(&denied.tool_use_id)
            .unwrap_or(Route::Primary);
        if let Some(turn) = self.turn_of_mut(&route) {
            turn.denied_tool_use_ids.insert(denied.tool_use_id);
        }
        out.push(self.event(AgentEvent::Notice {
            thread: self.thread_of(&route),
            turn: self.turn_of(&route).map(|turn| turn.id),
            message,
        }));
    }

    fn on_unknown(&mut self, frame_type: String, subtype: Option<String>) {
        let first = self
            .session
            .unknown_seen
            .insert((frame_type.clone(), subtype.clone()));
        if first {
            warn!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = display_opt(self.active_turn()),
                frame_type = %frame_type,
                frame_subtype = display_opt(subtype.as_deref()),
                "skipping a frame kind this adapter does not know; further ones log at debug"
            );
        } else {
            debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = display_opt(self.active_turn()),
                frame_type = %frame_type,
                frame_subtype = display_opt(subtype.as_deref()),
                "skipping a frame kind this adapter does not know"
            );
        }
    }

    // ---- tasks ---------------------------------------------------------------------------------

    fn on_task_started(&mut self, task: TaskStartedMessage, out: &mut Out) {
        let task_type = task
            .task_type
            .clone()
            .unwrap_or_else(|| TaskType::Unknown(String::new()));
        let agent = matches!(task_type, TaskType::LocalAgent);
        // A `local_agent` task names the `Agent` call that minted its route, or another open call
        // that runs a sub-agent (a forked skill's `Skill` call), whose route is minted now. A
        // task that already has a route is a resumed sub-agent (`SendMessage`): a new run of the
        // same task, whose frames still name the route's call.
        let mut resumed_with_prompt = false;
        let route = match &task.tool_use_id {
            Some(id) if agent => match self.resume_route(id, &task, out) {
                Some((route, with_prompt)) => {
                    resumed_with_prompt = with_prompt;
                    Some(route)
                }
                None if self.session.routes.contains_key(id) => Some(id.clone()),
                None if self.defer_resume(id, &task) => None,
                None => self.mint_task_route(id, &task, out),
            },
            _ => None,
        };
        // A backgrounded `local_bash` task names the background `Bash` call it runs. A foreground
        // `Bash` call that runs a while gets a task too (`is_backgrounded: false`), which ends with
        // a notification and no update; its item completes on its own `tool_result`.
        let command = match (&task_type, &task.tool_use_id, task.is_backgrounded) {
            (TaskType::LocalBash, Some(tool_use_id), Some(true)) => {
                self.background_command_of(tool_use_id)
            }
            _ => None,
        };
        info!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = display_opt(self.active_turn()),
            action = "task_started",
            task_id = %task.task_id,
            task_type = %task_type,
            is_backgrounded = display_opt(task.is_backgrounded),
            native_item_id = display_opt(task.tool_use_id.as_deref()),
            route = display_opt(route.as_ref().map(|id| format!("{TASK_ID_PREFIX}{id}"))),
            spawn_depth = display_opt(task.spawn_depth),
            background_command = command.is_some(),
            "Claude Code task started"
        );
        self.session.tasks.insert(
            task.task_id.clone(),
            TaskEntry {
                kind: task_type,
                route: route.clone(),
                command,
            },
        );
        if let Some(id) = &route {
            self.start_route(id, &task, out);
            if resumed_with_prompt {
                self.deliver_resumed_prompt(id, &task, out);
            }
        }
    }

    /// `task_started` of a route's agent task: record the task and open the route's turn.
    fn start_route(&mut self, id: &str, task: &TaskStartedMessage, out: &mut Out) {
        let Some(route) = self.session.routes.get_mut(id) else {
            return;
        };
        route.task_id = Some(task.task_id.clone());
        route.is_backgrounded = task.is_backgrounded == Some(true);
        // A resumed run's prompt is its message (or the `<task-notification>` that woke it), not
        // the delegated prompt the link reports.
        if route.prompt.is_none() && route.run.is_none() {
            route.prompt = task.prompt.clone();
        }
        if route.ended() || route.turn.is_some() {
            return;
        }
        route.status = SubagentStatus::Running;
        let turn = TurnId::new();
        route.turn = Some(TurnState::new(turn, TurnKind::User));
        let thread = route.thread;
        info!(
            thread_id = %thread,
            harness_thread_id = %self.harness_thread_id,
            route = %route.harness_thread_id,
            turn_id = %turn,
            task_id = %task.task_id,
            is_backgrounded = route.is_backgrounded,
            subagent_type = display_opt(route.subagent_type.as_deref()),
            action = "route_turn_started",
            "sub-agent turn started"
        );
        out.push(self.event(AgentEvent::TurnStarted { thread, turn }));
    }

    fn on_task_updated(&mut self, task: TaskUpdatedMessage, out: &mut Out) {
        let Some(status) = task.patch.status.clone() else {
            debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                task_id = %task.task_id,
                "task updated without a status"
            );
            return;
        };
        let terminal = is_terminal(&status);
        // A background command's entry stays until its notification, which carries the output.
        let background = terminal
            && self
                .session
                .tasks
                .get(&task.task_id)
                .is_some_and(|entry| entry.command.is_some());
        let entry = if terminal && !background {
            self.session.tasks.remove(&task.task_id)
        } else {
            None
        };
        let task_type = match &entry {
            Some(entry) => Some(entry.kind.clone()),
            None => self
                .session
                .tasks
                .get(&task.task_id)
                .map(|entry| entry.kind.clone()),
        };
        info!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = display_opt(self.active_turn()),
            action = "task_updated",
            task_id = %task.task_id,
            task_type = display_opt(task_type.as_ref()),
            status = %status,
            background_command = background,
            "Claude Code task updated"
        );
        if !terminal {
            return;
        }
        self.drop_pending_resume(&task.task_id);
        if background {
            if let Some(command) = self
                .session
                .tasks
                .get_mut(&task.task_id)
                .and_then(|entry| entry.command.as_mut())
            {
                command.terminal = Some(status);
                command.end_time_ms = task.patch.end_time;
            }
            return;
        }
        let error = task.patch.error.as_deref();
        match entry {
            Some(TaskEntry {
                route: Some(id), ..
            }) => self.end_route(&id, &task.task_id, error, &status, out),
            Some(_) => {}
            // `stop_task` on a task already terminal still emits `killed` for it.
            None => debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                task_id = %task.task_id,
                status = %status,
                action = "task_updated",
                "terminal update for an unknown or already ended task; ignored"
            ),
        }
    }

    /// The background command a `local_bash` task runs: the open `Command` tool `tool_use_id`
    /// names on any route.
    fn background_command_of(&self, tool_use_id: &str) -> Option<BackgroundCommand> {
        let route = if self
            .turn
            .as_ref()
            .is_some_and(|turn| turn.items.tools.contains_key(tool_use_id))
        {
            Route::Primary
        } else {
            self.route_with_open_tool(tool_use_id)?
        };
        let turn = self.turn_of(&route)?;
        let open = turn.items.tools.get(tool_use_id)?;
        let ToolKind::Command { command } = &open.call else {
            return None;
        };
        Some(BackgroundCommand {
            thread: self.thread_of(&route),
            turn: turn.id,
            item_id: open.item_id,
            tool_use_id: tool_use_id.to_owned(),
            command: command.clone(),
            description: open.description.clone(),
            started_at_ms: open.started_at_ms,
            output_file: None,
            terminal: None,
            end_time_ms: None,
            route,
        })
    }

    /// `task_notification`: for a background command, its end, with the output file to read; for
    /// an agent task still open (a forked skill's, which gets no terminal update), its end.
    fn on_task_notification(&mut self, task: TaskNotificationMessage, out: &mut Out) {
        if is_terminal(&task.status) {
            self.drop_pending_resume(&task.task_id);
        }
        let entry = self.session.tasks.get(&task.task_id);
        if is_terminal(&task.status)
            && entry.is_some_and(|entry| entry.kind == TaskType::LocalAgent)
        {
            let route = self
                .session
                .tasks
                .remove(&task.task_id)
                .and_then(|entry| entry.route);
            info!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = display_opt(self.active_turn()),
                task_id = %task.task_id,
                status = %task.status,
                route = display_opt(route.as_ref().map(|id| format!("{TASK_ID_PREFIX}{id}"))),
                action = "task_notification",
                "an agent task ended on its notification, with no terminal update"
            );
            if let Some(id) = route {
                self.end_route(&id, &task.task_id, None, &task.status, out);
            }
            return;
        }
        let command = entry.is_some_and(|entry| entry.command.is_some());
        if !command && entry.is_some_and(|entry| entry.kind == TaskType::LocalBash) {
            // A foreground command's task: no terminal update comes for it, so this ends it.
            self.session.tasks.remove(&task.task_id);
            debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = display_opt(self.active_turn()),
                task_id = %task.task_id,
                status = %task.status,
                action = "task_notification",
                "a foreground command's task ended; its tool_result completes the item"
            );
            return;
        }
        if !command {
            debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = display_opt(self.active_turn()),
                task_id = %task.task_id,
                status = %task.status,
                action = "task_notification",
                "notification for a task that is no background command; no event"
            );
            return;
        }
        let Some(command) = self
            .session
            .tasks
            .remove(&task.task_id)
            .and_then(|entry| entry.command)
        else {
            return;
        };
        let status = match &command.terminal {
            Some(status) => status.clone(),
            None => {
                warn!(
                    thread_id = %command.thread,
                    harness_thread_id = %self.harness_thread_id,
                    turn_id = %command.turn,
                    task_id = %task.task_id,
                    tool_call_id = %command.tool_use_id,
                    status = %task.status,
                    action = "task_notification",
                    "notification for a background command whose terminal update was not seen"
                );
                task.status.clone()
            }
        };
        let status = command_status(&status);
        let path = task
            .output_file
            .as_deref()
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
            .or_else(|| command.output_file.clone());
        let output = self.read_background_output(&task.task_id, &command, path.as_deref());
        info!(
            thread_id = %command.thread,
            harness_thread_id = %self.harness_thread_id,
            route = display_opt(command.route.label()),
            turn_id = %command.turn,
            task_id = %task.task_id,
            tool_call_id = %command.tool_use_id,
            item_id = %command.item_id,
            status,
            exit_code = display_opt(output.exit_code),
            output_bytes = output.text.len(),
            action = "task_notification",
            "background command ended"
        );
        self.complete_background_command(&task.task_id, command, status, output, out);
    }

    /// Whether a background command has its terminal update and still waits for its
    /// notification: the supervisor's grace runs while this holds.
    pub fn awaiting_notification(&self) -> bool {
        self.session.tasks.values().any(|entry| {
            entry
                .command
                .as_ref()
                .is_some_and(|command| command.terminal.is_some())
        })
    }

    /// Complete every background command whose terminal update arrived but whose notification did
    /// not, from the update and the output file its `tool_result` named. The notification has
    /// followed the update within milliseconds in every recording (a completion, a failure, a
    /// kill by `stop_task` or by the model's `TaskStop`), but nothing promises it, and without it
    /// the item would stay running and the task keep the child busy. Called by the supervisor's
    /// grace timer only: no frame settles it, since the notification can still be on its way when
    /// any other frame (a `result` among them) arrives, and it would then be dropped.
    pub fn settle_background_commands(&mut self) -> Vec<MapperOutput> {
        let mut out = Vec::new();
        let settled: Vec<String> = self
            .session
            .tasks
            .iter()
            .filter(|(_, entry)| {
                entry
                    .command
                    .as_ref()
                    .is_some_and(|command| command.terminal.is_some())
            })
            .map(|(id, _)| id.clone())
            .collect();
        for task_id in settled {
            let Some(command) = self
                .session
                .tasks
                .remove(&task_id)
                .and_then(|entry| entry.command)
            else {
                continue;
            };
            let status = command.terminal.as_ref().map_or("failed", command_status);
            let path = command.output_file.clone();
            let output = self.read_background_output(&task_id, &command, path.as_deref());
            warn!(
                thread_id = %command.thread,
                harness_thread_id = %self.harness_thread_id,
                route = display_opt(command.route.label()),
                turn_id = %command.turn,
                task_id = %task_id,
                tool_call_id = %command.tool_use_id,
                item_id = %command.item_id,
                status,
                exit_code = display_opt(output.exit_code),
                output_bytes = output.text.len(),
                action = "task_notification",
                "no notification followed a background command's terminal update; completing it \
                 from the update"
            );
            self.complete_background_command(&task_id, command, status, output, &mut out);
        }
        out
    }

    /// A background command's output file, best-effort: no path or an unreadable file is empty
    /// output, with a `warn`.
    fn read_background_output(
        &self,
        task_id: &str,
        command: &BackgroundCommand,
        path: Option<&Path>,
    ) -> BackgroundOutput {
        let Some(path) = path else {
            warn!(
                thread_id = %command.thread,
                harness_thread_id = %self.harness_thread_id,
                task_id = %task_id,
                action = "background_output",
                "no output file is known for the background command; reporting no output"
            );
            return BackgroundOutput::default();
        };
        match background_output(path) {
            Ok(output) => output,
            Err(error) => {
                warn!(
                    thread_id = %command.thread,
                    harness_thread_id = %self.harness_thread_id,
                    task_id = %task_id,
                    path = %path.display(),
                    error = %error,
                    action = "background_output",
                    "could not read a background command's output file; reporting no output"
                );
                BackgroundOutput::default()
            }
        }
    }

    /// The second, terminal completion of a background command's item, on its original turn.
    fn complete_background_command(
        &mut self,
        task_id: &str,
        command: BackgroundCommand,
        status: &str,
        output: BackgroundOutput,
        out: &mut Out,
    ) {
        let duration_ms = match (command.started_at_ms, command.end_time_ms) {
            (Some(start), Some(end)) => i64::try_from(end)
                .ok()
                .and_then(|end| end.checked_sub(start))
                .filter(|duration| *duration >= 0),
            _ => None,
        };
        out.push(self.event(AgentEvent::ItemCompleted {
            thread: command.thread,
            turn: command.turn,
            item: Item {
                id: command.item_id,
                harness_item_id: command.tool_use_id,
                payload: ItemPayload::CommandExecution {
                    description: command.description,
                    command: command.command,
                    cwd: self.workspace_root.clone(),
                    output: output.text,
                    output_truncated: output.original_bytes.is_some(),
                    output_original_bytes: output.original_bytes,
                    output_original_lines: None,
                    exit_code: output.exit_code,
                    status: Some(status.into()),
                    process_id: Some(task_id.to_owned()),
                    duration_ms,
                },
                created_at: Utc::now(),
            },
        }));
    }

    // ---- turns ---------------------------------------------------------------------------------

    /// The active turn, or a turn the CLI started on its own (the continuation it runs once a
    /// background task ended), which the mapper opens itself.
    fn ensure_turn(&mut self, frame_type: &str, out: &mut Out) -> TurnId {
        if let Some(turn) = &self.turn {
            return turn.id;
        }
        let turn = TurnId::new();
        info!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = %turn,
            action = "external_turn",
            frame_type,
            "Claude Code started a turn on its own"
        );
        let mut state = TurnState::new(turn, TurnKind::User);
        state.external = true;
        self.turn = Some(state);
        out.push(self.event(AgentEvent::TurnStarted {
            thread: self.thread,
            turn,
        }));
        turn
    }

    /// A `result` ends the turn, whatever tasks it started: a backgrounded sub-agent runs on, on
    /// its own route, and the CLI serves the next prompt meanwhile.
    fn on_result(&mut self, result: Box<ResultMessage>, out: &mut Out) {
        let turn_id = self.ensure_turn("result", out);
        self.record_model_usage(&result, turn_id);
        let Some(turn) = self.turn.as_mut() else {
            return;
        };
        if let Some(usage) = &result.usage {
            turn.result_usage
                .get_or_insert_with(TokenUsage::default)
                .add(&token_usage(usage));
            turn.window_usage = last_request_usage(usage);
        }
        // The CLI's continuation after a background task is a turn of its own; its `init` carries
        // no marker, so its `result` is the first frame that says what the turn was.
        let origin = result.origin.as_ref().map(|origin| origin.kind.as_str());
        match origin {
            Some(TASK_NOTIFICATION_ORIGIN) => info!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = %turn_id,
                external = turn.external,
                origin = TASK_NOTIFICATION_ORIGIN,
                action = "continuation_turn",
                "the turn was the CLI's continuation after a background task ended"
            ),
            Some(kind) => info!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = %turn_id,
                external = turn.external,
                origin = %kind,
                action = "result_origin",
                "the result names an origin"
            ),
            None => {}
        }
        let status = result_status(&result, turn.interrupt_sent);
        self.emit_usage(&Route::Primary, out);
        self.finish_turn(status, origin, out);
    }

    fn record_model_usage(&mut self, result: &ResultMessage, turn: TurnId) {
        let Some(model_usage) = &result.model_usage else {
            return;
        };
        // `AgentEvent` has no per-model usage channel: the turn's usage is `result.usage`, and a
        // second model's tokens are visible only here.
        for (model, entry) in model_usage {
            debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = %turn,
                model = %model,
                input_tokens = entry.input_tokens,
                cache_creation_input_tokens = entry.cache_creation_input_tokens,
                cache_read_input_tokens = entry.cache_read_input_tokens,
                output_tokens = entry.output_tokens,
                context_window = entry.context_window,
                "model usage"
            );
        }
        let Some(entry) = self
            .session
            .model
            .as_ref()
            .and_then(|model| model_usage.get(model))
        else {
            return;
        };
        match u32::try_from(entry.context_window) {
            Ok(0) => {}
            Ok(window) => self.session.model_usage_window = Some(window),
            Err(_) => warn!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = %turn,
                context_window = entry.context_window,
                "ignoring a model context window that does not fit a u32"
            ),
        }
    }

    /// Emit `TurnUsageUpdated` for the route's active turn unless the same `(usage, window)` pair
    /// was the last one emitted.
    fn emit_usage(&mut self, route: &Route, out: &mut Out) {
        // The session's window is the parent's: its effective (auto-compact) window, which an agent
        // definition's `autoCompactWindow` overrides, and forwarded frames carry no window of
        // their own. A sub-agent's window is therefore unknown, never borrowed.
        let window = match route {
            Route::Primary => self.session.context_window(),
            Route::Task(_) => None,
        };
        let thread = self.thread_of(route);
        let Some(turn) = self.turn_of_mut(route) else {
            return;
        };
        let key = (turn.window_usage, window, turn.model.clone());
        if turn.emitted_usage.as_ref() == Some(&key) {
            return;
        }
        turn.emitted_usage = Some(key);
        let event = AgentEvent::TurnUsageUpdated {
            thread,
            turn: turn.id,
            usage: turn.window_usage,
            context_window: window,
            model: turn.model.clone(),
        };
        out.push(MapperOutput::Event(event));
    }

    /// Complete the active turn and drop its state. `origin` is the `result.origin.kind` that ended
    /// it, if any.
    fn finish_turn(&mut self, status: TurnStatus, origin: Option<&str>, out: &mut Out) {
        let Some(turn) = self.turn.take() else {
            return;
        };
        info!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = %turn.id,
            turn_kind = ?turn.kind,
            action = "turn_completed",
            status = ?status.kind,
            origin = display_opt(origin),
            input_tokens = turn.completed_usage().input,
            output_tokens = turn.completed_usage().output,
            "turn completed"
        );
        out.push(MapperOutput::Event(AgentEvent::TurnCompleted {
            thread: self.thread,
            turn: turn.id,
            usage: turn.completed_usage(),
            status,
        }));
        // The routes it spawned whose task ended go; a backgrounded one still running outlives it.
        self.drop_spawned_routes(&Route::Primary, out);
    }

    fn on_compact_boundary(&mut self, boundary: &CompactBoundaryMessage, out: &mut Out) {
        let metadata = &boundary.compact_metadata;
        let Some(turn) = self.active_turn() else {
            warn!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                trigger = %metadata.trigger,
                pre_tokens = metadata.pre_tokens,
                "skipping a compact boundary with no active turn"
            );
            return;
        };
        let detail = match metadata.post_tokens {
            Some(post) => format!(
                "{} → {post} tokens ({})",
                metadata.pre_tokens, metadata.trigger
            ),
            None => format!("{} tokens ({})", metadata.pre_tokens, metadata.trigger),
        };
        let metadata_json = match serde_json::to_value(metadata) {
            Ok(value) => Some(value),
            Err(error) => {
                warn!(
                    thread_id = %self.thread,
                    harness_thread_id = %self.harness_thread_id,
                    turn_id = %turn,
                    error = %error,
                    "compact metadata did not serialize; the activity carries none"
                );
                None
            }
        };
        let id = ItemId::new();
        let harness_item_id = format!(
            "compact_boundary:{}",
            boundary.uuid.clone().unwrap_or_else(|| id.to_string())
        );
        info!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = %turn,
            action = "compact_boundary",
            pre_tokens = metadata.pre_tokens,
            post_tokens = display_opt(metadata.post_tokens),
            trigger = %metadata.trigger,
            "context compacted"
        );
        out.push(self.event(AgentEvent::ItemCompleted {
            thread: self.thread,
            turn,
            item: Item {
                id,
                harness_item_id,
                payload: ItemPayload::Activity {
                    title: "Context compacted".into(),
                    detail: Some(detail),
                    metadata: metadata_json,
                    subagent: None,
                },
                created_at: Utc::now(),
            },
        }));
    }

    // ---- sub-agent routes ----------------------------------------------------------------------

    /// The thread a route's events are emitted on.
    fn thread_of(&self, route: &Route) -> ThreadId {
        match route {
            Route::Primary => self.thread,
            Route::Task(id) => self
                .session
                .routes
                .get(id)
                .map_or(self.thread, |route| route.thread),
        }
    }

    fn turn_of(&self, route: &Route) -> Option<&TurnState> {
        match route {
            Route::Primary => self.turn.as_ref(),
            Route::Task(id) => self
                .session
                .routes
                .get(id)
                .and_then(|route| route.turn.as_ref()),
        }
    }

    fn turn_of_mut(&mut self, route: &Route) -> Option<&mut TurnState> {
        match route {
            Route::Primary => self.turn.as_mut(),
            Route::Task(id) => self
                .session
                .routes
                .get_mut(id)
                .and_then(|route| route.turn.as_mut()),
        }
    }

    /// The route whose thread is `thread`: the primary, a live route, or `None`.
    fn route_of_thread(&self, thread: ThreadId) -> Option<Route> {
        if thread == self.thread {
            return Some(Route::Primary);
        }
        self.session
            .routes
            .iter()
            .find(|(_, route)| route.thread == thread)
            .map(|(id, _)| Route::Task(id.clone()))
    }

    /// The live route whose turn holds the open tool call `tool_use_id`.
    fn route_with_open_tool(&self, tool_use_id: &str) -> Option<Route> {
        self.session
            .routes
            .iter()
            .find(|(_, route)| {
                route
                    .turn
                    .as_ref()
                    .is_some_and(|turn| turn.items.tools.contains_key(tool_use_id))
            })
            .map(|(id, _)| Route::Task(id.clone()))
    }

    /// Mint the route of an `Agent` call made on `parent`. Returns its link for the call's
    /// `ToolCallStart`, or `None` when the call already has one.
    fn mint_route(
        &mut self,
        parent: &Route,
        tool_use_id: &str,
        input: &Value,
        out: &mut Out,
    ) -> Option<SubagentLink> {
        if self.session.routes.contains_key(tool_use_id) {
            warn!(
                thread_id = %self.thread_of(parent),
                harness_thread_id = %self.harness_thread_id,
                native_item_id = %tool_use_id,
                action = "route_opened",
                "an Agent call already has a sub-agent route; not minting another"
            );
            return None;
        }
        let text = |key: &str| input.get(key).and_then(Value::as_str).map(str::to_owned);
        let seed = RouteSeed {
            prompt: text("prompt"),
            subagent_type: text("subagent_type"),
            description: text("description"),
            is_backgrounded: input.get("run_in_background").and_then(Value::as_bool) == Some(true),
            model: text("model").filter(|model| !model.is_empty() && model != INHERIT_MODEL),
        };
        Some(self.open_route(parent, tool_use_id, seed, out))
    }

    /// Mint the route of a `local_agent` task whose `tool_use_id` names an open call that is no
    /// `Agent` call: a forked skill (`context: fork`) runs its sub-agent behind a `Skill` call,
    /// and nothing before its `task_started` says the call delegates. The call's item started
    /// without a link, so an `Activity` carrying the route's link announces the sub-agent on the
    /// call's turn, and the server links it while it runs. `None` when no open tool call has
    /// that id.
    fn mint_task_route(
        &mut self,
        tool_use_id: &str,
        task: &TaskStartedMessage,
        out: &mut Out,
    ) -> Option<String> {
        let parent = if self
            .turn
            .as_ref()
            .is_some_and(|turn| turn.items.tools.contains_key(tool_use_id))
        {
            Route::Primary
        } else {
            self.route_with_open_tool(tool_use_id)?
        };
        let turn = self.turn_of(&parent)?;
        let turn_id = turn.id;
        let ToolKind::Call { name, .. } = &turn.items.tools.get(tool_use_id)?.call else {
            debug!(
                thread_id = %self.thread_of(&parent),
                harness_thread_id = %self.harness_thread_id,
                task_id = %task.task_id,
                native_item_id = %tool_use_id,
                "an agent task names a command or file call; no sub-agent route"
            );
            return None;
        };
        let name = name.clone();
        let description = Some(task.description.clone()).filter(|text| !text.is_empty());
        let seed = RouteSeed {
            prompt: task.prompt.clone(),
            subagent_type: task.subagent_type.clone(),
            description: description.clone(),
            is_backgrounded: task.is_backgrounded == Some(true),
            model: None,
        };
        info!(
            thread_id = %self.thread_of(&parent),
            harness_thread_id = %self.harness_thread_id,
            turn_id = %turn_id,
            task_id = %task.task_id,
            native_item_id = %tool_use_id,
            tool_name = %name,
            action = "task_route",
            "a tool call runs a sub-agent; minting its route"
        );
        let link = self.open_route(&parent, tool_use_id, seed, out);
        let thread = self.thread_of(&parent);
        let id = ItemId::new();
        let harness_item_id = format!("task_started:{}", task.task_id);
        out.push(self.event(AgentEvent::ItemStarted {
            thread,
            turn: turn_id,
            item: ItemStart {
                id,
                harness_item_id: harness_item_id.clone(),
                kind: ItemKind::Activity,
                command: None,
                tool: None,
            },
        }));
        out.push(self.event(AgentEvent::ItemCompleted {
            thread,
            turn: turn_id,
            item: Item {
                id,
                harness_item_id,
                payload: ItemPayload::Activity {
                    title: description.unwrap_or_else(|| "Sub-agent".to_owned()),
                    detail: Some(format!("{name} sub-agent")),
                    metadata: None,
                    subagent: Some(link),
                },
                created_at: Utc::now(),
            },
        }));
        Some(tool_use_id.to_owned())
    }

    /// A new run of a task that already has a route: the CLI resumed its sub-agent, with
    /// `SendMessage` (whose call `tool_use_id` then names), or on its own when a background task
    /// it waited on finished (`tool_use_id` names the route's own call). The route runs again on
    /// its own thread rather than a new route being minted; a dropped route is revived
    /// (`RouteReopened`). Its frames still name the route's call. Returns the route's key and
    /// whether the run opens with `task_started.prompt` (only a run another call resumed: a run
    /// the sub-agent started itself was woken by a `<task-notification>`), or `None` when no
    /// route runs this task.
    fn resume_route(
        &mut self,
        tool_use_id: &str,
        task: &TaskStartedMessage,
        out: &mut Out,
    ) -> Option<(String, bool)> {
        let of_task = |route: &RouteState| route.task_id.as_deref() == Some(task.task_id.as_str());
        let live = self
            .session
            .routes
            .iter()
            .find(|(_, route)| of_task(route))
            .map(|(id, _)| id.clone());
        let ended = self
            .session
            .ended_routes
            .iter()
            .find(|(_, route)| of_task(route))
            .map(|(id, _)| id.clone());
        let key = live.clone().or(ended)?;
        if let Some(route) = self.session.routes.get(&key)
            && (!route.ended() || route.turn.is_some())
        {
            warn!(
                thread_id = %route.thread,
                harness_thread_id = %self.harness_thread_id,
                route = %route.harness_thread_id,
                task_id = %task.task_id,
                native_item_id = %tool_use_id,
                action = "route_resumed",
                "a sub-agent task started again while its route still runs; keeping that run"
            );
            return Some((key, false));
        }
        // The call that resumed the sub-agent, open on some turn. A run the sub-agent started
        // itself has none.
        let resuming = self.open_call(tool_use_id);
        let resumed_by = resuming.as_ref().and_then(|(_, name)| name.clone());
        let reopened = live.is_none();
        let mut route = match live {
            Some(key) => self.session.routes.remove(&key)?,
            None => self.session.ended_routes.remove(&key)?,
        };
        // The run belongs to the turn of the route that resumed it: the route is dropped once both
        // that turn and the run have ended, and reports its outcome there, since no tool result of
        // the route's own call follows. A run the sub-agent started itself keeps its parent while
        // that parent's turn runs, else falls to the primary.
        let parent = match resuming {
            Some((parent, _)) => parent,
            None if self.turn_of(&route.parent).is_some() => route.parent.clone(),
            None => Route::Primary,
        };
        let parent_changed = parent != route.parent;
        route.parent = parent;
        route.call_open = false;
        route.stop_sent = false;
        route.status = SubagentStatus::Pending;
        route.run = Some(
            task.run_id
                .clone()
                .unwrap_or_else(|| tool_use_id.to_owned()),
        );
        // The run's outcome row lands on the turn that resumed it (late once that turn ended). With
        // no parent turn running, the route keeps the turn that delegated to it, whose outcome row
        // then arrives late, and nothing but the run's own end drops it.
        // A turn of another thread (the parent fell to the primary) is no turn to report on.
        match self.turn_of(&route.parent).map(|turn| turn.id) {
            Some(turn) => {
                route.spawned_turn = Some(turn);
                route.turn_ended = false;
            }
            None => {
                if parent_changed {
                    route.spawned_turn = None;
                }
                route.turn_ended = true;
            }
        }
        let parent = route.parent.clone();
        let thread = route.thread;
        let link = route.link(SubagentAction::Started);
        let description = route.description.clone();
        let spawned_turn = route.spawned_turn;
        let parent_turn = self.turn_of(&parent).map(|turn| turn.id);
        info!(
            thread_id = %thread,
            harness_thread_id = %self.harness_thread_id,
            route = %route.harness_thread_id,
            parent_route = display_opt(parent.label()),
            turn_id = display_opt(parent_turn),
            task_id = %task.task_id,
            run_id = display_opt(task.run_id.as_deref()),
            native_item_id = %tool_use_id,
            resumed_by = display_opt(resumed_by.as_deref()),
            reopened,
            action = "route_resumed",
            "the sub-agent was resumed; its route runs again"
        );
        self.session.routes.insert(key.clone(), route);
        if reopened {
            out.push(MapperOutput::RouteReopened { thread });
        }
        self.resume_row(
            &parent,
            spawned_turn,
            link,
            description,
            task,
            tool_use_id,
            resumed_by.as_deref(),
            out,
        );
        Some((key, resumed_by.is_some()))
    }

    /// The row a resumed run adds on `turn`, the parent's turn it reports on, carrying the
    /// route's link: a late completion alone once that turn ended (the server amends a late row
    /// carrying a sub-agent link into the persisted turn), none without a turn.
    #[allow(clippy::too_many_arguments)]
    fn resume_row(
        &self,
        parent: &Route,
        turn: Option<TurnId>,
        link: SubagentLink,
        description: Option<String>,
        task: &TaskStartedMessage,
        tool_use_id: &str,
        resumed_by: Option<&str>,
        out: &mut Out,
    ) {
        let Some(parent_turn) = turn else {
            return;
        };
        let late = self.turn_of(parent).map(|turn| turn.id) != Some(parent_turn);
        let parent_thread = self.thread_of(parent);
        let id = ItemId::new();
        let harness_item_id = format!(
            "task_started:{}:{}",
            task.task_id,
            task.run_id.as_deref().unwrap_or(tool_use_id)
        );
        if !late {
            out.push(self.event(AgentEvent::ItemStarted {
                thread: parent_thread,
                turn: parent_turn,
                item: ItemStart {
                    id,
                    harness_item_id: harness_item_id.clone(),
                    kind: ItemKind::Activity,
                    command: None,
                    tool: None,
                },
            }));
        }
        let detail = match resumed_by {
            Some(name) => format!("resumed by {name}"),
            None => "resumed".to_owned(),
        };
        out.push(self.event(AgentEvent::ItemCompleted {
            thread: parent_thread,
            turn: parent_turn,
            item: Item {
                id,
                harness_item_id,
                payload: ItemPayload::Activity {
                    title: description.unwrap_or_else(|| "Sub-agent".to_owned()),
                    detail: Some(detail),
                    metadata: None,
                    subagent: Some(link),
                },
                created_at: Utc::now(),
            },
        }));
    }

    /// A held resume whose run ended before any frame of it arrived opens no route.
    fn drop_pending_resume(&mut self, task_id: &str) {
        if self.session.pending_resumes.remove(task_id).is_some() {
            debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                task_id = %task_id,
                action = "route_resume_pending",
                "a held sub-agent resume ended without a frame; no route opened"
            );
        }
    }

    /// The open call `tool_use_id`: the route it was made on, and its tool name (`None` for a
    /// command or file call).
    fn open_call(&self, tool_use_id: &str) -> Option<(Route, Option<String>)> {
        let call_name = |tool: &OpenTool| match &tool.call {
            ToolKind::Call { name, .. } => Some(name.clone()),
            _ => None,
        };
        if let Some(tool) = self
            .turn
            .as_ref()
            .and_then(|turn| turn.items.tools.get(tool_use_id))
        {
            return Some((Route::Primary, call_name(tool)));
        }
        let route = self.route_with_open_tool(tool_use_id)?;
        let name = call_name(self.turn_of(&route)?.items.tools.get(tool_use_id)?);
        Some((route, name))
    }

    /// A `SendMessage` run of a sub-agent this process never ran: hold it until its first frame
    /// names the call its route is keyed by (`adopt_pending_resume`). `false` when the task is
    /// not started by an open `SendMessage` call.
    fn defer_resume(&mut self, tool_use_id: &str, task: &TaskStartedMessage) -> bool {
        let Some((parent, Some(call))) = self.open_call(tool_use_id) else {
            return false;
        };
        if call != "SendMessage" {
            return false;
        }
        info!(
            thread_id = %self.thread_of(&parent),
            harness_thread_id = %self.harness_thread_id,
            turn_id = display_opt(self.turn_of(&parent).map(|turn| turn.id)),
            task_id = %task.task_id,
            run_id = display_opt(task.run_id.as_deref()),
            native_item_id = %tool_use_id,
            action = "route_resume_pending",
            "a sub-agent an earlier child ran was resumed; its route opens with its first frame"
        );
        self.session.pending_resumes.insert(
            task.task_id.clone(),
            PendingResume {
                turn: self.turn_of(&parent).map(|turn| turn.id),
                parent,
                call,
                task: task.clone(),
            },
        );
        true
    }

    /// A sub-agent frame whose `parent_tool_use_id` names no route, from a run held in
    /// `pending_resumes`: open the route under that call, on the thread an earlier child bound it
    /// to when there is one, and start the run there as `on_task_started` would have.
    fn adopt_pending_resume(
        &mut self,
        parent_tool_use_id: Option<&str>,
        agent_id: Option<&str>,
        out: &mut Out,
    ) {
        let (Some(call), Some(agent_id)) = (parent_tool_use_id, agent_id) else {
            return;
        };
        if self.session.routes.contains_key(call) {
            return;
        }
        let Some(pending) = self.session.pending_resumes.remove(agent_id) else {
            return;
        };
        let task = pending.task;
        // This process never saw the delegated prompt; the run's message is delivered apart.
        let seed = RouteSeed {
            prompt: None,
            subagent_type: task.subagent_type.clone(),
            description: Some(task.description.clone()).filter(|text| !text.is_empty()),
            is_backgrounded: task.is_backgrounded == Some(true),
            model: None,
        };
        self.open_route(&pending.parent, call, seed, out);
        let active = self.turn_of(&pending.parent).map(|turn| turn.id);
        if let Some(route) = self.session.routes.get_mut(call) {
            // No tool result of this call follows in this process: the outcome is reported on
            // the `SendMessage` call's turn, as for any resumed run, late once that turn ended,
            // which leaves the run's own end to drop the route.
            route.call_open = false;
            route.run = Some(task.run_id.clone().unwrap_or_else(|| call.to_owned()));
            route.spawned_turn = pending.turn;
            route.turn_ended = pending.turn.is_none() || active != pending.turn;
        }
        if let Some(entry) = self.session.tasks.get_mut(&task.task_id) {
            entry.route = Some(call.to_owned());
        }
        info!(
            harness_thread_id = %self.harness_thread_id,
            route = %format_args!("{TASK_ID_PREFIX}{call}"),
            task_id = %task.task_id,
            action = "route_resumed",
            resumed_by = %pending.call,
            reopened = true,
            "the sub-agent was resumed; its route runs again"
        );
        self.start_route(call, &task, out);
        self.deliver_resumed_prompt(call, &task, out);
        if let Some(route) = self.session.routes.get(call) {
            let link = route.link(SubagentAction::Started);
            let description = route.description.clone();
            self.resume_row(
                &pending.parent,
                pending.turn,
                link,
                description,
                &task,
                call,
                Some(&pending.call),
                out,
            );
        }
    }

    /// A resumed run's message (the `SendMessage` text, `task_started.prompt`) opens its turn: the
    /// CLI echoes no user frame into the sub-agent for it.
    fn deliver_resumed_prompt(&mut self, id: &str, task: &TaskStartedMessage, out: &mut Out) {
        let Some(route) = self.session.routes.get_mut(id) else {
            return;
        };
        let (Some(turn), Some(prompt)) =
            (route.turn.as_ref().map(|turn| turn.id), task.prompt.clone())
        else {
            return;
        };
        route.prompt_delivered = true;
        let harness_item_id = format!(
            "task_prompt:{}",
            task.run_id.as_deref().unwrap_or(&task.task_id)
        );
        self.user_message(
            &Route::Task(id.to_owned()),
            turn,
            harness_item_id,
            prompt,
            out,
        );
    }

    /// Record a new route for the call `tool_use_id` made on `parent` and announce it
    /// (`RouteOpened`). Returns the link the call's item carries.
    fn open_route(
        &mut self,
        parent: &Route,
        tool_use_id: &str,
        seed: RouteSeed,
        out: &mut Out,
    ) -> SubagentLink {
        let parent_harness_thread_id = match parent {
            Route::Primary => self.harness_thread_id.clone(),
            Route::Task(outer) => format!("{TASK_ID_PREFIX}{outer}"),
        };
        let spawned_turn = self.turn_of(parent).map(|turn| turn.id);
        let harness_thread_id = format!("{TASK_ID_PREFIX}{tool_use_id}");
        // A native id an earlier child of the session bound keeps its thread; any other gets a
        // new one.
        let thread = self
            .route_threads
            .as_ref()
            .and_then(|route_threads| route_threads(&harness_thread_id))
            .unwrap_or_default();
        let route = RouteState {
            thread,
            harness_thread_id,
            parent: parent.clone(),
            prompt: seed.prompt,
            prompt_delivered: false,
            subagent_type: seed.subagent_type,
            task_id: None,
            is_backgrounded: seed.is_backgrounded,
            stop_sent: false,
            call_open: true,
            status: SubagentStatus::Pending,
            spawned_turn,
            turn_ended: false,
            run: None,
            description: seed.description,
            turn: None,
            counted_messages: HashSet::new(),
        };
        info!(
            thread_id = %route.thread,
            harness_thread_id = %self.harness_thread_id,
            route = %route.harness_thread_id,
            parent_harness_thread_id = %parent_harness_thread_id,
            turn_id = display_opt(spawned_turn),
            subagent_type = display_opt(route.subagent_type.as_deref()),
            requested_model = display_opt(seed.model.as_deref()),
            action = "route_opened",
            "sub-agent route minted"
        );
        out.push(MapperOutput::RouteOpened {
            thread: route.thread,
            harness_thread_id: route.harness_thread_id.clone(),
            parent_harness_thread_id,
            agent_name: route.description.clone(),
            model: seed.model.map(anthropic_model),
        });
        let link = route.link(SubagentAction::Spawned);
        self.session.routes.insert(tool_use_id.to_owned(), route);
        link
    }

    /// The route's turn, opened here when a routed frame arrives before (or without) its
    /// `task_started`. `None` when the route's turn already ended: the frame is a trailing one.
    fn ensure_route_turn(&mut self, id: &str, frame_type: &str, out: &mut Out) -> Option<TurnId> {
        let harness_thread_id = self.harness_thread_id.clone();
        let route = self.session.routes.get_mut(id)?;
        if let Some(turn) = &route.turn {
            return Some(turn.id);
        }
        if route.ended() {
            debug!(
                thread_id = %route.thread,
                harness_thread_id = %harness_thread_id,
                route = %route.harness_thread_id,
                task_id = display_opt(route.task_id.as_deref()),
                action = "route_trailing_frame",
                frame_type,
                "dropping a frame that trails the sub-agent's terminal update"
            );
            return None;
        }
        let turn = TurnId::new();
        info!(
            thread_id = %route.thread,
            harness_thread_id = %harness_thread_id,
            route = %route.harness_thread_id,
            turn_id = %turn,
            action = "external_turn",
            frame_type,
            "a sub-agent frame arrived before its task started; opening its turn"
        );
        route.status = SubagentStatus::Running;
        route.turn = Some(TurnState::new(turn, TurnKind::User));
        let thread = route.thread;
        out.push(self.event(AgentEvent::TurnStarted { thread, turn }));
        Some(turn)
    }

    /// The end of route `id`'s task `task_id` (its terminal `task_updated`, or the
    /// `task_notification` of a task that got none): its turn ends now, a backgrounded
    /// delegation's outcome is reported on the spawning route, and a route that outlived its
    /// spawning turn is dropped.
    fn end_route(
        &mut self,
        id: &str,
        task_id: &str,
        error: Option<&str>,
        status: &TaskStatus,
        out: &mut Out,
    ) {
        let Some(route) = self.session.routes.get(id) else {
            return;
        };
        if route.ended() {
            debug!(
                thread_id = %route.thread,
                harness_thread_id = %self.harness_thread_id,
                route = %route.harness_thread_id,
                task_id = %task_id,
                status = %status,
                action = "task_updated",
                "terminal update for a sub-agent whose turn already ended; ignored"
            );
            return;
        }
        let interrupted = route.stop_sent
            || self
                .turn_of(&route.parent)
                .is_some_and(|t| t.interrupt_sent);
        let (subagent, turn_status) = match status {
            TaskStatus::Completed => (
                SubagentStatus::Completed,
                TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
            ),
            // A stop reads `killed` on the update and `stopped` on the notification that follows
            // (a stopped forked skill's included, `forked-skill-stop`). A `stopped` notification
            // with no update before it was never recorded; it maps as the update would.
            TaskStatus::Killed | TaskStatus::Stopped if interrupted => (
                SubagentStatus::Interrupted,
                TurnStatus {
                    kind: TurnStatusKind::Interrupted,
                    message: None,
                },
            ),
            TaskStatus::Killed | TaskStatus::Stopped => (
                SubagentStatus::Failed,
                TurnStatus {
                    kind: TurnStatusKind::Failed,
                    message: Some(format!("agent task {task_id} was {status}")),
                },
            ),
            // Only terminal statuses end a route; the rest are listed so a new one is a choice.
            other @ (TaskStatus::Failed
            | TaskStatus::Pending
            | TaskStatus::Running
            | TaskStatus::Paused
            | TaskStatus::Unknown(_)) => (
                SubagentStatus::Failed,
                TurnStatus {
                    kind: TurnStatusKind::Failed,
                    message: Some(error.map_or_else(
                        || format!("agent task {task_id} ended {other}"),
                        str::to_owned,
                    )),
                },
            ),
        };
        self.finish_route_turn(id, subagent, turn_status, out);
        self.report_background_outcome(id, task_id, error, out);
        if self
            .session
            .routes
            .get(id)
            .is_some_and(|route| route.turn_ended)
        {
            self.drop_route(id, out);
        }
    }

    /// A route whose parent's `Agent` call already completed (a backgrounded delegation) reports
    /// its outcome as one `Activity` on the spawning route, carrying the link: on the spawning turn
    /// while it is still the active one, else as a late completion of that turn, which the server
    /// amends into its persisted items.
    fn report_background_outcome(
        &mut self,
        id: &str,
        task_id: &str,
        error: Option<&str>,
        out: &mut Out,
    ) {
        let Some(route) = self.session.routes.get(id) else {
            return;
        };
        if route.call_open {
            return;
        }
        let parent = route.parent.clone();
        let spawned_turn = route.spawned_turn;
        let run = route.run.clone();
        let title = route
            .description
            .clone()
            .unwrap_or_else(|| "Sub-agent".to_owned());
        let detail = match route.status {
            SubagentStatus::Completed => "completed".to_owned(),
            SubagentStatus::Interrupted => "killed".to_owned(),
            _ => error.unwrap_or("failed").to_owned(),
        };
        let link = route.link(route.outcome_action());
        let active = self.turn_of(&parent).map(|turn| turn.id);
        let thread = self.thread_of(&parent);
        let (turn, late) = match (spawned_turn, active) {
            (Some(spawned), Some(active)) if spawned == active => (spawned, false),
            (Some(spawned), _) => (spawned, true),
            (None, Some(active)) => (active, false),
            (None, None) => {
                debug!(
                    thread_id = %thread,
                    harness_thread_id = %self.harness_thread_id,
                    route = %link.harness_thread_id,
                    task_id = %task_id,
                    action = "subagent_outcome",
                    "no active turn on the spawning thread; the sub-agent's outcome has no row"
                );
                return;
            }
        };
        info!(
            thread_id = %thread,
            harness_thread_id = %self.harness_thread_id,
            route = %link.harness_thread_id,
            turn_id = %turn,
            task_id = %task_id,
            status = ?link.status,
            late,
            action = "subagent_outcome",
            "sub-agent outcome reported on the spawning turn"
        );
        let id = ItemId::new();
        // A resumed run's row is its run's: one turn may report several runs of one task.
        let harness_item_id = match &run {
            Some(run) => format!("task_updated:{task_id}:{run}"),
            None => format!("task_updated:{task_id}"),
        };
        // The server ignores a start for a persisted turn: a late row is its completion alone.
        if !late {
            out.push(self.event(AgentEvent::ItemStarted {
                thread,
                turn,
                item: ItemStart {
                    id,
                    harness_item_id: harness_item_id.clone(),
                    kind: ItemKind::Activity,
                    command: None,
                    tool: None,
                },
            }));
        }
        out.push(self.event(AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id,
                harness_item_id,
                payload: ItemPayload::Activity {
                    title,
                    detail: Some(detail),
                    metadata: None,
                    subagent: Some(link),
                },
                created_at: Utc::now(),
            },
        }));
    }

    /// Complete route `id`'s turn with `status`: open tool calls end `interrupted` (no
    /// `tool_result` will complete them on an ended turn), open text blocks with what streamed.
    /// The routes it spawned are dropped with it.
    fn finish_route_turn(
        &mut self,
        id: &str,
        subagent: SubagentStatus,
        status: TurnStatus,
        out: &mut Out,
    ) {
        let Some(route) = self.session.routes.get_mut(id) else {
            return;
        };
        route.status = subagent;
        let Some(mut turn) = route.turn.take() else {
            return;
        };
        let thread = route.thread;
        let harness_thread_id = route.harness_thread_id.clone();
        let task_id = route.task_id.clone();
        let mut tools: Vec<(String, OpenTool)> = turn.items.tools.drain().collect();
        tools.sort_by(|(left, _), (right, _)| left.cmp(right));
        for (tool_use_id, open) in tools {
            let link = self
                .session
                .routes
                .get(&tool_use_id)
                .map(|nested| nested.link(nested.outcome_action()));
            let payload =
                interrupted_payload(open.call, open.description, &self.workspace_root, link);
            out.push(self.event(AgentEvent::ItemCompleted {
                thread,
                turn: turn.id,
                item: Item {
                    id: open.item_id,
                    harness_item_id: tool_use_id,
                    payload,
                    created_at: Utc::now(),
                },
            }));
        }
        for (key, block) in turn.items.blocks.drain() {
            if !block.started {
                continue;
            }
            let id = turn
                .items
                .item_ids
                .get(&key)
                .copied()
                .unwrap_or_else(ItemId::new);
            let payload = match block.kind {
                ItemKind::Reasoning => ItemPayload::Reasoning {
                    text: block.streamed,
                },
                _ => ItemPayload::AgentMessage {
                    text: block.streamed,
                },
            };
            out.push(self.event(AgentEvent::ItemCompleted {
                thread,
                turn: turn.id,
                item: Item {
                    id,
                    harness_item_id: key.harness_item_id(),
                    payload,
                    created_at: Utc::now(),
                },
            }));
        }
        info!(
            thread_id = %thread,
            harness_thread_id = %self.harness_thread_id,
            route = %harness_thread_id,
            turn_id = %turn.id,
            task_id = display_opt(task_id.as_deref()),
            action = "turn_completed",
            status = ?status.kind,
            input_tokens = turn.completed_usage().input,
            output_tokens = turn.completed_usage().output,
            "sub-agent turn completed"
        );
        out.push(MapperOutput::Event(AgentEvent::TurnCompleted {
            thread,
            turn: turn.id,
            usage: turn.completed_usage(),
            status,
        }));
        self.drop_spawned_routes(&Route::Task(id.to_owned()), out);
    }

    /// The turn of `parent` ended: drop the routes it spawned whose task has ended. A backgrounded
    /// route still running outlives the turn, and its own end drops it; a foreground route still
    /// running here (its turn was superseded, or the child exited) is completed `Failed` first.
    fn drop_spawned_routes(&mut self, parent: &Route, out: &mut Out) {
        self.close_spawned_routes(parent, true, out);
    }

    /// Drop the routes `parent` spawned: those still running are completed `Failed` first, except
    /// that with `outlive` set a backgrounded one is kept, marked as having outlived its turn.
    fn close_spawned_routes(&mut self, parent: &Route, outlive: bool, out: &mut Out) {
        let mut spawned: Vec<(String, ThreadId)> = self
            .session
            .routes
            .iter()
            .filter(|(_, route)| route.parent == *parent)
            .map(|(id, route)| (id.clone(), route.thread))
            .collect();
        spawned.sort_by(|(left, _), (right, _)| left.cmp(right));
        for (id, thread) in spawned {
            let Some(route) = self.session.routes.get_mut(&id) else {
                continue;
            };
            let open = !route.ended();
            if open && outlive && route.is_backgrounded {
                // Already kept by an earlier turn's end: it waits for its own.
                if !route.turn_ended {
                    route.turn_ended = true;
                    info!(
                        thread_id = %thread,
                        harness_thread_id = %self.harness_thread_id,
                        route = %route.harness_thread_id,
                        task_id = display_opt(route.task_id.as_deref()),
                        turn_id = display_opt(route.spawned_turn),
                        parent_route = display_opt(parent.label()),
                        action = "route_outlives_turn",
                        "the turn that spawned a background sub-agent ended; the sub-agent runs on"
                    );
                }
                continue;
            }
            if open {
                warn!(
                    thread_id = %thread,
                    harness_thread_id = %self.harness_thread_id,
                    route = %format_args!("{TASK_ID_PREFIX}{id}"),
                    parent_route = display_opt(parent.label()),
                    action = "route_still_open",
                    "the turn that spawned a sub-agent ended while it still ran; failing it"
                );
                self.finish_route_turn(
                    &id,
                    SubagentStatus::Failed,
                    TurnStatus {
                        kind: TurnStatusKind::Failed,
                        message: Some("parent turn ended".into()),
                    },
                    out,
                );
            }
            self.drop_route(&id, out);
        }
    }

    /// Forget route `id` (its turn has ended): its log closes, its trailing frames are dropped.
    fn drop_route(&mut self, id: &str, out: &mut Out) {
        // Nested routes go first, so a nested route never outlives its parent.
        self.close_spawned_routes(&Route::Task(id.to_owned()), false, out);
        let Some(route) = self.session.routes.remove(id) else {
            return;
        };
        for entry in self.session.tasks.values_mut() {
            if entry.route.as_deref() == Some(id) {
                entry.route = None;
            }
        }
        info!(
            thread_id = %route.thread,
            harness_thread_id = %self.harness_thread_id,
            route = %route.harness_thread_id,
            task_id = display_opt(route.task_id.as_deref()),
            status = ?route.status,
            action = "route_closed",
            "sub-agent route closed"
        );
        out.push(MapperOutput::RouteClosed {
            thread: route.thread,
        });
        self.session.ended_routes.insert(id.to_owned(), route);
    }

    /// Record the model a route's `assistant` frame names on the route's turn, logging the first
    /// one (and any change) at `info`. A change is reported at once, since the frame bringing it
    /// may carry no new usage; the first model rides on the usage that follows it.
    fn note_route_model(&mut self, route: &Route, turn_id: TurnId, native: String, out: &mut Out) {
        let Some(previous) = self.turn_of(route).map(|turn| turn.native_model.clone()) else {
            return;
        };
        if previous.as_deref() == Some(native.as_str()) {
            return;
        }
        let model = self.frame_model(&native);
        let thread = self.thread_of(route);
        info!(
            thread_id = %thread,
            harness_thread_id = %self.harness_thread_id,
            route = display_opt(route.label()),
            turn_id = %turn_id,
            native_model = %native,
            previous_native_model = display_opt(previous.as_deref()),
            model = %model.model,
            session_model = display_opt(self.session.model.as_deref()),
            action = "subagent_model",
            "sub-agent frame names its model"
        );
        let Some(turn) = self.turn_of_mut(route) else {
            return;
        };
        turn.model = Some(model);
        turn.native_model = Some(native);
        if previous.is_some() {
            self.emit_usage(route, out);
        }
    }

    /// Count one child API message's usage once and report it on the route's turn.
    fn note_route_usage(
        &mut self,
        id: &str,
        message_id: &str,
        usage: Option<&AssistantUsage>,
        out: &mut Out,
    ) {
        let Some(usage) = usage else {
            return;
        };
        let Some(route) = self.session.routes.get_mut(id) else {
            return;
        };
        if !route.counted_messages.insert(message_id.to_owned()) {
            return;
        }
        let Some(turn) = route.turn.as_mut() else {
            return;
        };
        let usage = assistant_token_usage(usage);
        turn.window_usage = usage;
        turn.result_usage
            .get_or_insert_with(TokenUsage::default)
            .add(&usage);
        self.emit_usage(&Route::Task(id.to_owned()), out);
    }

    // ---- routing -------------------------------------------------------------------------------

    /// The route a frame with this `parent_tool_use_id` belongs to, or `None` when it must be
    /// dropped. A frame without one is the primary thread's; one naming a minted route is that
    /// route's. One naming no route is dropped: attributing it to the primary thread would render a
    /// sub-agent's tool calls as the main agent's.
    fn route(&self, parent_tool_use_id: Option<&str>, frame_type: &str) -> Option<Route> {
        let Some(parent) = parent_tool_use_id else {
            return Some(Route::Primary);
        };
        if self.session.routes.contains_key(parent) {
            return Some(Route::Task(parent.to_owned()));
        }
        match self.session.ended_routes.get(parent) {
            Some(ended) => debug!(
                thread_id = %ended.thread,
                harness_thread_id = %self.harness_thread_id,
                route = %format_args!("{TASK_ID_PREFIX}{parent}"),
                frame_type,
                action = "route_trailing_frame",
                "dropping a frame that trails the sub-agent's terminal update"
            ),
            None => debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = display_opt(self.active_turn()),
                parent_tool_use_id = %parent,
                task_native_id = %format_args!("{TASK_ID_PREFIX}{parent}"),
                frame_type,
                "dropping a sub-agent frame: no route was minted for its parent tool use"
            ),
        }
        None
    }

    /// The route's turn for a routed `assistant` or `user` frame: the primary's (opened on its own
    /// when needed), or the route's. `None` drops the frame.
    fn frame_turn(&mut self, route: &Route, frame_type: &str, out: &mut Out) -> Option<TurnId> {
        match route {
            Route::Primary => Some(self.ensure_turn(frame_type, out)),
            Route::Task(id) => self.ensure_route_turn(id, frame_type, out),
        }
    }

    // ---- assistant, stream and user frames -----------------------------------------------------

    fn on_assistant(&mut self, message: AssistantMessage, out: &mut Out) {
        self.adopt_pending_resume(
            message.parent_tool_use_id.as_deref(),
            message.agent_id.as_deref(),
            out,
        );
        let Some(route) = self.route(message.parent_tool_use_id.as_deref(), "assistant") else {
            return;
        };
        let Some(turn) = self.frame_turn(&route, "assistant", out) else {
            return;
        };
        let message_id = message.message.id;
        if let Route::Task(id) = &route {
            if message.message.model == SYNTHETIC_MODEL {
                // No model ran it and its usage is zero: neither is the sub-agent's.
                debug!(
                    thread_id = %self.thread_of(&route),
                    harness_thread_id = %self.harness_thread_id,
                    route = display_opt(route.label()),
                    turn_id = %turn,
                    message_id = %message_id,
                    action = "subagent_model",
                    "a synthetic sub-agent message names no model; its model and usage are skipped"
                );
            } else {
                // The sub-agent's model is known only from its own frames; noted before the usage,
                // so the `TurnUsageUpdated` it emits carries it.
                self.note_route_model(&route, turn, message.message.model, out);
                // Forwarded frames carry no stream events: usage comes from each API message.
                self.note_route_usage(id, &message_id, message.message.usage.as_ref(), out);
            }
        }
        let Some(state) = self.turn_of_mut(&route) else {
            return;
        };
        let next = state
            .items
            .message_blocks
            .entry(message_id.clone())
            .or_insert(0);
        let first_index = *next;
        *next += u32::try_from(message.message.content.len()).unwrap_or(u32::MAX);
        for (offset, block) in message.message.content.into_iter().enumerate() {
            let index = first_index.saturating_add(u32::try_from(offset).unwrap_or(u32::MAX));
            let key = NativeItemKey::Block {
                message_id: message_id.clone(),
                index,
            };
            match block {
                ContentBlock::Text(text) => {
                    self.complete_block(&route, turn, key, ItemKind::AgentMessage, text.text, out)
                }
                ContentBlock::Thinking(thinking) => self.complete_block(
                    &route,
                    turn,
                    key,
                    ItemKind::Reasoning,
                    thinking.thinking,
                    out,
                ),
                ContentBlock::ToolUse(tool_use) => self.start_tool(&route, turn, tool_use, out),
                other => debug!(
                    thread_id = %self.thread_of(&route),
                    harness_thread_id = %self.harness_thread_id,
                    route = display_opt(route.label()),
                    turn_id = %turn,
                    native_item_id = %key.harness_item_id(),
                    block_type = content_block_type(&other),
                    "skipping an assistant block this adapter does not map"
                ),
            }
        }
    }

    fn on_stream(&mut self, stream: StreamEvent, out: &mut Out) {
        // Forwarded sub-agent text arrives as `assistant` frames only; a stream event naming a
        // parent is dropped like any other.
        match self.route(stream.parent_tool_use_id.as_deref(), "stream_event") {
            Some(Route::Primary) => {}
            Some(route @ Route::Task(_)) => {
                debug!(
                    thread_id = %self.thread_of(&route),
                    harness_thread_id = %self.harness_thread_id,
                    route = display_opt(route.label()),
                    frame_type = "stream_event",
                    "skipping a sub-agent stream event; its assistant frame carries the block"
                );
                return;
            }
            None => return,
        }
        let route = Route::Primary;
        let turn = self.ensure_turn("stream_event", out);
        match stream.event {
            StreamEventKind::MessageStart { message_id } => {
                if let Some(state) = self.turn.as_mut() {
                    state.items.stream_message_id = Some(message_id);
                }
            }
            StreamEventKind::ContentBlockStart { index, block } => {
                let Some(key) = self.stream_key(index) else {
                    return;
                };
                match block {
                    BlockStart::Text => {
                        self.open_block(&route, turn, key, ItemKind::AgentMessage, true, out);
                    }
                    BlockStart::Thinking => {
                        self.open_block(&route, turn, key, ItemKind::Reasoning, false, out);
                    }
                    // The `assistant` frame carries the call's final input and starts its item.
                    BlockStart::ToolUse { .. } => {}
                    BlockStart::Other(block_type) => debug!(
                        thread_id = %self.thread,
                        harness_thread_id = %self.harness_thread_id,
                        turn_id = %turn,
                        native_item_id = %key.harness_item_id(),
                        block_type = %block_type,
                        "skipping a streamed block this adapter does not map"
                    ),
                }
            }
            StreamEventKind::ContentBlockDelta { index, delta } => {
                let (kind, text) = match delta {
                    Delta::Text(text) => (ItemKind::AgentMessage, text),
                    Delta::Thinking(text) => (ItemKind::Reasoning, text),
                    // The `assistant` frame carries the final tool input; a signature is opaque.
                    Delta::InputJson(_) | Delta::Signature => return,
                    Delta::Other(delta_type) => {
                        debug!(
                            thread_id = %self.thread,
                            harness_thread_id = %self.harness_thread_id,
                            turn_id = %turn,
                            delta_type = %delta_type,
                            "skipping a stream delta this adapter does not map"
                        );
                        return;
                    }
                };
                if text.is_empty() {
                    return;
                }
                let Some(key) = self.stream_key(index) else {
                    return;
                };
                self.open_block(&route, turn, key.clone(), kind, true, out);
                let item_id = self.resolve_item(&route, &key);
                if let Some(block) = self
                    .turn
                    .as_mut()
                    .and_then(|state| state.items.blocks.get_mut(&key))
                {
                    block.streamed.push_str(&text);
                }
                out.push(self.event(AgentEvent::ItemDelta {
                    thread: self.thread,
                    turn,
                    item_id,
                    delta: ItemDelta::Text { text },
                }));
            }
            StreamEventKind::MessageDelta { usage, .. } => {
                if let Some(usage) = usage {
                    if let Some(state) = self.turn.as_mut() {
                        state.window_usage = token_usage(&usage);
                    }
                    self.emit_usage(&route, out);
                }
            }
            StreamEventKind::ContentBlockStop { .. } | StreamEventKind::MessageStop => {}
            StreamEventKind::Other(event_type) => debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = %turn,
                event_type = %event_type,
                "skipping a stream event this adapter does not map"
            ),
        }
    }

    /// The item key of block `index` of the stream's current message.
    fn stream_key(&self, index: u32) -> Option<NativeItemKey> {
        let message_id = self
            .turn
            .as_ref()
            .and_then(|turn| turn.items.stream_message_id.clone());
        if message_id.is_none() {
            debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = display_opt(self.active_turn()),
                index,
                "skipping a stream block event before any message_start"
            );
        }
        Some(NativeItemKey::Block {
            message_id: message_id?,
            index,
        })
    }

    /// Track an open text or thinking block; start its item now when `start` is set.
    fn open_block(
        &mut self,
        route: &Route,
        turn: TurnId,
        key: NativeItemKey,
        kind: ItemKind,
        start: bool,
        out: &mut Out,
    ) {
        let Some(state) = self.turn_of_mut(route) else {
            return;
        };
        let block = state.items.blocks.entry(key.clone()).or_insert(OpenBlock {
            kind,
            started: false,
            streamed: String::new(),
        });
        if !start || block.started {
            return;
        }
        block.started = true;
        let kind = block.kind;
        let id = self.resolve_item(route, &key);
        out.push(self.event(AgentEvent::ItemStarted {
            thread: self.thread_of(route),
            turn,
            item: ItemStart {
                id,
                harness_item_id: key.harness_item_id(),
                kind,
                command: None,
                tool: None,
            },
        }));
    }

    /// Complete a text or thinking block from its `assistant` frame. An empty thought that never
    /// streamed text emits nothing at all.
    fn complete_block(
        &mut self,
        route: &Route,
        turn: TurnId,
        key: NativeItemKey,
        kind: ItemKind,
        text: String,
        out: &mut Out,
    ) {
        let open = self
            .turn_of_mut(route)
            .and_then(|state| state.items.blocks.remove(&key));
        let started = open.as_ref().is_some_and(|block| block.started);
        let text = match open {
            Some(block) if text.is_empty() => block.streamed,
            _ => text,
        };
        let thread = self.thread_of(route);
        if kind == ItemKind::Reasoning && text.is_empty() && !started {
            debug!(
                thread_id = %thread,
                harness_thread_id = %self.harness_thread_id,
                route = display_opt(route.label()),
                turn_id = %turn,
                native_item_id = %key.harness_item_id(),
                "skipping an empty thinking block"
            );
            return;
        }
        let id = self.resolve_item(route, &key);
        if !started {
            out.push(self.event(AgentEvent::ItemStarted {
                thread,
                turn,
                item: ItemStart {
                    id,
                    harness_item_id: key.harness_item_id(),
                    kind,
                    command: None,
                    tool: None,
                },
            }));
        }
        let payload = match kind {
            ItemKind::Reasoning => ItemPayload::Reasoning { text },
            _ => ItemPayload::AgentMessage { text },
        };
        out.push(self.event(AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id,
                harness_item_id: key.harness_item_id(),
                payload,
                created_at: Utc::now(),
            },
        }));
    }

    fn start_tool(&mut self, route: &Route, turn: TurnId, tool_use: ToolUseBlock, out: &mut Out) {
        let key = NativeItemKey::ToolUse(tool_use.id.clone());
        let thread = self.thread_of(route);
        if self
            .turn_of(route)
            .is_some_and(|state| state.items.tools.contains_key(&tool_use.id))
        {
            warn!(
                thread_id = %thread,
                harness_thread_id = %self.harness_thread_id,
                route = display_opt(route.label()),
                turn_id = %turn,
                native_item_id = %tool_use.id,
                tool_name = %tool_use.name,
                "skipping a repeated tool_use block for an open tool call"
            );
            return;
        }
        // An `Agent` call delegates to a sub-agent: its route exists before any of its frames.
        let subagent = if tool_use.name == AGENT_TOOL {
            self.mint_route(route, &tool_use.id, &tool_use.input, out)
        } else {
            None
        };
        let id = self.resolve_item(route, &key);
        let started_at_ms = Some(Utc::now().timestamp_millis());
        let description = tool_description(&tool_use.name, &tool_use.input, &self.workspace_root);
        let call = classify_tool(&tool_use.name, tool_use.input);
        let item = match &call {
            ToolKind::Command { command } => ItemStart {
                id,
                harness_item_id: tool_use.id.clone(),
                kind: ItemKind::CommandExecution,
                command: Some(CommandExecutionStart {
                    description: description.clone(),
                    command: command.clone(),
                    cwd: self.workspace_root.display().to_string(),
                    status: Some("in_progress".into()),
                    process_id: None,
                    started_at_ms,
                }),
                tool: None,
            },
            ToolKind::FileChange { .. } => ItemStart {
                id,
                harness_item_id: tool_use.id.clone(),
                kind: ItemKind::FileChange,
                command: None,
                tool: None,
            },
            ToolKind::Call {
                name,
                server,
                input,
            } => ItemStart {
                id,
                harness_item_id: tool_use.id.clone(),
                kind: ItemKind::ToolCall,
                command: None,
                tool: Some(ToolCallStart {
                    description: description.clone(),
                    name: name.clone(),
                    input: input.clone(),
                    server: server.clone(),
                    status: Some("in_progress".into()),
                    metadata: None,
                    subagent,
                    started_at_ms,
                }),
            },
        };
        if let Some(state) = self.turn_of_mut(route) {
            state.items.tools.insert(
                tool_use.id.clone(),
                OpenTool {
                    item_id: id,
                    call,
                    description,
                    started_at_ms,
                },
            );
        }
        debug!(
            thread_id = %thread,
            harness_thread_id = %self.harness_thread_id,
            route = display_opt(route.label()),
            turn_id = %turn,
            native_item_id = %tool_use.id,
            tool_name = %tool_use.name,
            "tool call started"
        );
        out.push(self.event(AgentEvent::ItemStarted { thread, turn, item }));
    }

    fn on_user(&mut self, message: UserMessage, out: &mut Out) {
        self.adopt_pending_resume(
            message.parent_tool_use_id.as_deref(),
            message.agent_id.as_deref(),
            out,
        );
        let Some(route) = self.route(message.parent_tool_use_id.as_deref(), "user") else {
            return;
        };
        let turn = match &route {
            Route::Primary => self.active_turn(),
            Route::Task(id) => self.ensure_route_turn(id, "user", out),
        };
        let Some(turn) = turn else {
            if route == Route::Primary {
                warn!(
                    thread_id = %self.thread,
                    harness_thread_id = %self.harness_thread_id,
                    frame_uuid = display_opt(message.uuid.as_deref()),
                    "skipping a user frame with no active turn"
                );
            }
            return;
        };
        let results = message
            .message
            .content
            .iter()
            .filter(|block| matches!(block, ContentBlock::ToolResult(_)))
            .count();
        // `tool_use_result` describes the frame's one tool result; it is ambiguous with several.
        let tool_use_result = (results == 1)
            .then_some(message.tool_use_result.as_ref())
            .flatten();
        let meta = message.tool_result_meta.as_deref().unwrap_or_default();
        // The two flags are independent: `isReplay` marks a frame the CLI echoes of its input,
        // `isSynthetic` one it wrote itself (a compaction summary).
        let synthetic = message.is_synthetic == Some(true);
        let replay = message.is_replay == Some(true);
        if replay && route == Route::Primary && results == 0 {
            // The adapter does not pass `--replay-user-messages`, yet the CLI marks some of its own
            // frames `isReplay` (the `<local-command-stdout>` after `/compact`), and a recording
            // made with the flag carries prompts and queued inputs (`origin`). None is an item:
            // the prompt is acknowledged at `system/init` from the text the adapter wrote.
            debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = %turn,
                frame_uuid = display_opt(message.uuid.as_deref()),
                origin = display_opt(message.origin.as_ref().map(|origin| origin.kind.as_str())),
                action = "replay_ignored",
                "ignoring a replayed user frame"
            );
            return;
        }
        // A synthetic or replayed user message (a compaction summary, a slash command's output) is
        // the CLI's bookkeeping, not something the user or the agent said in this turn.
        let bookkeeping = synthetic || replay;
        for (index, block) in message.message.content.into_iter().enumerate() {
            match block {
                ContentBlock::ToolResult(result) => {
                    self.complete_tool(&route, turn, result, tool_use_result, meta, out)
                }
                ContentBlock::Text(text) if results == 0 && !bookkeeping => {
                    let harness_item_id = format!(
                        "user:{}:{index}",
                        message.uuid.as_deref().unwrap_or("unknown")
                    );
                    if self.take_delegated_prompt(&route, &text.text) {
                        self.user_message(&route, turn, harness_item_id, text.text, out);
                    } else {
                        self.activity(&route, turn, harness_item_id, text.text, out);
                    }
                }
                other => debug!(
                    thread_id = %self.thread_of(&route),
                    harness_thread_id = %self.harness_thread_id,
                    route = display_opt(route.label()),
                    turn_id = %turn,
                    frame_uuid = display_opt(message.uuid.as_deref()),
                    block_type = content_block_type(&other),
                    synthetic = bookkeeping,
                    "skipping a user block this adapter does not map"
                ),
            }
        }
    }

    /// The text the adapter wrote for the active user turn, called right after `begin_turn`.
    pub fn note_prompt(&mut self, text: String) {
        if let Some(turn) = self.turn.as_mut() {
            turn.prompt = Some(text);
        }
    }

    /// Acknowledge the active user turn's prompt: its `UserMessage` item, from the text the
    /// adapter wrote. `trigger` is `"init"` for the turn's `system/init`, the CLI's per-prompt
    /// frame; any other trigger is the backstop, the first answer frame of a turn that had no
    /// `init`, which is observed behaviour the CLI does not document.
    fn acknowledge_prompt(&mut self, trigger: &'static str, frame_type: &str, out: &mut Out) {
        let Some(state) = self.turn.as_mut() else {
            return;
        };
        if state.kind != TurnKind::User || state.prompt_acknowledged {
            return;
        }
        let Some(text) = state.prompt.take() else {
            return;
        };
        state.prompt_acknowledged = true;
        let turn = state.id;
        if trigger == "init" {
            info!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = %turn,
                trigger,
                bytes = text.len(),
                action = "prompt_acknowledged",
                "the CLI took the turn's prompt"
            );
        } else {
            warn!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = %turn,
                trigger,
                frame_type,
                bytes = text.len(),
                action = "prompt_acknowledged",
                "no system/init preceded the answer; acknowledged from the first answer frame"
            );
        }
        let harness_item_id = format!("user:{turn}:sent");
        self.user_message(&Route::Primary, turn, harness_item_id, text, out);
    }

    /// Whether `text` is the route's delegated prompt, seen for the first time.
    fn take_delegated_prompt(&mut self, route: &Route, text: &str) -> bool {
        let Route::Task(id) = route else {
            return false;
        };
        let Some(route) = self.session.routes.get_mut(id) else {
            return false;
        };
        if route.prompt_delivered || route.prompt.as_deref() != Some(text) {
            return false;
        }
        route.prompt_delivered = true;
        true
    }

    /// The user message of a turn: a primary turn's prompt, or a sub-agent's delegated prompt.
    fn user_message(
        &mut self,
        route: &Route,
        turn: TurnId,
        harness_item_id: String,
        text: String,
        out: &mut Out,
    ) {
        let id = ItemId::new();
        let thread = self.thread_of(route);
        out.push(self.event(AgentEvent::ItemStarted {
            thread,
            turn,
            item: ItemStart {
                id,
                harness_item_id: harness_item_id.clone(),
                kind: ItemKind::UserMessage,
                command: None,
                tool: None,
            },
        }));
        out.push(self.event(AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id,
                harness_item_id,
                payload: ItemPayload::UserMessage { text },
                created_at: Utc::now(),
            },
        }));
    }

    /// An `Activity` item started and completed together.
    fn activity(
        &mut self,
        route: &Route,
        turn: TurnId,
        harness_item_id: String,
        title: String,
        out: &mut Out,
    ) {
        let id = ItemId::new();
        let thread = self.thread_of(route);
        out.push(self.event(AgentEvent::ItemStarted {
            thread,
            turn,
            item: ItemStart {
                id,
                harness_item_id: harness_item_id.clone(),
                kind: ItemKind::Activity,
                command: None,
                tool: None,
            },
        }));
        out.push(self.event(AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id,
                harness_item_id,
                payload: ItemPayload::Activity {
                    title,
                    detail: None,
                    metadata: None,
                    subagent: None,
                },
                created_at: Utc::now(),
            },
        }));
    }

    fn complete_tool(
        &mut self,
        route: &Route,
        turn: TurnId,
        result: ToolResultBlock,
        tool_use_result: Option<&Value>,
        meta: &[ToolResultMeta],
        out: &mut Out,
    ) {
        let thread = self.thread_of(route);
        let Some(state) = self.turn_of_mut(route) else {
            return;
        };
        let Some(open) = state.items.tools.remove(&result.tool_use_id) else {
            warn!(
                thread_id = %thread,
                harness_thread_id = %self.harness_thread_id,
                route = display_opt(route.label()),
                turn_id = %turn,
                native_item_id = %result.tool_use_id,
                "skipping a tool_result that matches no open tool call"
            );
            return;
        };
        let not_executed = meta
            .iter()
            .any(|entry| entry.id == result.tool_use_id && entry.non_execution_kind.is_some());
        let is_error = result.is_error == Some(true);
        let status = if not_executed || state.denied_tool_use_ids.contains(&result.tool_use_id) {
            "declined"
        } else if is_error {
            "failed"
        } else {
            "completed"
        };
        // An `Agent` call's completion carries its route's link, as it stands now.
        let subagent = self
            .session
            .routes
            .get_mut(&result.tool_use_id)
            .map(|delegated| {
                delegated.call_open = false;
                delegated.link(delegated.outcome_action())
            });
        debug!(
            thread_id = %thread,
            harness_thread_id = %self.harness_thread_id,
            route = display_opt(route.label()),
            turn_id = %turn,
            native_item_id = %result.tool_use_id,
            status,
            "tool call completed"
        );
        // A background `Bash` call's result only says the command went to a task: the item is
        // running, its process id the task's, and the CLI's note is not its output.
        let background_task = (status == "completed")
            .then(|| {
                let text = tool_result_text(result.content.as_ref());
                self.background_task_of(
                    route,
                    turn,
                    &result.tool_use_id,
                    &open,
                    &text,
                    tool_use_result,
                )
            })
            .flatten();
        let description = open.description;
        let payload = match open.call {
            ToolKind::Command { command } if background_task.is_some() => {
                info!(
                    thread_id = %thread,
                    harness_thread_id = %self.harness_thread_id,
                    route = display_opt(route.label()),
                    turn_id = %turn,
                    task_id = display_opt(background_task.as_deref()),
                    tool_call_id = %result.tool_use_id,
                    item_id = %open.item_id,
                    command = %command,
                    action = "background_command",
                    "the command runs in the background; its item stays running"
                );
                ItemPayload::CommandExecution {
                    description,
                    command,
                    cwd: self.workspace_root.clone(),
                    output: String::new(),
                    output_truncated: false,
                    output_original_bytes: None,
                    output_original_lines: None,
                    exit_code: None,
                    status: Some("in_progress".into()),
                    process_id: background_task,
                    duration_ms: None,
                }
            }
            ToolKind::Command { command } => ItemPayload::CommandExecution {
                description,
                command,
                cwd: self.workspace_root.clone(),
                output: command_output(tool_use_result)
                    .unwrap_or_else(|| tool_result_text(result.content.as_ref())),
                output_truncated: false,
                output_original_bytes: None,
                output_original_lines: None,
                exit_code: None,
                status: Some(status.into()),
                process_id: None,
                duration_ms: None,
            },
            ToolKind::FileChange { path } => {
                let created = tool_use_result
                    .and_then(|value| value.get("type"))
                    .and_then(Value::as_str)
                    == Some("create");
                let change = if created {
                    FileChangeKind::Created
                } else {
                    FileChangeKind::Modified
                };
                ItemPayload::FileChange {
                    path: path.clone(),
                    change,
                    changes: vec![FileChangeEntry {
                        path,
                        change,
                        diff: None,
                        captured_diff: None,
                    }],
                    status: Some(status.into()),
                }
            }
            ToolKind::Call {
                name,
                server,
                input,
            } => ItemPayload::ToolCall {
                description,
                name,
                input,
                output: result.content.as_ref().map(tool_result_json),
                server,
                status: Some(status.into()),
                metadata: None,
                subagent,
                error: is_error.then(|| tool_result_text(result.content.as_ref())),
            },
        };
        out.push(self.event(AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: open.item_id,
                harness_item_id: result.tool_use_id,
                payload,
                created_at: Utc::now(),
            },
        }));
    }

    /// The `local_bash` task running the background command of `tool_use_id`, its command given
    /// the output file `result_text` names: the task whose command names the call, else the
    /// result's `backgroundTaskId` when it names a `local_bash` task. A task that has no command
    /// yet (a foreground call backgrounded while it ran) adopts this call's item now.
    fn background_task_of(
        &mut self,
        route: &Route,
        turn: TurnId,
        tool_use_id: &str,
        open: &OpenTool,
        result_text: &str,
        tool_use_result: Option<&Value>,
    ) -> Option<String> {
        let tasks = &self.session.tasks;
        let task_id = tasks
            .iter()
            .find(|(_, entry)| {
                entry
                    .command
                    .as_ref()
                    .is_some_and(|command| command.tool_use_id == tool_use_id)
            })
            .map(|(id, _)| id.clone())
            .or_else(|| {
                tool_use_result
                    .and_then(|value| value.get("backgroundTaskId"))
                    .and_then(Value::as_str)
                    .filter(|id| {
                        tasks
                            .get(*id)
                            .is_some_and(|entry| entry.kind == TaskType::LocalBash)
                    })
                    .map(str::to_owned)
            })?;
        let ToolKind::Command { command } = &open.call else {
            return None;
        };
        let thread = self.thread_of(route);
        let entry = self.session.tasks.get_mut(&task_id)?;
        let adopted = entry.command.get_or_insert_with(|| BackgroundCommand {
            route: route.clone(),
            thread,
            turn,
            item_id: open.item_id,
            tool_use_id: tool_use_id.to_owned(),
            command: command.clone(),
            description: open.description.clone(),
            started_at_ms: open.started_at_ms,
            output_file: None,
            terminal: None,
            end_time_ms: None,
        });
        adopted.output_file = background_output_path(result_text, &task_id);
        Some(task_id)
    }

    /// Get-or-mint the item id for a native key, so start, delta and completion share it.
    fn resolve_item(&mut self, route: &Route, key: &NativeItemKey) -> ItemId {
        match self.turn_of_mut(route) {
            Some(turn) => *turn
                .items
                .item_ids
                .entry(key.clone())
                .or_insert_with(ItemId::new),
            None => ItemId::new(),
        }
    }

    // ---- approvals and server requests ---------------------------------------------------------

    /// The route an ask belongs to: by its `agent_id` (the task of a route), else by its
    /// `tool_use_id` among the routes' open tool calls, else the primary.
    fn ask_route(
        &self,
        request_id: &str,
        agent_id: Option<&str>,
        tool_use_id: Option<&str>,
    ) -> Route {
        let Some(agent_id) = agent_id else {
            return Route::Primary;
        };
        let live = |id: &String| {
            self.session
                .routes
                .get(id)
                .is_some_and(|route| !route.ended())
        };
        if let Some(id) = self
            .session
            .tasks
            .get(agent_id)
            .and_then(|entry| entry.route.as_ref())
            .filter(|id| live(id))
        {
            return Route::Task(id.clone());
        }
        if let Some(route) = tool_use_id.and_then(|id| self.route_with_open_tool(id)) {
            debug!(
                thread_id = %self.thread_of(&route),
                harness_thread_id = %self.harness_thread_id,
                route = display_opt(route.label()),
                request_id = %request_id,
                agent_id = %agent_id,
                native_item_id = display_opt(tool_use_id),
                action = "can_use_tool",
                "routed a sub-agent's ask by its tool use; its task is unknown"
            );
            return route;
        }
        warn!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = display_opt(self.active_turn()),
            request_id = %request_id,
            agent_id = %agent_id,
            native_item_id = display_opt(tool_use_id),
            action = "ask_route_unknown",
            "a sub-agent asked to use a tool, but no route is known for it; publishing the ask on \
             the primary thread"
        );
        Route::Primary
    }

    fn on_can_use_tool(
        &mut self,
        request_id: String,
        request: ToolPermissionRequest,
        agent_id: Option<String>,
        raw: &Value,
        out: &mut Out,
    ) {
        let tool_name = request.tool_name.as_str();
        let route = self.ask_route(
            &request_id,
            agent_id.as_deref(),
            request.tool_use_id.as_deref(),
        );
        if matches!(tool_name, "ExitPlanMode" | "EnterPlanMode") {
            // With `--disallowedTools EnterPlanMode ExitPlanMode` this ask never appears; if it
            // does, Giskard still owns the mode, so answer it without a user.
            warn!(
                thread_id = %self.thread_of(&route),
                harness_thread_id = %self.harness_thread_id,
                route = display_opt(route.label()),
                turn_id = display_opt(self.turn_of(&route).map(|turn| turn.id)),
                action = "deny_plan_mode_tool",
                request_id = %request_id,
                tool_name,
                native_item_id = display_opt(request.tool_use_id.as_deref()),
                "denying a plan-mode tool the CLI asked to use; Giskard chooses the mode per turn"
            );
            if let (Some(turn), Some(tool_use_id)) =
                (self.turn_of_mut(&route), &request.tool_use_id)
            {
                turn.denied_tool_use_ids.insert(tool_use_id.clone());
            }
            out.push(MapperOutput::Reply(json!({
                "type": "control_response",
                "response": {
                    "subtype": "success",
                    "request_id": request_id,
                    "response": {"behavior": "deny", "message": PLAN_MODE_DENIAL},
                },
            })));
            return;
        }
        let (route, turn) = match self.frame_turn(&route, "control_request", out) {
            Some(turn) => (route, turn),
            // The route ended between the lookup and now: keep the ask answerable.
            None => (Route::Primary, self.ensure_turn("control_request", out)),
        };
        let thread = self.thread_of(&route);
        info!(
            thread_id = %thread,
            harness_thread_id = %self.harness_thread_id,
            route = display_opt(route.label()),
            turn_id = %turn,
            action = "can_use_tool",
            request_id = %request_id,
            tool_name,
            native_item_id = display_opt(request.tool_use_id.as_deref()),
            agent_id = display_opt(agent_id.as_deref()),
            "Claude Code asked to use a tool"
        );
        if tool_name == "AskUserQuestion" {
            let id = ServerRequestId::new(request_id.clone());
            out.push(self.event(AgentEvent::ServerRequestReceived {
                thread,
                turn: Some(turn),
                request: ServerRequest {
                    id: id.clone(),
                    method: "claude/ask_user_question".into(),
                    params: ask_user_question_params(&request.input),
                    received_at: Utc::now(),
                },
            }));
            out.push(MapperOutput::PendingServerRequest {
                id,
                thread,
                request_id,
                subtype: "can_use_tool".into(),
                input: request.input,
            });
            return;
        }
        let description = raw
            .get("description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let display_name = raw
            .get("display_name")
            .and_then(Value::as_str)
            .unwrap_or(tool_name)
            .to_owned();
        let kind = match classify_tool(tool_name, request.input.clone()) {
            ToolKind::Command { command } => ApprovalKind::CommandExecution {
                command,
                cwd: self.workspace_root.clone(),
            },
            ToolKind::FileChange { path } => ApprovalKind::FileChange {
                path,
                change: FileChangeKind::Modified,
            },
            ToolKind::Call {
                name,
                server: Some(server),
                ..
            } => ApprovalKind::McpToolCall {
                server,
                tool_name: name,
            },
            ToolKind::Call { .. } => ApprovalKind::Permission {
                detail: description.clone().unwrap_or_else(|| tool_name.to_owned()),
            },
        };
        let mut metadata = vec![ApprovalMetadata::Text {
            label: "Tool".into(),
            value: display_name,
        }];
        if let Some(path) = &request.blocked_path {
            metadata.push(ApprovalMetadata::Path {
                label: "Blocked path".into(),
                path: path.into(),
                source_link: false,
            });
        }
        // Name each suggestion by its type and destination only; its rules are kept raw on the
        // pending ask for `AcceptForSession` to echo back.
        let suggestions: Vec<Value> = raw
            .get("permission_suggestions")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for suggestion in &suggestions {
            let suggestion_type = suggestion.get("type").and_then(Value::as_str);
            let destination = suggestion.get("destination").and_then(Value::as_str);
            let value = match (suggestion_type, destination) {
                (Some(kind), Some(destination)) => format!("{kind} ({destination})"),
                (Some(kind), None) => kind.to_owned(),
                (None, _) => continue,
            };
            metadata.push(ApprovalMetadata::Text {
                label: "Suggestion".into(),
                value,
            });
        }
        let id = ApprovalId::new(request_id.clone());
        out.push(self.event(AgentEvent::ApprovalRequested {
            thread,
            turn,
            request: ApprovalRequest {
                id: id.clone(),
                kind,
                reason: description,
                metadata,
                available: vec![
                    ApprovalDecision::Accept,
                    ApprovalDecision::AcceptForSession,
                    ApprovalDecision::Decline,
                    ApprovalDecision::Cancel,
                ],
            },
        }));
        out.push(MapperOutput::PendingApproval {
            id,
            request_id,
            tool_use_id: request.tool_use_id,
            tool_name: tool_name.to_owned(),
            suggestions,
            thread,
        });
    }

    fn on_control_request(&mut self, request_id: String, subtype: &str, raw: Value, out: &mut Out) {
        info!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = display_opt(self.active_turn()),
            action = "control_request",
            request_id = %request_id,
            subtype,
            "Claude Code sent a control request"
        );
        let id = ServerRequestId::new(request_id.clone());
        out.push(self.event(AgentEvent::ServerRequestReceived {
            thread: self.thread,
            turn: self.active_turn(),
            request: ServerRequest {
                id: id.clone(),
                method: format!("claude/{subtype}"),
                params: raw,
                received_at: Utc::now(),
            },
        }));
        out.push(MapperOutput::PendingServerRequest {
            id,
            thread: self.thread,
            request_id,
            subtype: subtype.to_owned(),
            input: Value::Null,
        });
    }

    fn event(&self, event: AgentEvent) -> MapperOutput {
        MapperOutput::Event(event)
    }
}

/// The `ServerRequestReceived` params of an `AskUserQuestion` ask: `{questions: [...]}` with each
/// question the CLI's object plus `"id": "<index>"`. The browser's question card keys its answers
/// by `id`, and the CLI's questions carry none; the answer maps each id back to its index.
fn ask_user_question_params(input: &Value) -> Value {
    let questions: Vec<Value> = input
        .get("questions")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .enumerate()
        .map(|(index, question)| {
            let mut question = question.clone();
            if let Some(object) = question.as_object_mut() {
                object.insert("id".into(), Value::String(index.to_string()));
            }
            question
        })
        .collect();
    json!({ "questions": questions })
}

/// Whether a task in `status` has ended.
fn is_terminal(status: &TaskStatus) -> bool {
    matches!(
        status,
        TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Killed | TaskStatus::Stopped
    )
}

/// One child API message's usage, the same three-summand input as `token_usage`.
fn assistant_token_usage(usage: &AssistantUsage) -> TokenUsage {
    let input = u64::from(usage.input_tokens)
        + u64::from(usage.cache_creation_input_tokens)
        + u64::from(usage.cache_read_input_tokens);
    TokenUsage::new(input, u64::from(usage.output_tokens))
}

/// The completion of a tool call whose turn ended before its `tool_result`: `interrupted`, with
/// no output. An `Agent` call carries its route's link.
fn interrupted_payload(
    call: ToolKind,
    description: Option<String>,
    workspace_root: &std::path::Path,
    subagent: Option<SubagentLink>,
) -> ItemPayload {
    const INTERRUPTED: &str = "interrupted";
    match call {
        ToolKind::Command { command } => ItemPayload::CommandExecution {
            description,
            command,
            cwd: workspace_root.to_path_buf(),
            output: String::new(),
            output_truncated: false,
            output_original_bytes: None,
            output_original_lines: None,
            exit_code: None,
            status: Some(INTERRUPTED.into()),
            process_id: None,
            duration_ms: None,
        },
        ToolKind::FileChange { path } => ItemPayload::FileChange {
            path: path.clone(),
            change: FileChangeKind::Modified,
            changes: vec![FileChangeEntry {
                path,
                change: FileChangeKind::Modified,
                diff: None,
                captured_diff: None,
            }],
            status: Some(INTERRUPTED.into()),
        },
        ToolKind::Call {
            name,
            server,
            input,
        } => ItemPayload::ToolCall {
            description,
            name,
            input,
            output: None,
            server,
            status: Some(INTERRUPTED.into()),
            metadata: None,
            subagent,
            error: None,
        },
    }
}

/// Plan §6: Claude reports cached input separately, and all three summands are context.
fn token_usage(usage: &UsageInfo) -> TokenUsage {
    let input = u64::from(usage.input_tokens)
        + u64::from(usage.cache_creation_input_tokens)
        + u64::from(usage.cache_read_input_tokens);
    TokenUsage::new(input, u64::from(usage.output_tokens))
}

/// The usage of a result's last API request, which is what fills the context window when the turn
/// ends. `result.usage` itself sums every request of the turn; `iterations` carries the last one.
/// A result without iterations (the degenerate `/compact` result) falls back to the sum.
fn last_request_usage(usage: &UsageInfo) -> TokenUsage {
    match usage.iterations.last() {
        Some(last) => {
            let input = u64::from(last.input_tokens)
                + u64::from(last.cache_creation_input_tokens.unwrap_or(0))
                + u64::from(last.cache_read_input_tokens.unwrap_or(0));
            TokenUsage::new(input, u64::from(last.output_tokens))
        }
        None => token_usage(usage),
    }
}

fn result_status(result: &ResultMessage, interrupt_sent: bool) -> TurnStatus {
    if !result.is_error {
        return TurnStatus {
            kind: TurnStatusKind::Completed,
            message: None,
        };
    }
    let aborted = matches!(
        result.terminal_reason.as_deref(),
        Some("aborted_streaming" | "aborted_tools")
    );
    if interrupt_sent || aborted {
        return TurnStatus {
            kind: TurnStatusKind::Interrupted,
            message: None,
        };
    }
    let message = match result.result.as_deref().filter(|text| !text.is_empty()) {
        Some(text) => text.to_owned(),
        None if !result.errors.is_empty() => result.errors.join("; "),
        None => result.subtype.as_str().to_owned(),
    };
    TurnStatus {
        kind: TurnStatusKind::Failed,
        message: Some(message),
    }
}

fn classify_tool(name: &str, input: Value) -> ToolKind {
    let text = |key: &str| input.get(key).and_then(Value::as_str).map(str::to_owned);
    match name {
        "Bash" => ToolKind::Command {
            command: text("command").unwrap_or_default(),
        },
        "Write" | "Edit" | "NotebookEdit" => ToolKind::FileChange {
            path: text("file_path")
                .or_else(|| text("notebook_path"))
                .unwrap_or_default()
                .into(),
        },
        _ => match name
            .strip_prefix("mcp__")
            .and_then(|rest| rest.split_once("__"))
        {
            Some((server, tool)) => ToolKind::Call {
                name: tool.to_owned(),
                server: Some(server.to_owned()),
                input,
            },
            None => ToolKind::Call {
                name: name.to_owned(),
                server: None,
                input,
            },
        },
    }
}

/// A tool call's human-readable title: the call's own `description` for the tools that take one
/// (`Bash`, `Monitor`, `Agent`), else one built from the input's main argument ("Read src/main.rs",
/// `Grep "foo" in src`). MCP tools (`mcp__server__tool`) have no common argument to name: none.
fn tool_description(name: &str, input: &Value, workspace_root: &std::path::Path) -> Option<String> {
    let text = |key: &str| {
        input
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
    };
    let path = |key: &str| text(key).map(|value| workspace_relative(value, workspace_root));
    let scoped = |title: String| match path("path") {
        Some(path) => format!("{title} in {path}"),
        None => title,
    };
    let title = match name {
        "Bash" | "Monitor" | "Agent" | "Task" => text("description")?.to_owned(),
        "Read" => format!("Read {}", path("file_path")?),
        "Grep" => {
            let title = format!("Grep \"{}\"", text("pattern")?);
            match (path("path"), text("glob")) {
                (Some(path), Some(glob)) => format!("{title} in {path} ({glob})"),
                (None, Some(glob)) => format!("{title} in {glob}"),
                _ => scoped(title),
            }
        }
        "Glob" => scoped(format!("Glob {}", text("pattern")?)),
        "WebFetch" => format!("Fetch {}", text("url")?),
        "WebSearch" => format!("Web search \"{}\"", text("query")?),
        "Skill" => format!("Skill {}", text("skill")?),
        _ => return None,
    };
    command_description(Some(&title))
}

/// `path` relative to the workspace when it lies inside it, as typed otherwise.
fn workspace_relative(path: &str, workspace_root: &std::path::Path) -> String {
    match std::path::Path::new(path).strip_prefix(workspace_root) {
        Ok(relative) if relative.as_os_str().is_empty() => ".".to_owned(),
        Ok(relative) => relative.display().to_string(),
        Err(_) => path.to_owned(),
    }
}

/// A Bash call's output from its `{stdout, stderr, …}` `tool_use_result`: stdout, then stderr.
fn command_output(tool_use_result: Option<&Value>) -> Option<String> {
    let result = tool_use_result?.as_object()?;
    let stdout = result.get("stdout")?.as_str()?;
    let stderr = result.get("stderr").and_then(Value::as_str).unwrap_or("");
    let mut output = stdout.to_owned();
    if !stderr.is_empty() {
        if !output.is_empty() && !output.ends_with('\n') {
            output.push('\n');
        }
        output.push_str(stderr);
    }
    Some(output)
}

fn tool_result_text(content: Option<&ToolResultContent>) -> String {
    match content {
        Some(ToolResultContent::Text(text)) => text.clone(),
        Some(ToolResultContent::Structured(blocks)) => blocks
            .iter()
            .filter_map(|block| block.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        None => String::new(),
    }
}

fn tool_result_json(content: &ToolResultContent) -> Value {
    match content {
        ToolResultContent::Text(text) => Value::String(text.clone()),
        ToolResultContent::Structured(blocks) => Value::Array(blocks.clone()),
    }
}

fn content_block_type(block: &ContentBlock) -> &'static str {
    match block {
        ContentBlock::Text(_) => "text",
        ContentBlock::Image(_) => "image",
        ContentBlock::Thinking(_) => "thinking",
        ContentBlock::ToolUse(_) => "tool_use",
        ContentBlock::ToolResult(_) => "tool_result",
        ContentBlock::ServerToolUse(_) => "server_tool_use",
        ContentBlock::WebSearchToolResult(_) => "web_search_tool_result",
        ContentBlock::CodeExecutionToolResult(_) => "code_execution_tool_result",
        ContentBlock::McpToolUse(_) => "mcp_tool_use",
        ContentBlock::McpToolResult(_) => "mcp_tool_result",
        ContentBlock::ContainerUpload(_) => "container_upload",
        ContentBlock::Fallback(_) => "fallback",
        ContentBlock::Unknown(_) => "unknown",
    }
}

/// How much of a background command's output file is read: its head and its tail. The server
/// bounds persisted output again; the read takes microseconds at this size, so it stays on the
/// supervisor's task.
const BACKGROUND_OUTPUT_HEAD: u64 = 64 * 1024;
const BACKGROUND_OUTPUT_TAIL: u64 = 64 * 1024;

/// What a background command's output file says.
#[derive(Debug, Default, PartialEq, Eq)]
struct BackgroundOutput {
    /// stdout and stderr as written, without the CLI's marker line.
    text: String,
    /// From `[exited with code N]`; none after `[killed]` or without a marker.
    exit_code: Option<i32>,
    /// The file's size, when only its head and tail were read.
    original_bytes: Option<u64>,
}

/// The frame type, when `frame` is the CLI answering the primary thread's turn: the backstop
/// that acknowledges a prompt no `system/init` acknowledged. A replayed user frame is not an
/// answer, nor is a sub-agent's frame.
fn answers_the_prompt(frame: &Frame) -> Option<&'static str> {
    match frame {
        Frame::Assistant(message) if message.parent_tool_use_id.is_none() => Some("assistant"),
        Frame::Stream(stream) if stream.parent_tool_use_id.is_none() => Some("stream_event"),
        Frame::User(message)
            if message.is_replay != Some(true) && message.parent_tool_use_id.is_none() =>
        {
            Some("user")
        }
        Frame::Result(_) => Some("result"),
        Frame::CanUseTool { agent_id: None, .. } => Some("control_request"),
        _ => None,
    }
}

/// The item status of a background command that ended with the task status `status`.
fn command_status(status: &TaskStatus) -> &'static str {
    match status {
        TaskStatus::Completed => "completed",
        TaskStatus::Killed | TaskStatus::Stopped => "terminated",
        _ => "failed",
    }
}

/// The output file a background `Bash` call's `tool_result` names: "Command running in
/// background with ID: <id>. Output is being written to: <path>/<id>.output. …". Matched up to
/// the task's own `<id>.output`, so a path with spaces, or another `.output` earlier in it,
/// survives; `None` for any other text.
fn background_output_path(text: &str, task_id: &str) -> Option<PathBuf> {
    const MARKER: &str = "Output is being written to: ";
    let rest = &text[text.find(MARKER)? + MARKER.len()..];
    let file = format!("{task_id}.output");
    let end = rest.find(&file)? + file.len();
    Some(PathBuf::from(&rest[..end]))
}

/// Read the output file the CLI writes for a `local_bash` task (`…/tasks/<task_id>.output`):
/// stdout and stderr as written, then a blank line and one marker line, `[exited with code N]`
/// or `[killed]`. The file and its marker are the CLI's own and unversioned, so an unknown last
/// line is kept as output and gives no exit code. At most `BACKGROUND_OUTPUT_HEAD` bytes from
/// the start and `BACKGROUND_OUTPUT_TAIL` from the end are read.
fn background_output(path: &Path) -> std::io::Result<BackgroundOutput> {
    let mut file = std::fs::File::open(path)?;
    let size = file.metadata()?.len();
    let (text, original_bytes) = if size <= BACKGROUND_OUTPUT_HEAD + BACKGROUND_OUTPUT_TAIL {
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        (String::from_utf8_lossy(&bytes).into_owned(), None)
    } else {
        let mut head = Vec::new();
        (&mut file)
            .take(BACKGROUND_OUTPUT_HEAD)
            .read_to_end(&mut head)?;
        file.seek(SeekFrom::Start(size - BACKGROUND_OUTPUT_TAIL))?;
        let mut tail = Vec::new();
        file.read_to_end(&mut tail)?;
        let omitted = size - BACKGROUND_OUTPUT_HEAD - BACKGROUND_OUTPUT_TAIL;
        let text = format!(
            "{}\n[… {omitted} bytes omitted …]\n{}",
            String::from_utf8_lossy(&head),
            String::from_utf8_lossy(&tail)
        );
        (text, Some(size))
    };
    let body = text.trim_end_matches(['\n', '\r']);
    let (rest, last) = match body.rsplit_once('\n') {
        Some((rest, last)) => (rest, last.trim_end()),
        None => ("", body.trim_end()),
    };
    let exit_code = if last == "[killed]" {
        None
    } else if let Some(code) = last
        .strip_prefix("[exited with code ")
        .and_then(|code| code.strip_suffix(']'))
        .and_then(|code| code.parse::<i32>().ok())
    {
        Some(code)
    } else {
        return Ok(BackgroundOutput {
            text,
            exit_code: None,
            original_bytes,
        });
    };
    // The marker and the blank line before it.
    let rest = rest.strip_suffix('\n').unwrap_or(rest);
    let rest = rest.strip_suffix('\r').unwrap_or(rest);
    Ok(BackgroundOutput {
        text: rest.to_owned(),
        exit_code,
        original_bytes,
    })
}

#[cfg(test)]
mod tests {
    use tracing_test::traced_test;

    use super::*;
    use crate::log_checks::{a_line_with, lines_with, no_line_with};

    const WORKSPACE: &str = "/work/project";

    fn fixture(name: &str, extension: &str) -> Option<String> {
        let path = format!(
            "{}/tests/fixtures/{name}.{extension}",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read_to_string(path).ok()
    }

    fn out_lines(name: &str) -> Vec<String> {
        fixture(name, "out.jsonl")
            .unwrap_or_else(|| panic!("fixture {name} has no .out.jsonl"))
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn in_frames(name: &str) -> Vec<Value> {
        fixture(name, "in.jsonl")
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    /// Whether the recorder interrupted: an `interrupt` control request, or a deny with
    /// `interrupt: true`.
    fn interrupted(name: &str) -> bool {
        in_frames(name).iter().any(|frame| {
            frame.pointer("/request/subtype") == Some(&json!("interrupt"))
                || frame.pointer("/response/response/interrupt") == Some(&json!(true))
        })
    }

    fn new_mapper() -> ClaudeMapper {
        ClaudeMapper::new(
            ThreadId::new(),
            "f18693ff-2d11-4f87-9556-2b527e19e081".into(),
            PathBuf::from(WORKSPACE),
        )
    }

    /// Drive `mapper` through `lines`, beginning one turn per recorded user message whenever no
    /// turn is active (so a second message opens its turn only once the first completed). The
    /// last user message's turn has `kind`; earlier ones are `User`. Returns each line's outputs.
    fn drive_lines(
        mapper: &mut ClaudeMapper,
        lines: &[String],
        user_messages: usize,
        kind: TurnKind,
        interrupt: bool,
    ) -> Vec<Vec<MapperOutput>> {
        let mut remaining = user_messages;
        lines
            .iter()
            .map(|line| {
                let mut outputs = Vec::new();
                if mapper.active_turn().is_none() && remaining > 0 {
                    remaining -= 1;
                    let turn_kind = if remaining == 0 { kind } else { TurnKind::User };
                    outputs.extend(mapper.begin_turn(TurnId::new(), turn_kind));
                    if interrupt {
                        mapper.note_interrupt_sent();
                    }
                }
                outputs.extend(mapper.map_line(line));
                outputs
            })
            .collect()
    }

    fn drive(mapper: &mut ClaudeMapper, name: &str, kind: TurnKind) -> Vec<Vec<MapperOutput>> {
        let users = in_frames(name)
            .iter()
            .filter(|frame| frame["type"] == "user")
            .count();
        drive_lines(mapper, &out_lines(name), users, kind, interrupted(name))
    }

    /// Reads tests/fixtures/<name>.out.jsonl and drives a fresh mapper through it.
    fn run_fixture(name: &str, kind: TurnKind) -> Vec<MapperOutput> {
        drive(&mut new_mapper(), name, kind)
            .into_iter()
            .flatten()
            .collect()
    }

    fn events(outputs: &[MapperOutput]) -> Vec<&AgentEvent> {
        outputs
            .iter()
            .filter_map(|output| match output {
                MapperOutput::Event(event) => Some(event),
                _ => None,
            })
            .collect()
    }

    fn completed_items(outputs: &[MapperOutput]) -> Vec<&Item> {
        events(outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::ItemCompleted { item, .. } => Some(item),
                _ => None,
            })
            .collect()
    }

    fn started_items(outputs: &[MapperOutput]) -> Vec<&ItemStart> {
        events(outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::ItemStarted { item, .. } => Some(item),
                _ => None,
            })
            .collect()
    }

    fn turn_completions(outputs: &[MapperOutput]) -> Vec<(TurnId, TokenUsage, TurnStatus)> {
        events(outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::TurnCompleted {
                    turn,
                    usage,
                    status,
                    ..
                } => Some((*turn, *usage, status.clone())),
                _ => None,
            })
            .collect()
    }

    fn usage_updates(outputs: &[MapperOutput]) -> Vec<(TokenUsage, Option<u32>)> {
        events(outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::TurnUsageUpdated {
                    usage,
                    context_window,
                    ..
                } => Some((*usage, *context_window)),
                _ => None,
            })
            .collect()
    }

    fn notices(outputs: &[MapperOutput]) -> Vec<&str> {
        events(outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::Notice { message, .. } => Some(message.as_str()),
                _ => None,
            })
            .collect()
    }

    fn command_statuses(outputs: &[MapperOutput]) -> Vec<(String, Option<String>)> {
        completed_items(outputs)
            .into_iter()
            .filter_map(|item| match &item.payload {
                ItemPayload::CommandExecution {
                    command, status, ..
                } => Some((command.clone(), status.clone())),
                _ => None,
            })
            .collect()
    }

    /// The events on `thread`, in order.
    fn on_thread(outputs: &[MapperOutput], thread: ThreadId) -> Vec<MapperOutput> {
        outputs
            .iter()
            .filter(|output| {
                matches!(output, MapperOutput::Event(event) if crate::session::event_thread(event) == thread)
            })
            .cloned()
            .collect()
    }

    /// Every `RouteOpened`: `(thread, harness_thread_id, parent_harness_thread_id, agent_name)`.
    fn routes_opened(outputs: &[MapperOutput]) -> Vec<(ThreadId, String, String, Option<String>)> {
        outputs
            .iter()
            .filter_map(|output| match output {
                MapperOutput::RouteOpened {
                    thread,
                    harness_thread_id,
                    parent_harness_thread_id,
                    agent_name,
                    ..
                } => Some((
                    *thread,
                    harness_thread_id.clone(),
                    parent_harness_thread_id.clone(),
                    agent_name.clone(),
                )),
                _ => None,
            })
            .collect()
    }

    fn routes_reopened(outputs: &[MapperOutput]) -> Vec<ThreadId> {
        outputs
            .iter()
            .filter_map(|output| match output {
                MapperOutput::RouteReopened { thread } => Some(*thread),
                _ => None,
            })
            .collect()
    }

    /// `(kind, text)` of the user and agent messages completed in `events`, in order.
    fn messages(events: &[&AgentEvent]) -> Vec<(&'static str, String)> {
        events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::ItemCompleted { item, .. } => match &item.payload {
                    ItemPayload::UserMessage { text } => Some(("user", text.clone())),
                    ItemPayload::AgentMessage { text } => Some(("agent", text.clone())),
                    _ => None,
                },
                _ => None,
            })
            .collect()
    }

    /// The resumed run of `name` (a delegation, then `SendMessage` to the same sub-agent): one
    /// route, whose thread gets a second turn opening with the message the sub-agent was sent.
    fn assert_resumed_on_one_thread(name: &str, outputs: &[MapperOutput]) -> ThreadId {
        let opened = routes_opened(outputs);
        assert_eq!(opened.len(), 1, "{name}: {opened:?}");
        let child = opened[0].0;
        let child_events: Vec<MapperOutput> = on_thread(outputs, child);
        let turns = per_turn(&child_events);
        assert_eq!(turns.len(), 2, "{name}: {turns:?}");
        let statuses: Vec<TurnStatusKind> = turn_completions(&child_events)
            .into_iter()
            .map(|(_, _, status)| status.kind)
            .collect();
        assert_eq!(
            statuses,
            [TurnStatusKind::Completed, TurnStatusKind::Completed],
            "{name}"
        );
        let resumed = messages(&turns[1]);
        let sent = out_lines(name)
            .iter()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|frame| frame["subtype"] == "task_started")
            .nth(1)
            .and_then(|frame| frame["prompt"].as_str().map(str::to_owned))
            .unwrap();
        assert_eq!(
            resumed.first(),
            Some(&("user", sent)),
            "{name}: {resumed:?}"
        );
        assert!(
            resumed
                .iter()
                .any(|(kind, text)| *kind == "agent" && text.contains("4272")),
            "{name}: {resumed:?}"
        );

        // The parent links the resumed run to the same thread, under the `SendMessage` call.
        let link = completed_items(outputs)
            .into_iter()
            .find_map(|item| match &item.payload {
                ItemPayload::Activity {
                    detail: Some(detail),
                    subagent: Some(link),
                    ..
                } if detail == "resumed by SendMessage" => Some(link.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{name}: no resume row"));
        assert_eq!(link.harness_thread_id, opened[0].1);
        assert_eq!(link.action, SubagentAction::Started);
        child
    }

    #[test]
    #[traced_test]
    fn a_resumed_background_sub_agent_runs_again_on_its_thread() {
        // The first run was backgrounded, and ended before `SendMessage` resumes it.
        let outputs = run_fixture("subagent-resume", TurnKind::User);
        let child = assert_resumed_on_one_thread("subagent-resume", &outputs);
        // The route outlived the delegating turn and was dropped at its own end: the resume
        // reopens it.
        assert_eq!(routes_reopened(&outputs), [child]);
        logs_assert(lines_with(
            1,
            &[" INFO ", r#"action="route_resumed""#, "reopened=true"],
        ));
        logs_assert(lines_with(0, &["route_trailing_frame"]));
    }

    #[test]
    #[traced_test]
    fn a_resumed_sub_agent_whose_route_was_dropped_is_reopened() {
        // Background tasks disabled: the first run was a foreground delegation, so its route was
        // dropped with the first turn. The resume runs awaited, in the second turn.
        let outputs = run_fixture("subagent-resume-awaited", TurnKind::User);
        let child = assert_resumed_on_one_thread("subagent-resume-awaited", &outputs);
        assert_eq!(routes_reopened(&outputs), [child]);
        assert_eq!(routes_closed(&outputs), [child, child]);
        let reopened = outputs
            .iter()
            .position(|output| matches!(output, MapperOutput::RouteReopened { .. }))
            .unwrap();
        let resumed_turn = outputs
            .iter()
            .enumerate()
            .filter(|(_, output)| matches!(output, MapperOutput::Event(AgentEvent::TurnStarted { thread, .. }) if *thread == child))
            .nth(1)
            .map(|(index, _)| index)
            .unwrap();
        assert!(
            reopened < resumed_turn,
            "the log is back before the turn starts"
        );
        logs_assert(lines_with(
            1,
            &[" INFO ", r#"action="route_resumed""#, "reopened=true"],
        ));
        logs_assert(lines_with(0, &["route_trailing_frame"]));
    }

    #[test]
    #[traced_test]
    fn a_resumed_foreground_sub_agent_is_reopened_as_a_background_run() {
        // Background tasks enabled, the first run in the foreground: its route was dropped with
        // the first turn, and `SendMessage` resumes it as a background run of the second.
        let outputs = run_fixture("subagent-resume-foreground", TurnKind::User);
        let child = assert_resumed_on_one_thread("subagent-resume-foreground", &outputs);
        assert_eq!(routes_reopened(&outputs), [child]);
        logs_assert(lines_with(0, &["route_trailing_frame"]));
    }

    #[test]
    #[traced_test]
    fn a_sub_agent_that_starts_its_own_run_runs_again_on_its_thread() {
        // A message `SendMessage` queued for a running sub-agent starts no run; later the
        // sub-agent's background command finishes and wakes it: a second run of the same task,
        // named by the route's own `Agent` call, after the route was dropped.
        let name = "subagent-message-running";
        let outputs = run_fixture(name, TurnKind::User);
        let opened = routes_opened(&outputs);
        assert_eq!(opened.len(), 1, "{opened:?}");
        let child = opened[0].0;
        assert_eq!(routes_reopened(&outputs), [child]);
        let child_events = on_thread(&outputs, child);
        let turns = per_turn(&child_events);
        assert_eq!(turns.len(), 2, "{turns:?}");
        // The woken run opens with no user message: what woke it is a `<task-notification>`.
        let resumed = messages(&turns[1]);
        assert!(
            resumed.iter().all(|(kind, _)| *kind == "agent"),
            "{resumed:?}"
        );
        assert!(
            resumed.iter().any(|(_, text)| text.contains("4272")),
            "{resumed:?}"
        );
        // No parent turn runs when it wakes: its resume row and its outcome reach the delegating
        // turn as late rows, after the first run's outcome there, each with an id of its own.
        let rows: Vec<(TurnId, &Item)> = events(&outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::ItemCompleted { turn, item, .. } => match &item.payload {
                    ItemPayload::Activity {
                        subagent: Some(link),
                        ..
                    } if link.harness_thread_id == opened[0].1 => Some((*turn, item)),
                    _ => None,
                },
                _ => None,
            })
            .collect();
        let details: Vec<Option<&str>> = rows
            .iter()
            .map(|(_, item)| match &item.payload {
                ItemPayload::Activity { detail, .. } => detail.as_deref(),
                _ => None,
            })
            .collect();
        assert_eq!(
            details,
            [Some("completed"), Some("resumed"), Some("completed")],
            "{rows:?}"
        );
        assert!(rows.iter().all(|(turn, _)| *turn == rows[0].0), "{rows:?}");
        let ids: HashSet<&str> = rows
            .iter()
            .map(|(_, item)| item.harness_item_id.as_str())
            .collect();
        assert_eq!(ids.len(), 3, "{ids:?}");
        // The link keeps the delegated prompt, never the `<task-notification>` that woke the run.
        let prompts: HashSet<Option<&str>> = rows
            .iter()
            .filter_map(|(_, item)| match &item.payload {
                ItemPayload::Activity {
                    subagent: Some(link),
                    ..
                } => Some(link.initial_prompt.as_deref()),
                _ => None,
            })
            .collect();
        assert_eq!(prompts.len(), 1, "{prompts:?}");
        assert!(
            prompts
                .iter()
                .all(|prompt| prompt.is_some_and(|text| !text.starts_with("<task-notification>"))),
            "{prompts:?}"
        );
        logs_assert(lines_with(
            1,
            &[" INFO ", r#"action="route_resumed""#, "reopened=true"],
        ));
        logs_assert(a_line_with(&[r#"action="subagent_outcome""#, "late=true"]));
        logs_assert(lines_with(0, &["route_trailing_frame"]));
    }

    /// The `Agent` call of `subagent-resume-respawn-first`, which the respawned child's frames
    /// name.
    fn respawn_route() -> String {
        out_lines("subagent-resume-respawn-first")
            .iter()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .find(|frame| frame["subtype"] == "task_started")
            .and_then(|frame| {
                frame["tool_use_id"]
                    .as_str()
                    .map(|id| format!("{TASK_ID_PREFIX}{id}"))
            })
            .unwrap()
    }

    #[test]
    #[traced_test]
    fn a_sub_agent_an_earlier_child_ran_resumes_on_its_thread() {
        // A respawned child (`--resume`) resumes a sub-agent the previous child ran: the run is
        // held until its first frame names that child's `Agent` call, then runs on the thread
        // the façade still binds to it.
        let name = "subagent-resume-respawn";
        let bound = ThreadId::new();
        let native = respawn_route();
        let mut mapper = new_mapper();
        let resolved = native.clone();
        mapper.set_route_threads(Arc::new(move |id: &str| (id == resolved).then_some(bound)));
        let outputs: Vec<MapperOutput> = drive(&mut mapper, name, TurnKind::User)
            .into_iter()
            .flatten()
            .collect();
        let opened = routes_opened(&outputs);
        assert_eq!(opened.len(), 1, "{opened:?}");
        assert_eq!(opened[0].0, bound);
        assert_eq!(opened[0].1, native);
        let child_events = on_thread(&outputs, bound);
        let turns = per_turn(&child_events);
        assert_eq!(turns.len(), 1, "{turns:?}");
        let resumed = messages(&turns[0]);
        let sent = out_lines(name)
            .iter()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .find(|frame| frame["subtype"] == "task_started")
            .and_then(|frame| frame["prompt"].as_str().map(str::to_owned))
            .unwrap();
        assert_eq!(resumed.first(), Some(&("user", sent)), "{resumed:?}");
        assert!(
            resumed
                .iter()
                .any(|(kind, text)| *kind == "agent" && text.contains("4272")),
            "{resumed:?}"
        );
        assert!(completed_items(&outputs).iter().any(|item| matches!(
            &item.payload,
            ItemPayload::Activity { detail: Some(detail), subagent: Some(link), .. }
                if detail == "resumed by SendMessage" && link.harness_thread_id == native
        )));
        logs_assert(lines_with(1, &[r#"action="route_resume_pending""#]));
        logs_assert(lines_with(
            0,
            &["no route was minted for its parent tool use"],
        ));
    }

    #[test]
    fn a_sub_agent_an_earlier_child_ran_gets_a_thread_when_none_is_bound() {
        // No thread bound to the native id (a façade that never saw it): the route still opens
        // under the original call, on a new thread, rather than under `SendMessage`.
        let outputs = run_fixture("subagent-resume-respawn", TurnKind::User);
        let opened = routes_opened(&outputs);
        assert_eq!(opened.len(), 1, "{opened:?}");
        assert_eq!(opened[0].1, respawn_route());
        assert_eq!(per_turn(&on_thread(&outputs, opened[0].0)).len(), 1);
    }

    /// The turns started on threads other than `child`, in order.
    fn primary_turns(outputs: &[MapperOutput], child: ThreadId) -> Vec<TurnId> {
        events(outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::TurnStarted { thread, turn } if *thread != child => Some(*turn),
                _ => None,
            })
            .collect()
    }

    /// `lines` with every occurrence of each `(from, to)` pair replaced.
    fn renamed(lines: &[String], renames: &[(String, String)]) -> Vec<String> {
        lines
            .iter()
            .map(|line| {
                renames
                    .iter()
                    .fold(line.clone(), |line, (from, to)| line.replace(from, to))
            })
            .collect()
    }

    #[test]
    fn two_runs_reported_on_one_turn_get_rows_of_their_own() {
        // `subagent-resume-awaited` with its `SendMessage` resume made twice in the second turn
        // (lines 20–26 again, under new call, run and message ids): both runs report on that
        // turn, which the server keys by `harness_item_id`.
        let lines = out_lines("subagent-resume-awaited");
        let frame = |index: usize| serde_json::from_str::<Value>(&lines[index]).unwrap();
        let call = frame(19)["message"]["content"][0]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let run = frame(21)["run_id"].as_str().unwrap().to_owned();
        let mut renames = vec![
            (call.clone(), format!("{call}2")),
            (run.clone(), format!("{run}2")),
        ];
        for index in [19, 22] {
            let id = frame(index)["message"]["id"].as_str().unwrap().to_owned();
            renames.push((id.clone(), format!("{id}2")));
        }
        let again: Vec<String> = [19, 21, 22, 23, 24, 25]
            .iter()
            .map(|index| lines[*index].clone())
            .collect();
        let mut driven: Vec<String> = lines[..26].to_vec();
        driven.extend(renamed(&again, &renames));
        driven.extend(lines[26..].iter().cloned());
        let outputs: Vec<MapperOutput> =
            drive_lines(&mut new_mapper(), &driven, 2, TurnKind::User, false)
                .into_iter()
                .flatten()
                .collect();
        let child = routes_opened(&outputs)[0].0;
        let second = primary_turns(&outputs, child)[1];
        let outcomes: Vec<&str> = events(&outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::ItemCompleted { turn, item, .. }
                    if *turn == second && item.harness_item_id.starts_with("task_updated:") =>
                {
                    Some(item.harness_item_id.as_str())
                }
                _ => None,
            })
            .collect();
        assert_eq!(outcomes.len(), 2, "{outcomes:?}");
        assert_ne!(outcomes[0], outcomes[1]);
        assert!(outcomes[0].ends_with(&run) && outcomes[1].ends_with(&format!("{run}2")));
    }

    #[test]
    #[traced_test]
    fn a_held_resume_adopted_after_its_turn_ended_reports_late_and_closes() {
        // `subagent-resume-respawn` with the `SendMessage` turn's `result` (line 21) before the
        // run's frames: the run is adopted once its turn ended. Its rows reach that turn late,
        // and its own end drops the route, which would otherwise keep the child from idling.
        let lines = out_lines("subagent-resume-respawn");
        let mut driven: Vec<String> = lines[..11].to_vec();
        driven.push(lines[20].clone());
        driven.extend(lines[11..15].iter().cloned());
        driven.extend(lines[21..].iter().cloned());
        let mut mapper = new_mapper();
        let outputs: Vec<MapperOutput> =
            drive_lines(&mut mapper, &driven, 1, TurnKind::User, false)
                .into_iter()
                .flatten()
                .collect();
        assert!(!mapper.has_routes());
        let sent_turn = primary_turns(&outputs, routes_opened(&outputs)[0].0)[0];
        let rows: Vec<&str> = events(&outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::ItemCompleted { turn, item, .. }
                    if *turn == sent_turn && item.harness_item_id.starts_with("task_") =>
                {
                    Some(item.harness_item_id.as_str())
                }
                _ => None,
            })
            .collect();
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert!(rows[0].starts_with("task_started:") && rows[1].starts_with("task_updated:"));
        // Late rows are completions alone.
        assert!(
            !started_items(&outputs)
                .iter()
                .any(|start| start.harness_item_id.starts_with("task_started:")
                    || start.harness_item_id.starts_with("task_updated:"))
        );
        logs_assert(a_line_with(&[r#"action="subagent_outcome""#, "late=true"]));
    }

    #[test]
    fn a_held_resume_that_ends_on_its_notification_opens_nothing() {
        // `subagent-resume-respawn` without the run's frames and `task_updated` (lines 12–14):
        // its notification ends the held run, and a stray frame of it afterwards opens no route.
        let lines = out_lines("subagent-resume-respawn");
        let mut driven: Vec<String> = lines[..11].to_vec();
        driven.extend(lines[14..].iter().cloned());
        driven.push(lines[11].clone());
        let outputs: Vec<MapperOutput> =
            drive_lines(&mut new_mapper(), &driven, 1, TurnKind::User, false)
                .into_iter()
                .flatten()
                .collect();
        assert!(routes_opened(&outputs).is_empty());
    }

    #[test]
    fn a_stop_of_an_earlier_run_does_not_mark_the_resumed_one() {
        // The user stops the first run of `subagent-resume-awaited` (which completes anyway);
        // the resumed run, ended by a child exit, fails rather than reading as interrupted.
        let lines = out_lines("subagent-resume-awaited");
        let mut mapper = new_mapper();
        let mut outputs: Vec<MapperOutput> =
            drive_lines(&mut mapper, &lines[..4], 2, TurnKind::User, false)
                .into_iter()
                .flatten()
                .collect();
        let child = routes_opened(&outputs)[0].0;
        mapper.note_stop_sent(child);
        outputs.extend(
            drive_lines(&mut mapper, &lines[4..24], 1, TurnKind::User, false)
                .into_iter()
                .flatten(),
        );
        outputs.extend(mapper.child_exited("exit status 1"));
        let statuses: Vec<TurnStatusKind> = turn_completions(&on_thread(&outputs, child))
            .into_iter()
            .map(|(_, _, status)| status.kind)
            .collect();
        assert_eq!(
            statuses,
            [TurnStatusKind::Completed, TurnStatusKind::Failed]
        );
    }

    fn routes_closed(outputs: &[MapperOutput]) -> Vec<ThreadId> {
        outputs
            .iter()
            .filter_map(|output| match output {
                MapperOutput::RouteClosed { thread } => Some(*thread),
                _ => None,
            })
            .collect()
    }

    /// The `Agent` tool call's completed payload on `outputs`: `(status, error, link)`.
    fn agent_completion(
        outputs: &[MapperOutput],
    ) -> (Option<String>, Option<String>, SubagentLink) {
        completed_items(outputs)
            .into_iter()
            .find_map(|item| match &item.payload {
                ItemPayload::ToolCall {
                    name,
                    status,
                    error,
                    subagent: Some(link),
                    ..
                } if name == AGENT_TOOL => Some((status.clone(), error.clone(), link.clone())),
                _ => None,
            })
            .expect("no completed Agent call with a link")
    }

    fn frame_of(line: &str) -> Value {
        serde_json::from_str(line).unwrap()
    }

    /// Drive one of the `subagent-*` fixtures: one turn, and when `stop` is set, the adapter's
    /// `note_stop_sent` for the route right after the sub-agent's ask (where the recorder sent its
    /// `stop_task`). Returns each line's outputs and the route's thread.
    fn drive_subagent(
        mapper: &mut ClaudeMapper,
        name: &str,
        stop: bool,
    ) -> (Vec<Vec<MapperOutput>>, ThreadId) {
        let mut route = None;
        let mut per_line = Vec::new();
        for (index, line) in out_lines(name).iter().enumerate() {
            let mut outputs = Vec::new();
            if index == 0 {
                outputs.extend(mapper.begin_turn(TurnId::new(), TurnKind::User));
            }
            outputs.extend(mapper.map_line(line));
            if let Some((thread, ..)) = routes_opened(&outputs).first() {
                route = Some(*thread);
            }
            if stop && frame_of(line).pointer("/request/subtype") == Some(&json!("can_use_tool")) {
                mapper.note_stop_sent(route.unwrap());
            }
            per_line.push(outputs);
        }
        (per_line, route.expect("no route was minted"))
    }

    /// A fixture line with one JSON field rewritten.
    fn rewrite(line: &str, pointer: &str, value: Value) -> String {
        let mut frame: Value = serde_json::from_str(line).unwrap();
        *frame.pointer_mut(pointer).unwrap() = value;
        frame.to_string()
    }

    fn line_of(name: &str, frame_type: &str) -> String {
        out_lines(name)
            .into_iter()
            .find(|line| serde_json::from_str::<Value>(line).unwrap()["type"] == frame_type)
            .unwrap()
    }

    #[test]
    fn a_text_turn_streams_one_agent_message_and_completes() {
        let per_line = drive(&mut new_mapper(), "text-turn", TurnKind::User);
        let outputs: Vec<MapperOutput> = per_line.iter().flatten().cloned().collect();

        let started = started_items(&outputs);
        assert_eq!(started.len(), 1, "{started:?}");
        assert_eq!(started[0].kind, ItemKind::AgentMessage);
        assert_eq!(started[0].harness_item_id, "msg_011CfaU4sfyRtL8m7Sg3g87p:1");

        let deltas: Vec<_> = events(&outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::ItemDelta { item_id, delta, .. } => Some((*item_id, delta.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            deltas,
            vec![(
                started[0].id,
                ItemDelta::Text {
                    text: "pong".into()
                }
            )]
        );

        let completed = completed_items(&outputs);
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].id, started[0].id);
        assert_eq!(
            completed[0].payload,
            ItemPayload::AgentMessage {
                text: "pong".into()
            }
        );

        // Live usage from the `message_delta` line, then once more before completion.
        let message_delta_line = out_lines("text-turn")
            .iter()
            .position(|line| line.contains(r#""type": "message_delta""#))
            .unwrap();
        assert_eq!(usage_updates(&per_line[message_delta_line]).len(), 1);
        let last_event = events(&outputs).len() - 1;
        assert!(matches!(
            events(&outputs)[last_event - 1],
            AgentEvent::TurnUsageUpdated { .. }
        ));

        let completions = turn_completions(&outputs);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Completed);
        assert_eq!(completions[0].2.message, None);
        assert_eq!(completions[0].1, TokenUsage::new(21_970, 53));
    }

    #[test]
    fn the_input_token_count_is_the_three_summand_sum() {
        let usage: UsageInfo = serde_json::from_value(json!({
            "input_tokens": 10,
            "cache_creation_input_tokens": 7149,
            "cache_read_input_tokens": 14811,
            "output_tokens": 53
        }))
        .unwrap();
        assert_eq!(token_usage(&usage), TokenUsage::new(10 + 7149 + 14811, 53));
    }

    #[test]
    fn an_allowed_bash_call_is_a_command_item_that_completes() {
        let outputs = run_fixture("tool-allowed", TurnKind::User);

        let commands: Vec<_> = started_items(&outputs)
            .into_iter()
            .filter_map(|item| item.command.as_ref())
            .collect();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command, "touch probe.txt");
        assert_eq!(
            commands[0].description.as_deref(),
            Some("Create an empty file named probe.txt")
        );
        assert_eq!(commands[0].cwd, WORKSPACE);
        assert_eq!(commands[0].status.as_deref(), Some("in_progress"));
        let completions = command_completions(&outputs);
        assert_eq!(completions.len(), 1);
        assert!(matches!(
            &completions[0].1.payload,
            ItemPayload::CommandExecution { description: Some(description), .. }
                if description == "Create an empty file named probe.txt"
        ));

        let approvals: Vec<_> = events(&outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::ApprovalRequested { request, .. } => Some(request),
                _ => None,
            })
            .collect();
        assert_eq!(approvals.len(), 1);
        assert_eq!(
            approvals[0].kind,
            ApprovalKind::CommandExecution {
                command: "touch probe.txt".into(),
                cwd: WORKSPACE.into()
            }
        );
        assert_eq!(approvals[0].available.len(), 4);

        let pending: Vec<_> = outputs
            .iter()
            .filter_map(|output| match output {
                MapperOutput::PendingApproval {
                    id,
                    request_id,
                    tool_use_id,
                    tool_name,
                    suggestions,
                    ..
                } => Some((id, request_id, tool_use_id, tool_name, suggestions)),
                _ => None,
            })
            .collect();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].1, "e8ab465d-f0b9-426b-be10-95604d7e4154");
        assert_eq!(pending[0].0, &approvals[0].id);
        assert_eq!(
            pending[0].2.as_deref(),
            Some("toolu_01FZdtNSm7HPbRG2vZ6fknrF")
        );
        assert_eq!(pending[0].3, "Bash");
        // The raw suggestions, `directories` and the `localSettings` destination included.
        let suggestions = pending[0].4;
        assert_eq!(suggestions.len(), 3);
        assert_eq!(suggestions[0]["destination"], "localSettings");
        assert_eq!(suggestions[1]["directories"], json!(["/work/project"]));

        assert_eq!(
            command_statuses(&outputs),
            vec![("touch probe.txt".into(), Some("completed".into()))]
        );
    }

    #[test]
    fn generic_tools_get_a_title_from_their_main_argument() {
        let root = std::path::Path::new(WORKSPACE);
        let title = |name: &str, input: Value| tool_description(name, &input, root);
        assert_eq!(
            title("Read", json!({"file_path": "/work/project/src/main.rs"})).as_deref(),
            Some("Read src/main.rs")
        );
        assert_eq!(
            title("Read", json!({"file_path": "/etc/hosts"})).as_deref(),
            Some("Read /etc/hosts")
        );
        assert_eq!(
            title(
                "Grep",
                json!({"pattern": "fn main", "path": "/work/project/src"})
            )
            .as_deref(),
            Some(r#"Grep "fn main" in src"#)
        );
        assert_eq!(
            title("Grep", json!({"pattern": "todo", "glob": "*.rs"})).as_deref(),
            Some(r#"Grep "todo" in *.rs"#)
        );
        assert_eq!(
            title(
                "Grep",
                json!({"pattern": "todo", "path": "/work/project/src", "glob": "*.rs"})
            )
            .as_deref(),
            Some(r#"Grep "todo" in src (*.rs)"#)
        );
        assert_eq!(
            title(
                "Glob",
                json!({"pattern": "**/*.toml", "path": "/work/project"})
            )
            .as_deref(),
            Some("Glob **/*.toml in .")
        );
        assert_eq!(
            title("WebFetch", json!({"url": "https://docs.rs", "prompt": "x"})).as_deref(),
            Some("Fetch https://docs.rs")
        );
        assert_eq!(
            title("WebSearch", json!({"query": "tokio 1.54"})).as_deref(),
            Some(r#"Web search "tokio 1.54""#)
        );
        assert_eq!(
            title(
                "Monitor",
                json!({"description": "CI run", "command": "gh run watch"})
            )
            .as_deref(),
            Some("CI run")
        );
        assert_eq!(
            title(
                "Agent",
                json!({"description": "reader", "prompt": "Read it"})
            )
            .as_deref(),
            Some("reader")
        );
        assert_eq!(
            title("Skill", json!({"skill": "init"})).as_deref(),
            Some("Skill init")
        );
        // No main argument, an unknown tool, or an MCP tool: the tool name stays the title.
        assert_eq!(title("Read", json!({})), None);
        assert_eq!(title("Monitor", json!({"description": " "})), None);
        assert_eq!(title("TodoWrite", json!({"todos": []})), None);
        assert_eq!(title("mcp__brave__search", json!({"query": "q"})), None);
    }

    #[test]
    fn a_blank_bash_description_is_dropped() {
        let root = std::path::Path::new(WORKSPACE);
        let input = json!({"command": "ls", "description": "  "});
        assert_eq!(tool_description("Bash", &input, root), None);
        let input = json!({"command": "ls", "description": " List files "});
        assert_eq!(
            tool_description("Bash", &input, root).as_deref(),
            Some("List files")
        );
    }

    #[test]
    fn a_denied_bash_call_completes_declined_not_failed() {
        let outputs = run_fixture("tool-denied", TurnKind::User);
        assert_eq!(
            command_statuses(&outputs),
            vec![("touch probe.txt".into(), Some("declined".into()))]
        );
        let completions = turn_completions(&outputs);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Completed);
    }

    #[test]
    fn a_cancelled_turn_is_interrupted_when_the_adapter_sent_the_interrupt() {
        let outputs = run_fixture("cancel", TurnKind::User);
        let completions = turn_completions(&outputs);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Interrupted);
        assert_eq!(completions[0].2.message, None);

        // The recorded result also says `terminal_reason: "aborted_tools"`, which marks an
        // interruption on its own. With neither that nor the adapter's note, the same
        // error-shaped result is a failure that carries the CLI's errors.
        let lines: Vec<String> = out_lines("cancel")
            .iter()
            .map(|line| {
                if line.contains(r#""terminal_reason""#) {
                    rewrite(line, "/terminal_reason", Value::Null)
                } else {
                    line.clone()
                }
            })
            .collect();
        let outputs: Vec<MapperOutput> =
            drive_lines(&mut new_mapper(), &lines, 1, TurnKind::User, false)
                .into_iter()
                .flatten()
                .collect();
        let completions = turn_completions(&outputs);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Failed);
        assert!(
            completions[0]
                .2
                .message
                .as_deref()
                .is_some_and(|message| message.contains("ede_diagnostic"))
        );
    }

    #[test]
    fn an_aborted_terminal_reason_is_an_interruption_without_the_adapter_note() {
        let outputs: Vec<MapperOutput> = drive_lines(
            &mut new_mapper(),
            &out_lines("cancel"),
            1,
            TurnKind::User,
            false,
        )
        .into_iter()
        .flatten()
        .collect();
        assert_eq!(
            turn_completions(&outputs)[0].2.kind,
            TurnStatusKind::Interrupted
        );
    }

    #[test]
    fn an_activity_item_marks_the_interruption_text() {
        let outputs = run_fixture("cancel", TurnKind::User);
        let activities: Vec<_> = completed_items(&outputs)
            .into_iter()
            .filter_map(|item| match &item.payload {
                ItemPayload::Activity { title, .. } => Some(title.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            activities,
            vec!["[Request interrupted by user for tool use]"]
        );
        assert_eq!(
            command_statuses(&outputs),
            vec![("touch probe.txt".into(), Some("declined".into()))]
        );
    }

    #[test]
    fn a_foreground_delegation_is_one_turn() {
        let lines = out_lines("delegation");
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        let per_line = drive(&mut mapper, "delegation", TurnKind::User);
        let outputs: Vec<MapperOutput> = on_thread(&per_line.concat(), primary);

        let completions = turn_completions(&outputs);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Completed);
        let result_line = lines
            .iter()
            .position(|line| line.contains(r#""type": "result""#))
            .unwrap();
        assert_eq!(
            turn_completions(&on_thread(&per_line[result_line], primary)).len(),
            1
        );

        let agent: Vec<_> = completed_items(&outputs)
            .into_iter()
            .filter_map(|item| match &item.payload {
                ItemPayload::ToolCall { name, status, .. } if name == "Agent" => {
                    Some(status.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(agent, vec![Some("completed".to_owned())]);

        // The child's `Read` is not the main agent's.
        assert!(
            started_items(&outputs)
                .iter()
                .all(|item| { item.tool.as_ref().is_none_or(|tool| tool.name != "Read") })
        );
    }

    #[test]
    #[traced_test]
    fn a_backgrounded_delegation_ends_its_turn_at_the_first_result() {
        // The recorder's interrupt came after the first `result`, with no turn left to note it on.
        let lines = out_lines("delegation-interrupted");
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        let per_line = drive_lines(&mut mapper, &lines, 1, TurnKind::User, false);
        let result_line = position(&lines, |frame| frame["type"] == "result");
        let completions = turn_completions(&on_thread(&per_line[result_line], primary));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Completed);
        // The route outlives the turn: the sub-agent's frames still land on it.
        assert!(routes_closed(&per_line[result_line]).is_empty());
        logs_assert(a_line_with(&[" INFO ", r#"action="route_outlives_turn""#]));
        let (child, ..) = routes_opened(&per_line.concat())[0].clone();
        let bash = position(&lines, |frame| {
            frame["parent_tool_use_id"].is_string()
                && frame.pointer("/message/content/0/name") == Some(&json!("Bash"))
        });
        assert!(bash > result_line);
        assert!(
            started_items(&on_thread(&per_line[bash], child))
                .iter()
                .any(|item| item.command.is_some())
        );

        // The kill ends the route and drops it, its spawning turn long over.
        let killed = position(&lines, |frame| frame["patch"]["status"] == "killed");
        assert_eq!(routes_closed(&per_line[killed]), vec![child]);
        assert!(turn_completions(&on_thread(&per_line[killed], primary)).is_empty());
        assert_eq!(
            turn_completions(&on_thread(&per_line.concat(), primary)).len(),
            1
        );
        assert!(!mapper.has_routes());
        logs_assert(no_line_with(r#"action="hold_result""#));
    }

    #[test]
    fn a_background_agent_killed_without_a_stop_fails_its_route() {
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        let outputs: Vec<MapperOutput> = drive_lines(
            &mut mapper,
            &out_lines("delegation-interrupted"),
            1,
            TurnKind::User,
            false,
        )
        .into_iter()
        .flatten()
        .collect();
        let (child, ..) = routes_opened(&outputs)[0].clone();
        let completions = turn_completions(&on_thread(&outputs, child));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Failed);
        assert!(
            completions[0]
                .2
                .message
                .as_deref()
                .is_some_and(|message| message.contains("killed"))
        );
        let primary = turn_completions(&on_thread(&outputs, primary));
        assert_eq!(primary.len(), 1);
        assert_eq!(primary[0].2.kind, TurnStatusKind::Completed);
    }

    #[test]
    fn the_closing_usage_is_the_last_request_and_the_completion_is_the_sum() {
        let outputs = run_fixture("tool-denied", TurnKind::User);
        let (closing, _) = *usage_updates(&outputs).last().unwrap();
        assert_eq!(closing, TokenUsage::new(22_191, 75));
        let completions = turn_completions(&outputs);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].1, TokenUsage::new(44_178, 222));
    }

    #[test]
    fn a_result_without_iterations_reports_its_summed_usage() {
        let compact = out_lines("compact");
        let degenerate = compact.last().unwrap();
        let frame: Value = serde_json::from_str(degenerate).unwrap();
        assert!(
            frame
                .pointer("/usage/iterations")
                .and_then(Value::as_array)
                .is_none_or(Vec::is_empty)
        );
        let usage: UsageInfo = serde_json::from_value(frame["usage"].clone()).unwrap();
        assert_eq!(last_request_usage(&usage), token_usage(&usage));
    }

    #[test]
    #[traced_test]
    fn blank_lines_are_skipped_without_a_warning() {
        let mut mapper = new_mapper();
        let mut outputs = Vec::new();
        outputs.extend(mapper.map_line(""));
        outputs.extend(mapper.map_line("  \r"));
        assert!(outputs.is_empty());
        // Nothing at `WARN` or above.
        logs_assert(lines_with(0, &[" WARN "]));
        logs_assert(lines_with(0, &[" ERROR "]));
    }

    #[test]
    #[traced_test]
    fn a_background_shell_task_never_gates_the_turn() {
        let lines = out_lines("background-bash");
        let per_line = drive(&mut new_mapper(), "background-bash", TurnKind::User);
        let results: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter(|(_, line)| line.contains(r#""type": "result""#))
            .map(|(index, _)| index)
            .collect();
        assert_eq!(results.len(), 2);

        let first = turn_completions(&per_line[results[0]]);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].2.kind, TurnStatusKind::Completed);
        // Both the running completion and the terminal one keep the `Bash` description.
        let all: Vec<MapperOutput> = per_line.iter().flatten().cloned().collect();
        let descriptions: Vec<_> = command_completions(&all)
            .into_iter()
            .map(|(_, item)| match &item.payload {
                ItemPayload::CommandExecution { description, .. } => description.clone(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(
            descriptions,
            vec![Some("Background sleep command".to_owned()); 2]
        );

        let outputs: Vec<MapperOutput> = per_line[results[0] + 1..]
            .iter()
            .flatten()
            .cloned()
            .collect();
        let started: Vec<TurnId> = events(&outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::TurnStarted { turn, .. } => Some(*turn),
                _ => None,
            })
            .collect();
        assert_eq!(started.len(), 1);
        assert_ne!(started[0], first[0].0);
        let second = turn_completions(&per_line[results[1]]);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].0, started[0]);
        // The command's terminal completion belongs to the first turn, not the continuation.
        let continuation: Vec<MapperOutput> = outputs
            .iter()
            .filter(|output| {
                !matches!(output, MapperOutput::Event(AgentEvent::ItemCompleted { turn, .. })
                    if *turn == first[0].0)
            })
            .cloned()
            .collect();
        assert_eq!(
            completed_items(&continuation)
                .iter()
                .map(|item| item.payload.clone())
                .collect::<Vec<_>>(),
            vec![ItemPayload::AgentMessage {
                text: "Background task completed.".into()
            }]
        );
        logs_assert(a_line_with(&[" INFO ", r#"action="external_turn""#]));
    }

    #[test]
    fn compaction_emits_an_activity_and_no_agent_message() {
        let outputs = run_fixture("compact", TurnKind::Compaction);
        let turns: Vec<TurnId> = events(&outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::TurnStarted { turn, .. } => Some(*turn),
                _ => None,
            })
            .collect();
        assert_eq!(turns.len(), 2);
        let compaction = turns[1];

        let items: Vec<&Item> = events(&outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::ItemCompleted { turn, item, .. } if *turn == compaction => Some(item),
                _ => None,
            })
            .collect();
        assert_eq!(items.len(), 1, "{items:?}");
        let ItemPayload::Activity {
            title,
            detail,
            metadata,
            ..
        } = &items[0].payload
        else {
            panic!("not an activity: {:?}", items[0]);
        };
        assert_eq!(title, "Context compacted");
        let detail = detail.as_deref().unwrap();
        assert!(
            detail.contains("22017") && detail.contains("1262"),
            "{detail}"
        );
        assert_eq!(metadata.as_ref().unwrap()["pre_tokens"], 22017);
        assert!(items[0].harness_item_id.starts_with("compact_boundary:"));

        let completions = turn_completions(&outputs);
        assert_eq!(completions.len(), 2);
        assert_eq!(completions[1].0, compaction);
        assert_eq!(completions[1].2.kind, TurnStatusKind::Completed);
    }

    /// Each turn's events, split at `TurnStarted`.
    fn per_turn(outputs: &[MapperOutput]) -> Vec<Vec<&AgentEvent>> {
        let mut turns: Vec<Vec<&AgentEvent>> = Vec::new();
        for event in events(outputs) {
            if matches!(event, AgentEvent::TurnStarted { .. }) {
                turns.push(Vec::new());
            }
            if let Some(turn) = turns.last_mut() {
                turn.push(event);
            }
        }
        turns
    }

    fn user_messages<'a>(events: &[&'a AgentEvent]) -> Vec<&'a Item> {
        events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::ItemCompleted { item, .. }
                    if matches!(item.payload, ItemPayload::UserMessage { .. }) =>
                {
                    Some(item)
                }
                _ => None,
            })
            .collect()
    }

    fn sent_texts(name: &str) -> Vec<String> {
        in_frames(name)
            .iter()
            .filter(|frame| frame["type"] == "user")
            .map(|frame| {
                frame["message"]["content"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|block| block["type"] == "text")
                    .map(|block| block["text"].as_str().unwrap())
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .collect()
    }

    #[test]
    #[traced_test]
    fn a_compaction_replay_is_not_a_user_message() {
        let outputs: Vec<MapperOutput> =
            drive_noting(&mut new_mapper(), "replay-compact", TurnKind::Compaction)
                .into_iter()
                .flatten()
                .collect();
        let turns = per_turn(&outputs);
        assert_eq!(turns.len(), 2);
        assert_eq!(user_messages(&turns[0]).len(), 1);
        let compaction: Vec<&Item> = turns[1]
            .iter()
            .filter_map(|event| match event {
                AgentEvent::ItemCompleted { item, .. } => Some(item),
                _ => None,
            })
            .collect();
        assert_eq!(compaction.len(), 1, "{compaction:?}");
        assert!(matches!(
            &compaction[0].payload,
            ItemPayload::Activity { title, .. } if title == "Context compacted"
        ));
        // The first turn's prompt and the `<local-command-stdout>` of `/compact` (the recording
        // was made with `--replay-user-messages`).
        logs_assert(lines_with(2, &[" DEBUG ", r#"action="replay_ignored""#]));
    }

    /// `drive`, noting each user turn's prompt (the text the recorder wrote) as the supervisor
    /// does with `note_prompt` right after `begin_turn`.
    fn drive_noting(
        mapper: &mut ClaudeMapper,
        name: &str,
        kind: TurnKind,
    ) -> Vec<Vec<MapperOutput>> {
        let mut prompts = sent_texts(name).into_iter();
        let mut left = prompts.len();
        out_lines(name)
            .iter()
            .map(|line| {
                let mut outputs = Vec::new();
                if mapper.active_turn().is_none() && left > 0 {
                    left -= 1;
                    let turn_kind = if left == 0 { kind } else { TurnKind::User };
                    outputs.extend(mapper.begin_turn(TurnId::new(), turn_kind));
                    if let Some(prompt) = prompts.next() {
                        mapper.note_prompt(prompt);
                    }
                }
                outputs.extend(mapper.map_line(line));
                outputs
            })
            .collect()
    }

    fn is_user_message_start(event: &AgentEvent) -> bool {
        matches!(event, AgentEvent::ItemStarted { item, .. } if item.kind == ItemKind::UserMessage)
    }

    #[test]
    #[traced_test]
    fn a_turn_is_acknowledged_at_init() {
        let mut mapper = new_mapper();
        let per_line = drive_noting(&mut mapper, "text-turn", TurnKind::User);
        let lines = out_lines("text-turn");
        let init = line_index(&lines, r#""subtype": "init""#);
        // Emitted by the `init` line itself.
        assert_eq!(user_messages(&events(&per_line[init])).len(), 1);
        let outputs: Vec<MapperOutput> = per_line.into_iter().flatten().collect();
        let all = events(&outputs);
        let messages = user_messages(&all);
        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].payload,
            ItemPayload::UserMessage {
                text: sent_texts("text-turn").remove(0)
            }
        );
        let started = all
            .iter()
            .position(|event| is_user_message_start(event))
            .unwrap();
        let answer = all
            .iter()
            .position(|event| {
                matches!(event, AgentEvent::ItemStarted { item, .. }
                    if item.kind == ItemKind::AgentMessage)
            })
            .unwrap();
        assert!(started < answer);
        logs_assert(lines_with(
            1,
            &[
                " INFO ",
                r#"action="prompt_acknowledged""#,
                r#"trigger="init""#,
            ],
        ));
        logs_assert(no_line_with(" WARN "));
    }

    #[test]
    fn a_compaction_turn_is_not_acknowledged() {
        let outputs: Vec<MapperOutput> =
            drive_noting(&mut new_mapper(), "compact", TurnKind::Compaction)
                .into_iter()
                .flatten()
                .collect();
        let turns = per_turn(&outputs);
        assert_eq!(turns.len(), 2);
        assert_eq!(user_messages(&turns[0]).len(), 1);
        assert!(user_messages(&turns[1]).is_empty());
    }

    #[test]
    fn a_mid_turn_init_acknowledges_once() {
        // A re-emitted `init` inside the turn (as after an auto-compaction).
        let mut lines = out_lines("text-turn");
        let init = line_index(&lines, r#""subtype": "init""#);
        lines.insert(init + 3, lines[init].clone());
        let mut mapper = new_mapper();
        let mut outputs = mapper.begin_turn(TurnId::new(), TurnKind::User);
        mapper.note_prompt("pong?".into());
        for line in &lines {
            outputs.extend(mapper.map_line(line));
        }
        assert_eq!(user_messages(&events(&outputs)).len(), 1);
    }

    #[test]
    fn an_init_outside_a_turn_acknowledges_nothing() {
        let lines = out_lines("text-turn");
        let init = &lines[line_index(&lines, r#""subtype": "init""#)];
        let mut mapper = new_mapper();
        // No turn: nothing to note, nothing to acknowledge.
        mapper.note_prompt("pong?".into());
        assert!(user_messages(&events(&mapper.map_line(init))).is_empty());
        // A turn whose prompt was acknowledged, then an `init` of the CLI's own continuation.
        let mut outputs = mapper.begin_turn(TurnId::new(), TurnKind::User);
        mapper.note_prompt("pong?".into());
        for line in &lines {
            outputs.extend(mapper.map_line(line));
        }
        assert_eq!(user_messages(&events(&outputs)).len(), 1);
        assert!(mapper.active_turn().is_none());
        assert!(user_messages(&events(&mapper.map_line(init))).is_empty());
    }

    #[test]
    #[traced_test]
    fn the_backstop_acknowledges_when_init_is_missing() {
        let mut lines = out_lines("text-turn");
        lines.remove(line_index(&lines, r#""subtype": "init""#));
        let answer = line_index(&lines, r#""type": "stream_event""#);
        let mut mapper = new_mapper();
        let mut outputs = mapper.begin_turn(TurnId::new(), TurnKind::User);
        mapper.note_prompt("pong?".into());
        for line in &lines[..answer] {
            outputs.extend(mapper.map_line(line));
        }
        assert!(
            user_messages(&events(&outputs)).is_empty(),
            "nothing before the answer"
        );
        let at = mapper.map_line(&lines[answer]);
        let messages: Vec<ItemPayload> = user_messages(&events(&at))
            .into_iter()
            .map(|item| item.payload.clone())
            .collect();
        assert_eq!(
            messages,
            [ItemPayload::UserMessage {
                text: "pong?".into()
            }]
        );
        logs_assert(lines_with(
            1,
            &[
                " WARN ",
                r#"action="prompt_acknowledged""#,
                r#"trigger="stream_event""#,
                "no system/init preceded the answer",
            ],
        ));
    }

    #[test]
    #[traced_test]
    fn replayed_frames_are_ignored() {
        // Recorded with `--replay-user-messages`: the prompts' own echoes, a prompt the CLI did
        // not echo, and a stopped task's queued `<task-notification>` replayed with an `origin`
        // before the next prompt.
        for name in ["background-stop-next-turn", "prompt-not-replayed"] {
            let outputs: Vec<MapperOutput> = drive_noting(&mut new_mapper(), name, TurnKind::User)
                .into_iter()
                .flatten()
                .collect();
            let sent = sent_texts(name);
            let turns = per_turn(&outputs);
            assert_eq!(turns.len(), sent.len(), "{name}");
            for (turn, sent) in turns.iter().zip(&sent) {
                let messages = user_messages(turn);
                assert_eq!(messages.len(), 1, "{name}: {turn:?}");
                assert_eq!(
                    messages[0].payload,
                    ItemPayload::UserMessage { text: sent.clone() }
                );
            }
        }
        logs_assert(lines_with(
            1,
            &[
                " DEBUG ",
                r#"action="replay_ignored""#,
                "origin=task-notification",
            ],
        ));
        // The backstop never fired: every prompt was acknowledged at its `init`.
        logs_assert(lines_with(
            0,
            &[" WARN ", r#"action="prompt_acknowledged""#],
        ));
    }

    /// `name`'s stdout lines with the recorded output file (in the notification's `output_file`
    /// and the `tool_result` text alike) replaced by a temp file holding `content`.
    fn with_output_file(name: &str, content: &str) -> (Vec<String>, tempfile::TempDir) {
        let lines = out_lines(name);
        let recorded = lines
            .iter()
            .find_map(|line| {
                let frame: Value = serde_json::from_str(line).unwrap();
                frame["output_file"]
                    .as_str()
                    .filter(|path| !path.is_empty())
                    .map(str::to_owned)
            })
            .unwrap();
        // Named `<task_id>.output`, as the CLI names it.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join(Path::new(&recorded).file_name().unwrap());
        std::fs::write(&file, content).unwrap();
        let path = file.display().to_string();
        let lines = lines
            .into_iter()
            .map(|line| line.replace(&recorded, &path))
            .collect();
        (lines, dir)
    }

    /// Every `CommandExecution` completion: `(turn, item)`.
    fn command_completions(outputs: &[MapperOutput]) -> Vec<(TurnId, &Item)> {
        events(outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::ItemCompleted { turn, item, .. }
                    if matches!(item.payload, ItemPayload::CommandExecution { .. }) =>
                {
                    Some((*turn, item))
                }
                _ => None,
            })
            .collect()
    }

    fn command_fields(item: &Item) -> (&str, Option<&str>, Option<i32>, Option<&str>) {
        let ItemPayload::CommandExecution {
            output,
            status,
            exit_code,
            process_id,
            ..
        } = &item.payload
        else {
            panic!("not a command: {item:?}");
        };
        (
            output.as_str(),
            status.as_deref(),
            *exit_code,
            process_id.as_deref(),
        )
    }

    fn line_index(lines: &[String], needle: &str) -> usize {
        lines.iter().position(|line| line.contains(needle)).unwrap()
    }

    #[test]
    #[traced_test]
    fn a_background_command_is_a_running_task() {
        let (lines, _file) = with_output_file(
            "background-complete",
            "line 1\nline 2\nfinished\n\n[exited with code 0]\n",
        );
        let result = line_index(&lines, r#""type": "result""#);
        let mut mapper = new_mapper();
        let first: Vec<MapperOutput> =
            drive_lines(&mut mapper, &lines[..=result], 1, TurnKind::User, false)
                .into_iter()
                .flatten()
                .collect();
        let running = command_completions(&first);
        assert_eq!(running.len(), 1);
        let (turn, item) = running[0];
        assert_eq!(
            command_fields(item),
            ("", Some("in_progress"), None, Some("bx9de5w1u"))
        );
        let completions = turn_completions(&first);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].0, turn);
        assert!(mapper.has_tasks(), "the command outlives its turn");
        assert_eq!(
            mapper
                .background_command("bx9de5w1u")
                .map(|(_, command)| command),
            Some("for i in 1 2 3 4 5 6; do echo line $i; sleep 5; done; echo finished")
        );

        let rest: Vec<MapperOutput> =
            drive_lines(&mut mapper, &lines[result + 1..], 0, TurnKind::User, false)
                .into_iter()
                .flatten()
                .collect();
        let ended = command_completions(&rest);
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0].0, turn, "completed on its original turn");
        assert_eq!(ended[0].1.id, item.id);
        assert_eq!(ended[0].1.harness_item_id, item.harness_item_id);
        assert_eq!(
            command_fields(ended[0].1),
            (
                "line 1\nline 2\nfinished",
                Some("completed"),
                Some(0),
                Some("bx9de5w1u")
            )
        );
        assert!(!mapper.has_tasks());
        assert!(mapper.background_command("bx9de5w1u").is_none());
        // The CLI's continuation is a turn of its own.
        let continuation = turn_completions(&rest);
        assert_eq!(continuation.len(), 1);
        assert_ne!(continuation[0].0, turn);
        logs_assert(a_line_with(&[" INFO ", r#"action="background_command""#]));
        logs_assert(a_line_with(&[
            " INFO ",
            r#"action="task_notification""#,
            "exit_code=0",
        ]));
        logs_assert(no_line_with(" WARN "));
    }

    #[test]
    fn a_stopped_background_command_is_terminated() {
        let (lines, _file) = with_output_file("background-stop", "line 1\nline 2\n\n[killed]\n");
        let outputs: Vec<MapperOutput> =
            drive_lines(&mut new_mapper(), &lines, 1, TurnKind::User, false)
                .into_iter()
                .flatten()
                .collect();
        let commands = command_completions(&outputs);
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].0, commands[1].0);
        assert_eq!(
            command_fields(commands[1].1),
            (
                "line 1\nline 2",
                Some("terminated"),
                None,
                Some("b93m9v2sw")
            )
        );
    }

    #[test]
    #[traced_test]
    fn a_background_command_dies_with_the_child() {
        // The output written so far, and no marker: the CLI went before the command ended.
        let (lines, _file) = with_output_file("background-complete", "line 1\nline 2\n");
        let result = line_index(&lines, r#""type": "result""#);
        let mut mapper = new_mapper();
        drive_lines(&mut mapper, &lines[..=result], 1, TurnKind::User, false);
        let outputs = mapper.child_exited("code 1");
        let commands = command_completions(&outputs);
        assert_eq!(commands.len(), 1);
        assert_eq!(
            command_fields(commands[0].1),
            (
                "line 1\nline 2\n",
                Some("terminated"),
                None,
                Some("bx9de5w1u")
            )
        );
        assert!(!mapper.has_tasks());
        logs_assert(lines_with(
            1,
            &[" WARN ", r#"action="background_command_lost""#, "bx9de5w1u"],
        ));
    }

    #[test]
    #[traced_test]
    fn a_notification_without_a_terminal_update_still_completes() {
        let (mut lines, _file) =
            with_output_file("background-complete", "finished\n\n[exited with code 3]\n");
        let update = line_index(&lines, r#""subtype": "task_updated""#);
        lines.remove(update);
        let outputs: Vec<MapperOutput> =
            drive_lines(&mut new_mapper(), &lines, 1, TurnKind::User, false)
                .into_iter()
                .flatten()
                .collect();
        let commands = command_completions(&outputs);
        assert_eq!(commands.len(), 2);
        assert_eq!(
            command_fields(commands[1].1),
            ("finished", Some("completed"), Some(3), Some("bx9de5w1u"))
        );
        logs_assert(lines_with(
            1,
            &[
                " WARN ",
                r#"action="task_notification""#,
                "terminal update was not seen",
            ],
        ));
    }

    #[test]
    #[traced_test]
    fn a_command_the_model_stops_with_task_stop_is_terminated() {
        let (lines, _file) = with_output_file(
            "background-taskstop",
            "line 1\nline 2\nline 3\n\n[killed]\n",
        );
        let mut mapper = new_mapper();
        let outputs: Vec<MapperOutput> = drive_lines(&mut mapper, &lines, 1, TurnKind::User, false)
            .into_iter()
            .flatten()
            .collect();
        let commands = command_completions(&outputs);
        let fields: Vec<_> = commands
            .iter()
            .map(|(_, item)| (item.harness_item_id.as_str(), command_fields(item)))
            .collect();
        assert_eq!(
            fields,
            [
                (
                    "toolu_01DWg4fX7gJ3GQKXfaXfsQ8s",
                    ("", Some("in_progress"), None, Some("bjo3ocmlf"))
                ),
                // The foreground `sleep 7` has a task of its own, and completes once, on its
                // `tool_result`, not as a background command.
                (
                    "toolu_01RQLtPCZdih1pzVLsPm6tDR",
                    ("", Some("completed"), None, None)
                ),
                (
                    "toolu_01DWg4fX7gJ3GQKXfaXfsQ8s",
                    (
                        "line 1\nline 2\nline 3",
                        Some("terminated"),
                        None,
                        Some("bjo3ocmlf")
                    )
                ),
            ]
        );
        // The foreground task ended with its notification: nothing keeps the child busy.
        assert!(!mapper.has_tasks());
        logs_assert(a_line_with(&[
            " DEBUG ",
            r#"action="task_notification""#,
            "a foreground command's task ended",
        ]));
        logs_assert(no_line_with(" WARN "));
    }

    #[test]
    fn a_failed_background_command_reports_its_exit_code() {
        let (lines, _file) = with_output_file("background-fail", "oops\n\n[exited with code 3]\n");
        let mut mapper = new_mapper();
        let outputs: Vec<MapperOutput> = drive_lines(&mut mapper, &lines, 1, TurnKind::User, false)
            .into_iter()
            .flatten()
            .collect();
        let commands = command_completions(&outputs);
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].0, commands[1].0);
        assert_eq!(
            command_fields(commands[1].1),
            ("oops", Some("failed"), Some(3), Some("bt4hdmilh"))
        );
        assert!(!mapper.has_tasks());
    }

    #[test]
    #[traced_test]
    fn a_result_never_settles_a_command_awaiting_its_notification() {
        // The notification arrives after the continuation's `result`: that result must not
        // settle the command early, or the real notification would be dropped.
        let (mut lines, _file) =
            with_output_file("background-complete", "finished\n\n[exited with code 0]\n");
        let notification = line_index(&lines, r#""subtype": "task_notification""#);
        let late = lines.remove(notification);
        let continuation = lines
            .iter()
            .rposition(|line| line.contains(r#""type": "result""#))
            .unwrap();
        let mut mapper = new_mapper();
        let before: Vec<MapperOutput> = drive_lines(
            &mut mapper,
            &lines[..=continuation],
            1,
            TurnKind::User,
            false,
        )
        .into_iter()
        .flatten()
        .collect();
        let commands = command_completions(&before);
        assert_eq!(commands.len(), 1, "still running after the result");
        assert!(mapper.awaiting_notification());
        let ended = mapper.map_line(&late);
        let completed = command_completions(&ended);
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].0, commands[0].0);
        assert_eq!(
            command_fields(completed[0].1),
            ("finished", Some("completed"), Some(0), Some("bx9de5w1u"))
        );
        assert!(!mapper.has_tasks());
        logs_assert(no_line_with("no notification followed"));
    }

    #[test]
    fn settling_completes_a_command_whose_notification_never_came() {
        let (mut lines, _file) = with_output_file("background-stop", "line 1\n\n[killed]\n");
        let notification = line_index(&lines, r#""subtype": "task_notification""#);
        lines.truncate(notification);
        let mut mapper = new_mapper();
        drive_lines(&mut mapper, &lines, 1, TurnKind::User, false);
        assert!(mapper.has_tasks());
        assert!(mapper.awaiting_notification());
        let outputs = mapper.settle_background_commands();
        let commands = command_completions(&outputs);
        assert_eq!(commands.len(), 1);
        assert_eq!(
            command_fields(commands[0].1),
            ("line 1", Some("terminated"), None, Some("b93m9v2sw"))
        );
        assert!(!mapper.has_tasks());
        assert!(mapper.settle_background_commands().is_empty());
    }

    #[test]
    fn a_foreground_call_backgrounded_while_it_ran_is_adopted() {
        // `task_started` says foreground; the `tool_result` still names the task.
        let (lines, _file) = with_output_file("background-stop", "line 1\n\n[killed]\n");
        let lines: Vec<String> = lines
            .into_iter()
            .map(|line| line.replace(r#""is_backgrounded": true"#, r#""is_backgrounded": false"#))
            .collect();
        let outputs: Vec<MapperOutput> =
            drive_lines(&mut new_mapper(), &lines, 1, TurnKind::User, false)
                .into_iter()
                .flatten()
                .collect();
        let commands = command_completions(&outputs);
        assert_eq!(commands.len(), 2);
        assert_eq!(
            command_fields(commands[0].1),
            ("", Some("in_progress"), None, Some("b93m9v2sw"))
        );
        assert_eq!(
            command_fields(commands[1].1),
            ("line 1", Some("terminated"), None, Some("b93m9v2sw"))
        );
    }

    #[test]
    fn the_output_file_is_read_from_the_tool_result_text() {
        assert_eq!(
            background_output_path(
                "Command running in background with ID: b1. Output is being written to: \
                 /tmp/x.output dir/-work-project/s 1/tasks/b1.output. You will be notified.",
                "b1"
            ),
            Some(PathBuf::from(
                "/tmp/x.output dir/-work-project/s 1/tasks/b1.output"
            ))
        );
        assert_eq!(
            background_output_path("the magic number is 4271", "b1"),
            None
        );
        assert_eq!(
            background_output_path("Output is being written to: /tmp/b2.output", "b1"),
            None
        );
    }

    #[test]
    fn output_file_markers_are_parsed() {
        let dir = tempfile::tempdir().unwrap();
        let write = |name: &str, content: &[u8]| {
            let path = dir.path().join(name);
            std::fs::write(&path, content).unwrap();
            path
        };
        let read = |path: &Path| background_output(path).unwrap();

        let done = write(
            "done",
            b"line 1 \xe2\x80\xa6 finished\n\n[exited with code 0]\n",
        );
        assert_eq!(
            read(&done),
            BackgroundOutput {
                text: "line 1 … finished".into(),
                exit_code: Some(0),
                original_bytes: None,
            }
        );
        let failed = write("failed", b"oops\n\n[exited with code 127]\n");
        assert_eq!(read(&failed).exit_code, Some(127));
        let killed = write("killed", b"line 1\nline 2\n\n[killed]\n");
        assert_eq!(
            read(&killed),
            BackgroundOutput {
                text: "line 1\nline 2".into(),
                exit_code: None,
                original_bytes: None,
            }
        );
        let silent = write("silent", b"\n[exited with code 0]\n");
        assert_eq!(read(&silent).text, "");
        // An unknown last line is output, not a marker.
        let running = write("running", b"line 1\nline 2\n");
        assert_eq!(
            read(&running),
            BackgroundOutput {
                text: "line 1\nline 2\n".into(),
                exit_code: None,
                original_bytes: None,
            }
        );
        assert!(background_output(&dir.path().join("missing")).is_err());

        // Over the bounds: the head and the tail, the marker still read from the tail.
        let mut big = vec![b'a'; (BACKGROUND_OUTPUT_HEAD + 100) as usize];
        big.extend(vec![b'z'; BACKGROUND_OUTPUT_TAIL as usize]);
        big.extend(b"\n\n[exited with code 2]\n");
        let size = big.len() as u64;
        let large = write("large", &big);
        let output = read(&large);
        assert_eq!(output.exit_code, Some(2));
        assert_eq!(output.original_bytes, Some(size));
        assert!(output.text.starts_with(&"a".repeat(1000)));
        assert!(output.text.ends_with(&"z".repeat(1000)));
        assert!(output.text.contains("bytes omitted"));
        assert!((output.text.len() as u64) < BACKGROUND_OUTPUT_HEAD + BACKGROUND_OUTPUT_TAIL + 100);
    }

    #[test]
    fn a_denied_exit_plan_mode_is_answered_by_the_mapper() {
        let outputs = run_fixture("plan-exit-denied", TurnKind::User);
        let replies: Vec<&Value> = outputs
            .iter()
            .filter_map(|output| match output {
                MapperOutput::Reply(reply) => Some(reply),
                _ => None,
            })
            .collect();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0]["type"], "control_response");
        assert_eq!(
            replies[0]["response"]["request_id"],
            "5684537b-5f2b-4105-bb7a-f5a78ed75165"
        );
        assert_eq!(replies[0]["response"]["response"]["behavior"], "deny");
        assert_eq!(
            replies[0]["response"]["response"]["message"],
            PLAN_MODE_DENIAL
        );
        assert!(
            !events(&outputs)
                .iter()
                .any(|event| matches!(event, AgentEvent::ApprovalRequested { .. }))
        );

        let completed = completed_items(&outputs);
        assert!(completed.iter().any(|item| matches!(
            &item.payload,
            ItemPayload::FileChange { path, change: FileChangeKind::Created, changes, .. }
                if path.starts_with("/home/user/.claude/plans") && changes.len() == 1
        )));
        assert!(completed.iter().any(|item| matches!(
            &item.payload,
            ItemPayload::ToolCall { name, status, .. }
                if name == "ToolSearch" && status.as_deref() == Some("completed")
        )));
        assert!(completed.iter().any(|item| matches!(
            &item.payload,
            ItemPayload::ToolCall { name, status, .. }
                if name == "ExitPlanMode" && status.as_deref() == Some("declined")
        )));
    }

    #[test]
    fn a_missing_resume_is_a_failed_turn() {
        let outputs = run_fixture("resume-missing", TurnKind::User);
        let completions = turn_completions(&outputs);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Failed);
        let message = completions[0].2.message.as_deref().unwrap();
        assert!(message.contains("No conversation found"), "{message}");
    }

    #[test]
    fn session_scope_asks_carry_the_suggestion_metadata() {
        let outputs = run_fixture("accept-for-session", TurnKind::User);
        let approvals: Vec<_> = events(&outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::ApprovalRequested { request, .. } => Some(request),
                _ => None,
            })
            .collect();
        assert_eq!(approvals.len(), 1);
        assert!(approvals[0].metadata.contains(&ApprovalMetadata::Text {
            label: "Suggestion".into(),
            value: "addRules (localSettings)".into()
        }));
        assert!(approvals[0].metadata.contains(&ApprovalMetadata::Text {
            label: "Tool".into(),
            value: "Bash".into()
        }));
        let commands = command_statuses(&outputs);
        assert_eq!(commands.len(), 3);
        assert!(
            commands
                .iter()
                .all(|(_, status)| status.as_deref() == Some("completed"))
        );
    }

    #[test]
    fn the_effective_window_wins_over_model_usage() {
        let mut mapper = new_mapper();
        drive(&mut mapper, "autocompact-state", TurnKind::User);
        let outputs: Vec<MapperOutput> = drive(&mut mapper, "text-turn", TurnKind::User)
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(usage_updates(&outputs).last().unwrap().1, Some(180_000));

        let outputs = run_fixture("text-turn", TurnKind::User);
        let updates = usage_updates(&outputs);
        assert_eq!(updates.first().unwrap().1, None);
        assert_eq!(updates.last().unwrap().1, Some(200_000));
    }

    #[test]
    fn the_turn_model_is_reported_only_when_the_adapter_noted_one() {
        let outputs = run_fixture("text-turn", TurnKind::User);
        assert!(
            events(&outputs)
                .iter()
                .all(|event| !matches!(event, AgentEvent::TurnUsageUpdated { model: Some(_), .. }))
        );

        let mut mapper = new_mapper();
        mapper.begin_turn(TurnId::new(), TurnKind::User);
        let model = anthropic_model("claude-haiku-4-5-20251001");
        mapper.note_turn_model(model.clone());
        let outputs: Vec<MapperOutput> = out_lines("text-turn")
            .iter()
            .flat_map(|line| mapper.map_line(line))
            .collect();
        assert!(events(&outputs).iter().any(|event| matches!(
            event,
            AgentEvent::TurnUsageUpdated { model: Some(reported), .. } if *reported == model
        )));
    }

    /// The `delegation` fixture with its `Agent` call asking for `requested` (when set) and the
    /// sub-agent's frames naming `subagent_model`; the session stays on `claude-haiku-4-5-20251001`.
    fn delegation_on(requested: Option<&str>, subagent_model: &str) -> Vec<String> {
        out_lines("delegation")
            .into_iter()
            .map(|line| {
                let mut frame = frame_of(&line);
                if frame["type"] != "assistant" {
                    return line;
                }
                if frame["parent_tool_use_id"].is_string() {
                    frame["message"]["model"] = json!(subagent_model);
                } else if let Some(requested) = requested {
                    for block in frame["message"]["content"].as_array_mut().unwrap() {
                        if block["type"] == "tool_use" && block["name"] == AGENT_TOOL {
                            block["input"]["model"] = json!(requested);
                        }
                    }
                }
                frame.to_string()
            })
            .collect()
    }

    fn route_models(outputs: &[MapperOutput]) -> Vec<Option<ModelRef>> {
        outputs
            .iter()
            .filter_map(|output| match output {
                MapperOutput::RouteOpened { model, .. } => Some(model.clone()),
                _ => None,
            })
            .collect()
    }

    fn reported_models(outputs: &[MapperOutput]) -> Vec<Option<ModelRef>> {
        events(outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::TurnUsageUpdated { model, .. } => Some(model.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    #[traced_test]
    fn a_sub_agent_reports_the_model_its_frames_name() {
        let mut mapper = new_mapper();
        mapper.note_catalog([
            ("default".to_owned(), "claude-sonnet-5-5".to_owned()),
            ("opus".to_owned(), "claude-opus-5-5".to_owned()),
            ("sonnet".to_owned(), "claude-sonnet-5-5".to_owned()),
        ]);
        let primary = mapper.thread;
        let lines = delegation_on(Some("sonnet"), "claude-sonnet-5-5");
        let outputs = drive_lines(&mut mapper, &lines, 1, TurnKind::User, false).concat();

        // The route opens on the model its `Agent` call asked for.
        assert_eq!(
            route_models(&outputs),
            vec![Some(anthropic_model("sonnet"))]
        );
        let (child, ..) = routes_opened(&outputs)[0].clone();

        // Its usage names the model its frames ran on, as the catalog's selector (not `default`),
        // and no context window: the session's is the parent model's.
        let child_outputs = on_thread(&outputs, child);
        let reported = reported_models(&child_outputs);
        assert!(!reported.is_empty());
        assert!(
            reported
                .iter()
                .all(|model| *model == Some(anthropic_model("sonnet")))
        );
        assert!(
            usage_updates(&child_outputs)
                .iter()
                .all(|(_, window)| window.is_none())
        );
        // The primary turn reports only the model the adapter noted (none here).
        assert!(
            reported_models(&on_thread(&outputs, primary))
                .iter()
                .all(Option::is_none)
        );
        logs_assert(a_line_with(&[
            " INFO ",
            r#"action="subagent_model""#,
            "native_model=claude-sonnet-5-5",
            "model=sonnet",
        ]));
    }

    #[test]
    fn a_sub_agent_model_outside_the_catalog_is_its_own_id() {
        let mut mapper = new_mapper();
        let lines = delegation_on(Some("inherit"), "claude-opus-5-5");
        let outputs = drive_lines(&mut mapper, &lines, 1, TurnKind::User, false).concat();
        // `inherit` names no model, so the route opens on none.
        assert_eq!(route_models(&outputs), vec![None]);
        let (child, ..) = routes_opened(&outputs)[0].clone();
        assert!(
            reported_models(&on_thread(&outputs, child))
                .iter()
                .all(|model| *model == Some(anthropic_model("claude-opus-5-5")))
        );
    }

    #[test]
    fn a_sub_agent_reports_no_context_window_even_on_the_session_model() {
        let mut mapper = new_mapper();
        mapper.note_context_window(200_000);
        let primary = mapper.thread;
        let outputs = drive(&mut mapper, "delegation", TurnKind::User).concat();
        assert_eq!(route_models(&outputs), vec![None]);
        let (child, ..) = routes_opened(&outputs)[0].clone();
        let child_outputs = on_thread(&outputs, child);
        // The fixture's sub-agent runs on the session's own model, and still borrows no window:
        // the session's is the parent's effective window, not necessarily the sub-agent's.
        assert!(!usage_updates(&child_outputs).is_empty());
        assert!(
            usage_updates(&child_outputs)
                .iter()
                .all(|(_, window)| window.is_none())
        );
        assert!(
            reported_models(&child_outputs)
                .iter()
                .all(|model| *model == Some(anthropic_model("claude-haiku-4-5-20251001")))
        );
        // The primary keeps the session's window.
        assert!(
            usage_updates(&on_thread(&outputs, primary))
                .iter()
                .any(|(_, window)| window.is_some())
        );
    }

    /// The `delegation` lines up to (and including) the sub-agent's first `assistant` frame, that
    /// frame, and the rest.
    fn delegation_split() -> (Vec<String>, String, Vec<String>) {
        let lines = out_lines("delegation");
        let first = lines
            .iter()
            .position(|line| {
                let frame = frame_of(line);
                frame["type"] == "assistant" && frame["parent_tool_use_id"].is_string()
            })
            .unwrap();
        (
            lines[..=first].to_vec(),
            lines[first].clone(),
            lines[first + 1..].to_vec(),
        )
    }

    #[test]
    #[traced_test]
    fn a_synthetic_sub_agent_message_is_not_the_sub_agent_model() {
        let (head, sub_frame, tail) = delegation_split();
        let mut synthetic = frame_of(&sub_frame);
        synthetic["message"]["model"] = json!(SYNTHETIC_MODEL);
        synthetic["message"]["id"] = json!("msg_synthetic");
        synthetic["message"]["content"] =
            json!([{"type": "text", "text": "API Error: overloaded"}]);
        synthetic["message"]["usage"] = json!({"input_tokens": 0, "output_tokens": 0});
        let mut lines = head;
        lines.push(synthetic.to_string());
        lines.extend(tail);

        let mut mapper = new_mapper();
        let outputs = drive_lines(&mut mapper, &lines, 1, TurnKind::User, false).concat();
        let (child, ..) = routes_opened(&outputs)[0].clone();
        let child_outputs = on_thread(&outputs, child);
        assert!(
            reported_models(&child_outputs)
                .iter()
                .all(|model| *model == Some(anthropic_model("claude-haiku-4-5-20251001")))
        );
        // Its zero usage is not the sub-agent's either.
        assert!(
            usage_updates(&child_outputs)
                .iter()
                .all(|(usage, _)| *usage != TokenUsage::default())
        );
        // Its text still reaches the transcript.
        assert!(completed_items(&child_outputs).iter().any(|item| matches!(
            &item.payload,
            ItemPayload::AgentMessage { text } if text.contains("overloaded")
        )));
        logs_assert(a_line_with(&[
            r#"action="subagent_model""#,
            "a synthetic sub-agent message names no model",
        ]));
    }

    #[test]
    fn a_sub_agent_model_change_is_reported_without_new_usage() {
        let (head, sub_frame, _) = delegation_split();
        // A second frame of the same API message (its usage already counted) on another model.
        let mut switched = frame_of(&sub_frame);
        switched["message"]["model"] = json!("claude-opus-5-5");
        switched["message"]["content"] = json!([{"type": "text", "text": "switched"}]);
        let mut mapper = new_mapper();
        let mut outputs = drive_lines(&mut mapper, &head, 1, TurnKind::User, false).concat();
        let (child, ..) = routes_opened(&outputs)[0].clone();
        let before = reported_models(&on_thread(&outputs, child)).len();
        outputs = mapper.map_line(&switched.to_string());
        assert_eq!(
            reported_models(&on_thread(&outputs, child)),
            vec![Some(anthropic_model("claude-opus-5-5"))]
        );
        assert!(before > 0);
    }

    #[test]
    fn a_foreign_api_key_source_is_a_notice() {
        let init = out_lines("text-turn")[0].clone();
        let foreign = rewrite(&init, "/apiKeySource", json!("ANTHROPIC_API_KEY"));
        let mut mapper = new_mapper();
        assert!(notices(&mapper.map_line(&init)).is_empty());
        let outputs = mapper.map_line(&foreign);
        let notices = notices(&outputs);
        assert_eq!(notices.len(), 1);
        assert!(notices[0].contains("ANTHROPIC_API_KEY"));
        // A re-emitted init does not repeat it.
        assert!(events(&mapper.map_line(&foreign)).is_empty());
    }

    #[test]
    fn a_rate_limit_warning_is_a_notice_and_an_allowed_one_is_not() {
        let recorded = line_of("text-turn", "rate_limit_event");
        let mut mapper = new_mapper();
        assert!(events(&mapper.map_line(&recorded)).is_empty());

        let warning = rewrite(
            &recorded,
            "/rate_limit_info/status",
            json!("allowed_warning"),
        );
        assert_eq!(notices(&mapper.map_line(&warning)).len(), 1);

        let hot = rewrite(
            &recorded,
            "/rate_limit_info/unifiedWindows/five_hour/utilization",
            json!(0.93),
        );
        let outputs = mapper.map_line(&hot);
        let notices = notices(&outputs);
        assert_eq!(notices.len(), 1);
        assert!(
            notices[0].contains("five-hour window 93% used"),
            "{}",
            notices[0]
        );
    }

    #[test]
    #[traced_test]
    fn a_permission_mode_the_adapter_did_not_set_is_a_notice() {
        let mut mapper = new_mapper();
        mapper.set_expected_mode("default");
        let status = r#"{"type":"system","subtype":"status","status":null,"permissionMode":"plan","session_id":"s"}"#;
        let outputs = mapper.map_line(status);
        assert_eq!(notices(&outputs).len(), 1);
        logs_assert(a_line_with(&[
            " WARN ",
            r#"action="permission_mode_drift""#,
        ]));

        mapper.set_expected_mode("plan");
        assert!(events(&mapper.map_line(status)).is_empty());
    }

    #[test]
    #[traced_test]
    fn a_permission_mode_the_adapter_did_not_set_on_init_is_a_notice() {
        let mut mapper = new_mapper();
        mapper.set_expected_mode("acceptEdits");
        let init = line_of("text-turn", "system");
        let outputs = mapper.map_line(&init);
        assert_eq!(
            notices(&outputs),
            ["Claude Code switched its permission mode to default; Giskard set acceptEdits"]
        );
        logs_assert(a_line_with(&[
            " WARN ",
            r#"action="permission_mode_drift""#,
        ]));

        // No expectation yet (before the adapter set a mode): no drift.
        let mut fresh = new_mapper();
        assert!(notices(&fresh.map_line(&init)).is_empty());
    }

    #[test]
    #[traced_test]
    fn a_control_cancel_request_is_handed_to_the_adapter() {
        let mut mapper = new_mapper();
        let outputs =
            mapper.map_line(r#"{"type":"control_cancel_request","request_id":"a949f115"}"#);
        assert!(matches!(
            &outputs[..],
            [MapperOutput::CancelRequest { request_id }] if request_id == "a949f115"
        ));
        logs_assert(a_line_with(&[
            " INFO ",
            r#"action="control_cancel_request""#,
        ]));
        logs_assert(no_line_with("does not know"));
    }

    #[test]
    #[traced_test]
    fn a_denial_the_adapter_noted_completes_the_tool_declined() {
        let mut mapper = new_mapper();
        let lines = out_lines("tool-allowed");
        let mut outputs = Vec::new();
        outputs.extend(mapper.begin_turn(TurnId::new(), TurnKind::User));
        for line in &lines {
            outputs.extend(mapper.map_line(line));
            if line.contains("\"control_request\"") {
                let thread = mapper.thread;
                mapper.note_denied(thread, "toolu_01FZdtNSm7HPbRG2vZ6fknrF");
            }
        }
        assert_eq!(
            command_statuses(&outputs),
            vec![("touch probe.txt".into(), Some("declined".into()))]
        );

        let mut idle = new_mapper();
        let thread = idle.thread;
        idle.note_denied(thread, "toolu_x");
        logs_assert(a_line_with(&[" WARN ", r#"action="note_denied""#]));
    }

    #[test]
    #[traced_test]
    fn an_orphan_tool_result_is_dropped_with_a_warning() {
        let mut mapper = new_mapper();
        mapper.begin_turn(TurnId::new(), TurnKind::User);
        let line = r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_orphan","content":"secret output","is_error":false}]},"parent_tool_use_id":null,"session_id":"f18693ff-2d11-4f87-9556-2b527e19e081"}"#;
        let outputs = mapper.map_line(line);
        assert!(events(&outputs).is_empty());
        logs_assert(lines_with(1, &[" WARN "]));
        logs_assert(lines_with(1, &[" WARN ", "native_item_id=toolu_orphan"]));
        logs_assert(no_line_with("secret output"));
    }

    #[test]
    #[traced_test]
    fn unknown_frames_are_logged_once_per_kind() {
        let mut mapper = new_mapper();
        let line = r#"{"type":"active_goal","value":null,"session_id":"s"}"#;
        let mut outputs = Vec::new();
        outputs.extend(mapper.map_line(line));
        outputs.extend(mapper.map_line(line));
        assert!(outputs.is_empty());
        let message = "frame kind this adapter does not know";
        logs_assert(lines_with(2, &[message]));
        logs_assert(lines_with(
            1,
            &[message, " WARN ", "frame_type=active_goal"],
        ));
        logs_assert(lines_with(1, &[message, " DEBUG "]));
    }

    #[test]
    #[traced_test]
    fn unparseable_lines_are_logged_without_their_content() {
        let mut mapper = new_mapper();
        let mut outputs = Vec::new();
        outputs.extend(mapper.map_line("Error: touch probe.txt failed"));
        outputs.extend(mapper.map_line(
            r#"{"type":"assistant","session_id":"s","message":{"id":"m","role":"assistant","content":"touch probe.txt"}}"#,
        ));
        assert!(outputs.is_empty());
        logs_assert(a_line_with(&[" WARN ", "not JSON", "bytes=29"]));
        logs_assert(a_line_with(&[" WARN ", "frame_type=assistant"]));
        logs_assert(no_line_with("touch probe.txt"));
    }

    #[test]
    #[traced_test]
    fn logs_never_carry_frame_content() {
        run_fixture("tool-denied", TurnKind::User);
        // Something was logged, at any level, and none of it is frame content.
        logs_assert(a_line_with(&[]));
        logs_assert(no_line_with("touch probe.txt"));
    }

    #[test]
    #[traced_test]
    fn a_turn_begun_while_one_is_active_fails_the_first() {
        let mut mapper = new_mapper();
        let first = TurnId::new();
        let second = TurnId::new();
        mapper.begin_turn(first, TurnKind::User);
        let outputs = mapper.begin_turn(second, TurnKind::User);
        let completions = turn_completions(&outputs);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].0, first);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Failed);
        assert_eq!(
            completions[0].2.message.as_deref(),
            Some("superseded by a new turn")
        );
        assert!(matches!(
            events(&outputs).last(),
            Some(AgentEvent::TurnStarted { turn, .. }) if *turn == second
        ));
        logs_assert(lines_with(1, &[" ERROR "]));
        assert_eq!(mapper.active_turn(), Some(second));
    }

    #[test]
    fn other_control_requests_become_server_requests() {
        let mut mapper = new_mapper();
        let turn = TurnId::new();
        mapper.begin_turn(turn, TurnKind::User);
        let outputs = mapper.map_line(
            r#"{"type":"control_request","request_id":"r9","request":{"subtype":"request_user_dialog","title":"t"}}"#,
        );
        assert!(matches!(
            events(&outputs)[..],
            [AgentEvent::ServerRequestReceived { turn: Some(t), request, .. }]
                if *t == turn && request.method == "claude/request_user_dialog"
                    && request.params["title"] == "t"
        ));
        assert!(outputs.iter().any(|output| matches!(
            output,
            MapperOutput::PendingServerRequest { request_id, subtype, input, .. }
                if request_id == "r9" && subtype == "request_user_dialog" && input.is_null()
        )));
    }

    #[test]
    fn an_ask_user_question_is_a_server_request() {
        let mut mapper = new_mapper();
        mapper.begin_turn(TurnId::new(), TurnKind::User);
        let outputs = mapper.map_line(
            r#"{"type":"control_request","request_id":"q1","request":{"subtype":"can_use_tool","tool_name":"AskUserQuestion","input":{"questions":[]},"tool_use_id":"toolu_q"}}"#,
        );
        assert!(matches!(
            events(&outputs)[..],
            [AgentEvent::ServerRequestReceived { turn: Some(_), request, .. }]
                if request.method == "claude/ask_user_question" && request.id.0 == "q1"
        ));
        assert!(outputs.iter().any(|output| matches!(
            output,
            MapperOutput::PendingServerRequest { request_id, subtype, input, .. }
                if request_id == "q1" && subtype == "can_use_tool" && input["questions"] == json!([])
        )));

        // Each question gains its index as `id`, which the browser's card keys answers by.
        let outputs = mapper.map_line(
            r#"{"type":"control_request","request_id":"q2","request":{"subtype":"can_use_tool","tool_name":"AskUserQuestion","input":{"questions":[{"question":"A?","header":"a","options":[],"multiSelect":false},{"question":"B?","header":"b","options":[],"multiSelect":true}]},"requires_user_interaction":true,"tool_use_id":"toolu_q2"}}"#,
        );
        let AgentEvent::ServerRequestReceived { request, .. } = events(&outputs)[0] else {
            panic!("no server request");
        };
        assert_eq!(request.params["questions"][0]["id"], "0");
        assert_eq!(request.params["questions"][1]["id"], "1");
        assert_eq!(request.params["questions"][1]["question"], "B?");
        assert!(outputs.iter().any(|output| matches!(
            output,
            MapperOutput::PendingServerRequest { input, .. } if input["questions"][0].get("id").is_none()
        )), "the pending input stays the CLI's own");
    }

    #[test]
    fn control_responses_are_handed_to_the_adapter() {
        let outputs = run_fixture("initialize", TurnKind::User);
        let responses: Vec<_> = outputs
            .iter()
            .filter_map(|output| match output {
                MapperOutput::ControlResponse {
                    request_id,
                    payload,
                } => Some((request_id.as_str(), payload)),
                _ => None,
            })
            .collect();
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0].0, "bfcc5636-ee33-4606-a861-2b87ffbfee71");
        assert!(responses[0].1["response"]["models"].is_array());
        assert_eq!(responses[1].0, "6bf79889-ef4e-48ef-89e8-51f8d988d387");
        assert!(events(&outputs).is_empty());
    }

    #[test]
    fn has_tasks_is_true_from_task_started_to_the_task_s_end() {
        for (name, kind) in [
            ("background-bash", "local_bash"),
            ("delegation", "local_agent"),
        ] {
            let lines = out_lines(name);
            let started = lines
                .iter()
                .position(|line| line.contains(r#""subtype": "task_started""#))
                .unwrap();
            // A background command's task ends with its notification, an agent's with its
            // terminal update.
            let end = match kind {
                "local_bash" => r#""subtype": "task_notification""#,
                _ => r#""subtype": "task_updated""#,
            };
            let terminal = lines.iter().rposition(|line| line.contains(end)).unwrap();
            let mut mapper = new_mapper();
            assert!(!mapper.has_tasks());
            drive_lines(&mut mapper, &lines[..started], 1, TurnKind::User, false);
            assert!(!mapper.has_tasks(), "{kind}: before task_started");
            drive_lines(
                &mut mapper,
                &lines[started..terminal],
                0,
                TurnKind::User,
                false,
            );
            assert!(mapper.has_tasks(), "{kind}: after task_started");
            assert_eq!(mapper.open_tasks(), 1, "{kind}");
            drive_lines(
                &mut mapper,
                &lines[terminal..=terminal],
                0,
                TurnKind::User,
                false,
            );
            assert!(!mapper.has_tasks(), "{kind}: after the task's end");
        }
    }

    #[test]
    #[traced_test]
    fn child_exited_and_note_context_window() {
        // No turn: nothing to complete.
        let mut mapper = new_mapper();
        assert!(mapper.child_exited("code 0").is_empty());

        // A turn cut short fails with the exit in its message.
        let turn = TurnId::new();
        mapper.begin_turn(turn, TurnKind::User);
        let lines = out_lines("text-turn");
        for line in &lines[..4] {
            mapper.map_line(line);
        }
        let outputs = mapper.child_exited("code 3");
        let completions = turn_completions(&outputs);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].0, turn);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Failed);
        assert_eq!(
            completions[0].2.message.as_deref(),
            Some("Claude Code exited (code 3) before the turn completed")
        );
        logs_assert(a_line_with(&[" WARN ", "action=\"child_exited\""]));
        assert!(mapper.active_turn().is_none());

        // After an interrupt the same exit is an interruption.
        mapper.begin_turn(TurnId::new(), TurnKind::User);
        mapper.note_interrupt_sent();
        let outputs = mapper.child_exited("signal 9");
        assert_eq!(
            turn_completions(&outputs)[0].2.kind,
            TurnStatusKind::Interrupted
        );

        // A seeded window is reported until the session reports its own.
        let mut mapper = new_mapper();
        mapper.note_context_window(150_000);
        mapper.begin_turn(TurnId::new(), TurnKind::User);
        let outputs: Vec<MapperOutput> = lines.iter().flat_map(|l| mapper.map_line(l)).collect();
        let updates = usage_updates(&outputs);
        assert_eq!(updates.first().unwrap().1, Some(150_000));
        assert_eq!(updates.last().unwrap().1, Some(200_000));

        // A window a result already reported is not overwritten by a later seed.
        mapper.note_context_window(1);
        mapper.begin_turn(TurnId::new(), TurnKind::User);
        let outputs: Vec<MapperOutput> = lines.iter().flat_map(|l| mapper.map_line(l)).collect();
        assert_eq!(usage_updates(&outputs).first().unwrap().1, Some(200_000));
    }

    #[test]
    fn mcp_tools_name_their_server() {
        let ToolKind::Call { name, server, .. } =
            classify_tool("mcp__brave-search__web_search", json!({}))
        else {
            panic!("not a tool call");
        };
        assert_eq!(name, "web_search");
        assert_eq!(server.as_deref(), Some("brave-search"));
    }

    // ---- sub-agent routes ----------------------------------------------------------------------

    const DELEGATION_CALL: &str = "toolu_01DSgcYLdZqTSvfAwdnE2njN";
    const DELEGATION_PROMPT: &str = "Read the file data.txt in the current directory \
        (/work/project) and find the magic number. Report what you find.";
    const SESSION: &str = "f18693ff-2d11-4f87-9556-2b527e19e081";

    fn position(lines: &[String], test: impl Fn(&Value) -> bool) -> usize {
        lines.iter().position(|line| test(&frame_of(line))).unwrap()
    }

    #[test]
    #[traced_test]
    fn a_foreground_delegation_materializes_a_child_route() {
        let lines = out_lines("delegation");
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        let per_line = drive(&mut mapper, "delegation", TurnKind::User);
        let outputs = per_line.concat();

        let opened = routes_opened(&outputs);
        assert_eq!(opened.len(), 1);
        let (child, native, parent, name) = opened[0].clone();
        assert_eq!(native, format!("task:{DELEGATION_CALL}"));
        assert_eq!(parent, SESSION);
        assert_eq!(name.as_deref(), Some("Read and find magic number"));

        // The child's own transcript: its turn, the delegated prompt, its tool call, its answer.
        let child_outputs = on_thread(&outputs, child);
        let child_events = events(&child_outputs);
        assert!(matches!(child_events[0], AgentEvent::TurnStarted { .. }));
        let items = completed_items(&child_outputs);
        assert_eq!(
            items[0].payload,
            ItemPayload::UserMessage {
                text: DELEGATION_PROMPT.into()
            }
        );
        assert!(items[0].harness_item_id.starts_with("user:"));
        assert!(items.iter().any(|item| matches!(
            &item.payload,
            ItemPayload::ToolCall { name, status, .. }
                if name == "Read" && status.as_deref() == Some("completed")
        )));
        assert!(
            started_items(&child_outputs)
                .iter()
                .any(|item| item.tool.as_ref().is_some_and(|tool| tool.name == "Read"))
        );
        assert!(items.iter().any(|item| matches!(
            &item.payload,
            ItemPayload::AgentMessage { text } if text.contains("4271")
        )));
        assert!(!usage_updates(&child_outputs).is_empty());
        let updated = position(&lines, |frame| frame["subtype"] == "task_updated");
        let completions = turn_completions(&on_thread(&per_line[updated], child));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Completed);
        assert_eq!(turn_completions(&child_outputs).len(), 1);

        // The parent's `Agent` item carries the link at start and at completion.
        let primary_outputs = on_thread(&outputs, primary);
        let start = started_items(&primary_outputs)
            .into_iter()
            .find_map(|item| item.tool.as_ref().filter(|tool| tool.name == AGENT_TOOL))
            .unwrap()
            .subagent
            .clone()
            .unwrap();
        assert_eq!(start.harness_thread_id, native);
        assert_eq!(start.action, SubagentAction::Spawned);
        assert_eq!(start.status, Some(SubagentStatus::Pending));
        assert_eq!(start.initial_prompt.as_deref(), Some(DELEGATION_PROMPT));
        let (status, error, link) = agent_completion(&primary_outputs);
        assert_eq!(status.as_deref(), Some("completed"));
        assert_eq!(error, None);
        assert_eq!(link.action, SubagentAction::Completed);
        assert_eq!(link.status, Some(SubagentStatus::Completed));
        assert!(
            started_items(&primary_outputs)
                .iter()
                .all(|item| item.tool.as_ref().is_none_or(|tool| tool.name != "Read"))
        );

        // The route is dropped when the turn that spawned it ends.
        let result = position(&lines, |frame| frame["type"] == "result");
        assert_eq!(
            turn_completions(&on_thread(&per_line[result], primary)).len(),
            1
        );
        assert_eq!(routes_closed(&per_line[result]), vec![child]);
        assert_eq!(routes_closed(&outputs).len(), 1);
        logs_assert(a_line_with(&[" INFO ", r#"action="route_opened""#]));
        logs_assert(a_line_with(&[" INFO ", r#"action="route_closed""#]));
    }

    const FORKED_SKILL_CALL: &str = "toolu_01RAGxZFrkYotPgFPKQwJLEc";
    const FORKED_SKILL_PROMPT: &str = "Base directory for this skill: \
        /work/project/.claude/skills/magic\n\nRead data.txt in the current directory with the \
        Read tool and reply with the magic number only.\n";

    #[test]
    #[traced_test]
    fn a_forked_skill_is_a_sub_agent_route() {
        let lines = out_lines("forked-skill");
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        let per_line = drive(&mut mapper, "forked-skill", TurnKind::User);
        let outputs = per_line.concat();

        // The route is minted at the task's start: nothing earlier says the call delegates.
        let started = position(&lines, |frame| frame["subtype"] == "task_started");
        let opened = routes_opened(&per_line[started]);
        assert_eq!(opened.len(), 1);
        assert_eq!(routes_opened(&outputs).len(), 1);
        let (child, native, parent, name) = opened[0].clone();
        assert_eq!(native, format!("task:{FORKED_SKILL_CALL}"));
        assert_eq!(parent, SESSION);
        assert_eq!(name.as_deref(), Some("/magic"));

        // The `Skill` item starts once, with no link: nothing then says it delegates.
        let primary_outputs = on_thread(&outputs, primary);
        let skill_starts: Vec<_> = started_items(&primary_outputs)
            .into_iter()
            .filter(|item| item.harness_item_id == FORKED_SKILL_CALL)
            .collect();
        assert_eq!(skill_starts.len(), 1);
        assert!(skill_starts[0].tool.as_ref().unwrap().subagent.is_none());

        // At the task's start, an activity on the spawning turn carries the link.
        let announced = on_thread(&per_line[started], primary);
        let activity_starts = started_items(&announced);
        assert_eq!(activity_starts.len(), 1);
        assert_eq!(activity_starts[0].kind, ItemKind::Activity);
        let activities = completed_items(&announced);
        assert_eq!(activities.len(), 1);
        let activity = activities[0];
        assert_eq!(activity.id, activity_starts[0].id);
        assert_eq!(activity.harness_item_id, "task_started:ab74653d91e526513");
        let ItemPayload::Activity {
            title,
            detail,
            subagent: Some(link),
            ..
        } = &activity.payload
        else {
            panic!("no linked activity: {:?}", activity.payload);
        };
        assert_eq!(title, "/magic");
        assert_eq!(detail.as_deref(), Some("Skill sub-agent"));
        assert_eq!(link.harness_thread_id, native);
        assert_eq!(link.initial_prompt.as_deref(), Some(FORKED_SKILL_PROMPT));
        assert_eq!(link.action, SubagentAction::Spawned);
        assert_eq!(link.status, Some(SubagentStatus::Pending));
        // Every item is started at most once.
        let mut ids: Vec<_> = started_items(&outputs).iter().map(|item| item.id).collect();
        let starts = ids.len();
        ids.sort_unstable_by_key(|id| id.to_string());
        ids.dedup();
        assert_eq!(ids.len(), starts);

        // The child's transcript: the skill's prompt, its tool calls and its answer.
        let child_outputs = on_thread(&outputs, child);
        assert!(matches!(
            events(&child_outputs)[0],
            AgentEvent::TurnStarted { .. }
        ));
        let items = completed_items(&child_outputs);
        assert_eq!(
            items[0].payload,
            ItemPayload::UserMessage {
                text: FORKED_SKILL_PROMPT.into()
            }
        );
        assert!(items.iter().any(|item| matches!(
            &item.payload,
            ItemPayload::ToolCall { name, status, .. }
                if name == "Read" && status.as_deref() == Some("completed")
        )));
        assert!(items.iter().any(|item| matches!(
            &item.payload,
            ItemPayload::CommandExecution { command, .. } if command.starts_with("find ")
        )));
        assert!(items.iter().any(|item| matches!(
            &item.payload,
            ItemPayload::AgentMessage { text } if text == "4271"
        )));
        // None of it lands on the primary thread.
        assert!(started_items(&primary_outputs).iter().all(|item| {
            item.tool.as_ref().is_none_or(|tool| tool.name != "Read") && item.command.is_none()
        }));

        // The notification ends the sub-agent: no terminal update comes for a forked skill.
        assert!(!lines.iter().any(|line| line.contains("task_updated")));
        let notified = position(&lines, |frame| frame["subtype"] == "task_notification");
        let completions = turn_completions(&on_thread(&per_line[notified], child));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Completed);
        assert_eq!(turn_completions(&child_outputs).len(), 1);

        // The `Skill` completion carries the ended link, like an `Agent` call's.
        let (status, link) = completed_items(&primary_outputs)
            .into_iter()
            .find_map(|item| match &item.payload {
                ItemPayload::ToolCall {
                    name,
                    status,
                    subagent: Some(link),
                    ..
                } if name == "Skill" => Some((status.clone(), link.clone())),
                _ => None,
            })
            .expect("no completed Skill call with a link");
        assert_eq!(status.as_deref(), Some("completed"));
        assert_eq!(link.action, SubagentAction::Completed);
        assert_eq!(link.status, Some(SubagentStatus::Completed));

        // The single `result` completes the primary turn: the task no longer holds it.
        let result = position(&lines, |frame| frame["type"] == "result");
        let completions = turn_completions(&on_thread(&per_line[result], primary));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Completed);
        assert_eq!(routes_closed(&per_line[result]), vec![child]);
        assert!(!mapper.has_tasks());
        logs_assert(a_line_with(&[" INFO ", r#"action="task_route""#]));
        logs_assert(a_line_with(&[" INFO ", r#"action="task_notification""#]));
        logs_assert(no_line_with(r#"action="hold_result""#));
    }

    #[test]
    #[traced_test]
    fn a_stopped_forked_skill_is_interrupted_and_its_turn_goes_on() {
        // `stop_task` on the forked task while its sub-agent's command ran; the model then ran
        // the skill again, and that run completed.
        let lines = out_lines("forked-skill-stop");
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        let mut per_line = vec![mapper.begin_turn(TurnId::new(), TurnKind::User)];
        let mut routes = Vec::new();
        for line in &lines {
            let outputs = mapper.map_line(line);
            for (thread, ..) in routes_opened(&outputs) {
                // The adapter notes its `stop_task` on the first route only.
                if routes.is_empty() {
                    mapper.note_stop_sent(thread);
                }
                routes.push(thread);
            }
            per_line.push(outputs);
        }
        // `per_line[0]` is the turn's start: line `i` of the fixture is `per_line[i + 1]`.
        let at = |test: &dyn Fn(&Value) -> bool| &per_line[position(&lines, test) + 1];
        let outputs = per_line.concat();
        assert_eq!(routes.len(), 2);
        let (stopped, rerun) = (routes[0], routes[1]);

        // A stopped forked task gets the usual pair: `killed`, then a `stopped` notification. The
        // update ends the route as interrupted; the notification finds nothing left to end.
        let killed = at(&|frame| {
            frame["subtype"] == "task_updated" && frame["task_id"] == "ac449741b86a05952"
        });
        let completions = turn_completions(&on_thread(killed, stopped));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Interrupted);
        assert_eq!(turn_completions(&on_thread(&outputs, stopped)).len(), 1);
        let notified = at(&|frame| {
            frame["subtype"] == "task_notification" && frame["task_id"] == "ac449741b86a05952"
        });
        assert!(events(notified).is_empty());
        // The sub-agent's interruption marker trails its end and is dropped.
        let marker = at(&|frame| {
            frame["parent_tool_use_id"].is_string()
                && frame.pointer("/message/content/0/text")
                    == Some(&json!("[Request interrupted by user]"))
        });
        assert!(events(marker).is_empty());

        // The sub-agent's own background command, killed with it, completes after the route's
        // turn ended: on that turn, which the server amends late, while the route still stands.
        let stopped_turn = completions[0].0;
        let command_update =
            at(&|frame| frame["subtype"] == "task_updated" && frame["task_id"] == "b7jfr5o7a");
        assert!(events(command_update).is_empty());
        let command_end =
            at(&|frame| frame["subtype"] == "task_notification" && frame["task_id"] == "b7jfr5o7a");
        let late: Vec<_> = events(command_end)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::ItemCompleted {
                    thread, turn, item, ..
                } => Some((*thread, *turn, item.payload.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(late.len(), 1);
        assert_eq!((late[0].0, late[0].1), (stopped, stopped_turn));
        assert!(matches!(
            &late[0].2,
            ItemPayload::CommandExecution { status: Some(status), .. } if status == "terminated"
        ));
        assert!(routes_closed(command_end).is_empty());

        // The stopped `Skill` call reads as the interrupted `Agent` call does.
        let skill_completions: Vec<_> = completed_items(&on_thread(&outputs, primary))
            .into_iter()
            .filter_map(|item| match &item.payload {
                ItemPayload::ToolCall {
                    name,
                    status,
                    error,
                    subagent: Some(link),
                    ..
                } if name == "Skill" => Some((status.clone(), error.clone(), link.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(skill_completions.len(), 2);
        let (status, error, link) = &skill_completions[0];
        assert_eq!(status.as_deref(), Some("declined"));
        assert!(
            error
                .as_deref()
                .unwrap()
                .contains("[Request interrupted by user for tool use]")
        );
        assert_eq!(link.status, Some(SubagentStatus::Interrupted));
        let (status, _, link) = &skill_completions[1];
        assert_eq!(status.as_deref(), Some("completed"));
        assert_eq!(link.status, Some(SubagentStatus::Completed));

        // The rerun is a second sub-agent, ended by its notification alone.
        let completions = turn_completions(&on_thread(&outputs, rerun));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Completed);

        // The stop does not end the primary turn: its single `result` does, held by nothing.
        let result = at(&|frame| frame["type"] == "result");
        let completions = turn_completions(&on_thread(result, primary));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Completed);
        assert_eq!(turn_completions(&on_thread(&outputs, primary)).len(), 1);
        assert!(!mapper.has_tasks());
        logs_assert(no_line_with(r#"action="hold_result""#));
        logs_assert(no_line_with(r#"action="agent_task_abandoned""#));
        // Its `tool_progress` heartbeats are read and ignored, not unknown.
        logs_assert(no_line_with(
            "skipping a frame kind this adapter does not know",
        ));
    }

    #[test]
    fn a_notification_only_stop_is_interrupted_only_when_the_adapter_stopped_it() {
        // No recording ends an agent task with a `stopped` notification alone: a stopped forked
        // skill gets its `killed` update first (`forked-skill-stop`). Should one come, as for a
        // task orphaned by a worker restart, it maps as that update would.
        let lines: Vec<String> = out_lines("forked-skill")
            .into_iter()
            .map(|line| match frame_of(&line)["subtype"].as_str() {
                Some("task_notification") => rewrite(&line, "/status", json!("stopped")),
                _ => line,
            })
            .collect();
        for stop in [true, false] {
            let mut mapper = new_mapper();
            let mut outputs = mapper.begin_turn(TurnId::new(), TurnKind::User);
            let mut child = None;
            for line in &lines {
                let mapped = mapper.map_line(line);
                if let Some((thread, ..)) = routes_opened(&mapped).first() {
                    child = Some(*thread);
                    if stop {
                        mapper.note_stop_sent(*thread);
                    }
                }
                outputs.extend(mapped);
            }
            let completions = turn_completions(&on_thread(&outputs, child.unwrap()));
            assert_eq!(completions.len(), 1);
            let status = &completions[0].2;
            if stop {
                assert_eq!(status.kind, TurnStatusKind::Interrupted);
                assert_eq!(status.message, None);
            } else {
                assert_eq!(status.kind, TurnStatusKind::Failed);
                assert_eq!(
                    status.message.as_deref(),
                    Some("agent task ab74653d91e526513 was stopped")
                );
            }
        }
    }

    #[test]
    fn a_notification_ends_an_agent_task_that_got_no_terminal_update() {
        // A `local_agent` task with no route, ended by its notification alone, then the result.
        let mut mapper = new_mapper();
        let turn = TurnId::new();
        mapper.begin_turn(turn, TurnKind::User);
        mapper.map_line(r#"{"type":"system","subtype":"task_started","session_id":"s","task_id":"a1","tool_use_id":"toolu_unknown","task_type":"local_agent","is_backgrounded":false,"description":"/review","uuid":"u1"}"#);
        assert!(mapper.has_tasks());
        let notified = mapper.map_line(r#"{"type":"system","subtype":"task_notification","session_id":"s","task_id":"a1","tool_use_id":"toolu_unknown","status":"completed","summary":"/review","uuid":"u2"}"#);
        assert!(turn_completions(&notified).is_empty());
        assert!(!mapper.has_tasks());
        let completions = turn_completions(&mapper.map_line(&line_of("text-turn", "result")));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].0, turn);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Completed);
    }

    #[test]
    #[traced_test]
    fn a_backgrounded_delegation_reports_its_outcome_on_the_parent() {
        let lines = out_lines("delegation-interrupted");
        let killed = position(&lines, |frame| frame["patch"]["status"] == "killed");
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        let turn = TurnId::new();
        let mut per_line = Vec::new();
        let mut child = None;
        for (index, line) in lines.iter().enumerate() {
            let mut outputs = Vec::new();
            if index == 0 {
                outputs.extend(mapper.begin_turn(turn, TurnKind::User));
            }
            // The adapter stops the sub-agent from its own thread.
            if index == killed {
                mapper.note_stop_sent(child.unwrap());
            }
            outputs.extend(mapper.map_line(line));
            if let Some((thread, ..)) = routes_opened(&outputs).first() {
                child = Some(*thread);
            }
            per_line.push(outputs);
        }
        let outputs = per_line.concat();
        let child = child.unwrap();

        // The call completes at launch, its link still running.
        let (status, _, link) = agent_completion(&on_thread(&outputs, primary));
        assert_eq!(status.as_deref(), Some("completed"));
        assert_eq!(link.action, SubagentAction::Started);
        assert_eq!(link.status, Some(SubagentStatus::Running));

        // The killed update ends the route at once: the adapter stopped it.
        let completions = turn_completions(&on_thread(&per_line[killed], child));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Interrupted);
        assert_eq!(turn_completions(&on_thread(&outputs, child)).len(), 1);

        // Its outcome is a row on the delegating turn, with the link: a late completion, since
        // that turn ended at its `result`.
        let at_kill = on_thread(&per_line[killed], primary);
        assert!(started_items(&at_kill).is_empty());
        let outcome = outcome_rows(&at_kill);
        assert_eq!(outcome.len(), 1);
        assert_eq!(outcome[0].0, turn);
        assert_eq!(outcome[0].1, "task_updated:a562b8d9494720fd6");
        assert_eq!(outcome[0].2, "Run bash command and report magic number");
        assert_eq!(outcome[0].3.as_deref(), Some("killed"));
        assert_eq!(outcome[0].4.action, SubagentAction::Interrupted);
        assert_eq!(outcome[0].4.status, Some(SubagentStatus::Interrupted));
        logs_assert(a_line_with(&[
            " INFO ",
            r#"action="subagent_outcome""#,
            "late=true",
        ]));

        // The rejection and the marker trail the kill: nothing, at debug.
        let trailing: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter(|(index, line)| {
                *index > killed && frame_of(line)["parent_tool_use_id"].is_string()
            })
            .map(|(index, _)| index)
            .collect();
        assert_eq!(trailing.len(), 2);
        for index in trailing {
            assert!(per_line[index].is_empty(), "{:?}", per_line[index]);
        }
        logs_assert(lines_with(
            2,
            &[" DEBUG ", r#"action="route_trailing_frame""#],
        ));

        let primary_completions = turn_completions(&on_thread(&outputs, primary));
        assert_eq!(primary_completions.len(), 1);
        assert_eq!(primary_completions[0].0, turn);
        assert_eq!(primary_completions[0].2.kind, TurnStatusKind::Completed);
    }

    /// Every outcome row on `outputs`: `(turn, harness_item_id, title, detail, link)`.
    fn outcome_rows(
        outputs: &[MapperOutput],
    ) -> Vec<(TurnId, String, String, Option<String>, SubagentLink)> {
        events(outputs)
            .into_iter()
            .filter_map(|event| match event {
                AgentEvent::ItemCompleted { turn, item, .. } => match &item.payload {
                    ItemPayload::Activity {
                        title,
                        detail,
                        subagent: Some(link),
                        ..
                    } if item.harness_item_id.starts_with("task_updated:") => Some((
                        *turn,
                        item.harness_item_id.clone(),
                        title.clone(),
                        detail.clone(),
                        link.clone(),
                    )),
                    _ => None,
                },
                _ => None,
            })
            .collect()
    }

    /// Drive a `delegation-background*` recording as the adapter would: a user turn begun before
    /// each line index of `turns` (where the recorder wrote its prompts), and `note_stop_sent` for
    /// the route before line index `stop`. Returns each line's outputs, the begun turns and the
    /// route's thread.
    fn drive_background(
        mapper: &mut ClaudeMapper,
        name: &str,
        turns: &[usize],
        stop: Option<usize>,
    ) -> (Vec<Vec<MapperOutput>>, Vec<TurnId>, ThreadId) {
        let mut begun = Vec::new();
        let mut route = None;
        let mut per_line = Vec::new();
        for (index, line) in out_lines(name).iter().enumerate() {
            let mut outputs = Vec::new();
            if turns.contains(&index) {
                let turn = TurnId::new();
                outputs.extend(mapper.begin_turn(turn, TurnKind::User));
                begun.push(turn);
            }
            if stop == Some(index) {
                mapper.note_stop_sent(route.unwrap());
            }
            outputs.extend(mapper.map_line(line));
            if let Some((thread, ..)) = routes_opened(&outputs).first() {
                route = Some(*thread);
            }
            per_line.push(outputs);
        }
        (per_line, begun, route.expect("no route was minted"))
    }

    /// The turn started on `thread` at `outputs`, if any.
    fn turn_started_on(outputs: &[MapperOutput], thread: ThreadId) -> Option<TurnId> {
        events(&on_thread(outputs, thread))
            .into_iter()
            .find_map(|event| match event {
                AgentEvent::TurnStarted { turn, .. } => Some(*turn),
                _ => None,
            })
    }

    #[test]
    #[traced_test]
    fn a_background_sub_agent_runs_beside_the_parent_s_next_prompt() {
        // Fixture lines are 1-based in the README; `per_line` is 0-based.
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        let (per_line, begun, child) =
            drive_background(&mut mapper, "delegation-background", &[0, 8], None);
        let outputs = per_line.concat();
        let lines = out_lines("delegation-background");

        // Three primary turns: "launched" at line 8, "pong" at line 24, the continuation at 27.
        let completed = |index: usize| turn_completions(&on_thread(&per_line[index], primary));
        assert_eq!(completed(7).len(), 1);
        assert_eq!(completed(7)[0].0, begun[0]);
        assert_eq!(completed(23).len(), 1);
        assert_eq!(completed(23)[0].0, begun[1]);
        assert_eq!(completed(26).len(), 1);
        let continuation = completed(26)[0].0;
        assert!(!begun.contains(&continuation));
        assert_eq!(turn_started_on(&per_line[25], primary), Some(continuation));
        assert_eq!(turn_completions(&on_thread(&outputs, primary)).len(), 3);
        assert!(
            turn_completions(&on_thread(&outputs, primary))
                .iter()
                .all(|(_, _, status)| status.kind == TurnStatusKind::Completed)
        );

        // The route's thread receives the sub-agent's frames, lines 10–19, and nothing of them
        // reaches the parent, busy with its own prompt.
        for (index, line) in lines.iter().enumerate().take(19).skip(9) {
            if frame_of(line)["parent_tool_use_id"].is_string() {
                assert!(on_thread(&per_line[index], primary).is_empty(), "{index}");
                assert!(!on_thread(&per_line[index], child).is_empty(), "{index}");
            }
        }
        let child_outputs = on_thread(&outputs, child);
        assert!(completed_items(&child_outputs).iter().any(|item| matches!(
            &item.payload,
            ItemPayload::ToolCall { name, status, .. }
                if name == "Read" && status.as_deref() == Some("completed")
        )));
        // It completes at line 20, and only then is its route dropped.
        let on_child = turn_completions(&on_thread(&per_line[19], child));
        assert_eq!(on_child.len(), 1);
        assert_eq!(on_child[0].2.kind, TurnStatusKind::Completed);
        assert_eq!(turn_completions(&child_outputs).len(), 1);
        assert_eq!(routes_closed(&per_line[19]), vec![child]);
        assert_eq!(routes_closed(&outputs), vec![child]);

        // The outcome row is a late completion of the delegating turn, not of "pong".
        let outcome = outcome_rows(&per_line[19]);
        assert_eq!(outcome.len(), 1);
        assert_eq!(outcome[0].0, begun[0]);
        assert_eq!(outcome[0].4.action, SubagentAction::Completed);
        assert_eq!(outcome[0].4.status, Some(SubagentStatus::Completed));
        assert!(started_items(&on_thread(&per_line[19], primary)).is_empty());
        assert_eq!(outcome_rows(&outputs).len(), 1);
        assert!(!mapper.has_routes());
        assert!(!mapper.has_tasks());
        assert_eq!(mapper.active_turn(), None);

        logs_assert(lines_with(
            1,
            &[" INFO ", r#"action="route_outlives_turn""#],
        ));
        logs_assert(a_line_with(&[
            " INFO ",
            r#"action="continuation_turn""#,
            "external=true",
        ]));
        logs_assert(a_line_with(&[
            " INFO ",
            r#"action="turn_completed""#,
            "origin=task-notification",
        ]));
        logs_assert(a_line_with(&[
            " INFO ",
            r#"action="subagent_outcome""#,
            "late=true",
        ]));
        logs_assert(no_line_with(r#"action="hold_result""#));
    }

    #[test]
    #[traced_test]
    fn a_background_sub_agent_ending_while_the_parent_is_idle_is_followed_by_its_continuation() {
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        let (per_line, begun, child) =
            drive_background(&mut mapper, "delegation-background-idle", &[0, 8], None);
        let outputs = per_line.concat();

        // "pong" completes at line 15 while the route is live.
        let pong = turn_completions(&on_thread(&per_line[14], primary));
        assert_eq!(pong.len(), 1);
        assert_eq!(pong[0].0, begun[1]);
        assert!(mapper_route_live_between(&per_line, child, 14, 82));

        // The sub-agent ends at line 83 with no parent turn active: a late row, then the drop.
        let on_child = turn_completions(&on_thread(&per_line[82], child));
        assert_eq!(on_child.len(), 1);
        assert_eq!(on_child[0].2.kind, TurnStatusKind::Completed);
        let outcome = outcome_rows(&per_line[82]);
        assert_eq!(outcome.len(), 1);
        assert_eq!(outcome[0].0, begun[0]);
        assert_eq!(outcome[0].4.status, Some(SubagentStatus::Completed));
        assert_eq!(routes_closed(&per_line[82]), vec![child]);

        // The continuation follows directly: an external turn, lines 86–88.
        let continuation = turn_completions(&on_thread(&per_line[87], primary));
        assert_eq!(continuation.len(), 1);
        assert!(!begun.contains(&continuation[0].0));
        assert_eq!(continuation[0].2.kind, TurnStatusKind::Completed);
        assert_eq!(turn_completions(&on_thread(&outputs, primary)).len(), 3);
        logs_assert(a_line_with(&[" INFO ", r#"action="continuation_turn""#]));
    }

    /// Whether `child`'s route took frames and closed no route on lines `from..to` (0-based).
    fn mapper_route_live_between(
        per_line: &[Vec<MapperOutput>],
        child: ThreadId,
        from: usize,
        to: usize,
    ) -> bool {
        let span = per_line[from..to].concat();
        routes_closed(&span).is_empty() && !on_thread(&span, child).is_empty()
    }

    #[test]
    #[traced_test]
    fn a_stopped_background_sub_agent_does_not_wait_for_a_continuation() {
        // `stop_task` 1 s after the first `result`; the user's next prompt 10 s later (line 25).
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        let (per_line, begun, child) =
            drive_background(&mut mapper, "delegation-background-stop", &[0, 24], Some(9));
        let outputs = per_line.concat();

        let first = turn_completions(&on_thread(&per_line[8], primary));
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].0, begun[0]);

        // The route ends interrupted at line 14, and is dropped there.
        let on_child = turn_completions(&on_thread(&per_line[13], child));
        assert_eq!(on_child.len(), 1);
        assert_eq!(on_child[0].2.kind, TurnStatusKind::Interrupted);
        assert_eq!(routes_closed(&per_line[13]), vec![child]);
        let outcome = outcome_rows(&per_line[13]);
        assert_eq!(outcome.len(), 1);
        assert_eq!(outcome[0].0, begun[0]);
        assert_eq!(outcome[0].4.action, SubagentAction::Interrupted);
        assert_eq!(outcome[0].4.status, Some(SubagentStatus::Interrupted));
        // The sub-agent's trailing interruption frame (line 18) is dropped.
        assert!(per_line[17].is_empty(), "{:?}", per_line[17]);

        // The continuation (lines 19–24) and the user's turn (25–27) both complete.
        let continuation = turn_completions(&on_thread(&per_line[23], primary));
        assert_eq!(continuation.len(), 1);
        assert!(!begun.contains(&continuation[0].0));
        let user = turn_completions(&on_thread(&per_line[26], primary));
        assert_eq!(user.len(), 1);
        assert_eq!(user[0].0, begun[1]);
        assert_eq!(turn_completions(&on_thread(&outputs, primary)).len(), 3);
        assert!(!mapper.has_routes());
        assert!(!mapper.has_tasks());
        logs_assert(a_line_with(&[" INFO ", r#"action="continuation_turn""#]));
    }

    #[test]
    #[traced_test]
    fn a_background_agent_ending_before_its_turn_s_result_reports_on_that_turn() {
        let lines = out_lines("delegation-background");
        let updated = position(&lines, |frame| frame["subtype"] == "task_updated");
        let notified = position(&lines, |frame| frame["subtype"] == "task_notification");
        let result = position(&lines, |frame| frame["type"] == "result");
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        let turn = TurnId::new();
        let mut outputs = mapper.begin_turn(turn, TurnKind::User);
        // The call, its task and its launch, then the task's end, all before the `result`.
        for line in &lines[..5] {
            outputs.extend(mapper.map_line(line));
        }
        let (child, ..) = routes_opened(&outputs)[0].clone();
        let ended = mapper.map_line(&lines[updated]);
        let on_parent = on_thread(&ended, primary);
        let outcome = outcome_rows(&on_parent);
        assert_eq!(outcome.len(), 1);
        assert_eq!(outcome[0].0, turn);
        // Not late: the row starts and completes on the live turn.
        let starts = started_items(&on_parent);
        assert_eq!(starts.len(), 1);
        assert_eq!(starts[0].kind, ItemKind::Activity);
        assert!(routes_closed(&ended).is_empty());
        mapper.map_line(&lines[notified]);

        // The `result` ends the turn and drops the route, whose task already ended.
        let closed = mapper.map_line(&lines[result]);
        assert_eq!(turn_completions(&on_thread(&closed, primary)).len(), 1);
        assert_eq!(routes_closed(&closed), vec![child]);
        assert!(!mapper.has_routes());
        logs_assert(no_line_with(r#"action="route_outlives_turn""#));
        logs_assert(a_line_with(&[
            " INFO ",
            r#"action="subagent_outcome""#,
            "late=false",
        ]));
    }

    #[test]
    #[traced_test]
    fn child_exit_fails_a_background_route_that_outlived_its_turn() {
        let lines = out_lines("delegation-background");
        let mut mapper = new_mapper();
        let per_line = drive_lines(&mut mapper, &lines[..12], 1, TurnKind::User, false);
        let (child, ..) = routes_opened(&per_line.concat())[0].clone();
        assert_eq!(mapper.active_turn(), None);
        assert!(mapper.has_routes());

        let outputs = mapper.child_exited("code 1");
        let on_child = turn_completions(&on_thread(&outputs, child));
        assert_eq!(on_child.len(), 1);
        assert_eq!(on_child[0].2.kind, TurnStatusKind::Failed);
        assert!(on_child[0].2.message.as_deref().unwrap().contains("code 1"));
        assert_eq!(routes_closed(&outputs), vec![child]);
        assert!(!mapper.has_routes());
        logs_assert(a_line_with(&[
            " WARN ",
            r#"action="child_exited""#,
            "route=task:",
        ]));
    }

    #[test]
    #[traced_test]
    fn a_result_with_an_unknown_origin_is_logged_and_completes_the_turn() {
        let mut mapper = new_mapper();
        let turn = TurnId::new();
        mapper.begin_turn(turn, TurnKind::User);
        let mut result = frame_of(&line_of("text-turn", "result"));
        result["origin"] = json!({"kind": "peer-message", "from": "elsewhere"});
        let completions = turn_completions(&mapper.map_line(&result.to_string()));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].0, turn);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Completed);
        logs_assert(a_line_with(&[
            " INFO ",
            r#"action="result_origin""#,
            "origin=peer-message",
        ]));
        logs_assert(no_line_with(r#"action="continuation_turn""#));
    }

    #[test]
    #[traced_test]
    fn a_superseded_turn_keeps_its_background_route() {
        let lines = out_lines("delegation-background");
        let started = position(&lines, |frame| frame["subtype"] == "task_started");
        let mut mapper = new_mapper();
        let per_line = drive_lines(&mut mapper, &lines[..=started], 1, TurnKind::User, false);
        let (child, ..) = routes_opened(&per_line.concat())[0].clone();
        let outputs = mapper.begin_turn(TurnId::new(), TurnKind::User);
        assert!(turn_completions(&on_thread(&outputs, child)).is_empty());
        assert!(routes_closed(&outputs).is_empty());
        assert!(mapper.has_routes());
        logs_assert(a_line_with(&[" INFO ", r#"action="route_outlives_turn""#]));
        logs_assert(no_line_with(r#"action="route_still_open""#));
    }

    #[test]
    #[traced_test]
    fn a_sub_agent_ask_routes_to_its_thread_by_agent_id() {
        let lines = out_lines("subagent-stop");
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        let (per_line, child) = drive_subagent(&mut mapper, "subagent-stop", true);
        let outputs = per_line.concat();

        // The ask precedes the `tool_use` block it asks for, and still lands on the route.
        let ask = position(&lines, |frame| frame["type"] == "control_request");
        let block = position(&lines, |frame| {
            frame["parent_tool_use_id"].is_string()
                && frame.pointer("/message/content/0/type") == Some(&json!("tool_use"))
        });
        assert!(ask < block);
        let child_turn = events(&on_thread(&outputs, child))
            .into_iter()
            .find_map(|event| match event {
                AgentEvent::TurnStarted { turn, .. } => Some(*turn),
                _ => None,
            })
            .unwrap();
        assert!(matches!(
            events(&per_line[ask])[..],
            [AgentEvent::ApprovalRequested { thread, turn, .. }]
                if *thread == child && *turn == child_turn
        ));
        assert!(per_line[ask].iter().any(|output| matches!(
            output,
            MapperOutput::PendingApproval { thread, .. } if *thread == child
        )));

        // After `stop_task`, the killed update ends the route as interrupted, its command too.
        let killed = position(&lines, |frame| frame["patch"]["status"] == "killed");
        let at_kill = on_thread(&per_line[killed], child);
        assert_eq!(
            command_statuses(&at_kill),
            vec![(
                "touch marker.txt && sleep 120 && cat data.txt".into(),
                Some("interrupted".into())
            )]
        );
        let completions = turn_completions(&at_kill);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].0, child_turn);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Interrupted);

        // The parent's call ends with the interruption, and its turn goes on to complete. Its
        // status comes from the `tool_result` as for any tool call: the CLI marks it
        // `non_execution_kind: interrupted`, which reads `declined`.
        let (status, error, link) = agent_completion(&on_thread(&outputs, primary));
        assert_eq!(status.as_deref(), Some("declined"));
        assert!(
            error
                .unwrap()
                .contains("[Request interrupted by user for tool use]")
        );
        assert_eq!(link.action, SubagentAction::Interrupted);
        assert_eq!(link.status, Some(SubagentStatus::Interrupted));
        let result = position(&lines, |frame| frame["type"] == "result");
        let completions = turn_completions(&on_thread(&per_line[result], primary));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Completed);
        assert_eq!(turn_completions(&on_thread(&outputs, primary)).len(), 1);
        logs_assert(no_line_with(r#"action="ask_route_unknown""#));
    }

    #[test]
    fn a_withdrawn_sub_agent_ask_is_a_cancel_for_the_route() {
        let lines = out_lines("subagent-ask-withdrawn");
        for stop in [true, false] {
            let mut mapper = new_mapper();
            let (per_line, child) = drive_subagent(&mut mapper, "subagent-ask-withdrawn", stop);
            let cancel = position(&lines, |frame| frame["type"] == "control_cancel_request");
            assert!(matches!(
                &per_line[cancel][..],
                [MapperOutput::CancelRequest { request_id }]
                    if request_id == "2bd2fa27-b607-4b4e-89ce-caad67645e58"
            ));
            let completions = turn_completions(&on_thread(&per_line.concat(), child));
            assert_eq!(completions.len(), 1);
            if stop {
                assert_eq!(completions[0].2.kind, TurnStatusKind::Interrupted);
            } else {
                assert_eq!(completions[0].2.kind, TurnStatusKind::Failed);
                assert_eq!(
                    completions[0].2.message.as_deref(),
                    Some("agent task a4e6e2839ca3bbd6a was killed")
                );
            }
        }
    }

    #[test]
    #[traced_test]
    fn an_ask_for_an_unknown_agent_attaches_to_the_primary() {
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        mapper.begin_turn(TurnId::new(), TurnKind::User);
        let outputs = mapper.map_line(
            r#"{"type":"control_request","request_id":"r1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"ls"},"tool_use_id":"toolu_z","agent_id":"a0000000000000000"}}"#,
        );
        assert!(matches!(
            events(&outputs)[..],
            [AgentEvent::ApprovalRequested { thread, .. }] if *thread == primary
        ));
        assert!(outputs.iter().any(|output| matches!(
            output,
            MapperOutput::PendingApproval { thread, .. } if *thread == primary
        )));
        assert!(logs_contain("ask_route_unknown"));
        logs_assert(a_line_with(&[
            " WARN ",
            r#"action="ask_route_unknown""#,
            "agent_id=a0000000000000000",
        ]));
    }

    #[test]
    fn a_nested_delegation_is_a_route_under_a_route() {
        let lines = out_lines("delegation");
        let mut mapper = new_mapper();
        mapper.begin_turn(TurnId::new(), TurnKind::User);
        let call = position(&lines, |frame| {
            frame.pointer("/message/content/0/name") == Some(&json!("Agent"))
        });
        let started = position(&lines, |frame| frame["subtype"] == "task_started");
        let read = position(&lines, |frame| {
            frame.pointer("/message/content/0/name") == Some(&json!("Read"))
        });
        let mut outputs = Vec::new();
        outputs.extend(mapper.map_line(&lines[call]));
        outputs.extend(mapper.map_line(&lines[started]));
        let (outer, ..) = routes_opened(&outputs)[0].clone();

        // The outer sub-agent delegates in turn: an `Agent` block on its route.
        let nested_call = rewrite(
            &rewrite(
                &rewrite(&lines[call], "/parent_tool_use_id", json!(DELEGATION_CALL)),
                "/message/content/0/id",
                json!("toolu_nested"),
            ),
            "/message/id",
            json!("msg_nested"),
        );
        let minted = mapper.map_line(&nested_call);
        let (inner, native, parent, _) = routes_opened(&minted)[0].clone();
        assert_eq!(native, "task:toolu_nested");
        assert_eq!(parent, format!("task:{DELEGATION_CALL}"));
        // The nested call is an item of the outer route, carrying the inner link.
        assert!(
            started_items(&on_thread(&minted, outer))
                .iter()
                .any(|item| item
                    .tool
                    .as_ref()
                    .and_then(|tool| tool.subagent.as_ref())
                    .is_some_and(|link| link.harness_thread_id == "task:toolu_nested"))
        );

        // Frames whose parent is the inner call land on the inner route's thread.
        let nested_read = rewrite(
            &rewrite(&lines[read], "/parent_tool_use_id", json!("toolu_nested")),
            "/message/content/0/id",
            json!("toolu_nested_read"),
        );
        let outputs = mapper.map_line(&nested_read);
        let on_inner = on_thread(&outputs, inner);
        assert!(matches!(
            events(&on_inner)[0],
            AgentEvent::TurnStarted { .. }
        ));
        assert!(
            started_items(&on_inner)
                .iter()
                .any(|item| item.tool.as_ref().is_some_and(|tool| tool.name == "Read"))
        );
        assert!(on_thread(&outputs, outer).is_empty());
    }

    #[test]
    #[traced_test]
    fn a_terminal_update_for_an_ended_route_is_ignored() {
        let lines = out_lines("delegation");
        let mut mapper = new_mapper();
        drive(&mut mapper, "delegation", TurnKind::User);
        let updated = lines[position(&lines, |frame| frame["subtype"] == "task_updated")].clone();
        let again = rewrite(&updated, "/patch/status", json!("killed"));
        assert!(mapper.map_line(&again).is_empty());
        logs_assert(a_line_with(&[
            " DEBUG ",
            "task_id=a60e08b61ab262e5b",
            "already ended task; ignored",
        ]));
    }

    #[test]
    #[traced_test]
    fn child_exit_completes_open_route_turns() {
        let lines = out_lines("delegation");
        let read = position(&lines, |frame| {
            frame.pointer("/message/content/0/name") == Some(&json!("Read"))
        });
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        let per_line = drive_lines(&mut mapper, &lines[..=read], 1, TurnKind::User, false);
        let (child, ..) = routes_opened(&per_line.concat())[0].clone();

        let outputs = mapper.child_exited("code 1");
        let on_child = turn_completions(&on_thread(&outputs, child));
        assert_eq!(on_child.len(), 1);
        assert_eq!(on_child[0].2.kind, TurnStatusKind::Failed);
        assert!(on_child[0].2.message.as_deref().unwrap().contains("code 1"));
        // The open `Read` ends with the route.
        assert!(
            completed_items(&on_thread(&outputs, child))
                .iter()
                .any(|item| matches!(
                    &item.payload,
                    ItemPayload::ToolCall { name, status, .. }
                        if name == "Read" && status.as_deref() == Some("interrupted")
                ))
        );
        let on_primary = turn_completions(&on_thread(&outputs, primary));
        assert_eq!(on_primary.len(), 1);
        assert_eq!(on_primary[0].2.kind, TurnStatusKind::Failed);
        assert_eq!(routes_closed(&outputs), vec![child]);
        assert!(!mapper.has_routes());
        logs_assert(a_line_with(&[
            " WARN ",
            r#"action="child_exited""#,
            "route=task:toolu_01DSgcYLdZqTSvfAwdnE2njN",
        ]));
        logs_assert(a_line_with(&[
            " WARN ",
            r#"action="child_exited""#,
            "Claude Code exited before the turn completed",
        ]));
        logs_assert(no_line_with(r#"action="route_still_open""#));
    }

    #[test]
    #[traced_test]
    fn a_frame_with_an_unknown_parent_is_dropped() {
        let lines = out_lines("delegation");
        let mut mapper = new_mapper();
        mapper.begin_turn(TurnId::new(), TurnKind::User);
        // No `Agent` block minted a route for these frames' parent.
        for line in lines
            .iter()
            .filter(|line| frame_of(line)["parent_tool_use_id"].is_string())
        {
            assert!(mapper.map_line(line).is_empty());
        }
        logs_assert(a_line_with(&[
            " DEBUG ",
            "no route was minted for its parent tool use",
        ]));
    }

    #[test]
    #[traced_test]
    fn route_stop_bookkeeping() {
        let lines = out_lines("subagent-stop");
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        assert_eq!(mapper.route_task_id(primary), Err(RouteLookup::NotARoute));
        mapper.begin_turn(TurnId::new(), TurnKind::User);
        let call = position(&lines, |frame| {
            frame.pointer("/message/content/0/name") == Some(&json!("Agent"))
        });
        let (child, ..) = routes_opened(&mapper.map_line(&lines[call]))[0].clone();
        // Before `task_started` there is no task to stop yet.
        assert_eq!(mapper.route_task_id(child), Ok(None));
        let started = position(&lines, |frame| frame["subtype"] == "task_started");
        mapper.map_line(&lines[started]);
        assert_eq!(
            mapper.route_task_id(child),
            Ok(Some("ada9b7fee5c0a73e9".into()))
        );
        let killed = position(&lines, |frame| frame["patch"]["status"] == "killed");
        mapper.map_line(&lines[killed]);
        assert_eq!(mapper.route_task_id(child), Err(RouteLookup::Ended));
        // A route whose turn failed without a stop names the kill.
        mapper.note_stop_sent(ThreadId::new());
        logs_assert(a_line_with(&[" WARN ", r#"action="note_stop_sent""#]));
    }

    #[test]
    #[traced_test]
    fn a_spawning_turn_that_ends_first_fails_its_running_route() {
        let lines = out_lines("delegation");
        let started = position(&lines, |frame| frame["subtype"] == "task_started");
        let mut mapper = new_mapper();
        let per_line = drive_lines(&mut mapper, &lines[..=started], 1, TurnKind::User, false);
        let (child, ..) = routes_opened(&per_line.concat())[0].clone();
        // A new turn supersedes the one whose agent task still runs.
        let outputs = mapper.begin_turn(TurnId::new(), TurnKind::User);
        let completions = turn_completions(&on_thread(&outputs, child));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Failed);
        assert_eq!(
            completions[0].2.message.as_deref(),
            Some("parent turn ended")
        );
        assert_eq!(routes_closed(&outputs), vec![child]);
        logs_assert(a_line_with(&[" WARN ", r#"action="route_still_open""#]));
        // Its agent task keeps the child busy until the CLI reports its end, which then finds no
        // route left to end.
        assert!(mapper.has_tasks());
        let updated = position(&lines, |frame| frame["subtype"] == "task_updated");
        assert!(events(&mapper.map_line(&lines[updated])).is_empty());
        assert!(!mapper.has_tasks());
    }
}
