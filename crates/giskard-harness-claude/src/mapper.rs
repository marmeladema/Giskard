//! The pure mapper from Claude Code stream-json frames to Giskard events and control replies.
//!
//! One [`ClaudeMapper`] serves one `claude` child process: one primary thread, plus the `task:`
//! sub-agent routes milestone 5 adds. Frames in, [`MapperOutput`]s out; no I/O, and no clock except
//! `Utc::now()` for timestamps.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use chrono::Utc;
use claude_codes::io::{AssistantMessage, AssistantUsage, ResultMessage, UserMessage};
use claude_codes::{
    ApiKeySource, ApiRetryMessage, CompactBoundaryMessage, ContentBlock, InitMessage,
    PermissionDeniedMessage, RateLimitEvent, RateLimitStatus, StatusMessage, TaskStartedMessage,
    TaskStatus, TaskType, TaskUpdatedMessage, ToolPermissionRequest, ToolResultBlock,
    ToolResultContent, ToolResultMeta, ToolUseBlock, UsageInfo,
};
use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalMetadata, ApprovalRequest};
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{
    CommandExecutionStart, FileChangeEntry, FileChangeKind, Item, ItemDelta, ItemKind, ItemPayload,
    ItemStart, SubagentAction, SubagentLink, SubagentStatus, ToolCallStart,
};
use giskard_core::model::ModelRef;
use giskard_core::server_request::ServerRequest;
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
use serde_json::{Value, json};
use tracing::{debug, error, info, warn};

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
/// - one sub-agent route, from its `Agent` call until its spawning turn ends or the child exits:
///   [`RouteState`] (its minted thread, its task, its link status, its own [`TurnState`]);
/// - one turn, dropped when the turn completes: [`TurnState`] (the turn id and kind, its model,
///   usage, open agent tasks, a held `result`, the interrupt flag, denied tool uses);
/// - one item within the turn, dropped with the turn: [`TurnItems`] (item ids, open text and
///   thinking blocks, open tool calls).
///
/// A new field joins the struct whose cleanup site matches its lifetime.
pub struct ClaudeMapper {
    thread: ThreadId,
    harness_thread_id: String,
    workspace_root: PathBuf,
    turn: Option<TurnState>,
    session: SessionState,
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
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Remember each Claude Code task's type, which only `task_started` carries, and the
    //   sub-agent route of a `local_agent` task, so an ask's `agent_id` or a `task_updated`
    //   resolves to its route.
    // Source of truth: `system/task_started` establishes the entry.
    // Structural reason: `task_updated` and `can_use_tool` name the task id alone; its type gates
    //   turn completion and its route receives the ask.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: A terminal `task_updated` removes the entry; dropping a route clears
    //   the entry's route; shutdown drops the rest.
    tasks: HashMap<String, TaskEntry>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: One sub-agent route per `Agent` call of this session, keyed by the call's tool-use id.
    // Source of truth: The `assistant` frame carrying the `Agent` tool_use block mints the entry.
    // Structural reason: Forwarded frames name the call (`parent_tool_use_id`), asks name the task
    //   (`agent_id`), and the server names the thread; the entry joins the three.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: Dropped by `finish_turn` of the route that spawned it once its task is
    //   terminal and its own turn has completed; `child_exited` completes and drops the rest.
    routes: HashMap<String, RouteState>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Recognise the frames that trail a dropped route (a killed sub-agent's rejection and
    //   interruption marker), so they are dropped as trailing frames, with the route's thread.
    // Source of truth: Dropping a route records its tool-use id and thread.
    // Structural reason: The CLI emits those frames after the terminal `task_updated`, which may
    //   be after the spawning turn ended and dropped the route.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: Session lifetime (one entry per delegation); `child_exited` clears it.
    ended_routes: HashMap<String, ThreadId>,
    /// `(type, subtype)` pairs of unknown frames already logged at `warn`.
    unknown_seen: HashSet<(String, Option<String>)>,
}

/// One Claude Code task the session reported.
struct TaskEntry {
    kind: TaskType,
    /// The sub-agent route of a `local_agent` task whose `tool_use_id` names a minted route.
    route: Option<String>,
}

/// One sub-agent route: an `Agent` call of this session and the thread its frames map onto.
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
    /// The route's turn ended (terminal task, child exit, or its spawning turn ended).
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
    /// The model the adapter asked for on this turn, through `note_turn_model`.
    model: Option<ModelRef>,
    /// The latest single API request's usage, which is what occupies the context window now: a
    /// `message_delta`'s, then the last `result.usage.iterations` entry. `TurnUsageUpdated`
    /// carries it.
    window_usage: TokenUsage,
    /// The sum of every `result.usage` of the turn, which `TurnCompleted` carries. A held result
    /// and the CLI's continuation result are two halves of one turn, so they add up.
    result_usage: Option<TokenUsage>,
    /// Last `(usage, context_window)` pair emitted as `TurnUsageUpdated`, for dedup.
    emitted_usage: Option<(TokenUsage, Option<u32>)>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Gate turn completion on the `local_agent` tasks the turn started.
    // Source of truth: `task_started` adds a task, a terminal `task_updated` removes it.
    // Structural reason: A backgrounded delegation's first `result` is not the turn's end.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: Terminal task updates remove entries; turn completion drops the set.
    open_agent_tasks: HashSet<String>,
    /// A `result` that arrived while agent tasks were open.
    held_result: Option<Box<ResultMessage>>,
    /// The adapter sent `interrupt` (or a deny with `interrupt: true`) during this turn.
    interrupt_sent: bool,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Mark tool uses whose ask was answered deny, so their completion reads `declined`.
    // Source of truth: The mapper's own `ExitPlanMode` denial and `system/permission_denied`.
    // Structural reason: The `tool_result` of a denial does not always say it was one.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: Turn completion drops the set.
    denied_tool_use_ids: HashSet<String>,
    /// The CLI replayed this turn's prompt (`isReplay`), which became its `UserMessage` item.
    prompt_acknowledged: bool,
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
            window_usage: TokenUsage::default(),
            result_usage: None,
            emitted_usage: None,
            open_agent_tasks: HashSet::new(),
            held_result: None,
            interrupt_sent: false,
            denied_tool_use_ids: HashSet::new(),
            prompt_acknowledged: false,
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
        }
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
        if self.session.ended_routes.values().any(|t| *t == thread) {
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
            frame @ (Frame::TaskNotification(_)
            | Frame::SessionTitleChanged(_)
            | Frame::SystemIgnored { .. }) => {
                let (frame_type, subtype) = frame.kind();
                debug!(
                    thread_id = %self.thread,
                    harness_thread_id = %self.harness_thread_id,
                    turn_id = display_opt(self.active_turn()),
                    frame_type,
                    frame_subtype = display_opt(subtype),
                    task_id = display_opt(match &frame {
                        Frame::TaskNotification(task) => Some(task.task_id.as_str()),
                        _ => None,
                    }),
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
        let gates = matches!(task_type, TaskType::LocalAgent);
        // A `local_agent` task names the `Agent` call that minted its route.
        let route = task
            .tool_use_id
            .as_ref()
            .filter(|id| gates && self.session.routes.contains_key(*id))
            .cloned();
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
            gates_turn = gates,
            "Claude Code task started"
        );
        self.session.tasks.insert(
            task.task_id.clone(),
            TaskEntry {
                kind: task_type,
                route: route.clone(),
            },
        );
        if let Some(id) = &route {
            self.start_route(id, &task, out);
        }
        if !gates {
            return;
        }
        match self.turn.as_mut() {
            Some(turn) => {
                turn.open_agent_tasks.insert(task.task_id);
            }
            None => warn!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                task_id = %task.task_id,
                "an agent task started with no active turn; it cannot hold one"
            ),
        }
    }

    /// `task_started` of a route's agent task: record the task and open the route's turn.
    fn start_route(&mut self, id: &str, task: &TaskStartedMessage, out: &mut Out) {
        let Some(route) = self.session.routes.get_mut(id) else {
            return;
        };
        route.task_id = Some(task.task_id.clone());
        route.is_backgrounded = task.is_backgrounded == Some(true);
        if route.prompt.is_none() {
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
        let terminal = matches!(
            status,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Killed | TaskStatus::Stopped
        );
        let entry = if terminal {
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
            "Claude Code task updated"
        );
        if !terminal {
            return;
        }
        match entry {
            Some(TaskEntry {
                route: Some(id), ..
            }) => self.end_route(&id, &task, &status, out),
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
        let Some(turn) = self.turn.as_mut() else {
            return;
        };
        if !turn.open_agent_tasks.remove(&task.task_id)
            || !turn.open_agent_tasks.is_empty()
            || turn.held_result.is_none()
        {
            return;
        }
        if matches!(status, TaskStatus::Completed) {
            // The CLI runs a continuation turn after a completed background agent; its `result`
            // completes the turn.
            debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = %turn.id,
                task_id = %task.task_id,
                action = "hold_result",
                "last agent task completed; holding the turn for the continuation result"
            );
            return;
        }
        // A killed, failed or stopped agent task produces no second `result`: finish now.
        let interrupted = turn.interrupt_sent;
        turn.held_result = None;
        info!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = %turn.id,
            task_id = %task.task_id,
            status = %status,
            interrupted,
            action = "release_held_result",
            "last agent task ended without completing; finishing the held turn"
        );
        let status = if interrupted {
            TurnStatus {
                kind: TurnStatusKind::Interrupted,
                message: None,
            }
        } else {
            TurnStatus {
                kind: TurnStatusKind::Failed,
                message: Some(
                    task.patch
                        .error
                        .unwrap_or_else(|| format!("agent task {} ended {status}", task.task_id)),
                ),
            }
        };
        self.emit_usage(&Route::Primary, out);
        self.finish_turn(status, out);
    }

    // ---- turns ---------------------------------------------------------------------------------

    /// The active turn, or a turn the CLI started on its own (a continuation after a background
    /// task), which the mapper opens itself.
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
        self.turn = Some(TurnState::new(turn, TurnKind::User));
        out.push(self.event(AgentEvent::TurnStarted {
            thread: self.thread,
            turn,
        }));
        turn
    }

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
        if !turn.open_agent_tasks.is_empty() {
            info!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = %turn_id,
                action = "hold_result",
                open_agent_tasks = turn.open_agent_tasks.len(),
                "result arrived while agent tasks are open; holding the turn"
            );
            turn.held_result = Some(result);
            return;
        }
        let status = result_status(&result, turn.interrupt_sent);
        self.emit_usage(&Route::Primary, out);
        self.finish_turn(status, out);
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
        let window = self.session.context_window();
        let thread = self.thread_of(route);
        let Some(turn) = self.turn_of_mut(route) else {
            return;
        };
        let pair = (turn.window_usage, window);
        if turn.emitted_usage == Some(pair) {
            return;
        }
        turn.emitted_usage = Some(pair);
        let event = AgentEvent::TurnUsageUpdated {
            thread,
            turn: turn.id,
            usage: turn.window_usage,
            context_window: window,
            model: turn.model.clone(),
        };
        out.push(MapperOutput::Event(event));
    }

    /// Complete the active turn and drop its state.
    fn finish_turn(&mut self, status: TurnStatus, out: &mut Out) {
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
        // The primary turn's gate holds it until every agent task it started is terminal, so the
        // routes it spawned have normally ended by now.
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
        let parent_harness_thread_id = match parent {
            Route::Primary => self.harness_thread_id.clone(),
            Route::Task(outer) => format!("{TASK_ID_PREFIX}{outer}"),
        };
        let route = RouteState {
            thread: ThreadId::new(),
            harness_thread_id: format!("{TASK_ID_PREFIX}{tool_use_id}"),
            parent: parent.clone(),
            prompt: text("prompt"),
            prompt_delivered: false,
            subagent_type: text("subagent_type"),
            task_id: None,
            is_backgrounded: input.get("run_in_background").and_then(Value::as_bool) == Some(true),
            stop_sent: false,
            call_open: true,
            status: SubagentStatus::Pending,
            description: text("description"),
            turn: None,
            counted_messages: HashSet::new(),
        };
        info!(
            thread_id = %route.thread,
            harness_thread_id = %self.harness_thread_id,
            route = %route.harness_thread_id,
            parent_harness_thread_id = %parent_harness_thread_id,
            turn_id = display_opt(self.turn_of(parent).map(|turn| turn.id)),
            subagent_type = display_opt(route.subagent_type.as_deref()),
            action = "route_opened",
            "sub-agent route minted"
        );
        out.push(MapperOutput::RouteOpened {
            thread: route.thread,
            harness_thread_id: route.harness_thread_id.clone(),
            parent_harness_thread_id,
            agent_name: route.description.clone(),
        });
        let link = route.link(SubagentAction::Spawned);
        self.session.routes.insert(tool_use_id.to_owned(), route);
        Some(link)
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

    /// A terminal `task_updated` of route `id`'s task: its turn ends now, and a backgrounded
    /// delegation's outcome is reported on the spawning route.
    fn end_route(
        &mut self,
        id: &str,
        task: &TaskUpdatedMessage,
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
                task_id = %task.task_id,
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
        let (subagent, turn_status) =
            match status {
                TaskStatus::Completed => (
                    SubagentStatus::Completed,
                    TurnStatus {
                        kind: TurnStatusKind::Completed,
                        message: None,
                    },
                ),
                TaskStatus::Killed if interrupted => (
                    SubagentStatus::Interrupted,
                    TurnStatus {
                        kind: TurnStatusKind::Interrupted,
                        message: None,
                    },
                ),
                TaskStatus::Killed => (
                    SubagentStatus::Failed,
                    TurnStatus {
                        kind: TurnStatusKind::Failed,
                        message: Some(format!("agent task {} was killed", task.task_id)),
                    },
                ),
                other => (
                    SubagentStatus::Failed,
                    TurnStatus {
                        kind: TurnStatusKind::Failed,
                        message: Some(task.patch.error.clone().unwrap_or_else(|| {
                            format!("agent task {} ended {other}", task.task_id)
                        })),
                    },
                ),
            };
        self.finish_route_turn(id, subagent, turn_status, out);
        self.report_background_outcome(id, task, out);
    }

    /// A route whose parent's `Agent` call already completed (a backgrounded delegation) reports
    /// its outcome as one `Activity` on the spawning route, carrying the link.
    fn report_background_outcome(&mut self, id: &str, task: &TaskUpdatedMessage, out: &mut Out) {
        let Some(route) = self.session.routes.get(id) else {
            return;
        };
        if route.call_open {
            return;
        }
        let parent = route.parent.clone();
        let title = route
            .description
            .clone()
            .unwrap_or_else(|| "Sub-agent".to_owned());
        let detail = match route.status {
            SubagentStatus::Completed => "completed".to_owned(),
            SubagentStatus::Interrupted => "killed".to_owned(),
            _ => task
                .patch
                .error
                .clone()
                .unwrap_or_else(|| "failed".to_owned()),
        };
        let link = route.link(route.outcome_action());
        let Some(turn) = self.turn_of(&parent).map(|turn| turn.id) else {
            debug!(
                thread_id = %self.thread_of(&parent),
                harness_thread_id = %self.harness_thread_id,
                route = %link.harness_thread_id,
                task_id = %task.task_id,
                action = "subagent_outcome",
                "no active turn on the spawning thread; the sub-agent's outcome has no row"
            );
            return;
        };
        let thread = self.thread_of(&parent);
        let id = ItemId::new();
        let harness_item_id = format!("task_updated:{}", task.task_id);
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
            let payload = interrupted_payload(open.call, &self.workspace_root, link);
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

    /// The turn of `parent` ended: drop the routes it spawned. A route still running here (an
    /// agent task the gate let through because the turn failed or was superseded) is completed
    /// `Failed` first.
    fn drop_spawned_routes(&mut self, parent: &Route, out: &mut Out) {
        let mut spawned: Vec<(String, ThreadId)> = self
            .session
            .routes
            .iter()
            .filter(|(_, route)| route.parent == *parent)
            .map(|(id, route)| (id.clone(), route.thread))
            .collect();
        spawned.sort_by(|(left, _), (right, _)| left.cmp(right));
        for (id, thread) in spawned {
            let open = self
                .session
                .routes
                .get(&id)
                .is_some_and(|route| !route.ended());
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
        self.drop_spawned_routes(&Route::Task(id.to_owned()), out);
        let Some(route) = self.session.routes.remove(id) else {
            return;
        };
        for entry in self.session.tasks.values_mut() {
            if entry.route.as_deref() == Some(id) {
                entry.route = None;
            }
        }
        self.session
            .ended_routes
            .insert(id.to_owned(), route.thread);
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
            Some(thread) => debug!(
                thread_id = %thread,
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
        let Some(route) = self.route(message.parent_tool_use_id.as_deref(), "assistant") else {
            return;
        };
        let Some(turn) = self.frame_turn(&route, "assistant", out) else {
            return;
        };
        let message_id = message.message.id;
        if let Route::Task(id) = &route {
            // Forwarded frames carry no stream events: usage comes from each API message.
            self.note_route_usage(id, &message_id, message.message.usage.as_ref(), out);
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
        let call = classify_tool(&tool_use.name, tool_use.input);
        let item = match &call {
            ToolKind::Command { command } => ItemStart {
                id,
                harness_item_id: tool_use.id.clone(),
                kind: ItemKind::CommandExecution,
                command: Some(CommandExecutionStart {
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
            state
                .items
                .tools
                .insert(tool_use.id.clone(), OpenTool { item_id: id, call });
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
        // The two flags are independent: a replay (`--replay-user-messages`) echoes a line the
        // adapter wrote, a synthetic frame (a compaction summary) is the CLI's own.
        let synthetic = message.is_synthetic == Some(true);
        let replay = message.is_replay == Some(true);
        if replay && !synthetic && route == Route::Primary && results == 0 {
            self.on_replayed_prompt(turn, message, out);
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

    /// A user frame the CLI replayed from stdin on the primary route. In a user turn the first
    /// one is the prompt, acknowledged: the turn's `UserMessage`. In a compaction turn it is the
    /// `/compact` command's stdout, bookkeeping.
    fn on_replayed_prompt(&mut self, turn: TurnId, message: UserMessage, out: &mut Out) {
        let frame_uuid = message.uuid.as_deref().unwrap_or("unknown").to_owned();
        let Some(state) = self.turn_of_mut(&Route::Primary) else {
            return;
        };
        match (state.kind, state.prompt_acknowledged) {
            (TurnKind::Compaction, _) => {
                debug!(
                    thread_id = %self.thread,
                    harness_thread_id = %self.harness_thread_id,
                    turn_id = %turn,
                    frame_uuid = %frame_uuid,
                    action = "compaction_replay",
                    "skipping the replayed output of /compact"
                );
            }
            (TurnKind::User, true) => {
                warn!(
                    thread_id = %self.thread,
                    harness_thread_id = %self.harness_thread_id,
                    turn_id = %turn,
                    frame_uuid = %frame_uuid,
                    action = "prompt_acknowledged",
                    "a second replayed user message in one turn; dropping it"
                );
            }
            (TurnKind::User, false) => {
                state.prompt_acknowledged = true;
                // The adapter writes one text block (after any attachment block); the join is
                // defensive, and an attachments-only message yields "".
                let text = message
                    .message
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        ContentBlock::Text(text) => Some(text.text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                debug!(
                    thread_id = %self.thread,
                    harness_thread_id = %self.harness_thread_id,
                    turn_id = %turn,
                    frame_uuid = %frame_uuid,
                    bytes = text.len(),
                    action = "prompt_acknowledged",
                    "the CLI replayed the turn's prompt"
                );
                let harness_item_id = format!("user:{frame_uuid}:0");
                self.user_message(&Route::Primary, turn, harness_item_id, text, out);
            }
        }
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

    /// The user message of a turn: a primary turn's replayed prompt, or a sub-agent's delegated
    /// prompt.
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
        let payload = match open.call {
            ToolKind::Command { command } => ItemPayload::CommandExecution {
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
    workspace_root: &std::path::Path,
    subagent: Option<SubagentLink>,
) -> ItemPayload {
    const INTERRUPTED: &str = "interrupted";
    match call {
        ToolKind::Command { command } => ItemPayload::CommandExecution {
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
        assert_eq!(commands[0].cwd, WORKSPACE);
        assert_eq!(commands[0].status.as_deref(), Some("in_progress"));

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
    fn a_backgrounded_delegation_holds_the_turn_until_the_task_is_terminal() {
        let lines = out_lines("delegation-interrupted");
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        let per_line = drive(&mut mapper, "delegation-interrupted", TurnKind::User);
        let result_line = lines
            .iter()
            .position(|line| line.contains(r#""type": "result""#))
            .unwrap();
        assert!(turn_completions(&on_thread(&per_line[result_line], primary)).is_empty());

        let killed_line = lines
            .iter()
            .position(|line| line.contains(r#""status": "killed""#))
            .unwrap();
        let completions = turn_completions(&on_thread(&per_line[killed_line], primary));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Interrupted);

        assert_eq!(
            turn_completions(&on_thread(&per_line.concat(), primary)).len(),
            1
        );
    }

    #[test]
    fn a_killed_agent_task_without_an_interrupt_fails_the_held_turn() {
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
        let completions = turn_completions(&on_thread(&outputs, primary));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Failed);
        assert!(
            completions[0]
                .2
                .message
                .as_deref()
                .is_some_and(|message| message.contains("killed"))
        );
    }

    #[test]
    fn a_completed_background_agent_keeps_the_turn_for_the_continuation_result() {
        let mut mapper = new_mapper();
        let turn = TurnId::new();
        mapper.begin_turn(turn, TurnKind::User);
        mapper.map_line(r#"{"type":"system","subtype":"task_started","session_id":"s","task_id":"a1","task_type":"local_agent","is_backgrounded":true,"description":"d","uuid":"u1"}"#);
        let result = line_of("text-turn", "result");
        assert!(turn_completions(&mapper.map_line(&result)).is_empty());
        let updated = mapper.map_line(r#"{"type":"system","subtype":"task_updated","session_id":"s","task_id":"a1","patch":{"status":"completed"},"uuid":"u2"}"#);
        assert!(turn_completions(&updated).is_empty());
        assert_eq!(mapper.active_turn(), Some(turn));
        let outputs = mapper.map_line(&result);
        let completions = turn_completions(&outputs);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].0, turn);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Completed);
        // The held result and the continuation are two halves of one turn: their usage adds up,
        // while the window still holds only the continuation's last request.
        assert_eq!(completions[0].1, TokenUsage::new(2 * 21_970, 2 * 53));
        assert_eq!(
            usage_updates(&outputs).last().map(|(usage, _)| *usage),
            Some(TokenUsage::new(21_970, 53))
        );
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
        assert_eq!(
            completed_items(&outputs)
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
    fn a_replayed_prompt_is_the_turns_user_message() {
        let outputs = run_fixture("replay-ack", TurnKind::User);
        let sent = sent_texts("replay-ack");
        let turns = per_turn(&outputs);
        assert_eq!(turns.len(), 2);
        for (turn, sent) in turns.iter().zip(&sent) {
            let messages = user_messages(turn);
            assert_eq!(messages.len(), 1, "{turn:?}");
            assert_eq!(
                messages[0].payload,
                ItemPayload::UserMessage { text: sent.clone() }
            );
            assert!(messages[0].harness_item_id.starts_with("user:"));
            // After `TurnStarted`, before the turn's first agent message or tool call.
            let at = |wanted: &dyn Fn(&AgentEvent) -> bool| turn.iter().position(|e| wanted(e));
            let started = at(&|event| {
                matches!(event, AgentEvent::ItemStarted { item, .. }
                    if item.kind == ItemKind::UserMessage)
            })
            .unwrap();
            let first_output = at(&|event| {
                matches!(event, AgentEvent::ItemStarted { item, .. }
                    if matches!(item.kind, ItemKind::AgentMessage | ItemKind::ToolCall
                        | ItemKind::CommandExecution))
            })
            .unwrap();
            assert!(started > 0 && started < first_output, "{turn:?}");
        }
        // The second turn's `tool_result` frame completes the `Bash` call and adds no message.
        assert_eq!(
            command_statuses(&outputs),
            vec![("cat data.txt".to_owned(), Some("completed".to_owned()))]
        );
        logs_assert(lines_with(
            2,
            &[" DEBUG ", r#"action="prompt_acknowledged""#],
        ));
        logs_assert(no_line_with(" WARN "));
    }

    #[test]
    #[traced_test]
    fn an_attachment_replay_yields_the_text_only() {
        let outputs = run_fixture("replay-image", TurnKind::User);
        let items = completed_items(&outputs);
        let messages: Vec<_> = items
            .iter()
            .filter(|item| matches!(item.payload, ItemPayload::UserMessage { .. }))
            .collect();
        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].payload,
            ItemPayload::UserMessage {
                text: "Say pong.".into()
            }
        );
        assert!(
            !items
                .iter()
                .any(|item| matches!(item.payload, ItemPayload::Activity { .. }))
        );
        logs_assert(no_line_with("skipping a user block"));
    }

    #[test]
    #[traced_test]
    fn a_compaction_replay_is_not_a_user_message() {
        let outputs = run_fixture("replay-compact", TurnKind::Compaction);
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
        logs_assert(lines_with(1, &[" DEBUG ", r#"action="compaction_replay""#]));
    }

    #[test]
    #[traced_test]
    fn a_second_replay_in_a_turn_is_dropped() {
        // The first turn of `replay-ack`, its replayed prompt (line 2) emitted twice.
        let lines = out_lines("replay-ack");
        let end = lines
            .iter()
            .position(|line| line.contains(r#""type": "result""#))
            .unwrap();
        let mut turn: Vec<String> = lines[..=end].to_vec();
        assert!(turn[1].contains(r#""isReplay": true"#));
        turn.insert(2, turn[1].clone());
        let outputs: Vec<MapperOutput> =
            drive_lines(&mut new_mapper(), &turn, 1, TurnKind::User, false)
                .into_iter()
                .flatten()
                .collect();
        assert_eq!(user_messages(&events(&outputs)).len(), 1);
        logs_assert(lines_with(
            1,
            &[" WARN ", "a second replayed user message in one turn"],
        ));
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
        let model = ModelRef {
            provider: "anthropic".into(),
            model: "claude-haiku-4-5-20251001".into(),
            reasoning_effort: None,
        };
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
    fn has_tasks_is_true_from_task_started_to_the_terminal_update() {
        for (name, kind) in [
            ("background-bash", "local_bash"),
            ("delegation", "local_agent"),
        ] {
            let lines = out_lines(name);
            let started = lines
                .iter()
                .position(|line| line.contains(r#""subtype": "task_started""#))
                .unwrap();
            let terminal = lines
                .iter()
                .rposition(|line| line.contains(r#""subtype": "task_updated""#))
                .unwrap();
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
            assert!(
                !mapper.has_tasks(),
                "{kind}: after the terminal task_updated"
            );
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

    #[test]
    #[traced_test]
    fn a_backgrounded_delegation_reports_its_outcome_on_the_parent() {
        let lines = out_lines("delegation-interrupted");
        let mut mapper = new_mapper();
        let primary = mapper.thread;
        let per_line = drive(&mut mapper, "delegation-interrupted", TurnKind::User);
        let outputs = per_line.concat();
        let (child, ..) = routes_opened(&outputs)[0].clone();

        // The call completes at launch, its link still running.
        let (status, _, link) = agent_completion(&on_thread(&outputs, primary));
        assert_eq!(status.as_deref(), Some("completed"));
        assert_eq!(link.action, SubagentAction::Started);
        assert_eq!(link.status, Some(SubagentStatus::Running));

        // The killed update ends the route at once: the primary sent the interrupt.
        let killed = position(&lines, |frame| frame["patch"]["status"] == "killed");
        let completions = turn_completions(&on_thread(&per_line[killed], child));
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Interrupted);
        assert_eq!(turn_completions(&on_thread(&outputs, child)).len(), 1);

        // Its outcome is a row on the parent, with the link.
        let outcome: Vec<_> = completed_items(&on_thread(&per_line[killed], primary))
            .into_iter()
            .filter_map(|item| match &item.payload {
                ItemPayload::Activity {
                    title,
                    detail,
                    subagent: Some(link),
                    ..
                } => Some((
                    item.harness_item_id.clone(),
                    title.clone(),
                    detail.clone(),
                    link.clone(),
                )),
                _ => None,
            })
            .collect();
        assert_eq!(outcome.len(), 1);
        assert_eq!(outcome[0].0, "task_updated:a562b8d9494720fd6");
        assert_eq!(outcome[0].1, "Run bash command and report magic number");
        assert_eq!(outcome[0].2.as_deref(), Some("killed"));
        assert_eq!(outcome[0].3.action, SubagentAction::Interrupted);
        assert_eq!(outcome[0].3.status, Some(SubagentStatus::Interrupted));

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
        assert_eq!(primary_completions[0].2.kind, TurnStatusKind::Interrupted);
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
    }
}
