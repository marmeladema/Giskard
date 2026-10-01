//! The pure mapper from Claude Code stream-json frames to Giskard events and control replies.
//!
//! One [`ClaudeMapper`] serves one `claude` child process: one primary thread, plus the `task:`
//! sub-agent routes milestone 5 adds. Frames in, [`MapperOutput`]s out; no I/O, and no clock except
//! `Utc::now()` for timestamps.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use chrono::Utc;
use claude_codes::io::{AssistantMessage, ResultMessage, UserMessage};
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
    ItemStart, ToolCallStart,
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

/// Where a frame's items belong: the child process's own thread, or a sub-agent route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Route {
    Primary,
    /// A sub-agent route, keyed by its `Agent` call's tool-use id. Milestone 5 claims these; no
    /// code in milestone 1 creates one, so a frame with a `parent_tool_use_id` is dropped.
    Task,
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
    },
    /// A server request the adapter must remember until `respond_server_request` answers it.
    PendingServerRequest {
        id: ServerRequestId,
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
}

/// Maps one child's frames onto `giskard-core` events.
///
/// State is grouped by lifetime, not by key type:
/// - one session, as long as the mapper lives: [`SessionState`] (the session model and permission
///   mode, the effective window, task types, the notices and unknown frames already reported);
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
    // Role: Remember each Claude Code task's type, which only `task_started` carries.
    // Source of truth: `system/task_started` establishes the entry.
    // Structural reason: `task_updated` names the task id alone; its type gates turn completion.
    // Synchronization: The single adapter task that owns the child process owns the mapper.
    // Invalidation/removal: A terminal `task_updated` removes the entry; shutdown drops the rest.
    task_kinds: HashMap<String, TaskType>,
    /// `(type, subtype)` pairs of unknown frames already logged at `warn`.
    unknown_seen: HashSet<(String, Option<String>)>,
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
    /// item as `declined`.
    pub fn note_denied(&mut self, tool_use_id: &str) {
        match self.turn.as_mut() {
            Some(turn) => {
                turn.denied_tool_use_ids.insert(tool_use_id.to_owned());
            }
            None => warn!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                action = "note_denied",
                native_item_id = %tool_use_id,
                "a denial noted with no active turn"
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

    /// The child process ended (`exit` reads `code 1` or `signal 9`) while a turn may be active.
    ///
    /// No `result` will ever arrive for that turn, so it completes here: `Interrupted` when the
    /// adapter sent an interrupt, else `Failed`, with a message naming the exit. With no active
    /// turn there is nothing to complete.
    pub fn child_exited(&mut self, exit: &str) -> Vec<MapperOutput> {
        let mut out = Vec::new();
        let Some(turn) = self.turn.as_ref() else {
            debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                action = "child_exited",
                exit,
                "Claude Code exited with no active turn"
            );
            return out;
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
            &mut out,
        );
        out
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
            Frame::TaskStarted(task) => self.on_task_started(task),
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
        if let Some(turn) = self.turn.as_mut() {
            turn.denied_tool_use_ids.insert(denied.tool_use_id);
        }
        out.push(self.event(AgentEvent::Notice {
            thread: self.thread,
            turn: self.active_turn(),
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

    fn on_task_started(&mut self, task: TaskStartedMessage) {
        let task_type = task
            .task_type
            .clone()
            .unwrap_or_else(|| TaskType::Unknown(String::new()));
        let gates = matches!(task_type, TaskType::LocalAgent);
        info!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = display_opt(self.active_turn()),
            action = "task_started",
            task_id = %task.task_id,
            task_type = %task_type,
            is_backgrounded = display_opt(task.is_backgrounded),
            native_item_id = display_opt(task.tool_use_id.as_deref()),
            gates_turn = gates,
            "Claude Code task started"
        );
        self.session
            .task_kinds
            .insert(task.task_id.clone(), task_type);
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

    fn on_task_updated(&mut self, task: TaskUpdatedMessage, out: &mut Out) {
        let Some(status) = task.patch.status else {
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
        let task_type = if terminal {
            self.session.task_kinds.remove(&task.task_id)
        } else {
            self.session.task_kinds.get(&task.task_id).cloned()
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
        self.emit_usage(out);
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
        self.emit_usage(out);
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

    /// Emit `TurnUsageUpdated` for the active turn unless the same `(usage, window)` pair was the
    /// last one emitted.
    fn emit_usage(&mut self, out: &mut Out) {
        let window = self.session.context_window();
        let Some(turn) = self.turn.as_mut() else {
            return;
        };
        let pair = (turn.window_usage, window);
        if turn.emitted_usage == Some(pair) {
            return;
        }
        turn.emitted_usage = Some(pair);
        let event = AgentEvent::TurnUsageUpdated {
            thread: self.thread,
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

    // ---- routing -------------------------------------------------------------------------------

    /// The route a frame with this `parent_tool_use_id` belongs to, or `None` when it must be
    /// dropped. A frame without one is the primary thread's. No code in this milestone claims a
    /// sub-agent route, so a frame with one is dropped: attributing it to the primary thread would
    /// render a sub-agent's tool calls as the main agent's.
    fn route(&self, parent_tool_use_id: Option<&str>, frame_type: &str) -> Option<Route> {
        let Some(parent) = parent_tool_use_id else {
            return Some(Route::Primary);
        };
        debug!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = display_opt(self.active_turn()),
            parent_tool_use_id = %parent,
            task_native_id = %format_args!("{TASK_ID_PREFIX}{parent}"),
            frame_type,
            "dropping a sub-agent frame: no route is claimed for its parent tool use"
        );
        None
    }

    fn is_primary(&self, parent_tool_use_id: Option<&str>, frame_type: &str) -> bool {
        self.route(parent_tool_use_id, frame_type) == Some(Route::Primary)
    }

    // ---- assistant, stream and user frames -----------------------------------------------------

    fn on_assistant(&mut self, message: AssistantMessage, out: &mut Out) {
        if !self.is_primary(message.parent_tool_use_id.as_deref(), "assistant") {
            return;
        }
        let turn = self.ensure_turn("assistant", out);
        let message_id = message.message.id;
        let Some(state) = self.turn.as_mut() else {
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
                    self.complete_block(turn, key, ItemKind::AgentMessage, text.text, out)
                }
                ContentBlock::Thinking(thinking) => {
                    self.complete_block(turn, key, ItemKind::Reasoning, thinking.thinking, out)
                }
                ContentBlock::ToolUse(tool_use) => self.start_tool(turn, tool_use, out),
                other => debug!(
                    thread_id = %self.thread,
                    harness_thread_id = %self.harness_thread_id,
                    turn_id = %turn,
                    native_item_id = %key.harness_item_id(),
                    block_type = content_block_type(&other),
                    "skipping an assistant block this adapter does not map"
                ),
            }
        }
    }

    fn on_stream(&mut self, stream: StreamEvent, out: &mut Out) {
        if !self.is_primary(stream.parent_tool_use_id.as_deref(), "stream_event") {
            return;
        }
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
                        self.open_block(turn, key, ItemKind::AgentMessage, true, out);
                    }
                    BlockStart::Thinking => {
                        self.open_block(turn, key, ItemKind::Reasoning, false, out);
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
                self.open_block(turn, key.clone(), kind, true, out);
                let item_id = self.resolve_item(&key);
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
                    self.emit_usage(out);
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
        turn: TurnId,
        key: NativeItemKey,
        kind: ItemKind,
        start: bool,
        out: &mut Out,
    ) {
        let Some(state) = self.turn.as_mut() else {
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
        let id = self.resolve_item(&key);
        out.push(self.event(AgentEvent::ItemStarted {
            thread: self.thread,
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
        turn: TurnId,
        key: NativeItemKey,
        kind: ItemKind,
        text: String,
        out: &mut Out,
    ) {
        let open = self
            .turn
            .as_mut()
            .and_then(|state| state.items.blocks.remove(&key));
        let started = open.as_ref().is_some_and(|block| block.started);
        let text = match open {
            Some(block) if text.is_empty() => block.streamed,
            _ => text,
        };
        if kind == ItemKind::Reasoning && text.is_empty() && !started {
            debug!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = %turn,
                native_item_id = %key.harness_item_id(),
                "skipping an empty thinking block"
            );
            return;
        }
        let id = self.resolve_item(&key);
        if !started {
            out.push(self.event(AgentEvent::ItemStarted {
                thread: self.thread,
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
            thread: self.thread,
            turn,
            item: Item {
                id,
                harness_item_id: key.harness_item_id(),
                payload,
                created_at: Utc::now(),
            },
        }));
    }

    fn start_tool(&mut self, turn: TurnId, tool_use: ToolUseBlock, out: &mut Out) {
        let key = NativeItemKey::ToolUse(tool_use.id.clone());
        if self
            .turn
            .as_ref()
            .is_some_and(|state| state.items.tools.contains_key(&tool_use.id))
        {
            warn!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = %turn,
                native_item_id = %tool_use.id,
                tool_name = %tool_use.name,
                "skipping a repeated tool_use block for an open tool call"
            );
            return;
        }
        let id = self.resolve_item(&key);
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
                    // Milestone 5 links an `Agent` call to its sub-agent thread.
                    subagent: None,
                    started_at_ms,
                }),
            },
        };
        if let Some(state) = self.turn.as_mut() {
            state
                .items
                .tools
                .insert(tool_use.id.clone(), OpenTool { item_id: id, call });
        }
        debug!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
            turn_id = %turn,
            native_item_id = %tool_use.id,
            tool_name = %tool_use.name,
            "tool call started"
        );
        out.push(self.event(AgentEvent::ItemStarted {
            thread: self.thread,
            turn,
            item,
        }));
    }

    fn on_user(&mut self, message: UserMessage, out: &mut Out) {
        if !self.is_primary(message.parent_tool_use_id.as_deref(), "user") {
            return;
        }
        let Some(turn) = self.active_turn() else {
            warn!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                frame_uuid = display_opt(message.uuid.as_deref()),
                "skipping a user frame with no active turn"
            );
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
        // A synthetic or replayed user message (a compaction summary, a slash command's output) is
        // the CLI's bookkeeping, not something the user or the agent said in this turn.
        let bookkeeping = message.is_synthetic == Some(true) || message.is_replay == Some(true);
        for (index, block) in message.message.content.into_iter().enumerate() {
            match block {
                ContentBlock::ToolResult(result) => {
                    self.complete_tool(turn, result, tool_use_result, meta, out)
                }
                ContentBlock::Text(text) if results == 0 && !bookkeeping => {
                    let harness_item_id = format!(
                        "user:{}:{index}",
                        message.uuid.as_deref().unwrap_or("unknown")
                    );
                    self.activity(turn, harness_item_id, text.text, out);
                }
                other => debug!(
                    thread_id = %self.thread,
                    harness_thread_id = %self.harness_thread_id,
                    turn_id = %turn,
                    frame_uuid = display_opt(message.uuid.as_deref()),
                    block_type = content_block_type(&other),
                    synthetic = bookkeeping,
                    "skipping a user block this adapter does not map"
                ),
            }
        }
    }

    /// An `Activity` item started and completed together.
    fn activity(&mut self, turn: TurnId, harness_item_id: String, title: String, out: &mut Out) {
        let id = ItemId::new();
        out.push(self.event(AgentEvent::ItemStarted {
            thread: self.thread,
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
            thread: self.thread,
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
        turn: TurnId,
        result: ToolResultBlock,
        tool_use_result: Option<&Value>,
        meta: &[ToolResultMeta],
        out: &mut Out,
    ) {
        let Some(state) = self.turn.as_mut() else {
            return;
        };
        let Some(open) = state.items.tools.remove(&result.tool_use_id) else {
            warn!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
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
        debug!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
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
                subagent: None,
                error: is_error.then(|| tool_result_text(result.content.as_ref())),
            },
        };
        out.push(self.event(AgentEvent::ItemCompleted {
            thread: self.thread,
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
    fn resolve_item(&mut self, key: &NativeItemKey) -> ItemId {
        match self.turn.as_mut() {
            Some(turn) => *turn
                .items
                .item_ids
                .entry(key.clone())
                .or_insert_with(ItemId::new),
            None => ItemId::new(),
        }
    }

    // ---- approvals and server requests ---------------------------------------------------------

    fn on_can_use_tool(
        &mut self,
        request_id: String,
        request: ToolPermissionRequest,
        agent_id: Option<String>,
        raw: &Value,
        out: &mut Out,
    ) {
        let tool_name = request.tool_name.as_str();
        if matches!(tool_name, "ExitPlanMode" | "EnterPlanMode") {
            // With `--disallowedTools EnterPlanMode ExitPlanMode` this ask never appears; if it
            // does, Giskard still owns the mode, so answer it without a user.
            warn!(
                thread_id = %self.thread,
                harness_thread_id = %self.harness_thread_id,
                turn_id = display_opt(self.active_turn()),
                action = "deny_plan_mode_tool",
                request_id = %request_id,
                tool_name,
                native_item_id = display_opt(request.tool_use_id.as_deref()),
                "denying a plan-mode tool the CLI asked to use; Giskard chooses the mode per turn"
            );
            if let (Some(turn), Some(tool_use_id)) = (self.turn.as_mut(), &request.tool_use_id) {
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
        let turn = self.ensure_turn("control_request", out);
        info!(
            thread_id = %self.thread,
            harness_thread_id = %self.harness_thread_id,
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
                thread: self.thread,
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
            thread: self.thread,
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
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use super::*;

    const WORKSPACE: &str = "/work/project";

    #[derive(Clone)]
    struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn capture_logs(level: tracing::Level, log: impl FnOnce()) -> String {
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer_output = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(level)
            .with_writer(move || CapturedLogWriter(writer_output.clone()))
            .finish();
        tracing::subscriber::with_default(subscriber, log);
        String::from_utf8(output.lock().unwrap().clone()).unwrap()
    }

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
        let per_line = drive(&mut new_mapper(), "delegation", TurnKind::User);
        let outputs: Vec<MapperOutput> = per_line.iter().flatten().cloned().collect();

        let completions = turn_completions(&outputs);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Completed);
        let result_line = lines
            .iter()
            .position(|line| line.contains(r#""type": "result""#))
            .unwrap();
        assert_eq!(turn_completions(&per_line[result_line]).len(), 1);

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

        let mut child_frames = 0;
        for (line, outputs) in lines.iter().zip(&per_line) {
            let frame: Value = serde_json::from_str(line).unwrap();
            if frame["parent_tool_use_id"].is_string() {
                child_frames += 1;
                assert!(outputs.is_empty(), "child frame produced {outputs:?}");
            }
        }
        assert!(child_frames > 0);
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
        let per_line = drive(&mut new_mapper(), "delegation-interrupted", TurnKind::User);
        let result_line = lines
            .iter()
            .position(|line| line.contains(r#""type": "result""#))
            .unwrap();
        assert!(turn_completions(&per_line[result_line]).is_empty());

        let killed_line = lines
            .iter()
            .position(|line| line.contains(r#""status": "killed""#))
            .unwrap();
        let completions = turn_completions(&per_line[killed_line]);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].2.kind, TurnStatusKind::Interrupted);

        let outputs: Vec<MapperOutput> = per_line.into_iter().flatten().collect();
        assert_eq!(turn_completions(&outputs).len(), 1);
    }

    #[test]
    fn a_killed_agent_task_without_an_interrupt_fails_the_held_turn() {
        let outputs: Vec<MapperOutput> = drive_lines(
            &mut new_mapper(),
            &out_lines("delegation-interrupted"),
            1,
            TurnKind::User,
            false,
        )
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
    fn blank_lines_are_skipped_without_a_warning() {
        let mut mapper = new_mapper();
        let mut outputs = Vec::new();
        let logs = capture_logs(tracing::Level::WARN, || {
            outputs.extend(mapper.map_line(""));
            outputs.extend(mapper.map_line("  \r"));
        });
        assert!(outputs.is_empty());
        assert!(logs.is_empty(), "{logs}");
    }

    #[test]
    fn a_background_shell_task_never_gates_the_turn() {
        let lines = out_lines("background-bash");
        let mut per_line = Vec::new();
        let logs = capture_logs(tracing::Level::INFO, || {
            per_line = drive(&mut new_mapper(), "background-bash", TurnKind::User);
        });
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
        assert!(logs.contains(r#"action="external_turn""#), "{logs}");
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
    fn a_permission_mode_the_adapter_did_not_set_is_a_notice() {
        let mut mapper = new_mapper();
        mapper.set_expected_mode("default");
        let status = r#"{"type":"system","subtype":"status","status":null,"permissionMode":"plan","session_id":"s"}"#;
        let mut outputs = Vec::new();
        let logs = capture_logs(tracing::Level::WARN, || {
            outputs = mapper.map_line(status);
        });
        assert_eq!(notices(&outputs).len(), 1);
        assert!(logs.contains(r#"action="permission_mode_drift""#), "{logs}");

        mapper.set_expected_mode("plan");
        assert!(events(&mapper.map_line(status)).is_empty());
    }

    #[test]
    fn a_permission_mode_the_adapter_did_not_set_on_init_is_a_notice() {
        let mut mapper = new_mapper();
        mapper.set_expected_mode("acceptEdits");
        let init = line_of("text-turn", "system");
        let mut outputs = Vec::new();
        let logs = capture_logs(tracing::Level::WARN, || {
            outputs = mapper.map_line(&init);
        });
        assert_eq!(
            notices(&outputs),
            ["Claude Code switched its permission mode to default; Giskard set acceptEdits"]
        );
        assert!(logs.contains(r#"action="permission_mode_drift""#), "{logs}");

        // No expectation yet (before the adapter set a mode): no drift.
        let mut fresh = new_mapper();
        assert!(notices(&fresh.map_line(&init)).is_empty());
    }

    #[test]
    fn a_control_cancel_request_is_handed_to_the_adapter() {
        let mut mapper = new_mapper();
        let mut outputs = Vec::new();
        let logs = capture_logs(tracing::Level::INFO, || {
            outputs =
                mapper.map_line(r#"{"type":"control_cancel_request","request_id":"a949f115"}"#);
        });
        assert!(matches!(
            &outputs[..],
            [MapperOutput::CancelRequest { request_id }] if request_id == "a949f115"
        ));
        assert!(
            logs.contains(r#"action="control_cancel_request""#),
            "{logs}"
        );
        assert!(!logs.contains("does not know"), "{logs}");
    }

    #[test]
    fn a_denial_the_adapter_noted_completes_the_tool_declined() {
        let mut mapper = new_mapper();
        let lines = out_lines("tool-allowed");
        let mut outputs = Vec::new();
        outputs.extend(mapper.begin_turn(TurnId::new(), TurnKind::User));
        for line in &lines {
            outputs.extend(mapper.map_line(line));
            if line.contains("\"control_request\"") {
                mapper.note_denied("toolu_01FZdtNSm7HPbRG2vZ6fknrF");
            }
        }
        assert_eq!(
            command_statuses(&outputs),
            vec![("touch probe.txt".into(), Some("declined".into()))]
        );

        let logs = capture_logs(tracing::Level::WARN, || new_mapper().note_denied("toolu_x"));
        assert!(logs.contains(r#"action="note_denied""#), "{logs}");
    }

    #[test]
    fn an_orphan_tool_result_is_dropped_with_a_warning() {
        let mut mapper = new_mapper();
        mapper.begin_turn(TurnId::new(), TurnKind::User);
        let line = r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_orphan","content":"secret output","is_error":false}]},"parent_tool_use_id":null,"session_id":"f18693ff-2d11-4f87-9556-2b527e19e081"}"#;
        let mut outputs = Vec::new();
        let logs = capture_logs(tracing::Level::WARN, || {
            outputs = mapper.map_line(line);
        });
        assert!(events(&outputs).is_empty());
        let warnings: Vec<&str> = logs.lines().filter(|line| line.contains("WARN")).collect();
        assert_eq!(warnings.len(), 1, "{logs}");
        assert!(
            warnings[0].contains("native_item_id=toolu_orphan"),
            "{logs}"
        );
        assert!(!logs.contains("secret output"));
    }

    #[test]
    fn unknown_frames_are_logged_once_per_kind() {
        let mut mapper = new_mapper();
        let line = r#"{"type":"active_goal","value":null,"session_id":"s"}"#;
        let mut outputs = Vec::new();
        let logs = capture_logs(tracing::Level::DEBUG, || {
            outputs.extend(mapper.map_line(line));
            outputs.extend(mapper.map_line(line));
        });
        assert!(outputs.is_empty());
        let lines: Vec<&str> = logs
            .lines()
            .filter(|line| line.contains("frame kind this adapter does not know"))
            .collect();
        assert_eq!(lines.len(), 2, "{logs}");
        assert!(lines[0].contains("WARN") && lines[0].contains("frame_type=active_goal"));
        assert!(lines[1].contains("DEBUG"));
    }

    #[test]
    fn unparseable_lines_are_logged_without_their_content() {
        let mut mapper = new_mapper();
        let mut outputs = Vec::new();
        let logs = capture_logs(tracing::Level::WARN, || {
            outputs.extend(mapper.map_line("Error: touch probe.txt failed"));
            outputs.extend(mapper.map_line(
                r#"{"type":"assistant","session_id":"s","message":{"id":"m","role":"assistant","content":"touch probe.txt"}}"#,
            ));
        });
        assert!(outputs.is_empty());
        assert!(
            logs.contains("not JSON") && logs.contains("bytes=29"),
            "{logs}"
        );
        assert!(logs.contains("frame_type=assistant"), "{logs}");
        assert!(!logs.contains("touch probe.txt"), "{logs}");
    }

    #[test]
    fn logs_never_carry_frame_content() {
        let logs = capture_logs(tracing::Level::TRACE, || {
            run_fixture("tool-denied", TurnKind::User);
        });
        assert!(!logs.is_empty());
        assert!(!logs.contains("touch probe.txt"), "{logs}");
    }

    #[test]
    fn a_turn_begun_while_one_is_active_fails_the_first() {
        let mut mapper = new_mapper();
        let first = TurnId::new();
        let second = TurnId::new();
        mapper.begin_turn(first, TurnKind::User);
        let mut outputs = Vec::new();
        let logs = capture_logs(tracing::Level::ERROR, || {
            outputs = mapper.begin_turn(second, TurnKind::User);
        });
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
        assert_eq!(
            logs.lines().filter(|line| line.contains("ERROR")).count(),
            1
        );
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
        let logs = capture_logs(tracing::Level::WARN, || {
            let outputs = mapper.child_exited("code 3");
            let completions = turn_completions(&outputs);
            assert_eq!(completions.len(), 1);
            assert_eq!(completions[0].0, turn);
            assert_eq!(completions[0].2.kind, TurnStatusKind::Failed);
            assert_eq!(
                completions[0].2.message.as_deref(),
                Some("Claude Code exited (code 3) before the turn completed")
            );
        });
        assert!(logs.contains("action=\"child_exited\""), "{logs}");
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
}
