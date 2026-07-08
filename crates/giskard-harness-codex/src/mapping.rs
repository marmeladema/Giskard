//! Mapping between `codex-codes` types and `giskard-core` types (spec §4.6).

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use chrono::Utc;
use serde::Serialize;
use serde_json::{Value, json};

use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
use giskard_core::diff::{DiffHunk, DiffLine, FileDiff};
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ThreadId, TurnId};
use giskard_core::item::{
    CommandExecutionStart, FileChangeEntry, FileChangeKind, Item, ItemDelta, ItemKind, ItemPayload,
    ItemStart,
};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{ApprovalPolicy, Mode, TurnStatus, TurnStatusKind};

use codex_codes::jsonrpc::RequestId;
use codex_codes::messages::{Notification, ServerRequest};
use codex_codes::protocol::{
    AgentMessageDeltaNotification, CommandExecutionOutputDeltaNotification,
    ItemCompletedNotification, ItemStartedNotification, TurnCompletedNotification,
    TurnDiffUpdatedNotification, TurnStartedNotification,
};

/// Maps Codex app-server messages onto `giskard-core` events, owning the id-translation registries
/// (spec §4.7): native `threadId → ThreadId` (B4), native `turnId → TurnId`, and native
/// `itemId → ItemId` (B2). The Giskard-owned ids are minted once and reused for every subsequent
/// delta/completion carrying the same native id, so events for one turn/item stay correlated.
pub struct CodexMapper {
    _workspace_root: PathBuf,
    thread_ids: HashMap<String, ThreadId>,
    turn_ids: HashMap<String, TurnId>,
    item_ids: HashMap<String, ItemId>,
    /// Latest per-turn token usage, keyed by native turn id. Codex reports usage via a separate
    /// `thread/tokenUsage/updated` notification (not on `turn/completed`), so we cache the most
    /// recent value per turn and attach it when the turn completes (spec §10.1).
    turn_usage: HashMap<String, TokenUsage>,
    active_turns: HashMap<ThreadId, String>,
    running_command_turns: HashMap<String, (ThreadId, String)>,
    running_commands: HashSet<String>,
    running_command_threads: HashMap<String, ThreadId>,
}

impl CodexMapper {
    pub fn new(workspace_root: PathBuf) -> Self {
        Self {
            _workspace_root: workspace_root,
            thread_ids: HashMap::new(),
            turn_ids: HashMap::new(),
            item_ids: HashMap::new(),
            turn_usage: HashMap::new(),
            active_turns: HashMap::new(),
            running_command_turns: HashMap::new(),
            running_commands: HashSet::new(),
            running_command_threads: HashMap::new(),
        }
    }

    pub fn has_running_commands(&self) -> bool {
        !self.running_commands.is_empty()
    }

    pub fn running_command_fallback_thread(&self) -> Option<ThreadId> {
        self.running_command_threads.values().next().copied()
    }

    pub fn active_native_turn_for_thread(&self, thread: ThreadId) -> Option<&str> {
        self.active_turns.get(&thread).map(String::as_str)
    }

    pub fn native_turn_for_process(&self, thread: ThreadId, process_id: &str) -> Option<&str> {
        self.running_command_turns
            .get(process_id)
            .filter(|(owner_thread, _)| *owner_thread == thread)
            .map(|(_, turn_id)| turn_id.as_str())
    }

    /// B4: bind a native thread id to its owned `ThreadId`. Called at `open_thread` for both fresh
    /// `thread/start` and `thread/resume` (and re-bound after a resume-fallback, §4.7/C5).
    pub fn register_thread(&mut self, harness_thread_id: String, thread: ThreadId) {
        self.thread_ids.insert(harness_thread_id, thread);
    }

    /// Resolve a native thread id to its owned `ThreadId`, falling back to the thread in scope
    /// when the message omits the id or it is not yet registered.
    fn resolve_thread(&self, native: &str, fallback: ThreadId) -> ThreadId {
        if native.is_empty() {
            return fallback;
        }
        self.thread_ids.get(native).copied().unwrap_or(fallback)
    }

    /// Resolve (get-or-mint) the owned `TurnId` for a native turn id.
    fn resolve_turn(&mut self, native: &str) -> TurnId {
        if native.is_empty() {
            return TurnId::new();
        }
        *self.turn_ids.entry(native.to_string()).or_default()
    }

    /// Resolve (get-or-mint) the owned `ItemId` for a native item id (B2).
    fn resolve_item(&mut self, native: &str) -> ItemId {
        if native.is_empty() {
            return ItemId::new();
        }
        *self.item_ids.entry(native.to_string()).or_default()
    }

    pub fn map_notification(
        &mut self,
        notif: &Notification,
        fallback_thread: ThreadId,
    ) -> Option<AgentEvent> {
        match notif {
            Notification::TurnStarted(TurnStartedNotification { thread_id, turn }) => {
                let thread = self.resolve_thread(thread_id, fallback_thread);
                self.active_turns.insert(thread, turn.id.clone());
                Some(AgentEvent::TurnStarted {
                    thread,
                    turn: self.resolve_turn(&turn.id),
                })
            }

            Notification::TurnCompleted(TurnCompletedNotification { thread_id, turn }) => {
                let thread = self.resolve_thread(thread_id, fallback_thread);
                if self
                    .active_turns
                    .get(&thread)
                    .map(|active| active == &turn.id)
                    .unwrap_or(false)
                {
                    self.active_turns.remove(&thread);
                }
                // Attach the usage cached from the last `thread/tokenUsage/updated` for this turn
                // (spec §10.1). Defaults to zero if Codex sent no usage update for the turn.
                let usage = self.turn_usage.remove(&turn.id).unwrap_or_default();
                let status = map_turn_status(&turn.status);
                Some(AgentEvent::TurnCompleted {
                    thread,
                    turn: self.resolve_turn(&turn.id),
                    usage,
                    status,
                })
            }

            Notification::ThreadTokenUsageUpdated(n) => {
                // Cache the last-turn usage breakdown; emit no Giskard event for the intermediate
                // update (the usage is surfaced on `TurnCompleted`).
                if !n.turn_id.is_empty() {
                    self.turn_usage
                        .insert(n.turn_id.clone(), breakdown_to_usage(&n.token_usage.last));
                }
                None
            }

            Notification::ItemStarted(ItemStartedNotification {
                item,
                started_at_ms,
                thread_id,
                turn_id,
                ..
            }) => {
                let thread = self.resolve_thread(thread_id, fallback_thread);
                let turn = self.resolve_turn(turn_id);
                let (harness_item_id, kind, command) = map_thread_item_start(item, *started_at_ms);
                let id = self.resolve_item(&harness_item_id);
                self.track_command_start(&harness_item_id, command.as_ref(), thread, turn_id);
                Some(AgentEvent::ItemStarted {
                    thread,
                    turn,
                    item: ItemStart {
                        id,
                        harness_item_id,
                        kind,
                        command,
                    },
                })
            }

            Notification::ItemCompleted(ItemCompletedNotification {
                item,
                completed_at_ms,
                thread_id,
                turn_id,
            }) => {
                let thread = self.resolve_thread(thread_id, fallback_thread);
                let turn = self.resolve_turn(turn_id);
                let harness_item_id = thread_item_id(item);
                let id = self.resolve_item(&harness_item_id);
                let giskard_item =
                    map_thread_item_complete(item, id, harness_item_id, *completed_at_ms);
                self.track_completed_item(&giskard_item, thread);
                Some(AgentEvent::ItemCompleted {
                    thread,
                    turn,
                    item: giskard_item,
                })
            }

            Notification::AgentMessageDelta(AgentMessageDeltaNotification {
                delta,
                item_id,
                thread_id,
                turn_id,
            }) => self.map_text_delta(thread_id, turn_id, item_id, delta, fallback_thread),

            Notification::CmdOutputDelta(CommandExecutionOutputDeltaNotification {
                delta,
                item_id,
                thread_id,
                turn_id,
                ..
            }) => {
                let thread = self.resolve_thread(thread_id, fallback_thread);
                let turn = self.resolve_turn(turn_id);
                Some(AgentEvent::ItemDelta {
                    thread,
                    turn,
                    item_id: self.resolve_item(item_id),
                    delta: ItemDelta::CommandOutput {
                        chunk: delta.clone(),
                    },
                })
            }

            Notification::FileChangeOutputDelta(n) => self.map_text_delta(
                &n.thread_id,
                &n.turn_id,
                &n.item_id,
                &n.delta,
                fallback_thread,
            ),

            Notification::FileChangePatchUpdated(n) => {
                let text = summarize_file_changes(&n.changes);
                self.map_text_delta(&n.thread_id, &n.turn_id, &n.item_id, &text, fallback_thread)
            }

            Notification::ReasoningDelta(n) => self.map_text_delta(
                &n.thread_id,
                &n.turn_id,
                &n.item_id,
                &n.delta,
                fallback_thread,
            ),

            Notification::ReasoningTextDelta(n) => self.map_text_delta(
                &n.thread_id,
                &n.turn_id,
                &n.item_id,
                &n.delta,
                fallback_thread,
            ),

            Notification::PlanDelta(n) => self.map_text_delta(
                &n.thread_id,
                &n.turn_id,
                &n.item_id,
                &n.delta,
                fallback_thread,
            ),

            Notification::McpToolCallProgress(n) => self.map_text_delta(
                &n.thread_id,
                &n.turn_id,
                &n.item_id,
                &n.message,
                fallback_thread,
            ),

            Notification::TurnDiffUpdated(TurnDiffUpdatedNotification {
                diff,
                thread_id,
                turn_id,
                ..
            }) => {
                let thread = self.resolve_thread(thread_id, fallback_thread);
                let turn = self.resolve_turn(turn_id);
                Some(AgentEvent::DiffUpdated {
                    thread,
                    turn,
                    diff: FileDiff {
                        path: PathBuf::new(),
                        change: FileChangeKind::Modified,
                        old_text: None,
                        new_text: Some(diff.clone()),
                        hunks: parse_diff_hunks(diff),
                        binary: false,
                    },
                })
            }

            Notification::Error(n) => {
                let thread = self.resolve_thread(&n.thread_id, fallback_thread);
                Some(AgentEvent::Error {
                    thread,
                    turn: None,
                    error: giskard_core::error::HarnessError::Protocol(compose_turn_error(
                        &n.error,
                        n.will_retry,
                    )),
                })
            }

            Notification::TurnPlanUpdated(n) => {
                let thread = self.resolve_thread(&n.thread_id, fallback_thread);
                let turn = self.resolve_turn(&n.turn_id);
                let mut lines = Vec::new();
                if let Some(explanation) = &n.explanation {
                    if !explanation.trim().is_empty() {
                        lines.push(explanation.clone());
                    }
                }
                for step in &n.plan {
                    lines.push(format!("{}: {}", enum_string(&step.status), step.step));
                }
                Some(self.activity_event(
                    thread,
                    turn,
                    format!("turn_plan:{}", n.turn_id),
                    "Plan updated",
                    lines.join("\n"),
                    &n.plan,
                ))
            }

            Notification::ModelRerouted(n) => {
                let thread = self.resolve_thread(&n.thread_id, fallback_thread);
                let turn = self.resolve_turn(&n.turn_id);
                Some(self.activity_event(
                    thread,
                    turn,
                    format!("model_rerouted:{}", n.turn_id),
                    "Model rerouted",
                    format!("{} -> {}", n.from_model, n.to_model),
                    n,
                ))
            }

            Notification::ContextCompacted(n) => {
                let thread = self.resolve_thread(&n.thread_id, fallback_thread);
                let turn = self.resolve_turn(&n.turn_id);
                Some(self.activity_event(
                    thread,
                    turn,
                    format!("context_compacted:{}", n.turn_id),
                    "Context compacted",
                    "",
                    n,
                ))
            }

            // Codex advisories are non-fatal — surface them as notices (warnings), not hard errors,
            // so they don't fail the turn or the pending message.
            Notification::Warning(n) => {
                let thread = n
                    .thread_id
                    .as_deref()
                    .map(|id| self.resolve_thread(id, fallback_thread))
                    .unwrap_or(fallback_thread);
                Some(AgentEvent::Notice {
                    thread,
                    turn: None,
                    message: n.message.clone(),
                })
            }

            Notification::ConfigWarning(n) => Some(AgentEvent::Notice {
                thread: fallback_thread,
                turn: None,
                message: format!(
                    "Configuration: {}{}",
                    n.summary,
                    n.details
                        .as_ref()
                        .map(|d| format!(": {d}"))
                        .unwrap_or_default()
                ),
            }),

            Notification::GuardianWarning(n) => {
                let thread = self.resolve_thread(&n.thread_id, fallback_thread);
                Some(AgentEvent::Notice {
                    thread,
                    turn: None,
                    message: format!("Guardian: {}", n.message),
                })
            }

            Notification::DeprecationNotice(n) => Some(AgentEvent::Notice {
                thread: fallback_thread,
                turn: None,
                message: format!(
                    "Deprecation: {}{}",
                    n.summary,
                    n.details
                        .as_ref()
                        .map(|d| format!(": {d}"))
                        .unwrap_or_default()
                ),
            }),

            _ => None,
        }
    }

    fn track_command_start(
        &mut self,
        harness_item_id: &str,
        command: Option<&CommandExecutionStart>,
        thread: ThreadId,
        native_turn_id: &str,
    ) {
        let Some(command) = command else {
            return;
        };
        if command
            .status
            .as_deref()
            .map(command_status_is_running)
            .unwrap_or(true)
        {
            self.running_commands.insert(harness_item_id.to_owned());
            self.running_command_threads
                .insert(harness_item_id.to_owned(), thread);
            if let Some(process_id) = &command.process_id {
                let turn_id = if native_turn_id.is_empty() {
                    self.active_turns.get(&thread).cloned().unwrap_or_default()
                } else {
                    native_turn_id.to_owned()
                };
                if !turn_id.is_empty() {
                    self.running_command_turns
                        .insert(process_id.clone(), (thread, turn_id));
                }
            }
        }
    }

    fn track_completed_item(&mut self, item: &Item, thread: ThreadId) {
        let ItemPayload::CommandExecution {
            status, process_id, ..
        } = &item.payload
        else {
            return;
        };
        if status
            .as_deref()
            .map(command_status_is_running)
            .unwrap_or(false)
        {
            self.running_commands.insert(item.harness_item_id.clone());
            self.running_command_threads
                .insert(item.harness_item_id.clone(), thread);
        } else {
            self.running_commands.remove(&item.harness_item_id);
            self.running_command_threads.remove(&item.harness_item_id);
            if let Some(process_id) = process_id {
                self.running_command_turns.remove(process_id);
            }
        }
    }

    fn map_text_delta(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        text: &str,
        fallback_thread: ThreadId,
    ) -> Option<AgentEvent> {
        if text.is_empty() {
            return None;
        }
        let thread = self.resolve_thread(thread_id, fallback_thread);
        let turn = self.resolve_turn(turn_id);
        Some(AgentEvent::ItemDelta {
            thread,
            turn,
            item_id: self.resolve_item(item_id),
            delta: ItemDelta::Text {
                text: text.to_owned(),
            },
        })
    }

    fn activity_event<T: Serialize>(
        &mut self,
        thread: ThreadId,
        turn: TurnId,
        harness_item_id: String,
        title: impl Into<String>,
        detail: impl Into<String>,
        metadata: &T,
    ) -> AgentEvent {
        let id = self.resolve_item(&harness_item_id);
        let detail = detail.into();
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id,
                harness_item_id,
                payload: ItemPayload::Activity {
                    title: title.into(),
                    detail: (!detail.trim().is_empty()).then_some(detail),
                    metadata: serde_json::to_value(metadata).ok(),
                },
                created_at: Utc::now(),
            },
        }
    }

    pub fn map_server_request(
        &mut self,
        id: &RequestId,
        request: &ServerRequest,
        fallback_thread: ThreadId,
    ) -> Option<AgentEvent> {
        let req_id = match id {
            RequestId::Integer(i) => ApprovalId(i.to_string()),
            RequestId::String(s) => ApprovalId(s.clone()),
        };

        match request {
            ServerRequest::CmdExecApproval(params) => {
                let thread = self.resolve_thread(&params.thread_id, fallback_thread);
                let turn = self.resolve_turn(&params.turn_id);
                Some(AgentEvent::ApprovalRequested {
                    thread,
                    turn,
                    request: ApprovalRequest {
                        id: req_id,
                        kind: ApprovalKind::CommandExecution {
                            command: params.command.clone().unwrap_or_default(),
                            cwd: params
                                .cwd
                                .as_ref()
                                .map(|c| PathBuf::from(&c.0))
                                .unwrap_or_default(),
                        },
                        reason: params.reason.clone(),
                        available: vec![
                            ApprovalDecision::Accept,
                            ApprovalDecision::AcceptForSession,
                            ApprovalDecision::Decline,
                            ApprovalDecision::Cancel,
                        ],
                    },
                })
            }
            ServerRequest::FileChangeApproval(params) => {
                let thread = self.resolve_thread(&params.thread_id, fallback_thread);
                let turn = self.resolve_turn(&params.turn_id);
                Some(AgentEvent::ApprovalRequested {
                    thread,
                    turn,
                    request: ApprovalRequest {
                        id: req_id,
                        kind: ApprovalKind::FileChange {
                            path: PathBuf::new(),
                            change: FileChangeKind::Modified,
                        },
                        reason: params.reason.clone(),
                        available: vec![
                            ApprovalDecision::Accept,
                            ApprovalDecision::AcceptForSession,
                            ApprovalDecision::Decline,
                            ApprovalDecision::Cancel,
                        ],
                    },
                })
            }
            _ => None,
        }
    }
}

// ---- Free functions ----

pub fn map_user_input(input: &giskard_core::user_input::UserInput) -> Vec<codex_codes::UserInput> {
    match input {
        giskard_core::user_input::UserInput::Text { text } => {
            vec![codex_codes::UserInput::Text {
                text: text.clone(),
                text_elements: None,
            }]
        }
    }
}

pub fn map_mode_to_sandbox(mode: Mode) -> codex_codes::SandboxPolicy {
    match mode {
        Mode::Plan => codex_codes::SandboxPolicy::ReadOnly {
            network_access: None,
        },
        Mode::Build => codex_codes::SandboxPolicy::WorkspaceWrite {
            exclude_slash_tmp: None,
            exclude_tmpdir_env_var: None,
            network_access: None,
            writable_roots: None,
        },
    }
}

pub fn map_approval_policy(policy: ApprovalPolicy) -> codex_codes::AskForApproval {
    match policy {
        ApprovalPolicy::ReadOnly => codex_codes::AskForApproval::Never,
        ApprovalPolicy::Ask => codex_codes::AskForApproval::OnRequest,
        ApprovalPolicy::Auto => codex_codes::AskForApproval::Never,
    }
}

pub fn map_effort(effort: giskard_core::model::Effort) -> codex_codes::ReasoningEffort {
    use giskard_core::model::Effort;
    // Matches Codex `ModelReasoningEffort` (minimal | low | medium | high | xhigh), S4.
    let s = match effort {
        Effort::Minimal => "minimal",
        Effort::Low => "low",
        Effort::Medium => "medium",
        Effort::High => "high",
        Effort::XHigh => "xhigh",
    };
    codex_codes::ReasoningEffort(s.into())
}

pub fn map_approval_decision(decision: &ApprovalDecision) -> Value {
    match decision {
        ApprovalDecision::Accept => serde_json::json!({"decision": "accept"}),
        ApprovalDecision::AcceptForSession => {
            serde_json::json!({"decision": "acceptForSession"})
        }
        ApprovalDecision::Decline => serde_json::json!({"decision": "decline"}),
        ApprovalDecision::Cancel => serde_json::json!({"decision": "cancel"}),
        ApprovalDecision::AcceptWithExecPolicyAmendment { amendment } => {
            serde_json::json!({
                "decision": "accept",
                "amendedExecPolicy": amendment,
            })
        }
    }
}

fn map_turn_status(status: &codex_codes::TurnStatus) -> TurnStatus {
    match status {
        codex_codes::TurnStatus::Completed => TurnStatus {
            kind: TurnStatusKind::Completed,
            message: None,
        },
        codex_codes::TurnStatus::Interrupted => TurnStatus {
            kind: TurnStatusKind::Interrupted,
            message: None,
        },
        codex_codes::TurnStatus::Failed => TurnStatus {
            kind: TurnStatusKind::Failed,
            message: None,
        },
        _ => TurnStatus {
            kind: TurnStatusKind::Completed,
            message: None,
        },
    }
}

/// Convert a Codex `TokenUsageBreakdown` into the neutral `TokenUsage` (spec §10.1).
///
/// `input`/`output`/`total` map to Codex's `input_tokens`/`output_tokens`/`total_tokens`
/// (cached-input and reasoning-output sub-counts are folded into those totals by Codex and are
/// not tracked separately in v1).
fn breakdown_to_usage(b: &codex_codes::protocol::TokenUsageBreakdown) -> TokenUsage {
    TokenUsage {
        input: b.input_tokens.max(0) as u64,
        output: b.output_tokens.max(0) as u64,
        total: b.total_tokens.max(0) as u64,
    }
}

/// Extract the native item id string from a Codex `ThreadItem`.
fn thread_item_id(item: &codex_codes::ThreadItem) -> String {
    match item {
        codex_codes::ThreadItem::UserMessage { id, .. }
        | codex_codes::ThreadItem::HookPrompt { id, .. }
        | codex_codes::ThreadItem::AgentMessage { id, .. }
        | codex_codes::ThreadItem::Plan { id, .. }
        | codex_codes::ThreadItem::Reasoning { id, .. }
        | codex_codes::ThreadItem::CommandExecution { id, .. }
        | codex_codes::ThreadItem::FileChange { id, .. }
        | codex_codes::ThreadItem::McpToolCall { id, .. }
        | codex_codes::ThreadItem::DynamicToolCall { id, .. }
        | codex_codes::ThreadItem::CollabAgentToolCall { id, .. }
        | codex_codes::ThreadItem::SubAgentActivity { id, .. }
        | codex_codes::ThreadItem::WebSearch { id, .. }
        | codex_codes::ThreadItem::ImageView { id, .. }
        | codex_codes::ThreadItem::Sleep { id, .. }
        | codex_codes::ThreadItem::ImageGeneration { id, .. }
        | codex_codes::ThreadItem::EnteredReviewMode { id, .. }
        | codex_codes::ThreadItem::ExitedReviewMode { id, .. }
        | codex_codes::ThreadItem::ContextCompaction { id, .. } => id.clone(),
    }
}

/// Returns the native item id, Giskard `ItemKind`, and any start-time metadata for an item.
fn map_thread_item_start(
    item: &codex_codes::ThreadItem,
    started_at_ms: i64,
) -> (String, ItemKind, Option<CommandExecutionStart>) {
    let kind = match item {
        codex_codes::ThreadItem::UserMessage { .. } => ItemKind::UserMessage,
        codex_codes::ThreadItem::AgentMessage { .. } | codex_codes::ThreadItem::Plan { .. } => {
            ItemKind::AgentMessage
        }
        codex_codes::ThreadItem::Reasoning { .. } => ItemKind::Reasoning,
        codex_codes::ThreadItem::CommandExecution { .. } => ItemKind::CommandExecution,
        codex_codes::ThreadItem::FileChange { .. } => ItemKind::FileChange,
        codex_codes::ThreadItem::McpToolCall { .. }
        | codex_codes::ThreadItem::DynamicToolCall { .. }
        | codex_codes::ThreadItem::CollabAgentToolCall { .. } => ItemKind::ToolCall,
        codex_codes::ThreadItem::HookPrompt { .. }
        | codex_codes::ThreadItem::SubAgentActivity { .. }
        | codex_codes::ThreadItem::WebSearch { .. }
        | codex_codes::ThreadItem::ImageView { .. }
        | codex_codes::ThreadItem::Sleep { .. }
        | codex_codes::ThreadItem::ImageGeneration { .. }
        | codex_codes::ThreadItem::EnteredReviewMode { .. }
        | codex_codes::ThreadItem::ExitedReviewMode { .. }
        | codex_codes::ThreadItem::ContextCompaction { .. } => ItemKind::Activity,
    };
    let command = match item {
        codex_codes::ThreadItem::CommandExecution {
            command,
            cwd,
            process_id,
            status,
            ..
        } => Some(CommandExecutionStart {
            command: command.clone(),
            cwd: json_display(cwd),
            status: Some(command_status_string(status)),
            process_id: process_id.clone(),
            started_at_ms: (started_at_ms > 0).then_some(started_at_ms),
        }),
        _ => None,
    };
    (thread_item_id(item), kind, command)
}

fn map_thread_item_complete(
    item: &codex_codes::ThreadItem,
    id: ItemId,
    harness_item_id: String,
    completed_at_ms: i64,
) -> Item {
    let payload = match item {
        codex_codes::ThreadItem::UserMessage { content, .. } => {
            let text = content
                .iter()
                .filter_map(|c| match c {
                    codex_codes::UserInput::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            ItemPayload::UserMessage { text }
        }
        codex_codes::ThreadItem::AgentMessage { text, .. } => {
            ItemPayload::AgentMessage { text: text.clone() }
        }
        codex_codes::ThreadItem::Plan { text, .. } => {
            ItemPayload::AgentMessage { text: text.clone() }
        }
        codex_codes::ThreadItem::Reasoning {
            content, summary, ..
        } => {
            let text = summary
                .as_ref()
                .or(content.as_ref())
                .map(|s| s.join("\n"))
                .unwrap_or_default();
            ItemPayload::Reasoning { text }
        }
        codex_codes::ThreadItem::CommandExecution {
            command,
            cwd,
            aggregated_output,
            exit_code,
            process_id,
            status,
            duration_ms,
            ..
        } => {
            let output = aggregated_output.clone().unwrap_or_default();
            let exit = exit_code.map(|c| c as i32);
            ItemPayload::CommandExecution {
                command: command.clone(),
                cwd: path_from_json_value(cwd),
                output,
                exit_code: exit,
                status: Some(command_status_string(status)),
                process_id: process_id.clone(),
                duration_ms: *duration_ms,
            }
        }
        codex_codes::ThreadItem::FileChange {
            changes, status, ..
        } => {
            let changes = map_file_changes(changes);
            let first = changes.first().cloned();
            ItemPayload::FileChange {
                path: first.as_ref().map(|c| c.path.clone()).unwrap_or_default(),
                change: first
                    .as_ref()
                    .map(|c| c.change)
                    .unwrap_or(FileChangeKind::Modified),
                changes,
                status: Some(enum_string(status)),
            }
        }
        codex_codes::ThreadItem::McpToolCall {
            server,
            tool,
            arguments,
            result,
            error,
            status,
            ..
        } => ItemPayload::ToolCall {
            name: tool.clone(),
            input: arguments.clone(),
            output: result.as_ref().and_then(json_value),
            server: Some(server.clone()),
            status: Some(enum_string(status)),
            error: error.as_ref().map(|e| e.message.clone()),
        },
        codex_codes::ThreadItem::DynamicToolCall {
            tool,
            arguments,
            content_items,
            status,
            namespace,
            success,
            ..
        } => ItemPayload::ToolCall {
            name: tool.clone(),
            input: arguments.clone(),
            output: content_items
                .as_ref()
                .and_then(json_value)
                .or_else(|| success.map(|s| json!({ "success": s }))),
            server: namespace.clone(),
            status: Some(enum_string(status)),
            error: None,
        },
        codex_codes::ThreadItem::CollabAgentToolCall {
            tool,
            status,
            prompt,
            model,
            ..
        } => ItemPayload::ToolCall {
            name: json_display(tool),
            input: json!({
                "prompt": prompt,
                "model": model,
            }),
            output: Some(status.clone()),
            server: Some("collab-agent".into()),
            status: Some(json_display(status)),
            error: None,
        },
        codex_codes::ThreadItem::HookPrompt { fragments, .. } => ItemPayload::Activity {
            title: "Hook prompt".into(),
            detail: Some(
                fragments
                    .iter()
                    .map(|f| f.text.as_str())
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            metadata: json_value(fragments),
        },
        codex_codes::ThreadItem::SubAgentActivity {
            agent_path,
            agent_thread_id,
            kind,
            ..
        } => ItemPayload::Activity {
            title: format!("Sub-agent {}", enum_string(kind)),
            detail: Some(format!("{agent_path} ({agent_thread_id})")),
            metadata: json_value(item),
        },
        codex_codes::ThreadItem::WebSearch { query, action, .. } => ItemPayload::Activity {
            title: "Web search".into(),
            detail: Some(query.clone()),
            metadata: action.as_ref().and_then(json_value),
        },
        codex_codes::ThreadItem::ImageView { path, .. } => ItemPayload::Activity {
            title: "Image viewed".into(),
            detail: Some(path.0.clone()),
            metadata: json_value(item),
        },
        codex_codes::ThreadItem::Sleep { duration_ms, .. } => ItemPayload::Activity {
            title: "Sleep".into(),
            detail: Some(format!("{duration_ms} ms")),
            metadata: json_value(item),
        },
        codex_codes::ThreadItem::ImageGeneration {
            status,
            result,
            revised_prompt,
            saved_path,
            ..
        } => ItemPayload::Activity {
            title: "Image generation".into(),
            detail: Some(
                [
                    Some(format!("status: {status}")),
                    saved_path.as_ref().map(|p| format!("saved: {}", p.0)),
                    revised_prompt.as_ref().map(|p| format!("prompt: {p}")),
                    (!result.trim().is_empty()).then(|| format!("result: {result}")),
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join("\n"),
            ),
            metadata: json_value(item),
        },
        codex_codes::ThreadItem::EnteredReviewMode { review, .. } => ItemPayload::Activity {
            title: "Entered review mode".into(),
            detail: Some(review.clone()),
            metadata: None,
        },
        codex_codes::ThreadItem::ExitedReviewMode { review, .. } => ItemPayload::Activity {
            title: "Exited review mode".into(),
            detail: Some(review.clone()),
            metadata: None,
        },
        codex_codes::ThreadItem::ContextCompaction { .. } => ItemPayload::Activity {
            title: "Context compaction".into(),
            detail: None,
            metadata: json_value(item),
        },
    };

    let created_at =
        chrono::DateTime::from_timestamp_millis(completed_at_ms).unwrap_or_else(Utc::now);

    Item {
        id,
        harness_item_id,
        payload,
        created_at,
    }
}

fn json_value<T: Serialize>(value: &T) -> Option<Value> {
    serde_json::to_value(value).ok()
}

fn json_display(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        _ => value.to_string(),
    }
}

fn enum_string<T: Serialize>(value: &T) -> String {
    json_value(value)
        .map(|v| match v {
            Value::String(s) => s,
            other => other.to_string(),
        })
        .unwrap_or_else(|| "unknown".into())
}

fn command_status_string(status: &codex_codes::CommandExecutionStatus) -> String {
    match status {
        codex_codes::CommandExecutionStatus::InProgress => "in_progress",
        codex_codes::CommandExecutionStatus::Completed => "completed",
        codex_codes::CommandExecutionStatus::Failed => "failed",
        codex_codes::CommandExecutionStatus::Declined => "declined",
    }
    .into()
}

fn command_status_is_running(status: &str) -> bool {
    matches!(
        status.to_ascii_lowercase().replace('-', "_").as_str(),
        "in_progress" | "inprogress" | "running"
    )
}

/// If `notif` is a non-retryable turn error, return its composed message.
///
/// Codex sends no `turn/completed` after a fatal error, so the harness uses this to synthesize a
/// terminal `Failed` turn (persisted to history, §7.1) instead of leaving the turn hanging with
/// only an ephemeral error event. Retryable errors (`will_retry`) are left alone — Codex retries
/// internally and eventually emits its own `turn/completed` or a final non-retryable error.
pub(crate) fn fatal_turn_error(notif: &Notification) -> Option<String> {
    match notif {
        Notification::Error(n) if !n.will_retry => Some(compose_turn_error(&n.error, false)),
        _ => None,
    }
}

/// Compose a human-readable message from a Codex `TurnError` (spec §12.2).
///
/// Codex puts the primary text in `message`, optional supplementary text in `additional_details`,
/// and a structured classification (e.g. `unauthorized`, `badRequest`, `contextWindowExceeded`) in
/// `codex_error_info`. The earlier code read only `additional_details` and fell back to a bare
/// `"error"`, discarding the real cause — so a Codex rejection surfaced in the browser as the
/// useless `protocol error: error`. Here we keep all three, preferring the most specific.
fn compose_turn_error(err: &codex_codes::protocol::TurnError, will_retry: bool) -> String {
    let mut body = String::new();
    let message = err.message.trim();
    if !message.is_empty() {
        body.push_str(message);
    }
    if let Some(details) = err.additional_details.as_deref() {
        let details = details.trim();
        // Skip details already contained in the message to avoid duplication.
        if !details.is_empty() && !message.contains(details) {
            if !body.is_empty() {
                body.push_str(": ");
            }
            body.push_str(details);
        }
    }
    if body.is_empty() {
        body.push_str("Codex reported an unspecified error");
    }

    // Prefix the structured classification when present, e.g. "unauthorized: <message>".
    let mut msg = match &err.codex_error_info {
        Some(info) => format!("{}: {body}", describe_codex_error_info(info)),
        None => body,
    };
    if will_retry {
        msg.push_str(" (retrying)");
    }
    msg
}

/// Short label for a Codex error classification. Unit variants serialize to their camelCase tag
/// (e.g. `unauthorized`); the connection-failure variants also carry an HTTP status we surface.
fn describe_codex_error_info(info: &codex_codes::protocol::CodexErrorInfo) -> String {
    use codex_codes::protocol::CodexErrorInfo as E;
    match info {
        E::HttpConnectionFailed { http_status_code }
        | E::ResponseStreamConnectionFailed { http_status_code }
        | E::ResponseStreamDisconnected { http_status_code }
        | E::ResponseTooManyFailedAttempts { http_status_code } => match http_status_code {
            Some(code) => format!("{} (HTTP {code})", enum_label(info)),
            None => enum_label(info),
        },
        other => enum_label(other),
    }
}

/// The variant tag of a serde-tagged enum value: the string itself for a unit variant, or the
/// first (tag) key for a struct variant — avoids dumping a whole JSON object into the message.
fn enum_label<T: Serialize>(value: &T) -> String {
    match json_value(value) {
        Some(Value::String(s)) => s,
        Some(Value::Object(map)) => map.keys().next().cloned().unwrap_or_else(|| "error".into()),
        _ => "error".into(),
    }
}

fn path_from_json_value(value: &Value) -> PathBuf {
    match value {
        Value::String(s) => PathBuf::from(s),
        other => PathBuf::from(other.to_string()),
    }
}

fn map_file_changes(changes: &[codex_codes::FileUpdateChange]) -> Vec<FileChangeEntry> {
    changes
        .iter()
        .map(|change| FileChangeEntry {
            path: PathBuf::from(&change.path),
            change: map_patch_change_kind(&change.kind),
            diff: (!change.diff.is_empty()).then(|| change.diff.clone()),
        })
        .collect()
}

fn map_patch_change_kind(kind: &codex_codes::PatchChangeKind) -> FileChangeKind {
    match kind {
        codex_codes::PatchChangeKind::Add => FileChangeKind::Created,
        codex_codes::PatchChangeKind::Delete => FileChangeKind::Deleted,
        codex_codes::PatchChangeKind::Update { .. } => FileChangeKind::Modified,
    }
}

fn summarize_file_changes(changes: &[codex_codes::FileUpdateChange]) -> String {
    if changes.is_empty() {
        return "File changes updated.".into();
    }
    changes
        .iter()
        .map(|change| {
            format!(
                "{} {}",
                file_change_label(map_patch_change_kind(&change.kind)),
                change.path
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn file_change_label(change: FileChangeKind) -> &'static str {
    match change {
        FileChangeKind::Created => "created",
        FileChangeKind::Modified => "modified",
        FileChangeKind::Deleted => "deleted",
    }
}

fn parse_diff_hunks(diff_text: &str) -> Vec<DiffHunk> {
    if diff_text.is_empty() {
        return vec![];
    }

    let mut hunks = Vec::new();
    let mut current_lines = Vec::new();
    let mut old_start = 0u32;
    let mut old_lines = 0u32;
    let mut new_start = 0u32;
    let mut new_lines = 0u32;

    for line in diff_text.lines() {
        if line.starts_with("@@") {
            if !current_lines.is_empty() {
                hunks.push(DiffHunk {
                    old_start,
                    old_lines,
                    new_start,
                    new_lines,
                    lines: std::mem::take(&mut current_lines),
                });
            }
            if let Some((os, ol, ns, nl)) = parse_hunk_header(line) {
                old_start = os;
                old_lines = ol;
                new_start = ns;
                new_lines = nl;
            }
        } else if let Some(stripped) = line.strip_prefix('+') {
            current_lines.push(DiffLine::Added(stripped.to_string()));
        } else if let Some(stripped) = line.strip_prefix('-') {
            current_lines.push(DiffLine::Removed(stripped.to_string()));
        } else {
            current_lines.push(DiffLine::Context(
                line.strip_prefix(' ').unwrap_or(line).to_string(),
            ));
        }
    }

    if !current_lines.is_empty() {
        hunks.push(DiffHunk {
            old_start,
            old_lines,
            new_start,
            new_lines,
            lines: current_lines,
        });
    }

    hunks
}

fn parse_hunk_header(line: &str) -> Option<(u32, u32, u32, u32)> {
    let line = line.trim_start_matches('@').trim_start_matches(' ');
    let line = line.split_whitespace().next()?;
    let line = line.trim_matches('@');

    let (old_part, new_part) = line.split_once(' ')?;
    let old_part = old_part.strip_prefix('-')?;
    let new_part = new_part.strip_prefix('+')?;

    let parse = |s: &str| -> Option<(u32, u32)> {
        let mut parts = s.split(',');
        let start = parts.next()?.parse().ok()?;
        let count = parts.next().unwrap_or("1").parse().ok()?;
        Some((start, count))
    };

    let (os, ol) = parse(old_part)?;
    let (ns, nl) = parse(new_part)?;
    Some((os, ol, ns, nl))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn completed_item(item: Value) -> Notification {
        Notification::ItemCompleted(
            serde_json::from_value(serde_json::json!({
                "threadId": "th1",
                "turnId": "t1",
                "completedAtMs": 1000,
                "item": item,
            }))
            .unwrap(),
        )
    }

    fn started_item(item: Value) -> Notification {
        started_item_in_turn(item, "t1")
    }

    fn started_item_in_turn(item: Value, turn_id: &str) -> Notification {
        Notification::ItemStarted(
            serde_json::from_value(serde_json::json!({
                "threadId": "th1",
                "turnId": turn_id,
                "startedAtMs": 500,
                "item": item,
            }))
            .unwrap(),
        )
    }

    fn turn_started(turn_id: &str) -> Notification {
        Notification::TurnStarted(
            serde_json::from_value(serde_json::json!({
                "threadId": "th1",
                "turn": { "id": turn_id, "status": "inProgress" }
            }))
            .unwrap(),
        )
    }

    fn turn_completed(turn_id: &str) -> Notification {
        Notification::TurnCompleted(
            serde_json::from_value(serde_json::json!({
                "threadId": "th1",
                "turn": { "id": turn_id, "status": "completed" }
            }))
            .unwrap(),
        )
    }

    fn text_delta(event: AgentEvent) -> String {
        match event {
            AgentEvent::ItemDelta {
                delta: ItemDelta::Text { text },
                ..
            } => text,
            other => panic!("expected text delta, got {other:?}"),
        }
    }

    /// Usage from a `thread/tokenUsage/updated` notification is cached per turn and surfaced on the
    /// matching `TurnCompleted` (spec §10.1) — regression guard for the previous zero stub.
    #[test]
    fn token_usage_attached_on_turn_completed() {
        let mut mapper = CodexMapper::new(PathBuf::from("/tmp"));
        let fallback = ThreadId::new();

        let usage_notif = Notification::ThreadTokenUsageUpdated(
            serde_json::from_value(serde_json::json!({
                "threadId": "th1",
                "turnId": "t1",
                "tokenUsage": {
                    "last": {
                        "cachedInputTokens": 10, "inputTokens": 100,
                        "outputTokens": 40, "reasoningOutputTokens": 5, "totalTokens": 140
                    },
                    "total": {
                        "cachedInputTokens": 10, "inputTokens": 100,
                        "outputTokens": 40, "reasoningOutputTokens": 5, "totalTokens": 140
                    }
                }
            }))
            .unwrap(),
        );
        // The intermediate update emits no Giskard event.
        assert!(mapper.map_notification(&usage_notif, fallback).is_none());

        let completed = Notification::TurnCompleted(
            serde_json::from_value(serde_json::json!({
                "threadId": "th1",
                "turn": { "id": "t1", "status": "completed" }
            }))
            .unwrap(),
        );
        match mapper.map_notification(&completed, fallback).unwrap() {
            AgentEvent::TurnCompleted { usage, .. } => {
                assert_eq!(usage.input, 100);
                assert_eq!(usage.output, 40);
                assert_eq!(usage.total, 140);
            }
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }

    #[test]
    fn file_change_item_preserves_all_changed_paths() {
        let mut mapper = CodexMapper::new(PathBuf::from("/tmp"));
        let notif = completed_item(serde_json::json!({
            "type": "fileChange",
            "id": "fc1",
            "status": "completed",
            "changes": [
                { "path": "src/main.rs", "kind": { "type": "update" }, "diff": "@@ -1 +1 @@\n-old\n+new" },
                { "path": "src/lib.rs", "kind": { "type": "add" }, "diff": "" }
            ]
        }));

        match mapper.map_notification(&notif, ThreadId::new()).unwrap() {
            AgentEvent::ItemCompleted { item, .. } => match item.payload {
                ItemPayload::FileChange {
                    path,
                    change,
                    changes,
                    status,
                } => {
                    assert_eq!(path, PathBuf::from("src/main.rs"));
                    assert_eq!(change, FileChangeKind::Modified);
                    assert_eq!(changes.len(), 2);
                    assert_eq!(changes[0].change, FileChangeKind::Modified);
                    assert_eq!(changes[1].change, FileChangeKind::Created);
                    assert_eq!(status.as_deref(), Some("completed"));
                }
                other => panic!("expected file change, got {other:?}"),
            },
            other => panic!("expected item completion, got {other:?}"),
        }
    }

    #[test]
    fn command_item_preserves_running_metadata() {
        let mut mapper = CodexMapper::new(PathBuf::from("/tmp"));
        let start = started_item(serde_json::json!({
            "type": "commandExecution",
            "id": "cmd1",
            "command": "sleep 60",
            "cwd": "/tmp/project",
            "commandActions": [],
            "status": "inProgress",
            "processId": "proc_1"
        }));

        match mapper.map_notification(&start, ThreadId::new()).unwrap() {
            AgentEvent::ItemStarted { item, .. } => {
                assert_eq!(item.kind, ItemKind::CommandExecution);
                let command = item.command.expect("command metadata");
                assert_eq!(command.command, "sleep 60");
                assert_eq!(command.cwd, "/tmp/project");
                assert_eq!(command.status.as_deref(), Some("in_progress"));
                assert_eq!(command.process_id.as_deref(), Some("proc_1"));
                assert_eq!(command.started_at_ms, Some(500));
            }
            other => panic!("expected command item start, got {other:?}"),
        }
        assert!(mapper.has_running_commands());

        let completed = completed_item(serde_json::json!({
            "type": "commandExecution",
            "id": "cmd1",
            "command": "sleep 60",
            "cwd": "/tmp/project",
            "commandActions": [],
            "aggregatedOutput": "",
            "status": "failed",
            "exitCode": 130,
            "processId": "proc_1",
            "durationMs": 60000
        }));

        match mapper
            .map_notification(&completed, ThreadId::new())
            .unwrap()
        {
            AgentEvent::ItemCompleted { item, .. } => match item.payload {
                ItemPayload::CommandExecution {
                    command,
                    cwd,
                    status,
                    process_id,
                    exit_code,
                    duration_ms,
                    ..
                } => {
                    assert_eq!(command, "sleep 60");
                    assert_eq!(cwd, PathBuf::from("/tmp/project"));
                    assert_eq!(status.as_deref(), Some("failed"));
                    assert_eq!(process_id.as_deref(), Some("proc_1"));
                    assert_eq!(exit_code, Some(130));
                    assert_eq!(duration_ms, Some(60000));
                }
                other => panic!("expected command execution, got {other:?}"),
            },
            other => panic!("expected command item completion, got {other:?}"),
        }
        assert!(!mapper.has_running_commands());
    }

    #[test]
    fn command_process_tracks_native_turn_for_interrupt() {
        let mut mapper = CodexMapper::new(PathBuf::from("/tmp"));
        let thread = ThreadId::new();

        assert!(matches!(
            mapper.map_notification(&turn_started("native_turn_1"), thread),
            Some(AgentEvent::TurnStarted { .. })
        ));
        assert_eq!(
            mapper.active_native_turn_for_thread(thread),
            Some("native_turn_1")
        );

        let start = started_item_in_turn(
            serde_json::json!({
                "type": "commandExecution",
                "id": "cmd1",
                "command": "sleep 60",
                "cwd": "/tmp/project",
                "commandActions": [],
                "status": "inProgress",
                "processId": "proc_1"
            }),
            "native_turn_1",
        );
        assert!(matches!(
            mapper.map_notification(&start, thread),
            Some(AgentEvent::ItemStarted { .. })
        ));
        assert_eq!(
            mapper.native_turn_for_process(thread, "proc_1"),
            Some("native_turn_1")
        );

        let completed = completed_item(serde_json::json!({
            "type": "commandExecution",
            "id": "cmd1",
            "command": "sleep 60",
            "cwd": "/tmp/project",
            "commandActions": [],
            "aggregatedOutput": "",
            "status": "failed",
            "exitCode": 130,
            "processId": "proc_1",
            "durationMs": 60000
        }));
        assert!(matches!(
            mapper.map_notification(&completed, thread),
            Some(AgentEvent::ItemCompleted { .. })
        ));
        assert_eq!(mapper.native_turn_for_process(thread, "proc_1"), None);

        assert!(matches!(
            mapper.map_notification(&turn_completed("native_turn_1"), thread),
            Some(AgentEvent::TurnCompleted { .. })
        ));
        assert_eq!(mapper.active_native_turn_for_thread(thread), None);
    }

    #[test]
    fn mcp_tool_call_item_preserves_tool_fields() {
        let mut mapper = CodexMapper::new(PathBuf::from("/tmp"));
        let notif = completed_item(serde_json::json!({
            "type": "mcpToolCall",
            "id": "tool1",
            "server": "cf-tools",
            "tool": "jira_search",
            "arguments": { "jql": "project = ERE" },
            "status": "failed",
            "error": { "message": "bad query" }
        }));

        match mapper.map_notification(&notif, ThreadId::new()).unwrap() {
            AgentEvent::ItemCompleted { item, .. } => match item.payload {
                ItemPayload::ToolCall {
                    name,
                    input,
                    server,
                    status,
                    error,
                    ..
                } => {
                    assert_eq!(name, "jira_search");
                    assert_eq!(input["jql"], "project = ERE");
                    assert_eq!(server.as_deref(), Some("cf-tools"));
                    assert_eq!(status.as_deref(), Some("failed"));
                    assert_eq!(error.as_deref(), Some("bad query"));
                }
                other => panic!("expected tool call, got {other:?}"),
            },
            other => panic!("expected item completion, got {other:?}"),
        }
    }

    #[test]
    fn codex_non_chat_item_maps_to_activity() {
        let mut mapper = CodexMapper::new(PathBuf::from("/tmp"));
        let notif = completed_item(serde_json::json!({
            "type": "webSearch",
            "id": "search1",
            "query": "rust serde",
            "action": { "type": "search", "query": "rust serde" }
        }));

        match mapper.map_notification(&notif, ThreadId::new()).unwrap() {
            AgentEvent::ItemCompleted { item, .. } => match item.payload {
                ItemPayload::Activity { title, detail, .. } => {
                    assert_eq!(title, "Web search");
                    assert_eq!(detail.as_deref(), Some("rust serde"));
                }
                other => panic!("expected activity, got {other:?}"),
            },
            other => panic!("expected item completion, got {other:?}"),
        }
    }

    #[test]
    fn additional_item_deltas_are_mapped() {
        let mut mapper = CodexMapper::new(PathBuf::from("/tmp"));
        let fallback = ThreadId::new();

        let plan = Notification::PlanDelta(
            serde_json::from_value(serde_json::json!({
                "threadId": "th1",
                "turnId": "t1",
                "itemId": "plan1",
                "delta": "step one"
            }))
            .unwrap(),
        );
        assert_eq!(
            text_delta(mapper.map_notification(&plan, fallback).unwrap()),
            "step one"
        );

        let patch = Notification::FileChangePatchUpdated(
            serde_json::from_value(serde_json::json!({
                "threadId": "th1",
                "turnId": "t1",
                "itemId": "fc1",
                "changes": [
                    { "path": "src/main.rs", "kind": { "type": "delete" }, "diff": "" }
                ]
            }))
            .unwrap(),
        );
        assert_eq!(
            text_delta(mapper.map_notification(&patch, fallback).unwrap()),
            "deleted src/main.rs"
        );

        let reasoning = Notification::ReasoningTextDelta(
            serde_json::from_value(serde_json::json!({
                "threadId": "th1",
                "turnId": "t1",
                "itemId": "r1",
                "delta": "thinking"
            }))
            .unwrap(),
        );
        assert_eq!(
            text_delta(mapper.map_notification(&reasoning, fallback).unwrap()),
            "thinking"
        );

        let progress = Notification::McpToolCallProgress(
            serde_json::from_value(serde_json::json!({
                "threadId": "th1",
                "turnId": "t1",
                "itemId": "tool1",
                "message": "running"
            }))
            .unwrap(),
        );
        assert_eq!(
            text_delta(mapper.map_notification(&progress, fallback).unwrap()),
            "running"
        );
    }

    fn error_message(event: AgentEvent) -> String {
        match event {
            AgentEvent::Error {
                error: giskard_core::error::HarnessError::Protocol(msg),
                ..
            } => msg,
            other => panic!("expected protocol error, got {other:?}"),
        }
    }

    /// An `error` notification must surface Codex's real message and classification — not the old
    /// `protocol error: error` placeholder that discarded `message`/`codexErrorInfo`.
    #[test]
    fn error_notification_surfaces_message_and_classification() {
        let mut mapper = CodexMapper::new(PathBuf::from("/tmp"));
        let fallback = ThreadId::new();

        // Real cause available in `message` + structured `codexErrorInfo`.
        let unauthorized = Notification::Error(
            serde_json::from_value(serde_json::json!({
                "threadId": "th1",
                "turnId": "t1",
                "willRetry": false,
                "error": {
                    "message": "No API key configured for provider openai",
                    "codexErrorInfo": "unauthorized"
                }
            }))
            .unwrap(),
        );
        let msg = error_message(mapper.map_notification(&unauthorized, fallback).unwrap());
        assert_eq!(
            msg,
            "unauthorized: No API key configured for provider openai"
        );

        // Connection failure carries an HTTP status; retry is flagged.
        let http = Notification::Error(
            serde_json::from_value(serde_json::json!({
                "willRetry": true,
                "error": {
                    "message": "stream failed",
                    "codexErrorInfo": { "httpConnectionFailed": { "httpStatusCode": 503 } }
                }
            }))
            .unwrap(),
        );
        assert_eq!(
            error_message(mapper.map_notification(&http, fallback).unwrap()),
            "httpConnectionFailed (HTTP 503): stream failed (retrying)"
        );

        // Even with an empty error object we never emit a bare "error".
        let empty = Notification::Error(
            serde_json::from_value(serde_json::json!({ "error": {} })).unwrap(),
        );
        assert_eq!(
            error_message(mapper.map_notification(&empty, fallback).unwrap()),
            "Codex reported an unspecified error"
        );
    }

    /// A non-retryable `error` is fatal (drives a synthesized Failed turn); a retryable one is not.
    #[test]
    fn fatal_turn_error_detects_non_retryable_errors() {
        let fatal: Notification = Notification::Error(
            serde_json::from_value(serde_json::json!({
                "willRetry": false,
                "error": { "message": "Quota exceeded", "codexErrorInfo": "usageLimitExceeded" }
            }))
            .unwrap(),
        );
        assert_eq!(
            fatal_turn_error(&fatal).as_deref(),
            Some("usageLimitExceeded: Quota exceeded")
        );

        let retryable: Notification = Notification::Error(
            serde_json::from_value(serde_json::json!({
                "willRetry": true,
                "error": { "message": "transient blip" }
            }))
            .unwrap(),
        );
        assert!(fatal_turn_error(&retryable).is_none());

        // A non-error notification is never fatal.
        let plan = Notification::PlanDelta(
            serde_json::from_value(serde_json::json!({
                "threadId": "th1", "turnId": "t1", "itemId": "p1", "delta": "x"
            }))
            .unwrap(),
        );
        assert!(fatal_turn_error(&plan).is_none());
    }

    /// A Codex `warning` is a non-fatal advisory: it maps to `Notice`, not `Error`, so it never
    /// fails the turn or the pending message (which previously caused a duplicate user bubble).
    #[test]
    fn warning_notification_maps_to_notice() {
        let mut mapper = CodexMapper::new(PathBuf::from("/tmp"));
        let warn = Notification::Warning(
            serde_json::from_value(serde_json::json!({
                "message": "Model metadata for `glm` not found. Using fallback."
            }))
            .unwrap(),
        );
        match mapper.map_notification(&warn, ThreadId::new()).unwrap() {
            AgentEvent::Notice { message, .. } => {
                assert!(message.contains("Model metadata"), "got {message:?}");
            }
            other => panic!("expected Notice, got {other:?}"),
        }
    }

    /// With no usage notification, a completed turn reports zero (not a panic).
    #[test]
    fn token_usage_defaults_to_zero() {
        let mut mapper = CodexMapper::new(PathBuf::from("/tmp"));
        let completed = Notification::TurnCompleted(
            serde_json::from_value(serde_json::json!({
                "threadId": "th1",
                "turn": { "id": "t9", "status": "completed" }
            }))
            .unwrap(),
        );
        match mapper
            .map_notification(&completed, ThreadId::new())
            .unwrap()
        {
            AgentEvent::TurnCompleted { usage, .. } => assert_eq!(usage.total, 0),
            other => panic!("expected TurnCompleted, got {other:?}"),
        }
    }
}
