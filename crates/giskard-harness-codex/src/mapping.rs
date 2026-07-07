//! Mapping between `codex-codes` types and `giskard-core` types (spec §4.6).

use std::collections::HashMap;
use std::path::PathBuf;

use chrono::Utc;
use serde_json::Value;

use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
use giskard_core::diff::{DiffHunk, DiffLine, FileDiff};
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ThreadId, TurnId};
use giskard_core::item::{FileChangeKind, Item, ItemDelta, ItemKind, ItemPayload, ItemStart};
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
}

impl CodexMapper {
    pub fn new(workspace_root: PathBuf) -> Self {
        Self {
            _workspace_root: workspace_root,
            thread_ids: HashMap::new(),
            turn_ids: HashMap::new(),
            item_ids: HashMap::new(),
            turn_usage: HashMap::new(),
        }
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
                Some(AgentEvent::TurnStarted {
                    thread,
                    turn: self.resolve_turn(&turn.id),
                })
            }

            Notification::TurnCompleted(TurnCompletedNotification { thread_id, turn }) => {
                let thread = self.resolve_thread(thread_id, fallback_thread);
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
                thread_id,
                turn_id,
                ..
            }) => {
                let thread = self.resolve_thread(thread_id, fallback_thread);
                let turn = self.resolve_turn(turn_id);
                let (harness_item_id, kind) = map_thread_item_start(item);
                let id = self.resolve_item(&harness_item_id);
                Some(AgentEvent::ItemStarted {
                    thread,
                    turn,
                    item: ItemStart {
                        id,
                        harness_item_id,
                        kind,
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
            }) => {
                let thread = self.resolve_thread(thread_id, fallback_thread);
                let turn = self.resolve_turn(turn_id);
                Some(AgentEvent::ItemDelta {
                    thread,
                    turn,
                    item_id: self.resolve_item(item_id),
                    delta: ItemDelta::Text {
                        text: delta.clone(),
                    },
                })
            }

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

            Notification::ReasoningDelta(n) => {
                let thread = self.resolve_thread(&n.thread_id, fallback_thread);
                let turn = self.resolve_turn(&n.turn_id);
                Some(AgentEvent::ItemDelta {
                    thread,
                    turn,
                    item_id: self.resolve_item(&n.item_id),
                    delta: ItemDelta::Text {
                        text: n.delta.clone(),
                    },
                })
            }

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
                let msg = n
                    .error
                    .additional_details
                    .clone()
                    .unwrap_or_else(|| "error".into());
                Some(AgentEvent::Error {
                    thread: fallback_thread,
                    turn: None,
                    error: giskard_core::error::HarnessError::Protocol(msg),
                })
            }

            _ => None,
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
        | codex_codes::ThreadItem::AgentMessage { id, .. }
        | codex_codes::ThreadItem::Reasoning { id, .. }
        | codex_codes::ThreadItem::CommandExecution { id, .. }
        | codex_codes::ThreadItem::FileChange { id, .. }
        | codex_codes::ThreadItem::McpToolCall { id, .. } => id.clone(),
        _ => String::new(),
    }
}

/// Returns the native item id + Giskard `ItemKind` for an item at `item/started`.
fn map_thread_item_start(item: &codex_codes::ThreadItem) -> (String, ItemKind) {
    let kind = match item {
        codex_codes::ThreadItem::UserMessage { .. } => ItemKind::UserMessage,
        codex_codes::ThreadItem::AgentMessage { .. } => ItemKind::AgentMessage,
        codex_codes::ThreadItem::Reasoning { .. } => ItemKind::Reasoning,
        codex_codes::ThreadItem::CommandExecution { .. } => ItemKind::CommandExecution,
        codex_codes::ThreadItem::FileChange { .. } => ItemKind::FileChange,
        codex_codes::ThreadItem::McpToolCall { .. } => ItemKind::ToolCall,
        _ => ItemKind::AgentMessage,
    };
    (thread_item_id(item), kind)
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
        codex_codes::ThreadItem::Reasoning { summary, .. } => {
            let text = summary.as_ref().map(|s| s.join("\n")).unwrap_or_default();
            ItemPayload::Reasoning { text }
        }
        codex_codes::ThreadItem::CommandExecution {
            command,
            cwd,
            aggregated_output,
            exit_code,
            ..
        } => {
            let output = aggregated_output.clone().unwrap_or_default();
            let exit = exit_code.map(|c| c as i32);
            ItemPayload::CommandExecution {
                command: command.clone(),
                cwd: PathBuf::from(cwd.to_string()),
                output,
                exit_code: exit,
            }
        }
        codex_codes::ThreadItem::FileChange { changes, .. } => {
            let path = changes
                .first()
                .map(|c| PathBuf::from(&c.path))
                .unwrap_or_default();
            ItemPayload::FileChange {
                path,
                change: FileChangeKind::Modified,
            }
        }
        codex_codes::ThreadItem::McpToolCall { .. } => ItemPayload::ToolCall {
            name: String::new(),
            input: Value::Null,
            output: None,
        },
        _ => ItemPayload::AgentMessage {
            text: String::new(),
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
