//! Mapping between `codex-codes` types and `giskard-core` types (spec §4.6).

use std::path::PathBuf;

use chrono::Utc;
use serde_json::Value;

use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
use giskard_core::diff::{DiffHunk, DiffLine, FileDiff};
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ThreadId};
use giskard_core::item::{FileChangeKind, Item, ItemDelta, ItemKind, ItemPayload, ItemStarted};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{ApprovalPolicy, Mode, TurnStatus, TurnStatusKind};

use codex_codes::jsonrpc::RequestId;
use codex_codes::messages::{Notification, ServerRequest};
use codex_codes::protocol::{
    AgentMessageDeltaNotification, CommandExecutionOutputDeltaNotification,
    ItemCompletedNotification, ItemStartedNotification, TurnCompletedNotification,
    TurnDiffUpdatedNotification, TurnStartedNotification,
};

pub struct CodexMapper {
    _workspace_root: PathBuf,
}

impl CodexMapper {
    pub fn new(workspace_root: PathBuf) -> Self {
        Self {
            _workspace_root: workspace_root,
        }
    }

    pub fn map_notification(&self, notif: &Notification, thread: ThreadId) -> Option<AgentEvent> {
        match notif {
            Notification::TurnStarted(TurnStartedNotification { .. }) => {
                Some(AgentEvent::TurnStarted {
                    thread,
                    turn: giskard_core::TurnId::new(),
                })
            }

            Notification::TurnCompleted(TurnCompletedNotification { turn, .. }) => {
                let usage = extract_token_usage(turn);
                let status = map_turn_status(&turn.status);
                Some(AgentEvent::TurnCompleted {
                    thread,
                    turn: giskard_core::TurnId::new(),
                    usage,
                    status,
                })
            }

            Notification::ItemStarted(ItemStartedNotification { item, .. }) => {
                let (id, kind) = map_thread_item_start(item);
                Some(AgentEvent::ItemStarted {
                    thread,
                    turn: giskard_core::TurnId::new(),
                    item: ItemStarted { id, kind },
                })
            }

            Notification::ItemCompleted(ItemCompletedNotification {
                item,
                completed_at_ms,
                ..
            }) => {
                let giskard_item = map_thread_item_complete(item, *completed_at_ms);
                Some(AgentEvent::ItemCompleted {
                    thread,
                    turn: giskard_core::TurnId::new(),
                    item: giskard_item,
                })
            }

            Notification::AgentMessageDelta(AgentMessageDeltaNotification {
                delta,
                item_id,
                ..
            }) => Some(AgentEvent::ItemDelta {
                thread,
                turn: giskard_core::TurnId::new(),
                item_id: ItemId(item_id.clone()),
                delta: ItemDelta::Text {
                    text: delta.clone(),
                },
            }),

            Notification::CmdOutputDelta(CommandExecutionOutputDeltaNotification {
                delta,
                item_id,
                ..
            }) => Some(AgentEvent::ItemDelta {
                thread,
                turn: giskard_core::TurnId::new(),
                item_id: ItemId(item_id.clone()),
                delta: ItemDelta::CommandOutput {
                    chunk: delta.clone(),
                },
            }),

            Notification::ReasoningDelta(n) => Some(AgentEvent::ItemDelta {
                thread,
                turn: giskard_core::TurnId::new(),
                item_id: ItemId(String::new()),
                delta: ItemDelta::Text {
                    text: n.delta.clone(),
                },
            }),

            Notification::TurnDiffUpdated(TurnDiffUpdatedNotification { diff, .. }) => {
                Some(AgentEvent::DiffUpdated {
                    thread,
                    turn: giskard_core::TurnId::new(),
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
                    thread,
                    turn: Some(giskard_core::TurnId::new()),
                    error: giskard_core::error::HarnessError::Protocol(msg),
                })
            }

            _ => None,
        }
    }

    pub fn map_server_request(
        &self,
        id: &RequestId,
        request: &ServerRequest,
        thread: ThreadId,
    ) -> Option<AgentEvent> {
        let req_id = match id {
            RequestId::Integer(i) => ApprovalId(i.to_string()),
            RequestId::String(s) => ApprovalId(s.clone()),
        };

        match request {
            ServerRequest::CmdExecApproval(params) => Some(AgentEvent::ApprovalRequested {
                thread,
                turn: giskard_core::TurnId::new(),
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
            }),
            ServerRequest::FileChangeApproval(params) => Some(AgentEvent::ApprovalRequested {
                thread,
                turn: giskard_core::TurnId::new(),
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
            }),
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
    match effort {
        giskard_core::model::Effort::Medium => codex_codes::ReasoningEffort("medium".into()),
        giskard_core::model::Effort::High => codex_codes::ReasoningEffort("high".into()),
        giskard_core::model::Effort::XHigh => codex_codes::ReasoningEffort("xhigh".into()),
    }
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

fn extract_token_usage(_turn: &codex_codes::Turn) -> TokenUsage {
    // TODO: inspect the pinned schema for the exact field name (spec §10.3).
    TokenUsage::default()
}

fn map_thread_item_start(item: &codex_codes::ThreadItem) -> (ItemId, ItemKind) {
    let (id, kind) = match item {
        codex_codes::ThreadItem::UserMessage { id, .. } => (id.as_str(), ItemKind::UserMessage),
        codex_codes::ThreadItem::AgentMessage { id, .. } => (id.as_str(), ItemKind::AgentMessage),
        codex_codes::ThreadItem::Reasoning { id, .. } => (id.as_str(), ItemKind::Reasoning),
        codex_codes::ThreadItem::CommandExecution { id, .. } => {
            (id.as_str(), ItemKind::CommandExecution)
        }
        codex_codes::ThreadItem::FileChange { id, .. } => (id.as_str(), ItemKind::FileChange),
        codex_codes::ThreadItem::McpToolCall { id, .. } => (id.as_str(), ItemKind::ToolCall),
        _ => ("", ItemKind::AgentMessage),
    };
    (ItemId(id.to_string()), kind)
}

fn map_thread_item_complete(item: &codex_codes::ThreadItem, completed_at_ms: i64) -> Item {
    let (id, payload) = match item {
        codex_codes::ThreadItem::UserMessage { id, content, .. } => {
            let text = content
                .iter()
                .filter_map(|c| match c {
                    codex_codes::UserInput::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            (id.clone(), ItemPayload::UserMessage { text })
        }
        codex_codes::ThreadItem::AgentMessage { id, text, .. } => {
            (id.clone(), ItemPayload::AgentMessage { text: text.clone() })
        }
        codex_codes::ThreadItem::Reasoning { id, summary, .. } => {
            let text = summary.as_ref().map(|s| s.join("\n")).unwrap_or_default();
            (id.clone(), ItemPayload::Reasoning { text })
        }
        codex_codes::ThreadItem::CommandExecution {
            id,
            command,
            cwd,
            aggregated_output,
            exit_code,
            ..
        } => {
            let output = aggregated_output.clone().unwrap_or_default();
            let exit = exit_code.map(|c| c as i32);
            (
                id.clone(),
                ItemPayload::CommandExecution {
                    command: command.clone(),
                    cwd: PathBuf::from(cwd.to_string()),
                    output,
                    exit_code: exit,
                },
            )
        }
        codex_codes::ThreadItem::FileChange { id, changes, .. } => {
            let path = changes
                .first()
                .map(|c| PathBuf::from(&c.path))
                .unwrap_or_default();
            (
                id.clone(),
                ItemPayload::FileChange {
                    path,
                    change: FileChangeKind::Modified,
                },
            )
        }
        codex_codes::ThreadItem::McpToolCall { id, .. } => (
            id.clone(),
            ItemPayload::ToolCall {
                name: String::new(),
                input: Value::Null,
                output: None,
            },
        ),
        _ => (
            String::new(),
            ItemPayload::AgentMessage {
                text: String::new(),
            },
        ),
    };

    let created_at =
        chrono::DateTime::from_timestamp_millis(completed_at_ms).unwrap_or_else(Utc::now);

    Item {
        id: ItemId(id),
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
