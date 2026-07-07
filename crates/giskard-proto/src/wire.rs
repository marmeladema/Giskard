//! Wire-mirror types for the streamed event tree (spec §3.5, C1/C2).
//!
//! `giskard-core` is authoritative and native-facing: it holds `PathBuf` internally, which
//! round-trips **lossily** through JSON for non-UTF-8 paths. The browser must never see that, so
//! any payload that carries a path is mirrored here with a plain UTF-8 `String`, and the server
//! maps `core → wire` once, at the outbound fan-out boundary, via `Path::to_string_lossy()`.
//!
//! Path-free domain types (ids, `ItemStart`, `ItemDelta`, `DiffHunk`/`DiffLine`, `FileChangeKind`,
//! `TokenUsage`, `TurnStatus`, `ApprovalDecision`) are **not** mirrored — they are re-exported
//! from `giskard-core` by this crate's `lib.rs`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
use giskard_core::diff::{DiffHunk, FileDiff};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ThreadId, TurnId};
use giskard_core::item::{FileChangeKind, Item, ItemDelta, ItemPayload, ItemStart};
use giskard_core::token::TokenUsage;
use giskard_core::turn::TurnStatus;

fn path_to_wire(p: &std::path::Path) -> String {
    p.to_string_lossy().into_owned()
}

/// Wire-mirror of [`AgentEvent`] with UTF-8 `String` paths (spec §3.5).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WireAgentEvent {
    ThreadOpened {
        thread: ThreadId,
        harness_thread_id: String,
    },
    TurnStarted {
        thread: ThreadId,
        turn: TurnId,
    },
    ItemStarted {
        thread: ThreadId,
        turn: TurnId,
        item: ItemStart,
    },
    ItemDelta {
        thread: ThreadId,
        turn: TurnId,
        item_id: ItemId,
        delta: ItemDelta,
    },
    ItemCompleted {
        thread: ThreadId,
        turn: TurnId,
        item: WireItem,
    },
    DiffUpdated {
        thread: ThreadId,
        turn: TurnId,
        diff: WireFileDiff,
    },
    ApprovalRequested {
        thread: ThreadId,
        turn: TurnId,
        request: WireApprovalRequest,
    },
    TurnCompleted {
        thread: ThreadId,
        turn: TurnId,
        usage: TokenUsage,
        status: TurnStatus,
    },
    Error {
        thread: ThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        turn: Option<TurnId>,
        error: WireHarnessError,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireHarnessError {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Wire-mirror of [`Item`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireItem {
    pub id: ItemId,
    pub harness_item_id: String,
    pub payload: WireItemPayload,
    pub created_at: DateTime<Utc>,
}

/// Wire-mirror of [`ItemPayload`] (paths as `String`; `serde_json::Value` kept as-is).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WireItemPayload {
    UserMessage {
        text: String,
    },
    AgentMessage {
        text: String,
    },
    Reasoning {
        text: String,
    },
    CommandExecution {
        command: String,
        cwd: String,
        output: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
    },
    FileChange {
        path: String,
        change: FileChangeKind,
    },
    ToolCall {
        name: String,
        input: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output: Option<serde_json::Value>,
    },
}

/// Wire-mirror of [`FileDiff`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireFileDiff {
    pub path: String,
    pub change: FileChangeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_text: Option<String>,
    #[serde(default)]
    pub hunks: Vec<DiffHunk>,
    #[serde(default)]
    pub binary: bool,
}

/// Wire-mirror of [`ApprovalRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireApprovalRequest {
    pub id: ApprovalId,
    pub kind: WireApprovalKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub available: Vec<ApprovalDecision>,
}

/// Wire-mirror of [`ApprovalKind`] (paths as `String`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WireApprovalKind {
    CommandExecution {
        command: String,
        cwd: String,
    },
    FileChange {
        path: String,
        change: FileChangeKind,
    },
    Permission {
        detail: String,
    },
}

// ---- core → wire conversions (lossy path narrowing happens here, server-side) ----

impl From<AgentEvent> for WireAgentEvent {
    fn from(e: AgentEvent) -> Self {
        match e {
            AgentEvent::ThreadOpened {
                thread,
                harness_thread_id,
            } => Self::ThreadOpened {
                thread,
                harness_thread_id,
            },
            AgentEvent::TurnStarted { thread, turn } => Self::TurnStarted { thread, turn },
            AgentEvent::ItemStarted { thread, turn, item } => {
                Self::ItemStarted { thread, turn, item }
            }
            AgentEvent::ItemDelta {
                thread,
                turn,
                item_id,
                delta,
            } => Self::ItemDelta {
                thread,
                turn,
                item_id,
                delta,
            },
            AgentEvent::ItemCompleted { thread, turn, item } => Self::ItemCompleted {
                thread,
                turn,
                item: item.into(),
            },
            AgentEvent::DiffUpdated { thread, turn, diff } => Self::DiffUpdated {
                thread,
                turn,
                diff: diff.into(),
            },
            AgentEvent::ApprovalRequested {
                thread,
                turn,
                request,
            } => Self::ApprovalRequested {
                thread,
                turn,
                request: request.into(),
            },
            AgentEvent::TurnCompleted {
                thread,
                turn,
                usage,
                status,
            } => Self::TurnCompleted {
                thread,
                turn,
                usage,
                status,
            },
            AgentEvent::Error {
                thread,
                turn,
                error,
            } => Self::Error {
                thread,
                turn,
                error: error.into(),
            },
        }
    }
}

impl From<HarnessError> for WireHarnessError {
    fn from(error: HarnessError) -> Self {
        let code = match &error {
            HarnessError::Spawn(_) => "harness_spawn_failed",
            HarnessError::NotInitialized => "harness_not_initialized",
            HarnessError::Unauthenticated => "harness_unauthenticated",
            HarnessError::Transport(_) => "harness_transport_error",
            HarnessError::Protocol(_) => "harness_protocol_error",
            HarnessError::Overloaded => "harness_overloaded",
            HarnessError::Unsupported(_) => "harness_unsupported",
            HarnessError::ThreadNotFound(_) => "thread_not_open",
            HarnessError::Timeout(_) => "harness_timeout",
        };
        Self {
            code: code.into(),
            message: error.to_string(),
            detail: None,
        }
    }
}

impl From<Item> for WireItem {
    fn from(i: Item) -> Self {
        Self {
            id: i.id,
            harness_item_id: i.harness_item_id,
            payload: i.payload.into(),
            created_at: i.created_at,
        }
    }
}

impl From<ItemPayload> for WireItemPayload {
    fn from(p: ItemPayload) -> Self {
        match p {
            ItemPayload::UserMessage { text } => Self::UserMessage { text },
            ItemPayload::AgentMessage { text } => Self::AgentMessage { text },
            ItemPayload::Reasoning { text } => Self::Reasoning { text },
            ItemPayload::CommandExecution {
                command,
                cwd,
                output,
                exit_code,
            } => Self::CommandExecution {
                command,
                cwd: path_to_wire(&cwd),
                output,
                exit_code,
            },
            ItemPayload::FileChange { path, change } => Self::FileChange {
                path: path_to_wire(&path),
                change,
            },
            ItemPayload::ToolCall {
                name,
                input,
                output,
            } => Self::ToolCall {
                name,
                input,
                output,
            },
        }
    }
}

impl From<FileDiff> for WireFileDiff {
    fn from(d: FileDiff) -> Self {
        Self {
            path: path_to_wire(&d.path),
            change: d.change,
            old_text: d.old_text,
            new_text: d.new_text,
            hunks: d.hunks,
            binary: d.binary,
        }
    }
}

impl From<ApprovalRequest> for WireApprovalRequest {
    fn from(r: ApprovalRequest) -> Self {
        Self {
            id: r.id,
            kind: r.kind.into(),
            reason: r.reason,
            available: r.available,
        }
    }
}

impl From<ApprovalKind> for WireApprovalKind {
    fn from(k: ApprovalKind) -> Self {
        match k {
            ApprovalKind::CommandExecution { command, cwd } => Self::CommandExecution {
                command,
                cwd: path_to_wire(&cwd),
            },
            ApprovalKind::FileChange { path, change } => Self::FileChange {
                path: path_to_wire(&path),
                change,
            },
            ApprovalKind::Permission { detail } => Self::Permission { detail },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use giskard_core::approval::ApprovalKind;
    use std::path::PathBuf;

    #[test]
    fn approval_kind_path_becomes_string() {
        let core = ApprovalKind::CommandExecution {
            command: "ls".into(),
            cwd: PathBuf::from("/home/elie/dev"),
        };
        let wire: WireApprovalKind = core.into();
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["cwd"], "/home/elie/dev");
        assert_eq!(json["kind"], "command_execution");
    }

    #[test]
    fn item_payload_path_becomes_string() {
        let core = ItemPayload::FileChange {
            path: PathBuf::from("/src/main.rs"),
            change: FileChangeKind::Modified,
        };
        let wire: WireItemPayload = core.into();
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["path"], "/src/main.rs");
    }

    #[test]
    fn agent_event_error_is_serializable() {
        let core = AgentEvent::Error {
            thread: ThreadId::new(),
            turn: None,
            error: HarnessError::Protocol("bad frame".into()),
        };
        let wire: WireAgentEvent = core.into();
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["kind"], "error");
        assert_eq!(json["error"]["code"], "harness_protocol_error");
        assert_eq!(json["error"]["message"], "protocol error: bad frame");
    }
}
