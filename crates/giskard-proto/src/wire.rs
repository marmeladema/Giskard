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

use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalMetadata, ApprovalRequest};
use giskard_core::diff::{DiffHunk, FileDiff};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{
    FileChangeEntry, FileChangeKind, Item, ItemDelta, ItemPayload, ItemStart,
};
use giskard_core::model::ModelRef;
use giskard_core::server_request::ServerRequest;
use giskard_core::token::TokenUsage;
use giskard_core::turn::{Mode, Turn, TurnStatus};
use giskard_core::user_input::UserInput;

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
    ServerRequestReceived {
        thread: ThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        turn: Option<TurnId>,
        request: ServerRequest,
    },
    ServerRequestResolved {
        thread: ThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        turn: Option<TurnId>,
        request_id: ServerRequestId,
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
    Notice {
        thread: ThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        turn: Option<TurnId>,
        message: String,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        process_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        duration_ms: Option<i64>,
    },
    FileChange {
        path: String,
        change: FileChangeKind,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        changes: Vec<WireFileChangeEntry>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
    },
    ToolCall {
        name: String,
        input: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output: Option<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        server: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    Activity {
        title: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<serde_json::Value>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireFileChangeEntry {
    pub path: String,
    pub change: FileChangeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff: Option<String>,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata: Vec<WireApprovalMetadata>,
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
    McpToolCall {
        server: String,
        tool_name: String,
    },
}

/// Wire-mirror of [`ApprovalMetadata`] (paths as `String`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WireApprovalMetadata {
    Text {
        label: String,
        value: String,
    },
    Path {
        label: String,
        path: String,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        source_link: bool,
    },
    Host {
        label: String,
        host: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        protocol: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        port: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<String>,
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
            AgentEvent::ServerRequestReceived {
                thread,
                turn,
                request,
            } => Self::ServerRequestReceived {
                thread,
                turn,
                request,
            },
            AgentEvent::ServerRequestResolved {
                thread,
                turn,
                request_id,
            } => Self::ServerRequestResolved {
                thread,
                turn,
                request_id,
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
            AgentEvent::Notice {
                thread,
                turn,
                message,
            } => Self::Notice {
                thread,
                turn,
                message,
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
            HarnessError::ThreadBusy { .. } => "thread_turn_active",
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
                status,
                process_id,
                duration_ms,
            } => Self::CommandExecution {
                command,
                cwd: path_to_wire(&cwd),
                output,
                exit_code,
                status,
                process_id,
                duration_ms,
            },
            ItemPayload::FileChange {
                path,
                change,
                changes,
                status,
            } => Self::FileChange {
                path: path_to_wire(&path),
                change,
                changes: changes.into_iter().map(Into::into).collect(),
                status,
            },
            ItemPayload::ToolCall {
                name,
                input,
                output,
                server,
                status,
                error,
            } => Self::ToolCall {
                name,
                input,
                output,
                server,
                status,
                error,
            },
            ItemPayload::Activity {
                title,
                detail,
                metadata,
            } => Self::Activity {
                title,
                detail,
                metadata,
            },
        }
    }
}

impl From<FileChangeEntry> for WireFileChangeEntry {
    fn from(entry: FileChangeEntry) -> Self {
        Self {
            path: path_to_wire(&entry.path),
            change: entry.change,
            diff: entry.diff,
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
            metadata: r.metadata.into_iter().map(Into::into).collect(),
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
            ApprovalKind::McpToolCall { server, tool_name } => {
                Self::McpToolCall { server, tool_name }
            }
        }
    }
}

impl From<ApprovalMetadata> for WireApprovalMetadata {
    fn from(metadata: ApprovalMetadata) -> Self {
        match metadata {
            ApprovalMetadata::Text { label, value } => Self::Text { label, value },
            ApprovalMetadata::Path {
                label,
                path,
                source_link,
            } => Self::Path {
                label,
                path: path_to_wire(&path),
                source_link,
            },
            ApprovalMetadata::Host {
                label,
                host,
                protocol,
                port,
                target,
            } => Self::Host {
                label,
                host,
                protocol,
                port,
                target,
            },
        }
    }
}

/// Wire-mirror of [`Turn`] (§4.5), used by paged history (`HistoryPage`, H6). Items and diffs use
/// their path-mirrored wire forms (C1/§3.5).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireTurn {
    pub id: TurnId,
    pub user_input: UserInput,
    pub items: Vec<WireItem>,
    pub model: ModelRef,
    pub mode: Mode,
    pub status: TurnStatus,
    pub usage: TokenUsage,
    pub diffs: Vec<WireFileDiff>,
    pub started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}

impl From<Turn> for WireTurn {
    fn from(t: Turn) -> Self {
        Self {
            id: t.id,
            user_input: t.user_input,
            items: t.items.into_iter().map(Into::into).collect(),
            model: t.model,
            mode: t.mode,
            status: t.status,
            usage: t.usage,
            diffs: t.diffs.into_iter().map(Into::into).collect(),
            started_at: t.started_at,
            completed_at: t.completed_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use giskard_core::approval::{ApprovalKind, ApprovalMetadata};
    use std::path::PathBuf;

    #[test]
    fn approval_kind_path_becomes_string() {
        let core = ApprovalKind::CommandExecution {
            command: "ls".into(),
            cwd: PathBuf::from("/home/user/dev"),
        };
        let wire: WireApprovalKind = core.into();
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["cwd"], "/home/user/dev");
        assert_eq!(json["kind"], "command_execution");
    }

    #[test]
    fn approval_metadata_paths_become_strings() {
        let request = ApprovalRequest {
            id: ApprovalId("ap_1".into()),
            kind: ApprovalKind::Permission {
                detail: "fileSystem".into(),
            },
            reason: None,
            metadata: vec![
                ApprovalMetadata::Text {
                    label: "Environment".into(),
                    value: "env_1".into(),
                },
                ApprovalMetadata::Path {
                    label: "Write".into(),
                    path: PathBuf::from("/home/user/dev/src/lib.rs"),
                    source_link: true,
                },
                ApprovalMetadata::Host {
                    label: "Network host".into(),
                    host: "api.example.com".into(),
                    protocol: Some("https".into()),
                    port: Some(443),
                    target: Some("https://api.example.com/v1".into()),
                },
                ApprovalMetadata::Path {
                    label: "Grant root".into(),
                    path: PathBuf::from("/home/user/dev"),
                    source_link: false,
                },
            ],
            available: vec![ApprovalDecision::Accept],
        };
        let wire: WireApprovalRequest = request.into();
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["metadata"][0]["kind"], "text");
        assert_eq!(json["metadata"][0]["label"], "Environment");
        assert_eq!(json["metadata"][0]["value"], "env_1");
        assert_eq!(json["metadata"][1]["kind"], "path");
        assert_eq!(json["metadata"][1]["path"], "/home/user/dev/src/lib.rs");
        assert_eq!(json["metadata"][1]["source_link"], true);
        assert_eq!(json["metadata"][2]["kind"], "host");
        assert_eq!(json["metadata"][2]["host"], "api.example.com");
        assert_eq!(json["metadata"][2]["protocol"], "https");
        assert_eq!(json["metadata"][2]["port"], 443);
        assert_eq!(json["metadata"][2]["target"], "https://api.example.com/v1");
        assert_eq!(json["metadata"][3]["kind"], "path");
        assert_eq!(json["metadata"][3]["path"], "/home/user/dev");
        assert!(
            !json["metadata"][3]
                .as_object()
                .unwrap()
                .contains_key("source_link")
        );
    }

    #[test]
    fn item_payload_path_becomes_string() {
        let core = ItemPayload::FileChange {
            path: PathBuf::from("/src/main.rs"),
            change: FileChangeKind::Modified,
            changes: vec![FileChangeEntry {
                path: PathBuf::from("/src/lib.rs"),
                change: FileChangeKind::Created,
                diff: None,
            }],
            status: Some("completed".into()),
        };
        let wire: WireItemPayload = core.into();
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["path"], "/src/main.rs");
        assert_eq!(json["changes"][0]["path"], "/src/lib.rs");
        assert_eq!(json["status"], "completed");
    }

    #[test]
    fn command_execution_payload_metadata_becomes_wire() {
        let core = ItemPayload::CommandExecution {
            command: "cargo test".into(),
            cwd: PathBuf::from("/tmp/project"),
            output: "ok".into(),
            exit_code: Some(0),
            status: Some("completed".into()),
            process_id: Some("proc_1".into()),
            duration_ms: Some(1_250),
        };
        let wire: WireItemPayload = core.into();
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["kind"], "command_execution");
        assert_eq!(json["command"], "cargo test");
        assert_eq!(json["cwd"], "/tmp/project");
        assert_eq!(json["status"], "completed");
        assert_eq!(json["process_id"], "proc_1");
        assert_eq!(json["duration_ms"], 1_250);
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
