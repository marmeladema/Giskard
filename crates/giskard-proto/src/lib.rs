//! Shared client↔server wire protocol types (spec §13.6).
//!
//! Defined once here so `giskard-server` and `giskard-ui` never disagree on the protocol.

use serde::{Deserialize, Serialize};

use chrono::{DateTime, Utc};
use giskard_core::ids::{ProjectId, ThreadId, TurnId};

pub mod wire;
pub use wire::{
    WireAgentEvent, WireApprovalKind, WireApprovalRequest, WireFileDiff, WireHarnessError,
    WireItem, WireItemPayload, WireTurn,
};

// C1/§3.5: `giskard-proto` is the single wire vocabulary. Path-free `giskard-core` domain types
// are re-exported here so `giskard-ui` depends only on this crate; path-bearing streamed types are
// mirrored in `wire` above.
pub use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
pub use giskard_core::diff::{DiffHunk, DiffLine};
pub use giskard_core::error::HarnessError;
pub use giskard_core::event::AgentEvent;
pub use giskard_core::ids::{ApprovalId, ItemId};
pub use giskard_core::item::{
    CommandExecutionStart, FileChangeEntry, FileChangeKind, ItemDelta, ItemKind, ItemStart,
};
pub use giskard_core::model::{Effort, ModelDescriptor, ModelRef};
pub use giskard_core::token::{ByModel, DailyTokenLedger, TokenLedger, TokenUsage};
pub use giskard_core::turn::{ApprovalPolicy, Mode, TurnStatus, TurnStatusKind};

// ---- Client → Server ----

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Subscribe {
        thread_id: ThreadId,
    },
    Unsubscribe {
        thread_id: ThreadId,
    },
    SendInput {
        thread_id: ThreadId,
        text: String,
    },
    SwitchMode {
        thread_id: ThreadId,
        mode: Mode,
    },
    SelectModel {
        thread_id: ThreadId,
        model_ref: ModelRef,
    },
    SetApprovalPolicy {
        thread_id: Option<ThreadId>,
        project_id: Option<ProjectId>,
        policy: ApprovalPolicy,
    },
    Interrupt {
        thread_id: ThreadId,
    },
    TerminateCommand {
        thread_id: ThreadId,
        process_id: String,
    },
    ApprovalDecision {
        request_id: String,
        decision: ApprovalDecision,
    },
    SavePlan {
        thread_id: ThreadId,
        path: String,
    },
    /// Request an older page of history (H6): the `limit` turns before `before` (a `TurnId`
    /// cursor); `before: None` requests the most recent page.
    LoadHistory {
        thread_id: ThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        before: Option<TurnId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
    },
    Ping,
}

// ---- Server → Client ----

/// A persisted thread snapshot sent on subscribe/resync (spec §13.6).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadState {
    pub thread_id: ThreadId,
    pub state: serde_json::Value,
}

/// In-flight turn reconstruction on reconnect (spec §13.6). Carries wire types (§3.5).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveTurnSnapshot {
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    pub accumulated: Vec<WireAgentEvent>,
    pub pending_approval: Option<WireApprovalRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunningCommand {
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    pub item_id: ItemId,
    pub harness_item_id: String,
    pub command: String,
    pub cwd: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<String>,
    pub started_at_ms: i64,
    pub output: String,
    pub after_turn: bool,
    #[serde(default)]
    pub terminating: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub code: String,
    pub severity: ErrorSeverity,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<ThreadId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    Event {
        thread_id: ThreadId,
        agent_event: WireAgentEvent,
    },
    ThreadState(ThreadState),
    /// A page of persisted history (H6), oldest-first; `has_more` if older turns exist before it.
    HistoryPage {
        thread_id: ThreadId,
        turns: Vec<WireTurn>,
        has_more: bool,
    },
    LiveTurnSnapshot(LiveTurnSnapshot),
    RunningCommands {
        thread_id: ThreadId,
        commands: Vec<RunningCommand>,
    },
    TokenUpdate {
        scope: TokenScope,
        ledger: serde_json::Value,
    },
    ApprovalRequest {
        thread_id: ThreadId,
        request: WireApprovalRequest,
    },
    Error {
        #[serde(flatten)]
        error: ErrorInfo,
    },
    Pong,
}

/// Which level of token ledger was updated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenScope {
    Thread,
    Project,
    Global,
}

// ---- HTTP API types ----

#[derive(Debug, Clone, Deserialize)]
pub struct LoginRequest {
    pub password: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct LoginResponse {
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct WsTicketResponse {
    pub ticket: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectSummary {
    pub id: ProjectId,
    pub name: String,
    pub dir: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListProjectsResponse {
    pub projects: Vec<ProjectSummary>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateProjectRequest {
    pub name: String,
    pub dir: String,
    pub workspace_root: Option<String>,
    pub default_model: ModelRef,
    pub approval_policy: ApprovalPolicy,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateProjectResponse {
    pub id: ProjectId,
}

#[derive(Debug, Clone, Serialize)]
pub struct ThreadSummary {
    pub id: ThreadId,
    pub title: String,
    pub mode: Mode,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListThreadsResponse {
    pub threads: Vec<ThreadSummary>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenThreadRequest {
    pub thread_id: Option<ThreadId>,
    pub resume: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenThreadResponse {
    pub thread_id: ThreadId,
    pub harness_thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<ErrorInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
    pub mtime: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BrowseResponse {
    pub path: String,
    pub entries: Vec<DirEntry>,
}

/// A per-provider failure encountered while refreshing the model list from `/v1/models` (§8.3),
/// surfaced so the user sees e.g. a 401 instead of silently getting no models.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderListingWarning {
    pub provider: String,
    pub message: String,
}

/// Static model list for the model picker (spec §8.3).
#[derive(Debug, Clone, Serialize)]
pub struct ListModelsResponse {
    pub models: Vec<ModelDescriptor>,
    /// Non-fatal per-provider discovery failures from a refresh (empty for the static listing).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<ProviderListingWarning>,
}

/// Token dashboard report for a project or the global scope (spec §10.2). All figures reuse the
/// one [`TokenUsage`] struct (B3); the day/week/month windows are derived from `by_day` on read.
#[derive(Debug, Clone, Serialize)]
pub struct TokenReport {
    pub total: TokenUsage,
    pub today: TokenUsage,
    pub this_week: TokenUsage,
    pub this_month: TokenUsage,
    pub by_day: std::collections::BTreeMap<String, TokenUsage>,
    pub by_model: ByModel,
    /// Estimated spend in euros, present only when cost estimation is enabled (§10.4).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_cost_eur: Option<f64>,
}

/// Result of a "Save plan to project" action (spec §7.4.1).
#[derive(Debug, Clone, Serialize)]
pub struct SavePlanResponse {
    /// Path the plan markdown was written to (relative to the project dir when possible).
    pub path: String,
}

/// Syntax-highlighted file content (spec §11.2).
///
/// The overlay displays the file's path, size, and language alongside the
/// highlighted HTML. When `is_binary` is true or the file exceeds the size
/// threshold, `html` is empty and the UI shows a fallback message.
#[derive(Debug, Clone, Serialize)]
pub struct HighlightResponse {
    /// Syntax-highlighted HTML (empty for binary or oversized files).
    pub html: String,
    /// Detected language name (e.g. "Rust", "Python").
    pub language: Option<String>,
    /// True if the file contains null bytes (§11.3 binary detection).
    pub is_binary: bool,
    /// Total number of lines in the file (before range slicing).
    pub total_lines: usize,
    /// File size in bytes (spec §11.2: overlay shows path, size, and language).
    pub file_size: u64,
}

/// A linkified span within agent text (spec §11.2).
#[derive(Debug, Clone, Serialize)]
pub struct LinkSpanResponse {
    pub start: usize,
    pub end: usize,
    pub path: String,
    /// Optional 1-based source line parsed from `path#<line>`, `path:<line>`,
    /// or `path:<line>:<column>` suffixes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<usize>,
}

/// Result of path linkification (spec §11.2).
#[derive(Debug, Clone, Serialize)]
pub struct LinkifyResponse {
    pub links: Vec<LinkSpanResponse>,
}

/// Request body for linkification (spec §11.2).
#[derive(Debug, Clone, Deserialize)]
pub struct LinkifyRequest {
    pub text: String,
}

/// Request to create a directory under `parent` (filesystem picker "New folder"). `name` is a
/// single path segment; the server rejects separators and `.`/`..` and enforces the browse roots.
#[derive(Debug, Clone, Deserialize)]
pub struct MkdirRequest {
    pub parent: String,
    pub name: String,
}

/// The canonical path of the directory created via [`MkdirRequest`].
#[derive(Debug, Clone, Serialize)]
pub struct MkdirResponse {
    pub path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_message_send_input_serde() {
        let msg = ClientMessage::SendInput {
            thread_id: ThreadId::new(),
            text: "Refactor the auth module".into(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"type\":\"send_input\""));
        let back: ClientMessage = serde_json::from_str(&json).unwrap();
        match back {
            ClientMessage::SendInput { text, .. } => assert_eq!(text, "Refactor the auth module"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn client_message_terminate_command_serde() {
        let tid = ThreadId::new();
        let msg = ClientMessage::TerminateCommand {
            thread_id: tid,
            process_id: "proc_1".into(),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "terminate_command");
        assert_eq!(json["thread_id"], tid.to_string());
        assert_eq!(json["process_id"], "proc_1");
        let back: ClientMessage = serde_json::from_value(json).unwrap();
        match back {
            ClientMessage::TerminateCommand { process_id, .. } => {
                assert_eq!(process_id, "proc_1");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn client_message_ping() {
        let json = serde_json::to_string(&ClientMessage::Ping).unwrap();
        assert_eq!(json, "{\"type\":\"ping\"}");
    }

    #[test]
    fn server_message_pong() {
        let json = serde_json::to_string(&ServerMessage::Pong).unwrap();
        assert_eq!(json, "{\"type\":\"pong\"}");
    }

    #[test]
    fn server_message_running_commands_serde() {
        let tid = ThreadId::new();
        let turn_id = TurnId::new();
        let item_id = ItemId::new();
        let msg = ServerMessage::RunningCommands {
            thread_id: tid,
            commands: vec![RunningCommand {
                thread_id: tid,
                turn_id,
                item_id,
                harness_item_id: "cmd1".into(),
                command: "sleep 60".into(),
                cwd: "/tmp/project".into(),
                status: "in_progress".into(),
                process_id: Some("proc_1".into()),
                started_at_ms: 1_785_000_000_000,
                output: "waiting".into(),
                after_turn: true,
                terminating: true,
            }],
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "running_commands");
        assert_eq!(json["thread_id"], tid.to_string());
        assert_eq!(json["commands"][0]["process_id"], "proc_1");
        assert_eq!(json["commands"][0]["after_turn"], true);
        assert_eq!(json["commands"][0]["terminating"], true);
        let back: ServerMessage = serde_json::from_value(json).unwrap();
        match back {
            ServerMessage::RunningCommands { commands, .. } => {
                assert_eq!(commands[0].item_id, item_id);
                assert_eq!(commands[0].status, "in_progress");
                assert!(commands[0].terminating);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn server_message_error_is_flattened() {
        let tid = ThreadId::new();
        let msg = ServerMessage::Error {
            error: ErrorInfo {
                code: "thread_not_found".into(),
                severity: ErrorSeverity::Error,
                message: "Thread not found.".into(),
                detail: Some("missing".into()),
                thread_id: Some(tid),
                action: Some("subscribe".into()),
            },
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "error");
        assert_eq!(json["code"], "thread_not_found");
        assert_eq!(json["message"], "Thread not found.");
        assert_eq!(json["thread_id"], tid.to_string());
        let back: ServerMessage = serde_json::from_value(json).unwrap();
        match back {
            ServerMessage::Error { error } => {
                assert_eq!(error.code, "thread_not_found");
                assert_eq!(error.action.as_deref(), Some("subscribe"));
            }
            _ => panic!("wrong variant"),
        }
    }
}
