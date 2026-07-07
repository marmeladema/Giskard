//! Shared client↔server wire protocol types (spec §13.6).
//!
//! Defined once here so `giskard-server` and `giskard-ui` never disagree on the protocol.

use serde::{Deserialize, Serialize};

use chrono::{DateTime, Utc};
use giskard_core::ids::{ProjectId, ThreadId, TurnId};

pub mod wire;
pub use wire::{
    WireAgentEvent, WireApprovalKind, WireApprovalRequest, WireFileDiff, WireItem, WireItemPayload,
};

// C1/§3.5: `giskard-proto` is the single wire vocabulary. Path-free `giskard-core` domain types
// are re-exported here so `giskard-ui` depends only on this crate; path-bearing streamed types are
// mirrored in `wire` above.
pub use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
pub use giskard_core::diff::{DiffHunk, DiffLine};
pub use giskard_core::error::HarnessError;
pub use giskard_core::event::AgentEvent;
pub use giskard_core::ids::{ApprovalId, ItemId};
pub use giskard_core::item::{FileChangeKind, ItemDelta, ItemKind, ItemStart};
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
    ApprovalDecision {
        request_id: String,
        decision: ApprovalDecision,
    },
    SavePlan {
        thread_id: ThreadId,
        path: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    Event {
        thread_id: ThreadId,
        agent_event: WireAgentEvent,
    },
    ThreadState(ThreadState),
    LiveTurnSnapshot(LiveTurnSnapshot),
    TokenUpdate {
        scope: TokenScope,
        ledger: serde_json::Value,
    },
    ApprovalRequest {
        thread_id: ThreadId,
        request: WireApprovalRequest,
    },
    Error {
        message: String,
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
    pub resume: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenThreadResponse {
    pub thread_id: ThreadId,
    pub harness_thread_id: String,
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

/// Static model list for the model picker (spec §8.3).
#[derive(Debug, Clone, Serialize)]
pub struct ListModelsResponse {
    pub models: Vec<ModelDescriptor>,
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
    fn client_message_ping() {
        let json = serde_json::to_string(&ClientMessage::Ping).unwrap();
        assert_eq!(json, "{\"type\":\"ping\"}");
    }

    #[test]
    fn server_message_pong() {
        let json = serde_json::to_string(&ServerMessage::Pong).unwrap();
        assert_eq!(json, "{\"type\":\"pong\"}");
    }
}
