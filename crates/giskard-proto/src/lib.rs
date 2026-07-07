//! Shared client↔server wire protocol types (spec §13.6).
//!
//! Defined once here so `giskard-server` and `giskard-ui` never disagree on the protocol.

use serde::{Deserialize, Serialize};

use chrono::{DateTime, Utc};
use giskard_core::approval::{ApprovalDecision, ApprovalRequest};
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ProjectId, ThreadId, TurnId};
use giskard_core::model::ModelRef;
use giskard_core::turn::{ApprovalPolicy, Mode};

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

/// In-flight turn reconstruction on reconnect (spec §13.6).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveTurnSnapshot {
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    pub accumulated: Vec<AgentEvent>,
    pub pending_approval: Option<ApprovalRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    Event {
        thread_id: ThreadId,
        agent_event: AgentEvent,
    },
    ThreadState(ThreadState),
    LiveTurnSnapshot(LiveTurnSnapshot),
    TokenUpdate {
        scope: TokenScope,
        ledger: serde_json::Value,
    },
    ApprovalRequest {
        thread_id: ThreadId,
        request: ApprovalRequest,
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
