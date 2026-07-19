//! Shared client↔server wire protocol types (spec §13.6).
//!
//! Defined once here so `giskard-server` and `giskard-ui` never disagree on the protocol.

use serde::{Deserialize, Serialize};

use chrono::{DateTime, Utc};
use giskard_core::ids::{ProjectId, ThreadId, TurnId};

pub mod wire;
pub use wire::{
    WireAgentEvent, WireApprovalKind, WireApprovalMetadata, WireApprovalRequest, WireFileDiff,
    WireHarnessError, WireItem, WireItemPayload, WireTurn,
};

// C1/§3.5: `giskard-proto` is the single wire vocabulary. Path-free `giskard-core` domain types
// are re-exported here so `giskard-ui` depends only on this crate; path-bearing streamed types are
// mirrored in `wire` above.
pub use giskard_core::approval::{
    ApprovalDecision, ApprovalKind, ApprovalMetadata, ApprovalRequest,
};
pub use giskard_core::diff::{DiffHunk, DiffLine};
pub use giskard_core::error::HarnessError;
pub use giskard_core::event::AgentEvent;
pub use giskard_core::ids::{ApprovalId, ItemId};
pub use giskard_core::item::{
    CommandExecutionStart, FileChangeEntry, FileChangeKind, ItemDelta, ItemKind, ItemStart,
};
pub use giskard_core::mcp::{
    McpAuthStatus, McpOauthStart, McpResource, McpResourceTemplate, McpServerInfo, McpServerStatus,
    McpTool,
};
pub use giskard_core::model::{Effort, ModelDescriptor, ModelRef};
pub use giskard_core::server_request::{ServerRequest, ServerRequestResponse};
pub use giskard_core::token::{ByModel, DailyTokenLedger, TokenLedger, TokenUsage};
pub use giskard_core::turn::{ApprovalPolicy, Mode, TurnStatus, TurnStatusKind};

// ---- Client → Server ----

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Subscribe {
        thread_id: ThreadId,
        /// Incremental resync cursor: the newest turn the client already has rendered. When present
        /// and resolvable, the server replies with a `HistoryDelta` of just the turns after it
        /// instead of a full `HistoryPage`, so the browser keeps its immutable completed-turn DOM
        /// and repaints only the in-flight turn. Omitted (or unresolvable) → a full snapshot.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        since: Option<TurnId>,
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
        thread_id: ThreadId,
        policy: ApprovalPolicy,
    },
    Interrupt {
        thread_id: ThreadId,
    },
    CompactContext {
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
    ServerRequestResponse {
        request_id: String,
        response: ServerRequestResponse,
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

/// Lightweight cross-thread activity update for sidebar badges and browser notifications. This is
/// intentionally much smaller than a transcript event: inactive threads should show that work is
/// happening without subscribing every browser to every live delta stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadActivity {
    pub thread_id: ThreadId,
    #[serde(flatten)]
    pub kind: ThreadActivityKind,
    pub active_turn: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ThreadActivityKind {
    TurnStarted,
    Progress,
    ApprovalRequested { approval_id: String },
    ServerRequestReceived { server_request_id: String },
    TurnCompleted,
    Error,
    Notice,
}

/// In-flight turn reconstruction on reconnect (spec §13.6). Carries wire types (§3.5).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveTurnSnapshot {
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    pub accumulated: Vec<WireAgentEvent>,
    pub pending_approval: Option<WireApprovalRequest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_server_requests: Vec<ServerRequest>,
}

/// Whether a running task is a shell command or a tool/MCP call. Both are tracked and surfaced the
/// same way (right-panel row, elapsed time, stop control); they differ only in labeling and how a
/// stop request is routed (commands terminate by process id, tools interrupt the owning turn).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    #[default]
    Command,
    Tool,
}

/// A unit of agent work still running (or outliving an interrupted turn): a shell command or a
/// tool/MCP call. Formerly `RunningCommand`; generalized so tool calls share the running-work UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunningTask {
    #[serde(default)]
    pub kind: TaskKind,
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    pub item_id: ItemId,
    pub harness_item_id: String,
    /// Primary label: the command line for commands, the tool name for tool calls.
    pub command: String,
    /// Secondary label: the working directory for commands (empty for tools).
    pub cwd: String,
    /// MCP/tool server name, when this is a tool call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
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
    /// Command process the error refers to, when the failing action targeted a specific command
    /// (e.g. `terminate_command`). Lets the client scope any recovery to that one command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    Event {
        thread_id: ThreadId,
        agent_event: WireAgentEvent,
    },
    ThreadActivity(ThreadActivity),
    ThreadState(ThreadState),
    /// A page of persisted history (H6), oldest-first; `has_more` if older turns exist before it.
    HistoryPage {
        thread_id: ThreadId,
        turns: Vec<WireTurn>,
        has_more: bool,
    },
    /// Incremental-resync delta: the persisted turns that completed after the client's `since`
    /// cursor, oldest-first. The client keeps its existing transcript, repaints only the in-flight
    /// turn, and appends these. Sent instead of `HistoryPage` when a resolvable `since` was given.
    HistoryDelta {
        thread_id: ThreadId,
        turns: Vec<WireTurn>,
    },
    LiveTurnSnapshot(LiveTurnSnapshot),
    RunningTasks {
        thread_id: ThreadId,
        tasks: Vec<RunningTask>,
    },
    TokenUpdate {
        scope: TokenScope,
        /// Present for thread-scoped token updates so clients can reject stale frames that belong
        /// to a previously selected thread.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread_id: Option<ThreadId>,
        ledger: serde_json::Value,
    },
    ApprovalRequest {
        thread_id: ThreadId,
        request: WireApprovalRequest,
    },
    ApprovalResolved {
        thread_id: ThreadId,
        request_id: String,
        decision: ApprovalDecision,
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
    pub archived: bool,
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

#[derive(Debug, Clone, Deserialize)]
pub struct StartThreadRequest {
    pub text: String,
    pub model_ref: ModelRef,
    pub mode: Mode,
    pub approval_policy: ApprovalPolicy,
}

#[derive(Debug, Clone, Serialize)]
pub struct StartThreadResponse {
    pub thread_id: ThreadId,
    pub harness_thread_id: String,
    pub turn_id: TurnId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<ErrorInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ArchiveThreadRequest {
    pub archived: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RenameThreadRequest {
    pub title: String,
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

#[derive(Debug, Clone, Serialize)]
pub struct McpCapabilitiesResponse {
    pub status: bool,
    pub reload: bool,
    pub oauth_login: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListMcpServersResponse {
    pub servers: Vec<McpServerStatus>,
    pub capabilities: McpCapabilitiesResponse,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StartMcpOauthLoginRequest {
    pub name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReloadMcpServersResponse {
    pub ok: bool,
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

/// Request body for Markdown rendering of agent text (spec §11.2).
#[derive(Debug, Clone, Deserialize)]
pub struct RenderRequest {
    pub text: String,
}

/// Result of rendering agent Markdown to sanitized HTML with embedded path links.
#[derive(Debug, Clone, Serialize)]
pub struct RenderResponse {
    /// Sanitized HTML: agent-authored raw HTML is escaped, link URLs are scheme-checked, and
    /// detected workspace paths are wrapped in `.path-link` buttons the client wires up.
    pub html: String,
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
    fn client_message_compact_context_serde() {
        let tid = ThreadId::new();
        let msg = ClientMessage::CompactContext { thread_id: tid };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "compact_context");
        assert_eq!(json["thread_id"], tid.to_string());

        let back: ClientMessage = serde_json::from_value(json).unwrap();
        match back {
            ClientMessage::CompactContext { thread_id } => assert_eq!(thread_id, tid),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn client_message_set_approval_policy_is_thread_scoped() {
        let tid = ThreadId::new();
        let msg = ClientMessage::SetApprovalPolicy {
            thread_id: tid,
            policy: ApprovalPolicy::ReadOnly,
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "set_approval_policy");
        assert_eq!(json["thread_id"], tid.to_string());
        assert_eq!(json["policy"], "read_only");
        assert!(json.get("project_id").is_none());

        let back: ClientMessage = serde_json::from_value(json).unwrap();
        match back {
            ClientMessage::SetApprovalPolicy { thread_id, policy } => {
                assert_eq!(thread_id, tid);
                assert_eq!(policy, ApprovalPolicy::ReadOnly);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn client_message_server_request_response_serde() {
        let msg = ClientMessage::ServerRequestResponse {
            request_id: "req_1".into(),
            response: ServerRequestResponse::result(serde_json::json!({
                "success": true,
                "contentItems": [],
            })),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "server_request_response");
        assert_eq!(json["request_id"], "req_1");
        assert_eq!(json["response"]["kind"], "result");
        assert_eq!(json["response"]["value"]["success"], true);

        let back: ClientMessage = serde_json::from_value(json).unwrap();
        match back {
            ClientMessage::ServerRequestResponse {
                request_id,
                response: ServerRequestResponse::Result { value },
            } => {
                assert_eq!(request_id, "req_1");
                assert_eq!(value["contentItems"], serde_json::json!([]));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn client_message_server_request_error_response_serde() {
        let msg = ClientMessage::ServerRequestResponse {
            request_id: "req_1".into(),
            response: ServerRequestResponse::error(-32000, "unsupported"),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "server_request_response");
        assert_eq!(json["request_id"], "req_1");
        assert_eq!(json["response"]["kind"], "error");
        assert_eq!(json["response"]["code"], -32000);
        assert_eq!(json["response"]["message"], "unsupported");

        let back: ClientMessage = serde_json::from_value(json).unwrap();
        match back {
            ClientMessage::ServerRequestResponse {
                request_id,
                response: ServerRequestResponse::Error { code, message },
            } => {
                assert_eq!(request_id, "req_1");
                assert_eq!(code, -32000);
                assert_eq!(message, "unsupported");
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
    fn server_message_thread_token_update_carries_thread_id() {
        let tid = ThreadId::new();
        let msg = ServerMessage::TokenUpdate {
            scope: TokenScope::Thread,
            thread_id: Some(tid),
            ledger: serde_json::json!({
                "total": { "input": 10, "output": 5, "total": 15 }
            }),
        };

        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "token_update");
        assert_eq!(json["scope"], "thread");
        assert_eq!(json["thread_id"], tid.to_string());
        assert_eq!(json["ledger"]["total"]["total"], 15);

        let back: ServerMessage = serde_json::from_value(json).unwrap();
        match back {
            ServerMessage::TokenUpdate {
                scope,
                thread_id,
                ledger,
            } => {
                assert_eq!(scope, TokenScope::Thread);
                assert_eq!(thread_id, Some(tid));
                assert_eq!(ledger["total"]["input"], 10);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn server_message_thread_activity_is_flattened() {
        let tid = ThreadId::new();
        let msg = ServerMessage::ThreadActivity(ThreadActivity {
            thread_id: tid,
            kind: ThreadActivityKind::ApprovalRequested {
                approval_id: "approval-1".into(),
            },
            active_turn: true,
            summary: Some("Approval requested".into()),
        });

        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "thread_activity");
        assert_eq!(json["thread_id"], tid.to_string());
        assert_eq!(json["kind"], "approval_requested");
        assert_eq!(json["active_turn"], true);
        assert_eq!(json["approval_id"], "approval-1");
        assert!(json.get("server_request_id").is_none());

        let back: ServerMessage = serde_json::from_value(json).unwrap();
        match back {
            ServerMessage::ThreadActivity(activity) => {
                assert_eq!(activity.thread_id, tid);
                match activity.kind {
                    ThreadActivityKind::ApprovalRequested { approval_id } => {
                        assert_eq!(approval_id, "approval-1");
                    }
                    other => panic!("expected approval activity, got {other:?}"),
                }
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn server_message_thread_activity_requires_variant_ids() {
        let json = serde_json::json!({
            "type": "thread_activity",
            "thread_id": ThreadId::new().to_string(),
            "kind": "approval_requested",
            "active_turn": true
        });

        let err = serde_json::from_value::<ServerMessage>(json).unwrap_err();
        assert!(
            err.to_string().contains("missing field `approval_id`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn server_message_approval_resolved_serde() {
        let tid = ThreadId::new();
        let msg = ServerMessage::ApprovalResolved {
            thread_id: tid,
            request_id: "approval-1".into(),
            decision: ApprovalDecision::Accept,
        };

        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "approval_resolved");
        assert_eq!(json["thread_id"], tid.to_string());
        assert_eq!(json["request_id"], "approval-1");
        assert_eq!(json["decision"], "accept");

        let back: ServerMessage = serde_json::from_value(json).unwrap();
        match back {
            ServerMessage::ApprovalResolved {
                thread_id,
                request_id,
                decision,
            } => {
                assert_eq!(thread_id, tid);
                assert_eq!(request_id, "approval-1");
                assert_eq!(decision, ApprovalDecision::Accept);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn server_message_running_tasks_serde() {
        let tid = ThreadId::new();
        let turn_id = TurnId::new();
        let item_id = ItemId::new();
        let tool_item = ItemId::new();
        let msg = ServerMessage::RunningTasks {
            thread_id: tid,
            tasks: vec![
                RunningTask {
                    kind: TaskKind::Command,
                    thread_id: tid,
                    turn_id,
                    item_id,
                    harness_item_id: "cmd1".into(),
                    command: "sleep 60".into(),
                    cwd: "/tmp/project".into(),
                    server: None,
                    status: "in_progress".into(),
                    process_id: Some("proc_1".into()),
                    started_at_ms: 1_785_000_000_000,
                    output: "waiting".into(),
                    after_turn: true,
                    terminating: true,
                },
                RunningTask {
                    kind: TaskKind::Tool,
                    thread_id: tid,
                    turn_id,
                    item_id: tool_item,
                    harness_item_id: "tool1".into(),
                    command: "search".into(),
                    cwd: String::new(),
                    server: Some("wiki".into()),
                    status: "in_progress".into(),
                    process_id: None,
                    started_at_ms: 1_785_000_000_500,
                    output: String::new(),
                    after_turn: false,
                    terminating: false,
                },
            ],
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "running_tasks");
        assert_eq!(json["thread_id"], tid.to_string());
        assert_eq!(json["tasks"][0]["kind"], "command");
        assert_eq!(json["tasks"][0]["process_id"], "proc_1");
        assert_eq!(json["tasks"][0]["after_turn"], true);
        assert_eq!(json["tasks"][1]["kind"], "tool");
        assert_eq!(json["tasks"][1]["server"], "wiki");
        let back: ServerMessage = serde_json::from_value(json).unwrap();
        match back {
            ServerMessage::RunningTasks { tasks, .. } => {
                assert_eq!(tasks[0].item_id, item_id);
                assert_eq!(tasks[0].kind, TaskKind::Command);
                assert!(tasks[0].terminating);
                assert_eq!(tasks[1].kind, TaskKind::Tool);
                assert_eq!(tasks[1].server.as_deref(), Some("wiki"));
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
                process_id: None,
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

    #[test]
    fn live_turn_snapshot_includes_pending_server_requests() {
        let tid = ThreadId::new();
        let turn = TurnId::new();
        let snapshot = LiveTurnSnapshot {
            thread_id: tid,
            turn_id: turn,
            accumulated: vec![],
            pending_approval: None,
            pending_server_requests: vec![ServerRequest {
                id: giskard_core::ids::ServerRequestId("req_1".into()),
                method: "item/tool/call".into(),
                params: serde_json::json!({ "tool": "example" }),
                received_at: chrono::Utc::now(),
            }],
        };
        let json = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(json["thread_id"], tid.to_string());
        assert_eq!(json["pending_server_requests"][0]["id"], "req_1");
        assert_eq!(
            json["pending_server_requests"][0]["method"],
            "item/tool/call"
        );
    }
}
