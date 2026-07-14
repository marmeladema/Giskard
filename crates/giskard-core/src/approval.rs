use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::ids::ApprovalId;
use crate::item::FileChangeKind;

/// A server-initiated approval request (spec §9).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub id: ApprovalId,
    pub kind: ApprovalKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata: Vec<ApprovalMetadata>,
    /// Decisions the harness will accept.
    pub available: Vec<ApprovalDecision>,
}

/// What kind of action requires approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ApprovalKind {
    CommandExecution {
        command: String,
        cwd: PathBuf,
    },
    FileChange {
        path: PathBuf,
        change: FileChangeKind,
    },
    Permission {
        detail: String,
    },
    /// An MCP tool call that the agent wants to execute (spec §9.2).
    ///
    /// Surfaced by Codex as a `ToolRequestUserInput` or `McpServerElicitationRequest`
    /// carrying the `codex_approval_kind: "mcp_tool_call"` marker. Giskard promotes
    /// these to first-class approval cards so the user can approve once, for the
    /// session, or decline — mirroring the command/file approval flow.
    McpToolCall {
        server: String,
        tool_name: String,
    },
}

/// Structured, card-facing metadata for approval prompts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ApprovalMetadata {
    Text {
        label: String,
        value: String,
    },
    Path {
        label: String,
        path: PathBuf,
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

/// A user's decision on an approval request (mirrors Codex, spec §9.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    Accept,
    /// See §9.2.1 for "session" definition.
    AcceptForSession,
    Decline,
    Cancel,
    /// Command exec only.
    AcceptWithExecPolicyAmendment {
        amendment: Vec<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn approval_request_roundtrip() {
        let req = ApprovalRequest {
            id: ApprovalId("ap_7".into()),
            kind: ApprovalKind::CommandExecution {
                command: "cargo test".into(),
                cwd: "/tmp/project".into(),
            },
            reason: Some("Running tests".into()),
            metadata: vec![
                ApprovalMetadata::Text {
                    label: "Environment".into(),
                    value: "env_1".into(),
                },
                ApprovalMetadata::Host {
                    label: "Network host".into(),
                    host: "api.example.com".into(),
                    protocol: Some("https".into()),
                    port: Some(443),
                    target: None,
                },
                ApprovalMetadata::Path {
                    label: "Write access".into(),
                    path: "/tmp/project".into(),
                    source_link: false,
                },
            ],
            available: vec![
                ApprovalDecision::Accept,
                ApprovalDecision::AcceptForSession,
                ApprovalDecision::Decline,
                ApprovalDecision::Cancel,
            ],
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: ApprovalRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn mcp_tool_call_approval_roundtrip() {
        let req = ApprovalRequest {
            id: ApprovalId("ap_mcp_1".into()),
            kind: ApprovalKind::McpToolCall {
                server: "brave-search".into(),
                tool_name: "brave_web_search".into(),
            },
            reason: Some(r#"Allow brave-search to run tool "brave_web_search"?"#.into()),
            metadata: vec![],
            available: vec![
                ApprovalDecision::Accept,
                ApprovalDecision::AcceptForSession,
                ApprovalDecision::Decline,
                ApprovalDecision::Cancel,
            ],
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: ApprovalRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
        // The wire tag is snake_case.
        assert!(json.contains(r#""kind":"mcp_tool_call""#));
        assert!(json.contains(r#""server":"brave-search""#));
        assert!(json.contains(r#""tool_name":"brave_web_search""#));
    }
}
