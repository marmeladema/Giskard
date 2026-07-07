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
}
