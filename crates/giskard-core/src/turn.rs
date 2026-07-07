use serde::{Deserialize, Serialize};

use crate::model::{Effort, ModelRef};

/// Thread-level mode (spec §7.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Plan,
    Build,
}

impl Mode {
    /// Returns the Codex sandbox policy for this mode.
    pub fn sandbox_policy(&self) -> &'static str {
        match self {
            Self::Plan => "read-only",
            Self::Build => "workspace-write",
        }
    }
}

/// Approval policy (spec §9.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalPolicy {
    ReadOnly,
    Ask,
    Auto,
}

/// Per-turn overrides sent to the harness (spec §7.5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnOverrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<Effort>,
    pub mode: Mode,
    pub approval_policy: ApprovalPolicy,
}

/// Outcome of a completed turn (spec §4.5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnStatus {
    pub kind: TurnStatusKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Kind of turn outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TurnStatusKind {
    Completed,
    Interrupted,
    Failed,
    Declined,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_sandbox_mapping() {
        assert_eq!(Mode::Plan.sandbox_policy(), "read-only");
        assert_eq!(Mode::Build.sandbox_policy(), "workspace-write");
    }

    #[test]
    fn mode_serde() {
        let json = serde_json::to_string(&Mode::Build).unwrap();
        assert_eq!(json, "\"build\"");
        let back: Mode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, Mode::Build);
    }

    #[test]
    fn approval_policy_serde() {
        let json = serde_json::to_string(&ApprovalPolicy::ReadOnly).unwrap();
        assert_eq!(json, "\"read_only\"");
    }

    #[test]
    fn turn_overrides_serde() {
        let overrides = TurnOverrides {
            model: Some(ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: Some(Effort::High),
            }),
            reasoning_effort: Some(Effort::High),
            mode: Mode::Build,
            approval_policy: ApprovalPolicy::Ask,
        };
        let json = serde_json::to_string(&overrides).unwrap();
        let back: TurnOverrides = serde_json::from_str(&json).unwrap();
        assert_eq!(overrides, back);
    }
}
