use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::diff::FileDiff;
use crate::ids::TurnId;
use crate::item::Item;
use crate::model::ModelRef;
use crate::token::TokenUsage;
use crate::user_input::UserInput;
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

/// Per-turn overrides sent to the harness (spec §7.5, P1).
///
/// A **resolved snapshot**, not a delta. The server constructs it at `start_turn` from the
/// thread's persisted state. `model = None` means "reuse the thread's current model."
/// Effort lives only in `ModelRef.reasoning_effort` (no standalone field).
/// `approval_policy` is the **resolved** effective policy (from project/thread config, or
/// coerced for a degraded harness, §9.4) — NOT a per-turn override (P3). The server reads
/// it and includes it in the snapshot so the harness can pass it to `turn/start`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnOverrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelRef>,
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
///
/// S2: no `Declined` — the pinned Codex `TurnStatus` is `Completed | Interrupted | Failed |
/// InProgress` (the last is non-terminal). Re-add a variant only when a real producer exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TurnStatusKind {
    Completed,
    Interrupted,
    Failed,
}

/// One unit of agent work initiated by a single user input (spec §4.5, B1).
///
/// Persisted inside the thread file (§5.3) as an element of `Thread.turns`, and the unit the
/// diff viewer / token gauge read from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Turn {
    pub id: TurnId,
    pub user_input: UserInput,
    /// Completed items, in order.
    #[serde(default)]
    pub items: Vec<Item>,
    /// Model used for this turn (may differ across turns of one thread, §8.4).
    pub model: ModelRef,
    /// Plan | build applied to this turn (§7.4).
    pub mode: Mode,
    pub status: TurnStatus,
    /// Per-turn usage; the same `TokenUsage` struct is reused in the ledgers (B3).
    pub usage: TokenUsage,
    /// File diffs produced during this turn (spec §11.1, fed by `DiffUpdated` events).
    #[serde(default)]
    pub diffs: Vec<FileDiff>,
    pub started_at: DateTime<Utc>,
    /// `None` while the turn is still live.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Effort;

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
            mode: Mode::Build,
            approval_policy: ApprovalPolicy::Ask,
        };
        let json = serde_json::to_string(&overrides).unwrap();
        let back: TurnOverrides = serde_json::from_str(&json).unwrap();
        assert_eq!(overrides, back);
    }
}
