use serde::{Deserialize, Serialize};

use crate::approval::ApprovalRequest;
use crate::diff::FileDiff;
use crate::error::HarnessError;
use crate::ids::{ItemId, ThreadId, TurnId};
use crate::item::{Item, ItemDelta, ItemStart};
use crate::token::TokenUsage;
use crate::turn::TurnStatus;

/// Giskard's internal, harness-neutral representation of everything streamed from a harness.
///
/// Codex protocol messages are mapped into these variants (spec §4.4).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentEvent {
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
        item: Item,
    },
    /// A structured file diff update (for the diff viewer).
    DiffUpdated {
        thread: ThreadId,
        turn: TurnId,
        diff: FileDiff,
    },
    /// Server-initiated approval request.
    ApprovalRequested {
        thread: ThreadId,
        turn: TurnId,
        request: ApprovalRequest,
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
        error: HarnessError,
    },
    /// A non-fatal advisory from the harness (Codex warnings, config/deprecation notices). Unlike
    /// [`AgentEvent::Error`] this does not fail the turn or the pending message — it is surfaced as
    /// a warning, not a hard error.
    Notice {
        thread: ThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        turn: Option<TurnId>,
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_event_serde_roundtrip() {
        let event = AgentEvent::TurnStarted {
            thread: ThreadId::new(),
            turn: TurnId::new(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: AgentEvent = serde_json::from_str(&json).unwrap();
        match (event, back) {
            (
                AgentEvent::TurnStarted {
                    thread: t1,
                    turn: tn1,
                },
                AgentEvent::TurnStarted {
                    thread: t2,
                    turn: tn2,
                },
            ) => {
                assert_eq!(t1, t2);
                assert_eq!(tn1, tn2);
            }
            _ => panic!("variant mismatch"),
        }
    }
}
