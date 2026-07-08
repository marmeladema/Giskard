use serde::{Deserialize, Serialize};

use crate::approval::ApprovalRequest;
use crate::diff::FileDiff;
use crate::error::HarnessError;
use crate::ids::{ItemId, ServerRequestId, ThreadId, TurnId};
use crate::item::{Item, ItemDelta, ItemStart};
use crate::server_request::ServerRequest;
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
    /// Server-initiated request that needs a browser response before the harness can continue.
    ServerRequestReceived {
        thread: ThreadId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        turn: Option<TurnId>,
        request: ServerRequest,
    },
    /// A previously surfaced server request received a browser response or otherwise resolved.
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

    #[test]
    fn server_request_events_serde_roundtrip() {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let request_id = ServerRequestId("req_1".into());
        let event = AgentEvent::ServerRequestReceived {
            thread,
            turn: Some(turn),
            request: crate::server_request::ServerRequest {
                id: request_id.clone(),
                method: "item/tool/call".into(),
                params: serde_json::json!({ "tool": "example" }),
                received_at: chrono::Utc::now(),
            },
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["kind"], "server_request_received");
        assert_eq!(json["request"]["id"], "req_1");
        let back: AgentEvent = serde_json::from_value(json).unwrap();
        match back {
            AgentEvent::ServerRequestReceived {
                thread: got_thread,
                turn: got_turn,
                request,
            } => {
                assert_eq!(got_thread, thread);
                assert_eq!(got_turn, Some(turn));
                assert_eq!(request.id, request_id);
                assert_eq!(request.params["tool"], "example");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
