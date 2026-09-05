use serde::{Deserialize, Serialize};

use crate::approval::ApprovalRequest;
use crate::diff::FileDiff;
use crate::error::HarnessError;
use crate::ids::{ItemId, ServerRequestId, ThreadId, TurnId};
use crate::item::{Item, ItemDelta, ItemStart};
use crate::model::ModelRef;
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
    /// Live token usage for an in-flight turn, emitted whenever the harness reports a change.
    ///
    /// `usage` is the turn's latest reported usage (the same value `TurnCompleted` will carry at
    /// the end). `context_window` is the effective window the harness applies to this turn when it
    /// reports one; it is turn-scoped runtime data, not a property of a model. `model` is present
    /// only when the harness acknowledged a model for this exact turn at start; the server persists
    /// the window per `(provider, model)` only then, and never derives a model from thread state.
    TurnUsageUpdated {
        thread: ThreadId,
        turn: TurnId,
        usage: TokenUsage,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_window: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<ModelRef>,
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

impl AgentEvent {
    /// The serialized `kind` tag of this variant, for logs and diagnostics.
    ///
    /// Kept equal to the `#[serde(tag = "kind", rename_all = "snake_case")]` name so a log line
    /// and a wire frame name the same event the same way; the test below pins that.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ThreadOpened { .. } => "thread_opened",
            Self::TurnStarted { .. } => "turn_started",
            Self::TurnUsageUpdated { .. } => "turn_usage_updated",
            Self::ItemStarted { .. } => "item_started",
            Self::ItemDelta { .. } => "item_delta",
            Self::ItemCompleted { .. } => "item_completed",
            Self::DiffUpdated { .. } => "diff_updated",
            Self::ApprovalRequested { .. } => "approval_requested",
            Self::ServerRequestReceived { .. } => "server_request_received",
            Self::ServerRequestResolved { .. } => "server_request_resolved",
            Self::TurnCompleted { .. } => "turn_completed",
            Self::Error { .. } => "error",
            Self::Notice { .. } => "notice",
        }
    }

    /// The turn this event belongs to, when it names one.
    ///
    /// Turn-scoped events always carry a turn; `ServerRequest*`, `Error` and `Notice` may be
    /// thread-scoped; `ThreadOpened` never has one.
    pub fn turn(&self) -> Option<TurnId> {
        match self {
            Self::TurnStarted { turn, .. }
            | Self::TurnUsageUpdated { turn, .. }
            | Self::ItemStarted { turn, .. }
            | Self::ItemDelta { turn, .. }
            | Self::ItemCompleted { turn, .. }
            | Self::DiffUpdated { turn, .. }
            | Self::ApprovalRequested { turn, .. }
            | Self::TurnCompleted { turn, .. } => Some(*turn),
            Self::ServerRequestReceived { turn, .. }
            | Self::ServerRequestResolved { turn, .. }
            | Self::Error { turn, .. }
            | Self::Notice { turn, .. } => *turn,
            Self::ThreadOpened { .. } => None,
        }
    }

    /// The Giskard item this event is about, for the three item events.
    pub fn item_id(&self) -> Option<ItemId> {
        match self {
            Self::ItemStarted { item, .. } => Some(item.id),
            Self::ItemDelta { item_id, .. } => Some(*item_id),
            Self::ItemCompleted { item, .. } => Some(item.id),
            _ => None,
        }
    }

    /// Re-address the event to another thread. Used by fixtures and replays that rebind a
    /// recorded stream to a fresh thread id.
    pub fn set_thread(&mut self, thread: ThreadId) {
        match self {
            Self::ThreadOpened {
                thread: event_thread,
                ..
            }
            | Self::TurnStarted {
                thread: event_thread,
                ..
            }
            | Self::TurnUsageUpdated {
                thread: event_thread,
                ..
            }
            | Self::ItemStarted {
                thread: event_thread,
                ..
            }
            | Self::ItemDelta {
                thread: event_thread,
                ..
            }
            | Self::ItemCompleted {
                thread: event_thread,
                ..
            }
            | Self::DiffUpdated {
                thread: event_thread,
                ..
            }
            | Self::ApprovalRequested {
                thread: event_thread,
                ..
            }
            | Self::ServerRequestReceived {
                thread: event_thread,
                ..
            }
            | Self::ServerRequestResolved {
                thread: event_thread,
                ..
            }
            | Self::TurnCompleted {
                thread: event_thread,
                ..
            }
            | Self::Error {
                thread: event_thread,
                ..
            }
            | Self::Notice {
                thread: event_thread,
                ..
            } => *event_thread = thread,
        }
    }

    pub fn thread_id(&self) -> ThreadId {
        match self {
            Self::ThreadOpened { thread, .. }
            | Self::TurnStarted { thread, .. }
            | Self::TurnUsageUpdated { thread, .. }
            | Self::ItemStarted { thread, .. }
            | Self::ItemDelta { thread, .. }
            | Self::ItemCompleted { thread, .. }
            | Self::DiffUpdated { thread, .. }
            | Self::ApprovalRequested { thread, .. }
            | Self::ServerRequestReceived { thread, .. }
            | Self::ServerRequestResolved { thread, .. }
            | Self::TurnCompleted { thread, .. }
            | Self::Error { thread, .. }
            | Self::Notice { thread, .. } => *thread,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approval::{ApprovalDecision, ApprovalKind};
    use crate::ids::ApprovalId;
    use crate::item::{FileChangeKind, ItemKind, ItemPayload};
    use crate::turn::TurnStatusKind;
    use std::path::PathBuf;

    fn every_variant(optional_turn: Option<TurnId>) -> Vec<AgentEvent> {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let started_item_id = ItemId::new();
        let delta_item_id = ItemId::new();
        let completed_item_id = ItemId::new();
        vec![
            AgentEvent::ThreadOpened {
                thread,
                harness_thread_id: "native-thread".into(),
            },
            AgentEvent::TurnStarted { thread, turn },
            AgentEvent::TurnUsageUpdated {
                thread,
                turn,
                usage: TokenUsage::default(),
                context_window: None,
                model: None,
            },
            AgentEvent::ItemStarted {
                thread,
                turn,
                item: ItemStart {
                    id: started_item_id,
                    harness_item_id: "native-started-item".into(),
                    kind: ItemKind::AgentMessage,
                    command: None,
                    tool: None,
                },
            },
            AgentEvent::ItemDelta {
                thread,
                turn,
                item_id: delta_item_id,
                delta: ItemDelta::Text {
                    text: "delta".into(),
                },
            },
            AgentEvent::ItemCompleted {
                thread,
                turn,
                item: Item {
                    id: completed_item_id,
                    harness_item_id: "native-completed-item".into(),
                    payload: ItemPayload::AgentMessage {
                        text: "complete".into(),
                    },
                    created_at: chrono::Utc::now(),
                },
            },
            AgentEvent::DiffUpdated {
                thread,
                turn,
                diff: FileDiff {
                    path: PathBuf::from("file.rs"),
                    change: FileChangeKind::Modified,
                    old_text: Some("old".into()),
                    new_text: Some("new".into()),
                    hunks: Vec::new(),
                    binary: false,
                    captured: None,
                },
            },
            AgentEvent::ApprovalRequested {
                thread,
                turn,
                request: ApprovalRequest {
                    id: ApprovalId("approval-1".into()),
                    kind: ApprovalKind::Permission {
                        detail: "test".into(),
                    },
                    reason: None,
                    metadata: Vec::new(),
                    available: vec![ApprovalDecision::Accept],
                },
            },
            AgentEvent::ServerRequestReceived {
                thread,
                turn: optional_turn,
                request: ServerRequest {
                    id: ServerRequestId("request-1".into()),
                    method: "test/request".into(),
                    params: serde_json::Value::Null,
                    received_at: chrono::Utc::now(),
                },
            },
            AgentEvent::ServerRequestResolved {
                thread,
                turn: optional_turn,
                request_id: ServerRequestId("request-1".into()),
            },
            AgentEvent::TurnCompleted {
                thread,
                turn,
                usage: TokenUsage::default(),
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
            },
            AgentEvent::Error {
                thread,
                turn: optional_turn,
                error: HarnessError::Overloaded,
            },
            AgentEvent::Notice {
                thread,
                turn: optional_turn,
                message: "test notice".into(),
            },
        ]
    }

    #[test]
    fn kind_matches_the_serde_tag_for_every_variant() {
        let events = every_variant(Some(TurnId::new()));
        assert_eq!(events.len(), 13);
        for event in events {
            let json = serde_json::to_value(&event).unwrap();
            assert_eq!(json["kind"], event.kind());
        }
    }

    #[test]
    fn turn_is_present_exactly_where_the_variant_carries_one() {
        for optional_turn in [Some(TurnId::new()), None] {
            let events = every_variant(optional_turn);
            assert_eq!(events.len(), 13);
            for event in events {
                let expected = match &event {
                    AgentEvent::ThreadOpened { .. } => None,
                    AgentEvent::TurnStarted { turn, .. }
                    | AgentEvent::TurnUsageUpdated { turn, .. }
                    | AgentEvent::ItemStarted { turn, .. }
                    | AgentEvent::ItemDelta { turn, .. }
                    | AgentEvent::ItemCompleted { turn, .. }
                    | AgentEvent::DiffUpdated { turn, .. }
                    | AgentEvent::ApprovalRequested { turn, .. }
                    | AgentEvent::TurnCompleted { turn, .. } => Some(*turn),
                    AgentEvent::ServerRequestReceived { turn, .. }
                    | AgentEvent::ServerRequestResolved { turn, .. }
                    | AgentEvent::Error { turn, .. }
                    | AgentEvent::Notice { turn, .. } => *turn,
                };
                assert_eq!(event.turn(), expected);
            }
        }
    }

    #[test]
    fn item_id_names_the_item_for_the_three_item_events() {
        let events = every_variant(Some(TurnId::new()));
        assert_eq!(events.len(), 13);
        for event in events {
            let expected = match &event {
                AgentEvent::ItemStarted { item, .. } => Some(item.id),
                AgentEvent::ItemCompleted { item, .. } => Some(item.id),
                AgentEvent::ItemDelta { item_id, .. } => Some(*item_id),
                _ => None,
            };
            assert_eq!(event.item_id(), expected);
        }
    }

    #[test]
    fn set_thread_readdresses_every_variant() {
        let new_thread = ThreadId::new();
        let events = every_variant(Some(TurnId::new()));
        assert_eq!(events.len(), 13);
        for mut event in events {
            event.set_thread(new_thread);
            assert_eq!(event.thread_id(), new_thread);
        }
    }

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
    fn turn_usage_update_serde_roundtrip() {
        let event = AgentEvent::TurnUsageUpdated {
            thread: ThreadId::new(),
            turn: TurnId::new(),
            usage: TokenUsage {
                input: 12,
                output: 3,
                total: 15,
            },
            context_window: None,
            model: None,
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["kind"], "turn_usage_updated");
        assert_eq!(json["usage"]["input"], 12);
        assert!(json.get("context_window").is_none());
        assert!(json.get("model").is_none());
        let decoded: AgentEvent = serde_json::from_value(json).unwrap();
        assert!(matches!(
            decoded,
            AgentEvent::TurnUsageUpdated {
                context_window: None,
                model: None,
                ..
            }
        ));
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
