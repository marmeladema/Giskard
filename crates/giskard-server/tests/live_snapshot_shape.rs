//! Regression coverage for the size of the reconnect snapshot (spec §13.6, LB1/LB2).
//!
//! The live buffer used to keep every streamed chunk for the life of the turn, so a reload late in
//! a long turn replayed thousands of deltas next to the completions that already carried their
//! text. The buffer is now keyed by item: a completed item replays as its start and completion, an
//! open item as its start and one coalesced delta. This test seeds a long turn and asserts the
//! snapshot a subscribing browser receives over the real WebSocket.

use std::collections::HashSet;

use chrono::Utc;
use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, TurnId};
use giskard_core::item::{Item, ItemDelta, ItemKind, ItemPayload, ItemStart};
use giskard_proto::{ClientMessage, WireAgentEvent};
use giskard_testenv::fake::{self, FakeHarness};
use giskard_testenv::{TestServer, ws};

const COMPLETED_MESSAGES: usize = 20;
const CHUNKS_PER_ITEM: usize = 30;
const APPROVAL_ID: &str = "ap_live_shape";

fn chunk(item: usize, index: usize) -> String {
    format!("item{item}-chunk{index} ")
}

#[tokio::test]
async fn a_long_turn_snapshot_carries_one_or_two_events_per_item() {
    use futures_util::SinkExt;

    let server = TestServer::spawn(fake::factory(FakeHarness::new(()))).await;
    let project = server.create_project("live-shape").await;
    let thread = server
        .register_thread(project.id, "live_shape_thread")
        .await;
    let turn = TurnId::new();
    let runtime = server
        .state
        .registry
        .thread_runtime(thread)
        .await
        .expect("thread runtime");
    runtime.replace_live_turn_for_test(turn, None);
    let apply = |event: AgentEvent| runtime.apply_event_for_test(&event, true);

    apply(AgentEvent::TurnStarted { thread, turn });
    let start = |id: ItemId, kind: ItemKind| AgentEvent::ItemStarted {
        thread,
        turn,
        item: ItemStart {
            id,
            harness_item_id: format!("h_{id}"),
            kind,
            command: None,
            tool: None,
        },
    };
    let delta = |id: ItemId, text: String| AgentEvent::ItemDelta {
        thread,
        turn,
        item_id: id,
        delta: ItemDelta::Text { text },
    };

    let mut completed = HashSet::new();
    for n in 0..COMPLETED_MESSAGES {
        let id = ItemId::new();
        apply(start(id, ItemKind::AgentMessage));
        let mut text = String::new();
        for c in 0..CHUNKS_PER_ITEM {
            text.push_str(&chunk(n, c));
            apply(delta(id, chunk(n, c)));
        }
        apply(AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id,
                harness_item_id: format!("h_{id}"),
                payload: ItemPayload::AgentMessage { text },
                created_at: Utc::now(),
            },
        });
        completed.insert(id);
    }

    let open = ItemId::new();
    apply(start(open, ItemKind::Reasoning));
    let mut open_text = String::new();
    for c in 0..CHUNKS_PER_ITEM {
        open_text.push_str(&chunk(COMPLETED_MESSAGES, c));
        apply(delta(open, chunk(COMPLETED_MESSAGES, c)));
    }
    apply(AgentEvent::ApprovalRequested {
        thread,
        turn,
        request: ApprovalRequest {
            id: ApprovalId(APPROVAL_ID.into()),
            kind: ApprovalKind::CommandExecution {
                command: "rm -rf ./build".into(),
                cwd: "/tmp/project".into(),
            },
            reason: None,
            metadata: vec![],
            available: vec![ApprovalDecision::Accept, ApprovalDecision::Decline],
        },
    });

    let mut socket = server.ws().await;
    socket
        .send(ws::text(&ClientMessage::Subscribe {
            thread_id: thread,
            since: None,
        }))
        .await
        .expect("subscribe");
    let snapshot = ws::expect_live_snapshot(&mut socket).await;

    assert_eq!(snapshot.turn_id, turn);
    // TurnStarted, a start and a completion per completed message, the open item's start and one
    // delta, and the approval.
    assert_eq!(
        snapshot.accumulated.len(),
        1 + COMPLETED_MESSAGES * 2 + 2 + 1
    );
    let deltas: Vec<(ItemId, &str)> = snapshot
        .accumulated
        .iter()
        .filter_map(|event| match event {
            WireAgentEvent::ItemDelta {
                item_id,
                delta: ItemDelta::Text { text },
                ..
            } => Some((*item_id, text.as_str())),
            _ => None,
        })
        .collect();
    assert!(
        deltas.iter().all(|(id, _)| !completed.contains(id)),
        "no delta may name a completed item"
    );
    assert_eq!(deltas, vec![(open, open_text.as_str())]);

    let answered: HashSet<&ApprovalId> = snapshot
        .answered_approvals
        .iter()
        .map(|answered| &answered.request_id)
        .collect();
    let outstanding: Vec<&ApprovalId> = snapshot
        .accumulated
        .iter()
        .filter_map(|event| match event {
            WireAgentEvent::ApprovalRequested { request, .. } => Some(&request.id),
            _ => None,
        })
        .filter(|id| !answered.contains(id))
        .collect();
    assert_eq!(outstanding, vec![&ApprovalId(APPROVAL_ID.into())]);
}
