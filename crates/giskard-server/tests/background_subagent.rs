//! A backgrounded sub-agent outlives the turn that delegated to it (hardening plan §4).
//!
//! The fake harness plays the shape the Claude Code adapter produces for a background delegation:
//! the delegating turn ends at once with the sub-agent's link on its `Agent` call, the sub-agent
//! keeps running on its own thread while the parent serves another prompt, and when it ends its
//! outcome row arrives as a late `ItemCompleted` (no `ItemStarted`) on the delegating turn. Every
//! step runs through the real server: registry, driver admission, forwarders, store and hub.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use futures_util::SinkExt;
use giskard_core::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemPayload, SubagentAction, SubagentLink, SubagentStatus};
use giskard_core::turn::TurnStatusKind;
use giskard_proto::{ClientMessage, ServerMessage, WireAgentEvent};
use giskard_server::DriverEvent;
use giskard_testenv::fake::{self, FakeCore, FakeHarness, Script, TurnCall};
use giskard_testenv::ws::TestWs;
use giskard_testenv::{TestServer, ws};

const CHILD_NATIVE_ID: &str = "task:toolu_background";
const OUTCOME_HARNESS_ITEM_ID: &str = "task_updated:b7k2";

fn link(action: SubagentAction, status: SubagentStatus) -> SubagentLink {
    SubagentLink {
        harness_thread_id: CHILD_NATIVE_ID.into(),
        path: None,
        initial_prompt: Some("read data.txt twenty times".into()),
        action,
        status: Some(status),
        message: None,
    }
}

fn agent_message(harness_item_id: String, text: &str) -> Item {
    Item {
        id: ItemId::new(),
        harness_item_id,
        payload: ItemPayload::AgentMessage { text: text.into() },
        created_at: chrono::Utc::now(),
    }
}

/// The first turn delegates in the background and ends; every later turn answers "pong".
#[derive(Default)]
struct BackgroundDelegation {
    turns: AtomicUsize,
}

#[async_trait]
impl Script for BackgroundDelegation {
    fn native_thread_id(&self, _thread: ThreadId) -> String {
        "native_parent".into()
    }

    async fn start_turn(&self, core: &FakeCore, call: &TurnCall) -> Result<(), HarnessError> {
        call.log.append(AgentEvent::TurnStarted {
            thread: call.thread,
            turn: call.turn,
        });
        if self.turns.fetch_add(1, Ordering::SeqCst) == 0 {
            call.log.append(AgentEvent::ItemCompleted {
                thread: call.thread,
                turn: call.turn,
                item: Item {
                    id: ItemId::new(),
                    harness_item_id: "toolu_background".into(),
                    payload: ItemPayload::ToolCall {
                        description: None,
                        name: "Agent".into(),
                        input: serde_json::json!({ "run_in_background": true }),
                        output: Some(serde_json::json!("Async agent launched successfully")),
                        server: None,
                        status: Some("completed".into()),
                        metadata: None,
                        subagent: Some(link(SubagentAction::Spawned, SubagentStatus::Running)),
                        error: None,
                    },
                    created_at: chrono::Utc::now(),
                },
            });
            call.log.append(AgentEvent::ItemCompleted {
                thread: call.thread,
                turn: call.turn,
                item: agent_message(format!("say_{}", call.turn), "launched"),
            });
        } else {
            call.log.append(AgentEvent::ItemCompleted {
                thread: call.thread,
                turn: call.turn,
                item: agent_message(format!("say_{}", call.turn), "pong"),
            });
        }
        core.complete_turn(call.thread, call.turn);
        Ok(())
    }
}

async fn send_input(socket: &mut TestWs, thread_id: ThreadId, text: &str) {
    socket
        .send(ws::text(&ClientMessage::SendInput {
            thread_id,
            text: text.into(),
            attachments: Vec::new(),
        }))
        .await
        .unwrap();
}

async fn turn_completed(socket: &mut TestWs) -> TurnId {
    ws::recv_until(socket, |message| match message {
        ServerMessage::Event { agent_event, .. } => match *agent_event {
            WireAgentEvent::TurnCompleted { turn, .. } => Some(turn),
            _ => None,
        },
        _ => None,
    })
    .await
    .expect("the turn completes")
}

async fn wait_until(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while !check().await {
        assert!(tokio::time::Instant::now() < deadline, "{what}");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_background_subagent_outlives_its_delegating_turn_and_its_outcome_lands_on_it() {
    let harness = FakeHarness::new(BackgroundDelegation::default());
    let (sink, mut probe) = giskard_testenv::driver::probe();
    let server = TestServer::builder(fake::factory(harness.clone()))
        .driver_events(sink)
        .start()
        .await;
    let project = server.create_project("background-subagent").await;
    let parent = server.register_thread(project.id, "native_parent").await;
    let store = server.state.store.clone();
    let registry = server.state.registry.clone();

    let mut socket = server.ws().await;
    socket
        .send(ws::text(&ClientMessage::Subscribe {
            thread_id: parent,
            since: None,
        }))
        .await
        .unwrap();

    // The delegating turn ends at once, and its `Agent` call's link materializes the sub-agent.
    send_input(&mut socket, parent, "delegate in the background").await;
    let delegating = turn_completed(&mut socket).await;
    let child = probe.expect_child_of(parent).await;

    // The sub-agent starts working on its own thread after its delegating turn has ended.
    let child_turn = TurnId::new();
    let child_log = harness.core.log(child);
    child_log.append(AgentEvent::TurnStarted {
        thread: child,
        turn: child_turn,
    });
    child_log.append(AgentEvent::ItemCompleted {
        thread: child,
        turn: child_turn,
        item: agent_message("child_read_1".into(), "read data.txt"),
    });
    wait_until("the sub-agent's turn is live", async || {
        registry.thread_has_active_turn(child).await
    })
    .await;
    assert!(
        !registry.thread_has_active_turn(parent).await,
        "a live sub-agent does not keep its parent busy"
    );

    // The parent serves a second prompt while the sub-agent runs.
    send_input(&mut socket, parent, "ping").await;
    let second = turn_completed(&mut socket).await;
    assert_ne!(second, delegating);
    assert!(registry.thread_has_active_turn(child).await);

    // The sub-agent ends; its outcome row arrives late, on the delegating turn.
    harness.core.complete_turn(child, child_turn);
    let outcome_id = ItemId::new();
    harness.core.append(
        parent,
        AgentEvent::ItemCompleted {
            thread: parent,
            turn: delegating,
            item: Item {
                id: outcome_id,
                harness_item_id: OUTCOME_HARNESS_ITEM_ID.into(),
                payload: ItemPayload::Activity {
                    title: "Read data.txt".into(),
                    detail: Some("completed".into()),
                    metadata: None,
                    subagent: Some(link(SubagentAction::Completed, SubagentStatus::Completed)),
                },
                created_at: chrono::Utc::now(),
            },
        },
    );

    // An open browser is shown the row, addressed to the delegating turn.
    let broadcast_turn = ws::recv_until(&mut socket, |message| match message {
        ServerMessage::Event { agent_event, .. } => match *agent_event {
            WireAgentEvent::ItemCompleted { turn, ref item, .. }
                if item.harness_item_id == OUTCOME_HARNESS_ITEM_ID =>
            {
                Some(turn)
            }
            _ => None,
        },
        _ => None,
    })
    .await
    .expect("the late outcome row reaches a connected client");
    assert_eq!(broadcast_turn, delegating);

    // And it is durable: the delegating turn's items carry it, the second turn's do not.
    let turns = store.load_all_turns(project.id, parent).await.unwrap();
    let ids: Vec<TurnId> = turns.iter().map(|turn| turn.id).collect();
    assert_eq!(
        ids,
        vec![delegating, second],
        "no turn is added for the row"
    );
    let outcome = turns[0]
        .items
        .iter()
        .find(|item| item.id == outcome_id)
        .expect("the delegating turn's items carry the outcome row");
    assert!(matches!(
        &outcome.payload,
        ItemPayload::Activity { subagent: Some(link), .. }
            if link.action == SubagentAction::Completed
                && link.status == Some(SubagentStatus::Completed)
    ));
    assert!(turns[1].items.iter().all(|item| item.id != outcome_id));

    // The row's link reaches the driver too, and finds the sub-agent's thread already admitted.
    let (_, event) = probe
        .expect(|event| {
            matches!(event, DriverEvent::LinkFinished { origin, .. } if *origin == "late_item_completed")
        })
        .await;
    let DriverEvent::LinkFinished {
        parent_thread_id,
        item_id,
        outcome,
        ..
    } = event
    else {
        unreachable!("the predicate accepts only link admissions");
    };
    assert_eq!(parent_thread_id, parent);
    assert_eq!(item_id, outcome_id);
    assert_eq!(
        outcome.unwrap(),
        Some(child),
        "no second thread is admitted"
    );
    let threads: HashSet<ThreadId> = store
        .list_threads(project.id)
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(threads, HashSet::from([parent, child]));

    // The sub-agent's own thread reads finished from its own turn, whatever the parent did.
    wait_until("the sub-agent's turn ends", async || {
        !registry.thread_has_active_turn(child).await
    })
    .await;
    let child_file = store.load_thread(project.id, child).await.unwrap().unwrap();
    assert_eq!(child_file.parent_thread_id, Some(parent));
    assert_eq!(child_file.spawned_by_turn_id, Some(delegating));
    let child_turns = store.load_all_turns(project.id, child).await.unwrap();
    assert_eq!(child_turns.len(), 1);
    assert_eq!(child_turns[0].id, child_turn);
    assert_eq!(child_turns[0].status.kind, TurnStatusKind::Completed);
}
