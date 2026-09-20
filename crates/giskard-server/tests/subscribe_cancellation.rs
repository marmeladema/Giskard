use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use giskard_core::error::HarnessError;
use giskard_core::ids::ThreadId;
use giskard_harness::{OpenThreadOptions, ThreadHandle};
use giskard_proto::{ClientMessage, ServerMessage};
use giskard_testenv::fake::{self, Call, FakeCore, FakeHarness, Gate, Script};
use giskard_testenv::{TestProject, TestServer, fixtures, ws};

struct HeldOpenScript {
    open: Gate,
}

#[async_trait::async_trait]
impl Script for HeldOpenScript {
    async fn open_thread(
        &self,
        core: &FakeCore,
        opts: &OpenThreadOptions,
    ) -> Result<ThreadHandle, HarnessError> {
        self.open.pass().await;
        Ok(core.opened(opts, self.native_thread_id(opts.thread)))
    }
}

async fn setup() -> (
    TestServer,
    TestProject,
    Arc<FakeHarness<HeldOpenScript>>,
    ThreadId,
) {
    let harness = FakeHarness::new(HeldOpenScript { open: Gate::held() });
    let server = TestServer::builder(fake::factory(harness.clone()))
        .start()
        .await;
    let project = server.create_project("subscribe-cancellation").await;
    let thread_id = fixtures::persist_primary_thread(
        &server.state.store,
        project.id,
        ThreadId::new(),
        "subscribe-cancellation-thread",
        fixtures::fake_native_model(),
    )
    .await;
    (server, project, harness, thread_id)
}

async fn wait_for_open(core: &FakeCore) {
    core.wait_for_calls(|call| matches!(call, Call::Open { .. }), 1)
        .await;
}

async fn wait_for_client_count(server: &TestServer, expected: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if server.state.hub.client_count().await == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!(
        "hub client count did not reach {expected}; current count is {}",
        server.state.hub.client_count().await
    );
}

async fn wait_for_loaded_binding(server: &TestServer, thread_id: ThreadId) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if server
            .state
            .registry
            .loaded_thread_binding(thread_id)
            .await
            .is_some()
        {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("thread binding was never loaded");
}

/// Round-trips a Ping so the caller knows every frame sent before it has been processed by the
/// connection's receive loop, which handles frames in order. Unrelated server pushes are skipped,
/// but any bootstrap message for `thread_id` is a failure: the gate is still held when this is
/// used, so nothing should have reached the bootstrap's send phase.
async fn wait_for_pong(socket: &mut ws::TestWs, thread_id: ThreadId) {
    ws::send(socket, &ClientMessage::Ping).await;
    loop {
        match receive_message(socket).await {
            ServerMessage::Pong => return,
            ServerMessage::ThreadState(state) if state.metadata.thread_id == thread_id => {
                panic!("held bootstrap sent ThreadState")
            }
            ServerMessage::HistoryDelta {
                thread_id: message_thread,
                ..
            } if message_thread == thread_id => panic!("held bootstrap sent HistoryDelta"),
            ServerMessage::LiveTurnSnapshot(snapshot) if snapshot.thread_id == thread_id => {
                panic!("held bootstrap sent LiveTurnSnapshot")
            }
            ServerMessage::RunningTasks {
                thread_id: message_thread,
                ..
            } if message_thread == thread_id => panic!("held bootstrap sent RunningTasks"),
            _ => {}
        }
    }
}

async fn receive_message(socket: &mut ws::TestWs) -> ServerMessage {
    let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("timed out waiting for websocket message")
        .expect("websocket closed while waiting for message")
        .expect("websocket read failed while waiting for message");
    let tokio_tungstenite::tungstenite::Message::Text(text) = frame else {
        panic!("expected websocket text message, got {frame:?}");
    };
    serde_json::from_str(&text).expect("server message should be valid JSON")
}

#[tokio::test]
async fn close_disconnects_client_while_cold_attach_is_held() {
    let (server, _project, harness, thread_id) = setup().await;
    let mut socket = server.ws().await;
    wait_for_client_count(&server, 1).await;

    ws::send(
        &mut socket,
        &ClientMessage::Subscribe {
            thread_id,
            since: None,
        },
    )
    .await;
    wait_for_open(&harness.core).await;
    assert_eq!(server.state.hub.client_count().await, 1);

    drop(socket);
    wait_for_client_count(&server, 0).await;

    let calls_before_release = harness.core.calls().len();
    harness.script.open.release();
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(harness.core.calls().len(), calls_before_release);
    assert_eq!(server.state.hub.client_count().await, 0);
}

#[tokio::test]
async fn same_thread_resubscribe_supersedes_held_bootstrap() {
    let (server, _project, harness, thread_id) = setup().await;
    let mut socket = server.ws().await;
    let subscribe = ClientMessage::Subscribe {
        thread_id,
        since: None,
    };

    ws::send(&mut socket, &subscribe).await;
    wait_for_open(&harness.core).await;
    ws::send(&mut socket, &subscribe).await;
    // Fence the resubscribe before opening the gate. The receive loop handles frames in order, so
    // a Pong for a Ping sent after the second `Subscribe` proves that frame was processed and the
    // first generation flagged as cancelled. Release without the fence and the held bootstrap can
    // wake first, pass its cancel check and send a full second bootstrap.
    wait_for_pong(&mut socket, thread_id).await;
    harness.script.open.release();

    let mut thread_states = 0;
    let mut history_deltas = 0;
    loop {
        match receive_message(&mut socket).await {
            ServerMessage::ThreadState(state) if state.metadata.thread_id == thread_id => {
                thread_states += 1;
            }
            ServerMessage::HistoryDelta {
                thread_id: message_thread,
                ..
            } if message_thread == thread_id => history_deltas += 1,
            ServerMessage::RunningTasks {
                thread_id: message_thread,
                ..
            } if message_thread == thread_id => break,
            _ => {}
        }
    }

    ws::send(&mut socket, &ClientMessage::Ping).await;
    loop {
        match receive_message(&mut socket).await {
            ServerMessage::Pong => break,
            ServerMessage::ThreadState(state) if state.metadata.thread_id == thread_id => {
                thread_states += 1;
            }
            ServerMessage::HistoryDelta {
                thread_id: message_thread,
                ..
            } if message_thread == thread_id => history_deltas += 1,
            _ => {}
        }
    }

    assert_eq!(thread_states, 1);
    assert_eq!(history_deltas, 1);
}

#[tokio::test]
async fn unsubscribe_cancels_held_bootstrap_before_it_sends() {
    let (server, _project, harness, thread_id) = setup().await;
    let mut socket = server.ws().await;

    ws::send(
        &mut socket,
        &ClientMessage::Subscribe {
            thread_id,
            since: None,
        },
    )
    .await;
    wait_for_open(&harness.core).await;
    ws::send(&mut socket, &ClientMessage::Unsubscribe { thread_id }).await;
    wait_for_pong(&mut socket, thread_id).await;

    harness.script.open.release();
    // Give the released bootstrap room to misbehave before the sentinel goes out. Waiting for the
    // binding proves the task resumed — `ensure_thread_open` returning is what loads it — and the
    // yields let it run through the history and snapshot work a bootstrap that ignored its cancel
    // flag would do. Ping straight after the release and the Pong outruns the attach, so the loop
    // below would stop reading before a broken cancel had sent anything.
    wait_for_loaded_binding(&server, thread_id).await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    ws::send(&mut socket, &ClientMessage::Ping).await;

    loop {
        match receive_message(&mut socket).await {
            ServerMessage::Pong => break,
            ServerMessage::ThreadState(state) if state.metadata.thread_id == thread_id => {
                panic!("cancelled bootstrap sent ThreadState")
            }
            ServerMessage::HistoryDelta {
                thread_id: message_thread,
                ..
            } if message_thread == thread_id => {
                panic!("cancelled bootstrap sent HistoryDelta")
            }
            ServerMessage::LiveTurnSnapshot(snapshot) if snapshot.thread_id == thread_id => {
                panic!("cancelled bootstrap sent LiveTurnSnapshot")
            }
            ServerMessage::RunningTasks {
                thread_id: message_thread,
                ..
            } if message_thread == thread_id => {
                panic!("cancelled bootstrap sent RunningTasks")
            }
            _ => {}
        }
    }
}
