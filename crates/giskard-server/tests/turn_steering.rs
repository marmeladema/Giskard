use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::{SinkExt, StreamExt};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemPayload};
use giskard_harness::{HarnessCapabilities, ThreadHandle};
use giskard_proto::{ClientMessage, RuntimeTurnState, ServerMessage, WireAgentEvent};
use giskard_server::AppState;
use giskard_testenv::fake::{
    self, Call, FakeCore, FakeHarness, Gate, Script, SteerCall, TurnCall, caps,
};
use giskard_testenv::{TestServer, auth, fixtures, ws};

type TestWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct SteeringScript {
    start: Gate,
    steer: Gate,
    steer_error: tokio::sync::Mutex<Option<HarnessError>>,
    interrupted: AtomicBool,
}

impl SteeringScript {
    fn acknowledged() -> Self {
        Self {
            start: Gate::open(),
            steer: Gate::open(),
            steer_error: tokio::sync::Mutex::new(None),
            interrupted: AtomicBool::new(false),
        }
    }

    fn unacknowledged() -> Self {
        Self {
            start: Gate::held(),
            ..Self::acknowledged()
        }
    }

    fn release_start(&self) {
        self.start.release();
    }

    fn hold_steer(&self) {
        self.steer.hold();
    }

    fn release_steer(&self) {
        self.steer.release();
    }

    async fn fail_steer_with(&self, error: HarnessError) {
        *self.steer_error.lock().await = Some(error);
    }
}

#[async_trait::async_trait]
impl Script for SteeringScript {
    fn capabilities(&self) -> HarnessCapabilities {
        caps::STEERING
    }

    async fn start_turn(&self, _core: &FakeCore, call: &TurnCall) -> Result<(), HarnessError> {
        self.start.pass().await;
        call.log.append(AgentEvent::TurnStarted {
            thread: call.thread,
            turn: call.turn,
        });
        Ok(())
    }

    async fn steer_turn(&self, _core: &FakeCore, call: &SteerCall) -> Result<(), HarnessError> {
        if let Some(error) = self.steer_error.lock().await.clone() {
            return Err(error);
        }
        self.steer.pass().await;
        if self.interrupted.load(Ordering::SeqCst) {
            return Err(HarnessError::Protocol("steering was interrupted".into()));
        }
        call.log.append(AgentEvent::ItemCompleted {
            thread: call.thread,
            turn: call.expected_turn,
            item: Item {
                id: ItemId::new(),
                harness_item_id: format!("steered_{}", call.expected_turn),
                payload: ItemPayload::UserMessage {
                    text: call.text.clone(),
                },
                created_at: chrono::Utc::now(),
            },
        });
        Ok(())
    }

    async fn interrupt(
        &self,
        _core: &FakeCore,
        _thread: &ThreadHandle,
    ) -> Result<(), HarnessError> {
        self.interrupted.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct CompactionSteeringScript {
    compact: Gate,
}

#[async_trait::async_trait]
impl Script for CompactionSteeringScript {
    fn capabilities(&self) -> HarnessCapabilities {
        HarnessCapabilities {
            turn_steering: true,
            ..caps::RESUMABLE_COMPACTION
        }
    }

    async fn compact_thread(
        &self,
        _core: &FakeCore,
        _thread: &ThreadHandle,
    ) -> Result<(), HarnessError> {
        self.compact.pass().await;
        Ok(())
    }
}

async fn start_server<S: Script>(harness: Arc<FakeHarness<S>>) -> TestServer {
    TestServer::builder(fake::factory(harness)).start().await
}

async fn create_project(client: &reqwest::Client, base: &str, cookie: &str) -> ProjectId {
    let response = client
        .post(format!("{base}/api/projects"))
        .header("cookie", cookie)
        .json(&serde_json::json!({
            "name": "turn-steering",
            "dir": "/tmp/turn-steering",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    response.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

async fn create_project_and_thread(
    state: &AppState,
    client: &reqwest::Client,
    base: &str,
    cookie: &str,
) -> (ProjectId, ThreadId) {
    let project_id = create_project(client, base, cookie).await;
    let thread_id = fixtures::persist_primary_thread(
        &state.store,
        project_id,
        ThreadId::new(),
        "steering-thread",
        fixtures::fake_native_model(),
    )
    .await;
    let response = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", cookie)
        .json(&serde_json::json!({"thread_id": thread_id}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    (project_id, thread_id)
}

async fn send(socket: &mut TestWebSocket, message: ClientMessage) {
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&message).unwrap().into(),
        ))
        .await
        .unwrap();
}

async fn subscribe(socket: &mut TestWebSocket, thread_id: ThreadId) {
    send(
        socket,
        ClientMessage::Subscribe {
            thread_id,
            since: None,
        },
    )
    .await;
}

async fn wait_for_error(
    socket: &mut TestWebSocket,
    action: &str,
    code: &str,
) -> giskard_proto::ErrorInfo {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), socket.next()).await
            && let Ok(ServerMessage::Error { error }) = serde_json::from_str(&text)
            && error.action.as_deref() == Some(action)
            && error.code == code
        {
            return error;
        }
    }
    panic!("websocket error {code}/{action} was not observed");
}

fn assert_authority_not_steerable(error: &giskard_proto::ErrorInfo) {
    let detail = error
        .detail
        .as_deref()
        .expect("authority rejection should explain why the turn is not steerable");
    assert!(detail.contains("not steerable"));
    assert!(!detail.contains("capability not offered"));
}

async fn wait_for_acknowledged_turn(
    socket: &mut TestWebSocket,
    thread_id: ThreadId,
    turn_id: TurnId,
) {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), socket.next()).await
            && let Ok(ServerMessage::ThreadRuntimeOverview(overview)) = serde_json::from_str(&text)
            && overview.threads.iter().any(|summary| {
                summary.thread_id == thread_id
                    && matches!(
                        summary.turn_state,
                        RuntimeTurnState::Active {
                            turn_id: Some(active_turn)
                        } if active_turn == turn_id
                    )
            })
        {
            return;
        }
    }
    panic!("acknowledged active turn {turn_id} for {thread_id} was not observed");
}

async fn wait_for_turn_completed(socket: &mut TestWebSocket, thread_id: ThreadId) {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), socket.next()).await
            && let Ok(ServerMessage::Event {
                thread_id: event_thread,
                agent_event,
            }) = serde_json::from_str(&text)
            && event_thread == thread_id
            && matches!(*agent_event, WireAgentEvent::TurnCompleted { .. })
        {
            return;
        }
    }
    panic!("turn completion for {thread_id} was not observed");
}

async fn wait_for_live_snapshot(
    socket: &mut TestWebSocket,
    thread_id: ThreadId,
) -> giskard_proto::LiveTurnSnapshot {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), socket.next()).await
            && let Ok(ServerMessage::LiveTurnSnapshot(snapshot)) = serde_json::from_str(&text)
            && snapshot.thread_id == thread_id
        {
            return snapshot;
        }
    }
    panic!("live turn snapshot for {thread_id} was not observed");
}

async fn wait_for_started_turn(
    harness: &FakeHarness<SteeringScript>,
    thread_id: ThreadId,
) -> TurnId {
    harness
        .core
        .wait_for_call(|call| match call {
            Call::StartTurn { thread, turn, .. } if *thread == thread_id => Some(*turn),
            _ => None,
        })
        .await
}

fn start_calls(core: &FakeCore) -> usize {
    core.count(|call| matches!(call, Call::StartTurn { .. }))
}

fn steer_calls(core: &FakeCore) -> usize {
    core.count(|call| matches!(call, Call::SteerTurn { .. }))
}

#[tokio::test]
async fn steer_input_targets_active_turn_and_persists_same_turn_user_message() {
    let harness = FakeHarness::new(SteeringScript::acknowledged());
    let server = start_server(harness.clone()).await;
    let state = &server.state;
    let client = reqwest::Client::new();
    let cookie = auth::login(&client, &server.base).await;
    let (project_id, thread_id) =
        create_project_and_thread(state, &client, &server.base, &cookie).await;
    let mut socket = ws::connect(server.addr, &cookie).await;
    subscribe(&mut socket, thread_id).await;
    send(
        &mut socket,
        ClientMessage::SendInput {
            thread_id,
            text: "original prompt".into(),
            attachments: Vec::new(),
        },
    )
    .await;
    let turn_id = wait_for_started_turn(&harness, thread_id).await;
    wait_for_acknowledged_turn(&mut socket, thread_id, turn_id).await;

    send(
        &mut socket,
        ClientMessage::SteerInput {
            thread_id,
            turn_id: TurnId::new(),
            text: "stale steer".into(),
        },
    )
    .await;
    let error = wait_for_error(&mut socket, "steer_input", "turn_not_steerable").await;
    assert_eq!(error.thread_id, Some(thread_id));
    assert_authority_not_steerable(&error);
    assert_eq!(steer_calls(&harness.core), 0);

    send(
        &mut socket,
        ClientMessage::SteerInput {
            thread_id,
            turn_id,
            text: "steered follow-up".into(),
        },
    )
    .await;
    let called = harness
        .core
        .wait_for_call(|call| match call {
            Call::SteerTurn {
                thread,
                expected_turn,
                text,
            } => Some((*thread, *expected_turn, text.clone())),
            _ => None,
        })
        .await;
    assert_eq!(called, (thread_id, turn_id, "steered follow-up".into()));
    assert_eq!(start_calls(&harness.core), 1);

    harness.core.complete_turn(thread_id, turn_id);
    wait_for_turn_completed(&mut socket, thread_id).await;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    let saved = loop {
        if let Some(turn) = state
            .store
            .load_all_turns(project_id, thread_id)
            .await
            .unwrap()
            .into_iter()
            .find(|turn| turn.id == turn_id)
        {
            break turn;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    };
    assert_eq!(saved.user_input.as_text(), Some("original prompt"));
    assert!(saved.items.iter().any(|item| matches!(
        &item.payload,
        ItemPayload::UserMessage { text } if text == "steered follow-up"
    )));
}

#[tokio::test]
async fn thread_open_and_start_responses_use_attached_harness_steering_capability() {
    let harness = FakeHarness::new(SteeringScript::acknowledged());
    let server = start_server(harness.clone()).await;
    let client = reqwest::Client::new();
    let cookie = auth::login(&client, &server.base).await;
    let project_id = create_project(&client, &server.base, &cookie).await;
    let existing_thread = fixtures::persist_primary_thread(
        &server.state.store,
        project_id,
        ThreadId::new(),
        "steering-capability-existing",
        fixtures::fake_native_model(),
    )
    .await;
    let opened = client
        .post(format!("{}/api/projects/{project_id}/threads", server.base))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"thread_id": existing_thread}))
        .send()
        .await
        .unwrap();
    assert_eq!(opened.status(), reqwest::StatusCode::OK);
    assert_eq!(
        opened.json::<serde_json::Value>().await.unwrap()["turn_steering"],
        true
    );

    let started = client
        .post(format!(
            "{}/api/projects/{project_id}/threads/start",
            server.base
        ))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "text": "start with captured steering capability",
            "model_ref": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
            "mode": "build",
            "permission_preset": "ask_first",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(started.status(), reqwest::StatusCode::OK);
    let started = started.json::<serde_json::Value>().await.unwrap();
    assert_eq!(started["turn_steering"], true);
    let thread_id = started["thread_id"].as_str().unwrap().parse().unwrap();
    let turn_id = started["turn_id"].as_str().unwrap().parse().unwrap();
    harness.core.complete_turn(thread_id, turn_id);
}

#[tokio::test]
async fn steer_input_rejects_turn_before_harness_acknowledgement() {
    let harness = FakeHarness::new(SteeringScript::unacknowledged());
    let server = start_server(harness.clone()).await;
    let client = reqwest::Client::new();
    let cookie = auth::login(&client, &server.base).await;
    let (_, thread_id) =
        create_project_and_thread(&server.state, &client, &server.base, &cookie).await;
    let mut start_socket = ws::connect(server.addr, &cookie).await;
    let mut steering_socket = ws::connect(server.addr, &cookie).await;
    subscribe(&mut start_socket, thread_id).await;
    send(
        &mut start_socket,
        ClientMessage::SendInput {
            thread_id,
            text: "not acknowledged yet".into(),
            attachments: Vec::new(),
        },
    )
    .await;
    let turn_id = wait_for_started_turn(&harness, thread_id).await;
    send(
        &mut steering_socket,
        ClientMessage::SteerInput {
            thread_id,
            turn_id,
            text: "too early".into(),
        },
    )
    .await;
    let error = wait_for_error(&mut steering_socket, "steer_input", "turn_not_steerable").await;
    assert_eq!(error.thread_id, Some(thread_id));
    assert_authority_not_steerable(&error);
    assert_eq!(steer_calls(&harness.core), 0);
    harness.script.release_start();
    wait_for_acknowledged_turn(&mut start_socket, thread_id, turn_id).await;
    harness.core.complete_turn(thread_id, turn_id);
    wait_for_turn_completed(&mut start_socket, thread_id).await;
}

#[tokio::test]
async fn steer_input_rejects_compaction_owner() {
    let harness = FakeHarness::new(CompactionSteeringScript {
        compact: Gate::held(),
    });
    let server = start_server(harness.clone()).await;
    let client = reqwest::Client::new();
    let cookie = auth::login(&client, &server.base).await;
    let (_, thread_id) =
        create_project_and_thread(&server.state, &client, &server.base, &cookie).await;
    let mut compact_socket = ws::connect(server.addr, &cookie).await;
    let mut steering_socket = ws::connect(server.addr, &cookie).await;
    subscribe(&mut compact_socket, thread_id).await;
    send(
        &mut compact_socket,
        ClientMessage::CompactContext { thread_id },
    )
    .await;
    harness
        .core
        .wait_for_calls(|call| matches!(call, Call::CompactThread { .. }), 1)
        .await;
    send(
        &mut steering_socket,
        ClientMessage::SteerInput {
            thread_id,
            turn_id: TurnId::new(),
            text: "do not steer compaction".into(),
        },
    )
    .await;
    let error = wait_for_error(&mut steering_socket, "steer_input", "turn_not_steerable").await;
    assert_eq!(error.thread_id, Some(thread_id));
    assert_authority_not_steerable(&error);
    assert_eq!(steer_calls(&harness.core), 0);
    harness.script.compact.release();
}

#[tokio::test]
async fn steer_input_failure_keeps_active_turn_owned() {
    let harness = FakeHarness::new(SteeringScript::acknowledged());
    harness
        .script
        .fail_steer_with(HarnessError::Protocol("scripted steer failure".into()))
        .await;
    let server = start_server(harness.clone()).await;
    let client = reqwest::Client::new();
    let cookie = auth::login(&client, &server.base).await;
    let (_, thread_id) =
        create_project_and_thread(&server.state, &client, &server.base, &cookie).await;
    let mut socket = ws::connect(server.addr, &cookie).await;
    subscribe(&mut socket, thread_id).await;
    send(
        &mut socket,
        ClientMessage::SendInput {
            thread_id,
            text: "keep this turn active".into(),
            attachments: Vec::new(),
        },
    )
    .await;
    let turn_id = wait_for_started_turn(&harness, thread_id).await;
    wait_for_acknowledged_turn(&mut socket, thread_id, turn_id).await;
    send(
        &mut socket,
        ClientMessage::SteerInput {
            thread_id,
            turn_id,
            text: "this fails".into(),
        },
    )
    .await;
    assert_eq!(
        wait_for_error(&mut socket, "steer_input", "turn_steer_failed")
            .await
            .thread_id,
        Some(thread_id)
    );
    assert_eq!(steer_calls(&harness.core), 1);
    assert!(
        server
            .state
            .registry
            .thread_has_active_turn(thread_id)
            .await
    );
    harness.core.complete_turn(thread_id, turn_id);
    wait_for_turn_completed(&mut socket, thread_id).await;
}

#[tokio::test]
async fn pending_steer_does_not_block_interrupt_on_same_websocket() {
    let harness = FakeHarness::new(SteeringScript::acknowledged());
    harness.script.hold_steer();
    let server = start_server(harness.clone()).await;
    let client = reqwest::Client::new();
    let cookie = auth::login(&client, &server.base).await;
    let (_, thread_id) =
        create_project_and_thread(&server.state, &client, &server.base, &cookie).await;
    let mut socket = ws::connect(server.addr, &cookie).await;
    subscribe(&mut socket, thread_id).await;
    send(
        &mut socket,
        ClientMessage::SendInput {
            thread_id,
            text: "interrupt while steering".into(),
            attachments: Vec::new(),
        },
    )
    .await;
    let turn_id = wait_for_started_turn(&harness, thread_id).await;
    wait_for_acknowledged_turn(&mut socket, thread_id, turn_id).await;
    send(
        &mut socket,
        ClientMessage::SteerInput {
            thread_id,
            turn_id,
            text: "held in the harness".into(),
        },
    )
    .await;
    harness
        .core
        .wait_for_calls(|call| matches!(call, Call::SteerTurn { .. }), 1)
        .await;
    send(&mut socket, ClientMessage::Interrupt { thread_id }).await;
    harness
        .core
        .wait_for_calls(|call| matches!(call, Call::Interrupt { .. }), 1)
        .await;
    harness.script.release_steer();
    harness.core.complete_turn(thread_id, turn_id);
    wait_for_turn_completed(&mut socket, thread_id).await;
}

#[tokio::test]
async fn reconnect_snapshot_contains_steered_input() {
    let harness = FakeHarness::new(SteeringScript::acknowledged());
    let server = start_server(harness.clone()).await;
    let client = reqwest::Client::new();
    let cookie = auth::login(&client, &server.base).await;
    let (_, thread_id) =
        create_project_and_thread(&server.state, &client, &server.base, &cookie).await;
    let mut first = ws::connect(server.addr, &cookie).await;
    subscribe(&mut first, thread_id).await;
    send(
        &mut first,
        ClientMessage::SendInput {
            thread_id,
            text: "original reconnect prompt".into(),
            attachments: Vec::new(),
        },
    )
    .await;
    let turn_id = wait_for_started_turn(&harness, thread_id).await;
    wait_for_acknowledged_turn(&mut first, thread_id, turn_id).await;
    send(
        &mut first,
        ClientMessage::SteerInput {
            thread_id,
            turn_id,
            text: "steered before reconnect".into(),
        },
    )
    .await;
    harness
        .core
        .wait_for_calls(|call| matches!(call, Call::SteerTurn { .. }), 1)
        .await;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    loop {
        let contains_steer = server
            .state
            .registry
            .thread_runtime(thread_id)
            .await
            .and_then(|runtime| runtime.live_snapshot())
            .is_some_and(|snapshot| has_steered_input(&snapshot));
        if contains_steer {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    drop(first);

    let mut reconnected = ws::connect(server.addr, &cookie).await;
    subscribe(&mut reconnected, thread_id).await;
    let snapshot = wait_for_live_snapshot(&mut reconnected, thread_id).await;
    assert_eq!(snapshot.turn_id, turn_id);
    assert!(has_steered_input(&snapshot));
    harness.core.complete_turn(thread_id, turn_id);
    wait_for_turn_completed(&mut reconnected, thread_id).await;
}

fn has_steered_input(snapshot: &giskard_proto::LiveTurnSnapshot) -> bool {
    snapshot.accumulated.iter().any(|event| {
        matches!(
            event,
            WireAgentEvent::ItemCompleted { item, .. }
                if matches!(
                    &item.payload,
                    giskard_proto::WireItemPayload::UserMessage { text }
                        if text == "steered before reconnect"
                )
        )
    })
}
