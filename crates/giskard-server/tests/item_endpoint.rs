use async_trait::async_trait;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ThreadId, TurnId};
use giskard_core::item::{
    CommandExecutionStart, Item, ItemKind, ItemPayload, ItemStart, REASONING_PREVIEW_MAX_BYTES,
};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
use giskard_proto::{ClientMessage, ServerMessage, WireAgentEvent};
use giskard_testenv::fake::{self, FakeCore, FakeHarness, Gate, Script, TurnCall};
use giskard_testenv::{TestProject, TestServer, factory, fixtures, ws};

async fn setup() -> (TestServer, TestProject, ThreadId) {
    let server = TestServer::builder(factory::fixture(fixtures::completed_turn_fixture()))
        .start()
        .await;
    let project = server.create_project("proj").await;
    let thread = server
        .register_thread(project.id, fixtures::COMPLETED_TURN_HARNESS_THREAD_ID)
        .await;
    (server, project, thread)
}

struct RunningItemScript {
    item_id: ItemId,
    complete: Gate,
    finish: Gate,
}

impl RunningItemScript {
    fn new() -> Self {
        Self {
            item_id: ItemId::new(),
            complete: Gate::held(),
            finish: Gate::held(),
        }
    }
}

#[async_trait]
impl Script for RunningItemScript {
    fn native_thread_id(&self, _thread: ThreadId) -> String {
        "running-item-thread".into()
    }

    async fn start_turn(&self, _core: &FakeCore, call: &TurnCall) -> Result<(), HarnessError> {
        call.log.append(AgentEvent::TurnStarted {
            thread: call.thread,
            turn: call.turn,
        });
        call.log.append(AgentEvent::ItemStarted {
            thread: call.thread,
            turn: call.turn,
            item: ItemStart {
                id: self.item_id,
                harness_item_id: "command-1".into(),
                kind: ItemKind::CommandExecution,
                command: Some(CommandExecutionStart {
                    command: "printf live".into(),
                    cwd: "/tmp/project".into(),
                    status: Some("in_progress".into()),
                    process_id: Some("process-1".into()),
                    started_at_ms: Some(1_785_000_000_000),
                }),
                tool: None,
            },
        });
        self.complete.pass().await;
        call.log.append(AgentEvent::ItemCompleted {
            thread: call.thread,
            turn: call.turn,
            item: Item {
                id: self.item_id,
                harness_item_id: "command-1".into(),
                payload: ItemPayload::CommandExecution {
                    command: "printf live".into(),
                    cwd: "/tmp/project".into(),
                    output: "live output".into(),
                    output_truncated: false,
                    output_original_bytes: None,
                    output_original_lines: None,
                    exit_code: Some(0),
                    status: Some("completed".into()),
                    process_id: Some("process-1".into()),
                    duration_ms: Some(10),
                },
                created_at: Utc::now(),
            },
        });
        self.finish.pass().await;
        call.log.append(AgentEvent::TurnCompleted {
            thread: call.thread,
            turn: call.turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        });
        Ok(())
    }
}

async fn wait_for_item_event(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    item_id: ItemId,
    completed: bool,
) -> TurnId {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), socket.next()).await
            && let Ok(ServerMessage::Event { agent_event, .. }) = serde_json::from_str(&text)
        {
            match *agent_event {
                WireAgentEvent::ItemStarted { turn, item, .. }
                    if !completed && item.id == item_id =>
                {
                    return turn;
                }
                WireAgentEvent::ItemCompleted { turn, item, .. }
                    if completed && item.id == item_id =>
                {
                    return turn;
                }
                _ => {}
            }
        }
    }
    panic!("item event was not observed");
}

async fn wait_for_turn_completed(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    turn_id: TurnId,
) {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), socket.next()).await
            && let Ok(ServerMessage::Event { agent_event, .. }) = serde_json::from_str(&text)
            && matches!(*agent_event, WireAgentEvent::TurnCompleted { turn, .. } if turn == turn_id)
        {
            return;
        }
    }
    panic!("turn completion was not observed");
}

async fn get_item(
    server: &TestServer,
    project_id: giskard_core::ids::ProjectId,
    thread_id: ThreadId,
    turn_id: TurnId,
    item_id: ItemId,
) -> reqwest::Response {
    server
        .client
        .get(format!(
            "{}/api/projects/{project_id}/threads/{thread_id}/turns/{turn_id}/items/{item_id}",
            server.base
        ))
        .header("cookie", &server.cookie)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn running_item_is_served_started_completed_and_persisted() {
    let harness = FakeHarness::new(RunningItemScript::new());
    let server = TestServer::spawn(fake::factory(harness.clone())).await;
    let project = server.create_project("running-item").await;
    let thread_id = server
        .register_thread(project.id, "running-item-thread")
        .await;
    let mut socket = server.ws().await;
    socket
        .send(ws::text(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        }))
        .await
        .unwrap();
    socket
        .send(ws::text(&ClientMessage::SendInput {
            thread_id,
            text: "run command".into(),
            attachments: Vec::new(),
        }))
        .await
        .unwrap();

    let item_id = harness.script.item_id;
    let turn_id = wait_for_item_event(&mut socket, item_id, false).await;
    let started: serde_json::Value = get_item(&server, project.id, thread_id, turn_id, item_id)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(started["state"], "started");
    assert_eq!(started["item"]["command"]["command"], "printf live");
    assert!(started["item"].get("output").is_none());
    assert!(started["item"].get("exit_code").is_none());
    assert_eq!(
        get_item(&server, project.id, thread_id, TurnId::new(), item_id)
            .await
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );

    harness.script.complete.release();
    assert_eq!(
        wait_for_item_event(&mut socket, item_id, true).await,
        turn_id
    );
    let completed: serde_json::Value = get_item(&server, project.id, thread_id, turn_id, item_id)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(completed["state"], "completed");
    assert_eq!(
        completed["item"]["payload"]["output"]["preview"],
        "live output"
    );
    assert_eq!(
        completed["item"]["payload"]["output"]["output_available"],
        true
    );

    harness.script.finish.release();
    wait_for_turn_completed(&mut socket, turn_id).await;
    let persisted: serde_json::Value = get_item(&server, project.id, thread_id, turn_id, item_id)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(persisted["state"], "completed");
    assert_eq!(
        persisted["item"]["payload"]["output"]["preview"],
        "live output"
    );
}

#[tokio::test]
async fn persisted_item_is_returned_complete_as_json() {
    let (server, project, thread) = setup().await;
    let turn = fixtures::completed_turn("complete agent text", fixtures::fake_native_model());
    let turn_id = turn.id;
    let item_id = turn.items[0].id;
    server
        .state
        .store
        .append_turn(project.id, thread, &turn)
        .await
        .unwrap();

    let response = server
        .client
        .get(format!(
            "{}/api/projects/{}/threads/{thread}/turns/{turn_id}/items/{item_id}",
            server.base, project.id
        ))
        .header("cookie", &server.cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.headers()[reqwest::header::CONTENT_TYPE],
        "application/json"
    );
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["state"], "completed");
    assert_eq!(body["item"]["payload"]["text"], "complete agent text");
}

#[tokio::test]
async fn history_previews_reasoning_but_item_read_is_full() {
    let (server, project, thread) = setup().await;
    let mut turn = fixtures::completed_turn("prompt", fixtures::fake_native_model());
    let full = format!("summary\n{}\ntail\n", "x".repeat(3 * 1024));
    let item_id = ItemId::new();
    turn.items = vec![Item {
        id: item_id,
        harness_item_id: "reasoning".into(),
        payload: ItemPayload::Reasoning { text: full.clone() },
        created_at: Utc::now(),
    }];
    let turn_id = turn.id;
    server
        .state
        .store
        .append_turn(project.id, thread, &turn)
        .await
        .unwrap();

    let history: serde_json::Value = server
        .client
        .get(format!(
            "{}/api/projects/{}/threads/{thread}/history",
            server.base, project.id
        ))
        .header("cookie", &server.cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let payload = &history["turns"][0]["items"][0]["payload"];
    let prefix = payload["text"].as_str().unwrap();
    assert!(prefix.len() <= REASONING_PREVIEW_MAX_BYTES);
    assert!(prefix.starts_with("summary\n"));
    assert!(prefix.ends_with('\n'));
    assert_eq!(payload["preview"]["prefix_bytes"], prefix.len() as u64);
    assert_eq!(payload["preview"]["total_bytes"], full.len() as u64);

    let item: serde_json::Value = server
        .client
        .get(format!(
            "{}/api/projects/{}/threads/{thread}/turns/{turn_id}/items/{item_id}",
            server.base, project.id
        ))
        .header("cookie", &server.cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(item["item"]["payload"]["text"], full);
    assert!(item["item"]["payload"].get("preview").is_none());

    let mut long_first_turn = fixtures::completed_turn("prompt", fixtures::fake_native_model());
    let long_first = format!("{}\nrest\n", "y".repeat(2 * 1024));
    long_first_turn.items = vec![Item {
        id: ItemId::new(),
        harness_item_id: "long-first".into(),
        payload: ItemPayload::Reasoning {
            text: long_first.clone(),
        },
        created_at: Utc::now(),
    }];
    server
        .state
        .store
        .append_turn(project.id, thread, &long_first_turn)
        .await
        .unwrap();
    let history: serde_json::Value = server
        .client
        .get(format!(
            "{}/api/projects/{}/threads/{thread}/history",
            server.base, project.id
        ))
        .header("cookie", &server.cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let turn = history["turns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|turn| turn["id"] == long_first_turn.id.to_string())
        .unwrap();
    assert_eq!(
        turn["items"][0]["payload"]["text"],
        format!("{}\n", "y".repeat(2 * 1024))
    );
}

#[tokio::test]
async fn item_lookup_enforces_turn_thread_and_project_containment() {
    let (server, project, thread) = setup().await;
    let turn = fixtures::completed_turn("one", fixtures::fake_native_model());
    let turn_id = turn.id;
    let item_id = turn.items[0].id;
    server
        .state
        .store
        .append_turn(project.id, thread, &turn)
        .await
        .unwrap();
    let other_turn = fixtures::completed_turn("two", fixtures::fake_native_model());
    server
        .state
        .store
        .append_turn(project.id, thread, &other_turn)
        .await
        .unwrap();

    for (candidate_thread, candidate_turn, candidate_item) in [
        (thread, turn_id, ItemId::new()),
        (thread, other_turn.id, item_id),
        (ThreadId::new(), turn_id, item_id),
    ] {
        let response = server
            .client
            .get(format!(
                "{}/api/projects/{}/threads/{candidate_thread}/turns/{candidate_turn}/items/{candidate_item}",
                server.base, project.id
            ))
            .header("cookie", &server.cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    }

    let other = server.create_project("other").await;
    let response = server
        .client
        .get(format!(
            "{}/api/projects/{}/threads/{thread}/turns/{turn_id}/items/{item_id}",
            server.base, other.id
        ))
        .header("cookie", &server.cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
}
