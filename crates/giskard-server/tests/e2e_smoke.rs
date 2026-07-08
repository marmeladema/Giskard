use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemDelta, ItemKind, ItemPayload, ItemStart};
use giskard_core::model::ModelRef;
use giskard_core::token::TokenUsage;
use giskard_core::turn::{ApprovalPolicy, Mode, TurnStatus, TurnStatusKind};
use giskard_harness_replay::{ReplayFixture, ReplayHarness};
use giskard_persist::store::ProjectConfig;
use giskard_proto::{ClientMessage, ErrorSeverity, ServerMessage, WireAgentEvent};
use giskard_server::{AppState, HarnessFactory, build_app};

struct TestFactory {
    fixture: ReplayFixture,
}

#[async_trait::async_trait]
impl HarnessFactory for TestFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, HarnessError> {
        Ok(Arc::new(ReplayHarness::from_fixture(self.fixture.clone())))
    }
}

fn make_fixture() -> ReplayFixture {
    let thread = ThreadId::new();
    let turn = TurnId::new();
    let it_1 = ItemId::new();
    let now = chrono::Utc::now();

    ReplayFixture::from_events(vec![
        AgentEvent::ThreadOpened {
            thread,
            harness_thread_id: "th_test".into(),
        },
        AgentEvent::TurnStarted { thread, turn },
        AgentEvent::ItemStarted {
            thread,
            turn,
            item: ItemStart {
                id: it_1,
                harness_item_id: "it_1".into(),
                kind: ItemKind::AgentMessage,
                command: None,
                tool: None,
            },
        },
        AgentEvent::ItemDelta {
            thread,
            turn,
            item_id: it_1,
            delta: ItemDelta::Text {
                text: "Hello from replay!".into(),
            },
        },
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: it_1,
                harness_item_id: "it_1".into(),
                payload: ItemPayload::AgentMessage {
                    text: "Hello from replay!".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::new(100, 50),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        },
    ])
}

fn duplicate_history_fixture(
    thread: ThreadId,
    old_turn: TurnId,
    new_turn: TurnId,
) -> ReplayFixture {
    let now = chrono::Utc::now();
    let old_user = ItemId::new();
    let old_agent = ItemId::new();
    let new_user = ItemId::new();
    let new_agent = ItemId::new();

    ReplayFixture::from_events(vec![
        AgentEvent::ThreadOpened {
            thread,
            harness_thread_id: "th_dupe".into(),
        },
        AgentEvent::TurnStarted {
            thread,
            turn: old_turn,
        },
        AgentEvent::ItemCompleted {
            thread,
            turn: old_turn,
            item: Item {
                id: old_user,
                harness_item_id: "old_user".into(),
                payload: ItemPayload::UserMessage {
                    text: "old input".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::ItemCompleted {
            thread,
            turn: old_turn,
            item: Item {
                id: old_agent,
                harness_item_id: "old_agent".into(),
                payload: ItemPayload::AgentMessage {
                    text: "old answer".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::TurnCompleted {
            thread,
            turn: old_turn,
            usage: TokenUsage::new(10, 10),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        },
        AgentEvent::TurnStarted {
            thread,
            turn: new_turn,
        },
        AgentEvent::ItemCompleted {
            thread,
            turn: new_turn,
            item: Item {
                id: new_user,
                harness_item_id: "new_user".into(),
                payload: ItemPayload::UserMessage {
                    text: "new input".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::ItemCompleted {
            thread,
            turn: new_turn,
            item: Item {
                id: new_agent,
                harness_item_id: "new_agent".into(),
                payload: ItemPayload::AgentMessage {
                    text: "new answer".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::TurnCompleted {
            thread,
            turn: new_turn,
            usage: TokenUsage::new(20, 20),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        },
    ])
}

/// A turn that starts, hits a fatal (non-retryable) error, and produces no agent output — the
/// sequence the Codex harness synthesizes for e.g. a quota rejection. The `Failed` `TurnCompleted`
/// carries the real message so it can be persisted to history rather than lost as a toast.
fn failed_turn_fixture(thread: ThreadId, turn: TurnId) -> ReplayFixture {
    let message = "usageLimitExceeded: Quota exceeded. Check your plan and billing details.";
    ReplayFixture::from_events(vec![
        AgentEvent::ThreadOpened {
            thread,
            harness_thread_id: "th_fail".into(),
        },
        AgentEvent::TurnStarted { thread, turn },
        AgentEvent::Error {
            thread,
            turn: Some(turn),
            error: HarnessError::Protocol(message.into()),
        },
        AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Failed,
                message: Some(message.into()),
            },
        },
    ])
}

/// A turn that emits a non-fatal advisory (a Codex warning) alongside normal agent output. The
/// notice must reach the client as a `Notice` event and must not fail the turn.
fn notice_fixture(thread: ThreadId, turn: TurnId) -> ReplayFixture {
    let item = ItemId::new();
    ReplayFixture::from_events(vec![
        AgentEvent::ThreadOpened {
            thread,
            harness_thread_id: "th_notice".into(),
        },
        AgentEvent::TurnStarted { thread, turn },
        AgentEvent::Notice {
            thread,
            turn: None,
            message: "Model metadata for `glm` not found. Using fallback.".into(),
        },
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: item,
                harness_item_id: "a1".into(),
                payload: ItemPayload::AgentMessage { text: "hi".into() },
                created_at: chrono::Utc::now(),
            },
        },
        AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::new(1, 1),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        },
    ])
}

fn generate_password_hash(password: &str) -> String {
    use argon2::password_hash::SaltString;
    use argon2::{Argon2, PasswordHasher};
    use rand::rngs::OsRng;
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string()
}

async fn start_server_with_extra_config(
    port: u16,
    extra_config: &str,
) -> (tempfile::TempDir, Arc<AppState>) {
    start_server_with_fixture_and_extra_config(port, make_fixture(), extra_config).await
}

async fn start_server_with_fixture_and_extra_config(
    port: u16,
    fixture: ReplayFixture,
    extra_config: &str,
) -> (tempfile::TempDir, Arc<AppState>) {
    let tmp = tempfile::TempDir::new().unwrap();

    let hash = generate_password_hash("testpass");
    let config_toml = format!(
        r#"
[server]
bind = "127.0.0.1:{port}"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30

{extra_config}
"#
    );
    tokio::fs::write(tmp.path().join("config.toml"), config_toml)
        .await
        .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let session_key: Vec<u8> = (0..32u8).collect();
    let factory = Arc::new(TestFactory { fixture });

    let state = AppState::new(store, factory, session_key);

    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (tmp, Arc::new(state))
}

async fn start_server(port: u16) -> (tempfile::TempDir, Arc<AppState>) {
    start_server_with_extra_config(port, "").await
}

async fn start_server_with_extra_config_on_available_port(
    extra_config: &str,
) -> (tempfile::TempDir, Arc<AppState>, u16) {
    start_server_with_fixture_and_extra_config_on_available_port(make_fixture(), extra_config).await
}

async fn start_server_with_fixture_and_extra_config_on_available_port(
    fixture: ReplayFixture,
    extra_config: &str,
) -> (tempfile::TempDir, Arc<AppState>, u16) {
    let tmp = tempfile::TempDir::new().unwrap();

    let hash = generate_password_hash("testpass");
    let config_toml = format!(
        r#"
[server]
bind = "127.0.0.1:0"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30

{extra_config}
"#
    );
    tokio::fs::write(tmp.path().join("config.toml"), config_toml)
        .await
        .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let session_key: Vec<u8> = (0..32u8).collect();
    let factory = Arc::new(TestFactory { fixture });

    let state = AppState::new(store, factory, session_key);

    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (tmp, Arc::new(state), port)
}

async fn login_cookie(client: &reqwest::Client, base: &str) -> String {
    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    resp.headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string()
}

async fn create_project_and_thread(
    client: &reqwest::Client,
    base: &str,
    cookie: &str,
) -> (ProjectId, ThreadId) {
    let project_resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", cookie)
        .json(&serde_json::json!({
            "name": "thread-actions",
            "dir": "/tmp/thread-actions",
            "default_model": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(project_resp.status(), 200);
    let project_id: ProjectId = project_resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    let thread_resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", cookie)
        .json(&serde_json::json!({"resume": "th_test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(thread_resp.status(), 200);
    let thread_id: ThreadId = thread_resp.json::<serde_json::Value>().await.unwrap()["thread_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    (project_id, thread_id)
}

#[tokio::test]
async fn thread_archive_unarchive_updates_thread_summary() {
    let (_tmp, _state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, thread_id) = create_project_and_thread(&client, &base, &cookie).await;

    let archived: serde_json::Value = client
        .post(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}/archive"
        ))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"archived": true}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(archived["id"].as_str().unwrap(), thread_id.to_string());
    assert_eq!(archived["archived"].as_bool(), Some(true));

    let listed: serde_json::Value = client
        .get(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["threads"][0]["archived"].as_bool(), Some(true));

    let unarchived: serde_json::Value = client
        .post(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}/archive"
        ))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"archived": false}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(unarchived["archived"].as_bool(), Some(false));
}

#[tokio::test]
async fn thread_rename_updates_thread_summary_and_persistence() {
    let (_tmp, state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, thread_id) = create_project_and_thread(&client, &base, &cookie).await;

    let renamed = client
        .patch(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}/title"
        ))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"title": "  Better   title\nnow  "}))
        .send()
        .await
        .unwrap();
    assert_eq!(renamed.status(), 200);
    let renamed: serde_json::Value = renamed.json().await.unwrap();
    assert_eq!(renamed["id"].as_str().unwrap(), thread_id.to_string());
    assert_eq!(renamed["title"].as_str(), Some("Better title now"));

    let saved = state
        .store
        .load_thread(project_id, thread_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.title, "Better title now");

    let listed: serde_json::Value = client
        .get(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        listed["threads"][0]["title"].as_str(),
        Some("Better title now")
    );

    let empty = client
        .patch(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}/title"
        ))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"title": " \n\t "}))
        .send()
        .await
        .unwrap();
    assert_eq!(empty.status(), 400);
    let saved = state
        .store
        .load_thread(project_id, thread_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.title, "Better title now");
}

#[tokio::test]
async fn thread_delete_removes_native_and_persisted_thread() {
    let (_tmp, state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, thread_id) = create_project_and_thread(&client, &base, &cookie).await;
    assert_eq!(
        state.registry.get_project_for_thread(thread_id).await,
        Some(project_id)
    );

    let resp = client
        .delete(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    assert!(
        state
            .store
            .load_thread(project_id, thread_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(state.registry.get_project_for_thread(thread_id).await, None);
}

#[tokio::test]
async fn thread_archive_and_delete_reject_active_turns() {
    let (_tmp, state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, thread_id) = create_project_and_thread(&client, &base, &cookie).await;
    state.live_buffers.start_turn(thread_id).await;

    let archive = client
        .post(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}/archive"
        ))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"archived": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(archive.status(), 409);

    let delete = client
        .delete(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(delete.status(), 409);
}

#[tokio::test]
async fn subscribe_unknown_thread_returns_structured_error() {
    let port = 18791;
    let (_tmp, _state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    let tid = ThreadId::new();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe { thread_id: tid })
            .unwrap()
            .into(),
    ))
    .await
    .unwrap();

    let msg = tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let tokio_tungstenite::tungstenite::Message::Text(text) = msg else {
        panic!("expected text WS frame");
    };
    let server_msg: ServerMessage = serde_json::from_str(&text).unwrap();
    match server_msg {
        ServerMessage::Error { error } => {
            assert_eq!(error.code, "thread_not_found");
            assert_eq!(error.severity, ErrorSeverity::Error);
            assert_eq!(error.thread_id, Some(tid));
            assert_eq!(error.action.as_deref(), Some("subscribe"));
            assert!(!error.message.is_empty());
        }
        other => panic!("expected structured error, got {other:?}"),
    }
}

#[tokio::test]
async fn websocket_accepts_ticket_without_cookie_header() {
    let port = 18793;
    let (_tmp, _state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let ticket_resp: serde_json::Value = client
        .get(format!("{base}/api/ws-ticket"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ticket = ticket_resp["ticket"].as_str().unwrap();

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws?ticket={ticket}"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect with ticket");

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Ping).unwrap().into(),
    ))
    .await
    .unwrap();

    let msg = tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let tokio_tungstenite::tungstenite::Message::Text(text) = msg else {
        panic!("expected text WS frame");
    };
    let server_msg: ServerMessage = serde_json::from_str(&text).unwrap();
    assert!(matches!(server_msg, ServerMessage::Pong));
}

#[tokio::test]
async fn websocket_serializes_harness_error_events() {
    let port = 18794;
    let (_tmp, state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "test-project",
            "dir": "/tmp/test",
            "default_model": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let project_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"resume": "th_test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let tid: ThreadId = resp.json::<serde_json::Value>().await.unwrap()["thread_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe { thread_id: tid })
            .unwrap()
            .into(),
    ))
    .await
    .unwrap();

    let msg = tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        serde_json::from_str::<ServerMessage>(msg.to_text().unwrap()).unwrap(),
        ServerMessage::ThreadState(_)
    ));

    state
        .hub
        .broadcast_event(
            tid,
            AgentEvent::Error {
                thread: tid,
                turn: None,
                error: HarnessError::Protocol("bad frame".into()),
            },
        )
        .await;

    // Skip the snapshot messages sent on subscribe and wait for the broadcast Error event.
    loop {
        let msg = tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match serde_json::from_str::<ServerMessage>(msg.to_text().unwrap()).unwrap() {
            ServerMessage::Event { agent_event, .. } => match agent_event {
                WireAgentEvent::Error { error, .. } => {
                    assert_eq!(error.code, "harness_protocol_error");
                    assert_eq!(error.message, "protocol error: bad frame");
                    break;
                }
                other => panic!("expected error event, got {other:?}"),
            },
            ServerMessage::HistoryPage { .. }
            | ServerMessage::LiveTurnSnapshot(_)
            | ServerMessage::RunningCommands { .. } => continue,
            other => panic!("expected event, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn subscribe_reopens_persisted_thread() {
    let port = 18792;
    let (_tmp, state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let model = ModelRef {
        provider: "openai".into(),
        model: "gpt-5.5".into(),
        reasoning_effort: None,
    };

    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "test-project",
            "dir": "/tmp/test",
            "default_model": model,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let project_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let pid: ProjectId = project_id.parse().unwrap();

    let tid = ThreadId::new();
    let now = chrono::Utc::now();
    state
        .store
        .save_thread(
            pid,
            &giskard_persist::store::ThreadFile {
                version: 1,
                id: tid,
                project_id: pid,
                title: "Saved thread".into(),
                harness_thread_id: "th_test".into(),
                mode: Mode::Build,
                current_model: model.clone(),
                context_window: 128_000,
                approval_policy: ApprovalPolicy::Ask,
                model_efforts: Default::default(),
                tokens: Default::default(),
                created_at: now,
                updated_at: now,
                archived: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(state.registry.get_project_for_thread(tid).await, None);

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe { thread_id: tid })
            .unwrap()
            .into(),
    ))
    .await
    .unwrap();

    let mut got_thread_state = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(2), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                let server_msg: ServerMessage = serde_json::from_str(&t).unwrap();
                if let ServerMessage::ThreadState(state) = server_msg {
                    assert_eq!(state.thread_id, tid);
                    got_thread_state = true;
                    break;
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }

    assert!(got_thread_state, "subscribe should return ThreadState");
    assert_eq!(state.registry.get_project_for_thread(tid).await, Some(pid));
}

#[tokio::test]
async fn persisted_thread_can_be_reopened_before_ws_send() {
    let port = 18790;
    let (_tmp, state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let model = ModelRef {
        provider: "openai".into(),
        model: "gpt-5.5".into(),
        reasoning_effort: None,
    };

    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "test-project",
            "dir": "/tmp/test",
            "default_model": model,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let project_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let pid: ProjectId = project_id.parse().unwrap();

    let tid = ThreadId::new();
    let now = chrono::Utc::now();
    state
        .store
        .save_thread(
            pid,
            &giskard_persist::store::ThreadFile {
                version: 1,
                id: tid,
                project_id: pid,
                title: "Saved thread".into(),
                harness_thread_id: "th_test".into(),
                mode: Mode::Build,
                current_model: model.clone(),
                context_window: 128_000,
                approval_policy: ApprovalPolicy::Ask,
                model_efforts: Default::default(),
                tokens: Default::default(),
                created_at: now,
                updated_at: now,
                archived: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(state.registry.get_project_for_thread(tid).await, None);

    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"thread_id": tid, "resume": null}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["thread_id"].as_str().unwrap(), tid.to_string());
    assert_eq!(state.registry.get_project_for_thread(tid).await, Some(pid));

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe { thread_id: tid })
            .unwrap()
            .into(),
    ))
    .await
    .unwrap();

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id: tid,
            text: "Hello".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let mut saw_completed = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                let server_msg: ServerMessage = serde_json::from_str(&t).unwrap();
                if let ServerMessage::Event { agent_event, .. } = server_msg {
                    if matches!(agent_event, WireAgentEvent::TurnCompleted { .. }) {
                        saw_completed = true;
                        break;
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }

    assert!(saw_completed, "reopened persisted thread should run a turn");
}

#[tokio::test]
async fn replayed_persisted_turn_events_are_not_duplicated() {
    let tid = ThreadId::new();
    let old_turn = TurnId::new();
    let new_turn = TurnId::new();
    let fixture = duplicate_history_fixture(tid, old_turn, new_turn);
    let (_tmp, state, port) =
        start_server_with_fixture_and_extra_config_on_available_port(fixture, "").await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let model = ModelRef {
        provider: "openai".into(),
        model: "gpt-5.5".into(),
        reasoning_effort: None,
    };

    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "test-project",
            "dir": "/tmp/test",
            "default_model": model,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let project_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let pid: ProjectId = project_id.parse().unwrap();

    let now = chrono::Utc::now();
    state
        .store
        .save_thread(
            pid,
            &giskard_persist::store::ThreadFile {
                version: 1,
                id: tid,
                project_id: pid,
                title: "Saved thread".into(),
                harness_thread_id: "th_dupe".into(),
                mode: Mode::Build,
                current_model: model.clone(),
                context_window: 128_000,
                approval_policy: ApprovalPolicy::Ask,
                model_efforts: Default::default(),
                tokens: Default::default(),
                created_at: now,
                updated_at: now,
                archived: false,
            },
        )
        .await
        .unwrap();
    // The persisted turn lives in the authoritative JSONL history (H1), not the metadata file.
    state
        .store
        .append_turn(
            pid,
            tid,
            &giskard_core::turn::Turn {
                id: old_turn,
                user_input: giskard_core::user_input::UserInput::text("old input"),
                items: vec![
                    Item {
                        id: ItemId::new(),
                        harness_item_id: "old_user".into(),
                        payload: ItemPayload::UserMessage {
                            text: "old input".into(),
                        },
                        created_at: now,
                    },
                    Item {
                        id: ItemId::new(),
                        harness_item_id: "old_agent".into(),
                        payload: ItemPayload::AgentMessage {
                            text: "old answer".into(),
                        },
                        created_at: now,
                    },
                ],
                model: model.clone(),
                mode: Mode::Build,
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
                usage: TokenUsage::new(10, 10),
                diffs: vec![],
                started_at: now,
                completed_at: Some(now),
            },
        )
        .await
        .unwrap();

    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"thread_id": tid, "resume": null}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe { thread_id: tid })
            .unwrap()
            .into(),
    ))
    .await
    .unwrap();

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id: tid,
            text: "new input".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let mut seen_old = false;
    let mut seen_new = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline && !seen_new {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                let server_msg: ServerMessage = serde_json::from_str(&t).unwrap();
                if let ServerMessage::Event { agent_event, .. } = server_msg {
                    match agent_event {
                        WireAgentEvent::ItemCompleted { item, .. } => match item.payload {
                            giskard_proto::WireItemPayload::AgentMessage { text }
                            | giskard_proto::WireItemPayload::UserMessage { text } => {
                                if text.starts_with("old ") {
                                    seen_old = true;
                                }
                                if text == "new answer" {
                                    seen_new = true;
                                }
                            }
                            _ => {}
                        },
                        WireAgentEvent::TurnCompleted { turn, .. } if turn == new_turn => {
                            seen_new = true;
                        }
                        _ => {}
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }

    assert!(
        !seen_old,
        "persisted replay items should not be rebroadcast"
    );
    assert!(seen_new, "new turn should still be streamed");

    let saved = state.store.load_all_turns(pid, tid).await.unwrap();
    assert_eq!(saved.len(), 2);
    assert_eq!(saved[0].id, old_turn);
    assert_eq!(saved[1].id, new_turn);
    let old_item_count = saved
        .iter()
        .flat_map(|turn| &turn.items)
        .filter(|item| item.harness_item_id.starts_with("old_"))
        .count();
    assert_eq!(old_item_count, 2);
}

/// A turn that fails without producing agent output must still be persisted as a `Failed` turn
/// carrying the user's input and the real error message, so history explains why the message got
/// no response (rather than the error only flashing by as a transient toast).
#[tokio::test]
async fn failed_turn_is_persisted_with_error_message() {
    let tid = ThreadId::new();
    let turn = TurnId::new();
    let fixture = failed_turn_fixture(tid, turn);
    let (_tmp, state, port) =
        start_server_with_fixture_and_extra_config_on_available_port(fixture, "").await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "test-project",
            "dir": "/tmp/test",
            "default_model": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let project_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let pid: ProjectId = project_id.parse().unwrap();

    // Open the thread (resume triggers the replay fixture).
    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"resume": "th_fail"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let thread_id = resp.json::<serde_json::Value>().await.unwrap()["thread_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(thread_id, tid.to_string());

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe { thread_id: tid })
            .unwrap()
            .into(),
    ))
    .await
    .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id: tid,
            text: "please summarize the repo".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    // Drive the WS until the failed turn completes, asserting the error surfaces live too.
    let mut saw_error = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                if let ServerMessage::Event { agent_event, .. } =
                    serde_json::from_str::<ServerMessage>(&t).unwrap()
                {
                    match agent_event {
                        WireAgentEvent::Error { error, .. } => {
                            assert!(error.message.contains("usageLimitExceeded"));
                            saw_error = true;
                        }
                        WireAgentEvent::TurnCompleted { .. } => break,
                        _ => {}
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }
    assert!(saw_error, "the error event should reach the client live");

    // The failed attempt is persisted: one turn, Failed, with the real message and the user input.
    let saved = state.store.load_all_turns(pid, tid).await.unwrap();
    assert_eq!(saved.len(), 1, "the failed turn should be persisted once");
    let failed = &saved[0];
    assert_eq!(failed.status.kind, TurnStatusKind::Failed);
    assert_eq!(
        failed.status.message.as_deref(),
        Some("usageLimitExceeded: Quota exceeded. Check your plan and billing details.")
    );
    assert_eq!(
        failed.user_input.as_text(),
        Some("please summarize the repo")
    );
    assert!(
        failed.items.is_empty(),
        "a turn that failed before output has no items"
    );
}

/// A non-fatal advisory reaches the client as a `Notice` event (not an `Error`) and the turn still
/// completes normally.
#[tokio::test]
async fn notice_event_is_delivered_to_client() {
    let tid = ThreadId::new();
    let turn = TurnId::new();
    let fixture = notice_fixture(tid, turn);
    let (_tmp, _state, port) =
        start_server_with_fixture_and_extra_config_on_available_port(fixture, "").await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "p",
            "dir": "/tmp/test",
            "default_model": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
        }))
        .send()
        .await
        .unwrap();
    let project_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"resume": "th_notice"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe { thread_id: tid })
            .unwrap()
            .into(),
    ))
    .await
    .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id: tid,
            text: "hi".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let mut saw_notice = false;
    let mut saw_error = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                if let ServerMessage::Event { agent_event, .. } =
                    serde_json::from_str::<ServerMessage>(&t).unwrap()
                {
                    match agent_event {
                        WireAgentEvent::Notice { message, .. } => {
                            assert!(message.contains("Model metadata"));
                            saw_notice = true;
                        }
                        WireAgentEvent::Error { .. } => saw_error = true,
                        WireAgentEvent::TurnCompleted { .. } => break,
                        _ => {}
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }
    assert!(saw_notice, "a notice event should reach the client");
    assert!(!saw_error, "a warning must not surface as an error");
}

#[tokio::test]
async fn open_thread_normalizes_stale_provider_from_configured_model() {
    let extra_config = r#"
[[providers]]
id = "proxy"
name = "proxy"
wire_api = "responses"

  [[providers.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = true
"#;
    let (_tmp, state, port) = start_server_with_extra_config_on_available_port(extra_config).await;
    let base = format!("http://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let stale_model = ModelRef {
        provider: "openai".into(),
        model: "gpt-5.5".into(),
        reasoning_effort: None,
    };

    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "test-project",
            "dir": "/tmp/test",
            "default_model": stale_model,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let project_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let pid: ProjectId = project_id.parse().unwrap();

    let saved_project = state.store.load_project(pid).await.unwrap().unwrap();
    assert_eq!(saved_project.default_model.provider, "proxy");

    let tid = ThreadId::new();
    let now = chrono::Utc::now();
    state
        .store
        .save_thread(
            pid,
            &giskard_persist::store::ThreadFile {
                version: 1,
                id: tid,
                project_id: pid,
                title: "Saved thread".into(),
                harness_thread_id: "th_test".into(),
                mode: Mode::Build,
                current_model: ModelRef {
                    provider: "openai".into(),
                    model: "gpt-5.5".into(),
                    reasoning_effort: None,
                },
                context_window: 128_000,
                approval_policy: ApprovalPolicy::Ask,
                model_efforts: Default::default(),
                tokens: Default::default(),
                created_at: now,
                updated_at: now,
                archived: false,
            },
        )
        .await
        .unwrap();

    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"thread_id": tid, "resume": null}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let saved_thread = state.store.load_thread(pid, tid).await.unwrap().unwrap();
    assert_eq!(saved_thread.current_model.provider, "proxy");
    assert_eq!(saved_thread.context_window, 262_144);
}

#[tokio::test]
async fn login_project_thread_message() {
    let port = 18787;
    let (_tmp, _state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    // 1. Login
    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Extract session cookie before consuming the body
    let cookie_header = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let cookie_val = cookie_header;

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);

    // 2. Create project
    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie_val)
        .json(&serde_json::json!({
            "name": "test-project",
            "dir": "/tmp/test",
            "default_model": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let project_resp: serde_json::Value = resp.json().await.unwrap();
    let project_id = project_resp["id"].as_str().unwrap().to_string();

    // 3. List projects
    let resp = client
        .get(format!("{base}/api/projects"))
        .header("cookie", &cookie_val)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let list: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(list["projects"].as_array().unwrap().len(), 1);

    // 4. Open thread (with resume to trigger replay fixture)
    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie_val)
        .json(&serde_json::json!({"resume": "th_test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let thread_resp: serde_json::Value = resp.json().await.unwrap();
    let thread_id = thread_resp["thread_id"].as_str().unwrap().to_string();

    // 5. WebSocket: subscribe + send input + receive events
    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie_val)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    // Subscribe
    let subscribe = serde_json::to_string(&ClientMessage::Subscribe {
        thread_id: thread_id.parse().unwrap(),
    })
    .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        subscribe.into(),
    ))
    .await
    .unwrap();

    // Wait for ThreadState snapshot
    let mut got_thread_state = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while !got_thread_state && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(2), ws.next()).await {
            Ok(Some(Ok(msg))) => {
                if let tokio_tungstenite::tungstenite::Message::Text(t) = msg {
                    let server_msg: ServerMessage = serde_json::from_str(&t).unwrap();
                    if matches!(server_msg, ServerMessage::ThreadState(_)) {
                        got_thread_state = true;
                    }
                }
            }
            _ => break,
        }
    }
    assert!(got_thread_state, "should receive ThreadState");

    // Send input
    let send_input = serde_json::to_string(&ClientMessage::SendInput {
        thread_id: thread_id.parse().unwrap(),
        text: "Hello".into(),
    })
    .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        send_input.into(),
    ))
    .await
    .unwrap();

    // Collect events until TurnCompleted
    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(msg))) => {
                if let tokio_tungstenite::tungstenite::Message::Text(t) = msg {
                    let server_msg: ServerMessage = serde_json::from_str(&t).unwrap();
                    if let ServerMessage::Event { agent_event, .. } = server_msg {
                        let is_done = matches!(agent_event, WireAgentEvent::TurnCompleted { .. });
                        events.push(agent_event);
                        if is_done {
                            break;
                        }
                    }
                }
            }
            _ => break,
        }
    }

    assert!(!events.is_empty(), "should receive at least one event");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WireAgentEvent::TurnCompleted { .. })),
        "should receive TurnCompleted"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WireAgentEvent::ItemDelta { .. })),
        "should receive ItemDelta"
    );
}

#[tokio::test]
async fn auth_rejected_without_cookie() {
    let port = 18788;
    let (_tmp, _state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");

    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/api/projects"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn login_rejected_wrong_password() {
    let port = 18789;
    let (_tmp, _state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "wrongpass"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], false);
}

/// The directory picker's "New folder" endpoint creates a directory under the given parent and
/// rejects path-segment escapes (`..`, separators).
#[tokio::test]
async fn browse_mkdir_creates_directory_and_rejects_escapes() {
    let (_tmp, _state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let parent = tempfile::TempDir::new().unwrap();
    let parent_path = parent.path().to_string_lossy().to_string();

    // Happy path: a new folder is created and its path returned.
    let resp = client
        .post(format!("{base}/api/browse/mkdir"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"parent": parent_path, "name": "my-project"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let created = resp.json::<serde_json::Value>().await.unwrap()["path"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(created.ends_with("my-project"));
    assert!(parent.path().join("my-project").is_dir());

    // Escape attempts are rejected without touching the filesystem.
    for bad in ["../evil", "a/b", "..", "."] {
        let resp = client
            .post(format!("{base}/api/browse/mkdir"))
            .header("cookie", &cookie)
            .json(&serde_json::json!({"parent": parent_path, "name": bad}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400, "name {bad:?} should be rejected");
    }
    assert!(!parent.path().parent().unwrap().join("evil").exists());
}
