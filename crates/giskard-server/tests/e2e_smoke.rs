use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemDelta, ItemKind, ItemPayload, ItemStart};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
use giskard_harness_replay::{ReplayFixture, ReplayHarness};
use giskard_persist::store::ProjectConfig;
use giskard_proto::{ClientMessage, ServerMessage, WireAgentEvent};
use giskard_server::{AppState, HarnessFactory, build_app};

struct TestFactory {
    fixture: ReplayFixture,
}

impl HarnessFactory for TestFactory {
    fn create(
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

async fn start_server(port: u16) -> (tempfile::TempDir, Arc<AppState>) {
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
"#
    );
    tokio::fs::write(tmp.path().join("config.toml"), config_toml)
        .await
        .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let session_key: Vec<u8> = (0..32u8).collect();
    let factory = Arc::new(TestFactory {
        fixture: make_fixture(),
    });

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
            "approval_policy": "auto"
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
