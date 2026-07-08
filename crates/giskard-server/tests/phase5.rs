//! Phase 5 integration tests: token ledgers/dashboard (§10.2) and dynamic model refresh (§8.3).

use std::sync::Arc;

use axum::{Router, response::Json as AxumJson, routing::get};
use chrono::Utc;
use futures_util::SinkExt;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemKind, ItemPayload, ItemStart};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{ApprovalPolicy, TurnStatus, TurnStatusKind};
use giskard_harness::AgentHarness;
use giskard_harness_replay::{ReplayFixture, ReplayHarness};
use giskard_persist::store::ProjectConfig;
use giskard_proto::ClientMessage;
use giskard_server::{AppState, HarnessFactory, build_app};

struct DiffFactory {
    fixture: ReplayFixture,
}

#[async_trait::async_trait]
impl HarnessFactory for DiffFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn AgentHarness>, giskard_core::HarnessError> {
        Ok(Arc::new(ReplayHarness::from_fixture(self.fixture.clone())))
    }
}

fn make_fixture() -> ReplayFixture {
    let thread = ThreadId::new();
    let turn = TurnId::new();
    let item = ItemId::new();
    let now = Utc::now();
    ReplayFixture::from_events(vec![
        AgentEvent::ThreadOpened {
            thread,
            harness_thread_id: "th_tok".into(),
        },
        AgentEvent::TurnStarted { thread, turn },
        AgentEvent::ItemStarted {
            thread,
            turn,
            item: ItemStart {
                id: item,
                harness_item_id: "it_1".into(),
                kind: ItemKind::AgentMessage,
                command: None,
            },
        },
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: item,
                harness_item_id: "it_1".into(),
                payload: ItemPayload::AgentMessage {
                    text: "done".into(),
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

fn make_turn(text: &str) -> giskard_core::turn::Turn {
    let now = Utc::now();
    giskard_core::turn::Turn {
        id: TurnId::new(),
        user_input: giskard_core::user_input::UserInput::text(text),
        items: vec![Item {
            id: ItemId::new(),
            harness_item_id: String::new(),
            payload: ItemPayload::AgentMessage {
                text: text.to_string(),
            },
            created_at: now,
        }],
        model: giskard_core::model::ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        },
        mode: giskard_core::turn::Mode::Build,
        status: TurnStatus {
            kind: TurnStatusKind::Completed,
            message: None,
        },
        usage: TokenUsage::new(1, 1),
        diffs: vec![],
        started_at: now,
        completed_at: Some(now),
    }
}

fn password_hash(password: &str) -> String {
    use argon2::password_hash::SaltString;
    use argon2::{Argon2, PasswordHasher};
    use rand::rngs::OsRng;
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string()
}

async fn login(base: &str) -> (reqwest::Client, String) {
    let client = reqwest::Client::new();
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
    (client, cookie)
}

#[tokio::test]
async fn token_ledgers_and_dashboard() {
    let port = 19200;
    let tmp = tempfile::TempDir::new().unwrap();
    let hash = password_hash("testpass");
    tokio::fs::write(
        tmp.path().join("config.toml"),
        format!(
            "[server]\nbind=\"127.0.0.1:{port}\"\nsecure_cookies=false\n\n[auth]\npassword_hash=\"{hash}\"\nsession_days=30\n"
        ),
    )
    .await
    .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(
        store,
        Arc::new(DiffFactory {
            fixture: make_fixture(),
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let base = format!("http://127.0.0.1:{port}");
    let (client, cookie) = login(&base).await;

    let proj_dir = tempfile::TempDir::new().unwrap();
    let pid = ProjectId::new();
    state
        .store
        .create_project(
            pid,
            "proj",
            &proj_dir.path().to_string_lossy(),
            giskard_core::model::ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
            ApprovalPolicy::Auto,
        )
        .await
        .unwrap();

    let thread_id: ThreadId = {
        let resp: serde_json::Value = client
            .post(format!("{base}/api/projects/{pid}/threads"))
            .header("cookie", &cookie)
            .json(&serde_json::json!({"resume": "th_tok"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        serde_json::from_value(resp["thread_id"].clone()).unwrap()
    };

    // Drive one turn over WS.
    let ws_req = tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(format!("ws://127.0.0.1:{port}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_req).await.unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id,
            text: "go".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    // Poll the global dashboard until the ledger actor has folded the usage in.
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    loop {
        let global: serde_json::Value = client
            .get(format!("{base}/api/tokens"))
            .header("cookie", &cookie)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if global["total"]["total"].as_u64() == Some(150) {
            // Windows derived from by_day.
            assert_eq!(global["today"]["total"].as_u64(), Some(150));
            assert_eq!(global["this_month"]["total"].as_u64(), Some(150));
            assert_eq!(global["by_day"][&today]["total"].as_u64(), Some(150));
            assert_eq!(
                global["by_model"]["openai"]["gpt-5.5"]["total"].as_u64(),
                Some(150)
            );
            // Cost estimation is off by default.
            assert!(global.get("estimated_cost_eur").is_none());
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("global ledger not updated: {global}");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // The project dashboard reflects the same usage.
    let project: serde_json::Value = client
        .get(format!("{base}/api/projects/{pid}/tokens"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(project["total"]["total"].as_u64(), Some(150));
    assert_eq!(project["total"]["input"].as_u64(), Some(100));
    assert_eq!(project["total"]["output"].as_u64(), Some(50));
}

#[tokio::test]
async fn dynamic_model_refresh_merges_provider_listing() {
    // A local mock provider exposing OpenAI-style GET /models.
    let mock = Router::new().route(
        "/models",
        get(|| async {
            AxumJson(serde_json::json!({
                "data": [ { "id": "dyn-model-1" }, { "id": "gpt-5.5" } ]
            }))
        }),
    );
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(mock_listener, mock).await.unwrap() });

    let port = 19201;
    let tmp = tempfile::TempDir::new().unwrap();
    let hash = password_hash("testpass");
    // Provider points at the mock; model_listing enabled; one static model.
    tokio::fs::write(
        tmp.path().join("config.toml"),
        format!(
            r#"
[server]
bind = "127.0.0.1:{port}"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30

[[providers]]
id = "mock"
name = "Mock"
base_url = "http://{mock_addr}"
wire_api = "responses"
model_listing = true
  [[providers.models]]
  id = "static-model"
  context_window = 65536
  supports_reasoning_effort = false
"#
        ),
    )
    .await
    .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(
        store,
        Arc::new(DiffFactory {
            fixture: make_fixture(),
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let base = format!("http://127.0.0.1:{port}");
    let (client, cookie) = login(&base).await;

    let refreshed: serde_json::Value = client
        .post(format!("{base}/api/models/refresh"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let models = refreshed["models"].as_array().unwrap();
    let ids: Vec<&str> = models
        .iter()
        .map(|m| m["model"].as_str().unwrap())
        .collect();
    assert!(
        ids.contains(&"static-model"),
        "static entry retained: {ids:?}"
    );
    assert!(
        ids.contains(&"dyn-model-1"),
        "dynamic id merged in: {ids:?}"
    );
    // "gpt-5.5" appeared in both the static list (no) and dynamic; it's added once.
    assert_eq!(
        ids.iter().filter(|id| **id == "gpt-5.5").count(),
        1,
        "no duplicate ids: {ids:?}"
    );
}

/// A provider's `api_key` is sent as `Authorization: Bearer …` on the `/models` discovery request,
/// so endpoints that require auth (e.g. a LiteLLM proxy with a master key) can be listed.
#[tokio::test]
async fn dynamic_model_refresh_sends_api_key() {
    // Mock only returns the model when the correct bearer token is presented.
    let mock = Router::new().route(
        "/models",
        get(|headers: axum::http::HeaderMap| async move {
            let authorized = headers.get("authorization").and_then(|v| v.to_str().ok())
                == Some("Bearer secret-key");
            let data = if authorized {
                serde_json::json!([{ "id": "secured-model" }])
            } else {
                serde_json::json!([])
            };
            AxumJson(serde_json::json!({ "data": data }))
        }),
    );
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(mock_listener, mock).await.unwrap() });

    let port = 19209;
    let tmp = tempfile::TempDir::new().unwrap();
    let hash = password_hash("testpass");
    tokio::fs::write(
        tmp.path().join("config.toml"),
        format!(
            r#"
[server]
bind = "127.0.0.1:{port}"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30

[[providers]]
id = "secured"
name = "Secured"
base_url = "http://{mock_addr}"
wire_api = "responses"
model_listing = true
api_key = "secret-key"
"#
        ),
    )
    .await
    .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(
        store,
        Arc::new(DiffFactory {
            fixture: make_fixture(),
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let base = format!("http://127.0.0.1:{port}");
    let (client, cookie) = login(&base).await;

    let refreshed: serde_json::Value = client
        .post(format!("{base}/api/models/refresh"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let ids: Vec<&str> = refreshed["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["model"].as_str().unwrap())
        .collect();
    assert!(
        ids.contains(&"secured-model"),
        "authorized discovery should list the model (bearer key sent): {ids:?}"
    );
}

/// A discovery failure (here: a 401 because no `api_key` is configured) is reported as a warning in
/// the refresh response instead of silently yielding no models.
#[tokio::test]
async fn dynamic_model_refresh_reports_failure() {
    let mock = Router::new().route(
        "/models",
        get(|| async {
            (
                axum::http::StatusCode::UNAUTHORIZED,
                AxumJson(serde_json::json!({ "error": "unauthorized" })),
            )
        }),
    );
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(mock_listener, mock).await.unwrap() });

    let port = 19210;
    let tmp = tempfile::TempDir::new().unwrap();
    let hash = password_hash("testpass");
    // model_listing enabled but no api_key ⇒ the mock rejects with 401.
    tokio::fs::write(
        tmp.path().join("config.toml"),
        format!(
            r#"
[server]
bind = "127.0.0.1:{port}"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30

[[providers]]
id = "secured"
name = "Secured"
base_url = "http://{mock_addr}"
wire_api = "responses"
model_listing = true
"#
        ),
    )
    .await
    .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(
        store,
        Arc::new(DiffFactory {
            fixture: make_fixture(),
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let base = format!("http://127.0.0.1:{port}");
    let (client, cookie) = login(&base).await;

    let refreshed: serde_json::Value = client
        .post(format!("{base}/api/models/refresh"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let warnings = refreshed["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 1, "one provider failed: {refreshed}");
    assert_eq!(warnings[0]["provider"], "secured");
    assert!(
        warnings[0]["message"].as_str().unwrap().contains("401"),
        "warning names the status: {}",
        warnings[0]["message"]
    );
}

#[tokio::test]
async fn history_pagination_over_websocket() {
    use futures_util::StreamExt;

    let port = 19202;
    let tmp = tempfile::TempDir::new().unwrap();
    let hash = password_hash("testpass");
    // Small page sizes so 5 seeded turns paginate: initial 2, page 2.
    tokio::fs::write(
        tmp.path().join("config.toml"),
        format!(
            "[server]\nbind=\"127.0.0.1:{port}\"\nsecure_cookies=false\n\n[auth]\npassword_hash=\"{hash}\"\nsession_days=30\n\n[history]\ninitial=2\npage=2\n"
        ),
    )
    .await
    .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(
        store,
        Arc::new(DiffFactory {
            fixture: make_fixture(),
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let base = format!("http://127.0.0.1:{port}");
    let (client, cookie) = login(&base).await;

    let proj_dir = tempfile::TempDir::new().unwrap();
    let pid = ProjectId::new();
    state
        .store
        .create_project(
            pid,
            "proj",
            &proj_dir.path().to_string_lossy(),
            giskard_core::model::ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
            ApprovalPolicy::Auto,
        )
        .await
        .unwrap();

    // Open (register) the thread, then seed 5 turns directly into the authoritative history.
    let tid: ThreadId = {
        let resp: serde_json::Value = client
            .post(format!("{base}/api/projects/{pid}/threads"))
            .header("cookie", &cookie)
            .json(&serde_json::json!({"resume": "th_tok"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        serde_json::from_value(resp["thread_id"].clone()).unwrap()
    };
    let mut ids = Vec::new();
    for i in 0..5 {
        let t = make_turn(&format!("turn {i}"));
        ids.push(t.id.to_string());
        state.store.append_turn(pid, tid, &t).await.unwrap();
    }

    // Connect WS + subscribe.
    let ws_req = tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(format!("ws://127.0.0.1:{port}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_req).await.unwrap();

    async fn next_history_page<S>(ws: &mut S) -> serde_json::Value
    where
        S: futures_util::Stream<
                Item = Result<
                    tokio_tungstenite::tungstenite::Message,
                    tokio_tungstenite::tungstenite::Error,
                >,
            > + Unpin,
    {
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            if let Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) =
                tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await
            {
                let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                if v["type"] == "history_page" {
                    return v;
                }
            }
        }
        panic!("no history_page received");
    }

    let subscribe = tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe { thread_id: tid })
            .unwrap()
            .into(),
    );
    ws.send(subscribe).await.unwrap();

    // Initial page = last 2 turns (ids[3], ids[4]), more available.
    let page = next_history_page(&mut ws).await;
    let turns = page["turns"].as_array().unwrap();
    assert_eq!(turns.len(), 2);
    assert_eq!(page["has_more"], true);
    assert_eq!(turns[0]["id"].as_str().unwrap(), ids[3]);
    assert_eq!(turns[1]["id"].as_str().unwrap(), ids[4]);

    // Page older: before ids[3] → ids[1], ids[2], still more.
    let cursor: TurnId = ids[3].parse().unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::LoadHistory {
            thread_id: tid,
            before: Some(cursor),
            limit: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();
    let page = next_history_page(&mut ws).await;
    let turns = page["turns"].as_array().unwrap();
    assert_eq!(turns.len(), 2);
    assert_eq!(page["has_more"], true);
    assert_eq!(turns[0]["id"].as_str().unwrap(), ids[1]);
    assert_eq!(turns[1]["id"].as_str().unwrap(), ids[2]);

    // Final page: before ids[1] → ids[0], no more.
    let cursor: TurnId = ids[1].parse().unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::LoadHistory {
            thread_id: tid,
            before: Some(cursor),
            limit: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();
    let page = next_history_page(&mut ws).await;
    let turns = page["turns"].as_array().unwrap();
    assert_eq!(turns.len(), 1);
    assert_eq!(page["has_more"], false);
    assert_eq!(turns[0]["id"].as_str().unwrap(), ids[0]);
}
