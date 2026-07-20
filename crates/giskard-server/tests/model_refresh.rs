//! Dynamic model refresh integration tests: merging a provider's `/v1/models` listing over the
//! static config, sending the provider API key on discovery, and reporting discovery failures
//! (spec §8.3).

use std::sync::Arc;

use axum::{Router, response::Json as AxumJson, routing::get};
use chrono::Utc;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemKind, ItemPayload, ItemStart};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
use giskard_harness::AgentHarness;
use giskard_harness_replay::{ReplayFixture, ReplayHarness};
use giskard_persist::store::ProjectConfig;
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
                tool: None,
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
