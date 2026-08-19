//! Dynamic model refresh integration tests: merging a provider's `/v1/models` listing over the
//! static config, sending the provider API key on discovery, and reporting discovery failures
//! (spec §8.3).
//!
//! Discovery is a per-project concern: the endpoint and key location come from the project
//! harness's own provider table (§8.2), so every test here goes through
//! `GET /api/projects/{id}/models` rather than the no-project baseline.

use std::sync::Arc;

use axum::{Router, response::Json as AxumJson, routing::get};
use chrono::Utc;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemKind, ItemPayload, ItemStart};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
use giskard_harness::{AgentHarness, HarnessProvider};
use giskard_harness_replay::{ReplayFixture, ReplayHarness};
use giskard_persist::store::ProjectConfig;
use giskard_server::{AppState, HarnessFactory, build_app};

struct DiffFactory {
    fixture: ReplayFixture,
    /// Stands in for Codex's `[model_providers]` table: the endpoint and key location Giskard
    /// resolves discovery against. Empty ⇒ the harness does not advertise provider listing at all,
    /// which is a different thing from advertising an empty table.
    providers: Vec<HarnessProvider>,
    /// The model the harness reports for a thread imported by native id, when a test asserts on it.
    imported_model: Option<giskard_core::model::ModelRef>,
}

#[async_trait::async_trait]
impl HarnessFactory for DiffFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn AgentHarness>, giskard_core::HarnessError> {
        let harness = ReplayHarness::from_fixture(self.fixture.clone());
        let harness = if self.providers.is_empty() {
            harness
        } else {
            harness.with_providers(self.providers.clone())
        };
        let harness = match &self.imported_model {
            Some(model) => harness.with_imported_model(model.clone()),
            None => harness,
        };
        Ok(Arc::new(harness))
    }
}

/// Create a project and return its id, so a test can ask for the per-project model catalog.
async fn create_project(client: &reqwest::Client, base: &str, cookie: &str) -> ProjectId {
    let response = client
        .post(format!("{base}/api/projects"))
        .header("cookie", cookie)
        .json(&serde_json::json!({
            "name": "discovery",
            "dir": "/tmp",
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
                "data": [
                    { "id": "dyn-model-1", "context_window": 258400 },
                    { "id": "gpt-5.5", "context_window": 0, "max_input_tokens": 272000 },
                    { "id": "bad-metadata", "context_window": -1 }
                ]
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

[providers.mock]
model_listing = true
  [[providers.mock.models]]
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
        store.clone(),
        Arc::new(DiffFactory {
            fixture: make_fixture(),
            // The thread below is imported by native id, and this test asserts it carries the
            // *discovered* model's context window — so that is the model the harness reports.
            imported_model: Some(giskard_core::model::ModelRef {
                provider: "mock".into(),
                model: "dyn-model-1".into(),
                reasoning_effort: None,
            }),
            providers: vec![HarnessProvider {
                id: "mock".into(),
                name: Some("Mock".into()),
                base_url: Some(format!("http://{mock_addr}")),
                api_key_env: None,
            }],
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
    let discovery_project = create_project(&client, &base, &cookie).await;

    let refreshed: serde_json::Value = client
        .get(format!("{base}/api/projects/{discovery_project}/models"))
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
    let context_window = |model: &str| {
        models
            .iter()
            .find(|entry| entry["model"] == model)
            .and_then(|entry| entry["context_window"].as_u64())
    };
    assert_eq!(context_window("dyn-model-1"), Some(258_400));
    assert_eq!(context_window("gpt-5.5"), Some(272_000));
    assert_eq!(context_window("bad-metadata"), Some(128_000));
    assert_eq!(refreshed["warnings"].as_array().unwrap().len(), 1);
    assert!(
        refreshed["warnings"][0]["message"]
            .as_str()
            .unwrap()
            .contains("invalid context capacity metadata")
    );

    let project_response = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "dynamic-resume",
            "dir": "/tmp",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(project_response.status(), reqwest::StatusCode::OK);
    let project_id: ProjectId = project_response.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    let resume_response = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"resume": "native-dynamic-model"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resume_response.status(), reqwest::StatusCode::OK);
    let thread_id: ThreadId =
        resume_response.json::<serde_json::Value>().await.unwrap()["thread_id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
    let imported = store
        .load_thread(project_id, thread_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(imported.current_model.provider, "mock");
    assert_eq!(imported.current_model.model, "dyn-model-1");
    assert_eq!(imported.context_window, 258_400);
}

/// The key named by the harness provider's env var is sent as `Authorization: Bearer …` on the
/// `/models` discovery request,
/// so endpoints that require auth (e.g. a LiteLLM proxy with a master key) can be listed.
#[tokio::test]
async fn dynamic_model_refresh_sends_api_key() {
    // The harness names the variable; Giskard reads the key out of the environment, so the secret
    // never has to be restated in config.toml (§8.2). That leaves the environment as the only way
    // to give this test a key — and the test must not put it there itself: `set_var` races every
    // other thread reading the environment, which is why it is `unsafe` in Rust 2024.
    //
    // `.cargo/config.toml` supplies the value instead, so it exists before the process has threads
    // and this test only ever reads it. Running the binary outside cargo therefore needs the
    // variable exported by hand — hence the explicit check rather than a mystifying 401.
    const KEY_ENV: &str = "GISKARD_TEST_DISCOVERY_KEY";
    let key = std::env::var(KEY_ENV).unwrap_or_else(|_| {
        panic!("{KEY_ENV} must be set for this test; cargo supplies it from .cargo/config.toml")
    });
    // Mock only returns the model when the correct bearer token is presented.
    let mock = Router::new().route(
        "/models",
        get(|headers: axum::http::HeaderMap| async move {
            let expected = format!("Bearer {key}");
            let authorized =
                headers.get("authorization").and_then(|v| v.to_str().ok()) == Some(&*expected);
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

[providers.secured]
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
            imported_model: None,
            providers: vec![HarnessProvider {
                id: "secured".into(),
                name: Some("Secured".into()),
                base_url: Some(format!("http://{mock_addr}")),
                api_key_env: Some(KEY_ENV.into()),
            }],
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
    let project = create_project(&client, &base, &cookie).await;

    let refreshed: serde_json::Value = client
        .get(format!("{base}/api/projects/{project}/models"))
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

/// A discovery failure (here: a 401 because the harness names no key for the provider) is reported
/// as a warning in the catalog response instead of silently yielding no models.
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
    // model_listing enabled but the harness reports no key env var ⇒ the mock rejects with 401.
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

[providers.secured]
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
            imported_model: None,
            providers: vec![HarnessProvider {
                id: "secured".into(),
                name: Some("Secured".into()),
                base_url: Some(format!("http://{mock_addr}")),
                api_key_env: None,
            }],
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
    let project = create_project(&client, &base, &cookie).await;

    let refreshed: serde_json::Value = client
        .get(format!("{base}/api/projects/{project}/models"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let warnings = refreshed["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 1, "one provider failed: {refreshed}");
    assert_eq!(warnings[0]["source"], "provider:secured");
    assert!(
        warnings[0]["message"].as_str().unwrap().contains("401"),
        "warning names the status: {}",
        warnings[0]["message"]
    );
}

/// A configured provider id the harness has never heard of is reported as a warning against that
/// provider, while its models stay in the picker (§8.2). Catching the mismatch here is the whole
/// point: the alternative is a provider-side `model_not_found` in the middle of a turn.
#[tokio::test]
async fn unknown_provider_id_is_reported_against_the_harness_table() {
    let port = 19211;
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

[providers.typoed]
  [[providers.typoed.models]]
  id = "some-model"
  context_window = 65536
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
            imported_model: None,
            // The harness knows "openai" — nothing named "typoed".
            providers: vec![HarnessProvider {
                id: "openai".into(),
                name: None,
                base_url: None,
                api_key_env: None,
            }],
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
    let project = create_project(&client, &base, &cookie).await;

    let catalog: serde_json::Value = client
        .get(format!("{base}/api/projects/{project}/models"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let warnings = catalog["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 1, "one unknown provider: {catalog}");
    assert_eq!(warnings[0]["source"], "provider:typoed");
    assert!(
        warnings[0]["message"]
            .as_str()
            .unwrap()
            .contains("not configured in"),
        "warning explains the mismatch: {}",
        warnings[0]["message"]
    );

    let ids: Vec<&str> = catalog["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["model"].as_str().unwrap())
        .collect();
    assert!(
        ids.contains(&"some-model"),
        "flagged, not hidden: the user still sees what they declared: {ids:?}"
    );
}

/// A harness that cannot introspect its own providers disables validation rather than reporting
/// every configured id as unknown: no table is not the same as an empty table.
#[tokio::test]
async fn provider_ids_are_not_validated_without_a_harness_table() {
    let port = 19212;
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

[providers.unverifiable]
  [[providers.unverifiable.models]]
  id = "some-model"
  context_window = 65536
"#
        ),
    )
    .await
    .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(
        store,
        // No `with_providers` ⇒ the replay harness leaves `provider_listing` off.
        Arc::new(DiffFactory {
            fixture: make_fixture(),
            imported_model: None,
            providers: Vec::new(),
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
    let project = create_project(&client, &base, &cookie).await;

    let catalog: serde_json::Value = client
        .get(format!("{base}/api/projects/{project}/models"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    // `warnings` is skipped entirely when empty, so absent and empty both mean "nothing to say".
    assert!(
        catalog["warnings"]
            .as_array()
            .is_none_or(|warnings| warnings.is_empty()),
        "silence when the harness cannot answer: {catalog}"
    );
}

/// A harness that cannot report providers cannot supply a discovery endpoint either, so a provider
/// configured for `model_listing` quietly comes back with only its declared models. Say so instead:
/// a short list with no explanation is the failure mode this warning exists to prevent.
#[tokio::test]
async fn model_listing_without_a_harness_table_explains_why_discovery_did_not_run() {
    let port = 19213;
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

[providers.wants-discovery]
model_listing = true
  [[providers.wants-discovery.models]]
  id = "declared-only"
  context_window = 65536
"#
        ),
    )
    .await
    .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(
        store,
        // No provider table: the replay harness leaves `provider_listing` off.
        Arc::new(DiffFactory {
            fixture: make_fixture(),
            imported_model: None,
            providers: Vec::new(),
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
    let project = create_project(&client, &base, &cookie).await;

    let catalog: serde_json::Value = client
        .get(format!("{base}/api/projects/{project}/models"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let warnings = catalog["warnings"].as_array().unwrap();
    assert_eq!(
        warnings.len(),
        1,
        "one explanation, not one per provider: {catalog}"
    );
    assert_eq!(warnings[0]["source"], "harness:codex");
    assert!(
        warnings[0]["message"]
            .as_str()
            .unwrap()
            .contains("cannot report"),
        "the warning names the missing capability: {}",
        warnings[0]["message"]
    );

    let ids: Vec<&str> = catalog["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["model"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["declared-only"], "the declared list still serves");
}
