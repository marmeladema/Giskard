//! Dynamic model refresh integration tests: merging a provider's `/v1/models` listing over the
//! static config, sending the provider API key on discovery, and reporting discovery failures
//! (spec §8.3).
//!
//! Discovery is a per-project concern: the endpoint and key location come from the project
//! harness's own provider table (§8.2), so every test here goes through
//! `GET /api/projects/{id}/models` rather than the no-project baseline.

use std::sync::Arc;
use std::time::Duration;

use axum::{Router, response::Json as AxumJson, routing::get};
use chrono::Utc;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemKind, ItemPayload, ItemStart};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
use giskard_harness::{AgentHarness, HarnessProvider, ProviderAuth, ProviderAuthCommand};
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
    /// The version the harness reports, sent to a provider's `/models` as `client_version`.
    client_version: Option<String>,
    /// What the harness's own catalog (`model/list`) reports.
    harness_models: Vec<giskard_core::model::ModelDescriptor>,
}

#[async_trait::async_trait]
impl HarnessFactory for DiffFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
        _bootstrap: giskard_harness::HarnessBootstrap,
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
        let harness = match &self.client_version {
            Some(version) => harness.with_client_version(version.clone()),
            None => harness,
        };
        let harness = if self.harness_models.is_empty() {
            harness
        } else {
            harness.with_models(self.harness_models.clone())
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

    let (listener, port) = ephemeral_listener().await;
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
                auth: None,
            }],
            client_version: None,
            harness_models: Vec::new(),
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state);
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
    assert_eq!(imported.current_model.as_known().unwrap().provider, "mock");
    assert_eq!(
        imported.current_model.as_known().unwrap().model,
        "dyn-model-1"
    );
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

    let (listener, port) = ephemeral_listener().await;
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
            client_version: None,
            harness_models: Vec::new(),
            providers: vec![HarnessProvider {
                id: "secured".into(),
                name: Some("Secured".into()),
                base_url: Some(format!("http://{mock_addr}")),
                auth: Some(ProviderAuth::Env(KEY_ENV.into())),
            }],
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state);
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

    let (listener, port) = ephemeral_listener().await;
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
            client_version: None,
            harness_models: Vec::new(),
            providers: vec![HarnessProvider {
                id: "secured".into(),
                name: Some("Secured".into()),
                base_url: Some(format!("http://{mock_addr}")),
                auth: None,
            }],
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state);
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
    let message = warnings[0]["message"].as_str().unwrap();
    assert!(
        message.contains("401"),
        "warning names the status: {message}"
    );
    // The hint has to match how this provider authenticates. Naming an env var here would send the
    // user looking for a variable the harness never mentioned.
    assert!(
        message.contains("names no key"),
        "warning should say the harness named no key: {message}"
    );
}

/// A configured provider id the harness has never heard of is reported as a warning against that
/// provider, while its models stay in the picker (§8.2). Catching the mismatch here is the whole
/// point: the alternative is a provider-side `model_not_found` in the middle of a turn.
#[tokio::test]
async fn unknown_provider_id_is_reported_against_the_harness_table() {
    let (listener, port) = ephemeral_listener().await;
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
            client_version: None,
            harness_models: Vec::new(),
            // The harness knows "openai" — nothing named "typoed".
            providers: vec![HarnessProvider {
                id: "openai".into(),
                name: None,
                base_url: None,
                auth: None,
            }],
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state);
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
    let (listener, port) = ephemeral_listener().await;
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
            client_version: None,
            harness_models: Vec::new(),
            providers: Vec::new(),
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state);
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
    let (listener, port) = ephemeral_listener().await;
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
            client_version: None,
            harness_models: Vec::new(),
            providers: Vec::new(),
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state);
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

/// A provider whose key comes from `[model_providers.<id>.auth]` — a command Codex runs — is
/// discoverable too: Giskard runs the command and sends its stdout as the bearer token.
///
/// Unlike the `env_key` case this needs nothing from the environment, so the command is the whole
/// contract: whatever it prints (trimmed) is the token.
#[tokio::test]
async fn dynamic_model_refresh_runs_a_provider_auth_command() {
    const TOKEN: &str = "token-from-command";
    let mock = Router::new().route(
        "/models",
        get(|headers: axum::http::HeaderMap| async move {
            let authorized = headers.get("authorization").and_then(|v| v.to_str().ok())
                == Some(&*format!("Bearer {TOKEN}"));
            let data = if authorized {
                serde_json::json!([{ "id": "command-auth-model" }])
            } else {
                serde_json::json!([])
            };
            AxumJson(serde_json::json!({ "data": data }))
        }),
    );
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(mock_listener, mock).await.unwrap() });

    // The trailing newline `echo` adds must not reach the header: the token is trimmed.
    let ids = discover_with_auth(
        mock_addr,
        ProviderAuth::Command(ProviderAuthCommand {
            command: "sh".into(),
            args: vec!["-c".into(), format!("echo {TOKEN}")],
            cwd: None,
            timeout: Duration::from_secs(5),
        }),
    )
    .await
    .0;
    assert!(
        ids.contains(&"command-auth-model".to_string()),
        "the command's stdout should have been sent as the bearer token: {ids:?}"
    );
}

/// A provider auth command that fails is reported as itself. Sending the request unauthenticated
/// would bury the real cause under a 401 that blames the endpoint, so discovery does not try.
#[tokio::test]
async fn a_failing_provider_auth_command_is_reported() {
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = hits.clone();
    let mock = Router::new().route(
        "/models",
        get(move || {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                AxumJson(serde_json::json!({ "data": [{ "id": "never-reached" }] }))
            }
        }),
    );
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(mock_listener, mock).await.unwrap() });

    let (ids, warnings) = discover_with_auth(
        mock_addr,
        ProviderAuth::Command(ProviderAuthCommand {
            command: "sh".into(),
            args: vec!["-c".into(), "echo no-such-vault >&2; exit 3".into()],
            cwd: None,
            timeout: Duration::from_secs(5),
        }),
    )
    .await;

    assert!(
        ids.is_empty(),
        "a provider with no token lists nothing: {ids:?}"
    );
    assert_eq!(
        hits.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "discovery must not fall back to an unauthenticated request"
    );
    let joined = warnings.join(" | ");
    for expected in ["sh", "exited with", "no-such-vault"] {
        assert!(
            joined.contains(expected),
            "the warning should name the command, its status, and its stderr ({expected:?}): \
             {joined}"
        );
    }
}

/// A listener on a port the OS picked, and that port.
///
/// The mock providers here already bind `:0`; the server under test used to take a hard-coded one,
/// which fails whenever anything else on the machine happens to hold it. Bound before the config is
/// written so the real port can go into `[server] bind` — inert for these tests, which serve on
/// this listener directly, but worth keeping honest.
async fn ephemeral_listener() -> (tokio::net::TcpListener, u16) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

/// Stand up a server whose single `model_listing` provider authenticates the given way, and return
/// the discovered model ids plus the catalog warnings.
async fn discover_with_auth(
    mock_addr: std::net::SocketAddr,
    auth: ProviderAuth,
) -> (Vec<String>, Vec<String>) {
    let (listener, port) = ephemeral_listener().await;
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
            client_version: None,
            harness_models: Vec::new(),
            providers: vec![HarnessProvider {
                id: "secured".into(),
                name: Some("Secured".into()),
                base_url: Some(format!("http://{mock_addr}")),
                auth: Some(auth),
            }],
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let base = format!("http://127.0.0.1:{port}");
    let (client, cookie) = login(&base).await;
    let project = create_project(&client, &base, &cookie).await;
    let body: serde_json::Value = client
        .get(format!("{base}/api/projects/{project}/models"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let ids = body["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["model"].as_str().unwrap().to_string())
        .collect();
    let warnings = body["warnings"]
        .as_array()
        .map(|w| {
            w.iter()
                .map(|entry| entry["message"].as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default();
    (ids, warnings)
}

/// End to end: a provider serving the harness catalog needs no `[[providers.<id>.models]]` at all.
/// The context window, display name, and effort list all come from the endpoint — the window in
/// particular is the field no harness reports over its own protocol, which is the whole reason
/// Giskard asks for this shape rather than reading it back from `model/list`.
#[tokio::test]
async fn a_harness_catalog_provider_needs_no_declared_models() {
    let seen_query = Arc::new(std::sync::Mutex::new(None::<String>));
    let recorder = seen_query.clone();
    let mock = Router::new().route(
        "/models",
        get(move |uri: axum::http::Uri| {
            let recorder = recorder.clone();
            async move {
                *recorder.lock().unwrap() = uri.query().map(str::to_string);
                AxumJson(serde_json::json!({
                    "models": [
                        {
                            "slug": "gpt-5.5",
                            "display_name": "GPT-5.5",
                            "context_window": 262144,
                            "supported_reasoning_levels": [
                                { "effort": "low", "description": "fast" },
                                { "effort": "high", "description": "thorough" }
                            ],
                            "visibility": "list"
                        },
                        { "slug": "internal-eval", "visibility": "hide" }
                    ]
                }))
            }
        }),
    );
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(mock_listener, mock).await.unwrap() });

    let (models, warnings) = discover_catalog(mock_addr, None).await;
    assert!(
        warnings.is_empty(),
        "nothing should have gone wrong: {warnings:?}"
    );

    let ids: Vec<&str> = models
        .iter()
        .map(|m| m["model"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        ["gpt-5.5"],
        "a hidden model stays out of the picker: {ids:?}"
    );

    let model = &models[0];
    assert_eq!(model["context_window"], 262_144);
    assert_eq!(model["display_name"], "GPT-5.5");
    assert_eq!(
        model["reasoning_efforts"],
        serde_json::json!(["low", "high"])
    );
    assert_eq!(model["supports_reasoning_effort"], true);

    // The replay harness reports no version, so nothing is claimed on its behalf.
    assert_eq!(
        seen_query.lock().unwrap().clone(),
        None,
        "a harness that does not know its version must not have one invented for it"
    );
}

/// Stand up a server whose single `model_listing` provider declares no models, and return the
/// composed catalog plus warnings.
async fn discover_catalog(
    mock_addr: std::net::SocketAddr,
    client_version: Option<&str>,
) -> (Vec<serde_json::Value>, Vec<String>) {
    discover_catalog_with(
        mock_addr,
        client_version,
        "\n[providers.opencodex]\nmodel_listing = true\n",
    )
    .await
}

/// As above, but the caller supplies the `[providers.*]` section — including none at all.
async fn discover_catalog_with(
    mock_addr: std::net::SocketAddr,
    client_version: Option<&str>,
    providers: &str,
) -> (Vec<serde_json::Value>, Vec<String>) {
    let (listener, port) = ephemeral_listener().await;
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
{providers}"#
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
                id: "opencodex".into(),
                name: Some("OpenCodex".into()),
                base_url: Some(format!("http://{mock_addr}")),
                auth: None,
            }],
            client_version: client_version.map(str::to_string),
            harness_models: Vec::new(),
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let base = format!("http://127.0.0.1:{port}");
    let (client, cookie) = login(&base).await;
    let project = create_project(&client, &base, &cookie).await;
    let body: serde_json::Value = client
        .get(format!("{base}/api/projects/{project}/models"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let models = body["models"].as_array().cloned().unwrap_or_default();
    let warnings = body["warnings"]
        .as_array()
        .map(|w| {
            w.iter()
                .map(|e| e["message"].as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default();
    (models, warnings)
}

/// A harness that knows its own version identifies itself on the discovery request, the way the
/// harness would: a provider decides whether to serve its richer catalog from `client_version`.
#[tokio::test]
async fn a_known_harness_version_is_sent_as_client_version() {
    let seen_query = Arc::new(std::sync::Mutex::new(None::<String>));
    let recorder = seen_query.clone();
    let mock = Router::new().route(
        "/models",
        get(move |uri: axum::http::Uri| {
            let recorder = recorder.clone();
            async move {
                *recorder.lock().unwrap() = uri.query().map(str::to_string);
                AxumJson(serde_json::json!({ "models": [ { "slug": "gpt-5.5" } ] }))
            }
        }),
    );
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(mock_listener, mock).await.unwrap() });

    let (models, warnings) = discover_catalog(mock_addr, Some("0.58.0")).await;
    assert!(
        warnings.is_empty(),
        "nothing should have gone wrong: {warnings:?}"
    );
    assert_eq!(
        models.len(),
        1,
        "the catalog should have been read: {models:?}"
    );
    assert_eq!(
        seen_query.lock().unwrap().clone(),
        Some("client_version=0.58.0".to_string())
    );
}

/// The point of the change: a config that names no providers at all still gets a full picker,
/// because the harness already knows which providers exist. Nobody should have to re-declare in
/// Giskard what they already declared to Codex.
#[tokio::test]
async fn an_empty_config_still_discovers_the_harness_providers() {
    let mock = Router::new().route(
        "/models",
        get(|| async {
            AxumJson(serde_json::json!({
                "models": [
                    { "slug": "gpt-5.5", "display_name": "GPT-5.5", "context_window": 272000 }
                ]
            }))
        }),
    );
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(mock_listener, mock).await.unwrap() });

    // No `[providers]` section whatsoever.
    let (models, warnings) = discover_catalog_with(mock_addr, None, "").await;
    assert!(
        warnings.is_empty(),
        "nothing should have gone wrong: {warnings:?}"
    );
    assert_eq!(
        models.len(),
        1,
        "the provider was discovered unprompted: {models:?}"
    );
    assert_eq!(models[0]["model"], "gpt-5.5");
    assert_eq!(models[0]["context_window"], 272_000);
}

/// The opt-out still works, and is now the only thing config needs to say about listing.
#[tokio::test]
async fn model_listing_false_still_opts_out() {
    let hit = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = hit.clone();
    let mock = Router::new().route(
        "/models",
        get(move || {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { AxumJson(serde_json::json!({ "models": [ { "slug": "nope" } ] })) }
        }),
    );
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(mock_listener, mock).await.unwrap() });

    let (models, _) = discover_catalog_with(
        mock_addr,
        None,
        "\n[providers.opencodex]\nmodel_listing = false\n",
    )
    .await;
    assert!(
        models.is_empty(),
        "opted out, so nothing discovered: {models:?}"
    );
    assert_eq!(
        hit.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "and the endpoint was never even asked"
    );
}

/// Providers are queried concurrently, which is what makes on-by-default affordable: serially,
/// every slow endpoint delayed all the ones behind it. Three providers that each take ~700ms
/// should finish in about 700ms, not ~2.1s.
#[tokio::test]
async fn providers_are_queried_concurrently() {
    const PROVIDERS: usize = 3;
    const DELAY: Duration = Duration::from_millis(700);

    let mut table = Vec::new();
    for i in 0..PROVIDERS {
        let slug = format!("model-{i}");
        let mock = Router::new().route(
            "/models",
            get(move || {
                let slug = slug.clone();
                async move {
                    tokio::time::sleep(DELAY).await;
                    AxumJson(serde_json::json!({ "models": [ { "slug": slug } ] }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });
        table.push(HarnessProvider {
            id: format!("p{i}"),
            name: None,
            base_url: Some(format!("http://{addr}")),
            auth: None,
        });
    }

    let config: giskard_persist::Config = toml::from_str("").unwrap();
    let started = std::time::Instant::now();
    let discovery = giskard_server::models::discover_models(&config, &table, None).await;
    let elapsed = started.elapsed();

    assert_eq!(
        discovery.models.len(),
        PROVIDERS,
        "every provider contributed: {:?}",
        discovery.models
    );
    assert!(
        discovery.warnings.is_empty(),
        "nothing should have failed: {:?}",
        discovery.warnings
    );
    // Generous: serial would be >= 2.1s, so anything under 1.5s proves the requests overlapped
    // without making the test sensitive to a slow machine.
    assert!(
        elapsed < DELAY * 2,
        "expected overlapping requests, took {elapsed:?} for {PROVIDERS} providers"
    );

    // Order still follows the target order rather than whichever endpoint answered first.
    let ids: Vec<&str> = discovery.models.iter().map(|m| m.model.as_str()).collect();
    assert_eq!(ids, ["model-0", "model-1", "model-2"]);
}

/// A stock harness: its own catalog answers, but no provider has an endpoint to discover against
/// and config names nothing. That is the plain ChatGPT-auth Codex setup, and until the catalog was
/// attributed to the provider Codex routes to, it produced an empty picker in silence.
#[tokio::test]
async fn a_stock_harness_catalog_fills_the_picker_on_its_own() {
    let tmp = tempfile::TempDir::new().unwrap();
    let hash = password_hash("testpass");
    let (listener, port) = ephemeral_listener().await;
    tokio::fs::write(
        tmp.path().join("config.toml"),
        format!(
            "\n[server]\nbind = \"127.0.0.1:{port}\"\nsecure_cookies = false\n\n[auth]\npassword_hash = \"{hash}\"\nsession_days = 30\n"
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
            client_version: None,
            harness_models: vec![giskard_core::model::ModelDescriptor {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                context_window: giskard_core::model::ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW,
                supports_reasoning_effort: true,
                reasoning_efforts: vec!["low".into(), "high".into()],
                display_name: Some("GPT-5.5".into()),
                is_default: true,
            }],
            // Like Codex's built-ins: known, but with nothing to query.
            providers: vec![HarnessProvider {
                id: "openai".into(),
                name: None,
                base_url: None,
                auth: None,
            }],
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let base = format!("http://127.0.0.1:{port}");
    let (client, cookie) = login(&base).await;
    let project = create_project(&client, &base, &cookie).await;
    let body: serde_json::Value = client
        .get(format!("{base}/api/projects/{project}/models"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let models = body["models"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        models.len(),
        1,
        "the harness catalog is a source of its own: {models:?}"
    );
    assert_eq!(models[0]["provider"], "openai");
    assert_eq!(models[0]["model"], "gpt-5.5");
    assert_eq!(models[0]["display_name"], "GPT-5.5");
    assert_eq!(models[0]["is_default"], true);
    let warnings = body["warnings"].as_array().cloned().unwrap_or_default();
    assert!(
        warnings.is_empty(),
        "and nothing to complain about: {warnings:?}"
    );
}

/// The backstop: when nothing at all can supply a model, say so instead of serving a blank picker.
#[tokio::test]
async fn an_empty_picker_explains_itself() {
    let tmp = tempfile::TempDir::new().unwrap();
    let hash = password_hash("testpass");
    let (listener, port) = ephemeral_listener().await;
    tokio::fs::write(
        tmp.path().join("config.toml"),
        format!(
            "\n[server]\nbind = \"127.0.0.1:{port}\"\nsecure_cookies = false\n\n[auth]\npassword_hash = \"{hash}\"\nsession_days = 30\n"
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
            client_version: None,
            harness_models: Vec::new(),
            providers: vec![HarnessProvider {
                id: "openai".into(),
                name: None,
                base_url: None,
                auth: None,
            }],
        }),
        (0..32u8).collect(),
    );
    let app = build_app(state);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let base = format!("http://127.0.0.1:{port}");
    let (client, cookie) = login(&base).await;
    let project = create_project(&client, &base, &cookie).await;
    let body: serde_json::Value = client
        .get(format!("{base}/api/projects/{project}/models"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(body["models"].as_array().is_none_or(|m| m.is_empty()));
    let warnings = body["warnings"].as_array().cloned().unwrap_or_default();
    assert!(
        warnings.iter().any(|w| w["message"]
            .as_str()
            .unwrap_or_default()
            .contains("no models are available")),
        "an empty picker must explain itself: {warnings:?}"
    );
}
