//! Per-project model list integration test: `GET /api/projects/{id}/models` composes the configured
//! models, each `model_listing` provider's `/v1/models` discovery, and the project harness's catalog
//! (names + reasoning efforts) with the §8.3 precedence — config-declared models keep their
//! configured metadata; discovery-only models pick up the harness catalog's names and efforts.

use std::sync::Arc;

use axum::{Router, response::Json as AxumJson, routing::get};
use futures::SinkExt;
use giskard_core::ids::ProjectId;
use giskard_core::model::{Effort, ModelDescriptor, ModelRef};
use giskard_harness::AgentHarness;
use giskard_harness_replay::ReplayHarness;
use giskard_persist::store::ProjectConfig;
use giskard_proto::ClientMessage;
use giskard_server::{AppState, HarnessFactory, build_app};

/// A factory whose harness advertises a fixed model catalog (standing in for Codex `model/list`).
struct CatalogFactory {
    models: Vec<ModelDescriptor>,
}

#[async_trait::async_trait]
impl HarnessFactory for CatalogFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn AgentHarness>, giskard_core::HarnessError> {
        Ok(Arc::new(
            ReplayHarness::new().with_models(self.models.clone()),
        ))
    }
}

/// A factory whose harness advertises `model_listing` but fails every catalog query.
struct FailingCatalogFactory;

#[async_trait::async_trait]
impl HarnessFactory for FailingCatalogFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn AgentHarness>, giskard_core::HarnessError> {
        Ok(Arc::new(
            ReplayHarness::new().with_failing_models("model/list boom"),
        ))
    }
}

fn catalog_model(model: &str, name: &str, efforts: &[&str]) -> ModelDescriptor {
    ModelDescriptor {
        provider: String::new(), // Codex `model/list` is provider-agnostic.
        model: model.into(),
        context_window: ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW,
        supports_reasoning_effort: !efforts.is_empty(),
        reasoning_efforts: efforts.iter().map(|e| (*e).to_string()).collect(),
        display_name: Some(name.into()),
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

async fn connect_ws(
    base: &str,
    cookie: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    use tokio_tungstenite::tungstenite::http::Request;

    let ws_base = base.replacen("http://", "ws://", 1);
    let host = base.trim_start_matches("http://");
    let request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", host)
        .header("cookie", cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    tokio_tungstenite::connect_async(request).await.unwrap().0
}

/// Start a mock discovery provider + a server with the given harness factory (config: `openai`
/// declares `gpt-5.5`; a `model_listing` `mock` provider discovers `glm-5.2`), log in, and create a
/// project. Returns the request base, an authenticated client + cookie, the project id, and the
/// TempDir (kept alive by the caller).
async fn spawn_project(
    factory: Arc<dyn HarnessFactory>,
) -> (
    String,
    reqwest::Client,
    String,
    ProjectId,
    tempfile::TempDir,
    Arc<giskard_persist::PersistStore>,
) {
    let mock = Router::new().route(
        "/models",
        get(|| async { AxumJson(serde_json::json!({ "data": [ { "id": "glm-5.2" } ] })) }),
    );
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(mock_listener, mock).await.unwrap() });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

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
id = "openai"
name = "OpenAI"
wire_api = "responses"
  [[providers.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = false

[[providers]]
id = "mock"
name = "Mock"
base_url = "http://{mock_addr}"
wire_api = "responses"
model_listing = true
"#
        ),
    )
    .await
    .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(store.clone(), factory, (0..32u8).collect());
    let app = build_app(state);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let base = format!("http://127.0.0.1:{port}");
    let (client, cookie) = login(&base).await;

    let project_id: ProjectId = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "proj",
            "dir": "/tmp/giskard-project-models-test",
            "default_model": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
        }))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    (base, client, cookie, project_id, tmp, store)
}

#[tokio::test]
async fn project_models_compose_discovery_and_harness_catalog() {
    // Harness catalog advertises efforts for BOTH models. Precedence: `gpt-5.5` is declared, so its
    // config name/effort setting must win; `glm-5.2` is discovery-only, so it picks up the catalog.
    let models = vec![
        catalog_model("gpt-5.5", "Catalog GPT (should not win)", &["low", "high"]),
        catalog_model("glm-5.2", "GLM 5.2 Pro", &["medium", "high"]),
    ];
    let (base, client, cookie, project_id, _tmp, _store) =
        spawn_project(Arc::new(CatalogFactory { models })).await;

    let body: serde_json::Value = client
        .get(format!("{base}/api/projects/{project_id}/models"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let models = body["models"].as_array().unwrap();
    let find = |model: &str| {
        models
            .iter()
            .find(|m| m["model"] == model)
            .unwrap_or_else(|| panic!("model {model} missing from {models:?}"))
    };

    // Config-declared `gpt-5.5`: config name wins over the catalog, and the declared effort setting
    // is preserved — the catalog does NOT override a declared model's efforts.
    let gpt = find("gpt-5.5");
    assert_eq!(gpt["display_name"], "GPT-5.5");
    assert_eq!(gpt["supports_reasoning_effort"], false);
    assert!(
        gpt.get("reasoning_efforts").is_none(),
        "declared model keeps no efforts: {gpt:?}"
    );

    // Discovery-only `glm-5.2`: merged in from /v1/models, then the catalog supplies its friendly
    // name and its exact reasoning efforts.
    let glm = find("glm-5.2");
    assert_eq!(glm["display_name"], "GLM 5.2 Pro");
    assert_eq!(glm["supports_reasoning_effort"], true);
    assert_eq!(
        glm["reasoning_efforts"],
        serde_json::json!(["medium", "high"])
    );

    // Discovery succeeded, so no warnings (the field is omitted when empty).
    let warnings = body.get("warnings").and_then(|w| w.as_array());
    assert!(
        warnings.is_none_or(|w| w.is_empty()),
        "no discovery warnings expected: {warnings:?}"
    );
}

#[tokio::test]
async fn project_models_degrade_when_harness_catalog_query_fails() {
    // Harness advertises model_listing but every `list_models` call errors. The overlay is
    // best-effort, so the endpoint must still return the config + discovery list — just without the
    // harness's names/efforts — rather than failing the request.
    let (base, client, cookie, project_id, _tmp, _store) =
        spawn_project(Arc::new(FailingCatalogFactory)).await;

    let resp = client
        .get(format!("{base}/api/projects/{project_id}/models"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "harness failure must not fail the request"
    );
    let body: serde_json::Value = resp.json().await.unwrap();

    let models = body["models"].as_array().unwrap();
    let find = |model: &str| {
        models
            .iter()
            .find(|m| m["model"] == model)
            .unwrap_or_else(|| panic!("model {model} missing from {models:?}"))
    };

    // Config metadata is untouched by the harness failure.
    let gpt = find("gpt-5.5");
    assert_eq!(gpt["display_name"], "GPT-5.5");

    // The discovered model is still present, but with no harness overlay: it falls back to a
    // conservative descriptor (no friendly name, no efforts).
    let glm = find("glm-5.2");
    assert!(
        glm.get("display_name").is_none(),
        "no harness name applied on failure: {glm:?}"
    );
    assert_eq!(glm["supports_reasoning_effort"], false);
    assert!(
        glm.get("reasoning_efforts").is_none(),
        "no harness efforts applied on failure: {glm:?}"
    );

    let warnings = body["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 1, "harness failure is surfaced: {body}");
    assert_eq!(warnings[0]["source"], "harness:codex");
    assert!(warnings[0]["message"].as_str().unwrap().contains("boom"));
}

#[tokio::test]
async fn catalog_effort_survives_new_thread_creation() {
    let models = vec![catalog_model("glm-5.2", "GLM 5.2 Pro", &["medium", "high"])];
    let (base, client, cookie, project_id, _tmp, store) =
        spawn_project(Arc::new(CatalogFactory { models })).await;

    // Populate the same project catalog that drives the browser picker.
    let catalog = client
        .get(format!("{base}/api/projects/{project_id}/models"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(catalog.status(), reqwest::StatusCode::OK);

    let response = client
        .post(format!("{base}/api/projects/{project_id}/threads/start"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "text": "Use the selected effort",
            "model_ref": {
                "provider": "mock",
                "model": "glm-5.2",
                "reasoning_effort": "high"
            },
            "mode": "build",
            "approval_policy": "ask"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    let thread_id = body["thread_id"].as_str().unwrap().parse().unwrap();

    let thread = store
        .load_thread(project_id, thread_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(thread.current_model.provider, "mock");
    assert_eq!(thread.current_model.model, "glm-5.2");
    assert_eq!(
        thread
            .current_model
            .reasoning_effort
            .as_ref()
            .map(|e| e.as_str()),
        Some("high")
    );

    let mut ws = connect_ws(&base, &cookie).await;
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SelectModel {
            thread_id,
            model_ref: ModelRef {
                provider: "mock".into(),
                model: "glm-5.2".into(),
                reasoning_effort: Some(Effort::new("medium")),
            },
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let selected = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let thread = store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .unwrap();
            if thread
                .current_model
                .reasoning_effort
                .as_ref()
                .map(|e| e.as_str())
                == Some("medium")
            {
                break thread;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("catalog effort selection should be persisted");
    assert_eq!(
        selected
            .current_model
            .reasoning_effort
            .as_ref()
            .map(|e| e.as_str()),
        Some("medium")
    );
}
