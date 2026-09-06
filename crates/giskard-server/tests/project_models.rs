//! Per-project model list integration test: `GET /api/projects/{id}/models` composes the configured
//! models, each `model_listing` provider's `/v1/models` discovery, and the project harness's catalog
//! (names + reasoning efforts) with the §8.3 precedence — config-declared models keep their
//! configured metadata; discovery-only models pick up the harness catalog's names and efforts.

use std::sync::Arc;

use axum::{Router, response::Json as AxumJson, routing::get};
use futures::SinkExt;
use giskard_core::ids::ProjectId;
use giskard_core::model::{Effort, ModelDescriptor, ModelRef};
use giskard_harness::HarnessProvider;
use giskard_harness_replay::ReplayHarness;
use giskard_proto::ClientMessage;
use giskard_server::HarnessFactory;
use giskard_testenv::{TestServer, factory};

/// The provider table these tests' harnesses report, standing in for Codex's `[model_providers]`:
/// `openai` with no endpoint, and `mock` pointing at the discovery stub.
fn harness_providers(mock_addr: &str) -> Vec<HarnessProvider> {
    vec![
        HarnessProvider {
            id: "openai".into(),
            name: Some("OpenAI".into()),
            base_url: None,
            auth: None,
        },
        HarnessProvider {
            id: "mock".into(),
            name: Some("Mock".into()),
            base_url: Some(format!("http://{mock_addr}")),
            auth: None,
        },
    ]
}

fn catalog_model(model: &str, name: &str, efforts: &[&str]) -> ModelDescriptor {
    ModelDescriptor {
        provider: String::new(), // Codex `model/list` is provider-agnostic.
        model: model.into(),
        context_window: ModelDescriptor::CONSERVATIVE_CONTEXT_WINDOW,
        supports_reasoning_effort: !efforts.is_empty(),
        reasoning_efforts: efforts.iter().map(|e| (*e).to_string()).collect(),
        display_name: Some(name.into()),
        is_default: false,
    }
}

struct Fixture {
    server: TestServer,
    project_id: ProjectId,
}

/// Start a mock discovery provider + a server with the given harness factory (config: `openai`
/// declares `gpt-5.5`; a `model_listing` `mock` provider discovers `glm-5.2`), log in, and create a
/// project. Returns the request base, an authenticated client + cookie, the project id, and the
/// TempDir (kept alive by the caller).
async fn spawn_project(make_factory: impl FnOnce(&str) -> Arc<dyn HarnessFactory>) -> Fixture {
    let mock = Router::new().route(
        "/models",
        get(|| async { AxumJson(serde_json::json!({ "data": [ { "id": "glm-5.2" } ] })) }),
    );
    let mock_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(mock_listener, mock).await.unwrap() });
    let server = TestServer::builder(make_factory(&mock_addr.to_string()))
        .config(
            r#"[providers.openai]
  [[providers.openai.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = false

[providers.mock]
model_listing = true
"#,
        )
        .start()
        .await;
    let project_id = server
        .create_project_via_api("proj", "/tmp/giskard-project-models-test")
        .await;
    Fixture { server, project_id }
}

#[tokio::test]
async fn project_models_compose_discovery_and_harness_catalog() {
    // Harness catalog advertises efforts for BOTH models. Precedence: `gpt-5.5` is declared, so its
    // config name/effort setting must win; `glm-5.2` is discovery-only, so it picks up the catalog.
    let models = vec![
        catalog_model("gpt-5.5", "Catalog GPT (should not win)", &["low", "high"]),
        catalog_model("glm-5.2", "GLM 5.2 Pro", &["medium", "high"]),
    ];
    let fixture = spawn_project(|mock_addr| {
        let providers = harness_providers(mock_addr);
        factory::from_fn(move |_, _| {
            Ok(Arc::new(
                ReplayHarness::new()
                    .with_models(models.clone())
                    .with_providers(providers.clone()),
            ))
        })
    })
    .await;
    let base = fixture.server.base.clone();
    let client = fixture.server.client.clone();
    let cookie = fixture.server.cookie.clone();
    let project_id = fixture.project_id;

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
    let fixture = spawn_project(|mock_addr| {
        let providers = harness_providers(mock_addr);
        factory::from_fn(move |_, _| {
            Ok(Arc::new(
                ReplayHarness::new()
                    .with_failing_models("model/list boom")
                    .with_providers(providers.clone()),
            ))
        })
    })
    .await;
    let base = fixture.server.base.clone();
    let client = fixture.server.client.clone();
    let cookie = fixture.server.cookie.clone();
    let project_id = fixture.project_id;

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
    let fixture = spawn_project(|mock_addr| {
        let providers = harness_providers(mock_addr);
        factory::from_fn(move |_, _| {
            Ok(Arc::new(
                ReplayHarness::new()
                    .with_models(models.clone())
                    .with_providers(providers.clone()),
            ))
        })
    })
    .await;
    let base = fixture.server.base.clone();
    let client = fixture.server.client.clone();
    let cookie = fixture.server.cookie.clone();
    let project_id = fixture.project_id;
    let store = fixture.server.store().clone();

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
            "permission_preset": "ask_first"
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
    let current_model = thread.current_model.as_known().unwrap();
    assert_eq!(current_model.provider, "mock");
    assert_eq!(current_model.model, "glm-5.2");
    assert_eq!(
        current_model.reasoning_effort.as_ref().map(|e| e.as_str()),
        Some("high")
    );

    let mut ws = fixture.server.ws().await;
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SelectModel {
            thread_id,
            request_id: "select-model".into(),
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
                .as_known()
                .and_then(|model| model.reasoning_effort.as_ref())
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
            .as_known()
            .unwrap()
            .reasoning_effort
            .as_ref()
            .map(|e| e.as_str()),
        Some("medium")
    );
}

/// The draft's starting model is derived from the project's live catalog, preferring the model the
/// harness marks as its default over the first entry (§8.3). Nothing is stored on the project: the
/// catalog is the only source, so the answer follows provider and harness config instead of a
/// remembered choice that can go stale.
#[tokio::test]
async fn draft_default_model_comes_from_the_live_catalog() {
    // "gpt-5.5" is declared first, so picking it would prove nothing; the harness marks the
    // discovered "glm-5.2" as its default instead.
    let mut default_marked = catalog_model("glm-5.2", "GLM 5.2", &["high"]);
    default_marked.is_default = true;
    let models = vec![catalog_model("gpt-5.5", "GPT-5.5", &[]), default_marked];

    let fixture = spawn_project(|mock_addr| {
        let providers = harness_providers(mock_addr);
        factory::from_fn(move |_, _| {
            Ok(Arc::new(
                ReplayHarness::new()
                    .with_models(models.clone())
                    .with_providers(providers.clone()),
            ))
        })
    })
    .await;
    let base = fixture.server.base.clone();
    let client = fixture.server.client.clone();
    let cookie = fixture.server.cookie.clone();
    let project_id = fixture.project_id;
    let store = fixture.server.store().clone();

    let project: serde_json::Value = client
        .get(format!("{base}/api/projects/{project_id}"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        project.get("default_model").is_none(),
        "a project record carries no model to go stale: {project}"
    );
    assert!(
        !serde_json::to_string(&store.load_project(project_id).await.unwrap().unwrap())
            .unwrap()
            .contains("default_model"),
        "and none is persisted either"
    );

    let catalog: serde_json::Value = client
        .get(format!("{base}/api/projects/{project_id}/models"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let models = catalog["models"].as_array().unwrap();
    let default_entry = models
        .iter()
        .find(|m| m["is_default"] == true)
        .unwrap_or_else(|| panic!("catalog marks a default the browser can pick: {catalog}"));
    assert_eq!(default_entry["model"], "glm-5.2");
    assert_eq!(default_entry["provider"], "mock");
    // Order matters for the fallback: the marked model is not first, so a browser that took
    // `models[0]` would get the wrong one.
    assert_eq!(models[0]["model"], "gpt-5.5");
}

/// With nothing marked default, the catalog exposes no `is_default` at all and the first entry is
/// what a caller falls back to.
#[tokio::test]
async fn unmarked_catalog_exposes_no_default_and_falls_back_to_first() {
    let models = vec![
        catalog_model("gpt-5.5", "GPT-5.5", &[]),
        catalog_model("glm-5.2", "GLM 5.2", &["high"]),
    ];
    let fixture = spawn_project(|mock_addr| {
        let providers = harness_providers(mock_addr);
        factory::from_fn(move |_, _| {
            Ok(Arc::new(
                ReplayHarness::new()
                    .with_models(models.clone())
                    .with_providers(providers.clone()),
            ))
        })
    })
    .await;
    let base = fixture.server.base.clone();
    let client = fixture.server.client.clone();
    let cookie = fixture.server.cookie.clone();
    let project_id = fixture.project_id;

    let catalog: serde_json::Value = client
        .get(format!("{base}/api/projects/{project_id}/models"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let models = catalog["models"].as_array().unwrap();
    assert!(
        !models.iter().any(|m| m["is_default"] == true),
        "no model claims to be the default: {catalog}"
    );
    assert_eq!(models[0]["model"], "gpt-5.5", "first entry is the fallback");
}
