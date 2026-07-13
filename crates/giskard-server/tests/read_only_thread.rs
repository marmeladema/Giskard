//! A thread whose harness can no longer attach — e.g. its provider was removed from config — must
//! still load **read-only**: the persisted history is served and a non-fatal `thread_read_only`
//! warning is surfaced, instead of the whole subscribe failing with a JSON-RPC/harness error.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemPayload};
use giskard_core::model::ModelRef;
use giskard_core::token::{TokenLedger, TokenUsage};
use giskard_core::turn::{ApprovalPolicy, Mode, Turn, TurnStatus, TurnStatusKind};
use giskard_persist::store::{ProjectConfig, ThreadFile};
use giskard_proto::ClientMessage;
use giskard_server::{AppState, HarnessFactory, build_app};

/// Always fails to create a harness — simulating a thread whose provider has been removed from
/// config, so the agent app-server can no longer be started/resumed for it.
struct FailingFactory;

#[async_trait::async_trait]
impl HarnessFactory for FailingFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, giskard_core::HarnessError> {
        Err(giskard_core::HarnessError::Spawn(
            "unknown provider: cloudflare-litellm".into(),
        ))
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

/// A model referencing a provider that no longer exists in config.
fn orphaned_model() -> ModelRef {
    ModelRef {
        provider: "cloudflare-litellm".into(),
        model: "@cf/z-ai/glm-4.7".into(),
        reasoning_effort: None,
    }
}

fn make_turn(text: &str) -> Turn {
    let now = Utc::now();
    Turn {
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
        model: orphaned_model(),
        mode: Mode::Build,
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

fn seeded_thread(project_id: ProjectId, thread_id: ThreadId) -> ThreadFile {
    let now = Utc::now();
    ThreadFile {
        version: 1,
        id: thread_id,
        project_id,
        title: "Orphaned thread".into(),
        harness_thread_id: format!("harness-{thread_id}"),
        mode: Mode::Build,
        current_model: orphaned_model(),
        context_window: 131_072,
        approval_policy: ApprovalPolicy::Ask,
        model_efforts: HashMap::new(),
        tokens: TokenLedger::default(),
        created_at: now,
        updated_at: now,
        archived: false,
    }
}

#[tokio::test]
async fn subscribe_to_thread_with_missing_provider_loads_read_only() {
    let tmp = tempfile::TempDir::new().unwrap();
    let hash = password_hash("testpass");
    // Note: no `[[providers]]` — the thread's provider is intentionally absent from config.
    tokio::fs::write(
        tmp.path().join("config.toml"),
        format!(
            r#"
[server]
bind = "127.0.0.1:0"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30
"#
        ),
    )
    .await
    .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));

    // Seed a project + thread + two turns of history directly in persistence (no harness).
    let pid = ProjectId::new();
    let tid = ThreadId::new();
    let proj_dir = tempfile::TempDir::new().unwrap();
    store
        .create_project(
            pid,
            "proj",
            &proj_dir.path().to_string_lossy(),
            orphaned_model(),
        )
        .await
        .unwrap();
    store
        .save_thread(pid, &seeded_thread(pid, tid))
        .await
        .unwrap();
    for i in 0..2 {
        store
            .append_turn(pid, tid, &make_turn(&format!("turn {i}")))
            .await
            .unwrap();
    }

    let state = AppState::new(store, Arc::new(FailingFactory), (0..32u8).collect());
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let base = format!("http://{addr}");
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
        serde_json::to_string(&ClientMessage::Subscribe { thread_id: tid })
            .unwrap()
            .into(),
    ))
    .await
    .unwrap();

    // Collect messages for a short window; we expect both the read-only warning and a history page.
    let mut history_page: Option<serde_json::Value> = None;
    let mut read_only_warning: Option<serde_json::Value> = None;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline
        && (history_page.is_none() || read_only_warning.is_none())
    {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                // `ServerMessage::Error` flattens `ErrorInfo`, so its fields sit at the top level.
                match v["type"].as_str() {
                    Some("history_page") => history_page = Some(v),
                    Some("error") if v["code"] == "thread_read_only" => read_only_warning = Some(v),
                    _ => {}
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }

    // The persisted history is served despite the harness being unable to attach.
    let page = history_page.expect("read-only thread must still deliver a history page");
    assert_eq!(page["turns"].as_array().unwrap().len(), 2);

    // …and the attach failure is surfaced as a non-fatal warning, not a hard error.
    let warning =
        read_only_warning.expect("read-only subscribe must surface a thread_read_only warning");
    assert_eq!(warning["severity"], "warning");
    let detail = warning["detail"].as_str().unwrap_or_default();
    assert!(
        detail.contains("cloudflare-litellm"),
        "warning detail should explain the attach failure: {detail}"
    );
}
