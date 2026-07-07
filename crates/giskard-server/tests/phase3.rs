//! Phase 3 integration tests (modes, models, approvals, plan dump), replay-driven.

use std::sync::Arc;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemKind, ItemPayload, ItemStart};
use giskard_core::model::ModelRef;
use giskard_core::token::TokenUsage;
use giskard_core::turn::{Mode, TurnStatus, TurnStatusKind};
use giskard_harness::AgentHarness;
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
    ) -> Result<Arc<dyn AgentHarness>, giskard_core::HarnessError> {
        Ok(Arc::new(ReplayHarness::from_fixture(self.fixture.clone())))
    }
}

/// Fixture: an agent plan message + a command-execution approval + turn completion.
fn make_fixture() -> ReplayFixture {
    let thread = ThreadId::new();
    let turn = TurnId::new();
    let item = ItemId::new();
    let now = Utc::now();

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
                id: item,
                harness_item_id: "it_1".into(),
                kind: ItemKind::AgentMessage,
            },
        },
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: item,
                harness_item_id: "it_1".into(),
                payload: ItemPayload::AgentMessage {
                    text: "## Plan\n1. Read auth.rs\n2. Refactor token refresh".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::ApprovalRequested {
            thread,
            turn,
            request: ApprovalRequest {
                id: ApprovalId("ap_1".into()),
                kind: ApprovalKind::CommandExecution {
                    command: "cargo test".into(),
                    cwd: "/tmp".into(),
                },
                reason: None,
                available: vec![ApprovalDecision::Accept, ApprovalDecision::Decline],
            },
        },
        AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::new(1200, 340),
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

[plan]
default_dir = "docs"
filename_template = "plan-{{slug}}-{{ts}}.md"
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

fn ws_text(msg: &ClientMessage) -> tokio_tungstenite::tungstenite::Message {
    tokio_tungstenite::tungstenite::Message::Text(serde_json::to_string(msg).unwrap().into())
}

#[tokio::test]
async fn modes_models_approvals_and_plan_dump() {
    let port = 18899;
    let (_tmp, state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    // A writable project directory (workspace root) so the plan dump can be written.
    let proj_dir = tempfile::TempDir::new().unwrap();
    let proj_dir_path = proj_dir.path().to_string_lossy().to_string();

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    // Login.
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

    // Model listing endpoint (static list is empty here; just assert it responds).
    let resp = client
        .get(format!("{base}/api/models"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Create project pointing at the writable dir, approval policy "ask".
    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "proj",
            "dir": proj_dir_path,
            "default_model": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
            "approval_policy": "ask"
        }))
        .send()
        .await
        .unwrap();
    let project_id: String = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Open thread.
    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"resume": "th_test"}))
        .send()
        .await
        .unwrap();
    let thread_id: String = resp.json::<serde_json::Value>().await.unwrap()["thread_id"]
        .as_str()
        .unwrap()
        .to_string();

    let pid: ProjectId = project_id.parse().unwrap();
    let tid: ThreadId = thread_id.parse().unwrap();

    // Connect WS.
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

    ws.send(ws_text(&ClientMessage::Subscribe { thread_id: tid }))
        .await
        .unwrap();

    // --- SwitchMode -> Plan (persisted) ---
    ws.send(ws_text(&ClientMessage::SwitchMode {
        thread_id: tid,
        mode: Mode::Plan,
    }))
    .await
    .unwrap();
    let tf = poll_thread(&state, pid, tid, |tf| tf.mode == Mode::Plan).await;
    assert_eq!(tf.mode, Mode::Plan, "mode switch should persist");

    // --- SelectModel -> glm (context window recomputed from defaults table) ---
    ws.send(ws_text(&ClientMessage::SelectModel {
        thread_id: tid,
        model_ref: ModelRef {
            provider: "cloudflare-litellm".into(),
            model: "@cf/z-ai/glm-4.7".into(),
            reasoning_effort: None,
        },
    }))
    .await
    .unwrap();
    let tf = poll_thread(&state, pid, tid, |tf| {
        tf.current_model.model == "@cf/z-ai/glm-4.7"
    })
    .await;
    assert_eq!(
        tf.context_window, 131_072,
        "context window should recompute"
    );

    // --- SendInput -> turn streams and is persisted ---
    ws.send(ws_text(&ClientMessage::SendInput {
        thread_id: tid,
        text: "make a plan".into(),
    }))
    .await
    .unwrap();

    // Drain WS until TurnCompleted.
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    let mut saw_completed = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                if let Ok(ServerMessage::Event { agent_event, .. }) = serde_json::from_str(&t) {
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
    assert!(saw_completed, "should observe TurnCompleted");

    // Turn persisted: one Plan-mode turn with the agent item, tokens folded.
    let tf = poll_thread(&state, pid, tid, |tf| !tf.turns.is_empty()).await;
    assert_eq!(tf.turns.len(), 1);
    assert_eq!(tf.turns[0].mode, Mode::Plan);
    assert!(!tf.turns[0].items.is_empty());
    assert_eq!(
        tf.tokens.total.total, 1540,
        "usage folded into thread ledger"
    );
    // by_model nested under provider (C3).
    assert!(
        tf.tokens
            .by_model
            .get("cloudflare-litellm", "@cf/z-ai/glm-4.7")
            .is_some()
    );

    // --- Approval routing: the streamed approval was indexed; responding routes it (no error). ---
    let routed = state
        .registry
        .respond_approval(ApprovalId("ap_1".into()), ApprovalDecision::Accept)
        .await;
    assert!(
        routed.is_ok(),
        "approval decision should route to the harness"
    );

    // --- Plan dump: writes the latest Plan-mode turn's agent text to markdown. ---
    ws.send(ws_text(&ClientMessage::SavePlan {
        thread_id: tid,
        path: "docs/plan.md".into(),
    }))
    .await
    .unwrap();

    let plan_path = proj_dir.path().join("docs/plan.md");
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while !plan_path.exists() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    assert!(plan_path.exists(), "plan file should be written");
    let contents = tokio::fs::read_to_string(&plan_path).await.unwrap();
    assert!(
        contents.contains("Refactor token refresh"),
        "plan content written"
    );
}

async fn poll_thread<F>(
    state: &AppState,
    pid: ProjectId,
    tid: ThreadId,
    pred: F,
) -> giskard_persist::store::ThreadFile
where
    F: Fn(&giskard_persist::store::ThreadFile) -> bool,
{
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    loop {
        if let Ok(Some(tf)) = state.store.load_thread(pid, tid).await {
            if pred(&tf) {
                return tf;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("thread predicate not satisfied in time");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
}
