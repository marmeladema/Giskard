//! Token ledger / dashboard integration test: a completed turn's usage folds into the global and
//! per-project token dashboards (spec §10.2).

use std::sync::Arc;

use chrono::Utc;
use futures_util::SinkExt;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemKind, ItemPayload, ItemStart};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
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
            attachments: Vec::new(),
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
