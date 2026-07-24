//! Diff-accumulation integration test: `DiffUpdated` events fold into `Turn.diffs` (deduplicated by
//! path, keeping the latest) and are persisted with the completed turn.

use std::sync::Arc;

use chrono::Utc;
use futures_util::SinkExt;
use giskard_core::diff::{DiffHunk, DiffLine, FileDiff};
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{FileChangeKind, Item, ItemKind, ItemPayload, ItemStart};
use giskard_core::model::ModelRef;
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
use giskard_harness::AgentHarness;
use giskard_harness_replay::{ReplayFixture, ReplayHarness};
use giskard_persist::store::ProjectConfig;
use giskard_proto::ClientMessage;
use giskard_server::{AppState, HarnessFactory, build_app};

/// Harness factory that wraps a replay harness with a diff-containing fixture.
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

/// Build a fixture that emits two `DiffUpdated` events for the same file
/// (simulating incremental diff updates) plus one for a second file.
fn make_diff_fixture() -> ReplayFixture {
    let thread = ThreadId::new();
    let turn = TurnId::new();
    let item = ItemId::new();
    let now = Utc::now();

    let diff1 = FileDiff {
        path: "src/main.rs".into(),
        change: FileChangeKind::Modified,
        old_text: Some("fn main() {}".into()),
        new_text: Some("fn main() {\n    println!(\"hi\");\n}".into()),
        hunks: vec![DiffHunk {
            old_start: 1,
            old_lines: 1,
            new_start: 1,
            new_lines: 3,
            lines: vec![
                DiffLine::Removed("fn main() {}".into()),
                DiffLine::Added("fn main() {".into()),
                DiffLine::Added("    println!(\"hi\");".into()),
                DiffLine::Added("}".into()),
            ],
        }],
        binary: false,
    };

    let diff2 = FileDiff {
        path: "src/main.rs".into(),
        change: FileChangeKind::Modified,
        old_text: Some("fn main() {\n    println!(\"hi\");\n}".into()),
        new_text: Some("fn main() {\n    println!(\"hello\");\n}".into()),
        hunks: vec![DiffHunk {
            old_start: 2,
            old_lines: 1,
            new_start: 2,
            new_lines: 1,
            lines: vec![
                DiffLine::Removed("    println!(\"hi\");".into()),
                DiffLine::Added("    println!(\"hello\");".into()),
            ],
        }],
        binary: false,
    };

    let diff3 = FileDiff {
        path: "src/lib.rs".into(),
        change: FileChangeKind::Created,
        old_text: None,
        new_text: Some("pub fn lib() {}".into()),
        hunks: vec![],
        binary: false,
    };

    ReplayFixture::from_events(vec![
        AgentEvent::ThreadOpened {
            thread,
            harness_thread_id: "th_diff".into(),
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
        AgentEvent::DiffUpdated {
            thread,
            turn,
            diff: diff1,
        },
        AgentEvent::DiffUpdated {
            thread,
            turn,
            diff: diff2,
        },
        AgentEvent::DiffUpdated {
            thread,
            turn,
            diff: diff3,
        },
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: item,
                harness_item_id: "it_1".into(),
                payload: ItemPayload::AgentMessage {
                    text: "Modified src/main.rs and created src/lib.rs".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::new(200, 100),
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

/// DiffUpdated events should be accumulated into Turn.diffs and persisted.
///
/// Two diffs for the same path (`src/main.rs`) should be deduplicated to the
/// most recent one, while the second file (`src/lib.rs`) should appear as a
/// separate entry.
#[tokio::test]
async fn diff_accumulation_persists_turn_diffs() {
    let port = 19010;
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
    let factory = Arc::new(DiffFactory {
        fixture: make_diff_fixture(),
    });
    let state = AppState::new(store.clone(), factory, session_key);
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let proj_dir = tempfile::TempDir::new().unwrap();
    let proj_dir_path = proj_dir.path().to_string_lossy().to_string();
    let pid = ProjectId::new();
    state
        .store
        .create_project(
            pid,
            "diff-test",
            &proj_dir_path,
            ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
        )
        .await
        .unwrap();

    let http_client = reqwest::Client::new();
    let cookie = {
        let resp = http_client
            .post(format!("http://127.0.0.1:{port}/api/login"))
            .json(&serde_json::json!({"password": "testpass"}))
            .send()
            .await
            .unwrap();
        resp.headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string()
    };

    let open_resp: serde_json::Value = http_client
        .post(format!(
            "http://127.0.0.1:{port}/api/projects/{pid}/threads"
        ))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"resume": "th_diff"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let thread_id: ThreadId = serde_json::from_value(open_resp["thread_id"].clone()).unwrap();

    let ws_base = format!("ws://127.0.0.1:{port}");
    let ws_request = tokio_tungstenite::tungstenite::http::Request::builder()
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

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id,
            text: "modify files".into(),
            attachments: Vec::new(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    loop {
        if let Ok(turns) = state.store.load_all_turns(pid, thread_id).await
            && !turns.is_empty()
        {
            let turn = &turns[0];
            assert_eq!(
                turn.diffs.len(),
                2,
                "two distinct file paths should have diffs (dedup by path)"
            );

            let main_rs_diff = turn
                .diffs
                .iter()
                .find(|d| d.path.to_string_lossy() == "src/main.rs")
                .expect("src/main.rs diff should exist");
            assert_eq!(main_rs_diff.change, FileChangeKind::Modified);
            assert!(
                main_rs_diff.new_text.as_ref().unwrap().contains("hello"),
                "should contain the latest diff (hello, not hi)"
            );

            let lib_rs_diff = turn
                .diffs
                .iter()
                .find(|d| d.path.to_string_lossy() == "src/lib.rs")
                .expect("src/lib.rs diff should exist");
            assert_eq!(lib_rs_diff.change, FileChangeKind::Created);

            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("turn was not persisted within 10 seconds");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
}
