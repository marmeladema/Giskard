//! WebSocket history sync integration tests: paginated history load, resync deltas vs. full-page
//! fallback on reconnect, and a structured error when persisted history is corrupt.

use std::sync::Arc;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemKind, ItemPayload, ItemStart};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
use giskard_harness::AgentHarness;
use giskard_harness_replay::{ReplayFixture, ReplayHarness};
use giskard_persist::store::ProjectConfig;
use giskard_proto::{ClientMessage, ErrorSeverity, ServerMessage};
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

async fn ws_connect(
    port: u16,
    cookie: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let req = tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(format!("ws://127.0.0.1:{port}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    tokio_tungstenite::connect_async(req).await.unwrap().0
}

#[tokio::test]
async fn history_pagination_over_websocket() {
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
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: tid,
            since: None,
        })
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

/// Reconnect with a resync cursor: a resolvable `since` yields a `HistoryDelta` of just the turns
/// after it, and a stale `since` falls back to a full `HistoryPage`.
#[tokio::test]
async fn resync_delta_over_websocket() {
    let port = 19204;
    let tmp = tempfile::TempDir::new().unwrap();
    let hash = password_hash("testpass");
    // initial=2 so the stale-cursor fallback returns a bounded page we can assert on.
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
        )
        .await
        .unwrap();

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

    async fn next_history_frame<S>(ws: &mut S) -> serde_json::Value
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
                if v["type"] == "history_delta" || v["type"] == "history_page" {
                    return v;
                }
            }
        }
        panic!("no history frame received");
    }

    // Resolvable cursor (ids[2]) → HistoryDelta with only the turns after it: ids[3], ids[4].
    let mut ws = ws_connect(port, &cookie).await;
    let cursor: TurnId = ids[2].parse().unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: tid,
            since: Some(cursor),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();
    let frame = next_history_frame(&mut ws).await;
    assert_eq!(frame["type"], "history_delta");
    let turns = frame["turns"].as_array().unwrap();
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0]["id"].as_str().unwrap(), ids[3]);
    assert_eq!(turns[1]["id"].as_str().unwrap(), ids[4]);

    // Stale cursor (a turn id never persisted) → full HistoryPage fallback (initial=2 → last two).
    let bogus: TurnId = make_turn("never persisted").id;
    let mut ws2 = ws_connect(port, &cookie).await;
    ws2.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: tid,
            since: Some(bogus),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();
    let frame = next_history_frame(&mut ws2).await;
    assert_eq!(frame["type"], "history_page");
    let turns = frame["turns"].as_array().unwrap();
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0]["id"].as_str().unwrap(), ids[3]);
    assert_eq!(turns[1]["id"].as_str().unwrap(), ids[4]);
}

#[tokio::test]
async fn subscribe_corrupt_history_returns_structured_error() {
    let port = 19203;
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

    let valid_turn = serde_json::to_string(&make_turn("valid after corrupt line")).unwrap();
    let history_path = tmp
        .path()
        .join("projects")
        .join(pid.to_string())
        .join("threads")
        .join(format!("{tid}.jsonl"));
    tokio::fs::write(&history_path, format!("not json\n{valid_turn}\n"))
        .await
        .unwrap();

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
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: tid,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        let text = match tokio::time::timeout(tokio::time::Duration::from_secs(1), ws.next()).await
        {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => text,
            Ok(Some(Ok(_))) | Err(_) => continue,
            Ok(Some(Err(error))) => {
                panic!("websocket error while waiting for subscribe error: {error}")
            }
            Ok(None) => break,
        };
        match serde_json::from_str::<ServerMessage>(&text).unwrap() {
            ServerMessage::Error { error } => {
                assert_eq!(error.code, "persistence_error");
                assert_eq!(error.severity, ErrorSeverity::Error);
                assert_eq!(error.thread_id, Some(tid));
                assert_eq!(error.action.as_deref(), Some("subscribe"));
                assert!(error.detail.unwrap_or_default().contains("line 1"));
                return;
            }
            _ => continue,
        }
    }

    panic!("subscribe did not return a structured persistence error");
}
