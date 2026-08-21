//! WebSocket history sync integration tests: staged bootstrap history, pagination, cursor reset,
//! and a structured error when persisted history is corrupt.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
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
use giskard_proto::{
    BootstrapHistory, BootstrapSection, ClientMessage, ErrorSeverity, HistoryPageResponse,
    ServerMessage, ThreadBootstrapFrame, ThreadBootstrapPayload, ThreadHistoryCursor,
};
use giskard_server::{AppState, HarnessFactory, build_app};

type TestWs =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

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

async fn ws_connect(port: u16, cookie: &str) -> TestWs {
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
async fn history_pagination_over_http() {
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
        .create_project(pid, "proj", &proj_dir.path().to_string_lossy())
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
    let bootstrap = wait_for_bootstrap(&mut ws, tid).await;
    let BootstrapHistory::FullPage {
        cursor: history_cursor,
        turns,
        has_more,
    } = bootstrap.history
    else {
        panic!("initial subscribe should carry a full history page");
    };
    assert_eq!(
        history_cursor.newest_turn_id.map(|id| id.to_string()),
        Some(ids[4].clone())
    );
    assert_eq!(turns.len(), 2);
    assert!(has_more);
    assert_eq!(turns[0].id.to_string(), ids[3]);
    assert_eq!(turns[1].id.to_string(), ids[4]);

    // Page older over authenticated HTTP: before ids[3] → ids[1], ids[2], still more.
    let cursor: TurnId = ids[3].parse().unwrap();
    let page: HistoryPageResponse = client
        .get(format!("{base}/api/projects/{pid}/threads/{tid}/history"))
        .header("cookie", &cookie)
        .query(&[("before", cursor.to_string())])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page.before, cursor);
    assert_eq!(page.turns.len(), 2);
    assert!(page.has_more);
    assert_eq!(page.turns[0].id.to_string(), ids[1]);
    assert_eq!(page.turns[1].id.to_string(), ids[2]);

    // Final page: before ids[1] → ids[0], no more.
    let cursor: TurnId = ids[1].parse().unwrap();
    let page: HistoryPageResponse = client
        .get(format!("{base}/api/projects/{pid}/threads/{tid}/history"))
        .header("cookie", &cookie)
        .query(&[("before", cursor.to_string())])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page.before, cursor);
    assert_eq!(page.turns.len(), 1);
    assert!(!page.has_more);
    assert_eq!(page.turns[0].id.to_string(), ids[0]);

    // The history route is protected by the same session middleware as the rest of the API.
    let unauthenticated = reqwest::Client::new()
        .get(format!("{base}/api/projects/{pid}/threads/{tid}/history"))
        .query(&[("before", cursor.to_string())])
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), reqwest::StatusCode::UNAUTHORIZED);

    let missing_cursor = client
        .get(format!("{base}/api/projects/{pid}/threads/{tid}/history"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(missing_cursor.status(), reqwest::StatusCode::BAD_REQUEST);

    let other_project = ProjectId::new();
    state
        .store
        .create_project(other_project, "other", &proj_dir.path().to_string_lossy())
        .await
        .unwrap();
    let wrong_project = client
        .get(format!(
            "{base}/api/projects/{other_project}/threads/{tid}/history"
        ))
        .header("cookie", &cookie)
        .query(&[("before", cursor.to_string())])
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_project.status(), reqwest::StatusCode::NOT_FOUND);
}

/// Reconnect with a resync cursor: a resolvable `since` yields only later turns, while an unknown
/// cursor produces an explicit reset to a bounded newest page.
#[tokio::test]
async fn resync_delta_over_websocket() {
    let port = 19204;
    let tmp = tempfile::TempDir::new().unwrap();
    let hash = password_hash("testpass");
    // initial=2 so the cursor-reset payload is a bounded page we can assert on.
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
        .create_project(pid, "proj", &proj_dir.path().to_string_lossy())
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

    // Resolvable cursor (ids[2]) yields only the turns after it: ids[3], ids[4].
    let mut ws = ws_connect(port, &cookie).await;
    let authority = state
        .store
        .load_history_snapshot(pid, tid, None, None, 2)
        .await
        .unwrap()
        .cursor;
    let cursor = ThreadHistoryCursor {
        newest_turn_id: Some(ids[2].parse().unwrap()),
        server_epoch: authority.server_epoch.clone(),
        amendment_sequence: authority.amendment_sequence,
    };
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: tid,
            since: Some(cursor.clone()),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();
    let bootstrap = wait_for_bootstrap(&mut ws, tid).await;
    let BootstrapHistory::Delta { after, turns, .. } = bootstrap.history else {
        panic!("a known cursor should produce delta bootstrap history");
    };
    assert_eq!(after, cursor);
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0].id.to_string(), ids[3]);
    assert_eq!(turns[1].id.to_string(), ids[4]);

    // An unknown cursor explicitly resets to the bounded newest page (initial=2 → last two).
    let bogus = ThreadHistoryCursor {
        newest_turn_id: Some(make_turn("never persisted").id),
        server_epoch: authority.server_epoch,
        amendment_sequence: authority.amendment_sequence,
    };
    let mut ws2 = ws_connect(port, &cookie).await;
    ws2.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: tid,
            since: Some(bogus.clone()),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();
    let bootstrap = wait_for_bootstrap(&mut ws2, tid).await;
    let BootstrapHistory::CursorReset {
        requested_after,
        turns,
        has_more,
        ..
    } = bootstrap.history
    else {
        panic!("an unknown cursor should produce cursor-reset bootstrap history");
    };
    assert_eq!(requested_after, bogus);
    assert_eq!(turns.len(), 2);
    assert!(has_more);
    assert_eq!(turns[0].id.to_string(), ids[3]);
    assert_eq!(turns[1].id.to_string(), ids[4]);
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
        .create_project(pid, "proj", &proj_dir.path().to_string_lossy())
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

    // A bad **interior** line of the history index is real corruption, not a torn final append.
    let valid_turn = serde_json::to_string(&make_turn("valid after corrupt line")).unwrap();
    let history_path = tmp
        .path()
        .join("projects")
        .join(pid.to_string())
        .join("threads")
        .join(tid.to_string())
        .join("history.jsonl");
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
                assert_eq!(error.code, "thread_bootstrap_failed");
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

async fn wait_for_bootstrap(ws: &mut TestWs, thread_id: ThreadId) -> ThreadBootstrapPayload {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    let mut generation = None;
    let mut expected = HashMap::new();
    let mut chunks: HashMap<BootstrapSection, BTreeMap<u32, Vec<u8>>> = HashMap::new();
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                let Ok(ServerMessage::ThreadBootstrap {
                    thread_id: message_thread_id,
                    subscription_generation,
                    frame,
                }) = serde_json::from_str(&text)
                else {
                    continue;
                };
                if message_thread_id != thread_id {
                    continue;
                }
                match frame {
                    ThreadBootstrapFrame::Start { sections } => {
                        generation = Some(subscription_generation);
                        expected = sections
                            .into_iter()
                            .map(|section| (section.section, section.chunk_count))
                            .collect();
                        chunks.clear();
                    }
                    ThreadBootstrapFrame::Chunk {
                        section,
                        index,
                        payload_base64,
                    } if generation == Some(subscription_generation) => {
                        let payload = BASE64
                            .decode(payload_base64)
                            .expect("bootstrap chunks should contain valid base64");
                        chunks.entry(section).or_default().insert(index, payload);
                    }
                    ThreadBootstrapFrame::Commit if generation == Some(subscription_generation) => {
                        for (section, chunk_count) in &expected {
                            assert_eq!(
                                chunks.get(section).map(BTreeMap::len),
                                Some(*chunk_count as usize),
                                "bootstrap section {section:?} was incomplete at commit"
                            );
                        }
                        return decode_bootstrap_sections(&mut chunks);
                    }
                    ThreadBootstrapFrame::Chunk { .. } | ThreadBootstrapFrame::Commit => {}
                }
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(error))) => panic!("websocket error during bootstrap: {error}"),
            Ok(None) => break,
            Err(_) => {}
        }
    }
    panic!("committed thread bootstrap not observed");
}

fn decode_bootstrap_sections(
    chunks: &mut HashMap<BootstrapSection, BTreeMap<u32, Vec<u8>>>,
) -> ThreadBootstrapPayload {
    ThreadBootstrapPayload {
        metadata: take_bootstrap_section(chunks, BootstrapSection::Metadata),
        history: take_bootstrap_section(chunks, BootstrapSection::History),
        live_turn: take_bootstrap_section(chunks, BootstrapSection::LiveTurn),
        ordered_suffix: take_bootstrap_section(chunks, BootstrapSection::OrderedSuffix),
        final_runtime: take_bootstrap_section(chunks, BootstrapSection::FinalRuntime),
        notices: take_bootstrap_section(chunks, BootstrapSection::Notices),
    }
}

fn take_bootstrap_section<T>(
    chunks: &mut HashMap<BootstrapSection, BTreeMap<u32, Vec<u8>>>,
    section: BootstrapSection,
) -> T
where
    T: serde::de::DeserializeOwned,
{
    let section_chunks = chunks
        .remove(&section)
        .unwrap_or_else(|| panic!("bootstrap section {section:?} was absent"));
    let mut encoded = Vec::new();
    for (expected_index, (index, chunk)) in section_chunks.into_iter().enumerate() {
        assert_eq!(index as usize, expected_index, "bootstrap chunk gap");
        encoded.extend(chunk);
    }
    serde_json::from_slice(&encoded)
        .unwrap_or_else(|error| panic!("bootstrap section {section:?} was invalid: {error}"))
}
