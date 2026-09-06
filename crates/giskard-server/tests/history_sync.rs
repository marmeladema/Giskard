//! WebSocket history sync integration tests: paginated history load, resync deltas vs. full-page
//! fallback on reconnect, and a structured error when persisted history is corrupt.

use futures_util::{SinkExt, StreamExt};
use giskard_core::ids::{ProjectId, ThreadId, TurnId};
use giskard_proto::{ClientMessage, ErrorSeverity, ServerMessage};
use giskard_testenv::{TestProject, TestServer, factory, fixtures};

async fn setup(extra: &str) -> (TestServer, TestProject, ThreadId) {
    let server = TestServer::builder(factory::fixture(fixtures::completed_turn_fixture()))
        .config(extra)
        .start()
        .await;
    let project = server.create_project("proj").await;
    let tid = server
        .register_thread(project.id, fixtures::COMPLETED_TURN_HARNESS_THREAD_ID)
        .await;
    (server, project, tid)
}

#[tokio::test]
async fn history_pagination_over_http() {
    let (server, project, tid) = setup("[history]\ninitial=2\npage=2\n").await;
    let state = &server.state;
    let client = server.client.clone();
    let cookie = server.cookie.clone();
    let base = server.base.clone();
    let pid = project.id;

    // Open (register) the thread, then seed 5 turns directly into the authoritative history.
    let mut ids = Vec::new();
    for i in 0..5 {
        let t = fixtures::completed_turn(&format!("turn {i}"), fixtures::fake_native_model());
        ids.push(t.id.to_string());
        state.store.append_turn(pid, tid, &t).await.unwrap();
    }

    // Initial page = last 2 turns (ids[3], ids[4]), more available.
    let history_url = format!("{base}/api/projects/{pid}/threads/{tid}/history");
    let page: serde_json::Value = client
        .get(&history_url)
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let turns = page["turns"].as_array().unwrap();
    assert_eq!(turns.len(), 2);
    assert_eq!(page["has_more"], true);
    assert_eq!(turns[0]["id"].as_str().unwrap(), ids[3]);
    assert_eq!(turns[1]["id"].as_str().unwrap(), ids[4]);

    // Page older: before ids[3] → ids[1], ids[2], still more.
    let page: serde_json::Value = client
        .get(format!("{history_url}?before={}", ids[3]))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let turns = page["turns"].as_array().unwrap();
    assert_eq!(turns.len(), 2);
    assert_eq!(page["has_more"], true);
    assert_eq!(turns[0]["id"].as_str().unwrap(), ids[1]);
    assert_eq!(turns[1]["id"].as_str().unwrap(), ids[2]);

    // Final page: before ids[1] → ids[0], no more.
    let page: serde_json::Value = client
        .get(format!("{history_url}?before={}", ids[1]))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let turns = page["turns"].as_array().unwrap();
    assert_eq!(turns.len(), 1);
    assert_eq!(page["has_more"], false);
    assert_eq!(turns[0]["id"].as_str().unwrap(), ids[0]);

    // The path is project-scoped: both an unknown thread and a real thread named under the wrong
    // project have the same non-disclosing 404 contract.
    let unknown = client
        .get(format!(
            "{base}/api/projects/{pid}/threads/{}/history",
            ThreadId::new()
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(unknown.text().await.unwrap(), "not found");

    let other_proj_dir = tempfile::TempDir::new().unwrap();
    let other_pid = ProjectId::new();
    state
        .store
        .create_project(other_pid, "other", &other_proj_dir.path().to_string_lossy())
        .await
        .unwrap();
    let wrong_project = client
        .get(format!(
            "{base}/api/projects/{other_pid}/threads/{tid}/history"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_project.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(wrong_project.text().await.unwrap(), "not found");
}

/// Reconnect with a resync cursor: a resolvable `since` yields a `HistoryDelta` of just the turns
/// after it, and a stale `since` falls back to a bounded reset delta.
#[tokio::test]
async fn resync_delta_over_websocket() {
    let (server, project, tid) = setup("[history]\ninitial=2\npage=2\n").await;
    let state = &server.state;
    let pid = project.id;
    let mut ids = Vec::new();
    for i in 0..5 {
        let t = fixtures::completed_turn(&format!("turn {i}"), fixtures::fake_native_model());
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
                if v["type"] == "history_delta" {
                    return v;
                }
            }
        }
        panic!("no history frame received");
    }

    // A fresh subscription receives bounded bootstrap history, not an HTTP pagination response.
    let mut fresh_ws = server.ws().await;
    fresh_ws
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&ClientMessage::Subscribe {
                thread_id: tid,
                since: None,
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    let frame = next_history_frame(&mut fresh_ws).await;
    assert_eq!(frame["reset"], true);
    assert_eq!(frame["has_more"], true);
    let turns = frame["turns"].as_array().unwrap();
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0]["id"].as_str().unwrap(), ids[3]);
    assert_eq!(turns[1]["id"].as_str().unwrap(), ids[4]);

    // Resolvable cursor (ids[2]) → HistoryDelta with only the turns after it: ids[3], ids[4].
    let mut ws = server.ws().await;
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

    // Stale cursor → bounded reset delta (initial=2 → last two).
    let bogus: TurnId =
        fixtures::completed_turn("never persisted", fixtures::fake_native_model()).id;
    let mut ws2 = server.ws().await;
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
    assert_eq!(frame["type"], "history_delta");
    assert_eq!(frame["reset"], true);
    assert_eq!(frame["has_more"], true);
    let turns = frame["turns"].as_array().unwrap();
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0]["id"].as_str().unwrap(), ids[3]);
    assert_eq!(turns[1]["id"].as_str().unwrap(), ids[4]);
}

#[tokio::test]
async fn subscribe_corrupt_history_returns_structured_error() {
    let (server, project, tid) = setup("").await;
    let pid = project.id;

    // A bad **interior** line of the history index is real corruption, not a torn final append.
    let valid_turn = serde_json::to_string(&fixtures::completed_turn(
        "valid after corrupt line",
        fixtures::fake_native_model(),
    ))
    .unwrap();
    let history_path = server
        .data_dir()
        .join("projects")
        .join(pid.to_string())
        .join("threads")
        .join(tid.to_string())
        .join("history.jsonl");
    tokio::fs::write(&history_path, format!("not json\n{valid_turn}\n"))
        .await
        .unwrap();

    let mut ws = server.ws().await;

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
                assert_eq!(error.action.as_deref(), Some("subscribe_history"));
                assert!(error.detail.unwrap_or_default().contains("line 1"));
                return;
            }
            _ => continue,
        }
    }

    panic!("subscribe did not return a structured persistence error");
}
