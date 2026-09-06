//! Diff-accumulation integration test: `DiffUpdated` events fold into `Turn.diffs` (deduplicated by
//! path, keeping the latest) and are persisted with the completed turn.

use chrono::Utc;
use futures_util::SinkExt;
use giskard_core::diff::{DiffHunk, DiffLine, FileDiff};
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ThreadId, TurnId};
use giskard_core::item::{FileChangeEntry, FileChangeKind, Item, ItemKind, ItemPayload, ItemStart};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
use giskard_harness_replay::ReplayFixture;
use giskard_proto::ClientMessage;
use giskard_testenv::{TestServer, factory};

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
        captured: None,
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
        captured: None,
    };

    let diff3 = FileDiff {
        path: "src/lib.rs".into(),
        change: FileChangeKind::Created,
        old_text: None,
        new_text: Some("pub fn lib() {}".into()),
        hunks: vec![],
        binary: false,
        captured: None,
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
                kind: ItemKind::FileChange,
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
                payload: ItemPayload::FileChange {
                    path: "src/inline.rs".into(),
                    change: FileChangeKind::Modified,
                    changes: vec![FileChangeEntry {
                        path: "src/inline.rs".into(),
                        change: FileChangeKind::Modified,
                        diff: Some("--- a/src/inline.rs\n+++ b/src/inline.rs\n-old\n+new\n".into()),
                        captured_diff: None,
                    }],
                    status: Some("completed".into()),
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

/// DiffUpdated events should be accumulated into Turn.diffs and persisted.
///
/// Two diffs for the same path (`src/main.rs`) should be deduplicated to the
/// most recent one, while the second file (`src/lib.rs`) should appear as a
/// separate entry.
#[tokio::test]
async fn diff_accumulation_persists_turn_diffs() {
    let server = TestServer::spawn(factory::fixture(make_diff_fixture())).await;
    let project = server.create_project("diff-test").await;
    let pid = project.id;
    let thread_id = server.register_thread(pid, "th_diff").await;
    let state = &server.state;
    let http_client = &server.client;
    let cookie = server.cookie.clone();
    let port = server.addr.port();
    let mut ws = server.ws().await;

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
            assert!(main_rs_diff.old_text.is_none() && main_rs_diff.new_text.is_none());
            assert!(
                main_rs_diff.hunks.is_empty(),
                "history projection is descriptor-only"
            );
            let main_descriptor = main_rs_diff.captured.as_ref().expect("captured descriptor");
            let main_content: serde_json::Value = http_client
                .get(format!(
                    "http://127.0.0.1:{port}/api/projects/{pid}/threads/{thread_id}/turns/{}/diffs/{}",
                    turn.id, main_descriptor.id
                ))
                .header("cookie", &cookie)
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            assert!(
                main_content["content"]["diff"]["new_text"]
                    .as_str()
                    .unwrap()
                    .contains("hello"),
                "lazy endpoint should return the latest captured body"
            );

            let lib_rs_diff = turn
                .diffs
                .iter()
                .find(|d| d.path.to_string_lossy() == "src/lib.rs")
                .expect("src/lib.rs diff should exist");
            assert_eq!(lib_rs_diff.change, FileChangeKind::Created);
            assert!(lib_rs_diff.captured.is_some());

            let inline_descriptor = match &turn.items[0].payload {
                ItemPayload::FileChange { changes, .. } => {
                    assert!(
                        changes[0].diff.is_none(),
                        "inline body must not hydrate history"
                    );
                    changes[0]
                        .captured_diff
                        .as_ref()
                        .expect("inline descriptor")
                }
                other => panic!("expected file-change item, got {other:?}"),
            };
            let inline_content: serde_json::Value = http_client
                .get(format!(
                    "http://127.0.0.1:{port}/api/projects/{pid}/threads/{thread_id}/turns/{}/diffs/{}",
                    turn.id, inline_descriptor.id
                ))
                .header("cookie", &cookie)
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(inline_content["content"]["kind"], "unified");
            assert!(
                inline_content["content"]["text"]
                    .as_str()
                    .unwrap()
                    .contains("+new")
            );

            let raw_payload = tokio::fs::read_to_string(
                server
                    .data_dir()
                    .join("projects")
                    .join(pid.to_string())
                    .join("threads")
                    .join(thread_id.to_string())
                    .join("turns")
                    .join(format!("{}.jsonl", turn.id)),
            )
            .await
            .unwrap();
            assert!(raw_payload.contains(r#""format":1"#));
            assert!(raw_payload.contains(r#""diff":"--- a/src/inline.rs"#));
            assert!(!raw_payload.contains("diff_content"));
            assert!(!raw_payload.contains("captured_diff"));

            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("turn was not persisted within 10 seconds");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
}
