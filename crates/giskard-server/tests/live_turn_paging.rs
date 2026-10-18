//! The bounded reconnect snapshot and the turn items route (spec §13.6, LP1–LP3).
//!
//! A live turn with more items than `[history] turn_items` subscribes with only its newest items,
//! plus what is still open or waiting on the user, and names how many settled items it left out
//! and where paging them starts. The items route then pages those from the live buffer while the
//! turn runs, and from the payload once the turn is persisted.

use chrono::Utc;
use futures_util::SinkExt;
use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemDelta, ItemKind, ItemPayload, ItemStart};
use giskard_proto::{ClientMessage, OmittedItems, WireAgentEvent};
use giskard_testenv::fake::{self, FakeHarness};
use giskard_testenv::{TestServer, fixtures, ws};

const TURN_ITEMS: usize = 10;
const MESSAGES: usize = 30;
const APPROVAL_ID: &str = "ap_live_paging";

async fn get(server: &TestServer, url: &str) -> (reqwest::StatusCode, serde_json::Value) {
    let response = server
        .client
        .get(url)
        .header("cookie", &server.cookie)
        .send()
        .await
        .expect("request");
    let status = response.status();
    let body = response.json().await.unwrap_or(serde_json::Value::Null);
    (status, body)
}

/// The ids of a page's items, and its `has_more`.
fn page_ids(body: &serde_json::Value) -> (Vec<String>, bool) {
    let ids = body["items"]
        .as_array()
        .expect("items")
        .iter()
        .map(|item| item["id"].as_str().expect("id").to_owned())
        .collect();
    (ids, body["has_more"].as_bool().expect("has_more"))
}

fn strings(ids: &[ItemId]) -> Vec<String> {
    ids.iter().map(ToString::to_string).collect()
}

fn items_url(server: &TestServer, project: ProjectId, thread: ThreadId, turn: TurnId) -> String {
    format!(
        "{}/api/projects/{project}/threads/{thread}/turns/{turn}/items",
        server.base
    )
}

#[tokio::test]
async fn a_long_live_turn_subscribes_with_its_tail_and_pages_the_rest() {
    let server = TestServer::builder(fake::factory(FakeHarness::new(())))
        .config(&format!("[history]\nturn_items = {TURN_ITEMS}\n"))
        .start()
        .await;
    let project = server.create_project("live-paging").await;
    let thread = server
        .register_thread(project.id, "live_paging_thread")
        .await;
    let turn = TurnId::new();
    let runtime = server
        .state
        .registry
        .thread_runtime(thread)
        .await
        .expect("thread runtime");
    runtime.replace_live_turn_for_test(turn, None);
    let apply = |event: AgentEvent| runtime.apply_event_for_test(&event, true);
    let start = |id: ItemId, kind: ItemKind| AgentEvent::ItemStarted {
        thread,
        turn,
        item: ItemStart {
            id,
            harness_item_id: format!("h_{id}"),
            kind,
            command: None,
            tool: None,
        },
    };

    apply(AgentEvent::TurnStarted { thread, turn });
    let open = ItemId::new();
    let mut ids = Vec::new();
    for n in 0..MESSAGES {
        let id = ItemId::new();
        apply(start(id, ItemKind::AgentMessage));
        apply(AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id,
                harness_item_id: format!("h_{id}"),
                payload: ItemPayload::AgentMessage {
                    text: format!("message {n}"),
                },
                created_at: Utc::now(),
            },
        });
        ids.push(id);
        if n == 4 {
            apply(AgentEvent::ApprovalRequested {
                thread,
                turn,
                request: ApprovalRequest {
                    id: ApprovalId(APPROVAL_ID.into()),
                    kind: ApprovalKind::CommandExecution {
                        command: "rm -rf ./build".into(),
                        cwd: "/tmp/project".into(),
                    },
                    reason: None,
                    metadata: vec![],
                    available: vec![ApprovalDecision::Accept, ApprovalDecision::Decline],
                },
            });
            apply(start(open, ItemKind::Reasoning));
            apply(AgentEvent::ItemDelta {
                thread,
                turn,
                item_id: open,
                delta: ItemDelta::Text {
                    text: "still thinking".into(),
                },
            });
        }
    }

    let mut socket = server.ws().await;
    socket
        .send(ws::text(&ClientMessage::Subscribe {
            thread_id: thread,
            since: None,
        }))
        .await
        .expect("subscribe");
    let snapshot = ws::expect_live_snapshot(&mut socket).await;

    assert_eq!(
        snapshot.omitted,
        Some(OmittedItems {
            count: (MESSAGES - TURN_ITEMS) as u32,
            before: ids[MESSAGES - TURN_ITEMS],
        })
    );
    let shape: Vec<String> = snapshot
        .accumulated
        .iter()
        .map(|event| match event {
            WireAgentEvent::TurnStarted { .. } => "turn_started".to_owned(),
            WireAgentEvent::ApprovalRequested { request, .. } => {
                format!("approval:{}", request.id.0)
            }
            WireAgentEvent::ItemStarted { item, .. } => format!("start:{}", item.id),
            WireAgentEvent::ItemCompleted { item, .. } => format!("completed:{}", item.id),
            WireAgentEvent::ItemDelta { item_id, .. } => format!("delta:{item_id}"),
            other => format!("{other:?}"),
        })
        .collect();
    let mut expected = vec![
        "turn_started".to_owned(),
        format!("approval:{APPROVAL_ID}"),
        format!("start:{open}"),
        format!("delta:{open}"),
    ];
    for id in &ids[MESSAGES - TURN_ITEMS..] {
        expected.extend([format!("start:{id}"), format!("completed:{id}")]);
    }
    assert_eq!(shape, expected);

    // Page the omitted items from the live buffer, walking backwards to the start.
    let url = items_url(&server, project.id, thread, turn);
    let (status, body) = get(&server, &format!("{url}?before={}", ids[20])).await;
    assert_eq!(status, 200);
    assert_eq!(body["turn_id"], turn.to_string());
    assert_eq!(page_ids(&body), (strings(&ids[10..20]), true));
    let (_, body) = get(&server, &format!("{url}?before={}", ids[10])).await;
    assert_eq!(page_ids(&body), (strings(&ids[..10]), false));
    let (_, body) = get(&server, &format!("{url}?before={}&limit=3", ids[20])).await;
    assert_eq!(page_ids(&body), (strings(&ids[17..20]), true));
    let (_, body) = get(&server, &url).await;
    assert_eq!(
        page_ids(&body),
        (strings(&ids[20..]), true),
        "without a cursor the page ends at the newest item"
    );

    let (status, _) = get(&server, &format!("{url}?before={}", ItemId::new())).await;
    assert_eq!(status, 404, "an unknown cursor is a client error");
    let other = server
        .register_thread(project.id, "live_paging_other_thread")
        .await;
    let (status, _) = get(&server, &items_url(&server, project.id, other, turn)).await;
    assert_eq!(status, 404, "the live turn is not another thread's turn");
    let (status, _) = get(
        &server,
        &items_url(&server, project.id, thread, TurnId::new()),
    )
    .await;
    assert_eq!(status, 404, "an unknown turn");

    // Unauthenticated: the same answer the history route gives.
    let history = format!(
        "{}/api/projects/{}/threads/{thread}/history",
        server.base, project.id
    );
    let history_status = server.client.get(&history).send().await.unwrap().status();
    let items_status = server.client.get(&url).send().await.unwrap().status();
    assert!(history_status.is_client_error());
    assert_eq!(items_status, history_status);
}

#[tokio::test]
async fn a_persisted_turn_pages_from_its_payload_with_reasoning_previewed() {
    let server = TestServer::builder(fake::factory(FakeHarness::new(())))
        .config(&format!("[history]\nturn_items = {TURN_ITEMS}\n"))
        .start()
        .await;
    let project = server.create_project("persisted-paging").await;
    let thread = server
        .register_thread(project.id, "persisted_paging_thread")
        .await;
    let long_note = "a long thought.\n".repeat(192);
    let mut turn = fixtures::completed_turn("prompt", fixtures::fake_native_model());
    turn.items = (0..25)
        .map(|n| Item {
            id: ItemId::new(),
            harness_item_id: format!("h_{n}"),
            payload: if n % 5 == 0 {
                ItemPayload::Reasoning {
                    text: long_note.clone(),
                }
            } else {
                ItemPayload::AgentMessage {
                    text: format!("message {n}"),
                }
            },
            created_at: Utc::now(),
        })
        .collect();
    let ids: Vec<ItemId> = turn.items.iter().map(|item| item.id).collect();
    server
        .state
        .store
        .append_turn(project.id, thread, &turn)
        .await
        .expect("persist turn");

    let url = items_url(&server, project.id, thread, turn.id);
    let (status, body) = get(&server, &url).await;
    assert_eq!(status, 200);
    assert_eq!(page_ids(&body), (strings(&ids[15..]), true));
    let (_, body) = get(&server, &format!("{url}?before={}", ids[15])).await;
    assert_eq!(page_ids(&body), (strings(&ids[5..15]), true));
    let reasoning: Vec<&serde_json::Value> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["payload"]["kind"] == "reasoning")
        .collect();
    assert_eq!(reasoning.len(), 2);
    for item in reasoning {
        assert!(item["payload"]["text"].as_str().unwrap().len() <= 1024);
        assert!(item["payload"]["preview"].is_object(), "{item}");
    }
    let (_, body) = get(&server, &format!("{url}?before={}", ids[5])).await;
    assert_eq!(page_ids(&body), (strings(&ids[..5]), false));
    let (status, _) = get(&server, &format!("{url}?before={}", ItemId::new())).await;
    assert_eq!(status, 404);
}
