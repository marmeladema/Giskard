//! Token ledger / dashboard integration test: a completed turn's usage folds into the global and
//! per-project token dashboards (spec §10.2).

use chrono::Utc;
use futures_util::SinkExt;
use giskard_proto::ClientMessage;
use giskard_testenv::{TestServer, factory, fixtures};

#[tokio::test]
async fn token_ledgers_and_dashboard() {
    let server = TestServer::spawn(factory::fixture(fixtures::completed_turn_fixture())).await;
    let project = server.create_project("proj").await;
    let pid = project.id;
    let thread_id = server
        .register_thread(pid, fixtures::COMPLETED_TURN_HARNESS_THREAD_ID)
        .await;
    let client = &server.client;
    let cookie = server.cookie.clone();
    let base = server.base.clone();

    // Drive one turn over WS.
    let mut ws = server.ws().await;
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
