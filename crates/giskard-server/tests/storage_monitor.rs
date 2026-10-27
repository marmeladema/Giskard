//! Storage health is global socket state: no project selection or thread subscription is needed.

use chrono::Utc;
use giskard_proto::{ServerMessage, StorageFilesystem, StorageStatus};
use giskard_testenv::fake::{self, FakeHarness};
use giskard_testenv::{TestServer, TestWs, ws};

async fn next_storage_status(socket: &mut TestWs) -> StorageStatus {
    ws::recv_until(socket, |message| match message {
        ServerMessage::StorageStatus(status) => Some(status),
        _ => None,
    })
    .await
    .expect("global storage status was not delivered")
}

fn snapshot(revision: u64, low: bool) -> StorageStatus {
    const GIB: u64 = 1024 * 1024 * 1024;
    StorageStatus {
        revision,
        checked_at: Some(Utc::now()),
        enabled: true,
        filesystems: vec![StorageFilesystem {
            id: "path-0".into(),
            paths: vec!["/test-data".into()],
            available_bytes: Some(if low { 2 * GIB } else { 20 * GIB }),
            total_bytes: Some(100 * GIB),
            minimum_available_bytes: 10 * GIB,
            low,
            error: None,
        }],
        issues: Vec::new(),
    }
}

#[tokio::test]
async fn global_storage_bootstrap_and_live_recovery_need_no_thread_subscription() {
    let harness = FakeHarness::new(());
    let server = TestServer::spawn(fake::factory(harness.clone())).await;
    let mut socket = server.ws().await;

    // TestServer intentionally does not launch production background monitoring. Its disabled
    // initial projection still bootstraps every socket, without any client messages.
    let initial = next_storage_status(&mut socket).await;
    assert_eq!(initial, server.state.storage_monitor.current());
    assert!(!initial.enabled);
    assert_eq!(initial.checked_at, None);
    assert!(initial.filesystems.is_empty());

    let low = snapshot(initial.revision + 1, true);
    server.state.hub.publish_storage_status(low.clone()).await;
    assert_eq!(next_storage_status(&mut socket).await, low);

    let recovered = snapshot(low.revision + 1, false);
    server
        .state
        .hub
        .publish_storage_status(recovered.clone())
        .await;
    assert_eq!(next_storage_status(&mut socket).await, recovered);
    assert!(
        harness.core.calls().is_empty(),
        "health delivery must not start a harness"
    );
    socket.close(None).await.unwrap();
}
