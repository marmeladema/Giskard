//! Smoke test: the desktop UI page is served at `/`, is public, and is self-contained (§13).

use std::sync::Arc;

use giskard_persist::store::ProjectConfig;
use giskard_server::{AppState, HarnessFactory, build_app};

struct NoFactory;
#[async_trait::async_trait]
impl HarnessFactory for NoFactory {
    async fn create(
        &self,
        _c: &ProjectConfig,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, giskard_core::HarnessError> {
        Err(giskard_core::HarnessError::Spawn("unused".into()))
    }
}

#[tokio::test]
async fn index_page_is_served_and_public() {
    let port = 19300;
    let tmp = tempfile::TempDir::new().unwrap();
    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let state = AppState::new(store, Arc::new(NoFactory), (0..32u8).collect());
    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // No cookie: the page must load without authentication.
    let body = reqwest::get(format!("http://127.0.0.1:{port}/"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("<title>Giskard</title>"));
    assert!(body.contains("/api/ws"), "app talks to the WS endpoint");
    assert!(
        body.contains("/api/ws-ticket"),
        "app fetches a WS auth ticket"
    );
    assert!(body.contains("send_input"), "composer wired to SendInput");
    assert!(
        body.contains("pendingClientMsgs"),
        "messages are queued while WS connects"
    );
    assert!(
        body.contains("Connecting to agent"),
        "UI exposes WS connecting state"
    );
    assert!(
        body.contains("WebSocket closed"),
        "UI surfaces WS close failures"
    );
    assert!(
        body.contains("Invalid WebSocket message from server"),
        "UI surfaces malformed WS frames"
    );
    assert!(
        body.contains("renderedHarnessItemIds"),
        "UI de-duplicates rendered harness items"
    );
    assert!(
        body.contains("streamElsByItemId"),
        "UI tracks streamed items independently"
    );
    assert!(
        body.contains("renderedItemIds"),
        "UI de-duplicates completed items by Giskard item id"
    );
    assert!(
        body.contains("finalizeStreamedItem"),
        "UI finalizes streamed agent messages instead of duplicating them"
    );
    assert!(
        body.contains("addItem(ev.item)"),
        "UI renders completed events with full item metadata"
    );
    assert!(
        body.contains("addItem(it)"),
        "UI replays persisted history with full item metadata"
    );
    // Self-contained: no external script/style hosts.
    assert!(!body.contains("http://"), "no external http asset refs");
    assert!(!body.contains("cdn"), "no CDN references");
}
