use futures_util::StreamExt;
use giskard_core::HarnessError;
use giskard_proto::ServerMessage;
use giskard_testenv::{TestServer, factory};
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn test_server_smoke() {
    let server = TestServer::spawn(factory::failing(HarnessError::Spawn("smoke".into()))).await;
    let project_id = server.create_project_via_api("smoke", "/tmp").await;
    assert!(
        server
            .store()
            .load_project(project_id)
            .await
            .unwrap()
            .is_some()
    );
    let mut ws = server.ws().await;
    let frame = ws.next().await.unwrap().unwrap();
    let Message::Text(text) = frame else {
        panic!("first websocket frame was not text")
    };
    serde_json::from_str::<ServerMessage>(&text).unwrap();
}
