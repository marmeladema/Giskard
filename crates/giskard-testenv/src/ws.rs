use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use giskard_proto::{ClientMessage, ErrorInfo, LiveTurnSnapshot, ServerMessage};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub type TestWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn connect(addr: SocketAddr, cookie: &str) -> TestWs {
    let request = tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(format!("ws://{addr}/api/ws"))
        .header("host", addr.to_string())
        .header("cookie", cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    tokio_tungstenite::connect_async(request)
        .await
        .expect("WS connect")
        .0
}

pub fn text(message: &ClientMessage) -> Message {
    Message::Text(serde_json::to_string(message).unwrap().into())
}

pub async fn send(ws: &mut TestWs, message: &ClientMessage) {
    ws.send(text(message)).await.unwrap();
}

pub async fn recv_until<T>(
    ws: &mut TestWs,
    mut pick: impl FnMut(ServerMessage) -> Option<T>,
) -> Option<T> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                if let Ok(message) = serde_json::from_str::<ServerMessage>(&text)
                    && let Some(value) = pick(message)
                {
                    return Some(value);
                }
            }
            Ok(Some(Ok(_))) | Err(_) => {}
            Ok(Some(Err(_))) | Ok(None) => return None,
        }
    }
    None
}

pub async fn next_matching(
    ws: &mut TestWs,
    pred: impl Fn(&serde_json::Value) -> bool,
) -> Option<serde_json::Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                if pred(&value) {
                    return Some(value);
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => return None,
        }
    }
    None
}

pub async fn expect_error(ws: &mut TestWs) -> ErrorInfo {
    recv_until(ws, |message| match message {
        ServerMessage::Error { error } => Some(error),
        _ => None,
    })
    .await
    .expect("websocket error not observed")
}

pub async fn expect_error_for(ws: &mut TestWs, action: &str, code: &str) -> ErrorInfo {
    recv_until(ws, |message| match message {
        ServerMessage::Error { error }
            if error.action.as_deref() == Some(action) && error.code == code =>
        {
            Some(error)
        }
        _ => None,
    })
    .await
    .unwrap_or_else(|| panic!("websocket error {code}/{action} was not observed"))
}

pub async fn expect_live_snapshot(ws: &mut TestWs) -> LiveTurnSnapshot {
    recv_until(ws, |message| match message {
        ServerMessage::LiveTurnSnapshot(snapshot) => Some(snapshot),
        _ => None,
    })
    .await
    .expect("live turn snapshot not observed")
}
