//! Cancellation-safe stdio transport for the Codex app-server.
//!
//! `codex-codes::AsyncClient` currently frames stdout with `read_line`, whose future may lose
//! partially read bytes when another `tokio::select!` branch wins. Giskard must select between
//! app-server messages and harness commands, so this transport keeps the SDK's protocol types but
//! uses cancellation-safe `Lines::next_line` framing.

use std::collections::VecDeque;

use async_trait::async_trait;
use serde::Serialize;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tracing::{debug, warn};

use giskard_core::error::HarnessError;

use super::CodexTransport;

const STDOUT_BUFFER_SIZE: usize = 10 * 1024 * 1024;

async fn read_json_rpc_message<R>(
    reader: &mut Lines<R>,
) -> Result<Option<codex_codes::JsonRpcMessage>, HarnessError>
where
    R: AsyncBufRead + Unpin,
{
    loop {
        let Some(line) = reader
            .next_line()
            .await
            .map_err(|error| HarnessError::Transport(error.to_string()))?
        else {
            debug!("Codex app-server stdout reached EOF");
            return Ok(None);
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        return serde_json::from_str(line)
            .map(Some)
            .map_err(|error| HarnessError::Protocol(error.to_string()));
    }
}

pub(crate) struct CodexStdioTransport {
    child: Child,
    writer: BufWriter<ChildStdin>,
    reader: Lines<BufReader<ChildStdout>>,
    stderr_drain: tokio::task::JoinHandle<()>,
    next_id: i64,
    buffered: VecDeque<codex_codes::ServerMessage>,
}

impl CodexStdioTransport {
    pub(crate) async fn start(
        builder: codex_codes::AppServerBuilder,
        initialize: codex_codes::InitializeParams,
    ) -> Result<Self, HarnessError> {
        codex_codes::version::check_codex_version_async()
            .await
            .map_err(|error| HarnessError::Spawn(error.to_string()))?;
        let mut child = builder
            .spawn()
            .await
            .map_err(|error| HarnessError::Spawn(error.to_string()))?;
        let stdin = child.stdin.take().ok_or_else(|| {
            HarnessError::Spawn("Codex app-server stdin was not available".into())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            HarnessError::Spawn("Codex app-server stdout was not available".into())
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            HarnessError::Spawn("Codex app-server stderr was not available".into())
        })?;
        let stderr_drain = tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => debug!(target: "codex_app_server", %line),
                    Ok(None) => break,
                    Err(error) => {
                        warn!(%error, "failed reading Codex app-server stderr");
                        break;
                    }
                }
            }
        });
        let mut client = Self {
            child,
            writer: BufWriter::new(stdin),
            reader: BufReader::with_capacity(STDOUT_BUFFER_SIZE, stdout).lines(),
            stderr_drain,
            next_id: 1,
            buffered: VecDeque::new(),
        };
        let params = serde_json::to_value(initialize)
            .map_err(|error| HarnessError::Protocol(error.to_string()))?;
        client
            .request_json(codex_codes::protocol::methods::INITIALIZE, params)
            .await
            .map_err(|error| HarnessError::Spawn(error.to_string()))?;
        client
            .send_notification(codex_codes::protocol::methods::INITIALIZED)
            .await
            .map_err(|error| HarnessError::Spawn(error.to_string()))?;
        Ok(client)
    }

    async fn send_raw<T: Serialize>(&mut self, message: &T) -> Result<(), HarnessError> {
        let mut json = serde_json::to_vec(message)
            .map_err(|error| HarnessError::Protocol(error.to_string()))?;
        json.push(b'\n');
        self.writer
            .write_all(&json)
            .await
            .map_err(|error| HarnessError::Transport(error.to_string()))?;
        self.writer
            .flush()
            .await
            .map_err(|error| HarnessError::Transport(error.to_string()))
    }

    async fn send_notification(&mut self, method: &str) -> Result<(), HarnessError> {
        self.send_raw(&codex_codes::JsonRpcNotification {
            method: method.to_owned(),
            params: None,
        })
        .await
    }

    async fn read_message(&mut self) -> Result<Option<codex_codes::JsonRpcMessage>, HarnessError> {
        read_json_rpc_message(&mut self.reader).await
    }

    fn map_notification(
        notification: codex_codes::JsonRpcNotification,
    ) -> Result<codex_codes::ServerMessage, HarnessError> {
        codex_codes::Notification::from_envelope(&notification.method, notification.params)
            .map(codex_codes::ServerMessage::Notification)
            .map_err(|error| HarnessError::Protocol(error.to_string()))
    }

    fn map_request(
        request: codex_codes::JsonRpcRequest,
    ) -> Result<codex_codes::ServerMessage, HarnessError> {
        codex_codes::ServerRequest::from_envelope(&request.method, request.params)
            .map(|request_body| codex_codes::ServerMessage::Request {
                id: request.id,
                request: request_body,
            })
            .map_err(|error| HarnessError::Protocol(error.to_string()))
    }
}

#[async_trait]
impl CodexTransport for CodexStdioTransport {
    async fn request_json(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, HarnessError> {
        let id = codex_codes::RequestId::Integer(self.next_id);
        self.next_id = self.next_id.saturating_add(1);
        self.send_raw(&codex_codes::JsonRpcRequest {
            id: id.clone(),
            method: method.to_owned(),
            params: Some(params),
        })
        .await?;

        loop {
            let message = self.read_message().await?.ok_or_else(|| {
                HarnessError::Transport("Codex app-server closed before responding".into())
            })?;
            match message {
                codex_codes::JsonRpcMessage::Response(response) if response.id == id => {
                    return Ok(response.result);
                }
                codex_codes::JsonRpcMessage::Error(error) if error.id == id => {
                    return Err(HarnessError::Transport(format!(
                        "JSON-RPC error ({}): {}",
                        error.error.code, error.error.message
                    )));
                }
                codex_codes::JsonRpcMessage::Notification(notification) => self
                    .buffered
                    .push_back(Self::map_notification(notification)?),
                codex_codes::JsonRpcMessage::Request(request) => {
                    self.buffered.push_back(Self::map_request(request)?)
                }
                codex_codes::JsonRpcMessage::Response(response) => warn!(
                    response_id = %response.id,
                    expected_request_id = %id,
                    "ignoring unexpected Codex JSON-RPC response"
                ),
                codex_codes::JsonRpcMessage::Error(error) => warn!(
                    response_id = %error.id,
                    expected_request_id = %id,
                    code = error.error.code,
                    message = %error.error.message,
                    "ignoring unexpected Codex JSON-RPC error response"
                ),
            }
        }
    }

    async fn next_message(&mut self) -> Result<Option<codex_codes::ServerMessage>, HarnessError> {
        if let Some(message) = self.buffered.pop_front() {
            return Ok(Some(message));
        }
        loop {
            match self.read_message().await? {
                Some(codex_codes::JsonRpcMessage::Notification(notification)) => {
                    return Self::map_notification(notification).map(Some);
                }
                Some(codex_codes::JsonRpcMessage::Request(request)) => {
                    return Self::map_request(request).map(Some);
                }
                Some(codex_codes::JsonRpcMessage::Response(response)) => warn!(
                    response_id = %response.id,
                    "ignoring unexpected Codex JSON-RPC response without a pending request"
                ),
                Some(codex_codes::JsonRpcMessage::Error(error)) => warn!(
                    response_id = %error.id,
                    code = error.error.code,
                    message = %error.error.message,
                    "ignoring unexpected Codex JSON-RPC error without a pending request"
                ),
                None => return Ok(None),
            }
        }
    }

    async fn respond_json(
        &mut self,
        id: codex_codes::RequestId,
        value: serde_json::Value,
    ) -> Result<(), HarnessError> {
        self.send_raw(&codex_codes::JsonRpcResponse { id, result: value })
            .await
    }

    async fn respond_error_json(
        &mut self,
        id: codex_codes::RequestId,
        code: i64,
        message: &str,
    ) -> Result<(), HarnessError> {
        self.send_raw(&codex_codes::JsonRpcError {
            id,
            error: codex_codes::JsonRpcErrorData {
                code,
                message: message.to_owned(),
                data: None,
            },
        })
        .await
    }

    async fn shutdown_transport(mut self) -> Result<(), HarnessError> {
        self.child
            .kill()
            .await
            .map_err(|error| HarnessError::Transport(error.to_string()))
    }
}

impl Drop for CodexStdioTransport {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        self.stderr_drain.abort();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    use super::read_json_rpc_message;

    #[tokio::test]
    async fn canceling_a_partial_read_preserves_the_json_rpc_line() {
        let (reader, mut writer) = tokio::io::duplex(1024);
        let mut reader = BufReader::new(reader).lines();
        writer.write_all(b"{\"method\":\"test").await.unwrap();

        let mut partial_read = Box::pin(read_json_rpc_message(&mut reader));
        tokio::select! {
            result = &mut partial_read => panic!("partial line completed unexpectedly: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        drop(partial_read);

        writer.write_all(b"\",\"params\":null}\n").await.unwrap();
        let message = read_json_rpc_message(&mut reader).await.unwrap().unwrap();
        let codex_codes::JsonRpcMessage::Notification(notification) = message else {
            panic!("expected notification");
        };
        assert_eq!(notification.method, "test");
    }
}
