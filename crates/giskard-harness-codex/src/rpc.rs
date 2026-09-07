use super::*;

#[derive(Debug)]
pub(crate) enum CodexStreamError {
    /// A non-JSON line was consumed from app-server stdout. Since JSON-RPC is
    /// newline-delimited, the next read starts at a fresh frame boundary.
    NonJsonStdout {
        parse_error: String,
        raw_preview: String,
        raw_bytes: usize,
    },
    Fatal(HarnessError),
}

pub(crate) const NON_JSON_STDOUT_PREVIEW_BYTES: usize = 4 * 1024;

pub(crate) fn bounded_utf8_preview(value: &str, max_bytes: usize) -> String {
    let mut end = value.len().min(max_bytes);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

pub(crate) async fn codex_request<P, R>(
    client: &mut dyn CodexTransport,
    context: CodexOperationContext<'_>,
    method: &str,
    params: &P,
) -> Result<R, HarnessError>
where
    P: Serialize + Sync,
    R: DeserializeOwned,
{
    let params = serde_json::to_value(params).map_err(|e| HarnessError::Protocol(e.to_string()))?;
    let started = Instant::now();
    let response =
        tokio::time::timeout(CODEX_JSON_RPC_TIMEOUT, client.request_json(method, params))
            .await
            .map_err(|_| {
                context.log_timeout(
                    Some(method),
                    started.elapsed(),
                    "Codex JSON-RPC request timed out; worker will resume processing commands",
                );
                HarnessError::Timeout(format!("Codex JSON-RPC request {method} timed out"))
            })??;
    serde_json::from_value(response).map_err(|e| HarnessError::Protocol(e.to_string()))
}

pub(crate) async fn codex_respond_json(
    client: &mut dyn CodexTransport,
    context: CodexOperationContext<'_>,
    id: codex_codes::jsonrpc::RequestId,
    value: serde_json::Value,
) -> Result<(), HarnessError> {
    let started = Instant::now();
    let id_for_log = id.clone();
    tokio::time::timeout(CODEX_JSON_RPC_TIMEOUT, client.respond_json(id, value))
        .await
        .map_err(|_| {
            context.with_request_id(&id_for_log).log_timeout(
                None,
                started.elapsed(),
                "Codex JSON-RPC response timed out; worker will resume processing commands",
            );
            HarnessError::Timeout(format!("Codex JSON-RPC response {id_for_log} timed out"))
        })?
}

pub(crate) async fn codex_respond_error_json(
    client: &mut dyn CodexTransport,
    context: CodexOperationContext<'_>,
    id: codex_codes::jsonrpc::RequestId,
    code: i64,
    message: &str,
) -> Result<(), HarnessError> {
    let started = Instant::now();
    let id_for_log = id.clone();
    tokio::time::timeout(
        CODEX_JSON_RPC_TIMEOUT,
        client.respond_error_json(id, code, message),
    )
    .await
    .map_err(|_| {
        context.with_request_id(&id_for_log).log_timeout(
            None,
            started.elapsed(),
            "Codex JSON-RPC error response timed out; worker will resume processing commands",
        );
        HarnessError::Timeout(format!(
            "Codex JSON-RPC error response {id_for_log} timed out"
        ))
    })?
}
