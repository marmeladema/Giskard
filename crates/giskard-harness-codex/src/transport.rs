use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard};
use std::time::Duration;

use codex_codes::jsonrpc::{
    JsonRpcError, JsonRpcMessage, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, RequestId,
};
use codex_codes::messages::{Notification, ServerMessage, ServerRequest};
use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::Child;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tracing::{debug, warn};

use giskard_core::error::HarnessError;

use crate::{CodexStreamError, NON_JSON_STDOUT_PREVIEW_BYTES, bounded_utf8_preview};

const WRITE_CAPACITY: usize = 64;
const CONTROL_WRITE_CAPACITY: usize = 32;
const MESSAGE_CAPACITY: usize = 1;
const MAX_IN_FLIGHT_REQUESTS: usize = 64;
const MAX_IN_FLIGHT_CONTROL_REQUESTS: usize = 8;
const STDOUT_BUFFER_SIZE: usize = 10 * 1024 * 1024;
#[cfg(not(test))]
const WRITER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
#[cfg(test)]
const WRITER_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(25);
#[cfg(not(test))]
const CHILD_EXIT_TIMEOUT: Duration = Duration::from_secs(2);
#[cfg(test)]
const CHILD_EXIT_TIMEOUT: Duration = Duration::from_millis(250);
#[cfg(not(test))]
pub(crate) const FORCED_KILL_REAP_RESERVE: Duration = Duration::from_secs(1);
#[cfg(test)]
pub(crate) const FORCED_KILL_REAP_RESERVE: Duration = Duration::from_millis(10);

pub(crate) type SuccessResponseHook = Box<
    dyn FnOnce(Value) -> Pin<Box<dyn Future<Output = Result<Value, HarnessError>> + Send>> + Send,
>;
pub(crate) type AbandonedErrorHook =
    Box<dyn FnOnce(HarnessError) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

struct PendingRequest {
    result: oneshot::Sender<ResponseDelivery>,
    success_hook: Option<SuccessResponseHook>,
    abandoned_error_hook: Option<AbandonedErrorHook>,
    _permit: OwnedSemaphorePermit,
}

type Pending = Arc<StdMutex<HashMap<RequestId, PendingRequest>>>;

pub(crate) struct PendingResponse {
    result: oneshot::Receiver<ResponseDelivery>,
    _cancellation: Option<PendingCorrelationCancellation>,
}

impl PendingResponse {
    pub(crate) async fn receive(self, method: &str) -> Result<Value, HarnessError> {
        self.result
            .await
            .map_err(|_| {
                HarnessError::Transport(format!("Codex response correlation dropped for {method}"))
            })?
            .consume()
    }
}

struct ResponseDelivery {
    result: Option<Result<Value, HarnessError>>,
    abandoned_error_hook: Option<AbandonedErrorHook>,
}

impl ResponseDelivery {
    fn new(
        result: Result<Value, HarnessError>,
        abandoned_error_hook: Option<AbandonedErrorHook>,
    ) -> Self {
        Self {
            result: Some(result),
            abandoned_error_hook,
        }
    }

    fn consume(mut self) -> Result<Value, HarnessError> {
        self.abandoned_error_hook.take();
        match self.result.take() {
            Some(result) => result,
            None => Err(HarnessError::Transport(
                "Codex response delivery was consumed twice".into(),
            )),
        }
    }
}

impl Drop for ResponseDelivery {
    fn drop(&mut self) {
        let Some(Err(error)) = self.result.as_ref() else {
            return;
        };
        let Some(hook) = self.abandoned_error_hook.take() else {
            return;
        };
        let error = error.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(hook(error));
            }
            Err(runtime_error) => warn!(
                %runtime_error,
                %error,
                "could not run abandoned Codex response error hook outside a Tokio runtime"
            ),
        }
    }
}

struct PendingCorrelationCancellation {
    pending: Pending,
    id: RequestId,
}

impl Drop for PendingCorrelationCancellation {
    fn drop(&mut self) {
        match self.pending.lock() {
            Ok(mut pending) => {
                pending.remove(&self.id);
            }
            Err(_) => warn!(
                request_id = %self.id,
                "could not cancel Codex response correlation because its lock was poisoned"
            ),
        }
    }
}

/// Whether a response correlation belongs to the waiting caller or must survive it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CorrelationLifetime {
    /// Dropping or timing out the caller removes the correlation and restores its permit.
    CallerBound,
    /// The reader retains the correlation so a late response can commit protocol state.
    Retained,
}

struct Admission {
    accepting: bool,
}

pub(crate) struct ProductionFrame {
    message: ServerMessage,
    acknowledgement: ProductionFrameAck,
}

impl ProductionFrame {
    fn gated(
        message: ServerMessage,
        acknowledgement: oneshot::Sender<Result<(), HarnessError>>,
    ) -> Self {
        Self {
            message,
            acknowledgement: ProductionFrameAck(Some(acknowledgement)),
        }
    }

    /// Construct a frame that does not gate a real stdout reader, for harness implementations and
    /// test transports that already own their delivery ordering.
    #[cfg(test)]
    pub(crate) fn ungated(message: ServerMessage) -> Self {
        Self {
            message,
            acknowledgement: ProductionFrameAck(None),
        }
    }

    pub(crate) fn into_parts(self) -> (ServerMessage, ProductionFrameAck) {
        (self.message, self.acknowledgement)
    }
}

pub(crate) struct ProductionFrameAck(Option<oneshot::Sender<Result<(), HarnessError>>>);

impl ProductionFrameAck {
    pub(crate) fn acknowledge(mut self, result: Result<(), HarnessError>) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(result);
        }
    }
}

type IncomingFrame = Result<Option<ProductionFrame>, CodexStreamError>;

enum Outbound {
    Value(Value),
    AcknowledgedValue {
        value: Value,
        written: oneshot::Sender<Result<(), HarnessError>>,
    },
    Shutdown(oneshot::Sender<Result<(), HarnessError>>),
}

#[derive(Clone)]
pub(crate) struct DispatchClient {
    normal_tx: mpsc::Sender<Outbound>,
    control_tx: mpsc::Sender<Outbound>,
    pending: Pending,
    in_flight: Arc<Semaphore>,
    control_in_flight: Arc<Semaphore>,
    admission: Arc<StdMutex<Admission>>,
    next_id: Arc<AtomicI64>,
    child: Arc<Mutex<Child>>,
    stopped: Arc<AtomicBool>,
}

/// The sole consumer of production frames read from Codex stdout.
///
/// Deliberately neither `Clone` nor internally shared: request/write handles may be cloned, but
/// stdout ownership is transferred exactly once to the inbound dispatcher.
pub(crate) struct ProductionFrameReceiver {
    messages: mpsc::Receiver<IncomingFrame>,
}

impl ProductionFrameReceiver {
    pub(crate) async fn next_message(&mut self) -> IncomingFrame {
        self.messages.recv().await.unwrap_or_else(|| {
            Err(CodexStreamError::Fatal(HarnessError::Transport(
                "Codex stdout dispatcher closed".into(),
            )))
        })
    }
}

impl DispatchClient {
    pub(crate) async fn spawn(
        builder: codex_codes::AppServerBuilder,
    ) -> Result<(Self, ProductionFrameReceiver), HarnessError> {
        let mut command = builder
            .build_command()
            .map_err(|error| HarnessError::Spawn(error.to_string()))?;
        // Explicit shutdown owns the normal bounded kill/reap path. This last-owner guard covers
        // construction and initialization failures before a worker exists to call it.
        command.kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|error| HarnessError::Spawn(error.to_string()))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| HarnessError::Spawn("Codex app-server stdin was not piped".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| HarnessError::Spawn("Codex app-server stdout was not piped".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| HarnessError::Spawn("Codex app-server stderr was not piped".into()))?;

        let (normal_tx, normal_rx) = mpsc::channel(WRITE_CAPACITY);
        let (control_tx, control_rx) = mpsc::channel(CONTROL_WRITE_CAPACITY);
        let (message_tx, messages) = mpsc::channel(MESSAGE_CAPACITY);
        let pending = Arc::new(StdMutex::new(HashMap::new()));
        let admission = Arc::new(StdMutex::new(Admission { accepting: true }));

        tokio::spawn(run_writer(
            BufWriter::new(stdin),
            normal_rx,
            control_rx,
            message_tx.clone(),
            pending.clone(),
        ));
        tokio::spawn(run_reader(
            BufReader::with_capacity(STDOUT_BUFFER_SIZE, stdout),
            message_tx,
            pending.clone(),
        ));
        tokio::spawn(drain_stderr(BufReader::new(stderr)));

        let client = Self {
            normal_tx,
            control_tx,
            pending,
            in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUESTS)),
            control_in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT_CONTROL_REQUESTS)),
            admission,
            next_id: Arc::new(AtomicI64::new(1)),
            child: Arc::new(Mutex::new(child)),
            stopped: Arc::new(AtomicBool::new(false)),
        };
        Ok((client, ProductionFrameReceiver { messages }))
    }

    pub(crate) async fn request(&self, method: &str, params: Value) -> Result<Value, HarnessError> {
        self.request_inner(method, params, None, CorrelationLifetime::CallerBound, None)
            .await
    }

    pub(crate) async fn request_with_hook(
        &self,
        method: &str,
        params: Value,
        hook: SuccessResponseHook,
        lifetime: CorrelationLifetime,
        abandoned_error_hook: Option<AbandonedErrorHook>,
    ) -> Result<Value, HarnessError> {
        self.request_inner(method, params, Some(hook), lifetime, abandoned_error_hook)
            .await
    }

    async fn request_inner(
        &self,
        method: &str,
        params: Value,
        success_hook: Option<SuccessResponseHook>,
        lifetime: CorrelationLifetime,
        abandoned_error_hook: Option<AbandonedErrorHook>,
    ) -> Result<Value, HarnessError> {
        let permit = self
            .in_flight
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| HarnessError::Transport("Codex request correlation closed".into()))?;
        let id = RequestId::Integer(self.next_id.fetch_add(1, Ordering::Relaxed));
        let request = JsonRpcRequest {
            id: id.clone(),
            method: method.to_owned(),
            params: Some(params),
        };
        let value = serde_json::to_value(request)
            .map_err(|error| HarnessError::Protocol(error.to_string()))?;
        let outbound = self
            .normal_tx
            .reserve()
            .await
            .map_err(|_| HarnessError::Transport("Codex writer closed".into()))?;
        let (result_tx, result_rx) = oneshot::channel();
        {
            let admission = lock_admission(&self.admission)?;
            if !admission.accepting {
                return Err(HarnessError::Transport(
                    "Codex transport is shutting down".into(),
                ));
            }
            lock_pending(&self.pending)?.insert(
                id.clone(),
                PendingRequest {
                    result: result_tx,
                    success_hook,
                    abandoned_error_hook,
                    _permit: permit,
                },
            );
            outbound.send(Outbound::Value(value));
        }
        let cancellation = (lifetime == CorrelationLifetime::CallerBound).then(|| {
            PendingCorrelationCancellation {
                pending: self.pending.clone(),
                id: id.clone(),
            }
        });
        PendingResponse {
            result: result_rx,
            _cancellation: cancellation,
        }
        .receive(method)
        .await
    }

    /// Register an interrupt-class request without waiting for either bounded capacity or its
    /// response. This keeps the urgent harness dispatcher available for protocol replies while a
    /// response is held behind stdout backpressure.
    pub(crate) fn submit_control_request(
        &self,
        method: &str,
        params: Value,
    ) -> Result<PendingResponse, HarnessError> {
        let permit = self
            .control_in_flight
            .clone()
            .try_acquire_owned()
            .map_err(|error| {
                HarnessError::Transport(format!(
                    "Codex control request correlation is unavailable: {error}"
                ))
            })?;
        let id = RequestId::Integer(self.next_id.fetch_add(1, Ordering::Relaxed));
        let request = JsonRpcRequest {
            id: id.clone(),
            method: method.to_owned(),
            params: Some(params),
        };
        let value = serde_json::to_value(request)
            .map_err(|error| HarnessError::Protocol(error.to_string()))?;
        let outbound = self.control_tx.try_reserve().map_err(|error| {
            HarnessError::Transport(format!("Codex control writer is unavailable: {error}"))
        })?;
        let (result_tx, result_rx) = oneshot::channel();
        {
            let admission = lock_admission(&self.admission)?;
            if !admission.accepting {
                return Err(HarnessError::Transport(
                    "Codex transport is shutting down".into(),
                ));
            }
            lock_pending(&self.pending)?.insert(
                id.clone(),
                PendingRequest {
                    result: result_tx,
                    success_hook: None,
                    abandoned_error_hook: None,
                    _permit: permit,
                },
            );
            outbound.send(Outbound::Value(value));
        }
        Ok(PendingResponse {
            result: result_rx,
            _cancellation: Some(PendingCorrelationCancellation {
                pending: self.pending.clone(),
                id,
            }),
        })
    }

    pub(crate) async fn respond(&self, id: RequestId, result: Value) -> Result<(), HarnessError> {
        self.send_control(
            serde_json::to_value(JsonRpcResponse { id, result })
                .map_err(|error| HarnessError::Protocol(error.to_string()))?,
        )
        .await
    }

    pub(crate) async fn respond_error(
        &self,
        id: RequestId,
        code: i64,
        message: &str,
    ) -> Result<(), HarnessError> {
        self.send_control(serde_json::json!({
            "id": id,
            "error": { "code": code, "message": message }
        }))
        .await
    }

    async fn send_control(&self, value: Value) -> Result<(), HarnessError> {
        let (written_tx, written_rx) = oneshot::channel();
        self.control_tx
            .send(Outbound::AcknowledgedValue {
                value,
                written: written_tx,
            })
            .await
            .map_err(|_| HarnessError::Transport("Codex control writer closed".into()))?;
        written_rx.await.map_err(|_| {
            HarnessError::Transport("Codex control write acknowledgement dropped".into())
        })?
    }

    pub(crate) async fn shutdown(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), HarnessError> {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        lock_admission(&self.admission)?.accepting = false;
        self.in_flight.close();
        self.control_in_flight.close();

        let graceful_writer_shutdown = async {
            let (finished_tx, finished_rx) = oneshot::channel();
            self.control_tx
                .send(Outbound::Shutdown(finished_tx))
                .await
                .map_err(|_| HarnessError::Transport("Codex control writer closed".into()))?;
            finished_rx.await.map_err(|_| {
                HarnessError::Transport("Codex writer shutdown acknowledgement dropped".into())
            })?
        };
        let graceful_deadline = deadline
            .checked_sub(FORCED_KILL_REAP_RESERVE)
            .unwrap_or(deadline);
        let writer_deadline = std::cmp::min(
            graceful_deadline,
            tokio::time::Instant::now() + WRITER_SHUTDOWN_TIMEOUT,
        );
        let stdin_closed =
            match tokio::time::timeout_at(writer_deadline, graceful_writer_shutdown).await {
                Ok(Ok(())) => true,
                Ok(Err(error)) => {
                    warn!(%error, "Codex writer did not shut down gracefully");
                    false
                }
                Err(_) => {
                    warn!(
                        timeout_ms = WRITER_SHUTDOWN_TIMEOUT.as_millis(),
                        "Codex writer shutdown timed out; killing the child process"
                    );
                    false
                }
            };

        let mut child = tokio::time::timeout_at(deadline, self.child.lock())
            .await
            .map_err(|_| {
                HarnessError::Timeout(
                    "Codex child-process lock remained held through the shutdown deadline".into(),
                )
            })?;
        if stdin_closed {
            let natural_exit_deadline = std::cmp::min(
                graceful_deadline,
                tokio::time::Instant::now() + CHILD_EXIT_TIMEOUT,
            );
            match tokio::time::timeout_at(natural_exit_deadline, child.wait()).await {
                Ok(Ok(_)) => return Ok(()),
                Ok(Err(error)) => warn!(
                    %error,
                    "waiting for Codex after stdin closed failed; killing the child process"
                ),
                Err(_) => warn!(
                    timeout_ms = CHILD_EXIT_TIMEOUT.as_millis(),
                    "Codex did not exit after stdin closed; killing the child process"
                ),
            }
        }

        // This handle is independent of both bounded write queues. Queue saturation can delay the
        // graceful attempt only until the deadline above, never the fallback kill.
        child
            .start_kill()
            .map_err(|error| HarnessError::Transport(error.to_string()))?;
        tokio::time::timeout_at(deadline, child.wait())
            .await
            .map_err(|_| {
                HarnessError::Timeout(
                    "Codex child process did not reap before the shutdown deadline".into(),
                )
            })?
            .map_err(|error| HarnessError::Transport(error.to_string()))?;
        Ok(())
    }
}

fn lock_pending(
    pending: &Pending,
) -> Result<MutexGuard<'_, HashMap<RequestId, PendingRequest>>, HarnessError> {
    pending
        .lock()
        .map_err(|_| HarnessError::Transport("Codex response-correlation lock was poisoned".into()))
}

fn lock_admission(
    admission: &StdMutex<Admission>,
) -> Result<MutexGuard<'_, Admission>, HarnessError> {
    admission
        .lock()
        .map_err(|_| HarnessError::Transport("Codex transport admission lock was poisoned".into()))
}

async fn run_writer<W>(
    mut writer: BufWriter<W>,
    mut normal_rx: mpsc::Receiver<Outbound>,
    mut control_rx: mpsc::Receiver<Outbound>,
    message_tx: mpsc::Sender<IncomingFrame>,
    pending: Pending,
) where
    W: AsyncWrite + Unpin,
{
    loop {
        let outbound = tokio::select! {
            biased;
            control = control_rx.recv() => control,
            normal = normal_rx.recv() => normal,
        };
        let Some(outbound) = outbound else { break };
        match outbound {
            Outbound::Value(value) => {
                if let Err(error) = write_value(&mut writer, value).await {
                    let error = fail_pending_for_fatal(&pending, error);
                    let _ = message_tx.send(Err(CodexStreamError::Fatal(error))).await;
                    break;
                }
            }
            Outbound::AcknowledgedValue { value, written } => {
                let result = write_value(&mut writer, value).await;
                let _ = written.send(result.clone());
                if let Err(error) = result {
                    let error = fail_pending_for_fatal(&pending, error);
                    let _ = message_tx.send(Err(CodexStreamError::Fatal(error))).await;
                    break;
                }
            }
            Outbound::Shutdown(finished) => {
                normal_rx.close();
                control_rx.close();
                let result = drain_writer(&mut writer, &mut control_rx, &mut normal_rx).await;
                let _ = finished.send(result.clone());
                if let Err(error) = result {
                    let error = fail_pending_for_fatal(&pending, error);
                    let _ = message_tx.send(Err(CodexStreamError::Fatal(error))).await;
                }
                break;
            }
        }
    }
}

async fn write_value<W>(writer: &mut BufWriter<W>, value: Value) -> Result<(), HarnessError>
where
    W: AsyncWrite + Unpin,
{
    let mut bytes =
        serde_json::to_vec(&value).map_err(|error| HarnessError::Protocol(error.to_string()))?;
    bytes.push(b'\n');
    writer
        .write_all(&bytes)
        .await
        .map_err(|error| HarnessError::Transport(error.to_string()))?;
    writer
        .flush()
        .await
        .map_err(|error| HarnessError::Transport(error.to_string()))
}

async fn drain_writer<W>(
    writer: &mut BufWriter<W>,
    control_rx: &mut mpsc::Receiver<Outbound>,
    normal_rx: &mut mpsc::Receiver<Outbound>,
) -> Result<(), HarnessError>
where
    W: AsyncWrite + Unpin,
{
    while let Some(outbound) = control_rx.recv().await {
        match outbound {
            Outbound::Value(value) => write_value(writer, value).await?,
            Outbound::AcknowledgedValue { value, written } => {
                let result = write_value(writer, value).await;
                let _ = written.send(result.clone());
                result?;
            }
            Outbound::Shutdown(_) => {}
        }
    }
    while let Some(outbound) = normal_rx.recv().await {
        match outbound {
            Outbound::Value(value) => write_value(writer, value).await?,
            Outbound::AcknowledgedValue { value, written } => {
                let result = write_value(writer, value).await;
                let _ = written.send(result.clone());
                result?;
            }
            Outbound::Shutdown(_) => {}
        }
    }
    writer
        .shutdown()
        .await
        .map_err(|error| HarnessError::Transport(error.to_string()))
}

async fn run_reader<R>(mut reader: R, message_tx: mpsc::Sender<IncomingFrame>, pending: Pending)
where
    R: AsyncBufRead + Unpin,
{
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                match fail_pending(&pending, "Codex stdout closed".into()) {
                    Ok(()) => {
                        let _ = message_tx.send(Ok(None)).await;
                    }
                    Err(error) => {
                        let _ = message_tx.send(Err(CodexStreamError::Fatal(error))).await;
                    }
                }
                break;
            }
            Ok(_) => {}
            Err(error) => {
                let error = HarnessError::Transport(error.to_string());
                let error = fail_pending_for_fatal(&pending, error);
                let _ = message_tx.send(Err(CodexStreamError::Fatal(error))).await;
                break;
            }
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        let message = match serde_json::from_str::<JsonRpcMessage>(trimmed) {
            Ok(message) => message,
            Err(error) if !trimmed.trim_start().starts_with('{') => {
                let stream_error = CodexStreamError::NonJsonStdout {
                    parse_error: error.to_string(),
                    raw_preview: bounded_utf8_preview(trimmed, NON_JSON_STDOUT_PREVIEW_BYTES),
                    raw_bytes: trimmed.len(),
                };
                if message_tx.send(Err(stream_error)).await.is_err() {
                    break;
                }
                continue;
            }
            Err(error) => {
                let error = HarnessError::Transport(format!(
                    "Codex JSON-RPC deserialization error: {error} (raw_bytes: {}, raw_preview: {:?})",
                    trimmed.len(),
                    bounded_utf8_preview(trimmed, NON_JSON_STDOUT_PREVIEW_BYTES)
                ));
                let error = fail_pending_for_fatal(&pending, error);
                let _ = message_tx.send(Err(CodexStreamError::Fatal(error))).await;
                break;
            }
        };
        match message {
            JsonRpcMessage::Response(response) => {
                if let Err(error) =
                    complete_success_response(&pending, response.id, response.result).await
                {
                    let error = fail_pending_for_fatal(&pending, error);
                    let _ = message_tx.send(Err(CodexStreamError::Fatal(error))).await;
                    break;
                }
            }
            JsonRpcMessage::Error(error) => {
                if let Err(error) = complete_jsonrpc_error(&pending, error) {
                    let error = fail_pending_for_fatal(&pending, error);
                    let _ = message_tx.send(Err(CodexStreamError::Fatal(error))).await;
                    break;
                }
            }
            JsonRpcMessage::Notification(JsonRpcNotification { method, params }) => {
                match Notification::from_envelope(&method, params) {
                    Ok(notification) => {
                        if let Err(error) = deliver_production_frame(
                            ServerMessage::Notification(notification),
                            &message_tx,
                            &pending,
                        )
                        .await
                        {
                            let _ = message_tx.send(Err(CodexStreamError::Fatal(error))).await;
                            break;
                        }
                    }
                    Err(error) => {
                        let error = HarnessError::Transport(format!(
                            "Codex notification {method} could not be decoded: {error}"
                        ));
                        let error = fail_pending_for_fatal(&pending, error);
                        let _ = message_tx.send(Err(CodexStreamError::Fatal(error))).await;
                        break;
                    }
                }
            }
            JsonRpcMessage::Request(JsonRpcRequest { id, method, params }) => {
                match ServerRequest::from_envelope(&method, params) {
                    Ok(request) => {
                        if let Err(error) = deliver_production_frame(
                            ServerMessage::Request { id, request },
                            &message_tx,
                            &pending,
                        )
                        .await
                        {
                            let _ = message_tx.send(Err(CodexStreamError::Fatal(error))).await;
                            break;
                        }
                    }
                    Err(error) => {
                        let error = HarnessError::Transport(format!(
                            "Codex server request {method} could not be decoded: {error}"
                        ));
                        let error = fail_pending_for_fatal(&pending, error);
                        let _ = message_tx.send(Err(CodexStreamError::Fatal(error))).await;
                        break;
                    }
                }
            }
        }
    }
}

async fn deliver_production_frame(
    message: ServerMessage,
    message_tx: &mpsc::Sender<IncomingFrame>,
    pending: &Pending,
) -> Result<(), HarnessError> {
    let (acknowledgement_tx, acknowledgement_rx) = oneshot::channel();
    if message_tx
        .send(Ok(Some(ProductionFrame::gated(
            message,
            acknowledgement_tx,
        ))))
        .await
        .is_err()
    {
        let error = HarnessError::Transport("Codex production message receiver closed".into());
        return Err(fail_pending_for_fatal(pending, error));
    }

    let acknowledgement = match acknowledgement_rx.await {
        Ok(result) => result,
        Err(_) => Err(HarnessError::Transport(
            "Codex production frame acknowledgement dropped".into(),
        )),
    };
    match acknowledgement {
        Ok(()) => Ok(()),
        Err(error) => Err(fail_pending_for_fatal(pending, error)),
    }
}

fn complete_jsonrpc_error(pending: &Pending, error: JsonRpcError) -> Result<(), HarnessError> {
    complete_pending(
        pending,
        error.id,
        Err(HarnessError::ProviderRejected {
            code: error.error.code,
            message: error.error.message,
        }),
    )
}

async fn complete_success_response(
    pending: &Pending,
    id: RequestId,
    value: Value,
) -> Result<(), HarnessError> {
    let Some(pending_request) = lock_pending(pending)?.remove(&id) else {
        warn!(request_id = %id, "Codex returned a response for an unknown request");
        return Ok(());
    };

    let PendingRequest {
        result: result_sender,
        success_hook,
        abandoned_error_hook,
        _permit,
    } = pending_request;
    let result = match success_hook {
        Some(hook) => hook(value).await,
        None => Ok(value),
    };
    let hook_error = result.as_ref().err().cloned();
    let _ = result_sender.send(ResponseDelivery::new(result, abandoned_error_hook));
    hook_error.map_or(Ok(()), Err)
}

fn complete_pending(
    pending: &Pending,
    id: RequestId,
    result: Result<Value, HarnessError>,
) -> Result<(), HarnessError> {
    if let Some(pending) = lock_pending(pending)?.remove(&id) {
        let _ = pending
            .result
            .send(ResponseDelivery::new(result, pending.abandoned_error_hook));
    } else {
        warn!(request_id = %id, "Codex returned a response for an unknown request");
    }
    Ok(())
}

fn fail_pending(pending: &Pending, reason: String) -> Result<(), HarnessError> {
    let requests = std::mem::take(&mut *lock_pending(pending)?);
    for (_, pending) in requests {
        let _ = pending.result.send(ResponseDelivery::new(
            Err(HarnessError::Transport(reason.clone())),
            pending.abandoned_error_hook,
        ));
    }
    Ok(())
}

fn fail_pending_for_fatal(pending: &Pending, error: HarnessError) -> HarnessError {
    match fail_pending(pending, error.to_string()) {
        Ok(()) => error,
        Err(lock_error) => lock_error,
    }
}

async fn drain_stderr(mut stderr: BufReader<tokio::process::ChildStderr>) {
    let mut line = String::new();
    loop {
        line.clear();
        match stderr.read_line(&mut line).await {
            Ok(0) => break,
            Ok(_) => debug!(target: "codex_app_server", message = line.trim_end()),
            Err(error) => {
                warn!(%error, "failed to drain Codex app-server stderr");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Stdio;
    use tokio::io::{AsyncReadExt, duplex};
    use tokio::process::Command;

    fn lock_test_pending(pending: &Pending) -> MutexGuard<'_, HashMap<RequestId, PendingRequest>> {
        lock_pending(pending).expect("test response-correlation lock should not be poisoned")
    }

    fn poisoned_pending() -> Pending {
        let pending: Pending = Arc::new(StdMutex::new(HashMap::new()));
        let pending_to_poison = pending.clone();
        let poisoning = std::thread::spawn(move || {
            let _guard = pending_to_poison
                .lock()
                .expect("fresh test response-correlation lock should be available");
            panic!("poison test response-correlation lock");
        });
        assert!(
            poisoning.join().is_err(),
            "test thread should poison the response-correlation lock"
        );
        pending
    }

    async fn caller_lifetime_test_client() -> (DispatchClient, mpsc::Receiver<Outbound>) {
        let (normal_tx, normal_rx) = mpsc::channel(1);
        let (control_tx, _control_rx) = mpsc::channel(1);
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("exit 0")
            .spawn()
            .expect("test child should start");
        child.wait().await.expect("test child should exit");
        (
            DispatchClient {
                normal_tx,
                control_tx,
                pending: Arc::new(StdMutex::new(HashMap::new())),
                in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUESTS)),
                control_in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT_CONTROL_REQUESTS)),
                admission: Arc::new(StdMutex::new(Admission { accepting: true })),
                next_id: Arc::new(AtomicI64::new(1)),
                child: Arc::new(Mutex::new(child)),
                stopped: Arc::new(AtomicBool::new(false)),
            },
            normal_rx,
        )
    }

    #[test]
    fn response_correlation_lock_poisoning_is_an_error() {
        let pending = poisoned_pending();

        assert!(matches!(
            lock_pending(&pending),
            Err(HarnessError::Transport(message))
                if message == "Codex response-correlation lock was poisoned"
        ));
    }

    #[tokio::test]
    async fn reader_reports_response_correlation_lock_poisoning_as_fatal() {
        let (wire, stdout) = duplex(64);
        let (message_tx, mut messages) = mpsc::channel(1);
        let pending = poisoned_pending();
        drop(wire);

        run_reader(BufReader::new(stdout), message_tx, pending).await;

        assert!(matches!(
            messages.recv().await,
            Some(Err(CodexStreamError::Fatal(HarnessError::Transport(message))))
                if message == "Codex response-correlation lock was poisoned"
        ));
    }

    #[tokio::test]
    async fn cancelled_caller_bound_request_releases_correlation() {
        let (client, mut writes) = caller_lifetime_test_client().await;
        let request_client = client.clone();
        let request = tokio::spawn(async move {
            request_client
                .request("model/list", serde_json::json!({}))
                .await
        });
        let Some(Outbound::Value(value)) = writes.recv().await else {
            panic!("request should be written");
        };
        let id: RequestId = serde_json::from_value(value["id"].clone()).unwrap();
        assert_eq!(lock_test_pending(&client.pending).len(), 1);

        request.abort();
        let _ = request.await;
        assert!(lock_test_pending(&client.pending).is_empty());
        assert_eq!(client.in_flight.available_permits(), MAX_IN_FLIGHT_REQUESTS);
        complete_pending(&client.pending, id, Ok(serde_json::json!({})))
            .expect("late response for a cancelled request should be harmless");
    }

    #[tokio::test]
    async fn cancelled_retained_hook_runs_on_late_response() {
        let (client, mut writes) = caller_lifetime_test_client().await;
        let hook_ran = Arc::new(AtomicBool::new(false));
        let hook_flag = hook_ran.clone();
        let request_client = client.clone();
        let request = tokio::spawn(async move {
            request_client
                .request_with_hook(
                    "thread/start",
                    serde_json::json!({}),
                    Box::new(move |value| {
                        Box::pin(async move {
                            hook_flag.store(true, Ordering::Release);
                            Ok(value)
                        })
                    }),
                    CorrelationLifetime::Retained,
                    None,
                )
                .await
        });
        let Some(Outbound::Value(value)) = writes.recv().await else {
            panic!("request should be written");
        };
        let id: RequestId = serde_json::from_value(value["id"].clone()).unwrap();
        request.abort();
        let _ = request.await;
        assert_eq!(lock_test_pending(&client.pending).len(), 1);

        complete_success_response(&client.pending, id, serde_json::json!({}))
            .await
            .expect("late retained response should run its hook");
        assert!(hook_ran.load(Ordering::Acquire));
        assert!(lock_test_pending(&client.pending).is_empty());
        assert_eq!(client.in_flight.available_permits(), MAX_IN_FLIGHT_REQUESTS);
    }

    #[tokio::test]
    async fn consumed_retained_error_does_not_run_abandoned_hook() {
        let (client, mut writes) = caller_lifetime_test_client().await;
        let abandoned_ran = Arc::new(AtomicBool::new(false));
        let abandoned_flag = abandoned_ran.clone();
        let request_client = client.clone();
        let request = tokio::spawn(async move {
            request_client
                .request_with_hook(
                    "thread/resume",
                    serde_json::json!({}),
                    Box::new(|value| Box::pin(async move { Ok(value) })),
                    CorrelationLifetime::Retained,
                    Some(Box::new(move |_| {
                        Box::pin(async move {
                            abandoned_flag.store(true, Ordering::Release);
                        })
                    })),
                )
                .await
        });
        let Some(Outbound::Value(value)) = writes.recv().await else {
            panic!("request should be written");
        };
        let error: JsonRpcError = serde_json::from_value(serde_json::json!({
            "id": value["id"],
            "error": { "code": -32600, "message": "no rollout" }
        }))
        .unwrap();

        complete_jsonrpc_error(&client.pending, error).unwrap();
        assert!(matches!(
            request.await.unwrap(),
            Err(HarnessError::ProviderRejected { code: -32600, message })
                if message == "no rollout"
        ));
        tokio::task::yield_now().await;
        assert!(!abandoned_ran.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn abandoned_retained_error_runs_abandoned_hook() {
        let (client, mut writes) = caller_lifetime_test_client().await;
        let (abandoned_tx, abandoned_rx) = oneshot::channel();
        let request_client = client.clone();
        let request = tokio::spawn(async move {
            request_client
                .request_with_hook(
                    "thread/start",
                    serde_json::json!({}),
                    Box::new(|value| Box::pin(async move { Ok(value) })),
                    CorrelationLifetime::Retained,
                    Some(Box::new(move |error| {
                        Box::pin(async move {
                            let _ = abandoned_tx.send(error);
                        })
                    })),
                )
                .await
        });
        let Some(Outbound::Value(value)) = writes.recv().await else {
            panic!("request should be written");
        };
        let id: RequestId = serde_json::from_value(value["id"].clone()).unwrap();
        request.abort();
        let _ = request.await;

        complete_pending(
            &client.pending,
            id,
            Err(HarnessError::ProviderRejected {
                code: -32600,
                message: "start rejected".into(),
            }),
        )
        .unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), abandoned_rx)
            .await
            .expect("abandoned error hook should run")
            .expect("abandoned error hook should send its error");
        assert!(matches!(
            error,
            HarnessError::ProviderRejected { code: -32600, message }
                if message == "start rejected"
        ));
    }

    #[tokio::test]
    async fn abandoned_retained_success_does_not_run_error_hook() {
        let (client, mut writes) = caller_lifetime_test_client().await;
        let abandoned_ran = Arc::new(AtomicBool::new(false));
        let abandoned_flag = abandoned_ran.clone();
        let success_ran = Arc::new(AtomicBool::new(false));
        let success_flag = success_ran.clone();
        let request_client = client.clone();
        let request = tokio::spawn(async move {
            request_client
                .request_with_hook(
                    "thread/start",
                    serde_json::json!({}),
                    Box::new(move |value| {
                        Box::pin(async move {
                            success_flag.store(true, Ordering::Release);
                            Ok(value)
                        })
                    }),
                    CorrelationLifetime::Retained,
                    Some(Box::new(move |_| {
                        Box::pin(async move {
                            abandoned_flag.store(true, Ordering::Release);
                        })
                    })),
                )
                .await
        });
        let Some(Outbound::Value(value)) = writes.recv().await else {
            panic!("request should be written");
        };
        let id: RequestId = serde_json::from_value(value["id"].clone()).unwrap();
        request.abort();
        let _ = request.await;

        complete_success_response(&client.pending, id, serde_json::json!({}))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert!(success_ran.load(Ordering::Acquire));
        assert!(!abandoned_ran.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn writer_reports_response_correlation_lock_poisoning_as_fatal() {
        let (stdin, wire) = duplex(64);
        let (normal_tx, normal_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::channel(1);
        let (message_tx, mut messages) = mpsc::channel(1);
        let pending = poisoned_pending();
        drop(wire);
        normal_tx
            .send(Outbound::Value(serde_json::json!({ "request": true })))
            .await
            .expect("test writer queue should be open");

        run_writer(
            BufWriter::new(stdin),
            normal_rx,
            control_rx,
            message_tx,
            pending,
        )
        .await;

        assert!(matches!(
            messages.recv().await,
            Some(Err(CodexStreamError::Fatal(HarnessError::Transport(message))))
                if message == "Codex response-correlation lock was poisoned"
        ));
    }

    #[tokio::test]
    async fn acknowledged_control_write_reports_flush_failure() {
        let (writer, peer) = duplex(64);
        drop(peer);
        let (_normal_tx, normal_rx) = mpsc::channel(1);
        let (control_tx, control_rx) = mpsc::channel(1);
        let (message_tx, mut messages) = mpsc::channel(1);
        let pending: Pending = Arc::new(StdMutex::new(HashMap::new()));
        tokio::spawn(run_writer(
            BufWriter::new(writer),
            normal_rx,
            control_rx,
            message_tx,
            pending,
        ));
        let (written_tx, written_rx) = oneshot::channel();
        control_tx
            .send(Outbound::AcknowledgedValue {
                value: serde_json::json!({"id": "approval", "result": "accept"}),
                written: written_tx,
            })
            .await
            .unwrap();

        assert!(matches!(
            written_rx.await.unwrap(),
            Err(HarnessError::Transport(_))
        ));
        assert!(matches!(
            messages.recv().await,
            Some(Err(CodexStreamError::Fatal(HarnessError::Transport(_))))
        ));
    }

    #[tokio::test]
    async fn non_json_stdout_is_bounded_and_does_not_stop_the_reader() {
        let (mut wire, stdout) = duplex(16 * 1024);
        let (message_tx, mut messages) = mpsc::channel(1);
        let pending: Pending = Arc::new(StdMutex::new(HashMap::new()));
        tokio::spawn(run_reader(BufReader::new(stdout), message_tx, pending));
        let raw = format!("{}é", "x".repeat(NON_JSON_STDOUT_PREVIEW_BYTES - 1));
        wire
            .write_all(
                format!(
                    "{raw}\n{{\"method\":\"thread/status/changed\",\"params\":{{\"threadId\":\"native-a\",\"status\":{{\"type\":\"idle\"}}}}}}\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();

        assert!(matches!(
            messages.recv().await,
            Some(Err(CodexStreamError::NonJsonStdout {
                raw_preview,
                raw_bytes,
                ..
            })) if raw_preview.len() == NON_JSON_STDOUT_PREVIEW_BYTES - 1
                && raw_bytes == raw.len()
        ));
        let frame = messages.recv().await.unwrap().unwrap().unwrap();
        let (message, acknowledgement) = frame.into_parts();
        assert!(matches!(
            message,
            ServerMessage::Notification(Notification::ThreadStatusChanged(_))
        ));
        acknowledgement.acknowledge(Ok(()));
    }

    #[tokio::test]
    async fn malformed_json_rpc_object_is_fatal() {
        let (mut wire, stdout) = duplex(4096);
        let (message_tx, mut messages) = mpsc::channel(1);
        let pending: Pending = Arc::new(StdMutex::new(HashMap::new()));
        tokio::spawn(run_reader(BufReader::new(stdout), message_tx, pending));
        wire.write_all(b"{\"method\":\"turn/completed\"\n")
            .await
            .unwrap();

        assert!(matches!(
            messages.recv().await,
            Some(Err(CodexStreamError::Fatal(HarnessError::Transport(message))))
                if message.contains("deserialization")
        ));
    }

    #[tokio::test]
    async fn earlier_notification_backpressure_holds_later_response() {
        let (mut wire, stdout) = duplex(4096);
        let (message_tx, mut messages) = mpsc::channel(1);
        let pending: Pending = Arc::new(StdMutex::new(HashMap::new()));
        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        let (result_tx, mut result_rx) = oneshot::channel();
        lock_test_pending(&pending).insert(
            RequestId::Integer(1),
            PendingRequest {
                result: result_tx,
                success_hook: None,
                abandoned_error_hook: None,
                _permit: permit,
            },
        );
        tokio::spawn(run_reader(
            BufReader::new(stdout),
            message_tx,
            pending.clone(),
        ));

        wire
            .write_all(
                b"{\"method\":\"thread/status/changed\",\"params\":{\"threadId\":\"native-a\",\"status\":{\"type\":\"idle\"}}}\n{\"id\":1,\"result\":{\"ok\":true}}\n",
            )
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert!(matches!(
            result_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        let frame = messages.recv().await.unwrap().unwrap().unwrap();
        let (message, acknowledgement) = frame.into_parts();
        assert!(matches!(
            message,
            ServerMessage::Notification(Notification::ThreadStatusChanged(_))
        ));
        assert!(matches!(
            result_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        acknowledgement.acknowledge(Ok(()));
        let result = result_rx.await.unwrap().consume().unwrap();
        assert_eq!(result["ok"], true);
    }

    #[tokio::test]
    async fn reader_stages_only_one_unacknowledged_production_frame() {
        let (mut wire, stdout) = duplex(4096);
        let (message_tx, mut messages) = mpsc::channel(1);
        let pending: Pending = Arc::new(StdMutex::new(HashMap::new()));
        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        let (result_tx, mut result_rx) = oneshot::channel();
        lock_test_pending(&pending).insert(
            RequestId::Integer(1),
            PendingRequest {
                result: result_tx,
                success_hook: None,
                abandoned_error_hook: None,
                _permit: permit,
            },
        );
        tokio::spawn(run_reader(
            BufReader::new(stdout),
            message_tx,
            pending.clone(),
        ));

        wire
            .write_all(
                b"{\"method\":\"thread/status/changed\",\"params\":{\"threadId\":\"native-a\",\"status\":{\"type\":\"idle\"}}}\n{\"method\":\"thread/status/changed\",\"params\":{\"threadId\":\"native-b\",\"status\":{\"type\":\"idle\"}}}\n{\"id\":1,\"result\":{\"ok\":true}}\n",
            )
            .await
            .unwrap();

        let first = messages.recv().await.unwrap().unwrap().unwrap();
        let (first_message, first_acknowledgement) = first.into_parts();
        assert!(matches!(
            first_message,
            ServerMessage::Notification(Notification::ThreadStatusChanged(notification))
                if notification.thread_id == "native-a"
        ));
        assert!(matches!(
            messages.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            result_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        first_acknowledgement.acknowledge(Ok(()));
        let second = messages.recv().await.unwrap().unwrap().unwrap();
        let (second_message, second_acknowledgement) = second.into_parts();
        assert!(matches!(
            second_message,
            ServerMessage::Notification(Notification::ThreadStatusChanged(notification))
                if notification.thread_id == "native-b"
        ));
        assert!(matches!(
            result_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        second_acknowledgement.acknowledge(Ok(()));
        let result = result_rx.await.unwrap().consume().unwrap();
        assert_eq!(result["ok"], true);
    }

    #[tokio::test]
    async fn production_frame_acknowledgement_failure_is_fatal() {
        let (mut wire, stdout) = duplex(4096);
        let (message_tx, mut messages) = mpsc::channel(1);
        let pending: Pending = Arc::new(StdMutex::new(HashMap::new()));
        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        let (result_tx, result_rx) = oneshot::channel();
        lock_test_pending(&pending).insert(
            RequestId::Integer(1),
            PendingRequest {
                result: result_tx,
                success_hook: None,
                abandoned_error_hook: None,
                _permit: permit,
            },
        );
        let reader = tokio::spawn(run_reader(
            BufReader::new(stdout),
            message_tx,
            pending.clone(),
        ));

        wire
            .write_all(
                b"{\"method\":\"thread/status/changed\",\"params\":{\"threadId\":\"native-a\",\"status\":{\"type\":\"idle\"}}}\n{\"id\":1,\"result\":{\"mustNotComplete\":true}}\n",
            )
            .await
            .unwrap();
        let frame = messages.recv().await.unwrap().unwrap().unwrap();
        let (_, acknowledgement) = frame.into_parts();
        acknowledgement.acknowledge(Err(HarnessError::Protocol(
            "listener registration failed".into(),
        )));

        assert!(matches!(
            result_rx.await.unwrap().consume(),
            Err(HarnessError::Transport(message)) if message.contains("listener registration failed")
        ));
        assert!(matches!(
            messages.recv().await,
            Some(Err(CodexStreamError::Fatal(HarnessError::Protocol(message))))
                if message == "listener registration failed"
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), reader)
            .await
            .expect("reader should stop after an acknowledgement failure")
            .unwrap();
    }

    #[tokio::test]
    async fn dropped_production_frame_acknowledgement_is_fatal() {
        let (mut wire, stdout) = duplex(4096);
        let (message_tx, mut messages) = mpsc::channel(1);
        let pending: Pending = Arc::new(StdMutex::new(HashMap::new()));
        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        let (result_tx, result_rx) = oneshot::channel();
        lock_test_pending(&pending).insert(
            RequestId::Integer(1),
            PendingRequest {
                result: result_tx,
                success_hook: None,
                abandoned_error_hook: None,
                _permit: permit,
            },
        );
        let reader = tokio::spawn(run_reader(
            BufReader::new(stdout),
            message_tx,
            pending.clone(),
        ));

        wire
            .write_all(
                b"{\"method\":\"thread/status/changed\",\"params\":{\"threadId\":\"native-a\",\"status\":{\"type\":\"idle\"}}}\n{\"id\":1,\"result\":{\"mustNotComplete\":true}}\n",
            )
            .await
            .unwrap();
        let frame = messages.recv().await.unwrap().unwrap().unwrap();
        let (_, acknowledgement) = frame.into_parts();
        drop(acknowledgement);

        assert!(matches!(
            result_rx.await.unwrap().consume(),
            Err(HarnessError::Transport(message))
                if message.contains("production frame acknowledgement dropped")
        ));
        assert!(matches!(
            messages.recv().await,
            Some(Err(CodexStreamError::Fatal(HarnessError::Transport(message))))
                if message.contains("production frame acknowledgement dropped")
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), reader)
            .await
            .expect("reader should stop after a dropped acknowledgement")
            .unwrap();
    }

    #[tokio::test]
    async fn success_hook_finishes_before_correlation_and_the_next_frame() {
        let (mut wire, stdout) = duplex(4096);
        let (message_tx, _messages) = mpsc::channel(1);
        let pending: Pending = Arc::new(StdMutex::new(HashMap::new()));
        let in_flight = Arc::new(Semaphore::new(2));
        let hook_started = Arc::new(tokio::sync::Notify::new());
        let release_hook = Arc::new(tokio::sync::Notify::new());

        let first_permit = in_flight.clone().acquire_owned().await.unwrap();
        let (first_tx, mut first_rx) = oneshot::channel();
        let pending_for_hook = pending.clone();
        let hook_started_for_hook = hook_started.clone();
        let release_hook_for_hook = release_hook.clone();
        let hook: SuccessResponseHook = Box::new(move |mut value| {
            Box::pin(async move {
                hook_started_for_hook.notify_one();
                release_hook_for_hook.notified().await;
                assert!(
                    !lock_test_pending(&pending_for_hook).contains_key(&RequestId::Integer(1)),
                    "the pending mutex must be released and the request removed before the hook"
                );
                value["hooked"] = Value::Bool(true);
                Ok(value)
            })
        });
        lock_test_pending(&pending).insert(
            RequestId::Integer(1),
            PendingRequest {
                result: first_tx,
                success_hook: Some(hook),
                abandoned_error_hook: None,
                _permit: first_permit,
            },
        );

        let second_permit = in_flight.acquire_owned().await.unwrap();
        let (second_tx, mut second_rx) = oneshot::channel();
        lock_test_pending(&pending).insert(
            RequestId::Integer(2),
            PendingRequest {
                result: second_tx,
                success_hook: None,
                abandoned_error_hook: None,
                _permit: second_permit,
            },
        );

        tokio::spawn(run_reader(
            BufReader::new(stdout),
            message_tx,
            pending.clone(),
        ));
        wire.write_all(
            b"{\"id\":1,\"result\":{\"ok\":true}}\n{\"id\":2,\"result\":{\"next\":true}}\n",
        )
        .await
        .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(1), hook_started.notified())
            .await
            .expect("success hook should start");
        assert!(matches!(
            first_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            second_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        release_hook.notify_one();
        let first = first_rx.await.unwrap().consume().unwrap();
        assert_eq!(first["hooked"], true);
        let second = second_rx.await.unwrap().consume().unwrap();
        assert_eq!(second["next"], true);
    }

    #[tokio::test]
    async fn success_hook_failure_is_fatal_and_fails_every_pending_request() {
        let (mut wire, stdout) = duplex(4096);
        let (message_tx, mut messages) = mpsc::channel(1);
        let pending: Pending = Arc::new(StdMutex::new(HashMap::new()));
        let in_flight = Arc::new(Semaphore::new(2));

        let first_permit = in_flight.clone().acquire_owned().await.unwrap();
        let (first_tx, first_rx) = oneshot::channel();
        let hook: SuccessResponseHook = Box::new(|_| {
            Box::pin(async { Err(HarnessError::Protocol("identity hook failed".into())) })
        });
        lock_test_pending(&pending).insert(
            RequestId::Integer(1),
            PendingRequest {
                result: first_tx,
                success_hook: Some(hook),
                abandoned_error_hook: None,
                _permit: first_permit,
            },
        );

        let second_permit = in_flight.acquire_owned().await.unwrap();
        let (second_tx, second_rx) = oneshot::channel();
        lock_test_pending(&pending).insert(
            RequestId::Integer(2),
            PendingRequest {
                result: second_tx,
                success_hook: None,
                abandoned_error_hook: None,
                _permit: second_permit,
            },
        );

        let reader = tokio::spawn(run_reader(
            BufReader::new(stdout),
            message_tx,
            pending.clone(),
        ));
        wire.write_all(
            b"{\"id\":1,\"result\":{}}\n{\"id\":2,\"result\":{\"mustNotComplete\":true}}\n",
        )
        .await
        .unwrap();

        assert!(matches!(
            first_rx.await.unwrap().consume(),
            Err(HarnessError::Protocol(message)) if message == "identity hook failed"
        ));
        assert!(matches!(
            second_rx.await.unwrap().consume(),
            Err(HarnessError::Transport(message)) if message.contains("identity hook failed")
        ));
        assert!(matches!(
            messages.recv().await,
            Some(Err(CodexStreamError::Fatal(HarnessError::Protocol(message))))
                if message == "identity hook failed"
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), reader)
            .await
            .expect("reader should stop after a success hook failure")
            .unwrap();
        assert!(lock_test_pending(&pending).is_empty());
    }

    #[tokio::test]
    async fn reserved_control_writer_operates_independently() {
        let (stdin, mut wire) = duplex(4096);
        let (normal_tx, normal_rx) = mpsc::channel(1);
        let (control_tx, control_rx) = mpsc::channel(1);
        let (message_tx, _messages) = mpsc::channel(1);
        let pending: Pending = Arc::new(StdMutex::new(HashMap::new()));
        tokio::spawn(run_writer(
            BufWriter::new(stdin),
            normal_rx,
            control_rx,
            message_tx,
            pending,
        ));
        let _keep_normal_open = normal_tx;

        control_tx
            .send(Outbound::Value(serde_json::json!({
                "id": "approval-1",
                "result": { "decision": "accept" }
            })))
            .await
            .unwrap();
        let mut line = Vec::new();
        loop {
            let mut byte = [0_u8; 1];
            wire.read_exact(&mut byte).await.unwrap();
            line.push(byte[0]);
            if byte[0] == b'\n' {
                break;
            }
        }
        let value: Value = serde_json::from_slice(&line).unwrap();
        assert_eq!(value["id"], "approval-1");
    }

    #[tokio::test]
    async fn control_correlation_is_separate_bounded_and_recovers_after_timeout() {
        let (normal_tx, _normal_rx) = mpsc::channel(1);
        let (control_tx, mut control_rx) = mpsc::channel(CONTROL_WRITE_CAPACITY);
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("exit 0")
            .spawn()
            .expect("test child should start");
        child.wait().await.expect("test child should exit");
        let client = DispatchClient {
            normal_tx,
            control_tx,
            pending: Arc::new(StdMutex::new(HashMap::new())),
            in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUESTS)),
            control_in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT_CONTROL_REQUESTS)),
            admission: Arc::new(StdMutex::new(Admission { accepting: true })),
            next_id: Arc::new(AtomicI64::new(1)),
            child: Arc::new(Mutex::new(child)),
            stopped: Arc::new(AtomicBool::new(false)),
        };
        let _all_normal_correlation = client
            .in_flight
            .clone()
            .acquire_many_owned(MAX_IN_FLIGHT_REQUESTS as u32)
            .await
            .expect("normal correlation should initially be available");

        let mut pending_responses = Vec::new();
        for sequence in 0..MAX_IN_FLIGHT_CONTROL_REQUESTS {
            pending_responses.push(
                client
                    .submit_control_request(
                        "turn/interrupt",
                        serde_json::json!({"turnId": format!("turn-{sequence}")}),
                    )
                    .expect("control request must use its reserved correlation capacity"),
            );
        }
        assert!(matches!(
            client.submit_control_request("turn/interrupt", serde_json::json!({})),
            Err(HarnessError::Transport(message)) if message.contains("correlation is unavailable")
        ));
        for _ in 0..MAX_IN_FLIGHT_CONTROL_REQUESTS {
            let Some(Outbound::Value(value)) = control_rx.recv().await else {
                panic!("control request should use the existing control write lane");
            };
            assert_eq!(value["method"], "turn/interrupt");
        }
        assert_eq!(pending_responses.len(), MAX_IN_FLIGHT_CONTROL_REQUESTS);

        let timed_out = pending_responses
            .pop()
            .expect("one control response should remain pending");
        assert!(
            tokio::time::timeout(Duration::ZERO, timed_out.receive("turn/interrupt"))
                .await
                .is_err(),
            "unanswered control response should time out"
        );
        drop(pending_responses);
        assert!(lock_test_pending(&client.pending).is_empty());
        assert_eq!(
            client.control_in_flight.available_permits(),
            MAX_IN_FLIGHT_CONTROL_REQUESTS
        );
        let _recovered = client
            .submit_control_request("turn/interrupt", serde_json::json!({"turnId": "retry"}))
            .expect("timed-out control correlations should release their capacity");
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_accepted_normal_writes() {
        let (stdin, mut wire) = duplex(4096);
        let (normal_tx, normal_rx) = mpsc::channel(2);
        let (control_tx, control_rx) = mpsc::channel(1);
        let (message_tx, _messages) = mpsc::channel(1);
        let pending: Pending = Arc::new(StdMutex::new(HashMap::new()));
        normal_tx
            .send(Outbound::Value(serde_json::json!({"sequence": 1})))
            .await
            .expect("first normal write should queue");
        normal_tx
            .send(Outbound::Value(serde_json::json!({"sequence": 2})))
            .await
            .expect("second normal write should queue");
        let (finished_tx, finished_rx) = oneshot::channel();
        control_tx
            .send(Outbound::Shutdown(finished_tx))
            .await
            .expect("shutdown should queue");

        let writer = tokio::spawn(run_writer(
            BufWriter::new(stdin),
            normal_rx,
            control_rx,
            message_tx,
            pending,
        ));
        finished_rx
            .await
            .expect("writer should acknowledge shutdown")
            .expect("writer should close stdin cleanly");
        writer.await.expect("writer should not panic");
        let mut bytes = Vec::new();
        wire.read_to_end(&mut bytes)
            .await
            .expect("closed test stdin should reach EOF");
        let values = String::from_utf8(bytes)
            .expect("writer output should be UTF-8")
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("writer line should be JSON"))
            .collect::<Vec<_>>();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0]["sequence"], 1);
        assert_eq!(values[1]["sequence"], 2);
    }

    #[tokio::test]
    async fn graceful_shutdown_allows_child_to_exit_after_stdin_closes() {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("cat >/dev/null")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("test child should start");
        let stdin = child
            .stdin
            .take()
            .expect("test child stdin should be piped");
        let child = Arc::new(Mutex::new(child));
        let (normal_tx, normal_rx) = mpsc::channel(1);
        let (control_tx, control_rx) = mpsc::channel(1);
        let (message_tx, _messages) = mpsc::channel(1);
        let pending = Arc::new(StdMutex::new(HashMap::new()));
        tokio::spawn(run_writer(
            BufWriter::new(stdin),
            normal_rx,
            control_rx,
            message_tx,
            pending.clone(),
        ));
        let client = DispatchClient {
            normal_tx,
            control_tx,
            pending,
            in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUESTS)),
            control_in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT_CONTROL_REQUESTS)),
            admission: Arc::new(StdMutex::new(Admission { accepting: true })),
            next_id: Arc::new(AtomicI64::new(1)),
            child: child.clone(),
            stopped: Arc::new(AtomicBool::new(false)),
        };

        client
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .expect("graceful transport shutdown should succeed");
        let status = child
            .lock()
            .await
            .try_wait()
            .expect("test child status should be readable")
            .expect("test child should have exited");
        assert!(
            status.success(),
            "stdin EOF should let the child exit normally before the kill fallback"
        );
    }

    #[tokio::test]
    async fn full_control_queue_cannot_block_shutdown_kill_fallback() {
        let (normal_tx, _normal_rx) = mpsc::channel(1);
        let (control_tx, _control_rx) = mpsc::channel(1);
        control_tx
            .send(Outbound::Value(serde_json::json!({"fills": "queue"})))
            .await
            .expect("control queue should accept its filler");
        let child = Command::new("sh")
            .arg("-c")
            .arg("exec sleep 60")
            .spawn()
            .expect("test child should start");
        let client = DispatchClient {
            normal_tx,
            control_tx,
            pending: Arc::new(StdMutex::new(HashMap::new())),
            in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUESTS)),
            control_in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT_CONTROL_REQUESTS)),
            admission: Arc::new(StdMutex::new(Admission { accepting: true })),
            next_id: Arc::new(AtomicI64::new(1)),
            child: Arc::new(Mutex::new(child)),
            stopped: Arc::new(AtomicBool::new(false)),
        };

        tokio::time::timeout(
            Duration::from_secs(1),
            client.shutdown(tokio::time::Instant::now() + Duration::from_secs(1)),
        )
        .await
        .expect("queue saturation must not block the child-kill fallback")
        .expect("fallback child kill should succeed");
        assert!(matches!(
            client.submit_control_request("turn/interrupt", serde_json::json!({})),
            Err(HarnessError::Transport(message)) if message.contains("shutting down")
                || message.contains("correlation is unavailable")
        ));
    }

    #[tokio::test]
    async fn held_child_lock_cannot_exceed_absolute_shutdown_deadline() {
        let (normal_tx, _normal_rx) = mpsc::channel(1);
        let (control_tx, _control_rx) = mpsc::channel(1);
        let child = Command::new("sh")
            .arg("-c")
            .arg("exec sleep 60")
            .spawn()
            .expect("test child should start");
        let child = Arc::new(Mutex::new(child));
        let client = DispatchClient {
            normal_tx,
            control_tx,
            pending: Arc::new(StdMutex::new(HashMap::new())),
            in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUESTS)),
            control_in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT_CONTROL_REQUESTS)),
            admission: Arc::new(StdMutex::new(Admission { accepting: true })),
            next_id: Arc::new(AtomicI64::new(1)),
            child: child.clone(),
            stopped: Arc::new(AtomicBool::new(false)),
        };
        let mut held_child = child.lock().await;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(25);
        let result = client.shutdown(deadline).await;
        assert!(matches!(result, Err(HarnessError::Timeout(message))
            if message.contains("lock remained held")));

        held_child
            .start_kill()
            .expect("test child should be killable");
        held_child.wait().await.expect("test child should reap");
    }
}
