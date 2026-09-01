//! Task-owned Codex stdio transport.
//!
//! The transport owns process pipes, JSONL framing, and request correlation. It deliberately has
//! no access to mapper, routing, or thread lifecycle state; those remain owned by `CodexInstance`.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use codex_codes::jsonrpc::{
    JsonRpcError, JsonRpcErrorData, JsonRpcMessage, JsonRpcNotification, JsonRpcRequest,
    JsonRpcResponse, RequestId,
};
use giskard_core::error::HarnessError;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::Child;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, trace, warn};

pub(super) const CODEX_TRANSPORT_CAPACITY: usize = 64;
pub(super) const NON_JSON_STDOUT_PREVIEW_BYTES: usize = 4 * 1024;
const STDERR_PREVIEW_BYTES: usize = 4 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum TransportTerminalCause {
    ExplicitShutdown,
    StdoutEof,
    StdoutReadFailure(String),
    InvalidProtocolInput(String),
    StdinWriteFailure(String),
    ChildProcessFailure(String),
    InternalFailure(String),
}

impl TransportTerminalCause {
    pub(super) fn to_harness_error(&self) -> HarnessError {
        HarnessError::Transport(self.to_string())
    }
}

impl fmt::Display for TransportTerminalCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExplicitShutdown => formatter.write_str("Codex transport shut down"),
            Self::StdoutEof => formatter.write_str("Codex stdout closed"),
            Self::StdoutReadFailure(error) => {
                write!(formatter, "Codex stdout read failed: {error}")
            }
            Self::InvalidProtocolInput(error) => formatter.write_str(error),
            Self::StdinWriteFailure(error) => {
                write!(formatter, "Codex stdin write failed: {error}")
            }
            Self::ChildProcessFailure(error) => {
                write!(formatter, "Codex app-server process failed: {error}")
            }
            Self::InternalFailure(error) => write!(formatter, "Codex transport failed: {error}"),
        }
    }
}

struct TransportLifecycleInner {
    stop_cause: OnceLock<TransportTerminalCause>,
    stopping: watch::Sender<bool>,
    finalized: watch::Sender<bool>,
    correlation_admission: Arc<Semaphore>,
}

#[derive(Clone)]
pub(super) struct TransportLifecycle {
    inner: Arc<TransportLifecycleInner>,
}

impl TransportLifecycle {
    pub(super) fn new(capacity: usize) -> Self {
        let (stopping, _) = watch::channel(false);
        let (finalized, _) = watch::channel(false);
        Self {
            inner: Arc::new(TransportLifecycleInner {
                stop_cause: OnceLock::new(),
                stopping,
                finalized,
                correlation_admission: Arc::new(Semaphore::new(capacity)),
            }),
        }
    }

    pub(super) fn receiver(&self) -> TransportTerminalReceiver {
        TransportTerminalReceiver {
            lifecycle: self.clone(),
            finalized: self.inner.finalized.subscribe(),
        }
    }

    fn stopping_receiver(&self) -> TransportStoppingReceiver {
        TransportStoppingReceiver {
            lifecycle: self.clone(),
            stopping: self.inner.stopping.subscribe(),
        }
    }

    fn cause(&self) -> Option<TransportTerminalCause> {
        self.inner.stop_cause.get().cloned()
    }

    pub(super) fn begin_stop(&self, proposed: TransportTerminalCause) -> TransportTerminalCause {
        let _ = self.inner.stop_cause.set(proposed);
        let cause = self.inner.stop_cause.get().cloned().unwrap_or_else(|| {
            TransportTerminalCause::InternalFailure("terminal cause missing".into())
        });
        self.inner.correlation_admission.close();
        self.inner.stopping.send_replace(true);
        cause
    }

    pub(super) fn finish(&self) {
        if self.cause().is_none() {
            self.begin_stop(TransportTerminalCause::InternalFailure(
                "transport finalized without a stop cause".into(),
            ));
        }
        self.inner.finalized.send_replace(true);
    }

    fn ensure_running(&self) -> Result<(), HarnessError> {
        match self.cause() {
            Some(cause) => Err(cause.to_harness_error()),
            None => Ok(()),
        }
    }
}

#[derive(Clone)]
pub(super) struct TransportTerminalReceiver {
    lifecycle: TransportLifecycle,
    finalized: watch::Receiver<bool>,
}

impl TransportTerminalReceiver {
    pub(super) fn cause(&self) -> Option<TransportTerminalCause> {
        self.lifecycle.cause()
    }

    pub(super) async fn changed(&mut self) -> TransportTerminalCause {
        loop {
            if *self.finalized.borrow_and_update() {
                return self.cause().unwrap_or_else(|| {
                    TransportTerminalCause::InternalFailure(
                        "finalized transport has no stop cause".into(),
                    )
                });
            }
            if self.finalized.changed().await.is_err() {
                return self.cause().unwrap_or_else(|| {
                    TransportTerminalCause::InternalFailure(
                        "finalization notification channel closed".into(),
                    )
                });
            }
        }
    }
}

struct TransportStoppingReceiver {
    lifecycle: TransportLifecycle,
    stopping: watch::Receiver<bool>,
}

impl TransportStoppingReceiver {
    async fn changed(&mut self) -> TransportTerminalCause {
        loop {
            if *self.stopping.borrow_and_update() {
                return self.lifecycle.cause().unwrap_or_else(|| {
                    TransportTerminalCause::InternalFailure("stopping without a cause".into())
                });
            }
            if self.stopping.changed().await.is_err() {
                return self
                    .lifecycle
                    .begin_stop(TransportTerminalCause::InternalFailure(
                        "stopping notification channel closed".into(),
                    ));
            }
        }
    }
}

#[async_trait]
pub(super) trait CodexTransport: Send {
    async fn request_json(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, HarnessError>;
    async fn next_message(
        &mut self,
    ) -> Result<Option<codex_codes::ServerMessage>, CodexStreamError>;
    async fn respond_json(
        &mut self,
        id: RequestId,
        value: serde_json::Value,
    ) -> Result<(), HarnessError>;
    async fn respond_error_json(
        &mut self,
        id: RequestId,
        code: i64,
        message: &str,
    ) -> Result<(), HarnessError>;
    fn terminal(&self) -> TransportTerminalReceiver;
    async fn shutdown_transport(self) -> Result<(), HarnessError>
    where
        Self: Sized;
}

#[derive(Debug)]
pub(super) enum CodexStreamError {
    NonJsonStdout {
        parse_error: String,
        raw_preview: String,
        raw_bytes: usize,
    },
    Fatal(HarnessError),
}

pub(super) fn bounded_utf8_preview(value: &str, max_bytes: usize) -> String {
    let mut end = value.len().min(max_bytes);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

pub(super) fn classify_codex_stream_error(error: codex_codes::Error) -> CodexStreamError {
    match error {
        codex_codes::Error::Deserialization(parse_error)
            if parse_error.method.is_none()
                && parse_error.raw_json.is_none()
                && !parse_error.raw_line.trim_start().starts_with('{') =>
        {
            CodexStreamError::NonJsonStdout {
                raw_preview: bounded_utf8_preview(
                    &parse_error.raw_line,
                    NON_JSON_STDOUT_PREVIEW_BYTES,
                ),
                raw_bytes: parse_error.raw_line.len(),
                parse_error: parse_error.error_message,
            }
        }
        codex_codes::Error::Deserialization(parse_error) => {
            let raw_preview =
                bounded_utf8_preview(&parse_error.raw_line, NON_JSON_STDOUT_PREVIEW_BYTES);
            let method = parse_error.method.as_deref().unwrap_or("unknown");
            CodexStreamError::Fatal(HarnessError::Transport(format!(
                "Codex JSON-RPC deserialization error for method {method}: {} \
                 (raw_bytes: {}, raw_preview: {raw_preview:?})",
                parse_error.error_message,
                parse_error.raw_line.len(),
            )))
        }
        error => CodexStreamError::Fatal(HarnessError::Transport(error.to_string())),
    }
}

type CorrelationResult = Result<serde_json::Value, String>;

struct PendingCorrelation {
    response: oneshot::Sender<CorrelationResult>,
    cancelled: Arc<AtomicBool>,
}

enum ReaderCommand {
    Register {
        id: RequestId,
        response: oneshot::Sender<CorrelationResult>,
        cancelled: Arc<AtomicBool>,
        acknowledged: oneshot::Sender<Result<(), TransportTerminalCause>>,
    },
}

struct CorrelationRegistration {
    _permit: OwnedSemaphorePermit,
    cancelled: Arc<AtomicBool>,
    response: oneshot::Receiver<CorrelationResult>,
}

impl Drop for CorrelationRegistration {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

struct WriteJob {
    frame: Vec<u8>,
    cancelled: Arc<AtomicBool>,
    completion: oneshot::Sender<Result<(), TransportTerminalCause>>,
}

/// The concrete transport for one Codex app-server process.
pub(super) struct StdioCodexTransport {
    writer_tx: mpsc::Sender<WriteJob>,
    registration_tx: mpsc::Sender<ReaderCommand>,
    incoming_rx: mpsc::UnboundedReceiver<Result<codex_codes::ServerMessage, CodexStreamError>>,
    next_request_id: AtomicI64,
    lifecycle: TransportLifecycle,
    supervisor: TransportSupervisor,
}

struct TransportSupervisor {
    task: Option<JoinHandle<Result<(), String>>>,
}

impl TransportSupervisor {
    async fn join(mut self) -> Result<(), String> {
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        task.await.map_err(|error| error.to_string())?
    }
}

impl Drop for TransportSupervisor {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl StdioCodexTransport {
    pub(super) async fn spawn(
        builder: codex_codes::AppServerBuilder,
        workspace_root: PathBuf,
    ) -> Result<Self, HarnessError> {
        let mut command = builder
            .build_command()
            .map_err(|error| HarnessError::Spawn(error.to_string()))?;
        command.kill_on_drop(true);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .map_err(|error| HarnessError::Spawn(error.to_string()))?;
        let stdin = child.stdin.take().ok_or_else(|| {
            HarnessError::Spawn("Codex app-server did not provide a stdin pipe".into())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            HarnessError::Spawn("Codex app-server did not provide a stdout pipe".into())
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            HarnessError::Spawn("Codex app-server did not provide a stderr pipe".into())
        })?;
        Ok(Self::from_pipes(
            stdin,
            BufReader::new(stdout),
            BufReader::new(stderr),
            Some(child),
            workspace_root,
            CODEX_TRANSPORT_CAPACITY,
        ))
    }

    fn from_pipes<W, R, E>(
        stdin: W,
        stdout: R,
        stderr: E,
        child: Option<Child>,
        workspace_root: PathBuf,
        capacity: usize,
    ) -> Self
    where
        W: AsyncWrite + Unpin + Send + 'static,
        R: AsyncBufRead + Unpin + Send + 'static,
        E: AsyncBufRead + Unpin + Send + 'static,
    {
        let (writer_tx, writer_rx) = mpsc::channel(capacity);
        let (registration_tx, registration_rx) = mpsc::channel(capacity);
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let lifecycle = TransportLifecycle::new(capacity);
        let writer_task = tokio::spawn(run_writer(stdin, writer_rx, lifecycle.clone()));
        let reader_task = tokio::spawn(run_reader(
            stdout,
            registration_rx,
            incoming_tx,
            lifecycle.clone(),
        ));
        let stderr_task = tokio::spawn(drain_stderr(stderr, workspace_root));
        let supervisor_task = tokio::spawn(run_transport_supervisor(
            child,
            writer_task,
            reader_task,
            stderr_task,
            lifecycle.clone(),
        ));
        Self {
            writer_tx,
            registration_tx,
            incoming_rx,
            next_request_id: AtomicI64::new(1),
            lifecycle,
            supervisor: TransportSupervisor {
                task: Some(supervisor_task),
            },
        }
    }

    pub(super) async fn send_notification_json(
        &mut self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<(), HarnessError> {
        let frame = serialize_frame(&JsonRpcNotification {
            method: method.to_owned(),
            params,
        })?;
        self.submit_frame(frame, Arc::new(AtomicBool::new(false)))
            .await
    }

    fn allocate_request_id(&self) -> Result<RequestId, HarnessError> {
        let id = self
            .next_request_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| HarnessError::Transport("Codex request ID space exhausted".into()))?;
        Ok(RequestId::Integer(id))
    }

    async fn request_json_inner(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, HarnessError> {
        self.lifecycle.ensure_running()?;
        let mut stopping = self.lifecycle.stopping_receiver();
        let permit = tokio::select! {
            biased;
            cause = stopping.changed() => return Err(cause.to_harness_error()),
            permit = Arc::clone(&self.lifecycle.inner.correlation_admission).acquire_owned() => {
                permit.map_err(|_| lifecycle_error(&self.lifecycle))?
            }
        };
        self.lifecycle.ensure_running()?;
        let id = self.allocate_request_id()?;
        let frame = serialize_frame(&JsonRpcRequest {
            id: id.clone(),
            method: method.to_owned(),
            params: Some(params),
        })?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let (response, response_rx) = oneshot::channel();
        let (acknowledged, acknowledged_rx) = oneshot::channel();
        let mut registration = CorrelationRegistration {
            _permit: permit,
            cancelled: Arc::clone(&cancelled),
            response: response_rx,
        };
        let command = ReaderCommand::Register {
            id,
            response,
            cancelled: Arc::clone(&cancelled),
            acknowledged,
        };
        tokio::select! {
            biased;
            cause = stopping.changed() => return Err(cause.to_harness_error()),
            result = self.registration_tx.send(command) => {
                result.map_err(|_| lifecycle_error(&self.lifecycle))?;
            }
        }
        acknowledged_rx
            .await
            .map_err(|_| lifecycle_error(&self.lifecycle))?
            .map_err(|cause| cause.to_harness_error())?;
        self.submit_frame(frame, cancelled).await?;
        (&mut registration.response)
            .await
            .map_err(|_| lifecycle_error(&self.lifecycle))?
            .map_err(HarnessError::Transport)
    }

    async fn submit_frame(
        &self,
        frame: Vec<u8>,
        cancelled: Arc<AtomicBool>,
    ) -> Result<(), HarnessError> {
        self.lifecycle.ensure_running()?;
        let mut stopping = self.lifecycle.stopping_receiver();
        let (completion, written) = oneshot::channel();
        let job = WriteJob {
            frame,
            cancelled,
            completion,
        };
        tokio::select! {
            biased;
            cause = stopping.changed() => return Err(cause.to_harness_error()),
            result = self.writer_tx.send(job) => {
                result.map_err(|_| lifecycle_error(&self.lifecycle))?;
            }
        }
        written
            .await
            .map_err(|_| lifecycle_error(&self.lifecycle))?
            .map_err(|cause| cause.to_harness_error())
    }

    async fn shutdown_inner(self) -> Result<(), HarnessError> {
        self.lifecycle
            .begin_stop(TransportTerminalCause::ExplicitShutdown);
        drop(self.writer_tx);
        drop(self.registration_tx);
        self.supervisor
            .join()
            .await
            .map_err(HarnessError::Transport)
    }
}

async fn run_transport_supervisor(
    child: Option<Child>,
    writer_task: JoinHandle<()>,
    reader_task: JoinHandle<()>,
    stderr_task: JoinHandle<()>,
    lifecycle: TransportLifecycle,
) -> Result<(), String> {
    run_transport_supervisor_inner(child, writer_task, reader_task, stderr_task, lifecycle).await
}

#[async_trait]
trait TransportChild: Send {
    async fn wait_status(&mut self) -> Result<String, String>;
    fn start_kill_process(&mut self) -> Result<(), String>;
    fn try_wait_exited(&mut self) -> Result<bool, String>;
}

#[async_trait]
impl TransportChild for Child {
    async fn wait_status(&mut self) -> Result<String, String> {
        self.wait()
            .await
            .map(|status| status.to_string())
            .map_err(|error| error.to_string())
    }

    fn start_kill_process(&mut self) -> Result<(), String> {
        self.start_kill().map_err(|error| error.to_string())
    }

    fn try_wait_exited(&mut self) -> Result<bool, String> {
        self.try_wait()
            .map(|status| status.is_some())
            .map_err(|error| error.to_string())
    }
}

async fn run_transport_supervisor_inner<P>(
    mut child: Option<P>,
    writer_task: JoinHandle<()>,
    reader_task: JoinHandle<()>,
    mut stderr_task: JoinHandle<()>,
    lifecycle: TransportLifecycle,
) -> Result<(), String>
where
    P: TransportChild,
{
    let had_child = child.is_some();
    let mut stopping = lifecycle.stopping_receiver();
    let mut cleanup_error = None;
    if let Some(process) = child.as_mut() {
        tokio::select! {
            result = process.wait_status() => {
                let cause = match result {
                    Ok(status) => TransportTerminalCause::ChildProcessFailure(
                        format!("exited unexpectedly with {status}"),
                    ),
                    Err(error) => TransportTerminalCause::ChildProcessFailure(
                        format!("wait failed: {error}"),
                    ),
                };
                lifecycle.begin_stop(cause);
                child = None;
            }
            _ = stopping.changed() => {
                cleanup_error = kill_and_reap(process).await.err();
                child = None;
            }
        }
    } else {
        let _ = stopping.changed().await;
    }

    if cleanup_error.is_some() {
        writer_task.abort();
        reader_task.abort();
        stderr_task.abort();
    }
    if let Err(error) = writer_task.await
        && !error.is_cancelled()
    {
        cleanup_error.get_or_insert_with(|| error.to_string());
    }
    if let Err(error) = reader_task.await
        && !error.is_cancelled()
    {
        cleanup_error.get_or_insert_with(|| error.to_string());
    }
    if had_child && cleanup_error.is_none() {
        if let Err(error) = (&mut stderr_task).await {
            cleanup_error.get_or_insert_with(|| error.to_string());
        }
    } else {
        stderr_task.abort();
        let _ = stderr_task.await;
    }
    drop(child);
    lifecycle.finish();
    match cleanup_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn kill_and_reap<P>(child: &mut P) -> Result<(), String>
where
    P: TransportChild,
{
    if let Err(kill_error) = child.start_kill_process() {
        match child.try_wait_exited() {
            Ok(true) => return child.wait_status().await.map(|_| ()),
            Ok(false) => return Err(kill_error),
            Err(wait_error) => {
                return Err(format!(
                    "kill failed: {kill_error}; status check failed: {wait_error}"
                ));
            }
        }
    }
    child.wait_status().await.map(|_| ())
}

fn lifecycle_error(lifecycle: &TransportLifecycle) -> HarnessError {
    lifecycle
        .cause()
        .unwrap_or_else(|| TransportTerminalCause::InternalFailure("transport task closed".into()))
        .to_harness_error()
}

#[async_trait]
impl CodexTransport for StdioCodexTransport {
    async fn request_json(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, HarnessError> {
        self.request_json_inner(method, params).await
    }
    async fn next_message(
        &mut self,
    ) -> Result<Option<codex_codes::ServerMessage>, CodexStreamError> {
        match self.incoming_rx.recv().await {
            Some(result) => result.map(Some),
            None => Ok(None),
        }
    }
    async fn respond_json(
        &mut self,
        id: RequestId,
        value: serde_json::Value,
    ) -> Result<(), HarnessError> {
        self.submit_frame(
            serialize_frame(&JsonRpcResponse { id, result: value })?,
            Arc::new(AtomicBool::new(false)),
        )
        .await
    }
    async fn respond_error_json(
        &mut self,
        id: RequestId,
        code: i64,
        message: &str,
    ) -> Result<(), HarnessError> {
        self.submit_frame(
            serialize_frame(&JsonRpcError {
                id,
                error: JsonRpcErrorData {
                    code,
                    message: message.to_owned(),
                    data: None,
                },
            })?,
            Arc::new(AtomicBool::new(false)),
        )
        .await
    }
    fn terminal(&self) -> TransportTerminalReceiver {
        self.lifecycle.receiver()
    }
    async fn shutdown_transport(self) -> Result<(), HarnessError> {
        self.shutdown_inner().await
    }
}

fn serialize_frame(value: &impl serde::Serialize) -> Result<Vec<u8>, HarnessError> {
    let mut frame = serde_json::to_vec(value).map_err(|error| {
        HarnessError::Protocol(format!("could not serialize JSON-RPC: {error}"))
    })?;
    frame.push(b'\n');
    Ok(frame)
}

async fn run_writer<W>(
    mut stdin: W,
    mut jobs: mpsc::Receiver<WriteJob>,
    lifecycle: TransportLifecycle,
) where
    W: AsyncWrite + Unpin,
{
    let mut stopping = lifecycle.stopping_receiver();
    loop {
        let job = tokio::select! {
            biased;
            cause = stopping.changed() => {
                fail_queued_writes(&mut jobs, &cause).await;
                return;
            }
            job = jobs.recv() => match job { Some(job) => job, None => return },
        };
        if let Err(error) = lifecycle.ensure_running() {
            let cause = lifecycle
                .cause()
                .unwrap_or_else(|| TransportTerminalCause::InternalFailure(error.to_string()));
            let _ = job.completion.send(Err(cause.clone()));
            fail_queued_writes(&mut jobs, &cause).await;
            return;
        }
        if job.cancelled.load(Ordering::Acquire) {
            continue;
        }
        let write_result = tokio::select! {
            biased;
            cause = stopping.changed() => {
                warn!(cause = %cause, "Codex stdin delivery interrupted; delivery is ambiguous");
                let _ = job.completion.send(Err(cause.clone()));
                fail_queued_writes(&mut jobs, &cause).await;
                return;
            }
            result = async { stdin.write_all(&job.frame).await?; stdin.flush().await } => result,
        };
        if let Err(error) = write_result {
            let cause =
                lifecycle.begin_stop(TransportTerminalCause::StdinWriteFailure(error.to_string()));
            let _ = job.completion.send(Err(cause.clone()));
            fail_queued_writes(&mut jobs, &cause).await;
            return;
        }
        let _ = job.completion.send(Ok(()));
    }
}

async fn fail_queued_writes(jobs: &mut mpsc::Receiver<WriteJob>, cause: &TransportTerminalCause) {
    jobs.close();
    while let Some(job) = jobs.recv().await {
        let _ = job.completion.send(Err(cause.clone()));
    }
}

async fn run_reader<R>(
    stdout: R,
    mut registrations: mpsc::Receiver<ReaderCommand>,
    incoming_tx: mpsc::UnboundedSender<Result<codex_codes::ServerMessage, CodexStreamError>>,
    lifecycle: TransportLifecycle,
) where
    R: AsyncBufRead + Unpin,
{
    let mut lines = stdout.lines();
    let mut pending = HashMap::new();
    let mut registrations_open = true;
    let mut stopping = lifecycle.stopping_receiver();
    let mut stopping_observed = false;
    loop {
        let action = tokio::select! {
            biased;
            cause = stopping.changed(), if !stopping_observed => ReaderAction::Stopping(cause),
            action = async {
                tokio::select! {
                    registration = registrations.recv(), if registrations_open => ReaderAction::Registration(registration),
                    line = lines.next_line() => ReaderAction::Line(line),
                }
            } => action,
        };
        match action {
            ReaderAction::Stopping(cause) => {
                stopping_observed = true;
                registrations.close();
                registrations_open = false;
                fail_pending(&mut pending, &cause);
            }
            ReaderAction::Registration(Some(ReaderCommand::Register {
                id,
                response,
                cancelled,
                acknowledged,
            })) => {
                if let Some(cause) = lifecycle.cause() {
                    let _ = acknowledged.send(Err(cause));
                    continue;
                }
                pending.retain(|_, correlation: &mut PendingCorrelation| {
                    !correlation.cancelled.load(Ordering::Acquire)
                });
                if cancelled.load(Ordering::Acquire) {
                    let _ = acknowledged.send(Ok(()));
                    continue;
                }
                match pending.entry(id.clone()) {
                    Entry::Vacant(entry) => {
                        entry.insert(PendingCorrelation {
                            response,
                            cancelled,
                        });
                        let _ = acknowledged.send(Ok(()));
                    }
                    Entry::Occupied(_) => {
                        let cause = lifecycle.begin_stop(TransportTerminalCause::InternalFailure(
                            format!("duplicate Codex request correlation for id {id}"),
                        ));
                        let _ = acknowledged.send(Err(cause.clone()));
                        fail_pending(&mut pending, &cause);
                        return;
                    }
                }
            }
            ReaderAction::Registration(None) => {
                // No further correlations can be registered. Continue draining stdout so decoded
                // messages remain ordered ahead of finalization.
                registrations_open = false;
            }
            ReaderAction::Line(Ok(Some(line))) => {
                if !handle_stdout_line(&line, &mut pending, &incoming_tx, &lifecycle) {
                    return;
                }
            }
            ReaderAction::Line(Ok(None)) => {
                let cause = lifecycle.begin_stop(TransportTerminalCause::StdoutEof);
                fail_pending(&mut pending, &cause);
                return;
            }
            ReaderAction::Line(Err(error)) => {
                let cause = lifecycle
                    .begin_stop(TransportTerminalCause::StdoutReadFailure(error.to_string()));
                fail_pending(&mut pending, &cause);
                let _ = incoming_tx.send(Err(CodexStreamError::Fatal(cause.to_harness_error())));
                return;
            }
        }
    }
}

enum ReaderAction {
    Stopping(TransportTerminalCause),
    Registration(Option<ReaderCommand>),
    Line(std::io::Result<Option<String>>),
}

fn handle_stdout_line(
    line: &str,
    pending: &mut HashMap<RequestId, PendingCorrelation>,
    incoming_tx: &mpsc::UnboundedSender<Result<codex_codes::ServerMessage, CodexStreamError>>,
    lifecycle: &TransportLifecycle,
) -> bool {
    if line.trim().is_empty() {
        return true;
    }
    let value: serde_json::Value = match serde_json::from_str(line) {
        Ok(value) => value,
        Err(error) if !line.trim_start().starts_with('{') => {
            let _ = incoming_tx.send(Err(CodexStreamError::NonJsonStdout {
                parse_error: error.to_string(),
                raw_preview: bounded_utf8_preview(line, NON_JSON_STDOUT_PREVIEW_BYTES),
                raw_bytes: line.len(),
            }));
            return true;
        }
        Err(error) => {
            terminate_invalid_input(
                lifecycle,
                incoming_tx,
                pending,
                invalid_input_message(line, &error.to_string()),
            );
            return false;
        }
    };
    let envelope: JsonRpcMessage = match serde_json::from_value(value.clone()) {
        Ok(envelope) => envelope,
        Err(error) => {
            terminate_invalid_input(
                lifecycle,
                incoming_tx,
                pending,
                invalid_input_message(line, &error.to_string()),
            );
            return false;
        }
    };
    match envelope {
        JsonRpcMessage::Response(response) => {
            resolve_correlation(pending, response.id, Ok(response.result))
        }
        JsonRpcMessage::Error(response) => resolve_correlation(
            pending,
            response.id,
            Err(format!(
                "JSON-RPC error ({}): {}",
                response.error.code, response.error.message
            )),
        ),
        JsonRpcMessage::Notification(_) | JsonRpcMessage::Request(_) => {
            match codex_codes::ServerMessage::from_value(value) {
                Ok(message) => {
                    let _ = incoming_tx.send(Ok(message));
                }
                Err(error) => {
                    let stream_error = classify_codex_stream_error(error);
                    let message = match &stream_error {
                        CodexStreamError::Fatal(error) => error.to_string(),
                        CodexStreamError::NonJsonStdout { .. } => {
                            "unexpected non-JSON classification for JSON object".into()
                        }
                    };
                    let cause =
                        lifecycle.begin_stop(TransportTerminalCause::InvalidProtocolInput(message));
                    fail_pending(pending, &cause);
                    let _ = incoming_tx.send(Err(stream_error));
                    return false;
                }
            }
        }
    }
    true
}

fn invalid_input_message(line: &str, error: &str) -> String {
    format!(
        "Codex JSON-RPC deserialization error for method unknown: {error} (raw_bytes: {}, raw_preview: {:?})",
        line.len(),
        bounded_utf8_preview(line, NON_JSON_STDOUT_PREVIEW_BYTES)
    )
}

fn terminate_invalid_input(
    lifecycle: &TransportLifecycle,
    incoming_tx: &mpsc::UnboundedSender<Result<codex_codes::ServerMessage, CodexStreamError>>,
    pending: &mut HashMap<RequestId, PendingCorrelation>,
    message: String,
) {
    let cause = lifecycle.begin_stop(TransportTerminalCause::InvalidProtocolInput(message));
    fail_pending(pending, &cause);
    let _ = incoming_tx.send(Err(CodexStreamError::Fatal(cause.to_harness_error())));
}

fn resolve_correlation(
    pending: &mut HashMap<RequestId, PendingCorrelation>,
    id: RequestId,
    result: CorrelationResult,
) {
    if let Some(correlation) = pending.remove(&id) {
        if correlation.cancelled.load(Ordering::Acquire) {
            debug!(request_id = %id, "ignoring response for cancelled Codex request");
        } else {
            let _ = correlation.response.send(result);
        }
    } else {
        warn!(request_id = %id, "ignoring unmatched Codex JSON-RPC response");
    }
}

fn fail_pending(
    pending: &mut HashMap<RequestId, PendingCorrelation>,
    cause: &TransportTerminalCause,
) {
    for (_, correlation) in pending.drain() {
        let _ = correlation.response.send(Err(cause.to_string()));
    }
}

async fn drain_stderr<E>(stderr: E, workspace_root: PathBuf)
where
    E: AsyncBufRead + Unpin,
{
    let mut lines = stderr.lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                forward_stderr_line(&workspace_root, &line);
            }
            Ok(None) => return,
            Err(error) => {
                warn!(workspace_root = %workspace_root.display(), error = %error, "failed to drain Codex app-server stderr");
                return;
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StderrLevel {
    Error,
    Warn,
    Debug,
    Trace,
}

fn forward_stderr_line(workspace_root: &std::path::Path, raw: &str) {
    let line = strip_ansi_csi(raw);
    let trimmed = line.trim_end_matches(['\n', '\r']);
    let Some(level) = stderr_level(trimmed) else {
        return;
    };
    let preview = bounded_utf8_preview(trimmed, STDERR_PREVIEW_BYTES);
    match level {
        StderrLevel::Error => {
            error!(workspace_root = %workspace_root.display(), stderr_bytes = trimmed.len(), stderr_preview = %preview, "Codex app-server stderr")
        }
        StderrLevel::Warn => {
            warn!(workspace_root = %workspace_root.display(), stderr_bytes = trimmed.len(), stderr_preview = %preview, "Codex app-server stderr")
        }
        StderrLevel::Debug => {
            debug!(workspace_root = %workspace_root.display(), stderr_bytes = trimmed.len(), stderr_preview = %preview, "Codex app-server stderr")
        }
        StderrLevel::Trace => {
            trace!(workspace_root = %workspace_root.display(), stderr_bytes = trimmed.len(), stderr_preview = %preview, "Codex app-server stderr")
        }
    }
}

fn stderr_level(line: &str) -> Option<StderrLevel> {
    if line.trim().is_empty() {
        None
    } else if line.contains(" ERROR ") {
        Some(StderrLevel::Error)
    } else if line.contains(" WARN ") {
        Some(StderrLevel::Warn)
    } else if line.contains(" DEBUG ") {
        Some(StderrLevel::Debug)
    } else {
        Some(StderrLevel::Trace)
    }
}

fn strip_ansi_csi(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for control in chars.by_ref() {
                if ('@'..='~').contains(&control) {
                    break;
                }
            }
        } else {
            output.push(character);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio::time::{Duration, timeout};

    struct Peer {
        requests: tokio::io::Lines<BufReader<DuplexStream>>,
        responses: DuplexStream,
        _stderr: DuplexStream,
    }

    fn test_transport(capacity: usize) -> (StdioCodexTransport, Peer) {
        let (transport_stdin, peer_requests) = tokio::io::duplex(16 * 1024);
        let (peer_responses, transport_stdout) = tokio::io::duplex(16 * 1024);
        let (transport_stderr, peer_stderr) = tokio::io::duplex(1024);
        let transport = StdioCodexTransport::from_pipes(
            transport_stdin,
            BufReader::new(transport_stdout),
            BufReader::new(transport_stderr),
            None,
            PathBuf::from("/test-workspace"),
            capacity,
        );
        (
            transport,
            Peer {
                requests: BufReader::new(peer_requests).lines(),
                responses: peer_responses,
                _stderr: peer_stderr,
            },
        )
    }

    async fn next_request(peer: &mut Peer) -> serde_json::Value {
        let line = timeout(Duration::from_secs(1), peer.requests.next_line())
            .await
            .expect("request timeout")
            .expect("request read")
            .expect("request EOF");
        serde_json::from_str(&line).expect("valid request JSON")
    }

    async fn send(peer: &mut Peer, value: serde_json::Value) {
        let mut bytes = serde_json::to_vec(&value).expect("serialize response");
        bytes.push(b'\n');
        peer.responses
            .write_all(&bytes)
            .await
            .expect("write response");
    }

    #[tokio::test]
    async fn reverse_ordered_responses_use_exact_correlations() {
        let (transport, mut peer) = test_transport(2);
        let transport = Arc::new(transport);
        let first = {
            let transport = Arc::clone(&transport);
            tokio::spawn(async move {
                transport
                    .request_json_inner("first", serde_json::json!({}))
                    .await
            })
        };
        let second = {
            let transport = Arc::clone(&transport);
            tokio::spawn(async move {
                transport
                    .request_json_inner("second", serde_json::json!({}))
                    .await
            })
        };
        let one = next_request(&mut peer).await;
        let two = next_request(&mut peer).await;
        let (first_id, second_id) = if one["method"] == "first" {
            (one["id"].clone(), two["id"].clone())
        } else {
            (two["id"].clone(), one["id"].clone())
        };
        send(
            &mut peer,
            serde_json::json!({"id":second_id,"result":"two"}),
        )
        .await;
        send(&mut peer, serde_json::json!({"id":first_id,"result":"one"})).await;
        assert_eq!(first.await.expect("first task").expect("first"), "one");
        assert_eq!(second.await.expect("second task").expect("second"), "two");
    }

    #[tokio::test]
    async fn resolved_response_precedes_later_eof() {
        let lifecycle = TransportLifecycle::new(1);
        let (response, response_rx) = oneshot::channel();
        let mut pending = HashMap::from([(
            RequestId::Integer(1),
            PendingCorrelation {
                response,
                cancelled: Arc::new(AtomicBool::new(false)),
            },
        )]);
        resolve_correlation(
            &mut pending,
            RequestId::Integer(1),
            Ok(serde_json::json!("complete")),
        );
        let cause = lifecycle.begin_stop(TransportTerminalCause::StdoutEof);
        fail_pending(&mut pending, &cause);
        assert_eq!(
            response_rx.await.expect("correlation owner"),
            Ok(serde_json::json!("complete"))
        );
    }

    #[tokio::test]
    async fn stopping_precedes_later_response() {
        let (transport, mut peer) = test_transport(1);
        let transport = Arc::new(transport);
        let request = {
            let transport = Arc::clone(&transport);
            tokio::spawn(async move {
                transport
                    .request_json_inner("pending", serde_json::json!({}))
                    .await
            })
        };
        let frame = next_request(&mut peer).await;
        let cause = transport
            .lifecycle
            .begin_stop(TransportTerminalCause::ChildProcessFailure(
                "stopped first".into(),
            ));
        let error = timeout(Duration::from_secs(1), request)
            .await
            .expect("pending request resolves")
            .expect("request task")
            .expect_err("stop fails pending correlation");
        assert_eq!(error.to_string(), cause.to_harness_error().to_string());
        send(
            &mut peer,
            serde_json::json!({"id":frame["id"],"result":"too late"}),
        )
        .await;
    }

    #[tokio::test]
    async fn completed_write_precedes_later_eof() {
        let (writer, mut peer) = tokio::io::duplex(1024);
        let (jobs_tx, jobs_rx) = mpsc::channel(1);
        let lifecycle = TransportLifecycle::new(1);
        let writer_task = tokio::spawn(run_writer(writer, jobs_rx, lifecycle.clone()));
        let (completion, completed) = oneshot::channel();
        jobs_tx
            .send(WriteJob {
                frame: b"complete\n".to_vec(),
                cancelled: Arc::new(AtomicBool::new(false)),
                completion,
            })
            .await
            .expect("queue frame");
        let mut line = String::new();
        BufReader::new(&mut peer)
            .read_line(&mut line)
            .await
            .expect("read complete frame");
        assert_eq!(line, "complete\n");
        lifecycle.begin_stop(TransportTerminalCause::StdoutEof);
        assert_eq!(completed.await.expect("writer completion"), Ok(()));
        writer_task.await.expect("writer task");
    }

    #[tokio::test]
    async fn stop_precedes_queued_write_without_writing_bytes() {
        let (writer, mut peer) = tokio::io::duplex(1024);
        let (jobs_tx, jobs_rx) = mpsc::channel(1);
        let lifecycle = TransportLifecycle::new(1);
        let (completion, completed) = oneshot::channel();
        jobs_tx
            .send(WriteJob {
                frame: b"must-not-write\n".to_vec(),
                cancelled: Arc::new(AtomicBool::new(false)),
                completion,
            })
            .await
            .expect("queue frame");
        let cause = lifecycle.begin_stop(TransportTerminalCause::StdoutEof);
        drop(jobs_tx);
        run_writer(writer, jobs_rx, lifecycle).await;
        assert_eq!(completed.await.expect("failed queued write"), Err(cause));
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.expect("read stdin");
        assert!(bytes.is_empty());
    }

    #[tokio::test]
    async fn messages_before_response_are_retained_and_delivered_once() {
        let (mut transport, mut peer) = test_transport(1);
        let request = transport.request_json_inner("call", serde_json::json!({}));
        let exchange = async {
            let frame = next_request(&mut peer).await;
            send(
                &mut peer,
                serde_json::json!({"method":"future/notification","params":{}}),
            )
            .await;
            send(
                &mut peer,
                serde_json::json!({"id":"server","method":"future/request","params":{}}),
            )
            .await;
            send(
                &mut peer,
                serde_json::json!({"id":frame["id"],"result":true}),
            )
            .await;
        };
        let (result, ()) = tokio::join!(request, exchange);
        assert_eq!(result.expect("response"), true);
        assert!(matches!(
            transport.next_message().await.expect("notification"),
            Some(codex_codes::ServerMessage::Notification(_))
        ));
        assert!(matches!(
            transport.next_message().await.expect("request"),
            Some(codex_codes::ServerMessage::Request { .. })
        ));
    }

    #[tokio::test]
    async fn json_rpc_error_preserves_expected_format() {
        let (transport, mut peer) = test_transport(1);
        let request = transport.request_json_inner("call", serde_json::json!({}));
        let exchange = async {
            let frame = next_request(&mut peer).await;
            send(&mut peer, serde_json::json!({"id":frame["id"],"error":{"code":-32001,"message":"missing rollout"}})).await;
        };
        let (result, ()) = tokio::join!(request, exchange);
        assert!(
            matches!(result, Err(HarnessError::Transport(message)) if message == "JSON-RPC error (-32001): missing rollout")
        );
    }

    #[tokio::test]
    async fn initialization_request_precedes_initialized_notification() {
        let (mut transport, mut peer) = test_transport(1);
        let initialize = transport.request_json_inner("initialize", serde_json::json!({}));
        let exchange = async {
            let frame = next_request(&mut peer).await;
            assert_eq!(frame["id"], 1);
            send(&mut peer, serde_json::json!({"id":1,"result":{}})).await;
        };
        let (result, ()) = tokio::join!(initialize, exchange);
        result.expect("initialize response");
        transport
            .send_notification_json("initialized", None)
            .await
            .expect("notification");
        let initialized = next_request(&mut peer).await;
        assert_eq!(initialized["method"], "initialized");
        assert!(initialized.get("id").is_none());
    }

    #[tokio::test]
    async fn concurrent_outgoing_frames_are_separate_jsonl_records() {
        let (transport, mut peer) = test_transport(3);
        let transport = Arc::new(transport);
        let frames = [
            serialize_frame(&JsonRpcRequest {
                id: RequestId::Integer(700),
                method: "request".into(),
                params: Some(serde_json::json!({})),
            })
            .expect("request frame"),
            serialize_frame(&JsonRpcResponse {
                id: RequestId::Integer(701),
                result: serde_json::json!({"result":true}),
            })
            .expect("response frame"),
            serialize_frame(&JsonRpcError {
                id: RequestId::Integer(702),
                error: JsonRpcErrorData {
                    code: -32000,
                    message: "error".into(),
                    data: None,
                },
            })
            .expect("error frame"),
        ];
        let jobs = frames
            .into_iter()
            .map(|frame| {
                let transport = Arc::clone(&transport);
                tokio::spawn(async move {
                    transport
                        .submit_frame(frame, Arc::new(AtomicBool::new(false)))
                        .await
                })
            })
            .collect::<Vec<_>>();
        let mut ids = Vec::new();
        for _ in 0..3 {
            ids.push(
                next_request(&mut peer).await["id"]
                    .as_i64()
                    .expect("integer id"),
            );
        }
        ids.sort_unstable();
        assert_eq!(ids, vec![700, 701, 702]);
        for job in jobs {
            job.await.expect("task").expect("write");
        }
    }

    #[tokio::test]
    async fn writer_failure_terminates_reader_without_a_command_lane() {
        let (transport, peer) = test_transport(2);
        let transport = Arc::new(transport);
        drop(peer.requests);
        let error = transport
            .submit_frame(
                serialize_frame(&JsonRpcResponse {
                    id: RequestId::Integer(1),
                    result: serde_json::json!({}),
                })
                .expect("frame"),
                Arc::new(AtomicBool::new(false)),
            )
            .await
            .expect_err("closed stdin fails");
        let cause = transport.terminal().cause().expect("terminal cause");
        assert!(matches!(
            cause,
            TransportTerminalCause::StdinWriteFailure(_)
        ));
        assert_eq!(error.to_string(), cause.to_harness_error().to_string());
        drop(peer.responses);
    }

    #[tokio::test]
    async fn waiter_behind_saturated_admission_receives_terminal_cause() {
        let (transport, mut peer) = test_transport(1);
        let transport = Arc::new(transport);
        let first = {
            let transport = Arc::clone(&transport);
            tokio::spawn(async move {
                transport
                    .request_json_inner("first", serde_json::json!({}))
                    .await
            })
        };
        let _ = next_request(&mut peer).await;
        let second = {
            let transport = Arc::clone(&transport);
            tokio::spawn(async move {
                transport
                    .request_json_inner("second", serde_json::json!({}))
                    .await
            })
        };
        drop(peer.responses);
        let first_error = first
            .await
            .expect("first task")
            .expect_err("EOF fails first")
            .to_string();
        let second_error = second
            .await
            .expect("second task")
            .expect_err("EOF fails waiter")
            .to_string();
        assert_eq!(first_error, second_error);
        assert!(first_error.contains("Codex stdout closed"));
    }

    struct GatedWriter {
        entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    }

    impl AsyncWrite for GatedWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if let Some(entered) = self.entered.lock().expect("entered lock").take() {
                let _ = entered.send(());
            }
            Poll::Pending
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn stdout_eof_interrupts_a_writer_inside_poll_write() {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (peer_responses, transport_stdout) = tokio::io::duplex(1024);
        let (transport_stderr, _peer_stderr) = tokio::io::duplex(1024);
        let transport = Arc::new(StdioCodexTransport::from_pipes(
            GatedWriter {
                entered: Arc::new(Mutex::new(Some(entered_tx))),
            },
            BufReader::new(transport_stdout),
            BufReader::new(transport_stderr),
            None,
            PathBuf::from("/test-workspace"),
            1,
        ));
        let request = {
            let transport = Arc::clone(&transport);
            tokio::spawn(async move {
                transport
                    .request_json_inner("blocked", serde_json::json!({}))
                    .await
            })
        };
        entered_rx.await.expect("writer entered poll_write");
        drop(peer_responses);
        let error = timeout(Duration::from_secs(1), request)
            .await
            .expect("request completes")
            .expect("request task")
            .expect_err("EOF fails request");
        assert!(error.to_string().contains("Codex stdout closed"));
    }

    #[tokio::test]
    async fn simultaneous_terminal_failures_preserve_the_first_cause() {
        let terminal = TransportLifecycle::new(1);
        let first = terminal.begin_stop(TransportTerminalCause::StdoutEof);
        let second = terminal.begin_stop(TransportTerminalCause::StdinWriteFailure("later".into()));
        assert_eq!(first, TransportTerminalCause::StdoutEof);
        assert_eq!(second, first);
        assert_eq!(terminal.receiver().cause(), Some(first));
    }

    #[tokio::test]
    async fn cancellation_before_admission_releases_no_capacity() {
        let terminal = TransportLifecycle::new(1);
        let held = Arc::clone(&terminal.inner.correlation_admission)
            .acquire_owned()
            .await
            .expect("initial permit");
        let admission = Arc::clone(&terminal.inner.correlation_admission);
        let waiter = tokio::spawn(async move { admission.acquire_owned().await });
        tokio::task::yield_now().await;
        waiter.abort();
        let _ = waiter.await;
        drop(held);
        let recovered = timeout(
            Duration::from_secs(1),
            Arc::clone(&terminal.inner.correlation_admission).acquire_owned(),
        )
        .await
        .expect("capacity released")
        .expect("transport remains open");
        drop(recovered);
    }

    #[tokio::test]
    async fn cancellation_before_writing_skips_the_frame() {
        let (mut peer, writer) = tokio::io::duplex(1024);
        let (jobs_tx, jobs_rx) = mpsc::channel(1);
        let terminal = TransportLifecycle::new(1);
        let writer_task = tokio::spawn(run_writer(writer, jobs_rx, terminal.clone()));
        let cancelled = Arc::new(AtomicBool::new(true));
        let (completion, completion_rx) = oneshot::channel();
        jobs_tx
            .send(WriteJob {
                frame: b"should-not-be-written\n".to_vec(),
                cancelled,
                completion,
            })
            .await
            .expect("queue cancelled frame");
        drop(completion_rx);
        drop(jobs_tx);
        writer_task.await.expect("writer task");
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes)
            .await
            .expect("read writer output");
        assert!(bytes.is_empty());
    }

    #[tokio::test]
    async fn cancellation_after_writing_keeps_late_response_isolated() {
        let (transport, mut peer) = test_transport(1);
        let transport = Arc::new(transport);
        let abandoned = {
            let transport = Arc::clone(&transport);
            tokio::spawn(async move {
                transport
                    .request_json_inner("abandoned", serde_json::json!({}))
                    .await
            })
        };
        let abandoned_frame = next_request(&mut peer).await;
        abandoned.abort();
        let _ = abandoned.await;

        let current = {
            let transport = Arc::clone(&transport);
            tokio::spawn(async move {
                transport
                    .request_json_inner("current", serde_json::json!({}))
                    .await
            })
        };
        let current_frame = next_request(&mut peer).await;
        assert_ne!(abandoned_frame["id"], current_frame["id"]);
        send(
            &mut peer,
            serde_json::json!({"id":abandoned_frame["id"],"result":"late"}),
        )
        .await;
        send(
            &mut peer,
            serde_json::json!({"id":current_frame["id"],"result":"current"}),
        )
        .await;
        assert_eq!(current.await.expect("task").expect("response"), "current");
    }

    #[tokio::test]
    async fn cancellation_churn_is_bounded_and_does_not_starve_stdout() {
        let (transport, mut peer) = test_transport(2);
        let transport = Arc::new(transport);
        for _ in 0..100 {
            let request = {
                let transport = Arc::clone(&transport);
                tokio::spawn(async move {
                    transport
                        .request_json_inner("cancel", serde_json::json!({}))
                        .await
                })
            };
            let _ = next_request(&mut peer).await;
            request.abort();
            let _ = request.await;
        }
        let current = {
            let transport = Arc::clone(&transport);
            tokio::spawn(async move {
                transport
                    .request_json_inner("current", serde_json::json!({}))
                    .await
            })
        };
        let frame = next_request(&mut peer).await;
        send(
            &mut peer,
            serde_json::json!({"method":"future/notification","params":{}}),
        )
        .await;
        send(
            &mut peer,
            serde_json::json!({"id":frame["id"],"result":"ok"}),
        )
        .await;
        assert_eq!(current.await.expect("task").expect("response"), "ok");
    }

    #[tokio::test]
    async fn non_json_is_skipped_but_invalid_object_is_terminal() {
        let (mut transport, mut peer) = test_transport(1);
        peer.responses
            .write_all(b"noise\n{bad json\n")
            .await
            .expect("write frames");
        assert!(matches!(
            transport.next_message().await,
            Err(CodexStreamError::NonJsonStdout { .. })
        ));
        assert!(matches!(
            transport.next_message().await,
            Err(CodexStreamError::Fatal(_))
        ));
        assert!(matches!(
            transport.terminal().cause(),
            Some(TransportTerminalCause::InvalidProtocolInput(_))
        ));
    }

    #[test]
    fn blank_stdout_lines_are_ignored() {
        let lifecycle = TransportLifecycle::new(1);
        let (incoming_tx, mut incoming_rx) = mpsc::unbounded_channel();
        let mut pending = HashMap::new();
        for line in ["", "   ", "\t\r"] {
            assert!(handle_stdout_line(
                line,
                &mut pending,
                &incoming_tx,
                &lifecycle
            ));
        }
        assert!(matches!(
            incoming_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(lifecycle.cause().is_none());
    }

    #[test]
    fn stderr_strips_ansi_and_preserves_provider_severity() {
        let warning = strip_ansi_csi("\u{1b}[33m2026-01-01 WARN codex: caution\u{1b}[0m");
        assert_eq!(warning, "2026-01-01 WARN codex: caution");
        assert_eq!(stderr_level(&warning), Some(StderrLevel::Warn));
        assert_eq!(
            stderr_level("2026-01-01 ERROR codex: failed"),
            Some(StderrLevel::Error)
        );
        assert_eq!(
            stderr_level("2026-01-01 DEBUG codex: detail"),
            Some(StderrLevel::Debug)
        );
        assert_eq!(
            stderr_level("2026-01-01 INFO codex: routine"),
            Some(StderrLevel::Trace)
        );
        assert_eq!(stderr_level("  \t"), None);
    }

    #[tokio::test]
    async fn valid_message_precedes_immediately_following_invalid_input() {
        let (mut transport, mut peer) = test_transport(1);
        send(
            &mut peer,
            serde_json::json!({"method":"future/notification","params":{}}),
        )
        .await;
        peer.responses
            .write_all(b"{invalid\n")
            .await
            .expect("write invalid frame");
        assert!(matches!(
            transport.next_message().await,
            Ok(Some(codex_codes::ServerMessage::Notification(_)))
        ));
        assert!(matches!(
            transport.next_message().await,
            Err(CodexStreamError::Fatal(_))
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn child_final_message_precedes_finalization() {
        let mut command = tokio::process::Command::new("sh");
        command
            .arg("-c")
            .arg("printf '%s\\n' '{\"method\":\"future/notification\",\"params\":{}}'")
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn child");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = child.stdout.take().expect("stdout");
        let stderr = child.stderr.take().expect("stderr");
        let mut transport = StdioCodexTransport::from_pipes(
            stdin,
            BufReader::new(stdout),
            BufReader::new(stderr),
            Some(child),
            PathBuf::from("/test-workspace"),
            1,
        );
        let mut finalized = transport.terminal();
        assert!(matches!(
            transport.next_message().await,
            Ok(Some(codex_codes::ServerMessage::Notification(_)))
        ));
        let cause = timeout(Duration::from_secs(1), finalized.changed())
            .await
            .expect("transport finalized");
        assert!(matches!(
            cause,
            TransportTerminalCause::ChildProcessFailure(_) | TransportTerminalCause::StdoutEof
        ));
        transport.shutdown_inner().await.expect("join supervisor");
    }

    #[tokio::test]
    async fn invalid_envelope_and_typed_payload_are_terminal() {
        for frame in [
            serde_json::json!({"unexpected":true}),
            serde_json::json!({"method":"thread/started","params":{}}),
        ] {
            let (mut transport, mut peer) = test_transport(1);
            send(&mut peer, frame).await;
            assert!(matches!(
                transport.next_message().await,
                Err(CodexStreamError::Fatal(_))
            ));
            assert!(matches!(
                transport.terminal().cause(),
                Some(TransportTerminalCause::InvalidProtocolInput(_))
            ));
        }
    }

    enum FakeChildMode {
        WaitFailure,
        KillFailure,
    }

    struct FakeChild {
        mode: FakeChildMode,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for FakeChild {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    #[async_trait]
    impl TransportChild for FakeChild {
        async fn wait_status(&mut self) -> Result<String, String> {
            match self.mode {
                FakeChildMode::WaitFailure => Err("synthetic wait failure".into()),
                FakeChildMode::KillFailure => std::future::pending().await,
            }
        }

        fn start_kill_process(&mut self) -> Result<(), String> {
            match self.mode {
                FakeChildMode::WaitFailure => Ok(()),
                FakeChildMode::KillFailure => Err("synthetic kill failure".into()),
            }
        }

        fn try_wait_exited(&mut self) -> Result<bool, String> {
            Ok(false)
        }
    }

    fn completed_task() -> JoinHandle<()> {
        tokio::spawn(async {})
    }

    #[tokio::test]
    async fn child_wait_failure_publishes_exact_terminal_cause() {
        let lifecycle = TransportLifecycle::new(1);
        let mut finalized = lifecycle.receiver();
        let dropped = Arc::new(AtomicBool::new(false));
        run_transport_supervisor_inner(
            Some(FakeChild {
                mode: FakeChildMode::WaitFailure,
                dropped: Arc::clone(&dropped),
            }),
            completed_task(),
            completed_task(),
            completed_task(),
            lifecycle,
        )
        .await
        .expect("wait failure is the terminal cause, not a cleanup error");
        assert_eq!(
            finalized.changed().await,
            TransportTerminalCause::ChildProcessFailure(
                "wait failed: synthetic wait failure".into()
            )
        );
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn kill_failure_does_not_hang_or_detach_supervisor() {
        let lifecycle = TransportLifecycle::new(1);
        let mut finalized = lifecycle.receiver();
        let dropped = Arc::new(AtomicBool::new(false));
        let supervisor = tokio::spawn(run_transport_supervisor_inner(
            Some(FakeChild {
                mode: FakeChildMode::KillFailure,
                dropped: Arc::clone(&dropped),
            }),
            tokio::spawn(std::future::pending()),
            tokio::spawn(std::future::pending()),
            tokio::spawn(std::future::pending()),
            lifecycle.clone(),
        ));
        lifecycle.begin_stop(TransportTerminalCause::ExplicitShutdown);
        let error = timeout(Duration::from_secs(1), supervisor)
            .await
            .expect("supervisor must not hang")
            .expect("supervisor task")
            .expect_err("kill failure is retained");
        assert_eq!(error, "synthetic kill failure");
        assert_eq!(
            finalized.changed().await,
            TransportTerminalCause::ExplicitShutdown
        );
        assert!(dropped.load(Ordering::Acquire));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn child_supervisor_reports_unexpected_exit() {
        let mut command = tokio::process::Command::new("sh");
        command
            .arg("-c")
            .arg("exit 7")
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn child");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = child.stdout.take().expect("stdout");
        let stderr = child.stderr.take().expect("stderr");
        let transport = StdioCodexTransport::from_pipes(
            stdin,
            BufReader::new(stdout),
            BufReader::new(stderr),
            Some(child),
            PathBuf::from("/test-workspace"),
            1,
        );
        let mut terminal = transport.terminal();
        let cause = timeout(Duration::from_secs(1), terminal.changed())
            .await
            .expect("terminal cause");
        assert!(matches!(
            cause,
            TransportTerminalCause::ChildProcessFailure(_) | TransportTerminalCause::StdoutEof
        ));
        transport.shutdown_inner().await.expect("cleanup child");
    }
}
