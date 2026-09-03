use super::{
    CodexStreamError, CodexTransport, HarnessError, NON_JSON_STDOUT_PREVIEW_BYTES,
    bounded_utf8_preview,
};
use async_trait::async_trait;
use codex_codes::jsonrpc::{
    JsonRpcError, JsonRpcErrorData, JsonRpcMessage, JsonRpcNotification, JsonRpcRequest,
    JsonRpcResponse, RequestId,
};
use codex_codes::{Notification, ServerMessage, ServerRequest};
use giskard_harness::{EventLog, EventLogReader, EventStreamError};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
};
use tokio::process::Child;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, error, trace, warn};

const STDOUT_BUFFER_SIZE: usize = 10 * 1024 * 1024;
const WRITER_QUEUE_CAPACITY: usize = 64;
pub(super) const CODEX_INBOX_RETAIN_LIMIT: usize = 65_536;
pub(super) const CODEX_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

type Waiter = oneshot::Sender<Result<Value, HarnessError>>;
type Waiters = Arc<Mutex<HashMap<RequestId, Waiter>>>;

#[derive(Clone)]
enum InboxItem {
    Message(Box<ServerMessage>),
    NonJson {
        parse_error: String,
        raw_preview: String,
        raw_bytes: usize,
    },
    Fatal(String),
    Eof,
}

struct Frame {
    line: String,
    description: String,
    written: Option<oneshot::Sender<Result<(), HarnessError>>>,
    state: Option<Arc<AtomicU8>>,
}

struct Registration {
    waiters: Waiters,
    id: RequestId,
    method: String,
    state: Arc<AtomicU8>,
    abandoned_states: Option<Arc<Mutex<Vec<u8>>>>,
    armed: bool,
}

impl Registration {
    fn disarm(&mut self) {
        self.armed = false;
    }

    fn abandon(&mut self) {
        lock_waiters(&self.waiters).remove(&self.id);
        self.disarm();
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        lock_waiters(&self.waiters).remove(&self.id);
        let state = self.state.load(Ordering::Acquire);
        if let Some(states) = &self.abandoned_states {
            lock_mutex(states).push(state);
        }
        match state {
            0 => debug!(method = %self.method, request_id = %self.id,
                "Codex request timed out before it was queued"),
            1 => debug!(method = %self.method, request_id = %self.id,
                "Codex request timed out after it was queued but before it was written"),
            _ => debug!(method = %self.method, request_id = %self.id,
                "Codex request timed out after it was written"),
        }
    }
}

pub(super) struct StdioTransport {
    writer_tx: Option<mpsc::Sender<Frame>>,
    inbox: Arc<EventLog<InboxItem>>,
    inbox_reader: EventLogReader<InboxItem>,
    waiters: Waiters,
    next_id: Arc<AtomicI64>,
    child: Option<Child>,
    reader_task: Option<JoinHandle<()>>,
    writer_task: Option<JoinHandle<()>>,
    stderr_task: Option<JoinHandle<()>>,
}

impl StdioTransport {
    pub(super) async fn spawn(
        builder: codex_codes::AppServerBuilder,
    ) -> Result<Self, HarnessError> {
        let mut child = builder
            .spawn()
            .await
            .map_err(|error| HarnessError::Spawn(error.to_string()))?;
        let stdin = child.stdin.take().ok_or_else(|| {
            HarnessError::Spawn("Codex app-server stdin was not piped".to_owned())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            HarnessError::Spawn("Codex app-server stdout was not piped".to_owned())
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            HarnessError::Spawn("Codex app-server stderr was not piped".to_owned())
        })?;
        let stderr_task = drain_stderr(stderr);
        Ok(Self::from_io(
            stdout,
            stdin,
            Some(child),
            Some(stderr_task),
            CODEX_INBOX_RETAIN_LIMIT,
        ))
    }

    fn from_io<R, W>(
        reader: R,
        writer: W,
        child: Option<Child>,
        stderr_task: Option<JoinHandle<()>>,
        inbox_limit: usize,
    ) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let inbox = Arc::new(EventLog::with_limit(inbox_limit));
        // The reader must exist before the producer starts. Otherwise eviction before the first
        // call to next_message would be invisible rather than reported as a Gap.
        let inbox_reader = inbox.reader();
        let waiters = Arc::new(Mutex::new(HashMap::new()));
        let (writer_tx, writer_rx) = mpsc::channel(WRITER_QUEUE_CAPACITY);
        let reader_task = tokio::spawn(read_stdout(reader, inbox.clone(), waiters.clone()));
        let writer_task = tokio::spawn(write_stdin(writer, writer_rx, waiters.clone()));
        Self {
            writer_tx: Some(writer_tx),
            inbox,
            inbox_reader,
            waiters,
            next_id: Arc::new(AtomicI64::new(1)),
            child,
            reader_task: Some(reader_task),
            writer_task: Some(writer_task),
            stderr_task,
        }
    }

    #[cfg(test)]
    fn from_pipes<R, W>(reader: R, writer: W) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        Self::from_io(reader, writer, None, None, CODEX_INBOX_RETAIN_LIMIT)
    }

    pub(super) async fn send_notification(&self, method: &str) -> Result<(), HarnessError> {
        self.send_frame(
            &JsonRpcNotification {
                method: method.to_owned(),
                params: None,
            },
            format!("notification {method}"),
        )
        .await
    }

    async fn send_frame<T: Serialize>(
        &self,
        value: &T,
        description: String,
    ) -> Result<(), HarnessError> {
        send_frame(self.writer_tx.as_ref().cloned(), value, description).await
    }

    fn fail_waiters(&self, message: &str) {
        fail_all_waiters(&self.waiters, message);
    }
}

#[async_trait]
impl CodexTransport for StdioTransport {
    async fn request_json(&mut self, method: &str, params: Value) -> Result<Value, HarnessError> {
        request_json(
            self.writer_tx.as_ref().cloned(),
            self.waiters.clone(),
            self.next_id.clone(),
            method,
            params,
            None,
        )
        .await
    }

    async fn next_message(&mut self) -> Result<Option<ServerMessage>, CodexStreamError> {
        match self.inbox_reader.recv().await {
            Ok(InboxItem::Message(message)) => Ok(Some(*message)),
            Ok(InboxItem::NonJson {
                parse_error,
                raw_preview,
                raw_bytes,
            }) => Err(CodexStreamError::NonJsonStdout {
                parse_error,
                raw_preview,
                raw_bytes,
            }),
            Ok(InboxItem::Fatal(message)) => {
                Err(CodexStreamError::Fatal(HarnessError::Transport(message)))
            }
            Ok(InboxItem::Eof) | Err(EventStreamError::Closed) => Ok(None),
            Err(EventStreamError::Gap { dropped }) => {
                Err(CodexStreamError::Fatal(HarnessError::Transport(format!(
                    "Codex inbox overflowed; {dropped} frames dropped"
                ))))
            }
        }
    }

    async fn respond_json(&mut self, id: RequestId, value: Value) -> Result<(), HarnessError> {
        self.send_frame(
            &JsonRpcResponse {
                id: id.clone(),
                result: value,
            },
            format!("response id {id}"),
        )
        .await
    }

    async fn respond_error_json(
        &mut self,
        id: RequestId,
        code: i64,
        message: &str,
    ) -> Result<(), HarnessError> {
        self.send_frame(
            &JsonRpcError {
                id: id.clone(),
                error: JsonRpcErrorData {
                    code,
                    message: message.to_owned(),
                    data: None,
                },
            },
            format!("error response id {id}"),
        )
        .await
    }

    async fn shutdown_transport(mut self) -> Result<(), HarnessError> {
        self.writer_tx.take();
        self.fail_waiters("Codex transport shut down");
        self.inbox.close();
        if let Some(writer_task) = self.writer_task.take() {
            let _ = writer_task.await;
        }
        if let Some(mut child) = self.child.take() {
            child
                .kill()
                .await
                .map_err(|error| HarnessError::Transport(error.to_string()))?;
        }
        if let Some(reader_task) = self.reader_task.take() {
            reader_task.abort();
            let _ = reader_task.await;
        }
        if let Some(stderr_task) = self.stderr_task.take() {
            stderr_task.abort();
        }
        Ok(())
    }
}

impl Drop for StdioTransport {
    fn drop(&mut self) {
        self.writer_tx.take();
        self.fail_waiters("Codex transport shut down");
        self.inbox.close();
        if let Some(child) = &mut self.child
            && let Err(error) = child.start_kill()
        {
            warn!(%error, "failed to kill Codex app-server while dropping transport");
        }
        if let Some(task) = &self.reader_task {
            task.abort();
        }
        if let Some(task) = &self.writer_task {
            task.abort();
        }
        if let Some(task) = &self.stderr_task {
            task.abort();
        }
    }
}

async fn request_json(
    writer_tx: Option<mpsc::Sender<Frame>>,
    waiters: Waiters,
    next_id: Arc<AtomicI64>,
    method: &str,
    params: Value,
    abandoned_states: Option<Arc<Mutex<Vec<u8>>>>,
) -> Result<Value, HarnessError> {
    let id = RequestId::Integer(next_id.fetch_add(1, Ordering::Relaxed));
    let (response_tx, response_rx) = oneshot::channel();
    let state = Arc::new(AtomicU8::new(0));
    lock_waiters(&waiters).insert(id.clone(), response_tx);
    let mut registration = Registration {
        waiters,
        id: id.clone(),
        method: method.to_owned(),
        state: state.clone(),
        abandoned_states,
        armed: true,
    };
    let line = match serialize_line(&JsonRpcRequest {
        id: id.clone(),
        method: method.to_owned(),
        params: Some(params),
    }) {
        Ok(line) => line,
        Err(error) => {
            registration.abandon();
            return Err(error);
        }
    };
    let Some(writer_tx) = writer_tx else {
        registration.abandon();
        return Err(transport_closed());
    };
    if writer_tx
        .send(Frame {
            line,
            description: format!("request {method} id {id}"),
            written: None,
            state: Some(state.clone()),
        })
        .await
        .is_err()
    {
        registration.abandon();
        return Err(transport_closed());
    }
    // The writer can finish before send returns. Do not overwrite its stronger state.
    let _ = state.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire);
    match response_rx.await {
        Ok(result) => {
            registration.disarm();
            result
        }
        Err(_) => {
            registration.abandon();
            Err(transport_closed())
        }
    }
}

async fn send_frame<T: Serialize>(
    writer_tx: Option<mpsc::Sender<Frame>>,
    value: &T,
    description: String,
) -> Result<(), HarnessError> {
    let line = serialize_line(value)?;
    let (written_tx, written_rx) = oneshot::channel();
    writer_tx
        .ok_or_else(transport_closed)?
        .send(Frame {
            line,
            description,
            written: Some(written_tx),
            state: None,
        })
        .await
        .map_err(|_| transport_closed())?;
    written_rx.await.map_err(|_| transport_closed())?
}

async fn read_stdout<R>(reader: R, inbox: Arc<EventLog<InboxItem>>, waiters: Waiters)
where
    R: AsyncRead + Unpin,
{
    let mut reader = BufReader::with_capacity(STDOUT_BUFFER_SIZE, reader);
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        let mut limited = (&mut reader).take(CODEX_MAX_FRAME_BYTES as u64 + 1);
        match limited.read_until(b'\n', &mut buffer).await {
            Ok(0) => {
                inbox.append(InboxItem::Eof);
                fail_all_waiters(&waiters, "Codex stream closed");
                inbox.close();
                return;
            }
            Ok(_) => {}
            Err(error) => {
                inbox.append(InboxItem::Fatal(format!(
                    "failed to read Codex stdout: {error}"
                )));
                fail_all_waiters(&waiters, "Codex stream read failed");
                inbox.close();
                return;
            }
        }
        if buffer.len() > CODEX_MAX_FRAME_BYTES {
            inbox.append(InboxItem::Fatal(format!(
                "Codex stdout frame exceeded {CODEX_MAX_FRAME_BYTES} bytes"
            )));
            fail_all_waiters(&waiters, "Codex stream produced an oversized frame");
            inbox.close();
            return;
        }
        let mut line = String::from_utf8_lossy(&buffer).into_owned();
        trim_line_ending(&mut line);
        if line.trim().is_empty() {
            continue;
        }
        let envelope = match serde_json::from_str::<JsonRpcMessage>(&line) {
            Ok(envelope) => envelope,
            Err(error) if !line.trim_start().starts_with('{') => {
                inbox.append(InboxItem::NonJson {
                    parse_error: error.to_string(),
                    raw_preview: bounded_utf8_preview(&line, NON_JSON_STDOUT_PREVIEW_BYTES),
                    raw_bytes: line.len(),
                });
                continue;
            }
            Err(error) => {
                append_fatal_decode(&inbox, "unknown", &line, &error.to_string());
                fail_all_waiters(&waiters, "Codex stream contained malformed JSON-RPC");
                inbox.close();
                return;
            }
        };
        match envelope {
            JsonRpcMessage::Response(response) => {
                deliver_response(&waiters, response.id, Ok(response.result));
            }
            JsonRpcMessage::Error(response) => {
                let message = format!(
                    "JSON-RPC error ({}): {}",
                    response.error.code, response.error.message
                );
                deliver_response(&waiters, response.id, Err(HarnessError::Transport(message)));
            }
            JsonRpcMessage::Notification(JsonRpcNotification { method, params }) => {
                match Notification::from_envelope(&method, params) {
                    Ok(notification) => {
                        inbox.append(InboxItem::Message(Box::new(ServerMessage::Notification(
                            notification,
                        ))));
                    }
                    Err(error) => {
                        append_fatal_decode(&inbox, &method, &line, &error.to_string());
                        fail_all_waiters(&waiters, "Codex notification decode failed");
                        inbox.close();
                        return;
                    }
                }
            }
            JsonRpcMessage::Request(JsonRpcRequest { id, method, params }) => {
                match ServerRequest::from_envelope(&method, params) {
                    Ok(request) => {
                        inbox.append(InboxItem::Message(Box::new(ServerMessage::Request {
                            id,
                            request,
                        })));
                    }
                    Err(error) => {
                        append_fatal_decode(&inbox, &method, &line, &error.to_string());
                        fail_all_waiters(&waiters, "Codex request decode failed");
                        inbox.close();
                        return;
                    }
                }
            }
        }
    }
}

async fn write_stdin<W>(writer: W, mut frames: mpsc::Receiver<Frame>, waiters: Waiters)
where
    W: AsyncWrite + Unpin,
{
    let mut writer = BufWriter::new(writer);
    while let Some(frame) = frames.recv().await {
        let result = async {
            writer.write_all(frame.line.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await
        }
        .await
        .map_err(|error| HarnessError::Transport(error.to_string()));
        if result.is_ok()
            && let Some(state) = &frame.state
        {
            state.store(2, Ordering::Release);
        }
        if let Some(written) = frame.written {
            let _ = written.send(result.clone());
        }
        if let Err(error) = result {
            error!(description = %frame.description, %error, "failed to write Codex JSON-RPC frame");
            fail_all_waiters(&waiters, &error.to_string());
            return;
        }
    }
}

fn append_fatal_decode(inbox: &EventLog<InboxItem>, method: &str, line: &str, error: &str) {
    let raw_preview = bounded_utf8_preview(line, NON_JSON_STDOUT_PREVIEW_BYTES);
    inbox.append(InboxItem::Fatal(format!(
        "Codex JSON-RPC deserialization error for method {method}: {error} \
         (raw_bytes: {}, raw_preview: {raw_preview:?})",
        line.len()
    )));
}

fn deliver_response(waiters: &Waiters, id: RequestId, result: Result<Value, HarnessError>) {
    if let Some(waiter) = lock_waiters(waiters).remove(&id) {
        let _ = waiter.send(result);
    } else {
        debug!(request_id = %id, "dropping Codex response without a pending request");
    }
}

fn fail_all_waiters(waiters: &Waiters, message: &str) {
    let pending = std::mem::take(&mut *lock_waiters(waiters));
    for (_, waiter) in pending {
        let _ = waiter.send(Err(HarnessError::Transport(message.to_owned())));
    }
}

fn lock_waiters(waiters: &Waiters) -> MutexGuard<'_, HashMap<RequestId, Waiter>> {
    lock_mutex(waiters)
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!("Codex response-correlation lock was poisoned; recovering");
            poisoned.into_inner()
        }
    }
}

fn serialize_line<T: Serialize>(value: &T) -> Result<String, HarnessError> {
    let line =
        serde_json::to_string(value).map_err(|error| HarnessError::Protocol(error.to_string()))?;
    if line.contains(['\r', '\n']) {
        return Err(HarnessError::Protocol(
            "raw app-server frame contains an embedded line break".to_owned(),
        ));
    }
    Ok(line)
}

fn trim_line_ending(line: &mut String) {
    if line.ends_with('\n') {
        line.pop();
        if line.ends_with('\r') {
            line.pop();
        }
    }
}

fn transport_closed() -> HarnessError {
    HarnessError::Transport("Codex transport closed".to_owned())
}

fn drain_stderr(stderr: tokio::process::ChildStderr) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => return,
                Ok(_) => {
                    let line = strip_ansi(&line);
                    let line = line.trim_end_matches(['\n', '\r']);
                    if line.contains(" ERROR ") {
                        error!(target: "codex_codes::stderr", "{line}");
                    } else if line.contains(" WARN ") {
                        warn!(target: "codex_codes::stderr", "{line}");
                    } else if line.contains(" DEBUG ") {
                        debug!(target: "codex_codes::stderr", "{line}");
                    } else {
                        trace!(target: "codex_codes::stderr", "{line}");
                    }
                }
                Err(error) => {
                    debug!(%error, "stopped draining Codex stderr");
                    return;
                }
            }
        }
    })
}

fn strip_ansi(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for control in chars.by_ref() {
                if !(control.is_ascii_digit() || control == ';') {
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
    use serde_json::json;
    use std::io::Write;
    use std::pin::Pin;
    use std::sync::atomic::AtomicBool;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf, duplex, split};
    use tokio::time::{Duration, timeout};

    struct Peer {
        reader: BufReader<ReadHalf<DuplexStream>>,
        writer: WriteHalf<DuplexStream>,
    }

    struct GatedWriter {
        polled: Arc<AtomicBool>,
    }

    #[derive(Clone)]
    struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CapturedLogWriter {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            lock_mutex(&self.0).extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn capture_debug_logs(log: impl FnOnce()) -> String {
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer_output = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || CapturedLogWriter(writer_output.clone()))
            .finish();
        tracing::subscriber::with_default(subscriber, log);
        String::from_utf8(lock_mutex(&output).clone()).unwrap()
    }

    impl AsyncWrite for GatedWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buffer: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.polled.store(true, Ordering::Release);
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn test_transport(limit: usize) -> (StdioTransport, Peer) {
        let (transport_pipe, peer_pipe) = duplex(1024 * 1024);
        let (transport_reader, transport_writer) = split(transport_pipe);
        let (peer_reader, peer_writer) = split(peer_pipe);
        (
            StdioTransport::from_io(transport_reader, transport_writer, None, None, limit),
            Peer {
                reader: BufReader::new(peer_reader),
                writer: peer_writer,
            },
        )
    }

    impl Peer {
        async fn read_json(&mut self) -> Value {
            let mut line = String::new();
            self.reader.read_line(&mut line).await.unwrap();
            serde_json::from_str(&line).unwrap()
        }

        async fn write_json(&mut self, value: Value) {
            self.writer
                .write_all(format!("{value}\n").as_bytes())
                .await
                .unwrap();
        }

        async fn write_raw(&mut self, value: &str) {
            self.writer.write_all(value.as_bytes()).await.unwrap();
            self.writer.write_all(b"\n").await.unwrap();
        }
    }

    #[tokio::test]
    async fn notifications_during_a_request_are_delivered_after_it() {
        let (mut transport, mut peer) = test_transport(CODEX_INBOX_RETAIN_LIMIT);
        let request = transport.request_json("test/request", json!({"value": 1}));
        let server = async {
            let frame = peer.read_json().await;
            assert_eq!(frame["id"], 1);
            peer.write_json(json!({"method":"unknown/one","params":{"n":1}}))
                .await;
            peer.write_json(json!({"method":"unknown/two","params":{"n":2}}))
                .await;
            peer.write_json(json!({"id":1,"result":{"ok":true}})).await;
        };
        let (response, ()) = tokio::join!(request, server);
        assert_eq!(response.unwrap(), json!({"ok": true}));

        for expected in ["unknown/one", "unknown/two"] {
            let message = transport.next_message().await.unwrap().unwrap();
            let ServerMessage::Notification(Notification::Unknown { method, .. }) = message else {
                panic!("expected unknown notification");
            };
            assert_eq!(method, expected);
        }
    }

    #[tokio::test]
    async fn responses_are_correlated_and_notifications_keep_their_order() {
        let (mut transport, mut peer) = test_transport(CODEX_INBOX_RETAIN_LIMIT);
        let writer = transport.writer_tx.as_ref().unwrap().clone();
        let waiters = transport.waiters.clone();
        let next_id = transport.next_id.clone();
        let first = request_json(
            Some(writer.clone()),
            waiters.clone(),
            next_id.clone(),
            "request/first",
            json!({}),
            None,
        );
        let second = request_json(
            Some(writer),
            waiters,
            next_id,
            "request/second",
            json!({}),
            None,
        );
        let server = async {
            let first_frame = peer.read_json().await;
            let second_frame = peer.read_json().await;
            let ids = HashMap::from([
                (
                    first_frame["method"].as_str().unwrap().to_owned(),
                    first_frame["id"].clone(),
                ),
                (
                    second_frame["method"].as_str().unwrap().to_owned(),
                    second_frame["id"].clone(),
                ),
            ]);
            peer.write_json(json!({"method":"unknown/one"})).await;
            peer.write_json(json!({"id":ids["request/second"],"result":"second"}))
                .await;
            peer.write_json(json!({"method":"unknown/two"})).await;
            peer.write_json(json!({"id":ids["request/first"],"result":"first"}))
                .await;
        };
        let (first, second, ()) = tokio::join!(first, second, server);
        assert_eq!(first.unwrap(), json!("first"));
        assert_eq!(second.unwrap(), json!("second"));
        for expected in ["unknown/one", "unknown/two"] {
            let ServerMessage::Notification(Notification::Unknown { method, .. }) =
                transport.next_message().await.unwrap().unwrap()
            else {
                panic!("expected unknown notification");
            };
            assert_eq!(method, expected);
        }
    }

    #[tokio::test]
    async fn writer_never_interleaves_frames() {
        let (transport, mut peer) = test_transport(CODEX_INBOX_RETAIN_LIMIT);
        let sender = transport.writer_tx.as_ref().unwrap().clone();
        let mut tasks = Vec::new();
        for index in 0..5 {
            let request_sender = sender.clone();
            let waiters = transport.waiters.clone();
            let next_id = transport.next_id.clone();
            tasks.push(tokio::spawn(async move {
                request_json(
                    Some(request_sender),
                    waiters,
                    next_id,
                    &format!("request/{index}"),
                    json!({"body":"x".repeat(4096)}),
                    None,
                )
                .await
            }));
            let response_sender = sender.clone();
            tasks.push(tokio::spawn(async move {
                send_frame(
                    Some(response_sender),
                    &JsonRpcResponse {
                        id: RequestId::Integer(100 + index),
                        result: json!({"body":"x".repeat(4096)}),
                    },
                    format!("response {index}"),
                )
                .await
                .map(|()| Value::Null)
            }));
        }
        let mut request_count = 0;
        let mut response_count = 0;
        while request_count + response_count < 10 {
            let frame = peer.read_json().await;
            if frame.get("method").is_some() {
                request_count += 1;
                peer.write_json(json!({"id":frame["id"],"result":null}))
                    .await;
            } else {
                response_count += 1;
                assert!(frame.get("result").is_some());
            }
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert_eq!((request_count, response_count), (5, 5));
    }

    #[tokio::test]
    async fn a_late_response_for_a_timed_out_request_is_dropped() {
        let (mut transport, mut peer) = test_transport(CODEX_INBOX_RETAIN_LIMIT);
        let first = timeout(
            Duration::from_millis(20),
            transport.request_json("first", json!({})),
        );
        let read_first = peer.read_json();
        let (timed_out, first_frame) = tokio::join!(first, read_first);
        assert!(timed_out.is_err());
        assert_eq!(first_frame["id"], 1);
        peer.write_json(json!({"id":1,"result":"late"})).await;

        let second = transport.request_json("second", json!({}));
        let server = async {
            let frame = peer.read_json().await;
            assert_eq!(frame["id"], 2);
            peer.write_json(json!({"id":2,"result":"current"})).await;
        };
        let (response, ()) = tokio::join!(second, server);
        assert_eq!(response.unwrap(), json!("current"));
    }

    #[test]
    fn a_late_response_is_logged_exactly_once() {
        let waiters = Arc::new(Mutex::new(HashMap::new()));
        let output = capture_debug_logs(|| {
            deliver_response(&waiters, RequestId::Integer(7), Ok(json!("late")));
        });
        assert_eq!(
            output
                .matches("dropping Codex response without a pending request")
                .count(),
            1
        );
        assert!(output.contains("request_id=7"), "{output}");
    }

    #[tokio::test]
    async fn timeout_records_written_and_not_queued_states() {
        let (transport, mut peer) = test_transport(CODEX_INBOX_RETAIN_LIMIT);
        let written_states = Arc::new(Mutex::new(Vec::new()));
        let request = timeout(
            Duration::from_millis(20),
            request_json(
                transport.writer_tx.as_ref().cloned(),
                transport.waiters.clone(),
                transport.next_id.clone(),
                "written",
                json!({}),
                Some(written_states.clone()),
            ),
        );
        let read = peer.read_json();
        let (timed_out, _) = tokio::join!(request, read);
        assert!(timed_out.is_err());
        assert_eq!(*lock_mutex(&written_states), vec![2]);

        let (reader, _peer) = duplex(64);
        let polled = Arc::new(AtomicBool::new(false));
        let transport = StdioTransport::from_io(
            reader,
            GatedWriter {
                polled: polled.clone(),
            },
            None,
            None,
            CODEX_INBOX_RETAIN_LIMIT,
        );
        let sender = transport.writer_tx.as_ref().unwrap().clone();
        sender
            .send(Frame {
                line: "{}".into(),
                description: "blocked frame".into(),
                written: None,
                state: None,
            })
            .await
            .unwrap();
        while !polled.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        for index in 0..WRITER_QUEUE_CAPACITY {
            sender
                .send(Frame {
                    line: "{}".into(),
                    description: format!("queued frame {index}"),
                    written: None,
                    state: None,
                })
                .await
                .unwrap();
        }
        let unqueued_states = Arc::new(Mutex::new(Vec::new()));
        assert!(
            timeout(
                Duration::from_millis(20),
                request_json(
                    Some(sender),
                    transport.waiters.clone(),
                    transport.next_id.clone(),
                    "not-queued",
                    json!({}),
                    Some(unqueued_states.clone()),
                ),
            )
            .await
            .is_err()
        );
        assert_eq!(*lock_mutex(&unqueued_states), vec![0]);
    }

    #[tokio::test]
    async fn eof_drains_the_inbox_then_reports_none() {
        let (mut transport, mut peer) = test_transport(CODEX_INBOX_RETAIN_LIMIT);
        let pending = request_json(
            transport.writer_tx.as_ref().cloned(),
            transport.waiters.clone(),
            transport.next_id.clone(),
            "pending",
            json!({}),
            None,
        );
        let establish_pending = async {
            peer.read_json().await;
            peer.write_json(json!({"method":"unknown/one"})).await;
            peer.write_json(json!({"method":"unknown/two"})).await;
            peer.writer.shutdown().await.unwrap();
        };
        let (pending_result, ()) = tokio::join!(pending, establish_pending);

        assert!(transport.next_message().await.unwrap().is_some());
        assert!(transport.next_message().await.unwrap().is_some());
        assert!(transport.next_message().await.unwrap().is_none());
        assert!(matches!(
            pending_result,
            Err(HarnessError::Transport(message)) if message == "Codex stream closed"
        ));
    }

    #[tokio::test]
    async fn an_oversized_frame_is_fatal() {
        let (mut transport, mut peer) = test_transport(CODEX_INBOX_RETAIN_LIMIT);
        let (waiter_tx, waiter_rx) = oneshot::channel();
        lock_waiters(&transport.waiters).insert(RequestId::Integer(7), waiter_tx);

        let mut frame = vec![b'x'; CODEX_MAX_FRAME_BYTES];
        frame.push(b'\n');
        peer.writer.write_all(&frame).await.unwrap();

        assert!(matches!(
            waiter_rx.await.unwrap(),
            Err(HarnessError::Transport(message))
                if message == "Codex stream produced an oversized frame"
        ));
        assert!(matches!(
            transport.next_message().await,
            Err(CodexStreamError::Fatal(HarnessError::Transport(message)))
                if message == format!(
                    "Codex stdout frame exceeded {CODEX_MAX_FRAME_BYTES} bytes"
                )
        ));
        assert!(transport.next_message().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_frame_at_the_limit_is_accepted() {
        let (mut transport, mut peer) = test_transport(CODEX_INBOX_RETAIN_LIMIT);
        let mut frame = vec![b'x'; CODEX_MAX_FRAME_BYTES - 1];
        frame.push(b'\n');
        peer.writer.write_all(&frame).await.unwrap();

        let Err(CodexStreamError::NonJsonStdout { raw_bytes, .. }) = transport.next_message().await
        else {
            panic!("expected an accepted non-JSON frame");
        };
        assert_eq!(raw_bytes, CODEX_MAX_FRAME_BYTES - 1);
    }

    #[tokio::test]
    async fn non_json_is_recoverable_and_valid_message_precedes_fatal_garbage() {
        let (mut transport, mut peer) = test_transport(CODEX_INBOX_RETAIN_LIMIT);
        let raw_non_json = format!("{}é", "x".repeat(NON_JSON_STDOUT_PREVIEW_BYTES - 1));
        peer.write_raw("   ").await;
        peer.write_raw(&raw_non_json).await;
        peer.write_json(json!({"method":"unknown/valid"})).await;
        peer.write_raw(r#"{"method":"turn/completed""#).await;

        let Err(CodexStreamError::NonJsonStdout {
            raw_preview,
            raw_bytes,
            ..
        }) = transport.next_message().await
        else {
            panic!("expected recoverable non-JSON line");
        };
        assert_eq!(raw_preview.len(), NON_JSON_STDOUT_PREVIEW_BYTES - 1);
        assert_eq!(raw_bytes, raw_non_json.len());
        assert!(transport.next_message().await.unwrap().is_some());
        assert!(matches!(
            transport.next_message().await,
            Err(CodexStreamError::Fatal(HarnessError::Transport(_)))
        ));
    }

    #[tokio::test]
    async fn parseable_invalid_envelope_and_typed_decode_error_are_fatal() {
        let (mut invalid_envelope, mut peer) = test_transport(CODEX_INBOX_RETAIN_LIMIT);
        peer.write_raw(r#"{"unexpected":true}"#).await;
        assert!(matches!(
            invalid_envelope.next_message().await,
            Err(CodexStreamError::Fatal(HarnessError::Transport(_)))
        ));

        let (mut typed_error, mut peer) = test_transport(CODEX_INBOX_RETAIN_LIMIT);
        peer.write_json(json!({"method":"turn/completed","params":{"unexpected":true}}))
            .await;
        assert!(matches!(
            typed_error.next_message().await,
            Err(CodexStreamError::Fatal(HarnessError::Transport(_)))
        ));
    }

    #[tokio::test]
    async fn inbox_overflow_is_fatal_with_a_count() {
        let (mut transport, mut peer) = test_transport(2);
        let (barrier_tx, barrier_rx) = oneshot::channel();
        lock_waiters(&transport.waiters).insert(RequestId::Integer(99), barrier_tx);
        for index in 0..5 {
            peer.write_json(json!({"method":"unknown/event","params":{"index":index}}))
                .await;
        }
        peer.write_json(json!({"id":99,"result":null})).await;
        barrier_rx.await.unwrap().unwrap();
        let Err(CodexStreamError::Fatal(HarnessError::Transport(message))) =
            transport.next_message().await
        else {
            panic!("expected fatal overflow");
        };
        assert!(message.contains("3 frames dropped"), "{message}");
    }

    #[tokio::test]
    async fn initialize_gets_id_one_and_initialized_has_no_id() {
        let (mut transport, mut peer) = test_transport(CODEX_INBOX_RETAIN_LIMIT);
        let initialize = transport.request_json("initialize", json!({}));
        let server = async {
            let frame = peer.read_json().await;
            assert_eq!(frame["id"], 1);
            peer.write_json(json!({"id":1,"result":{}})).await;
        };
        let (response, ()) = tokio::join!(initialize, server);
        response.unwrap();
        transport.send_notification("initialized").await.unwrap();
        let frame = peer.read_json().await;
        assert_eq!(frame, json!({"method":"initialized"}));
    }

    #[tokio::test]
    async fn shutdown_fails_pending_waiters_and_closes_the_inbox() {
        let (transport, _peer) = test_transport(CODEX_INBOX_RETAIN_LIMIT);
        let mut inbox_reader = transport.inbox.reader();
        let (waiter_tx, waiter_rx) = oneshot::channel();
        lock_waiters(&transport.waiters).insert(RequestId::Integer(7), waiter_tx);
        transport.shutdown_transport().await.unwrap();
        assert!(matches!(
            waiter_rx.await.unwrap(),
            Err(HarnessError::Transport(message)) if message == "Codex transport shut down"
        ));
        assert!(matches!(
            inbox_reader.recv().await,
            Err(EventStreamError::Closed)
        ));
    }

    #[test]
    fn stderr_ansi_stripping_preserves_unicode() {
        assert_eq!(strip_ansi("\u{1b}[32mréussi\u{1b}[0m"), "réussi");
    }

    #[tokio::test]
    async fn from_pipes_constructs_a_transport() {
        let (reader, _) = duplex(64);
        let (writer, _) = duplex(64);
        let _transport = StdioTransport::from_pipes(reader, writer);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_kills_a_real_child_process() {
        use std::process::Stdio;

        let mut child = tokio::process::Command::new("sh")
            .args(["-c", "while :; do sleep 1; done"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let transport = StdioTransport::from_io(
            stdout,
            stdin,
            Some(child),
            Some(drain_stderr(stderr)),
            CODEX_INBOX_RETAIN_LIMIT,
        );
        timeout(Duration::from_secs(1), transport.shutdown_transport())
            .await
            .unwrap()
            .unwrap();
    }
}
