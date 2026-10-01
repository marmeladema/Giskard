//! The supervisor: one task per `claude` child.
//!
//! The task is the single owner of the child process, the [`ClaudeMapper`], the pending
//! control-request waiters and the thread's retained [`EventLog`]. Nothing else touches them: the
//! façade reaches the task only through its command channel, so there is no lock around the
//! mapper or the stdin handle (the `CodexInstance` rule of `AGENTS.md`, applied per child).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use giskard_core::error::HarnessError;
use giskard_core::ids::{ApprovalId, ServerRequestId, ThreadId, TurnId};
use giskard_core::model::ModelRef;
use giskard_harness::EventLog;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::log_fields::display_opt;
use crate::mapper::{ClaudeMapper, MapperOutput, TurnKind};
use crate::process::{ChildExit, ChildLogContext, ClaudeChild};

/// How long the stop sequence waits for an interrupted turn's `result`.
pub(crate) const STOP_INTERRUPT_GRACE: Duration = Duration::from_secs(5);
/// How long the stop sequence waits for EOF after closing stdin before it kills the child.
pub(crate) const STOP_EXIT_GRACE: Duration = Duration::from_secs(5);

/// What the façade asks a supervisor to do.
pub(crate) enum ChildCommand {
    StartTurn {
        line: String,
        turn: TurnId,
        model: ModelRef,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    },
    Interrupt {
        reply: oneshot::Sender<Result<(), HarnessError>>,
    },
    /// A control request whose success payload the caller wants: `rename_session` now,
    /// milestone 3's `set_permission_mode`, `set_model` and `get_settings` later.
    Control {
        request: Value,
        reply: oneshot::Sender<Result<Value, HarnessError>>,
    },
    Stop {
        reply: oneshot::Sender<()>,
    },
}

/// The façade's view of one live child.
pub(crate) struct ChildHandle {
    pub harness_thread_id: String,
    pub log: Arc<EventLog>,
    pub commands: mpsc::Sender<ChildCommand>,
    pub task: JoinHandle<()>,
    /// The model the thread was opened on; turns run on it until milestone 3.
    pub model: ModelRef,
    /// Distinguishes this child from a later one for the same thread, so a supervisor that ends
    /// after its thread was reopened never removes the new entry.
    pub generation: u64,
}

pub(crate) type Children = Arc<Mutex<HashMap<ThreadId, ChildHandle>>>;
pub(crate) type Pending = Arc<Mutex<PendingRequests>>;

/// Lock a std mutex whose guarded maps stay consistent even if a holder panicked.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One ask the CLI published and nothing has answered yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingAsk {
    pub thread: ThreadId,
    /// The CLI's `request_id` the answer must carry.
    pub request_id: String,
    pub tool_use_id: Option<String>,
}

/// Approvals and server requests awaiting milestone 3's `respond_*`. The ids are the CLI's own
/// `request_id` UUIDs, which is what makes them unique across this instance's children.
#[derive(Debug, Default)]
pub(crate) struct PendingRequests {
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: The approval half of `ClaudeHarness::pending` (see that field).
    // Source of truth: A supervisor inserts on the mapper's `PendingApproval`.
    // Structural reason: `respond_approval` carries no thread.
    // Synchronization: The façade's std mutex around this struct.
    // Invalidation/removal: Child exit, thread stop and `shutdown`; milestone 3 on answer.
    approvals: HashMap<ApprovalId, PendingAsk>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: The server-request half of `ClaudeHarness::pending` (see that field).
    // Source of truth: A supervisor inserts on the mapper's `PendingServerRequest`.
    // Structural reason: `respond_server_request` carries no thread.
    // Synchronization: The façade's std mutex around this struct.
    // Invalidation/removal: Child exit, thread stop and `shutdown`; milestone 3 on answer.
    server_requests: HashMap<ServerRequestId, PendingAsk>,
}

impl PendingRequests {
    pub fn insert_approval(&mut self, id: ApprovalId, ask: PendingAsk) {
        self.approvals.insert(id, ask);
    }

    pub fn insert_server_request(&mut self, id: ServerRequestId, ask: PendingAsk) {
        self.server_requests.insert(id, ask);
    }

    pub fn approval(&self, id: &ApprovalId) -> Option<&PendingAsk> {
        self.approvals.get(id)
    }

    pub fn server_request(&self, id: &ServerRequestId) -> Option<&PendingAsk> {
        self.server_requests.get(id)
    }

    /// Drop every ask of one thread; returns how many there were.
    pub fn remove_thread(&mut self, thread: ThreadId) -> usize {
        let before = self.len();
        self.approvals.retain(|_, ask| ask.thread != thread);
        self.server_requests.retain(|_, ask| ask.thread != thread);
        before - self.len()
    }

    pub fn clear(&mut self) -> usize {
        let count = self.len();
        self.approvals.clear();
        self.server_requests.clear();
        count
    }

    pub fn len(&self) -> usize {
        self.approvals.len() + self.server_requests.len()
    }
}

/// A fresh control-request id. The CLI echoes it on the response.
pub(crate) fn new_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// One stdin control request line.
pub(crate) fn control_line(request_id: &str, request: &Value) -> String {
    json!({"type": "control_request", "request_id": request_id, "request": request}).to_string()
}

/// A control response's outcome: the `response` payload of a success, the `error` of a failure.
pub(crate) fn control_outcome(payload: &Value) -> Result<Value, HarnessError> {
    match payload.get("subtype").and_then(Value::as_str) {
        Some("success") => Ok(payload.get("response").cloned().unwrap_or(Value::Null)),
        Some("error") => Err(HarnessError::Protocol(
            payload
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("Claude Code rejected the control request")
                .to_owned(),
        )),
        other => Err(HarnessError::Protocol(format!(
            "unexpected control response subtype {other:?}"
        ))),
    }
}

fn child_stopped() -> HarnessError {
    HarnessError::Transport("claude child stopped".into())
}

/// Who waits for a control response.
enum Waiter {
    Unit(oneshot::Sender<Result<(), HarnessError>>),
    Value(oneshot::Sender<Result<Value, HarnessError>>),
    /// The stop sequence's own interrupt: nobody awaits the response.
    Stop,
}

impl Waiter {
    fn resolve(self, result: Result<Value, HarnessError>) {
        // A caller that gave up (its timeout fired) has dropped the receiver; nothing to tell.
        match self {
            Waiter::Unit(reply) => {
                let _ = reply.send(result.map(|_| ()));
            }
            Waiter::Value(reply) => {
                let _ = reply.send(result);
            }
            Waiter::Stop => {}
        }
    }
}

/// Everything one supervisor task owns, handed over at spawn.
pub(crate) struct SupervisorParts {
    pub child: Box<dyn ClaudeChild>,
    pub mapper: ClaudeMapper,
    pub log: Arc<EventLog>,
    pub commands: mpsc::Receiver<ChildCommand>,
    pub shutdown: watch::Receiver<bool>,
    pub children: Children,
    pub pending: Pending,
    pub generation: u64,
    pub thread: ThreadId,
    pub context: ChildLogContext,
    /// Lines the handshake read that were not its own responses, mapped first.
    pub early_lines: Vec<String>,
    /// Handshake requests that timed out; the CLI may still answer them.
    pub abandoned_requests: Vec<String>,
}

pub(crate) fn spawn_supervisor(parts: SupervisorParts) -> JoinHandle<()> {
    let supervisor = Supervisor {
        pid: parts.child.pid(),
        child: parts.child,
        mapper: parts.mapper,
        log: parts.log,
        commands: parts.commands,
        shutdown: parts.shutdown,
        children: parts.children,
        pending: parts.pending,
        generation: parts.generation,
        thread: parts.thread,
        context: parts.context,
        waiters: HashMap::new(),
        early_lines: parts.early_lines,
        abandoned: parts.abandoned_requests.into_iter().collect(),
        eof: false,
        interrupt_sent: false,
        dropped_events: 0,
        failure: None,
    };
    tokio::spawn(supervisor.run())
}

/// How the main loop ended.
enum Ending {
    /// The child closed stdout on its own.
    Eof,
    /// A read or write failed; the child was killed.
    Broken,
    /// `Stop`, shutdown, or a dropped façade; the stop sequence ran.
    Stopped(Option<oneshot::Sender<()>>),
}

struct Supervisor {
    child: Box<dyn ClaudeChild>,
    mapper: ClaudeMapper,
    log: Arc<EventLog>,
    commands: mpsc::Receiver<ChildCommand>,
    shutdown: watch::Receiver<bool>,
    children: Children,
    pending: Pending,
    generation: u64,
    thread: ThreadId,
    context: ChildLogContext,
    pid: Option<u32>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Correlate the control requests this child was sent with their responses.
    // Source of truth: Writing a control request inserts its fresh `request_id`.
    // Structural reason: The CLI answers asynchronously, interleaved with other frames.
    // Synchronization: Owned by this supervisor task alone.
    // Invalidation/removal: The response removes its entry; child exit fails the rest.
    waiters: HashMap<String, Waiter>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Recognise late answers to handshake requests that timed out before open returned.
    // Source of truth: The handshake's `optional_request` timeouts, handed over at spawn.
    // Structural reason: The CLI still answers them, interleaved with session frames.
    // Synchronization: Owned by this supervisor task alone.
    // Invalidation/removal: The late answer removes its entry; the rest drop with the task.
    abandoned: HashSet<String>,
    early_lines: Vec<String>,
    /// stdout reached EOF.
    eof: bool,
    /// An interrupt was written during this child's life (an exit 1 after it is expected).
    interrupt_sent: bool,
    /// Events the closed log refused.
    dropped_events: u64,
    /// Why the supervisor killed the child, for the failed turn's message.
    failure: Option<&'static str>,
}

async fn shutdown_signal(shutdown: &mut watch::Receiver<bool>) {
    // A dropped sender (the façade is gone) is a shutdown too.
    let _ = shutdown.wait_for(|stopped| *stopped).await;
}

impl Supervisor {
    async fn run(mut self) {
        let mut ending = None;
        for line in std::mem::take(&mut self.early_lines) {
            if let Err(error) = self.dispatch_line(&line).await {
                self.broken("write_stdin", "a stdin write failed", &error);
                ending = Some(Ending::Broken);
                break;
            }
        }
        let ending = match ending {
            Some(ending) => ending,
            None => self.main_loop().await,
        };
        let (requested, stop_reply) = match ending {
            Ending::Eof | Ending::Broken => (false, None),
            Ending::Stopped(reply) => (true, reply),
        };
        let exit = self.child.wait().await;
        self.on_exit(&exit, requested);
        if let Some(reply) = stop_reply {
            let _ = reply.send(());
        }
    }

    async fn main_loop(&mut self) -> Ending {
        loop {
            tokio::select! {
                biased;
                line = self.child.next_line() => match line {
                    Ok(Some(line)) => {
                        if let Err(error) = self.dispatch_line(&line).await {
                            self.broken("write_stdin", "a stdin write failed", &error);
                            return Ending::Broken;
                        }
                    }
                    Ok(None) => {
                        self.eof = true;
                        return Ending::Eof;
                    }
                    Err(error) => {
                        self.broken("read_stdout", "a stdout read failed", &error);
                        return Ending::Broken;
                    }
                },
                command = self.commands.recv() => match command {
                    Some(ChildCommand::Stop { reply }) => {
                        self.stop("stop").await;
                        return Ending::Stopped(Some(reply));
                    }
                    Some(command) => {
                        if let Err(error) = self.handle_command(command).await {
                            self.broken("write_stdin", "a stdin write failed", &error);
                            return Ending::Broken;
                        }
                    }
                    None => {
                        debug!(
                            thread_id = %self.thread,
                            harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                            action = "stop",
                            "the harness dropped this child's command channel; stopping it"
                        );
                        self.stop("harness_dropped").await;
                        return Ending::Stopped(None);
                    }
                },
                () = shutdown_signal(&mut self.shutdown) => {
                    self.stop("shutdown").await;
                    return Ending::Stopped(None);
                }
            }
        }
    }

    /// A read or write failed: the child cannot be trusted with another frame.
    fn broken(&mut self, action: &'static str, failure: &'static str, error: &HarnessError) {
        error!(
            project_id = display_opt(self.context.project_id),
            harness = display_opt(self.context.harness.as_deref()),
            thread_id = %self.thread,
            harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
            turn_id = display_opt(self.mapper.active_turn()),
            pid = display_opt(self.pid),
            action,
            error = %error,
            "claude child I/O failed; killing it"
        );
        self.failure = Some(failure);
        self.child.start_kill();
    }

    async fn dispatch_line(&mut self, line: &str) -> Result<(), HarnessError> {
        let outputs = self.mapper.map_line(line);
        self.dispatch_all(outputs).await
    }

    async fn dispatch_all(&mut self, outputs: Vec<MapperOutput>) -> Result<(), HarnessError> {
        for output in outputs {
            self.dispatch(output).await?;
        }
        Ok(())
    }

    async fn dispatch(&mut self, output: MapperOutput) -> Result<(), HarnessError> {
        match output {
            MapperOutput::Event(event) => self.append(event),
            MapperOutput::Reply(value) => self.child.write_line(&value.to_string()).await?,
            MapperOutput::ControlResponse {
                request_id,
                payload,
            } => {
                let outcome = control_outcome(&payload);
                match self.waiters.remove(&request_id) {
                    Some(waiter) => {
                        debug!(
                            thread_id = %self.thread,
                            request_id = %request_id,
                            action = "control_response",
                            success = outcome.is_ok(),
                            "control response received"
                        );
                        waiter.resolve(outcome);
                    }
                    None if self.abandoned.remove(&request_id) => debug!(
                        thread_id = %self.thread,
                        request_id = %request_id,
                        action = "control_response",
                        success = outcome.is_ok(),
                        "late answer to a handshake request that timed out; ignored"
                    ),
                    None => warn!(
                        thread_id = %self.thread,
                        harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                        request_id = %request_id,
                        action = "control_response",
                        "control response for a request nobody is waiting on"
                    ),
                }
            }
            MapperOutput::PendingApproval {
                id,
                request_id,
                tool_use_id,
            } => {
                debug!(
                    thread_id = %self.thread,
                    request_id = %request_id,
                    tool_call_id = display_opt(tool_use_id.as_deref()),
                    action = "pending_approval",
                    "approval recorded; answers arrive in milestone 3"
                );
                lock(&self.pending).insert_approval(
                    id,
                    PendingAsk {
                        thread: self.thread,
                        request_id,
                        tool_use_id,
                    },
                );
            }
            MapperOutput::PendingServerRequest { id, request_id } => {
                debug!(
                    thread_id = %self.thread,
                    request_id = %request_id,
                    action = "pending_server_request",
                    "server request recorded; answers arrive in milestone 3"
                );
                lock(&self.pending).insert_server_request(
                    id,
                    PendingAsk {
                        thread: self.thread,
                        request_id,
                        tool_use_id: None,
                    },
                );
            }
        }
        Ok(())
    }

    /// Append to the retained log; a closed log is reported once and counted, never ignored.
    fn append(&mut self, event: giskard_core::event::AgentEvent) {
        if self.log.append(event) {
            return;
        }
        self.dropped_events += 1;
        if self.dropped_events == 1 {
            warn!(
                thread_id = %self.thread,
                harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                turn_id = display_opt(self.mapper.active_turn()),
                action = "append_event",
                "the thread's event log is closed; dropping this child's further events"
            );
        }
    }

    /// Handle one non-`Stop` command. `Err` is a stdin write failure: the child is broken.
    async fn handle_command(&mut self, command: ChildCommand) -> Result<(), HarnessError> {
        match command {
            ChildCommand::StartTurn {
                line,
                turn,
                model,
                reply,
            } => {
                // The caller's receiver is dropped the moment its timeout fires. Starting the turn
                // anyway would run the user's message under a turn the server never admitted, so a
                // hand-off that arrives after its caller gave up is dropped unwritten.
                if reply.is_closed() {
                    warn!(
                        project_id = display_opt(self.context.project_id),
                        thread_id = %self.thread,
                        harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                        turn_id = %turn,
                        action = "start_turn",
                        "the caller gave up on this turn before it was started; not writing it"
                    );
                    return Ok(());
                }
                if let Some(active) = self.mapper.active_turn() {
                    debug!(
                        thread_id = %self.thread,
                        turn_id = %active,
                        action = "start_turn",
                        "refusing a turn while one is active"
                    );
                    let _ = reply.send(Err(HarnessError::ThreadBusy {
                        thread: self.thread,
                    }));
                    return Ok(());
                }
                // `TurnStarted` reaches the log before the line is written, so the server sees
                // the turn before its first frame.
                let outputs = self.mapper.begin_turn(turn, TurnKind::User);
                self.mapper.note_turn_model(model);
                for output in outputs {
                    if let MapperOutput::Event(event) = output {
                        self.append(event);
                    }
                }
                info!(
                    project_id = display_opt(self.context.project_id),
                    thread_id = %self.thread,
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    turn_id = %turn,
                    pid = display_opt(self.pid),
                    action = "start_turn",
                    bytes = line.len(),
                    "writing a user message"
                );
                match self.child.write_line(&line).await {
                    Ok(()) => {
                        let _ = reply.send(Ok(()));
                        Ok(())
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error.clone()));
                        Err(error)
                    }
                }
            }
            ChildCommand::Interrupt { reply } => {
                let request_id = new_request_id();
                if let Err(error) = self
                    .child
                    .write_line(&control_line(&request_id, &json!({"subtype": "interrupt"})))
                    .await
                {
                    let _ = reply.send(Err(error.clone()));
                    return Err(error);
                }
                self.waiters.insert(request_id.clone(), Waiter::Unit(reply));
                self.interrupt_sent = true;
                let turn = self.mapper.active_turn();
                if turn.is_some() {
                    self.mapper.note_interrupt_sent();
                }
                info!(
                    thread_id = %self.thread,
                    turn_id = display_opt(turn),
                    request_id = %request_id,
                    action = "interrupt",
                    "interrupt sent"
                );
                Ok(())
            }
            ChildCommand::Control { request, reply } => {
                let request_id = new_request_id();
                if let Err(error) = self
                    .child
                    .write_line(&control_line(&request_id, &request))
                    .await
                {
                    let _ = reply.send(Err(error.clone()));
                    return Err(error);
                }
                let subtype = request.get("subtype").and_then(Value::as_str);
                debug!(
                    thread_id = %self.thread,
                    request_id = %request_id,
                    action = "control_request",
                    subtype = display_opt(subtype),
                    "control request sent"
                );
                self.waiters.insert(request_id, Waiter::Value(reply));
                Ok(())
            }
            ChildCommand::Stop { reply } => {
                // The main loop intercepts `Stop`; reaching here would be a routing bug.
                warn!(
                    thread_id = %self.thread,
                    action = "stop",
                    "a stop command reached the command handler"
                );
                let _ = reply.send(());
                Ok(())
            }
        }
    }

    /// Read and dispatch one line before `deadline`. `false` when the deadline passed first.
    async fn pump_until(&mut self, deadline: Instant) -> bool {
        match tokio::time::timeout_at(deadline, self.child.next_line()).await {
            Err(_) => false,
            Ok(Ok(Some(line))) => {
                if let Err(error) = self.dispatch_line(&line).await {
                    warn!(
                        thread_id = %self.thread,
                        action = "stop",
                        error = %error,
                        "a reply could not be written while stopping"
                    );
                }
                true
            }
            Ok(Ok(None)) => {
                self.eof = true;
                true
            }
            Ok(Err(error)) => {
                self.broken("read_stdout", "a stdout read failed", &error);
                self.eof = true;
                true
            }
        }
    }

    /// The stop sequence: interrupt a live turn, close stdin, kill on the grace timeout.
    ///
    /// SIGTERM is deliberately not used: it leaves the turn without a `result`.
    async fn stop(&mut self, reason: &'static str) {
        if self.eof {
            return;
        }
        if let Some(turn) = self.mapper.active_turn() {
            let started = Instant::now();
            let request_id = new_request_id();
            match self
                .child
                .write_line(&control_line(&request_id, &json!({"subtype": "interrupt"})))
                .await
            {
                Ok(()) => {
                    self.waiters.insert(request_id.clone(), Waiter::Stop);
                    self.interrupt_sent = true;
                    self.mapper.note_interrupt_sent();
                    let deadline = started + STOP_INTERRUPT_GRACE;
                    while self.mapper.active_turn().is_some() && !self.eof {
                        if !self.pump_until(deadline).await {
                            break;
                        }
                    }
                    info!(
                        thread_id = %self.thread,
                        turn_id = %turn,
                        request_id = %request_id,
                        action = "stop_interrupt",
                        reason,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        turn_closed = self.mapper.active_turn().is_none(),
                        "interrupted the live turn before stopping"
                    );
                }
                Err(error) => warn!(
                    thread_id = %self.thread,
                    turn_id = %turn,
                    action = "stop_interrupt",
                    reason,
                    error = %error,
                    "could not interrupt the live turn before stopping"
                ),
            }
        }
        if self.eof {
            return;
        }
        self.child.close_stdin();
        let deadline = Instant::now() + STOP_EXIT_GRACE;
        while !self.eof {
            if !self.pump_until(deadline).await {
                warn!(
                    project_id = display_opt(self.context.project_id),
                    thread_id = %self.thread,
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    pid = display_opt(self.pid),
                    action = "stop_kill",
                    reason,
                    grace_ms = STOP_EXIT_GRACE.as_millis() as u64,
                    "claude did not exit after stdin closed; killing it"
                );
                self.child.start_kill();
                break;
            }
        }
    }

    /// Child-exit handling, whatever ended the child.
    fn on_exit(&mut self, exit: &ChildExit, requested: bool) {
        let mut described = exit.describe();
        if let Some(failure) = self.failure {
            described = format!("{described}, after {failure}");
        }
        if self.mapper.active_turn().is_some() {
            for output in self.mapper.child_exited(&described) {
                if let MapperOutput::Event(event) = output {
                    self.append(event);
                }
            }
        }
        self.log.close();
        for (request_id, waiter) in self.waiters.drain() {
            if !matches!(waiter, Waiter::Stop) {
                warn!(
                    thread_id = %self.thread,
                    request_id = %request_id,
                    action = "control_request",
                    "control request unanswered when the child stopped"
                );
            }
            waiter.resolve(Err(child_stopped()));
        }
        let live_children = {
            let mut children = lock(&self.children);
            if children
                .get(&self.thread)
                .is_some_and(|handle| handle.generation == self.generation)
            {
                children.remove(&self.thread);
            }
            children.len()
        };
        let pending_dropped = lock(&self.pending).remove_thread(self.thread);
        let expected =
            requested && (exit.code == Some(0) || (exit.code == Some(1) && self.interrupt_sent));
        macro_rules! exit_line {
            ($level:ident, $message:literal) => {
                $level!(
                    project_id = display_opt(self.context.project_id),
                    harness = display_opt(self.context.harness.as_deref()),
                    thread_id = %self.thread,
                    harness_thread_id = display_opt(self.context.harness_thread_id.as_deref()),
                    pid = display_opt(self.pid),
                    action = "child_exited",
                    exit_code = display_opt(exit.code),
                    signal = display_opt(exit.signal),
                    stderr_tail = ?exit.stderr_tail,
                    requested,
                    live_children,
                    pending_dropped,
                    dropped_events = self.dropped_events,
                    $message
                )
            };
        }
        if expected {
            exit_line!(info, "claude child exited");
        } else {
            exit_line!(warn, "claude child exited unexpectedly");
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    //! `ScriptedChild`: an in-process `ClaudeChild` driven by a script of steps.

    use std::collections::VecDeque;

    use async_trait::async_trait;
    use tokio::sync::mpsc;

    use super::*;
    use crate::process::{MAX_STDOUT_LINE_BYTES, overlong_line};

    /// What a script does when its trigger fires.
    pub(crate) enum Action {
        /// Emit these stdout lines.
        Emit(Vec<String>),
        /// Emit a fixture's stdout lines, without its `control_response` lines (those belong to
        /// the scripted replies) and without `skip_types`.
        EmitFixture {
            name: &'static str,
            skip_types: &'static [&'static str],
        },
        /// Emit the first `count` lines `EmitFixture` would.
        EmitFixturePrefix { name: &'static str, count: usize },
        /// Answer the triggering control request with this success payload.
        Respond(Value),
        /// Answer the triggering control request with an error.
        RespondError(&'static str),
        /// Answer the triggering control request with this success payload, but only when the
        /// next stdin line arrives: a late answer.
        RespondOnNextWrite(Value),
        /// Close stdout and exit with this code and stderr.
        Exit { code: i32, stderr: Vec<String> },
    }

    pub(crate) type Matcher = Box<dyn Fn(&Value) -> bool + Send>;

    pub(crate) enum Step {
        /// Wait for a stdin line matching the predicate, then run the actions.
        OnStdin(Matcher, Vec<Action>),
    }

    /// What a test can inspect after the child moved into the harness.
    #[derive(Default)]
    pub(crate) struct ScriptRecord {
        pub written: Vec<String>,
        pub stdin_closed: bool,
        pub killed: bool,
    }

    enum Out {
        Line(String),
        Eof,
    }

    pub(crate) struct ScriptedChild {
        steps: VecDeque<Step>,
        tx: mpsc::UnboundedSender<Out>,
        rx: mpsc::UnboundedReceiver<Out>,
        record: Arc<Mutex<ScriptRecord>>,
        exit: Option<ChildExit>,
        closed: bool,
        exit_on_eof: bool,
        /// `wait` never returns, as for a process the kernel cannot reap.
        hang_on_wait: bool,
        /// `write_line` waits while this is `false`, as for a CLI that stopped reading stdin.
        gate: watch::Receiver<bool>,
        /// A late answer `RespondOnNextWrite` holds back until the next stdin line.
        deferred: Option<String>,
    }

    pub(crate) fn fixture_lines(name: &str) -> Vec<String> {
        let path = format!(
            "{}/tests/fixtures/{name}.out.jsonl",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("no fixture {path}"))
            .lines()
            .filter(|line| {
                serde_json::from_str::<Value>(line).unwrap()["type"] != "control_response"
            })
            .map(str::to_owned)
            .collect()
    }

    /// A stdin line that is a control request of this subtype.
    pub(crate) fn control(subtype: &'static str) -> Matcher {
        Box::new(move |value| {
            value["type"] == "control_request" && value["request"]["subtype"] == subtype
        })
    }

    /// A stdin line that is a user message.
    pub(crate) fn user() -> Matcher {
        Box::new(|value| value["type"] == "user")
    }

    impl ScriptedChild {
        pub(crate) fn new(steps: Vec<Step>) -> (Self, Arc<Mutex<ScriptRecord>>) {
            let (tx, rx) = mpsc::unbounded_channel();
            let record = Arc::new(Mutex::new(ScriptRecord::default()));
            let child = Self {
                steps: steps.into(),
                tx,
                rx,
                record: record.clone(),
                exit: None,
                closed: false,
                exit_on_eof: true,
                hang_on_wait: false,
                gate: watch::channel(true).1,
                deferred: None,
            };
            (child, record)
        }

        /// Writes block while the returned sender holds `false`.
        pub(crate) fn gated(mut self) -> (Self, watch::Sender<bool>) {
            let (open, gate) = watch::channel(true);
            self.gate = gate;
            (self, open)
        }

        /// The child ignores EOF on stdin, as a wedged CLI would.
        pub(crate) fn ignoring_eof(mut self) -> Self {
            self.exit_on_eof = false;
            self
        }

        /// `wait` never returns, so the supervisor cannot finish its exit handling.
        pub(crate) fn hanging_on_wait(mut self) -> Self {
            self.hang_on_wait = true;
            self
        }

        fn finish(&mut self, exit: ChildExit) {
            if self.exit.is_none() {
                self.exit = Some(exit);
                let _ = self.tx.send(Out::Eof);
            }
        }

        fn run(&mut self, actions: Vec<Action>, trigger: &Value) {
            for action in actions {
                match action {
                    Action::Emit(lines) => {
                        for line in lines {
                            let _ = self.tx.send(Out::Line(line));
                        }
                    }
                    Action::EmitFixture { name, skip_types } => {
                        for line in fixture_lines(name) {
                            let value: Value = serde_json::from_str(&line).unwrap();
                            if skip_types.iter().any(|skip| value["type"] == *skip) {
                                continue;
                            }
                            let _ = self.tx.send(Out::Line(line));
                        }
                    }
                    Action::EmitFixturePrefix { name, count } => {
                        for line in fixture_lines(name).into_iter().take(count) {
                            let _ = self.tx.send(Out::Line(line));
                        }
                    }
                    Action::Respond(payload) => {
                        let line = json!({"type": "control_response", "response": {
                            "subtype": "success",
                            "request_id": trigger["request_id"],
                            "response": payload,
                        }});
                        let _ = self.tx.send(Out::Line(line.to_string()));
                    }
                    Action::RespondError(message) => {
                        let line = json!({"type": "control_response", "response": {
                            "subtype": "error",
                            "request_id": trigger["request_id"],
                            "error": message,
                        }});
                        let _ = self.tx.send(Out::Line(line.to_string()));
                    }
                    Action::RespondOnNextWrite(payload) => {
                        let line = json!({"type": "control_response", "response": {
                            "subtype": "success",
                            "request_id": trigger["request_id"],
                            "response": payload,
                        }});
                        self.deferred = Some(line.to_string());
                    }
                    Action::Exit { code, stderr } => self.finish(ChildExit {
                        code: Some(code),
                        signal: None,
                        stderr_tail: stderr,
                    }),
                }
            }
        }
    }

    #[async_trait]
    impl ClaudeChild for ScriptedChild {
        async fn write_line(&mut self, line: &str) -> Result<(), HarnessError> {
            let _ = self.gate.wait_for(|open| *open).await;
            if self.closed || self.exit.is_some() {
                return Err(HarnessError::Transport("claude stdin is closed".into()));
            }
            if let Some(late) = self.deferred.take() {
                let _ = self.tx.send(Out::Line(late));
            }
            lock(&self.record).written.push(line.to_owned());
            let value: Value = serde_json::from_str(line).unwrap();
            let matched = match self.steps.front() {
                Some(Step::OnStdin(matcher, _)) => matcher(&value),
                None => false,
            };
            if matched && let Some(Step::OnStdin(_, actions)) = self.steps.pop_front() {
                self.run(actions, &value);
            }
            Ok(())
        }

        async fn next_line(&mut self) -> Result<Option<String>, HarnessError> {
            match self.rx.recv().await {
                Some(Out::Line(line)) if line.len() > MAX_STDOUT_LINE_BYTES => {
                    Err(overlong_line(line.len()))
                }
                Some(Out::Line(line)) => Ok(Some(line)),
                Some(Out::Eof) | None => {
                    // EOF is sticky, like a real pipe.
                    let _ = self.tx.send(Out::Eof);
                    Ok(None)
                }
            }
        }

        fn close_stdin(&mut self) {
            if self.closed {
                return;
            }
            self.closed = true;
            lock(&self.record).stdin_closed = true;
            if self.exit_on_eof {
                self.finish(ChildExit {
                    code: Some(0),
                    signal: None,
                    stderr_tail: Vec::new(),
                });
            }
        }

        async fn wait(&mut self) -> ChildExit {
            if self.hang_on_wait {
                std::future::pending::<()>().await;
            }
            self.exit.clone().unwrap_or_default()
        }

        fn start_kill(&mut self) {
            lock(&self.record).killed = true;
            self.finish(ChildExit {
                code: None,
                signal: Some(9),
                stderr_tail: Vec::new(),
            });
        }

        fn pid(&self) -> Option<u32> {
            Some(4242)
        }
    }
}
