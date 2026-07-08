//! Codex CLI harness adapter (spec §4.6).
//!
//! Wraps `codex-codes::AsyncClient` and implements the `AgentHarness` trait.
//! All Codex-specific types are confined to this crate and mapped to
//! `giskard-core` types at the boundary.

mod mapping;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tracing::warn;

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ServerRequestId, ThreadId, TurnId};
use giskard_core::model::ModelDescriptor;
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, HarnessCapabilities, HarnessNotice, OpenThreadOptions,
    ThreadHandle,
};

use mapping::CodexMapper;

const BROADCAST_CAPACITY: usize = 256;

enum HarnessCommand {
    OpenThread {
        opts: OpenThreadOptions,
        response: oneshot::Sender<Result<ThreadHandle, HarnessError>>,
    },
    StartTurn {
        thread: ThreadHandle,
        input: UserInput,
        overrides: TurnOverrides,
        response: oneshot::Sender<Result<TurnId, HarnessError>>,
    },
}

enum ControlCommand {
    RespondApproval {
        id: ApprovalId,
        decision: ApprovalDecision,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    RespondServerRequest {
        id: ServerRequestId,
        response_payload: ServerRequestResponse,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    Interrupt {
        thread: ThreadHandle,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    TerminateCommand {
        thread: ThreadHandle,
        process_id: String,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    Shutdown {
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
}

type SenderMap = Arc<Mutex<HashMap<ThreadId, broadcast::Sender<AgentEvent>>>>;

/// Codex CLI harness adapter (one app-server process per project).
pub struct CodexHarness {
    cmd_tx: mpsc::Sender<HarnessCommand>,
    control_tx: mpsc::Sender<ControlCommand>,
    senders: SenderMap,
    shutdown_called: AtomicBool,
    capabilities: HarnessCapabilities,
}

impl CodexHarness {
    pub async fn start(workspace_root: PathBuf) -> Result<Arc<Self>, HarnessError> {
        let client = codex_codes::AsyncClient::start()
            .await
            .map_err(|e| HarnessError::Spawn(e.to_string()))?;
        Self::spawn_harness(client, workspace_root)
    }

    pub async fn start_with(
        workspace_root: PathBuf,
        codex_path: PathBuf,
    ) -> Result<Arc<Self>, HarnessError> {
        let builder = codex_codes::cli::AppServerBuilder::new().command(codex_path);
        let client = codex_codes::AsyncClient::start_with(builder)
            .await
            .map_err(|e| HarnessError::Spawn(e.to_string()))?;
        Self::spawn_harness(client, workspace_root)
    }

    fn spawn_harness(
        client: codex_codes::AsyncClient,
        workspace_root: PathBuf,
    ) -> Result<Arc<Self>, HarnessError> {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (control_tx, control_rx) = mpsc::channel(64);
        let senders: SenderMap = Arc::new(Mutex::new(HashMap::new()));

        let harness = Arc::new(Self {
            cmd_tx,
            control_tx,
            senders: senders.clone(),
            shutdown_called: AtomicBool::new(false),
            capabilities: HarnessCapabilities {
                live_approvals: true,
                plan_build_modes: true,
                per_turn_model: true,
                reasoning_effort: true,
                structured_diffs: true,
                resumable_threads: true,
                model_listing: false,
                token_usage: true,
            },
        });

        tokio::spawn(background_task(
            client,
            cmd_rx,
            control_rx,
            senders,
            workspace_root,
        ));
        Ok(harness)
    }
}

#[async_trait]
impl AgentHarness for CodexHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        self.capabilities
    }

    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError> {
        Ok(vec![])
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(HarnessCommand::OpenThread { opts, response: tx })
            .await
            .map_err(|_| HarnessError::Transport("background task closed".into()))?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        input: UserInput,
        overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(HarnessCommand::StartTurn {
                thread: thread.clone(),
                input,
                overrides,
                response: tx,
            })
            .await
            .map_err(|_| HarnessError::Transport("background task closed".into()))?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream {
        if let Ok(senders) = self.senders.try_lock() {
            if let Some(sender) = senders.get(&thread.thread) {
                return AgentEventStream::new(sender.subscribe());
            }
        }
        let (_, rx) = broadcast::channel(1);
        AgentEventStream::new(rx)
    }

    async fn respond_approval(
        &self,
        id: ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(ControlCommand::RespondApproval {
                id,
                decision,
                response: tx,
            })
            .await
            .map_err(|_| HarnessError::Transport("background task closed".into()))?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn respond_server_request(
        &self,
        id: ServerRequestId,
        response_payload: ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(ControlCommand::RespondServerRequest {
                id,
                response_payload,
                response: tx,
            })
            .await
            .map_err(|_| HarnessError::Transport("background task closed".into()))?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn interrupt(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(ControlCommand::Interrupt {
                thread: thread.clone(),
                response: tx,
            })
            .await
            .map_err(|_| HarnessError::Transport("background task closed".into()))?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn terminate_command(
        &self,
        thread: &ThreadHandle,
        process_id: &str,
    ) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(ControlCommand::TerminateCommand {
                thread: thread.clone(),
                process_id: process_id.to_owned(),
                response: tx,
            })
            .await
            .map_err(|_| HarnessError::Transport("background task closed".into()))?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        if self.shutdown_called.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let (tx, rx) = oneshot::channel();
        let _ = self
            .control_tx
            .send(ControlCommand::Shutdown { response: tx })
            .await;
        let _ = rx.await;
        Ok(())
    }
}

async fn background_task(
    mut client: codex_codes::AsyncClient,
    mut cmd_rx: mpsc::Receiver<HarnessCommand>,
    mut control_rx: mpsc::Receiver<ControlCommand>,
    senders: SenderMap,
    workspace_root: PathBuf,
) {
    let mut mapper = CodexMapper::new(workspace_root);

    loop {
        tokio::select! {
            msg = client.next_message(), if mapper.has_running_commands() => {
                match msg {
                    Ok(Some(msg)) => {
                        if matches!(
                            handle_idle_server_message(
                                &mut client,
                                &mut mapper,
                                &senders,
                                &mut control_rx,
                                msg,
                            )
                            .await,
                            StreamOutcome::Shutdown,
                        ) {
                            let _ = client.shutdown().await;
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        warn!("Codex idle stream failed while commands were running: {e}");
                    }
                }
            }
            cmd = cmd_rx.recv() => {
                let cmd = match cmd {
                    Some(cmd) => cmd,
                    None => break,
                };

                match cmd {
                    HarnessCommand::OpenThread { opts, response } => {
                        let result = handle_open_thread(&mut client, &mut mapper, &opts, &senders).await;
                        let _ = response.send(result);
                    }
                    HarnessCommand::StartTurn {
                        thread,
                        input,
                        overrides,
                        response,
                    } => {
                        let result = handle_start_turn(&mut client, &thread, &input, &overrides).await;
                        let ok = result.is_ok();
                        let _ = response.send(result);
                        if ok && matches!(
                            stream_turn_events(
                                &mut client,
                                &mut mapper,
                                &thread,
                                &senders,
                                &mut control_rx,
                            )
                            .await,
                            StreamOutcome::Shutdown,
                        ) {
                            let _ = client.shutdown().await;
                            break;
                        }
                    }
                }
            }
            control = control_rx.recv() => {
                match control {
                    Some(ControlCommand::RespondApproval {
                        id,
                        decision,
                        response,
                    }) => {
                        let result =
                            handle_respond_approval(&mut client, &mut mapper, &id, &decision)
                                .await;
                        let _ = response.send(result);
                    }
                    Some(ControlCommand::RespondServerRequest {
                        id,
                        response_payload,
                        response,
                    }) => {
                        let result = handle_respond_server_request(
                            &mut client,
                            &mut mapper,
                            &senders,
                            &id,
                            response_payload,
                        )
                        .await;
                        let _ = response.send(result);
                    }
                    Some(ControlCommand::Interrupt { thread, response }) => {
                        let result = handle_interrupt(&mut client, &mapper, &thread).await;
                        let _ = response.send(result);
                    }
                    Some(ControlCommand::TerminateCommand {
                        thread,
                        process_id,
                        response,
                    }) => {
                        let result =
                            handle_terminate_command(&mut client, &mapper, &thread, &process_id)
                                .await;
                        let _ = response.send(result);
                    }
                    Some(ControlCommand::Shutdown { response }) => {
                        let _ = client.shutdown().await;
                        let _ = response.send(Ok(()));
                        break;
                    }
                    None => break,
                }
            }
        }
    }
}

async fn handle_idle_server_message(
    client: &mut codex_codes::AsyncClient,
    mapper: &mut CodexMapper,
    senders: &SenderMap,
    control_rx: &mut mpsc::Receiver<ControlCommand>,
    msg: codex_codes::ServerMessage,
) -> StreamOutcome {
    let fallback_thread = mapper.running_command_fallback_thread().unwrap_or_default();
    match msg {
        codex_codes::ServerMessage::Notification(notif) => {
            if let Some(event) = mapper.map_notification(&notif, fallback_thread) {
                let thread = event_thread(&event);
                let _ = broadcast_event(senders, thread, || event).await;
            }
            StreamOutcome::TurnEnded
        }
        codex_codes::ServerMessage::Request { id, request } => {
            let waiting_for = id.to_string();
            let event = mapper.map_server_request(&id, &request, fallback_thread);
            let thread = event_thread(&event);
            let _ = broadcast_event(senders, thread, || event).await;

            loop {
                match control_rx.recv().await {
                    Some(ControlCommand::RespondApproval {
                        id: resp_id,
                        decision,
                        response,
                    }) => {
                        let is_waiting = resp_id.0 == waiting_for;
                        let result =
                            handle_respond_approval(client, mapper, &resp_id, &decision).await;
                        let should_end = is_waiting && result.is_ok();
                        let _ = response.send(result);
                        if should_end {
                            return StreamOutcome::TurnEnded;
                        }
                    }
                    Some(ControlCommand::RespondServerRequest {
                        id: resp_id,
                        response_payload,
                        response,
                    }) => {
                        let is_waiting = resp_id.0 == waiting_for;
                        let result = handle_respond_server_request(
                            client,
                            mapper,
                            senders,
                            &resp_id,
                            response_payload,
                        )
                        .await;
                        let should_end = is_waiting && result.is_ok();
                        let _ = response.send(result);
                        if should_end {
                            return StreamOutcome::TurnEnded;
                        }
                    }
                    Some(ControlCommand::Interrupt { thread, response }) => {
                        let result = handle_interrupt(client, mapper, &thread).await;
                        let _ = response.send(result);
                    }
                    Some(ControlCommand::TerminateCommand {
                        thread,
                        process_id,
                        response,
                    }) => {
                        let result =
                            handle_terminate_command(client, mapper, &thread, &process_id).await;
                        let _ = response.send(result);
                    }
                    Some(ControlCommand::Shutdown { response }) => {
                        let _ = response.send(Ok(()));
                        return StreamOutcome::Shutdown;
                    }
                    None => return StreamOutcome::TurnEnded,
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamOutcome {
    TurnEnded,
    Shutdown,
}

async fn handle_open_thread(
    client: &mut codex_codes::AsyncClient,
    mapper: &mut CodexMapper,
    opts: &OpenThreadOptions,
    senders: &SenderMap,
) -> Result<ThreadHandle, HarnessError> {
    let cwd = opts.workspace_root.to_string_lossy().to_string();

    // Track whether resume-by-id failed and we fell back to a fresh native thread (C5), so we can
    // warn the caller that agent context was lost while keeping the Giskard-side history.
    let mut resume_warning = None;

    let harness_thread_id = if let Some(ref resume_id) = opts.resume {
        match resume_thread(client, resume_id, &cwd, &opts.initial_model).await {
            Ok(id) => id,
            Err(e) => {
                // C5: Codex thread store purged/rotated. Start fresh instead of hard-failing.
                resume_warning = Some(HarnessNotice {
                    code: "codex_resume_failed".into(),
                    message:
                        "Agent context was lost; started a fresh Codex session. History is intact."
                            .into(),
                    detail: Some(e.to_string()),
                });
                start_thread(client, &cwd, &opts.initial_model).await?
            }
        }
    } else {
        start_thread(client, &cwd, &opts.initial_model).await?
    };

    let thread_id = opts.thread.unwrap_or_default();
    // B4: bind the (possibly re-established) native id to the durable ThreadId.
    mapper.register_thread(harness_thread_id.clone(), thread_id);

    let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
    senders.lock().await.insert(thread_id, tx);

    let _ = broadcast_event(senders, thread_id, || AgentEvent::ThreadOpened {
        thread: thread_id,
        harness_thread_id: harness_thread_id.clone(),
    })
    .await;

    if let Some(warning) = &resume_warning {
        let message = warning.message.clone();
        let _ = broadcast_event(senders, thread_id, || AgentEvent::Error {
            thread: thread_id,
            turn: None,
            error: HarnessError::Transport(message),
        })
        .await;
    }

    Ok(ThreadHandle {
        thread: thread_id,
        harness_thread_id,
        warning: resume_warning,
    })
}

async fn resume_thread(
    client: &mut codex_codes::AsyncClient,
    resume_id: &str,
    cwd: &str,
    model: &giskard_core::model::ModelRef,
) -> Result<String, HarnessError> {
    let params: codex_codes::ThreadResumeParams = serde_json::from_value(serde_json::json!({
        "threadId": resume_id,
        "cwd": cwd,
        "model": model.model,
        "modelProvider": model.provider,
    }))
    .map_err(|e| HarnessError::Protocol(e.to_string()))?;
    let resp = client
        .thread_resume(&params)
        .await
        .map_err(|e| HarnessError::Transport(e.to_string()))?;
    Ok(resp.thread.id)
}

async fn start_thread(
    client: &mut codex_codes::AsyncClient,
    cwd: &str,
    initial_model: &giskard_core::model::ModelRef,
) -> Result<String, HarnessError> {
    let params: codex_codes::ThreadStartParams = serde_json::from_value(serde_json::json!({
        "cwd": cwd,
        "model": initial_model.model,
        "modelProvider": initial_model.provider,
    }))
    .map_err(|e| HarnessError::Protocol(e.to_string()))?;
    let resp = client
        .thread_start(&params)
        .await
        .map_err(|e| HarnessError::Transport(e.to_string()))?;
    Ok(resp.thread.id)
}

async fn handle_start_turn(
    client: &mut codex_codes::AsyncClient,
    thread: &ThreadHandle,
    input: &UserInput,
    overrides: &TurnOverrides,
) -> Result<TurnId, HarnessError> {
    let codex_input = mapping::map_user_input(input);
    let sandbox_policy = mapping::map_mode_to_sandbox(overrides.mode);
    let approval_policy = mapping::map_approval_policy(overrides.approval_policy);
    let effort = overrides
        .model
        .as_ref()
        .and_then(|m| m.reasoning_effort)
        .map(mapping::map_effort);

    let params: codex_codes::TurnStartParams = serde_json::from_value(serde_json::json!({
        "threadId": thread.harness_thread_id,
        "input": codex_input,
        "model": overrides.model.as_ref().map(|m| &m.model),
        "effort": effort,
        "sandboxPolicy": sandbox_policy,
        "approvalPolicy": approval_policy,
    }))
    .map_err(|e| HarnessError::Protocol(e.to_string()))?;

    let _resp = client
        .turn_start(&params)
        .await
        .map_err(|e| HarnessError::Transport(e.to_string()))?;

    Ok(TurnId::new())
}

async fn stream_turn_events(
    client: &mut codex_codes::AsyncClient,
    mapper: &mut CodexMapper,
    thread: &ThreadHandle,
    senders: &SenderMap,
    control_rx: &mut mpsc::Receiver<ControlCommand>,
) -> StreamOutcome {
    let thread_id = thread.thread;
    let mut active_turn: Option<TurnId> = None;

    loop {
        let msg = tokio::select! {
            msg = client.next_message() => msg,
            control = control_rx.recv() => {
                match handle_stream_control(client, mapper, senders, control).await {
                    StreamControlOutcome::Continue => continue,
                    StreamControlOutcome::Shutdown => return StreamOutcome::Shutdown,
                }
            }
        };

        let msg = match msg {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                emit_incomplete_turn(
                    senders,
                    thread_id,
                    active_turn,
                    "Codex stream ended before turn completion",
                )
                .await;
                break;
            }
            Err(e) => {
                let message = e.to_string();
                let event_message = message.clone();
                let _ = broadcast_event(senders, thread_id, || AgentEvent::Error {
                    thread: thread_id,
                    turn: None,
                    error: HarnessError::Transport(event_message),
                })
                .await;
                emit_incomplete_turn(
                    senders,
                    thread_id,
                    active_turn,
                    format!("Codex stream failed before turn completion: {message}"),
                )
                .await;
                break;
            }
        };

        match msg {
            codex_codes::ServerMessage::Notification(notif) => {
                // A non-retryable error ends the turn without Codex sending `turn/completed`.
                // Capture its message so we can synthesize a terminal Failed turn below (§7.1),
                // giving history a persistent record of why the turn produced no agent output.
                let terminal_error = mapping::fatal_turn_error(&notif);
                let event = mapper.map_notification(&notif, thread_id);
                if let Some(event) = event {
                    if let AgentEvent::TurnStarted { turn, .. } = &event {
                        active_turn = Some(*turn);
                    }
                    let is_completed = matches!(event, AgentEvent::TurnCompleted { .. });
                    let _ = broadcast_event(senders, thread_id, || event).await;
                    if is_completed {
                        break;
                    }
                }
                if let Some(message) = terminal_error {
                    // Mint a turn id when the error arrived before any `turn/started` (e.g. an
                    // immediate quota rejection) so the failed attempt is still persisted.
                    let turn = active_turn.unwrap_or_default();
                    let _ = broadcast_event(senders, thread_id, || AgentEvent::TurnCompleted {
                        thread: thread_id,
                        turn,
                        usage: TokenUsage::default(),
                        status: TurnStatus {
                            kind: TurnStatusKind::Failed,
                            message: Some(message),
                        },
                    })
                    .await;
                    break;
                }
            }
            codex_codes::ServerMessage::Request { id, request } => {
                let waiting_for = id.to_string();
                let event = mapper.map_server_request(&id, &request, thread_id);
                let event_thread_id = event_thread(&event);
                let _ = broadcast_event(senders, event_thread_id, || event).await;

                // Wait for the browser response. Normal harness commands keep queuing on the main
                // command channel while this live turn waits for control input.
                loop {
                    match control_rx.recv().await {
                        Some(ControlCommand::RespondApproval {
                            id: resp_id,
                            decision,
                            response,
                        }) => {
                            let is_waiting = resp_id.0 == waiting_for;
                            let result =
                                handle_respond_approval(client, mapper, &resp_id, &decision).await;
                            let should_break = is_waiting && result.is_ok();
                            let _ = response.send(result);
                            if should_break {
                                break;
                            }
                        }
                        Some(ControlCommand::RespondServerRequest {
                            id: resp_id,
                            response_payload,
                            response,
                        }) => {
                            let is_waiting = resp_id.0 == waiting_for;
                            let result = handle_respond_server_request(
                                client,
                                mapper,
                                senders,
                                &resp_id,
                                response_payload,
                            )
                            .await;
                            let should_break = is_waiting && result.is_ok();
                            let _ = response.send(result);
                            if should_break {
                                break;
                            }
                        }
                        Some(ControlCommand::Interrupt {
                            thread: t,
                            response,
                        }) => {
                            let result = handle_interrupt(client, mapper, &t).await;
                            let _ = response.send(result);
                        }
                        Some(ControlCommand::TerminateCommand {
                            thread,
                            process_id,
                            response,
                        }) => {
                            let result =
                                handle_terminate_command(client, mapper, &thread, &process_id)
                                    .await;
                            let _ = response.send(result);
                        }
                        Some(ControlCommand::Shutdown { response }) => {
                            let _ = response.send(Ok(()));
                            return StreamOutcome::Shutdown;
                        }
                        None => return StreamOutcome::TurnEnded,
                    }
                }
            }
        }
    }

    StreamOutcome::TurnEnded
}

enum StreamControlOutcome {
    Continue,
    Shutdown,
}

async fn handle_stream_control(
    client: &mut codex_codes::AsyncClient,
    mapper: &mut CodexMapper,
    senders: &SenderMap,
    control: Option<ControlCommand>,
) -> StreamControlOutcome {
    match control {
        Some(ControlCommand::RespondApproval {
            id,
            decision,
            response,
        }) => {
            let result = handle_respond_approval(client, mapper, &id, &decision).await;
            let _ = response.send(result);
            StreamControlOutcome::Continue
        }
        Some(ControlCommand::RespondServerRequest {
            id,
            response_payload,
            response,
        }) => {
            let result =
                handle_respond_server_request(client, mapper, senders, &id, response_payload).await;
            let _ = response.send(result);
            StreamControlOutcome::Continue
        }
        Some(ControlCommand::Interrupt { thread, response }) => {
            let result = handle_interrupt(client, mapper, &thread).await;
            let _ = response.send(result);
            StreamControlOutcome::Continue
        }
        Some(ControlCommand::TerminateCommand {
            thread,
            process_id,
            response,
        }) => {
            let result = handle_terminate_command(client, mapper, &thread, &process_id).await;
            let _ = response.send(result);
            StreamControlOutcome::Continue
        }
        Some(ControlCommand::Shutdown { response }) => {
            let _ = response.send(Ok(()));
            StreamControlOutcome::Shutdown
        }
        None => StreamControlOutcome::Shutdown,
    }
}

async fn broadcast_event<F: FnOnce() -> AgentEvent>(senders: &SenderMap, thread: ThreadId, f: F) {
    let sender = senders.lock().await.get(&thread).cloned();
    if let Some(sender) = sender {
        let _ = sender.send(f());
    }
}

fn event_thread(event: &AgentEvent) -> ThreadId {
    match event {
        AgentEvent::ThreadOpened { thread, .. }
        | AgentEvent::TurnStarted { thread, .. }
        | AgentEvent::ItemStarted { thread, .. }
        | AgentEvent::ItemDelta { thread, .. }
        | AgentEvent::ItemCompleted { thread, .. }
        | AgentEvent::DiffUpdated { thread, .. }
        | AgentEvent::ApprovalRequested { thread, .. }
        | AgentEvent::ServerRequestReceived { thread, .. }
        | AgentEvent::ServerRequestResolved { thread, .. }
        | AgentEvent::TurnCompleted { thread, .. }
        | AgentEvent::Error { thread, .. }
        | AgentEvent::Notice { thread, .. } => *thread,
    }
}

async fn emit_incomplete_turn(
    senders: &SenderMap,
    thread: ThreadId,
    turn: Option<TurnId>,
    message: impl Into<String>,
) {
    if let Some(turn) = turn {
        let message = message.into();
        let _ = broadcast_event(senders, thread, || AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Failed,
                message: Some(message),
            },
        })
        .await;
    }
}

async fn handle_respond_approval(
    client: &mut codex_codes::AsyncClient,
    mapper: &mut CodexMapper,
    id: &ApprovalId,
    decision: &ApprovalDecision,
) -> Result<(), HarnessError> {
    match mapper
        .map_approval_response(id, decision)
        .map_err(HarnessError::Protocol)?
    {
        mapping::ApprovalResponse::Result { request_id, value } => client
            .respond(request_id, &value)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string())),
        mapping::ApprovalResponse::Error {
            request_id,
            code,
            message,
        } => client
            .respond_error(request_id, code, &message)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string())),
    }
}

async fn handle_respond_server_request(
    client: &mut codex_codes::AsyncClient,
    mapper: &mut CodexMapper,
    senders: &SenderMap,
    id: &ServerRequestId,
    response: ServerRequestResponse,
) -> Result<(), HarnessError> {
    let pending = mapper
        .pending_server_request(id)
        .map_err(HarnessError::Protocol)?;
    match response {
        ServerRequestResponse::Result { value } => client
            .respond(pending.request_id, &value)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))?,
        ServerRequestResponse::Error { code, message } => client
            .respond_error(pending.request_id, code, &message)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))?,
    }
    mapper.resolve_server_request(id);
    let thread = pending.thread;
    let turn = pending.turn;
    let request_id = id.clone();
    let _ = broadcast_event(senders, thread, || AgentEvent::ServerRequestResolved {
        thread,
        turn,
        request_id,
    })
    .await;
    Ok(())
}

async fn handle_interrupt(
    client: &mut codex_codes::AsyncClient,
    mapper: &CodexMapper,
    thread: &ThreadHandle,
) -> Result<(), HarnessError> {
    let native_turn_id = mapper
        .active_native_turn_for_thread(thread.thread)
        .ok_or_else(|| HarnessError::Unsupported("no active Codex turn to interrupt".into()))?;
    handle_interrupt_turn(client, &thread.harness_thread_id, native_turn_id).await
}

async fn handle_terminate_command(
    client: &mut codex_codes::AsyncClient,
    mapper: &CodexMapper,
    thread: &ThreadHandle,
    process_id: &str,
) -> Result<(), HarnessError> {
    let native_turn_id = mapper
        .native_turn_for_process(thread.thread, process_id)
        .or_else(|| mapper.active_native_turn_for_thread(thread.thread))
        .ok_or_else(|| {
            HarnessError::Unsupported(format!(
                "Codex has no active turn for command process {process_id}"
            ))
        })?;
    handle_interrupt_turn(client, &thread.harness_thread_id, native_turn_id).await
}

async fn handle_interrupt_turn(
    client: &mut codex_codes::AsyncClient,
    native_thread_id: &str,
    native_turn_id: &str,
) -> Result<(), HarnessError> {
    let params = codex_codes::TurnInterruptParams {
        thread_id: native_thread_id.to_owned(),
        turn_id: native_turn_id.to_owned(),
    };
    client
        .turn_interrupt(&params)
        .await
        .map_err(|e| HarnessError::Transport(e.to_string()))?;
    Ok(())
}
