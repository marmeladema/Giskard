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

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ThreadId, TurnId};
use giskard_core::model::ModelDescriptor;
use giskard_core::turn::TurnOverrides;
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
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
    RespondApproval {
        id: ApprovalId,
        decision: ApprovalDecision,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    Interrupt {
        thread: ThreadHandle,
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
        let senders: SenderMap = Arc::new(Mutex::new(HashMap::new()));

        let harness = Arc::new(Self {
            cmd_tx,
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

        tokio::spawn(background_task(client, cmd_rx, senders, workspace_root));
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
        self.cmd_tx
            .send(HarnessCommand::RespondApproval {
                id,
                decision,
                response: tx,
            })
            .await
            .map_err(|_| HarnessError::Transport("background task closed".into()))?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn interrupt(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(HarnessCommand::Interrupt {
                thread: thread.clone(),
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
            .cmd_tx
            .send(HarnessCommand::Shutdown { response: tx })
            .await;
        let _ = rx.await;
        Ok(())
    }
}

async fn background_task(
    mut client: codex_codes::AsyncClient,
    mut cmd_rx: mpsc::Receiver<HarnessCommand>,
    senders: SenderMap,
    workspace_root: PathBuf,
) {
    let mapper = CodexMapper::new(workspace_root);

    loop {
        let cmd = match cmd_rx.recv().await {
            Some(cmd) => cmd,
            None => break,
        };

        match cmd {
            HarnessCommand::OpenThread { opts, response } => {
                let result = handle_open_thread(&mut client, &mapper, &opts, &senders).await;
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
                if ok {
                    stream_turn_events(&mut client, &mapper, &thread, &senders, &mut cmd_rx).await;
                }
            }
            HarnessCommand::RespondApproval {
                id,
                decision,
                response,
            } => {
                let result = handle_respond(&mut client, &id, &decision).await;
                let _ = response.send(result);
            }
            HarnessCommand::Interrupt { thread, response } => {
                let result = handle_interrupt(&mut client, &thread).await;
                let _ = response.send(result);
            }
            HarnessCommand::Shutdown { response } => {
                let _ = client.shutdown().await;
                let _ = response.send(Ok(()));
                break;
            }
        }
    }
}

async fn handle_open_thread(
    client: &mut codex_codes::AsyncClient,
    _mapper: &CodexMapper,
    opts: &OpenThreadOptions,
    senders: &SenderMap,
) -> Result<ThreadHandle, HarnessError> {
    let cwd = opts.workspace_root.to_string_lossy().to_string();

    let (harness_thread_id, _) = if let Some(ref resume_id) = opts.resume {
        let params: codex_codes::ThreadResumeParams = serde_json::from_value(serde_json::json!({
            "threadId": resume_id,
            "cwd": cwd,
        }))
        .map_err(|e| HarnessError::Protocol(e.to_string()))?;
        let resp = client
            .thread_resume(&params)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))?;
        (resp.thread.id, ())
    } else {
        let params: codex_codes::ThreadStartParams = serde_json::from_value(serde_json::json!({
            "cwd": cwd,
            "model": opts.initial_model.model,
            "modelProvider": opts.initial_model.provider,
        }))
        .map_err(|e| HarnessError::Protocol(e.to_string()))?;
        let resp = client
            .thread_start(&params)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))?;
        (resp.thread.id, ())
    };

    let thread_id = ThreadId::new();
    let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
    senders.lock().await.insert(thread_id, tx);

    let _ = broadcast_event(senders, thread_id, || AgentEvent::ThreadOpened {
        thread: thread_id,
        harness_thread_id: harness_thread_id.clone(),
    })
    .await;

    Ok(ThreadHandle {
        thread: thread_id,
        harness_thread_id,
    })
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
    let effort = overrides.reasoning_effort.map(mapping::map_effort);

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
    mapper: &CodexMapper,
    thread: &ThreadHandle,
    senders: &SenderMap,
    cmd_rx: &mut mpsc::Receiver<HarnessCommand>,
) {
    let thread_id = thread.thread;

    loop {
        let msg = match client.next_message().await {
            Ok(Some(msg)) => msg,
            Ok(None) => break,
            Err(e) => {
                let _ = broadcast_event(senders, thread_id, || AgentEvent::Error {
                    thread: thread_id,
                    turn: None,
                    error: HarnessError::Transport(e.to_string()),
                })
                .await;
                break;
            }
        };

        match msg {
            codex_codes::ServerMessage::Notification(notif) => {
                let event = mapper.map_notification(&notif, thread_id);
                if let Some(event) = event {
                    let is_completed = matches!(event, AgentEvent::TurnCompleted { .. });
                    let _ = broadcast_event(senders, thread_id, || event).await;
                    if is_completed {
                        break;
                    }
                }
            }
            codex_codes::ServerMessage::Request { id, request } => {
                let event = mapper.map_server_request(&id, &request, thread_id);
                if let Some(event) = event {
                    let _ = broadcast_event(senders, thread_id, || event).await;
                }

                // Wait for the user's approval response.
                loop {
                    match cmd_rx.recv().await {
                        Some(HarnessCommand::RespondApproval {
                            id: resp_id,
                            decision,
                            response,
                        }) => {
                            let result = handle_respond(client, &resp_id, &decision).await;
                            let _ = response.send(result);
                            break;
                        }
                        Some(HarnessCommand::Interrupt {
                            thread: t,
                            response,
                        }) => {
                            let result = handle_interrupt(client, &t).await;
                            let _ = response.send(result);
                        }
                        Some(HarnessCommand::Shutdown { response }) => {
                            let _ = response.send(Ok(()));
                            return;
                        }
                        Some(_) => {}
                        None => return,
                    }
                }
            }
        }
    }
}

async fn broadcast_event<F: FnOnce() -> AgentEvent>(senders: &SenderMap, thread: ThreadId, f: F) {
    let sender = senders.lock().await.get(&thread).cloned();
    if let Some(sender) = sender {
        let _ = sender.send(f());
    }
}

async fn handle_respond(
    client: &mut codex_codes::AsyncClient,
    id: &ApprovalId,
    decision: &ApprovalDecision,
) -> Result<(), HarnessError> {
    let response_json = mapping::map_approval_decision(decision);
    let request_id = codex_codes::jsonrpc::RequestId::String(id.0.clone());
    client
        .respond(request_id, &response_json)
        .await
        .map_err(|e| HarnessError::Transport(e.to_string()))
}

async fn handle_interrupt(
    client: &mut codex_codes::AsyncClient,
    thread: &ThreadHandle,
) -> Result<(), HarnessError> {
    let params: codex_codes::TurnInterruptParams = serde_json::from_value(serde_json::json!({
        "threadId": thread.harness_thread_id,
        "turnId": "",
    }))
    .map_err(|e| HarnessError::Protocol(e.to_string()))?;

    client
        .turn_interrupt(&params)
        .await
        .map_err(|e| HarnessError::Transport(e.to_string()))?;
    Ok(())
}
