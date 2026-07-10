//! Codex CLI harness adapter (spec §4.6).
//!
//! Wraps `codex-codes::AsyncClient` and implements the `AgentHarness` trait.
//! All Codex-specific types are confined to this crate and mapped to
//! `giskard-core` types at the boundary.

mod mapping;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard};
use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{info, warn};

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ServerRequestId, ThreadId, TurnId};
use giskard_core::mcp::{
    McpAuthStatus, McpOauthStart, McpResource, McpResourceTemplate, McpServerInfo, McpServerStatus,
    McpTool,
};
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
    CompactThread {
        thread: ThreadHandle,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    SetThreadName {
        thread: ThreadHandle,
        name: String,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    SetThreadArchived {
        thread: ThreadHandle,
        archived: bool,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    DeleteThread {
        thread: ThreadHandle,
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    ListMcpServers {
        response: oneshot::Sender<Result<Vec<McpServerStatus>, HarnessError>>,
    },
    ReloadMcpServers {
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
    StartMcpOauthLogin {
        name: String,
        response: oneshot::Sender<Result<McpOauthStart, HarnessError>>,
    },
    Shutdown {
        response: oneshot::Sender<Result<(), HarnessError>>,
    },
}

type SenderMap = Arc<StdMutex<HashMap<ThreadId, broadcast::Sender<AgentEvent>>>>;

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
        let client = start_codex_client(codex_codes::AppServerBuilder::new()).await?;
        Self::spawn_harness(client, workspace_root)
    }

    pub async fn start_with(
        workspace_root: PathBuf,
        codex_path: PathBuf,
    ) -> Result<Arc<Self>, HarnessError> {
        let builder = codex_codes::cli::AppServerBuilder::new().command(codex_path);
        let client = start_codex_client(builder).await?;
        Self::spawn_harness(client, workspace_root)
    }

    fn spawn_harness(
        client: codex_codes::AsyncClient,
        workspace_root: PathBuf,
    ) -> Result<Arc<Self>, HarnessError> {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (control_tx, control_rx) = mpsc::channel(64);
        let senders: SenderMap = Arc::new(StdMutex::new(HashMap::new()));

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
                mcp_status: true,
                mcp_reload: true,
                mcp_oauth_login: true,
                context_compaction: true,
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

async fn start_codex_client(
    builder: codex_codes::AppServerBuilder,
) -> Result<codex_codes::AsyncClient, HarnessError> {
    let mut client = codex_codes::AsyncClient::spawn(builder)
        .await
        .map_err(|e| HarnessError::Spawn(e.to_string()))?;
    client
        .initialize(&build_initialize_params())
        .await
        .map_err(|e| HarnessError::Spawn(e.to_string()))?;
    Ok(client)
}

fn build_initialize_params() -> codex_codes::InitializeParams {
    codex_codes::InitializeParams {
        client_info: codex_codes::ClientInfo {
            name: "giskard".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            title: Some("Giskard".into()),
        },
        capabilities: Some(codex_codes::InitializeCapabilities {
            experimental_api: Some(true),
            mcp_server_openai_form_elicitation: None,
            opt_out_notification_methods: None,
            request_attestation: None,
        }),
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

    async fn list_mcp_servers(&self) -> Result<Vec<McpServerStatus>, HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(ControlCommand::ListMcpServers { response: tx })
            .await
            .map_err(|_| HarnessError::Transport("background task closed".into()))?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn reload_mcp_servers(&self) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(ControlCommand::ReloadMcpServers { response: tx })
            .await
            .map_err(|_| HarnessError::Transport("background task closed".into()))?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn start_mcp_oauth_login(&self, name: &str) -> Result<McpOauthStart, HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(ControlCommand::StartMcpOauthLogin {
                name: name.to_owned(),
                response: tx,
            })
            .await
            .map_err(|_| HarnessError::Transport("background task closed".into()))?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
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
        if let Some(sender) = sender_for_thread(&self.senders, thread.thread) {
            return AgentEventStream::new(sender.subscribe());
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

    async fn compact_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(ControlCommand::CompactThread {
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

    async fn set_thread_name(&self, thread: &ThreadHandle, name: &str) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(ControlCommand::SetThreadName {
                thread: thread.clone(),
                name: name.to_owned(),
                response: tx,
            })
            .await
            .map_err(|_| HarnessError::Transport("background task closed".into()))?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn set_thread_archived(
        &self,
        thread: &ThreadHandle,
        archived: bool,
    ) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(ControlCommand::SetThreadArchived {
                thread: thread.clone(),
                archived,
                response: tx,
            })
            .await
            .map_err(|_| HarnessError::Transport("background task closed".into()))?;
        rx.await
            .map_err(|_| HarnessError::Transport("background task dropped response".into()))?
    }

    async fn delete_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(ControlCommand::DeleteThread {
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
    let mut pending_compactions: HashMap<ThreadId, PendingCompaction> = HashMap::new();

    loop {
        tokio::select! {
            msg = client.next_message(), if mapper.has_running_commands() || !pending_compactions.is_empty() => {
                match msg {
                    Ok(Some(msg)) => {
                        match handle_idle_server_message(
                                &mut client,
                                &mut mapper,
                                &senders,
                                &mut control_rx,
                                &mut pending_compactions,
                                msg,
                            )
                            .await
                        {
                            StreamOutcome::TurnEnded => {}
                            StreamOutcome::CompactionCompleted { thread, elapsed_ms } => {
                                info!(
                                    %thread,
                                    elapsed_ms,
                                    pending_compactions = pending_compactions.len(),
                                    "Codex context compaction completion observed"
                                );
                            }
                            StreamOutcome::Shutdown => {
                                let _ = client.shutdown().await;
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        if !pending_compactions.is_empty() {
                            warn!(
                                pending_compactions = pending_compactions.len(),
                                pending_compaction_states = ?pending_compaction_states(&pending_compactions),
                                "Codex message stream ended with pending context compactions"
                            );
                        }
                        break;
                    }
                    Err(e) => {
                        warn!(
                            pending_compactions = pending_compactions.len(),
                            pending_compaction_states = ?pending_compaction_states(&pending_compactions),
                            "Codex idle stream failed while background work was running: {e}"
                        );
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
                    Some(ControlCommand::CompactThread { thread, response }) => {
                        let started = Instant::now();
                        info!(
                            thread = %thread.thread,
                            harness_thread_id = %thread.harness_thread_id,
                            pending_compactions = pending_compactions.len(),
                            "requesting Codex context compaction"
                        );
                        let result = handle_compact_thread(&mut client, &thread).await;
                        match &result {
                            Ok(()) => {
                                pending_compactions
                                    .insert(thread.thread, PendingCompaction::new(started));
                                info!(
                                    thread = %thread.thread,
                                    harness_thread_id = %thread.harness_thread_id,
                                    ack_elapsed_ms = started.elapsed().as_millis(),
                                    pending_compactions = pending_compactions.len(),
                                    "Codex accepted context compaction request"
                                );
                            }
                            Err(error) => {
                                warn!(
                                    thread = %thread.thread,
                                    harness_thread_id = %thread.harness_thread_id,
                                    elapsed_ms = started.elapsed().as_millis(),
                                    "Codex context compaction request failed: {error}"
                                );
                            }
                        }
                        let _ = response.send(result);
                    }
                    Some(ControlCommand::SetThreadName {
                        thread,
                        name,
                        response,
                    }) => {
                        let result = handle_set_thread_name(&mut client, &thread, &name).await;
                        let _ = response.send(result);
                    }
                    Some(ControlCommand::SetThreadArchived {
                        thread,
                        archived,
                        response,
                    }) => {
                        let result = handle_set_thread_archived(&mut client, &thread, archived)
                            .await;
                        let _ = response.send(result);
                    }
                    Some(ControlCommand::DeleteThread { thread, response }) => {
                        let result = handle_delete_thread(&mut client, &thread).await;
                        if result.is_ok() {
                            lock_senders(&senders).remove(&thread.thread);
                        }
                        let _ = response.send(result);
                    }
                    Some(ControlCommand::ListMcpServers { response }) => {
                        let result = handle_list_mcp_servers(&mut client).await;
                        let _ = response.send(result);
                    }
                    Some(ControlCommand::ReloadMcpServers { response }) => {
                        let result = handle_reload_mcp_servers(&mut client).await;
                        let _ = response.send(result);
                    }
                    Some(ControlCommand::StartMcpOauthLogin { name, response }) => {
                        let result = handle_start_mcp_oauth_login(&mut client, &name).await;
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
    pending_compactions: &mut HashMap<ThreadId, PendingCompaction>,
    msg: codex_codes::ServerMessage,
) -> StreamOutcome {
    let fallback_thread = mapper.running_command_fallback_thread().unwrap_or_default();
    match msg {
        codex_codes::ServerMessage::Notification(notif) => {
            if let Some(event) = mapper.map_notification(&notif, fallback_thread) {
                let thread = event_thread(&event);
                let completed_compaction =
                    observe_pending_compaction(pending_compactions, thread, &event);
                let _ = broadcast_event(senders, thread, || event).await;
                if let Some(elapsed_ms) = completed_compaction {
                    return StreamOutcome::CompactionCompleted { thread, elapsed_ms };
                }
            }
            StreamOutcome::TurnEnded
        }
        codex_codes::ServerMessage::Request { id, request } => {
            let waiting_for = id.to_string();
            let Some(event) = mapper.map_server_request(&id, &request, fallback_thread) else {
                respond_unroutable_server_request(client, &id).await;
                return StreamOutcome::TurnEnded;
            };
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
                    Some(ControlCommand::CompactThread { response, .. }) => {
                        let _ = response.send(Err(HarnessError::Unsupported(
                            "context compaction is not available during an active turn".into(),
                        )));
                    }
                    Some(ControlCommand::SetThreadName {
                        thread,
                        name,
                        response,
                    }) => {
                        let result = handle_set_thread_name(client, &thread, &name).await;
                        let _ = response.send(result);
                    }
                    Some(ControlCommand::SetThreadArchived { response, .. }) => {
                        let _ = response.send(Err(HarnessError::Unsupported(
                            "thread archiving is not available during an active turn".into(),
                        )));
                    }
                    Some(ControlCommand::DeleteThread { response, .. }) => {
                        let _ = response.send(Err(HarnessError::Unsupported(
                            "thread deletion is not available during an active turn".into(),
                        )));
                    }
                    Some(ControlCommand::ListMcpServers { response }) => {
                        let result = handle_list_mcp_servers(client).await;
                        let _ = response.send(result);
                    }
                    Some(ControlCommand::ReloadMcpServers { response }) => {
                        let result = handle_reload_mcp_servers(client).await;
                        let _ = response.send(result);
                    }
                    Some(ControlCommand::StartMcpOauthLogin { name, response }) => {
                        let result = handle_start_mcp_oauth_login(client, &name).await;
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
    CompactionCompleted { thread: ThreadId, elapsed_ms: u128 },
    Shutdown,
}

#[derive(Debug)]
struct PendingCompaction {
    started_at: Instant,
    saw_turn_started: bool,
}

impl PendingCompaction {
    fn new(started_at: Instant) -> Self {
        Self {
            started_at,
            saw_turn_started: false,
        }
    }

    fn observe(&mut self, event: &AgentEvent) -> bool {
        match event {
            AgentEvent::TurnStarted { .. } => {
                self.saw_turn_started = true;
                false
            }
            AgentEvent::ItemCompleted { item, .. }
                if is_context_compaction_activity(item) && !self.saw_turn_started =>
            {
                true
            }
            AgentEvent::TurnCompleted { .. } => true,
            _ => false,
        }
    }
}

fn observe_pending_compaction(
    pending_compactions: &mut HashMap<ThreadId, PendingCompaction>,
    thread: ThreadId,
    event: &AgentEvent,
) -> Option<u128> {
    let event_name = compaction_event_name(event)?;
    let event_turn = agent_event_turn(event);
    let pending = pending_compactions.get_mut(&thread)?;
    let saw_turn_started_before = pending.saw_turn_started;
    let elapsed_ms = pending.started_at.elapsed().as_millis();
    let completed = pending.observe(event);
    info!(
        %thread,
        ?event_turn,
        event = event_name,
        saw_turn_started_before,
        saw_turn_started_after = pending.saw_turn_started,
        completed,
        elapsed_ms,
        "observed Codex context compaction event"
    );
    if !completed {
        return None;
    }
    pending_compactions
        .remove(&thread)
        .map(|pending| pending.started_at.elapsed().as_millis())
}

fn compaction_event_name(event: &AgentEvent) -> Option<&'static str> {
    match event {
        AgentEvent::TurnStarted { .. } => Some("turn_started"),
        AgentEvent::ItemCompleted { item, .. } if is_context_compaction_activity(item) => {
            Some("context_compacted_item")
        }
        AgentEvent::TurnCompleted { .. } => Some("turn_completed"),
        _ => None,
    }
}

fn agent_event_turn(event: &AgentEvent) -> Option<TurnId> {
    match event {
        AgentEvent::TurnStarted { turn, .. }
        | AgentEvent::ItemStarted { turn, .. }
        | AgentEvent::ItemDelta { turn, .. }
        | AgentEvent::ItemCompleted { turn, .. }
        | AgentEvent::DiffUpdated { turn, .. }
        | AgentEvent::ApprovalRequested { turn, .. }
        | AgentEvent::TurnCompleted { turn, .. } => Some(*turn),
        AgentEvent::ServerRequestReceived { turn, .. }
        | AgentEvent::ServerRequestResolved { turn, .. }
        | AgentEvent::Error { turn, .. }
        | AgentEvent::Notice { turn, .. } => *turn,
        AgentEvent::ThreadOpened { .. } => None,
    }
}

fn pending_compaction_states(
    pending_compactions: &HashMap<ThreadId, PendingCompaction>,
) -> Vec<String> {
    pending_compactions
        .iter()
        .map(|(thread, pending)| {
            format!(
                "{thread}:saw_turn_started={},elapsed_ms={}",
                pending.saw_turn_started,
                pending.started_at.elapsed().as_millis()
            )
        })
        .collect()
}

fn is_context_compaction_activity(item: &giskard_core::item::Item) -> bool {
    matches!(
        &item.payload,
        giskard_core::item::ItemPayload::Activity { title, .. } if title == "Context compacted"
    )
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
    ensure_thread_sender(senders, thread_id, tx);

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
    let params = build_turn_start_params(thread, input, overrides)?;
    let _resp: codex_codes::TurnStartResponse = client
        .request(codex_codes::protocol::methods::TURN_START, &params)
        .await
        .map_err(|e| HarnessError::Transport(e.to_string()))?;

    Ok(TurnId::new())
}

fn build_turn_start_params(
    thread: &ThreadHandle,
    input: &UserInput,
    overrides: &TurnOverrides,
) -> Result<serde_json::Value, HarnessError> {
    let codex_input = mapping::map_user_input(input);
    let sandbox_policy = mapping::map_mode_to_sandbox(overrides.mode);
    let approval_policy = mapping::map_approval_policy(overrides.approval_policy);
    let effort = overrides
        .model
        .as_ref()
        .and_then(|m| m.reasoning_effort)
        .map(mapping::map_effort);

    let mut params = serde_json::json!({
        "threadId": thread.harness_thread_id,
        "input": codex_input,
        "sandboxPolicy": sandbox_policy,
        "approvalPolicy": approval_policy,
    });
    let Some(map) = params.as_object_mut() else {
        return Err(HarnessError::Protocol(
            "turn/start params must serialize as an object".into(),
        ));
    };

    if let Some(model) = overrides.model.as_ref() {
        map.insert("model".into(), serde_json::json!(model.model));
        if let Some(effort) = effort.as_ref() {
            map.insert("effort".into(), serde_json::json!(effort));
        }
        map.insert(
            "collaborationMode".into(),
            serde_json::json!({
                "mode": mapping::map_mode_to_collaboration_mode(overrides.mode),
                "settings": {
                    "model": model.model,
                    "reasoning_effort": effort,
                    "developer_instructions": null,
                }
            }),
        );
    }

    Ok(params)
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
                let event = mapper.map_notification(&notif, thread_id);
                if let Some(event) = event {
                    let event_thread_id = event_thread(&event);
                    let is_current_thread = event_belongs_to_stream(thread_id, &event);
                    if is_current_thread {
                        if let AgentEvent::TurnStarted { turn, .. } = &event {
                            active_turn = Some(*turn);
                        }
                    }
                    let is_completed = event_completes_stream(thread_id, &event);
                    let _ = broadcast_event(senders, event_thread_id, || event).await;
                    if is_completed {
                        break;
                    }
                    if is_current_thread {
                        if let Some(message) = mapping::fatal_turn_error(&notif) {
                            // Mint a turn id when the error arrived before any `turn/started` (e.g.
                            // an immediate quota rejection) so the failed attempt is still persisted.
                            let turn = active_turn.unwrap_or_default();
                            let _ =
                                broadcast_event(senders, thread_id, || AgentEvent::TurnCompleted {
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
                } else if let Some(message) = mapping::fatal_turn_error(&notif) {
                    warn!(
                        %thread_id,
                        "dropping fatal Codex error notification that could not be mapped to a known thread: {message}"
                    );
                }
            }
            codex_codes::ServerMessage::Request { id, request } => {
                let waiting_for = id.to_string();
                let Some(event) = mapper.map_server_request(&id, &request, thread_id) else {
                    respond_unroutable_server_request(client, &id).await;
                    continue;
                };
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
                        Some(ControlCommand::CompactThread { response, .. }) => {
                            let _ = response.send(Err(HarnessError::Unsupported(
                                "context compaction is not available during an active turn".into(),
                            )));
                        }
                        Some(ControlCommand::SetThreadName {
                            thread,
                            name,
                            response,
                        }) => {
                            let result = handle_set_thread_name(client, &thread, &name).await;
                            let _ = response.send(result);
                        }
                        Some(ControlCommand::SetThreadArchived { response, .. }) => {
                            let _ = response.send(Err(HarnessError::Unsupported(
                                "thread archiving is not available during an active turn".into(),
                            )));
                        }
                        Some(ControlCommand::DeleteThread { response, .. }) => {
                            let _ = response.send(Err(HarnessError::Unsupported(
                                "thread deletion is not available during an active turn".into(),
                            )));
                        }
                        Some(ControlCommand::ListMcpServers { response }) => {
                            let result = handle_list_mcp_servers(client).await;
                            let _ = response.send(result);
                        }
                        Some(ControlCommand::ReloadMcpServers { response }) => {
                            let result = handle_reload_mcp_servers(client).await;
                            let _ = response.send(result);
                        }
                        Some(ControlCommand::StartMcpOauthLogin { name, response }) => {
                            let result = handle_start_mcp_oauth_login(client, &name).await;
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
        Some(ControlCommand::CompactThread { response, .. }) => {
            let _ = response.send(Err(HarnessError::Unsupported(
                "context compaction is not available during an active turn".into(),
            )));
            StreamControlOutcome::Continue
        }
        Some(ControlCommand::SetThreadName {
            thread,
            name,
            response,
        }) => {
            let result = handle_set_thread_name(client, &thread, &name).await;
            let _ = response.send(result);
            StreamControlOutcome::Continue
        }
        Some(ControlCommand::SetThreadArchived { response, .. }) => {
            let _ = response.send(Err(HarnessError::Unsupported(
                "thread archiving is not available during an active turn".into(),
            )));
            StreamControlOutcome::Continue
        }
        Some(ControlCommand::DeleteThread { response, .. }) => {
            let _ = response.send(Err(HarnessError::Unsupported(
                "thread deletion is not available during an active turn".into(),
            )));
            StreamControlOutcome::Continue
        }
        Some(ControlCommand::ListMcpServers { response }) => {
            let result = handle_list_mcp_servers(client).await;
            let _ = response.send(result);
            StreamControlOutcome::Continue
        }
        Some(ControlCommand::ReloadMcpServers { response }) => {
            let result = handle_reload_mcp_servers(client).await;
            let _ = response.send(result);
            StreamControlOutcome::Continue
        }
        Some(ControlCommand::StartMcpOauthLogin { name, response }) => {
            let result = handle_start_mcp_oauth_login(client, &name).await;
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
    let sender = sender_for_thread(senders, thread);
    if let Some(sender) = sender {
        let _ = sender.send(f());
    }
}

fn lock_senders(
    senders: &SenderMap,
) -> StdMutexGuard<'_, HashMap<ThreadId, broadcast::Sender<AgentEvent>>> {
    match senders.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!("Codex sender map lock was poisoned; recovering sender state");
            poisoned.into_inner()
        }
    }
}

fn sender_for_thread(
    senders: &SenderMap,
    thread: ThreadId,
) -> Option<broadcast::Sender<AgentEvent>> {
    lock_senders(senders).get(&thread).cloned()
}

fn ensure_thread_sender(
    senders: &SenderMap,
    thread: ThreadId,
    sender: broadcast::Sender<AgentEvent>,
) {
    lock_senders(senders).entry(thread).or_insert(sender);
}

async fn respond_unroutable_server_request(
    client: &mut codex_codes::AsyncClient,
    id: &codex_codes::jsonrpc::RequestId,
) {
    let message = "Giskard cannot route this Codex server request to a known thread.";
    if let Err(error) = client.respond_error(id.clone(), -32000, message).await {
        warn!(%id, %error, "failed to reject unroutable Codex server request");
    } else {
        warn!(%id, "rejected unroutable Codex server request");
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

fn event_belongs_to_stream(stream_thread: ThreadId, event: &AgentEvent) -> bool {
    event_thread(event) == stream_thread
}

fn event_completes_stream(stream_thread: ThreadId, event: &AgentEvent) -> bool {
    event_belongs_to_stream(stream_thread, event)
        && matches!(event, AgentEvent::TurnCompleted { .. })
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

async fn handle_compact_thread(
    client: &mut codex_codes::AsyncClient,
    thread: &ThreadHandle,
) -> Result<(), HarnessError> {
    let params = codex_codes::ThreadCompactStartParams {
        thread_id: thread.harness_thread_id.clone(),
    };
    let _: codex_codes::ThreadCompactStartResponse = client
        .request(
            codex_codes::protocol::methods::THREAD_COMPACT_START,
            &params,
        )
        .await
        .map_err(|e| HarnessError::Transport(e.to_string()))?;
    Ok(())
}

async fn handle_set_thread_archived(
    client: &mut codex_codes::AsyncClient,
    thread: &ThreadHandle,
    archived: bool,
) -> Result<(), HarnessError> {
    if archived {
        let params = codex_codes::ThreadArchiveParams {
            thread_id: thread.harness_thread_id.clone(),
        };
        client
            .thread_archive(&params)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))?;
    } else {
        let params = codex_codes::ThreadUnarchiveParams {
            thread_id: thread.harness_thread_id.clone(),
        };
        let _: codex_codes::ThreadUnarchiveResponse = client
            .request(codex_codes::protocol::methods::THREAD_UNARCHIVE, &params)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))?;
    }
    Ok(())
}

async fn handle_set_thread_name(
    client: &mut codex_codes::AsyncClient,
    thread: &ThreadHandle,
    name: &str,
) -> Result<(), HarnessError> {
    let params = codex_codes::ThreadSetNameParams {
        thread_id: thread.harness_thread_id.clone(),
        name: name.to_owned(),
    };
    let _: codex_codes::ThreadSetNameResponse = client
        .request(codex_codes::protocol::methods::THREAD_NAME_SET, &params)
        .await
        .map_err(|e| HarnessError::Transport(e.to_string()))?;
    Ok(())
}

async fn handle_list_mcp_servers(
    client: &mut codex_codes::AsyncClient,
) -> Result<Vec<McpServerStatus>, HarnessError> {
    let mut out = Vec::new();
    let mut cursor = None;

    loop {
        let params = codex_codes::ListMcpServerStatusParams {
            cursor: cursor.clone(),
            detail: Some(codex_codes::McpServerStatusDetail::Full),
            limit: None,
            thread_id: None,
        };
        let page: codex_codes::ListMcpServerStatusResponse = client
            .request(
                codex_codes::protocol::methods::MCPSERVERSTATUS_LIST,
                &params,
            )
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))?;

        out.extend(page.data.into_iter().map(map_mcp_server_status));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }

    Ok(out)
}

async fn handle_reload_mcp_servers(
    client: &mut codex_codes::AsyncClient,
) -> Result<(), HarnessError> {
    let _: codex_codes::McpServerRefreshResponse = client
        .request(
            codex_codes::protocol::methods::CONFIG_MCPSERVER_RELOAD,
            &serde_json::json!({}),
        )
        .await
        .map_err(|e| HarnessError::Transport(e.to_string()))?;
    Ok(())
}

async fn handle_start_mcp_oauth_login(
    client: &mut codex_codes::AsyncClient,
    name: &str,
) -> Result<McpOauthStart, HarnessError> {
    let params = codex_codes::McpServerOauthLoginParams {
        name: name.to_owned(),
        scopes: None,
        thread_id: None,
        timeout_secs: None,
    };
    let response: codex_codes::McpServerOauthLoginResponse = client
        .request(
            codex_codes::protocol::methods::MCPSERVER_OAUTH_LOGIN,
            &params,
        )
        .await
        .map_err(|e| HarnessError::Transport(e.to_string()))?;
    Ok(McpOauthStart {
        authorization_url: response.authorization_url,
    })
}

fn map_mcp_server_status(status: codex_codes::McpServerStatus) -> McpServerStatus {
    McpServerStatus {
        name: status.name,
        auth_status: map_mcp_auth_status(status.auth_status),
        server_info: status.server_info.map(map_mcp_server_info),
        tools: status.tools.into_values().map(map_mcp_tool).collect(),
        resources: status.resources.into_iter().map(map_mcp_resource).collect(),
        resource_templates: status
            .resource_templates
            .into_iter()
            .map(map_mcp_resource_template)
            .collect(),
    }
}

fn map_mcp_auth_status(status: codex_codes::McpAuthStatus) -> McpAuthStatus {
    match status {
        codex_codes::McpAuthStatus::Unsupported => McpAuthStatus::Unsupported,
        codex_codes::McpAuthStatus::NotLoggedIn => McpAuthStatus::NotLoggedIn,
        codex_codes::McpAuthStatus::BearerToken => McpAuthStatus::BearerToken,
        codex_codes::McpAuthStatus::OAuth => McpAuthStatus::OAuth,
    }
}

fn map_mcp_server_info(info: codex_codes::McpServerInfo) -> McpServerInfo {
    McpServerInfo {
        name: info.name,
        title: info.title,
        description: info.description,
        version: (!info.version.is_empty()).then_some(info.version),
        website_url: info.website_url,
    }
}

fn map_mcp_tool(tool: codex_codes::Tool) -> McpTool {
    McpTool {
        name: tool.name,
        title: tool.title,
        description: tool.description,
        input_schema: tool.input_schema,
        output_schema: tool.output_schema,
    }
}

fn map_mcp_resource(resource: codex_codes::Resource) -> McpResource {
    McpResource {
        name: resource.name,
        uri: resource.uri,
        title: resource.title,
        description: resource.description,
        mime_type: resource.mime_type,
        size: resource.size,
    }
}

fn map_mcp_resource_template(template: codex_codes::ResourceTemplate) -> McpResourceTemplate {
    McpResourceTemplate {
        name: template.name,
        uri_template: template.uri_template,
        title: template.title,
        description: template.description,
        mime_type: template.mime_type,
    }
}

async fn handle_delete_thread(
    client: &mut codex_codes::AsyncClient,
    thread: &ThreadHandle,
) -> Result<(), HarnessError> {
    let params = codex_codes::ThreadDeleteParams {
        thread_id: thread.harness_thread_id.clone(),
    };
    client
        .thread_delete(&params)
        .await
        .map_err(|e| HarnessError::Transport(e.to_string()))?;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use giskard_core::ids::ItemId;
    use giskard_core::item::{Item, ItemPayload};
    use giskard_core::model::{Effort, ModelRef};
    use giskard_core::turn::{ApprovalPolicy, Mode};

    fn test_thread() -> ThreadHandle {
        ThreadHandle {
            thread: ThreadId::new(),
            harness_thread_id: "native-thread".into(),
            warning: None,
        }
    }

    fn test_model(effort: Option<Effort>) -> ModelRef {
        ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: effort,
        }
    }

    fn turn_overrides(mode: Mode, effort: Option<Effort>) -> TurnOverrides {
        TurnOverrides {
            model: Some(test_model(effort)),
            mode,
            approval_policy: ApprovalPolicy::Ask,
        }
    }

    fn context_compacted_event(thread: ThreadId, turn: TurnId) -> AgentEvent {
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: ItemId::new(),
                harness_item_id: format!("context_compacted:{turn}"),
                payload: ItemPayload::Activity {
                    title: "Context compacted".into(),
                    detail: None,
                    metadata: None,
                },
                created_at: Utc::now(),
            },
        }
    }

    fn completed_event(thread: ThreadId, turn: TurnId) -> AgentEvent {
        AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }
    }

    #[test]
    fn foreign_turn_completion_does_not_end_live_stream() {
        let stream_thread = ThreadId::new();
        let foreign_thread = ThreadId::new();
        let turn = TurnId::new();
        let event = completed_event(foreign_thread, turn);

        assert!(!event_belongs_to_stream(stream_thread, &event));
        assert!(!event_completes_stream(stream_thread, &event));
        assert!(event_completes_stream(foreign_thread, &event));
    }

    #[test]
    fn opening_thread_preserves_existing_sender() {
        let thread = ThreadId::new();
        let senders: SenderMap = Arc::new(StdMutex::new(HashMap::new()));
        let (first_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let mut first_rx = first_tx.subscribe();
        ensure_thread_sender(&senders, thread, first_tx);

        let (replacement_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        ensure_thread_sender(&senders, thread, replacement_tx);

        let turn = TurnId::new();
        sender_for_thread(&senders, thread)
            .expect("sender exists")
            .send(AgentEvent::TurnStarted { thread, turn })
            .unwrap();
        assert!(matches!(
            first_rx.try_recv(),
            Ok(AgentEvent::TurnStarted { thread: got_thread, turn: got_turn })
                if got_thread == thread && got_turn == turn
        ));
    }

    #[test]
    fn pending_compaction_marker_only_completes_without_turn_started() {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let mut pending = HashMap::new();
        pending.insert(thread, PendingCompaction::new(Instant::now()));

        let elapsed_ms = observe_pending_compaction(
            &mut pending,
            thread,
            &context_compacted_event(thread, turn),
        );

        assert!(elapsed_ms.is_some());
        assert!(!pending.contains_key(&thread));
    }

    #[test]
    fn pending_compaction_marker_after_turn_started_waits_for_turn_completed() {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let mut pending = HashMap::new();
        pending.insert(thread, PendingCompaction::new(Instant::now()));

        let started = AgentEvent::TurnStarted { thread, turn };
        assert!(observe_pending_compaction(&mut pending, thread, &started).is_none());
        assert!(pending.get(&thread).unwrap().saw_turn_started);

        let marker = observe_pending_compaction(
            &mut pending,
            thread,
            &context_compacted_event(thread, turn),
        );
        assert!(marker.is_none());
        assert!(pending.contains_key(&thread));

        let completed =
            observe_pending_compaction(&mut pending, thread, &completed_event(thread, turn));
        assert!(completed.is_some());
        assert!(!pending.contains_key(&thread));
    }

    #[test]
    fn mcp_server_status_maps_codex_metadata() {
        let mut tools = std::collections::BTreeMap::new();
        tools.insert(
            "jira_search".into(),
            codex_codes::Tool {
                _meta: None,
                annotations: None,
                description: Some("Search Jira".into()),
                icons: None,
                input_schema: serde_json::json!({"type": "object"}),
                name: "jira_search".into(),
                output_schema: Some(serde_json::json!({"type": "object"})),
                title: Some("Jira Search".into()),
            },
        );

        let mapped = map_mcp_server_status(codex_codes::McpServerStatus {
            auth_status: codex_codes::McpAuthStatus::NotLoggedIn,
            name: "cf-mcp".into(),
            resource_templates: vec![codex_codes::ResourceTemplate {
                annotations: None,
                description: Some("Issue by key".into()),
                mime_type: Some("application/json".into()),
                name: "jira issue".into(),
                title: Some("Jira Issue".into()),
                uri_template: "jira://issue/{key}".into(),
            }],
            resources: vec![codex_codes::Resource {
                _meta: None,
                annotations: None,
                description: Some("Project metadata".into()),
                icons: None,
                mime_type: Some("application/json".into()),
                name: "project".into(),
                size: Some(42),
                title: Some("Project".into()),
                uri: "gitlab://project/group/name".into(),
            }],
            server_info: Some(codex_codes::McpServerInfo {
                description: Some("Cloudflare tools".into()),
                icons: None,
                name: "cf-mcp".into(),
                title: Some("Cloudflare MCP".into()),
                version: "1.2.3".into(),
                website_url: Some("https://example.invalid".into()),
            }),
            tools,
        });

        assert_eq!(mapped.name, "cf-mcp");
        assert_eq!(mapped.auth_status, McpAuthStatus::NotLoggedIn);
        assert_eq!(mapped.server_info.unwrap().title.unwrap(), "Cloudflare MCP");
        assert_eq!(mapped.tools[0].name, "jira_search");
        assert_eq!(mapped.tools[0].description.as_deref(), Some("Search Jira"));
        assert_eq!(mapped.resources[0].uri, "gitlab://project/group/name");
        assert_eq!(
            mapped.resource_templates[0].uri_template,
            "jira://issue/{key}"
        );
    }

    #[test]
    fn initialize_params_enable_experimental_app_server_api() {
        let params = serde_json::to_value(build_initialize_params()).unwrap();

        assert_eq!(params["clientInfo"]["name"], "giskard");
        assert_eq!(params["capabilities"]["experimentalApi"], true);
    }

    #[test]
    fn plan_turn_start_params_include_plan_collaboration_mode() {
        let params = build_turn_start_params(
            &test_thread(),
            &UserInput::text("make a plan"),
            &turn_overrides(Mode::Plan, Some(Effort::Medium)),
        )
        .unwrap();

        assert_eq!(params["threadId"], "native-thread");
        assert_eq!(params["model"], "gpt-5.5");
        assert_eq!(params["effort"], "medium");
        assert_eq!(params["collaborationMode"]["mode"], "plan");
        assert_eq!(params["collaborationMode"]["settings"]["model"], "gpt-5.5");
        assert_eq!(
            params["collaborationMode"]["settings"]["reasoning_effort"],
            "medium"
        );
        assert!(params["collaborationMode"]["settings"]["developer_instructions"].is_null());
    }

    #[test]
    fn build_turn_start_params_reset_collaboration_mode_to_default() {
        let params = build_turn_start_params(
            &test_thread(),
            &UserInput::text("implement it"),
            &turn_overrides(Mode::Build, None),
        )
        .unwrap();

        assert_eq!(params["collaborationMode"]["mode"], "default");
        assert_eq!(params["collaborationMode"]["settings"]["model"], "gpt-5.5");
        assert!(params.get("effort").is_none());
        assert!(params["collaborationMode"]["settings"]["reasoning_effort"].is_null());
    }
}
