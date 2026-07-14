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
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, info, warn};

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
const TURN_FIRST_EVENT_WARN_AFTER: Duration = Duration::from_secs(15);

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

#[async_trait]
trait CodexTransport: Send {
    async fn request_json(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, HarnessError>;

    async fn next_message(&mut self) -> Result<Option<codex_codes::ServerMessage>, HarnessError>;

    async fn respond_json(
        &mut self,
        id: codex_codes::jsonrpc::RequestId,
        value: serde_json::Value,
    ) -> Result<(), HarnessError>;

    async fn respond_error_json(
        &mut self,
        id: codex_codes::jsonrpc::RequestId,
        code: i64,
        message: &str,
    ) -> Result<(), HarnessError>;

    async fn shutdown_transport(self) -> Result<(), HarnessError>
    where
        Self: Sized;
}

#[async_trait]
impl CodexTransport for codex_codes::AsyncClient {
    async fn request_json(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, HarnessError> {
        self.request(method, &params)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))
    }

    async fn next_message(&mut self) -> Result<Option<codex_codes::ServerMessage>, HarnessError> {
        self.next_message()
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))
    }

    async fn respond_json(
        &mut self,
        id: codex_codes::jsonrpc::RequestId,
        value: serde_json::Value,
    ) -> Result<(), HarnessError> {
        self.respond(id, &value)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))
    }

    async fn respond_error_json(
        &mut self,
        id: codex_codes::jsonrpc::RequestId,
        code: i64,
        message: &str,
    ) -> Result<(), HarnessError> {
        self.respond_error(id, code, message)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))
    }

    async fn shutdown_transport(self) -> Result<(), HarnessError> {
        self.shutdown()
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))
    }
}

async fn codex_request<P, R>(
    client: &mut dyn CodexTransport,
    method: &str,
    params: &P,
) -> Result<R, HarnessError>
where
    P: Serialize + Sync,
    R: DeserializeOwned,
{
    let params = serde_json::to_value(params).map_err(|e| HarnessError::Protocol(e.to_string()))?;
    let response = client.request_json(method, params).await?;
    serde_json::from_value(response).map_err(|e| HarnessError::Protocol(e.to_string()))
}

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

    fn spawn_harness<C>(client: C, workspace_root: PathBuf) -> Result<Arc<Self>, HarnessError>
    where
        C: CodexTransport + 'static,
    {
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

async fn background_task<C>(
    mut client: C,
    mut cmd_rx: mpsc::Receiver<HarnessCommand>,
    mut control_rx: mpsc::Receiver<ControlCommand>,
    senders: SenderMap,
    workspace_root: PathBuf,
) where
    C: CodexTransport,
{
    let mut mapper = CodexMapper::new(workspace_root);
    let mut pending_compactions: HashMap<ThreadId, PendingCompaction> = HashMap::new();
    let mut active_turns: ActiveTurns = HashMap::new();
    let mut first_event_warn_tick = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            msg = client.next_message(), if should_poll_codex_messages(&mapper, &active_turns, &pending_compactions) => {
                match msg {
                    Ok(Some(msg)) => {
                        match handle_background_server_message(
                                &mut client,
                                &mut mapper,
                                &senders,
                                &mut pending_compactions,
                                &mut active_turns,
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
                            let _ = client.shutdown_transport().await;
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        emit_incomplete_active_turns(
                            &senders,
                            &mut mapper,
                            &mut active_turns,
                            "Codex stream ended before turn completion",
                        )
                        .await;
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
                        let message = e.to_string();
                        if active_turns.is_empty() {
                            warn!(
                                pending_compactions = pending_compactions.len(),
                                pending_compaction_states = ?pending_compaction_states(&pending_compactions),
                                "Codex idle stream failed while background work was running: {message}"
                            );
                        } else {
                            warn!(
                                active_turns = active_turns.len(),
                                pending_compactions = pending_compactions.len(),
                                pending_compaction_states = ?pending_compaction_states(&pending_compactions),
                                "Codex stream failed before all active turns completed: {message}"
                            );
                            emit_incomplete_active_turns(
                                &senders,
                                &mut mapper,
                                &mut active_turns,
                                format!("Codex stream failed before turn completion: {message}"),
                            )
                            .await;
                            break;
                        }
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
                        let result = handle_start_turn(
                            &mut client,
                            &mut mapper,
                            &thread,
                            &input,
                            &overrides,
                        )
                        .await;
                        let acknowledged_turn = result.as_ref().ok().copied();
                        let _ = response.send(result);
                        if let Some(acknowledged_turn) = acknowledged_turn {
                            active_turns.insert(
                                thread.thread,
                                ActiveTurn::new(thread, acknowledged_turn),
                            );
                        }
                    }
                }
            }
            control = control_rx.recv() => {
                if matches!(
                    handle_control_command(
                        &mut client,
                        &mut mapper,
                        &senders,
                        &mut pending_compactions,
                        &active_turns,
                        control,
                    )
                    .await,
                    StreamOutcome::Shutdown,
                ) {
                    let _ = client.shutdown_transport().await;
                    break;
                }
            }
            _ = first_event_warn_tick.tick(), if !active_turns.is_empty() => {
                warn_slow_first_events(&mut active_turns);
            }
        }
    }
}

#[derive(Debug)]
struct ActiveTurn {
    thread: ThreadHandle,
    acknowledged_turn: TurnId,
    active_turn: Option<TurnId>,
    started_at: Instant,
    saw_server_message: bool,
    warned_no_server_message: bool,
}

impl ActiveTurn {
    fn new(thread: ThreadHandle, acknowledged_turn: TurnId) -> Self {
        Self {
            thread,
            acknowledged_turn,
            active_turn: Some(acknowledged_turn),
            started_at: Instant::now(),
            saw_server_message: false,
            warned_no_server_message: false,
        }
    }

    fn mark_server_message(&mut self) {
        self.saw_server_message = true;
    }

    fn event_is_current_turn(&self, event: &AgentEvent) -> bool {
        agent_event_turn(event).is_none_or(|turn| turn == self.acknowledged_turn)
    }
}

type ActiveTurns = HashMap<ThreadId, ActiveTurn>;

fn should_poll_codex_messages(
    mapper: &CodexMapper,
    active_turns: &ActiveTurns,
    pending_compactions: &HashMap<ThreadId, PendingCompaction>,
) -> bool {
    !active_turns.is_empty() || mapper.has_running_commands() || !pending_compactions.is_empty()
}

fn fallback_thread(mapper: &CodexMapper, active_turns: &ActiveTurns) -> ThreadId {
    mapper
        .running_command_fallback_thread()
        .or_else(|| {
            (active_turns.len() == 1)
                .then(|| active_turns.keys().next().copied())
                .flatten()
        })
        .unwrap_or_default()
}

fn warn_slow_first_events(active_turns: &mut ActiveTurns) {
    for active in active_turns.values_mut() {
        if !active.saw_server_message
            && !active.warned_no_server_message
            && active.started_at.elapsed() >= TURN_FIRST_EVENT_WARN_AFTER
        {
            active.warned_no_server_message = true;
            warn!(
                thread_id = %active.thread.thread,
                harness_thread_id = %active.thread.harness_thread_id,
                acknowledged_turn = %active.acknowledged_turn,
                active_turn = ?active.active_turn,
                elapsed_ms = active.started_at.elapsed().as_millis(),
                "Codex accepted turn/start but has not emitted a server message yet"
            );
        }
    }
}

fn completed_current_active_turn(
    active_turns: &ActiveTurns,
    event: &AgentEvent,
) -> Option<(ThreadId, TurnId)> {
    let AgentEvent::TurnCompleted { thread, turn, .. } = event else {
        return None;
    };
    let active = active_turns.get(thread)?;
    (*turn == active.acknowledged_turn).then_some((*thread, *turn))
}

async fn handle_background_server_message(
    client: &mut dyn CodexTransport,
    mapper: &mut CodexMapper,
    senders: &SenderMap,
    pending_compactions: &mut HashMap<ThreadId, PendingCompaction>,
    active_turns: &mut ActiveTurns,
    msg: codex_codes::ServerMessage,
) -> StreamOutcome {
    let fallback_thread = fallback_thread(mapper, active_turns);
    match msg {
        codex_codes::ServerMessage::Notification(notif) => {
            if let Some(event) = mapper.map_notification(&notif, fallback_thread) {
                let thread = event_thread(&event);
                if let Some(active) = active_turns.get_mut(&thread) {
                    active.mark_server_message();
                    if let AgentEvent::TurnStarted { turn, .. } = &event
                        && *turn == active.acknowledged_turn
                    {
                        active.active_turn = Some(*turn);
                    }
                }
                let completed_compaction =
                    observe_pending_compaction(pending_compactions, thread, &event);
                let completed_active_turn =
                    completed_current_active_turn(active_turns, &event).map(|(_, turn)| turn);
                if active_turns.contains_key(&thread)
                    && matches!(&event, AgentEvent::TurnCompleted { .. })
                    && completed_active_turn.is_none()
                {
                    debug!(
                        %thread,
                        acknowledged_turn = ?active_turns.get(&thread).map(|active| active.acknowledged_turn),
                        event_turn = ?agent_event_turn(&event),
                        "ignoring Codex turn completion for a non-current turn"
                    );
                }
                let fatal_completion = active_turns.get(&thread).and_then(|active| {
                    active
                        .event_is_current_turn(&event)
                        .then(|| {
                            mapping::fatal_turn_error(&notif)
                                .map(|message| (active.active_turn, message))
                        })
                        .flatten()
                });
                let _ = broadcast_event(senders, thread, || event).await;
                if let Some(turn) = completed_active_turn {
                    active_turns.remove(&thread);
                    mapper.clear_active_turn(thread);
                    debug!(
                        %thread,
                        %turn,
                        remaining_active_turns = active_turns.len(),
                        "Codex turn completion observed"
                    );
                } else if let Some((turn, message)) = fatal_completion {
                    if emit_fatal_turn_completion(senders, thread, turn, message).await {
                        active_turns.remove(&thread);
                        mapper.clear_active_turn(thread);
                    }
                }
                if let Some(elapsed_ms) = completed_compaction {
                    return StreamOutcome::CompactionCompleted { thread, elapsed_ms };
                }
            } else if let Some(message) = mapping::fatal_turn_error(&notif) {
                warn!(
                    fallback_thread = %fallback_thread,
                    "dropping fatal Codex error notification that could not be mapped to a known thread: {message}"
                );
            }
            StreamOutcome::TurnEnded
        }
        codex_codes::ServerMessage::Request { id, request } => {
            let Some(event) = mapper.map_server_request(&id, &request, fallback_thread) else {
                respond_unroutable_server_request(client, &id).await;
                return StreamOutcome::TurnEnded;
            };
            let thread = event_thread(&event);
            if let Some(active) = active_turns.get_mut(&thread) {
                active.mark_server_message();
            }
            let _ = broadcast_event(senders, thread, || event).await;
            StreamOutcome::TurnEnded
        }
    }
}

async fn handle_control_command(
    client: &mut dyn CodexTransport,
    mapper: &mut CodexMapper,
    senders: &SenderMap,
    pending_compactions: &mut HashMap<ThreadId, PendingCompaction>,
    active_turns: &ActiveTurns,
    control: Option<ControlCommand>,
) -> StreamOutcome {
    match control {
        Some(ControlCommand::RespondApproval {
            id,
            decision,
            response,
        }) => {
            let result = handle_respond_approval(client, mapper, &id, &decision).await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::RespondServerRequest {
            id,
            response_payload,
            response,
        }) => {
            let result =
                handle_respond_server_request(client, mapper, senders, &id, response_payload).await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::Interrupt { thread, response }) => {
            let result = handle_interrupt(client, mapper, &thread).await;
            if result.is_ok() {
                reject_pending_requests_for_interrupted_thread(
                    client,
                    mapper,
                    senders,
                    thread.thread,
                )
                .await;
            }
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::TerminateCommand {
            thread,
            process_id,
            response,
        }) => {
            let result = handle_terminate_command(client, mapper, &thread, &process_id).await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::CompactThread { thread, response }) => {
            if active_turns.contains_key(&thread.thread) {
                let _ = response.send(Err(HarnessError::Unsupported(
                    "context compaction is not available during an active turn".into(),
                )));
                return StreamOutcome::TurnEnded;
            }
            let started = Instant::now();
            info!(
                thread = %thread.thread,
                harness_thread_id = %thread.harness_thread_id,
                pending_compactions = pending_compactions.len(),
                "requesting Codex context compaction"
            );
            let result = handle_compact_thread(client, &thread).await;
            match &result {
                Ok(()) => {
                    pending_compactions.insert(thread.thread, PendingCompaction::new(started));
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
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::SetThreadName {
            thread,
            name,
            response,
        }) => {
            let result = handle_set_thread_name(client, &thread, &name).await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::SetThreadArchived {
            thread,
            archived,
            response,
        }) => {
            let result = if active_turns.contains_key(&thread.thread) {
                Err(HarnessError::Unsupported(
                    "thread archiving is not available during an active turn".into(),
                ))
            } else {
                handle_set_thread_archived(client, &thread, archived).await
            };
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::DeleteThread { thread, response }) => {
            let result = if active_turns.contains_key(&thread.thread) {
                Err(HarnessError::Unsupported(
                    "thread deletion is not available during an active turn".into(),
                ))
            } else {
                handle_delete_thread(client, &thread).await
            };
            if result.is_ok() {
                lock_senders(senders).remove(&thread.thread);
            }
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::ListMcpServers { response }) => {
            let result = handle_list_mcp_servers(client).await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::ReloadMcpServers { response }) => {
            let result = handle_reload_mcp_servers(client).await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::StartMcpOauthLogin { name, response }) => {
            let result = handle_start_mcp_oauth_login(client, &name).await;
            let _ = response.send(result);
            StreamOutcome::TurnEnded
        }
        Some(ControlCommand::Shutdown { response }) => {
            let _ = response.send(Ok(()));
            StreamOutcome::Shutdown
        }
        None => StreamOutcome::Shutdown,
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
    client: &mut dyn CodexTransport,
    mapper: &mut CodexMapper,
    opts: &OpenThreadOptions,
    senders: &SenderMap,
) -> Result<ThreadHandle, HarnessError> {
    let cwd = opts.workspace_root.to_string_lossy().to_string();

    // Track whether resume-by-id failed and we fell back to a fresh native thread (C5), so we can
    // warn the caller that agent context was lost while keeping the Giskard-side history.
    let mut resume_warning = None;

    let (harness_thread_id, resumed_model) = if let Some(ref resume_id) = opts.resume {
        match resume_thread(client, resume_id, &cwd, &opts.initial_model).await {
            Ok(opened) => opened,
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
        resumed_model,
    })
}

/// The model/provider a `thread/start` / `thread/resume` response reports as effective. Codex can
/// intentionally ignore resume overrides for an already-loaded thread while still answering
/// success, so callers switching providers must compare this against what they requested (see
/// `specs/model-provider-switching-analysis.md`). Empty response fields (older servers) yield
/// `None`; the reasoning effort is not part of the response and is carried from the request.
fn effective_model(
    model: &str,
    model_provider: &str,
    requested: &giskard_core::model::ModelRef,
) -> Option<giskard_core::model::ModelRef> {
    if model.is_empty() || model_provider.is_empty() {
        return None;
    }
    Some(giskard_core::model::ModelRef {
        provider: model_provider.to_string(),
        model: model.to_string(),
        reasoning_effort: requested.reasoning_effort,
    })
}

async fn resume_thread(
    client: &mut dyn CodexTransport,
    resume_id: &str,
    cwd: &str,
    model: &giskard_core::model::ModelRef,
) -> Result<(String, Option<giskard_core::model::ModelRef>), HarnessError> {
    let params: codex_codes::ThreadResumeParams = serde_json::from_value(serde_json::json!({
        "threadId": resume_id,
        "cwd": cwd,
        "model": model.model,
        "modelProvider": model.provider,
    }))
    .map_err(|e| HarnessError::Protocol(e.to_string()))?;
    let resp: codex_codes::ThreadResumeResponse = codex_request(
        client,
        codex_codes::protocol::methods::THREAD_RESUME,
        &params,
    )
    .await?;
    let resumed = effective_model(&resp.model, &resp.model_provider, model);
    Ok((resp.thread.id, resumed))
}

async fn start_thread(
    client: &mut dyn CodexTransport,
    cwd: &str,
    initial_model: &giskard_core::model::ModelRef,
) -> Result<(String, Option<giskard_core::model::ModelRef>), HarnessError> {
    let params: codex_codes::ThreadStartParams = serde_json::from_value(serde_json::json!({
        "cwd": cwd,
        "model": initial_model.model,
        "modelProvider": initial_model.provider,
    }))
    .map_err(|e| HarnessError::Protocol(e.to_string()))?;
    let resp: codex_codes::ThreadStartResponse = codex_request(
        client,
        codex_codes::protocol::methods::THREAD_START,
        &params,
    )
    .await?;
    let started = effective_model(&resp.model, &resp.model_provider, initial_model);
    Ok((resp.thread.id, started))
}

async fn handle_start_turn(
    client: &mut dyn CodexTransport,
    mapper: &mut CodexMapper,
    thread: &ThreadHandle,
    input: &UserInput,
    overrides: &TurnOverrides,
) -> Result<TurnId, HarnessError> {
    let params = build_turn_start_params(thread, input, overrides)?;
    let resp: codex_codes::TurnStartResponse =
        codex_request(client, codex_codes::protocol::methods::TURN_START, &params).await?;

    mapper
        .register_active_turn(thread.thread, &resp.turn.id)
        .ok_or_else(|| {
            HarnessError::Protocol("turn/start response did not include a turn id".into())
        })
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
    client: &mut dyn CodexTransport,
    id: &codex_codes::jsonrpc::RequestId,
) {
    let message = "Giskard cannot route this Codex server request to a known thread.";
    if let Err(error) = client.respond_error_json(id.clone(), -32000, message).await {
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

#[cfg(test)]
fn event_belongs_to_stream(stream_thread: ThreadId, event: &AgentEvent) -> bool {
    event_thread(event) == stream_thread
}

#[cfg(test)]
fn event_belongs_to_current_turn(
    stream_thread: ThreadId,
    current_turn: TurnId,
    event: &AgentEvent,
) -> bool {
    event_belongs_to_stream(stream_thread, event)
        && agent_event_turn(event).is_none_or(|turn| turn == current_turn)
}

#[cfg(test)]
fn event_completes_stream(
    stream_thread: ThreadId,
    current_turn: TurnId,
    event: &AgentEvent,
) -> bool {
    event_belongs_to_stream(stream_thread, event)
        && matches!(event, AgentEvent::TurnCompleted { turn, .. } if *turn == current_turn)
}

async fn emit_incomplete_turn(
    senders: &SenderMap,
    thread: ThreadId,
    turn: Option<TurnId>,
    message: impl Into<String>,
) {
    let message = message.into();
    if let Some(turn) = turn {
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
    } else {
        let _ = broadcast_event(senders, thread, || AgentEvent::Error {
            thread,
            turn: None,
            error: HarnessError::Transport(message),
        })
        .await;
    }
}

async fn emit_incomplete_active_turns(
    senders: &SenderMap,
    mapper: &mut CodexMapper,
    active_turns: &mut ActiveTurns,
    message: impl Into<String>,
) {
    let message = message.into();
    let turns: Vec<(ThreadId, Option<TurnId>)> = active_turns
        .iter()
        .map(|(thread, active)| (*thread, active.active_turn))
        .collect();
    for (thread, turn) in turns {
        emit_incomplete_turn(senders, thread, turn, message.clone()).await;
        mapper.clear_active_turn(thread);
    }
    active_turns.clear();
}

async fn emit_fatal_turn_completion(
    senders: &SenderMap,
    thread: ThreadId,
    turn: Option<TurnId>,
    message: impl Into<String>,
) -> bool {
    let message = message.into();
    let Some(turn) = turn else {
        warn!(
            %thread,
            error = %message,
            "fatal Codex error notification arrived without an active turn; not synthesizing turn completion"
        );
        return false;
    };

    warn!(
        %thread,
        %turn,
        error = %message,
        "synthesizing failed turn completion from fatal Codex error notification"
    );
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
    true
}

async fn handle_respond_approval(
    client: &mut dyn CodexTransport,
    mapper: &mut CodexMapper,
    id: &ApprovalId,
    decision: &ApprovalDecision,
) -> Result<(), HarnessError> {
    match mapper
        .map_approval_response(id, decision)
        .map_err(HarnessError::Protocol)?
    {
        mapping::ApprovalResponse::Result { request_id, value } => client
            .respond_json(request_id, value)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string())),
        mapping::ApprovalResponse::Error {
            request_id,
            code,
            message,
        } => client
            .respond_error_json(request_id, code, &message)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string())),
    }
}

async fn handle_respond_server_request(
    client: &mut dyn CodexTransport,
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
            .respond_json(pending.request_id, value)
            .await
            .map_err(|e| HarnessError::Transport(e.to_string()))?,
        ServerRequestResponse::Error { code, message } => client
            .respond_error_json(pending.request_id, code, &message)
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

async fn reject_pending_requests_for_interrupted_thread(
    client: &mut dyn CodexTransport,
    mapper: &mut CodexMapper,
    senders: &SenderMap,
    thread: ThreadId,
) {
    let approval_ids = mapper.pending_approval_ids_for_thread(thread);
    let server_request_ids = mapper.pending_server_request_ids_for_thread(thread);

    if approval_ids.is_empty() && server_request_ids.is_empty() {
        debug!(
            %thread,
            "interrupt accepted with no pending Codex approval/server request to reject"
        );
        return;
    }

    for approval_id in approval_ids {
        if let Err(error) =
            handle_respond_approval(client, mapper, &approval_id, &ApprovalDecision::Cancel).await
        {
            warn!(
                %thread,
                request_id = %approval_id,
                %error,
                "failed to cancel pending approval after interrupt"
            );
        }
    }

    for server_request_id in server_request_ids {
        let response = ServerRequestResponse::Error {
            code: -32000,
            message: "Turn interrupted before this server request was answered.".into(),
        };
        if let Err(error) =
            handle_respond_server_request(client, mapper, senders, &server_request_id, response)
                .await
        {
            warn!(
                %thread,
                request_id = %server_request_id,
                %error,
                "failed to reject pending server request after interrupt"
            );
        }
    }
}

async fn handle_interrupt(
    client: &mut dyn CodexTransport,
    mapper: &CodexMapper,
    thread: &ThreadHandle,
) -> Result<(), HarnessError> {
    let native_turn_id = mapper
        .active_native_turn_for_thread(thread.thread)
        .ok_or_else(|| HarnessError::Unsupported("no active Codex turn to interrupt".into()))?;
    handle_interrupt_turn(client, &thread.harness_thread_id, native_turn_id).await
}

async fn handle_terminate_command(
    client: &mut dyn CodexTransport,
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
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
) -> Result<(), HarnessError> {
    let params = codex_codes::ThreadCompactStartParams {
        thread_id: thread.harness_thread_id.clone(),
    };
    let _: codex_codes::ThreadCompactStartResponse = codex_request(
        client,
        codex_codes::protocol::methods::THREAD_COMPACT_START,
        &params,
    )
    .await?;
    Ok(())
}

async fn handle_set_thread_archived(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
    archived: bool,
) -> Result<(), HarnessError> {
    if archived {
        let params = codex_codes::ThreadArchiveParams {
            thread_id: thread.harness_thread_id.clone(),
        };
        let _: codex_codes::ThreadArchiveResponse = codex_request(
            client,
            codex_codes::protocol::methods::THREAD_ARCHIVE,
            &params,
        )
        .await?;
    } else {
        let params = codex_codes::ThreadUnarchiveParams {
            thread_id: thread.harness_thread_id.clone(),
        };
        let _: codex_codes::ThreadUnarchiveResponse = codex_request(
            client,
            codex_codes::protocol::methods::THREAD_UNARCHIVE,
            &params,
        )
        .await?;
    }
    Ok(())
}

async fn handle_set_thread_name(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
    name: &str,
) -> Result<(), HarnessError> {
    let params = codex_codes::ThreadSetNameParams {
        thread_id: thread.harness_thread_id.clone(),
        name: name.to_owned(),
    };
    let _: codex_codes::ThreadSetNameResponse = codex_request(
        client,
        codex_codes::protocol::methods::THREAD_NAME_SET,
        &params,
    )
    .await?;
    Ok(())
}

async fn handle_list_mcp_servers(
    client: &mut dyn CodexTransport,
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
        let page: codex_codes::ListMcpServerStatusResponse = codex_request(
            client,
            codex_codes::protocol::methods::MCPSERVERSTATUS_LIST,
            &params,
        )
        .await?;

        out.extend(page.data.into_iter().map(map_mcp_server_status));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }

    Ok(out)
}

async fn handle_reload_mcp_servers(client: &mut dyn CodexTransport) -> Result<(), HarnessError> {
    let _: codex_codes::McpServerRefreshResponse = codex_request(
        client,
        codex_codes::protocol::methods::CONFIG_MCPSERVER_RELOAD,
        &serde_json::json!({}),
    )
    .await?;
    Ok(())
}

async fn handle_start_mcp_oauth_login(
    client: &mut dyn CodexTransport,
    name: &str,
) -> Result<McpOauthStart, HarnessError> {
    let params = codex_codes::McpServerOauthLoginParams {
        name: name.to_owned(),
        scopes: None,
        thread_id: None,
        timeout_secs: None,
    };
    let response: codex_codes::McpServerOauthLoginResponse = codex_request(
        client,
        codex_codes::protocol::methods::MCPSERVER_OAUTH_LOGIN,
        &params,
    )
    .await?;
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
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
) -> Result<(), HarnessError> {
    let params = codex_codes::ThreadDeleteParams {
        thread_id: thread.harness_thread_id.clone(),
    };
    let _: codex_codes::ThreadDeleteResponse = codex_request(
        client,
        codex_codes::protocol::methods::THREAD_DELETE,
        &params,
    )
    .await?;
    Ok(())
}

async fn handle_interrupt_turn(
    client: &mut dyn CodexTransport,
    native_thread_id: &str,
    native_turn_id: &str,
) -> Result<(), HarnessError> {
    let params = codex_codes::TurnInterruptParams {
        thread_id: native_thread_id.to_owned(),
        turn_id: native_turn_id.to_owned(),
    };
    let _: codex_codes::TurnInterruptResponse = codex_request(
        client,
        codex_codes::protocol::methods::TURN_INTERRUPT,
        &params,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use giskard_core::ids::{ItemId, ProjectId};
    use giskard_core::item::{Item, ItemPayload};
    use giskard_core::model::{Effort, ModelRef};
    use giskard_core::turn::{ApprovalPolicy, Mode};
    use serde_json::{Value, json};
    use tokio::sync::Mutex;
    use tokio::time::timeout;

    fn test_thread() -> ThreadHandle {
        ThreadHandle {
            thread: ThreadId::new(),
            harness_thread_id: "native-thread".into(),
            warning: None,
            resumed_model: None,
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

    #[derive(Debug, Clone, PartialEq)]
    struct FakeRequest {
        method: String,
        params: Value,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct FakeResponse {
        id: codex_codes::jsonrpc::RequestId,
        value: Value,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct FakeResponseError {
        id: codex_codes::jsonrpc::RequestId,
        code: i64,
        message: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct FakeStartedTurn {
        native_thread_id: String,
        native_turn_id: String,
    }

    #[derive(Debug, Default)]
    struct FakeCodexState {
        thread_counter: usize,
        turn_counter: usize,
        requests: Vec<FakeRequest>,
        responses: Vec<FakeResponse>,
        response_errors: Vec<FakeResponseError>,
        started_turns: Vec<FakeStartedTurn>,
        shutdowns: usize,
    }

    struct FakeCodexTransport {
        state: Arc<Mutex<FakeCodexState>>,
        events_rx: mpsc::Receiver<codex_codes::ServerMessage>,
    }

    #[derive(Clone)]
    struct FakeCodexController {
        state: Arc<Mutex<FakeCodexState>>,
        events_tx: mpsc::Sender<codex_codes::ServerMessage>,
    }

    impl FakeCodexController {
        async fn send_server_message(&self, msg: codex_codes::ServerMessage) {
            self.events_tx
                .send(msg)
                .await
                .expect("fake Codex event receiver should be open");
        }

        async fn requests(&self) -> Vec<FakeRequest> {
            self.state.lock().await.requests.clone()
        }

        async fn responses(&self) -> Vec<FakeResponse> {
            self.state.lock().await.responses.clone()
        }

        async fn response_errors(&self) -> Vec<FakeResponseError> {
            self.state.lock().await.response_errors.clone()
        }

        async fn started_turns(&self) -> Vec<FakeStartedTurn> {
            self.state.lock().await.started_turns.clone()
        }
    }

    fn fake_codex() -> (FakeCodexTransport, FakeCodexController) {
        let (events_tx, events_rx) = mpsc::channel(32);
        let state = Arc::new(Mutex::new(FakeCodexState::default()));
        (
            FakeCodexTransport {
                state: state.clone(),
                events_rx,
            },
            FakeCodexController { state, events_tx },
        )
    }

    #[async_trait]
    impl CodexTransport for FakeCodexTransport {
        async fn request_json(
            &mut self,
            method: &str,
            params: Value,
        ) -> Result<Value, HarnessError> {
            let mut state = self.state.lock().await;
            state.requests.push(FakeRequest {
                method: method.to_owned(),
                params: params.clone(),
            });

            match method {
                codex_codes::protocol::methods::THREAD_START => {
                    state.thread_counter += 1;
                    let native_thread_id = format!("native-thread-{}", state.thread_counter);
                    Ok(thread_open_response(
                        &native_thread_id,
                        params["model"].as_str().unwrap_or("gpt-5.5"),
                        params["modelProvider"].as_str().unwrap_or("openai"),
                    ))
                }
                codex_codes::protocol::methods::THREAD_RESUME => {
                    let native_thread_id = params["threadId"]
                        .as_str()
                        .filter(|id| !id.is_empty())
                        .unwrap_or("native-resumed");
                    Ok(thread_open_response(
                        native_thread_id,
                        params["model"].as_str().unwrap_or("gpt-5.5"),
                        params["modelProvider"].as_str().unwrap_or("openai"),
                    ))
                }
                codex_codes::protocol::methods::TURN_START => {
                    state.turn_counter += 1;
                    let native_thread_id =
                        params["threadId"].as_str().unwrap_or_default().to_owned();
                    let native_turn_id = format!("native-turn-{}", state.turn_counter);
                    state.started_turns.push(FakeStartedTurn {
                        native_thread_id,
                        native_turn_id: native_turn_id.clone(),
                    });
                    Ok(json!({
                        "turn": {
                            "id": native_turn_id,
                            "status": "inProgress"
                        }
                    }))
                }
                codex_codes::protocol::methods::THREAD_COMPACT_START
                | codex_codes::protocol::methods::THREAD_ARCHIVE
                | codex_codes::protocol::methods::THREAD_UNARCHIVE
                | codex_codes::protocol::methods::THREAD_NAME_SET
                | codex_codes::protocol::methods::CONFIG_MCPSERVER_RELOAD
                | codex_codes::protocol::methods::THREAD_DELETE
                | codex_codes::protocol::methods::TURN_INTERRUPT => Ok(json!({})),
                codex_codes::protocol::methods::MCPSERVERSTATUS_LIST => Ok(json!({
                    "data": [],
                    "nextCursor": null
                })),
                codex_codes::protocol::methods::MCPSERVER_OAUTH_LOGIN => Ok(json!({
                    "authorizationUrl": "https://example.invalid/oauth"
                })),
                other => Err(HarnessError::Unsupported(format!(
                    "fake Codex transport has no response for {other}"
                ))),
            }
        }

        async fn next_message(
            &mut self,
        ) -> Result<Option<codex_codes::ServerMessage>, HarnessError> {
            Ok(self.events_rx.recv().await)
        }

        async fn respond_json(
            &mut self,
            id: codex_codes::jsonrpc::RequestId,
            value: Value,
        ) -> Result<(), HarnessError> {
            self.state
                .lock()
                .await
                .responses
                .push(FakeResponse { id, value });
            Ok(())
        }

        async fn respond_error_json(
            &mut self,
            id: codex_codes::jsonrpc::RequestId,
            code: i64,
            message: &str,
        ) -> Result<(), HarnessError> {
            self.state
                .lock()
                .await
                .response_errors
                .push(FakeResponseError {
                    id,
                    code,
                    message: message.to_owned(),
                });
            Ok(())
        }

        async fn shutdown_transport(self) -> Result<(), HarnessError> {
            self.state.lock().await.shutdowns += 1;
            Ok(())
        }
    }

    fn thread_open_response(native_thread_id: &str, model: &str, provider: &str) -> Value {
        json!({
            "approvalPolicy": "never",
            "approvalsReviewer": null,
            "cwd": "/tmp",
            "model": model,
            "modelProvider": provider,
            "sandbox": {},
            "thread": {
                "id": native_thread_id
            }
        })
    }

    fn open_opts(thread: Option<ThreadId>, resume: Option<&str>) -> OpenThreadOptions {
        OpenThreadOptions {
            project: ProjectId::new(),
            thread,
            workspace_root: PathBuf::from("/tmp"),
            resume: resume.map(str::to_owned),
            initial_model: test_model(None),
        }
    }

    fn build_turn_overrides() -> TurnOverrides {
        turn_overrides(Mode::Build, None)
    }

    fn spawn_fake_harness() -> (Arc<CodexHarness>, FakeCodexController) {
        let (transport, controller) = fake_codex();
        let harness = CodexHarness::spawn_harness(transport, PathBuf::from("/tmp"))
            .expect("fake harness should spawn");
        (harness, controller)
    }

    fn generic_user_input_request(
        id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> codex_codes::ServerMessage {
        codex_codes::ServerMessage::Request {
            id: codex_codes::jsonrpc::RequestId::String(id.to_owned()),
            request: codex_codes::messages::ServerRequest::ToolRequestUserInput(
                serde_json::from_value(json!({
                    "itemId": format!("input-{id}"),
                    "threadId": native_thread_id,
                    "turnId": native_turn_id,
                    "questions": [{
                        "id": "confirm",
                        "header": "Confirm",
                        "question": "Continue?"
                    }]
                }))
                .expect("test user input request should deserialize"),
            ),
        }
    }

    fn command_approval_request(
        id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> codex_codes::ServerMessage {
        codex_codes::ServerMessage::Request {
            id: codex_codes::jsonrpc::RequestId::String(id.to_owned()),
            request: codex_codes::messages::ServerRequest::CmdExecApproval(
                serde_json::from_value(json!({
                    "approvalId": id,
                    "commandActions": [],
                    "cwd": "/tmp",
                    "environmentId": "env_1",
                    "itemId": format!("cmd-{id}"),
                    "threadId": native_thread_id,
                    "turnId": native_turn_id,
                    "startedAtMs": 123
                }))
                .expect("test approval request should deserialize"),
            ),
        }
    }

    async fn recv_matching_event(
        stream: &mut AgentEventStream,
        label: &str,
        matches: impl Fn(&AgentEvent) -> bool,
    ) -> AgentEvent {
        timeout(Duration::from_secs(1), async {
            loop {
                let event = stream.recv().await.expect("event stream should stay open");
                if matches(&event) {
                    break event;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {label}"))
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
        let current_turn = TurnId::new();
        let event = completed_event(foreign_thread, turn);

        assert!(!event_belongs_to_stream(stream_thread, &event));
        assert!(!event_belongs_to_current_turn(
            stream_thread,
            current_turn,
            &event
        ));
        assert!(!event_completes_stream(stream_thread, current_turn, &event));
        assert!(event_completes_stream(foreign_thread, turn, &event));
    }

    #[test]
    fn same_thread_stale_turn_completion_does_not_end_live_stream() {
        let thread = ThreadId::new();
        let current_turn = TurnId::new();
        let previous_turn = TurnId::new();
        let event = completed_event(thread, previous_turn);

        assert!(event_belongs_to_stream(thread, &event));
        assert!(!event_belongs_to_current_turn(thread, current_turn, &event));
        assert!(!event_completes_stream(thread, current_turn, &event));
        assert!(event_completes_stream(thread, previous_turn, &event));
    }

    #[test]
    fn same_thread_stale_turn_error_is_not_current_turn() {
        let thread = ThreadId::new();
        let current_turn = TurnId::new();
        let previous_turn = TurnId::new();
        let stale_error = AgentEvent::Error {
            thread,
            turn: Some(previous_turn),
            error: HarnessError::Protocol("previous failure".into()),
        };

        assert!(!event_belongs_to_current_turn(
            thread,
            current_turn,
            &stale_error
        ));

        let turnless_error = AgentEvent::Error {
            thread,
            turn: None,
            error: HarnessError::Protocol("unscoped failure".into()),
        };
        assert!(event_belongs_to_current_turn(
            thread,
            current_turn,
            &turnless_error
        ));
    }

    #[test]
    fn active_turn_table_completes_only_matching_thread_and_turn() {
        let first_thread = test_thread();
        let second_thread = ThreadHandle {
            thread: ThreadId::new(),
            harness_thread_id: "native-thread-2".into(),
            warning: None,
            resumed_model: None,
        };
        let first_turn = TurnId::new();
        let second_turn = TurnId::new();
        let stale_turn = TurnId::new();
        let mut active_turns = ActiveTurns::new();
        active_turns.insert(
            first_thread.thread,
            ActiveTurn::new(first_thread.clone(), first_turn),
        );
        active_turns.insert(
            second_thread.thread,
            ActiveTurn::new(second_thread.clone(), second_turn),
        );

        assert_eq!(
            completed_current_active_turn(
                &active_turns,
                &completed_event(second_thread.thread, second_turn)
            ),
            Some((second_thread.thread, second_turn))
        );
        assert_eq!(
            completed_current_active_turn(
                &active_turns,
                &completed_event(first_thread.thread, stale_turn)
            ),
            None
        );
        assert_eq!(
            completed_current_active_turn(
                &active_turns,
                &completed_event(ThreadId::new(), first_turn)
            ),
            None
        );
    }

    #[test]
    fn codex_messages_are_polled_while_any_turn_is_active() {
        let mapper = CodexMapper::new(PathBuf::from("/tmp"));
        let mut active_turns = ActiveTurns::new();
        let thread = test_thread();
        active_turns.insert(thread.thread, ActiveTurn::new(thread, TurnId::new()));

        assert!(should_poll_codex_messages(
            &mapper,
            &active_turns,
            &HashMap::new()
        ));
    }

    #[tokio::test]
    async fn codex_worker_opens_new_thread_while_turn_is_active() {
        let (harness, controller) = spawn_fake_harness();
        let first = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &first,
                UserInput::text("keep running"),
                build_turn_overrides(),
            )
            .await
            .unwrap();

        let second = timeout(
            Duration::from_secs(1),
            harness.open_thread(open_opts(None, None)),
        )
        .await
        .expect("opening another thread must not wait for the active turn")
        .unwrap();

        assert_eq!(second.harness_thread_id, "native-thread-2");
        assert_eq!(
            controller
                .requests()
                .await
                .iter()
                .filter(|req| req.method == codex_codes::protocol::methods::THREAD_START)
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn codex_worker_resumes_thread_while_turn_is_active() {
        let (harness, controller) = spawn_fake_harness();
        let first = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &first,
                UserInput::text("keep running"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        let resumed_thread = ThreadId::new();

        let resumed = timeout(
            Duration::from_secs(1),
            harness.open_thread(open_opts(Some(resumed_thread), Some("native-existing"))),
        )
        .await
        .expect("resuming another thread must not wait for the active turn")
        .unwrap();

        assert_eq!(resumed.thread, resumed_thread);
        assert_eq!(resumed.harness_thread_id, "native-existing");
        assert!(controller.requests().await.iter().any(|req| {
            req.method == codex_codes::protocol::methods::THREAD_RESUME
                && req.params["threadId"] == "native-existing"
        }));
    }

    #[tokio::test]
    async fn codex_worker_starts_other_thread_turn_while_first_turn_is_active() {
        let (harness, controller) = spawn_fake_harness();
        let first = harness.open_thread(open_opts(None, None)).await.unwrap();
        let second = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &first,
                UserInput::text("keep running"),
                build_turn_overrides(),
            )
            .await
            .unwrap();

        let second_turn = timeout(
            Duration::from_secs(1),
            harness.start_turn(
                &second,
                UserInput::text("run concurrently"),
                build_turn_overrides(),
            ),
        )
        .await
        .expect("starting another thread turn must not wait for the first turn")
        .unwrap();

        let started = controller.started_turns().await;
        assert_eq!(started.len(), 2);
        assert_eq!(started[0].native_thread_id, first.harness_thread_id);
        assert_eq!(started[1].native_thread_id, second.harness_thread_id);
        assert_ne!(started[0].native_turn_id, started[1].native_turn_id);
        assert!(second_turn != TurnId::default());
    }

    #[tokio::test]
    async fn codex_worker_pending_server_request_does_not_block_other_thread_start() {
        let (harness, controller) = spawn_fake_harness();
        let first = harness.open_thread(open_opts(None, None)).await.unwrap();
        let first_turn = harness
            .start_turn(&first, UserInput::text("ask later"), build_turn_overrides())
            .await
            .unwrap();
        let first_native_turn = controller.started_turns().await[0].native_turn_id.clone();
        let mut first_stream = harness.subscribe(&first);

        controller
            .send_server_message(generic_user_input_request(
                "server_req",
                &first.harness_thread_id,
                &first_native_turn,
            ))
            .await;
        let event = recv_matching_event(&mut first_stream, "server request", |event| {
            matches!(
                event,
                AgentEvent::ServerRequestReceived {
                    thread,
                    turn,
                    request,
                } if *thread == first.thread
                    && *turn == Some(first_turn)
                    && request.id == ServerRequestId("server_req".into())
            )
        })
        .await;
        assert!(matches!(event, AgentEvent::ServerRequestReceived { .. }));

        let second = harness.open_thread(open_opts(None, None)).await.unwrap();
        timeout(
            Duration::from_secs(1),
            harness.start_turn(
                &second,
                UserInput::text("not blocked"),
                build_turn_overrides(),
            ),
        )
        .await
        .expect("pending server request in one thread must not block another thread")
        .unwrap();
    }

    #[tokio::test]
    async fn codex_worker_routes_server_request_response_while_other_thread_is_active() {
        let (harness, controller) = spawn_fake_harness();
        let first = harness.open_thread(open_opts(None, None)).await.unwrap();
        let second = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &first,
                UserInput::text("ask a question"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        harness
            .start_turn(
                &second,
                UserInput::text("also running"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        let first_native_turn = controller.started_turns().await[0].native_turn_id.clone();
        let mut first_stream = harness.subscribe(&first);
        controller
            .send_server_message(generic_user_input_request(
                "server_req",
                &first.harness_thread_id,
                &first_native_turn,
            ))
            .await;
        recv_matching_event(&mut first_stream, "server request", |event| {
            matches!(
                event,
                AgentEvent::ServerRequestReceived { request, .. }
                    if request.id == ServerRequestId("server_req".into())
            )
        })
        .await;

        timeout(
            Duration::from_secs(1),
            harness.respond_server_request(
                ServerRequestId("server_req".into()),
                ServerRequestResponse::result(json!({"answer": true})),
            ),
        )
        .await
        .expect("server request response must be routed while another thread is active")
        .unwrap();

        let responses = controller.responses().await;
        assert_eq!(responses.len(), 1);
        assert_eq!(
            responses[0].id,
            codex_codes::jsonrpc::RequestId::String("server_req".into())
        );
        assert_eq!(responses[0].value, json!({"answer": true}));
        recv_matching_event(&mut first_stream, "server request resolution", |event| {
            matches!(
                event,
                AgentEvent::ServerRequestResolved { request_id, .. }
                    if *request_id == ServerRequestId("server_req".into())
            )
        })
        .await;
    }

    #[tokio::test]
    async fn codex_worker_routes_approval_response_while_other_thread_is_active() {
        let (harness, controller) = spawn_fake_harness();
        let first = harness.open_thread(open_opts(None, None)).await.unwrap();
        let second = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &first,
                UserInput::text("needs approval"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        harness
            .start_turn(
                &second,
                UserInput::text("also running"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        let first_native_turn = controller.started_turns().await[0].native_turn_id.clone();
        let mut first_stream = harness.subscribe(&first);
        controller
            .send_server_message(command_approval_request(
                "approval_req",
                &first.harness_thread_id,
                &first_native_turn,
            ))
            .await;
        recv_matching_event(&mut first_stream, "approval request", |event| {
            matches!(
                event,
                AgentEvent::ApprovalRequested { request, .. }
                    if request.id == ApprovalId("approval_req".into())
            )
        })
        .await;

        timeout(
            Duration::from_secs(1),
            harness.respond_approval(ApprovalId("approval_req".into()), ApprovalDecision::Accept),
        )
        .await
        .expect("approval response must be routed while another thread is active")
        .unwrap();

        let responses = controller.responses().await;
        assert_eq!(responses.len(), 1);
        assert_eq!(
            responses[0].id,
            codex_codes::jsonrpc::RequestId::String("approval_req".into())
        );
        assert_eq!(responses[0].value, json!({"decision": "accept"}));
    }

    #[tokio::test]
    async fn codex_worker_interrupt_rejects_only_interrupted_thread_requests() {
        let (harness, controller) = spawn_fake_harness();
        let first = harness.open_thread(open_opts(None, None)).await.unwrap();
        let second = harness.open_thread(open_opts(None, None)).await.unwrap();
        harness
            .start_turn(
                &first,
                UserInput::text("waits on input"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        harness
            .start_turn(
                &second,
                UserInput::text("also waits"),
                build_turn_overrides(),
            )
            .await
            .unwrap();
        let started = controller.started_turns().await;
        let first_native_turn = started[0].native_turn_id.clone();
        let second_native_turn = started[1].native_turn_id.clone();
        let mut first_stream = harness.subscribe(&first);
        let mut second_stream = harness.subscribe(&second);

        controller
            .send_server_message(generic_user_input_request(
                "first_server_req",
                &first.harness_thread_id,
                &first_native_turn,
            ))
            .await;
        recv_matching_event(&mut first_stream, "first server request", |event| {
            matches!(
                event,
                AgentEvent::ServerRequestReceived { request, .. }
                    if request.id == ServerRequestId("first_server_req".into())
            )
        })
        .await;
        controller
            .send_server_message(command_approval_request(
                "first_approval_req",
                &first.harness_thread_id,
                &first_native_turn,
            ))
            .await;
        recv_matching_event(&mut first_stream, "first approval request", |event| {
            matches!(
                event,
                AgentEvent::ApprovalRequested { request, .. }
                    if request.id == ApprovalId("first_approval_req".into())
            )
        })
        .await;
        controller
            .send_server_message(generic_user_input_request(
                "second_server_req",
                &second.harness_thread_id,
                &second_native_turn,
            ))
            .await;
        recv_matching_event(&mut second_stream, "second server request", |event| {
            matches!(
                event,
                AgentEvent::ServerRequestReceived { request, .. }
                    if request.id == ServerRequestId("second_server_req".into())
            )
        })
        .await;

        timeout(Duration::from_secs(1), harness.interrupt(&first))
            .await
            .expect("interrupt must be processed while another thread is active")
            .unwrap();

        let requests = controller.requests().await;
        assert!(requests.iter().any(|req| {
            req.method == codex_codes::protocol::methods::TURN_INTERRUPT
                && req.params["threadId"] == first.harness_thread_id
                && req.params["turnId"] == first_native_turn
        }));
        let responses = controller.responses().await;
        assert!(responses.iter().any(|response| {
            response.id == codex_codes::jsonrpc::RequestId::String("first_approval_req".into())
                && response.value == json!({"decision": "cancel"})
        }));
        let response_errors = controller.response_errors().await;
        assert!(response_errors.iter().any(|error| {
            error.id == codex_codes::jsonrpc::RequestId::String("first_server_req".into())
        }));
        assert!(!response_errors.iter().any(|error| {
            error.id == codex_codes::jsonrpc::RequestId::String("second_server_req".into())
        }));

        timeout(
            Duration::from_secs(1),
            harness.respond_server_request(
                ServerRequestId("second_server_req".into()),
                ServerRequestResponse::result(json!({"still": "routable"})),
            ),
        )
        .await
        .expect("interrupting one thread must not discard another thread request")
        .unwrap();
        let responses = controller.responses().await;
        assert!(responses.iter().any(|response| {
            response.id == codex_codes::jsonrpc::RequestId::String("second_server_req".into())
                && response.value == json!({"still": "routable"})
        }));
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

    #[tokio::test]
    async fn incomplete_stream_without_turn_emits_error_event() {
        let thread = ThreadId::new();
        let senders: SenderMap = Arc::new(StdMutex::new(HashMap::new()));
        let (tx, mut rx) = broadcast::channel(BROADCAST_CAPACITY);
        ensure_thread_sender(&senders, thread, tx);

        emit_incomplete_turn(&senders, thread, None, "stream ended").await;

        let event = rx.recv().await.unwrap();
        match event {
            AgentEvent::Error {
                thread: got_thread,
                turn: None,
                error: HarnessError::Transport(message),
            } => {
                assert_eq!(got_thread, thread);
                assert_eq!(message, "stream ended");
            }
            other => panic!("expected error event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn incomplete_stream_with_turn_emits_failed_completion() {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let senders: SenderMap = Arc::new(StdMutex::new(HashMap::new()));
        let (tx, mut rx) = broadcast::channel(BROADCAST_CAPACITY);
        ensure_thread_sender(&senders, thread, tx);

        emit_incomplete_turn(&senders, thread, Some(turn), "stream failed").await;

        let event = rx.recv().await.unwrap();
        match event {
            AgentEvent::TurnCompleted {
                thread: got_thread,
                turn: got_turn,
                status,
                ..
            } => {
                assert_eq!(got_thread, thread);
                assert_eq!(got_turn, turn);
                assert_eq!(status.kind, TurnStatusKind::Failed);
                assert_eq!(status.message.as_deref(), Some("stream failed"));
            }
            other => panic!("expected failed turn completion, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fatal_error_with_turn_emits_failed_completion() {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let senders: SenderMap = Arc::new(StdMutex::new(HashMap::new()));
        let (tx, mut rx) = broadcast::channel(BROADCAST_CAPACITY);
        ensure_thread_sender(&senders, thread, tx);

        assert!(emit_fatal_turn_completion(&senders, thread, Some(turn), "quota exceeded").await);

        let event = rx.recv().await.unwrap();
        match event {
            AgentEvent::TurnCompleted {
                thread: got_thread,
                turn: got_turn,
                status,
                ..
            } => {
                assert_eq!(got_thread, thread);
                assert_eq!(got_turn, turn);
                assert_eq!(status.kind, TurnStatusKind::Failed);
                assert_eq!(status.message.as_deref(), Some("quota exceeded"));
            }
            other => panic!("expected failed turn completion, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fatal_error_without_turn_does_not_synthesize_completion() {
        let thread = ThreadId::new();
        let senders: SenderMap = Arc::new(StdMutex::new(HashMap::new()));
        let (tx, mut rx) = broadcast::channel(BROADCAST_CAPACITY);
        ensure_thread_sender(&senders, thread, tx);

        assert!(!emit_fatal_turn_completion(&senders, thread, None, "quota exceeded").await);

        assert!(matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
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
