use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard};
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ProjectId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemPayload, command_status_is_running, normalized_command_status};
use giskard_core::mcp::{McpOauthStart, McpServerStatus};
use giskard_core::model::ModelRef;
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::turn::{Mode, Turn, TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{AgentHarness, HarnessCapabilities, OpenThreadOptions, ThreadHandle};
use giskard_persist::PersistStore;
use giskard_persist::store::ProjectConfig;
use giskard_proto::{RunningTask, ServerMessage, TokenScope};

use crate::hub::Hub;
use crate::ledger::LedgerHandle;
use crate::live_buffer::LiveBufferStore;
use crate::models::context_window_for;
use crate::running_commands::RunningTaskStore;

#[async_trait]
pub trait HarnessFactory: Send + Sync {
    async fn create(&self, config: &ProjectConfig) -> Result<Arc<dyn AgentHarness>, HarnessError>;
}

/// Context describing the turn being started, used to persist a `Turn` on completion (§7.1).
#[derive(Clone)]
struct TurnContext {
    user_input: UserInput,
    model: ModelRef,
    mode: Mode,
    kind: TurnContextKind,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TurnContextKind {
    User,
    ManualCompaction,
}

fn turn_context_kind_label(kind: TurnContextKind) -> &'static str {
    match kind {
        TurnContextKind::User => "user",
        TurnContextKind::ManualCompaction => "manual_compaction",
    }
}

/// Shared handle to the pending-approvals map (`ApprovalId -> ThreadId`), cloneable into the
/// spawned event forwarder so it can register approvals as they stream in.
type ApprovalMap = Arc<Mutex<HashMap<ApprovalId, ThreadId>>>;
type ServerRequestMap = Arc<Mutex<HashMap<ServerRequestId, ThreadId>>>;

#[derive(Clone)]
struct ThreadBinding {
    project: ProjectId,
    handle: ThreadHandle,
    native_model: ModelRef,
}

#[derive(Clone, Default)]
struct ThreadTurnGate {
    active: Arc<StdMutex<HashSet<ThreadId>>>,
}

impl ThreadTurnGate {
    fn reserve(&self, thread_id: ThreadId) -> Result<ThreadTurnLease, HarnessError> {
        let mut active = self.active_threads();
        if !active.insert(thread_id) {
            return Err(HarnessError::ThreadBusy { thread: thread_id });
        }
        debug!(%thread_id, "reserved active thread turn");
        Ok(ThreadTurnLease {
            gate: self.clone(),
            thread_id,
            released: false,
        })
    }

    fn active_threads(&self) -> StdMutexGuard<'_, HashSet<ThreadId>> {
        match self.active.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("thread turn gate lock was poisoned; recovering active-turn state");
                poisoned.into_inner()
            }
        }
    }

    fn release(&self, thread_id: ThreadId) {
        let mut active = self.active_threads();
        if active.remove(&thread_id) {
            debug!(%thread_id, "released active thread turn");
        }
    }

    fn is_active(&self, thread_id: ThreadId) -> bool {
        self.active_threads().contains(&thread_id)
    }
}

struct ThreadTurnLease {
    gate: ThreadTurnGate,
    thread_id: ThreadId,
    released: bool,
}

impl ThreadTurnLease {
    fn release(&mut self) {
        if self.released {
            return;
        }
        self.gate.release(self.thread_id);
        self.released = true;
    }

    fn is_released(&self) -> bool {
        self.released
    }
}

impl Drop for ThreadTurnLease {
    fn drop(&mut self) {
        self.release();
    }
}

pub struct HarnessRegistry {
    harnesses: Mutex<HashMap<ProjectId, Arc<dyn AgentHarness>>>,
    threads: Mutex<HashMap<ThreadId, ThreadBinding>>,
    /// Per-thread turn gate covering both start-in-progress and live turns. `LiveBufferStore` only
    /// becomes active after `TurnStarted`, so it cannot protect the `start_turn` race itself.
    turn_gate: ThreadTurnGate,
    /// Which thread a pending approval belongs to, so `ApprovalDecision { request_id }` (which
    /// carries no thread id, §13.6) can be routed to the right harness (§9.2).
    approvals: ApprovalMap,
    /// Which thread a pending non-approval server request belongs to. Browser responses carry only
    /// the opaque request id, so this mirrors the approval routing map for Codex server requests.
    server_requests: ServerRequestMap,
    factory: Arc<dyn HarnessFactory>,
    hub: Arc<Hub>,
    live_buffers: Arc<LiveBufferStore>,
    running_commands: Arc<RunningTaskStore>,
    store: Arc<PersistStore>,
    ledger: LedgerHandle,
}

impl HarnessRegistry {
    pub fn new(
        factory: Arc<dyn HarnessFactory>,
        hub: Arc<Hub>,
        live_buffers: Arc<LiveBufferStore>,
        running_commands: Arc<RunningTaskStore>,
        store: Arc<PersistStore>,
        ledger: LedgerHandle,
    ) -> Self {
        Self {
            harnesses: Mutex::new(HashMap::new()),
            threads: Mutex::new(HashMap::new()),
            turn_gate: ThreadTurnGate::default(),
            approvals: Arc::new(Mutex::new(HashMap::new())),
            server_requests: Arc::new(Mutex::new(HashMap::new())),
            factory,
            hub,
            live_buffers,
            running_commands,
            store,
            ledger,
        }
    }

    async fn get_or_create_harness(
        &self,
        project: ProjectId,
        config: &ProjectConfig,
    ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        let mut harnesses = self.harnesses.lock().await;
        if let Some(h) = harnesses.get(&project) {
            return Ok(h.clone());
        }
        let h = self.factory.create(config).await?;
        harnesses.insert(project, h.clone());
        Ok(h)
    }

    pub async fn open_thread(
        &self,
        config: &ProjectConfig,
        workspace_root: &str,
        thread: Option<ThreadId>,
        resume: Option<String>,
        initial_model: ModelRef,
    ) -> Result<ThreadHandle, HarnessError> {
        debug!(
            project_id = %config.id,
            thread_id = ?thread,
            resume = ?resume,
            "opening harness thread"
        );
        let harness = self.get_or_create_harness(config.id, config).await?;

        let handle = harness
            .open_thread(OpenThreadOptions {
                project: config.id,
                thread,
                workspace_root: workspace_root.into(),
                resume,
                initial_model: initial_model.clone(),
            })
            .await?;

        // Bind the model the harness reports as effective when it says so — Codex can ignore
        // resume overrides for a loaded thread, and the binding must reflect reality, not the
        // request (spec: model-provider-switching analysis).
        let native_model = handle
            .resumed_model
            .clone()
            .unwrap_or_else(|| initial_model.clone());
        let mut threads = self.threads.lock().await;
        threads.insert(
            handle.thread,
            ThreadBinding {
                project: config.id,
                handle: handle.clone(),
                native_model,
            },
        );
        debug!(
            project_id = %config.id,
            thread_id = %handle.thread,
            harness_thread_id = %handle.harness_thread_id,
            provider = %initial_model.provider,
            model = %initial_model.model,
            warning = handle.warning.as_ref().map(|w| w.code.as_str()).unwrap_or(""),
            "harness thread opened"
        );

        Ok(handle)
    }

    pub async fn start_turn(
        &self,
        thread_id: ThreadId,
        input: UserInput,
        overrides: TurnOverrides,
        effective_model: ModelRef,
    ) -> Result<TurnId, HarnessError> {
        let threads = self.threads.lock().await;
        let binding = threads
            .get(&thread_id)
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let project_id = binding.project;
        let handle = binding.handle.clone();
        drop(threads);
        debug!(
            %project_id,
            %thread_id,
            harness_thread_id = %handle.harness_thread_id,
            mode = ?overrides.mode,
            provider = %effective_model.provider,
            model = %effective_model.model,
            "starting harness turn"
        );

        let harnesses = self.harnesses.lock().await;
        let harness = harnesses
            .get(&project_id)
            .ok_or(HarnessError::ThreadNotFound(thread_id))?
            .clone();
        drop(harnesses);

        let request_started = Instant::now();
        let turn_gate = self.turn_gate.reserve(thread_id)?;
        let ctx = TurnContext {
            user_input: input.clone(),
            model: effective_model,
            mode: overrides.mode,
            kind: TurnContextKind::User,
        };

        let hub = self.hub.clone();
        let live_buffers = self.live_buffers.clone();
        let running_commands = self.running_commands.clone();
        let store = self.store.clone();
        let approvals_map = self.approvals.clone();
        let server_requests_map = self.server_requests.clone();
        let ledger = self.ledger.clone();

        let stream = harness.subscribe(&handle);
        let turn_id = match harness.start_turn(&handle, input, overrides).await {
            Ok(turn_id) => {
                info!(
                    %project_id,
                    %thread_id,
                    %turn_id,
                    harness_thread_id = %handle.harness_thread_id,
                    mode = ?ctx.mode,
                    provider = %ctx.model.provider,
                    model = %ctx.model.model,
                    ack_elapsed_ms = request_started.elapsed().as_millis(),
                    "harness accepted turn start request"
                );
                turn_id
            }
            Err(error) => {
                warn!(
                    %project_id,
                    %thread_id,
                    harness_thread_id = %handle.harness_thread_id,
                    mode = ?ctx.mode,
                    provider = %ctx.model.provider,
                    model = %ctx.model.model,
                    error = %error,
                    ack_elapsed_ms = request_started.elapsed().as_millis(),
                    "harness rejected turn start request"
                );
                return Err(error);
            }
        };

        tokio::spawn(async move {
            forward_events(
                thread_id,
                project_id,
                stream,
                hub,
                live_buffers,
                running_commands,
                store,
                approvals_map,
                server_requests_map,
                ledger,
                ctx,
                Some(turn_gate),
            )
            .await;
        });

        Ok(turn_id)
    }

    /// Route an approval decision to the harness that raised it (§9.2).
    pub async fn respond_approval(
        &self,
        request_id: ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<(), HarnessError> {
        let thread_id = self
            .approvals
            .lock()
            .await
            .get(&request_id)
            .copied()
            .ok_or_else(|| {
                HarnessError::Protocol(format!("no pending approval for id {request_id}"))
            })?;

        let project_id = self
            .get_project_for_thread(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;

        let harness = self
            .harnesses
            .lock()
            .await
            .get(&project_id)
            .cloned()
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;

        self.approvals.lock().await.remove(&request_id);
        harness.respond_approval(request_id, decision).await
    }

    /// Route a non-approval server-request response to the harness that raised it.
    pub async fn respond_server_request(
        &self,
        request_id: ServerRequestId,
        response: ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        let thread_id = self
            .server_requests
            .lock()
            .await
            .get(&request_id)
            .copied()
            .ok_or_else(|| {
                HarnessError::Protocol(format!("no pending server request for id {request_id}"))
            })?;

        let project_id = self
            .get_project_for_thread(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;

        let harness = self
            .harnesses
            .lock()
            .await
            .get(&project_id)
            .cloned()
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;

        harness
            .respond_server_request(request_id.clone(), response)
            .await?;
        self.server_requests.lock().await.remove(&request_id);
        Ok(())
    }

    pub async fn interrupt(&self, thread_id: ThreadId) -> Result<(), HarnessError> {
        let handle = self
            .get_thread_handle(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let project_id = self
            .get_project_for_thread(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let harness = self
            .harnesses
            .lock()
            .await
            .get(&project_id)
            .cloned()
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        harness.interrupt(&handle).await
    }

    pub async fn compact_thread(
        &self,
        thread_id: ThreadId,
        effective_model: ModelRef,
        mode: Mode,
    ) -> Result<(), HarnessError> {
        let handle = self
            .get_thread_handle(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let project_id = self
            .get_project_for_thread(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let harness = self
            .harnesses
            .lock()
            .await
            .get(&project_id)
            .cloned()
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;

        let request_started = Instant::now();
        info!(
            %project_id,
            %thread_id,
            harness_thread_id = %handle.harness_thread_id,
            provider = %effective_model.provider,
            model = %effective_model.model,
            mode = ?mode,
            "starting context compaction"
        );
        let turn_gate = self.turn_gate.reserve(thread_id)?;
        let ctx = TurnContext {
            user_input: UserInput::text("/compact"),
            model: effective_model,
            mode,
            kind: TurnContextKind::ManualCompaction,
        };

        let hub = self.hub.clone();
        let live_buffers = self.live_buffers.clone();
        let running_commands = self.running_commands.clone();
        let store = self.store.clone();
        let approvals_map = self.approvals.clone();
        let server_requests_map = self.server_requests.clone();
        let ledger = self.ledger.clone();

        let stream = harness.subscribe(&handle);
        harness.compact_thread(&handle).await?;
        info!(
            %project_id,
            %thread_id,
            harness_thread_id = %handle.harness_thread_id,
            ack_elapsed_ms = request_started.elapsed().as_millis(),
            "harness accepted context compaction request"
        );

        tokio::spawn(async move {
            forward_events(
                thread_id,
                project_id,
                stream,
                hub,
                live_buffers,
                running_commands,
                store,
                approvals_map,
                server_requests_map,
                ledger,
                ctx,
                Some(turn_gate),
            )
            .await;
        });
        Ok(())
    }

    pub async fn terminate_command(
        &self,
        thread_id: ThreadId,
        process_id: String,
    ) -> Result<(), HarnessError> {
        let handle = self
            .get_thread_handle(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let project_id = self
            .get_project_for_thread(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let harness = self
            .harnesses
            .lock()
            .await
            .get(&project_id)
            .cloned()
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        harness.terminate_command(&handle, &process_id).await
    }

    pub async fn set_thread_archived(
        &self,
        config: &ProjectConfig,
        thread_id: ThreadId,
        harness_thread_id: String,
        archived: bool,
    ) -> Result<(), HarnessError> {
        let harness = self.get_or_create_harness(config.id, config).await?;
        let handle = self
            .get_thread_handle(thread_id)
            .await
            .unwrap_or(ThreadHandle {
                thread: thread_id,
                harness_thread_id,
                warning: None,
                resumed_model: None,
            });
        harness.set_thread_archived(&handle, archived).await
    }

    pub async fn set_thread_name(
        &self,
        config: &ProjectConfig,
        thread_id: ThreadId,
        harness_thread_id: String,
        name: String,
    ) -> Result<(), HarnessError> {
        let harness = self.get_or_create_harness(config.id, config).await?;
        let handle = self
            .get_thread_handle(thread_id)
            .await
            .unwrap_or(ThreadHandle {
                thread: thread_id,
                harness_thread_id,
                warning: None,
                resumed_model: None,
            });
        harness.set_thread_name(&handle, &name).await
    }

    pub async fn list_mcp_servers(
        &self,
        config: &ProjectConfig,
    ) -> Result<Vec<McpServerStatus>, HarnessError> {
        let harness = self.get_or_create_harness(config.id, config).await?;
        harness.list_mcp_servers().await
    }

    pub async fn capabilities(
        &self,
        config: &ProjectConfig,
    ) -> Result<HarnessCapabilities, HarnessError> {
        let harness = self.get_or_create_harness(config.id, config).await?;
        Ok(harness.capabilities())
    }

    pub async fn reload_mcp_servers(&self, config: &ProjectConfig) -> Result<(), HarnessError> {
        let harness = self.get_or_create_harness(config.id, config).await?;
        harness.reload_mcp_servers().await
    }

    pub async fn start_mcp_oauth_login(
        &self,
        config: &ProjectConfig,
        name: &str,
    ) -> Result<McpOauthStart, HarnessError> {
        let harness = self.get_or_create_harness(config.id, config).await?;
        harness.start_mcp_oauth_login(name).await
    }

    pub async fn delete_thread(
        &self,
        config: &ProjectConfig,
        thread_id: ThreadId,
        harness_thread_id: String,
    ) -> Result<(), HarnessError> {
        let harness = self.get_or_create_harness(config.id, config).await?;
        let handle = self
            .get_thread_handle(thread_id)
            .await
            .unwrap_or(ThreadHandle {
                thread: thread_id,
                harness_thread_id,
                warning: None,
                resumed_model: None,
            });
        harness.delete_thread(&handle).await?;
        self.forget_thread(thread_id).await;
        Ok(())
    }

    pub async fn get_thread_handle(&self, thread_id: ThreadId) -> Option<ThreadHandle> {
        let threads = self.threads.lock().await;
        threads
            .get(&thread_id)
            .map(|binding| binding.handle.clone())
    }

    pub async fn get_thread_native_model(&self, thread_id: ThreadId) -> Option<ModelRef> {
        let threads = self.threads.lock().await;
        threads
            .get(&thread_id)
            .map(|binding| binding.native_model.clone())
    }

    pub async fn get_project_for_thread(&self, thread_id: ThreadId) -> Option<ProjectId> {
        let threads = self.threads.lock().await;
        threads.get(&thread_id).map(|binding| binding.project)
    }

    pub async fn thread_has_active_turn(&self, thread_id: ThreadId) -> bool {
        self.turn_gate.is_active(thread_id)
    }

    pub async fn forget_thread(&self, thread_id: ThreadId) {
        let mut threads = self.threads.lock().await;
        threads.remove(&thread_id);
    }
}

#[allow(clippy::too_many_arguments)]
async fn forward_events(
    thread_id: ThreadId,
    project_id: ProjectId,
    mut stream: giskard_harness::AgentEventStream,
    hub: Arc<Hub>,
    live_buffers: Arc<LiveBufferStore>,
    running_commands: Arc<RunningTaskStore>,
    store: Arc<PersistStore>,
    approvals: ApprovalMap,
    server_requests: ServerRequestMap,
    ledger: LedgerHandle,
    ctx: TurnContext,
    mut turn_gate: Option<ThreadTurnLease>,
) {
    let mut turn_id: Option<TurnId> = None;
    let mut owned_turn: Option<TurnId> = None;
    let mut owned_turn_completed = false;
    let mut started_at = Utc::now();
    let mut items: Vec<Item> = Vec::new();
    let mut diffs: Vec<giskard_core::FileDiff> = Vec::new();
    let mut seen_turn_ids = persisted_turn_ids(&store, project_id, thread_id).await;
    let mut seen_harness_item_ids = persisted_harness_item_ids(&store, project_id, thread_id).await;
    let mut duplicate_item_ids = HashSet::new();
    let mut seen_notices = HashSet::new();
    let forwarder_started = Instant::now();
    let mut saw_context_compaction_marker = false;
    debug!(
        %project_id,
        %thread_id,
        context_kind = turn_context_kind_label(ctx.kind),
        mode = ?ctx.mode,
        provider = %ctx.model.provider,
        model = %ctx.model.model,
        turn_gate_held = turn_gate.as_ref().is_some_and(|lease| !lease.is_released()),
        persisted_turn_count = seen_turn_ids.len(),
        persisted_harness_item_count = seen_harness_item_ids.len(),
        "event forwarder started"
    );

    loop {
        match stream.recv().await {
            Ok(event) => {
                let event_thread = event_thread_id(&event);
                if event_thread != thread_id {
                    error!(
                        %thread_id,
                        event_thread_id = %event_thread,
                        event_kind = event_kind(&event),
                        event_turn_id = ?event_turn_id(&event),
                        "dropping harness event for a different thread"
                    );
                    continue;
                }

                if should_skip_duplicate_notice(&event, &mut seen_notices) {
                    debug!(
                        %project_id,
                        %thread_id,
                        event_turn_id = ?event_turn_id(&event),
                        "skipping duplicate harness notice"
                    );
                    continue;
                }

                let event_turn = event_turn_id(&event);
                if let Some(owned) = owned_turn {
                    if let Some(turn) = event_turn {
                        if turn != owned {
                            warn!(
                                %project_id,
                                %thread_id,
                                owned_turn = %owned,
                                event_turn = %turn,
                                event_kind = event_kind(&event),
                                elapsed_ms = forwarder_started.elapsed().as_millis(),
                                "dropping harness event for a different turn on the same thread"
                            );
                            continue;
                        }
                    } else if owned_turn_completed {
                        continue;
                    }
                } else if let Some(turn) = event_turn {
                    if !seen_turn_ids.contains(&turn) {
                        owned_turn = Some(turn);
                        if !matches!(event, AgentEvent::TurnStarted { .. }) {
                            debug!(
                                %thread_id,
                                %turn,
                                "event forwarder attached to turn before seeing turn start"
                            );
                        }
                    }
                }

                if let Some(turn) = event_turn {
                    if seen_turn_ids.contains(&turn) {
                        let command_state_changed =
                            apply_running_command_event(&running_commands, &event).await;
                        if command_state_changed {
                            if is_terminal_command_completion(&event) {
                                hub.broadcast_event(thread_id, event).await;
                            }
                            broadcast_running_commands(&hub, &running_commands, thread_id).await;
                        }
                        if owned_turn_completed {
                            if let Some(owned) = owned_turn {
                                if !running_commands
                                    .has_running_for_turn(thread_id, owned)
                                    .await
                                {
                                    break;
                                }
                            }
                        }
                        continue;
                    }
                }

                if owned_turn.is_none() && event_turn.is_none() {
                    match &event {
                        AgentEvent::Error { error, .. } => {
                            warn!(
                                %project_id,
                                %thread_id,
                                context_kind = turn_context_kind_label(ctx.kind),
                                mode = ?ctx.mode,
                                provider = %ctx.model.provider,
                                model = %ctx.model.model,
                                error = %error,
                                turn_gate_held = turn_gate
                                    .as_ref()
                                    .is_some_and(|lease| !lease.is_released()),
                                elapsed_ms = forwarder_started.elapsed().as_millis(),
                                "turnless harness error received before turn ownership"
                            );
                            hub.broadcast_event(thread_id, event.clone()).await;
                        }
                        AgentEvent::Notice { message, .. } => {
                            debug!(
                                %project_id,
                                %thread_id,
                                context_kind = turn_context_kind_label(ctx.kind),
                                message,
                                turn_gate_held = turn_gate
                                    .as_ref()
                                    .is_some_and(|lease| !lease.is_released()),
                                elapsed_ms = forwarder_started.elapsed().as_millis(),
                                "turnless harness notice received before turn ownership"
                            );
                            hub.broadcast_event(thread_id, event.clone()).await;
                        }
                        _ => {}
                    }
                    continue;
                }

                if should_skip_duplicate_item(
                    &event,
                    &mut seen_harness_item_ids,
                    &mut duplicate_item_ids,
                ) {
                    debug!(
                        %project_id,
                        %thread_id,
                        event_kind = event_kind(&event),
                        event_turn_id = ?event_turn_id(&event),
                        item_id = ?event_item_id(&event),
                        harness_item_id = ?event_harness_item_id(&event),
                        "skipping duplicate harness item event"
                    );
                    continue;
                }

                let command_state_changed =
                    apply_running_command_event(&running_commands, &event).await;

                match &event {
                    AgentEvent::TurnStarted { turn, .. } => {
                        turn_id = Some(*turn);
                        started_at = Utc::now();
                        if ctx.kind == TurnContextKind::ManualCompaction {
                            info!(
                                %project_id,
                                %thread_id,
                                %turn,
                                elapsed_ms = forwarder_started.elapsed().as_millis(),
                                "context compaction turn started"
                            );
                        }
                    }
                    AgentEvent::ItemCompleted { item, turn, .. } => {
                        if ctx.kind == TurnContextKind::ManualCompaction
                            && is_context_compaction_item(item)
                        {
                            saw_context_compaction_marker = true;
                            info!(
                                %project_id,
                                %thread_id,
                                %turn,
                                turn_started_seen = turn_id.is_some(),
                                will_synthesize_completion = turn_id.is_none(),
                                items_buffered_after = items.len() + 1,
                                elapsed_ms = forwarder_started.elapsed().as_millis(),
                                "context compaction marker received"
                            );
                        }
                        items.push(item.clone());
                    }
                    AgentEvent::DiffUpdated { diff, .. } => {
                        let existing = diffs.iter_mut().find(|d| d.path == diff.path);
                        if let Some(existing) = existing {
                            *existing = diff.clone();
                        } else {
                            diffs.push(diff.clone());
                        }
                    }
                    AgentEvent::ApprovalRequested { request, .. } => {
                        approvals.lock().await.insert(request.id.clone(), thread_id);
                    }
                    AgentEvent::ServerRequestReceived { request, .. } => {
                        server_requests
                            .lock()
                            .await
                            .insert(request.id.clone(), thread_id);
                    }
                    AgentEvent::ServerRequestResolved { request_id, .. } => {
                        server_requests.lock().await.remove(request_id);
                    }
                    _ => {}
                }

                let is_turn_start = matches!(event, AgentEvent::TurnStarted { .. });
                let completed = if let AgentEvent::TurnCompleted {
                    turn,
                    usage,
                    status,
                    ..
                } = &event
                {
                    Some((*turn, *usage, status.clone()))
                } else {
                    None
                };
                let synthetic_compaction_completed = match &event {
                    AgentEvent::ItemCompleted { turn, item, .. }
                        if ctx.kind == TurnContextKind::ManualCompaction
                            && turn_id.is_none()
                            && is_context_compaction_item(item) =>
                    {
                        Some(*turn)
                    }
                    _ => None,
                };

                if is_turn_start {
                    live_buffers.start_turn(thread_id).await;
                }
                if live_buffers.is_active(thread_id).await {
                    live_buffers.append(thread_id, event.clone()).await;
                }

                if let Some((completed_turn, usage, status)) = completed {
                    if ctx.kind == TurnContextKind::ManualCompaction {
                        info!(
                            %project_id,
                            %thread_id,
                            turn = %completed_turn,
                            status = ?status.kind,
                            items_buffered = items.len(),
                            saw_context_compaction_marker,
                            elapsed_ms = forwarder_started.elapsed().as_millis(),
                            "context compaction turn completed"
                        );
                    }
                    let tid = complete_forwarded_turn(
                        thread_id,
                        project_id,
                        completed_turn,
                        usage,
                        status.clone(),
                        &ctx,
                        &mut items,
                        &mut diffs,
                        started_at,
                        turn_id,
                        &mut seen_turn_ids,
                        &store,
                        &hub,
                        &ledger,
                        &live_buffers,
                        turn_gate.as_mut(),
                    )
                    .await;
                    owned_turn_completed = true;
                    hub.broadcast_event(thread_id, event).await;
                    if command_state_changed {
                        broadcast_running_commands(&hub, &running_commands, thread_id).await;
                    }
                    if !running_commands.has_running_for_turn(thread_id, tid).await {
                        break;
                    }
                    continue;
                }

                hub.broadcast_event(thread_id, event).await;

                if command_state_changed {
                    broadcast_running_commands(&hub, &running_commands, thread_id).await;
                }

                if let Some(completed_turn) = synthetic_compaction_completed {
                    info!(
                        %project_id,
                        %thread_id,
                        turn = %completed_turn,
                        turn_started_seen = turn_id.is_some(),
                        items_buffered = items.len(),
                        elapsed_ms = forwarder_started.elapsed().as_millis(),
                        "context compaction completed from marker without turn completion"
                    );
                    let status = TurnStatus {
                        kind: TurnStatusKind::Completed,
                        message: None,
                    };
                    let completion_event = AgentEvent::TurnCompleted {
                        thread: thread_id,
                        turn: completed_turn,
                        usage: giskard_core::token::TokenUsage::default(),
                        status: status.clone(),
                    };
                    if live_buffers.is_active(thread_id).await {
                        live_buffers
                            .append(thread_id, completion_event.clone())
                            .await;
                    }
                    let tid = complete_forwarded_turn(
                        thread_id,
                        project_id,
                        completed_turn,
                        giskard_core::token::TokenUsage::default(),
                        status,
                        &ctx,
                        &mut items,
                        &mut diffs,
                        started_at,
                        turn_id,
                        &mut seen_turn_ids,
                        &store,
                        &hub,
                        &ledger,
                        &live_buffers,
                        turn_gate.as_mut(),
                    )
                    .await;
                    owned_turn_completed = true;
                    hub.broadcast_event(thread_id, completion_event).await;
                    if !running_commands.has_running_for_turn(thread_id, tid).await {
                        break;
                    }
                }
            }
            Err(e) => {
                if ctx.kind == TurnContextKind::ManualCompaction && !owned_turn_completed {
                    let live_buffer_active = live_buffers.is_active(thread_id).await;
                    warn!(
                        %project_id,
                        %thread_id,
                        ?e,
                        ?owned_turn,
                        ?turn_id,
                        saw_context_compaction_marker,
                        items_buffered = items.len(),
                        live_buffer_active,
                        turn_gate_held = turn_gate.is_some(),
                        elapsed_ms = forwarder_started.elapsed().as_millis(),
                        "context compaction event stream ended before completion"
                    );
                } else {
                    debug!(%thread_id, ?e, "event stream ended");
                }
                break;
            }
        }
    }
    let turn_gate_held = turn_gate.as_ref().is_some_and(|lease| !lease.is_released());
    if turn_gate_held && !owned_turn_completed {
        warn!(
            %project_id,
            %thread_id,
            context_kind = turn_context_kind_label(ctx.kind),
            mode = ?ctx.mode,
            provider = %ctx.model.provider,
            model = %ctx.model.model,
            ?owned_turn,
            ?turn_id,
            items_buffered = items.len(),
            diffs_buffered = diffs.len(),
            saw_context_compaction_marker,
            elapsed_ms = forwarder_started.elapsed().as_millis(),
            "event forwarder exited without turn completion; active-turn gate will be released by drop"
        );
    } else {
        debug!(
            %project_id,
            %thread_id,
            context_kind = turn_context_kind_label(ctx.kind),
            ?owned_turn,
            ?turn_id,
            owned_turn_completed,
            turn_gate_held,
            elapsed_ms = forwarder_started.elapsed().as_millis(),
            "event forwarder exited"
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn complete_forwarded_turn(
    thread_id: ThreadId,
    project_id: ProjectId,
    completed_turn: TurnId,
    usage: giskard_core::token::TokenUsage,
    status: TurnStatus,
    ctx: &TurnContext,
    items: &mut Vec<Item>,
    diffs: &mut Vec<giskard_core::FileDiff>,
    started_at: chrono::DateTime<Utc>,
    turn_id: Option<TurnId>,
    seen_turn_ids: &mut HashSet<TurnId>,
    store: &Arc<PersistStore>,
    hub: &Arc<Hub>,
    ledger: &LedgerHandle,
    live_buffers: &Arc<LiveBufferStore>,
    turn_gate: Option<&mut ThreadTurnLease>,
) -> TurnId {
    let tid = turn_id.unwrap_or(completed_turn);
    seen_turn_ids.insert(tid);
    let item_count = items.len();
    let has_context_compaction_marker = items.iter().any(is_context_compaction_item);
    if ctx.kind == TurnContextKind::ManualCompaction {
        info!(
            %project_id,
            %thread_id,
            turn = %tid,
            completed_turn = %completed_turn,
            started_turn = ?turn_id,
            item_count,
            has_context_compaction_marker,
            status = ?status.kind,
            "persisting context compaction turn"
        );
    }
    let turn = Turn {
        id: tid,
        user_input: ctx.user_input.clone(),
        items: std::mem::take(items),
        model: ctx.model.clone(),
        mode: ctx.mode,
        status,
        usage,
        diffs: std::mem::take(diffs),
        started_at,
        completed_at: Some(Utc::now()),
    };
    persist_turn(store, hub, ledger, project_id, thread_id, turn).await;
    if ctx.kind == TurnContextKind::ManualCompaction {
        info!(
            %project_id,
            %thread_id,
            turn = %tid,
            item_count,
            has_context_compaction_marker,
            "context compaction persistence path finished"
        );
    }
    live_buffers.clear_turn(thread_id).await;
    if let Some(turn_gate) = turn_gate {
        turn_gate.release();
    }
    tid
}

fn is_context_compaction_item(item: &Item) -> bool {
    matches!(
        &item.payload,
        ItemPayload::Activity { title, .. } if title == "Context compacted"
    )
}

fn should_skip_duplicate_notice(
    event: &AgentEvent,
    seen_notices: &mut HashSet<(Option<TurnId>, String)>,
) -> bool {
    let AgentEvent::Notice { turn, message, .. } = event else {
        return false;
    };
    !seen_notices.insert((*turn, message.clone()))
}

fn event_turn_id(event: &AgentEvent) -> Option<TurnId> {
    match event {
        AgentEvent::TurnStarted { turn, .. }
        | AgentEvent::ItemStarted { turn, .. }
        | AgentEvent::ItemDelta { turn, .. }
        | AgentEvent::ItemCompleted { turn, .. }
        | AgentEvent::DiffUpdated { turn, .. }
        | AgentEvent::ApprovalRequested { turn, .. }
        | AgentEvent::TurnCompleted { turn, .. } => Some(*turn),
        AgentEvent::ServerRequestReceived { turn, .. }
        | AgentEvent::ServerRequestResolved { turn, .. } => *turn,
        AgentEvent::ThreadOpened { .. }
        | AgentEvent::Error { turn: None, .. }
        | AgentEvent::Notice { turn: None, .. } => None,
        AgentEvent::Error {
            turn: Some(turn), ..
        }
        | AgentEvent::Notice {
            turn: Some(turn), ..
        } => Some(*turn),
    }
}

fn event_thread_id(event: &AgentEvent) -> ThreadId {
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

fn event_kind(event: &AgentEvent) -> &'static str {
    match event {
        AgentEvent::ThreadOpened { .. } => "thread_opened",
        AgentEvent::TurnStarted { .. } => "turn_started",
        AgentEvent::ItemStarted { .. } => "item_started",
        AgentEvent::ItemDelta { .. } => "item_delta",
        AgentEvent::ItemCompleted { .. } => "item_completed",
        AgentEvent::DiffUpdated { .. } => "diff_updated",
        AgentEvent::ApprovalRequested { .. } => "approval_requested",
        AgentEvent::ServerRequestReceived { .. } => "server_request_received",
        AgentEvent::ServerRequestResolved { .. } => "server_request_resolved",
        AgentEvent::TurnCompleted { .. } => "turn_completed",
        AgentEvent::Error { .. } => "error",
        AgentEvent::Notice { .. } => "notice",
    }
}

fn event_item_id(event: &AgentEvent) -> Option<ItemId> {
    match event {
        AgentEvent::ItemStarted { item, .. } => Some(item.id),
        AgentEvent::ItemDelta { item_id, .. } => Some(*item_id),
        AgentEvent::ItemCompleted { item, .. } => Some(item.id),
        _ => None,
    }
}

fn event_harness_item_id(event: &AgentEvent) -> Option<&str> {
    match event {
        AgentEvent::ItemStarted { item, .. } if !item.harness_item_id.is_empty() => {
            Some(item.harness_item_id.as_str())
        }
        AgentEvent::ItemCompleted { item, .. } if !item.harness_item_id.is_empty() => {
            Some(item.harness_item_id.as_str())
        }
        _ => None,
    }
}

async fn apply_running_command_event(
    running_commands: &RunningTaskStore,
    event: &AgentEvent,
) -> bool {
    let command_before_completion =
        terminating_command_before_terminal_completion(running_commands, event).await;
    let changed = running_commands.apply_event(event).await;
    log_command_completion_after_terminate(command_before_completion.as_ref(), event);
    changed
}

async fn terminating_command_before_terminal_completion(
    running_commands: &RunningTaskStore,
    event: &AgentEvent,
) -> Option<RunningTask> {
    let AgentEvent::ItemCompleted { thread, item, .. } = event else {
        return None;
    };
    let ItemPayload::CommandExecution { status, .. } = &item.payload else {
        return None;
    };
    if status
        .as_deref()
        .map(command_status_is_running)
        .unwrap_or(false)
    {
        return None;
    }

    let command = running_commands.get_by_item(*thread, item.id).await?;
    command.terminating.then_some(command)
}

fn log_command_completion_after_terminate(command: Option<&RunningTask>, event: &AgentEvent) {
    let Some(command) = command else {
        return;
    };
    let AgentEvent::ItemCompleted { thread, turn, item } = event else {
        return;
    };
    let ItemPayload::CommandExecution {
        status,
        exit_code,
        duration_ms,
        ..
    } = &item.payload
    else {
        return;
    };
    let Some(status) = status else {
        return;
    };
    if !command_completion_is_normal_success(status, *exit_code) {
        return;
    }

    warn!(
        thread_id = %thread,
        turn_id = %turn,
        item_id = %item.id,
        harness_item_id = %item.harness_item_id,
        process_id = ?command.process_id,
        command = %command.command,
        status = %status,
        exit_code = ?exit_code,
        duration_ms = ?duration_ms,
        "command completed normally after stop request; Codex did not terminate the process"
    );
}

async fn broadcast_running_commands(
    hub: &Hub,
    running_commands: &RunningTaskStore,
    thread_id: ThreadId,
) {
    let tasks = running_commands.snapshot(thread_id).await;
    hub.broadcast(thread_id, ServerMessage::RunningTasks { thread_id, tasks })
        .await;
}

fn is_terminal_command_completion(event: &AgentEvent) -> bool {
    let AgentEvent::ItemCompleted { item, .. } = event else {
        return false;
    };
    let ItemPayload::CommandExecution { status, .. } = &item.payload else {
        return false;
    };
    !status
        .as_deref()
        .map(command_status_is_running)
        .unwrap_or(false)
}

fn command_completion_is_normal_success(status: &str, exit_code: Option<i32>) -> bool {
    matches!(
        normalized_command_status(status).as_str(),
        "completed" | "succeeded" | "success"
    ) && exit_code == Some(0)
}

fn should_skip_duplicate_item(
    event: &AgentEvent,
    seen_harness_item_ids: &mut HashSet<String>,
    duplicate_item_ids: &mut HashSet<ItemId>,
) -> bool {
    match event {
        AgentEvent::ItemStarted { item, .. } => {
            if !item.harness_item_id.is_empty()
                && seen_harness_item_ids.contains(&item.harness_item_id)
            {
                duplicate_item_ids.insert(item.id);
                return true;
            }
            false
        }
        AgentEvent::ItemDelta { item_id, .. } => duplicate_item_ids.contains(item_id),
        AgentEvent::ItemCompleted { item, .. } => {
            if duplicate_item_ids.remove(&item.id) {
                return true;
            }
            if item.harness_item_id.is_empty() {
                return false;
            }
            !seen_harness_item_ids.insert(item.harness_item_id.clone())
        }
        _ => false,
    }
}

async fn persisted_turn_ids(
    store: &PersistStore,
    project_id: ProjectId,
    thread_id: ThreadId,
) -> HashSet<TurnId> {
    match store.load_all_turns(project_id, thread_id).await {
        Ok(turns) => turns.into_iter().map(|turn| turn.id).collect(),
        Err(error) => {
            warn!(
                %project_id,
                %thread_id,
                %error,
                "failed to load persisted turn ids; duplicate-turn detection starts empty"
            );
            HashSet::new()
        }
    }
}

async fn persisted_harness_item_ids(
    store: &PersistStore,
    project_id: ProjectId,
    thread_id: ThreadId,
) -> HashSet<String> {
    match store.load_all_turns(project_id, thread_id).await {
        Ok(turns) => turns
            .into_iter()
            .flat_map(|turn| turn.items)
            .filter_map(|item| {
                if item.harness_item_id.is_empty() {
                    None
                } else {
                    Some(item.harness_item_id)
                }
            })
            .collect(),
        Err(error) => {
            warn!(
                %project_id,
                %thread_id,
                %error,
                "failed to load persisted harness item ids; duplicate-item detection starts empty"
            );
            HashSet::new()
        }
    }
}

/// Append a completed `Turn` to the thread file, fold its usage into the thread ledger, recompute
/// the cached context window, persist atomically (§7.1), and hand the usage delta to the global +
/// project ledger actor (§10.2). Best-effort: logs on failure.
async fn persist_turn(
    store: &PersistStore,
    hub: &Hub,
    ledger: &LedgerHandle,
    project_id: ProjectId,
    thread_id: ThreadId,
    turn: Turn,
) {
    // C4: recompute the cached context window from the current model on write.
    let config = match store.load_config().await {
        Ok(config) => Some(config),
        Err(error) => {
            warn!(
                %project_id,
                %thread_id,
                %error,
                "failed to load config while persisting turn; context window cache will not be refreshed"
            );
            None
        }
    };

    // Only completed/interrupted turns carry real usage; capture the bits we need before `turn`
    // moves into the closure.
    let should_record = matches!(
        turn.status.kind,
        TurnStatusKind::Completed | TurnStatusKind::Interrupted
    );
    let provider = turn.model.provider.clone();
    let model = turn.model.model.clone();
    let usage = turn.usage;

    // H3 ordering: append the turn to the authoritative JSONL history FIRST, then update the
    // metadata aggregates. A crash between the two leaves the turn in history but not yet in the
    // aggregates cache — recoverable via `recompute_aggregates`.
    if let Err(e) = store.append_turn(project_id, thread_id, &turn).await {
        warn!(%thread_id, %e, "failed to append turn to history; skipping metadata update");
        return;
    }

    // Metadata-only RMW under the per-thread lock (§5.4): fold usage into the aggregates cache and
    // refresh the context window. The history no longer lives here.
    let updated = store
        .update_thread(project_id, thread_id, move |tf| {
            if should_record {
                tf.tokens
                    .record(&turn.model.provider, &turn.model.model, &turn.usage);
            }
            tf.updated_at = Utc::now();
            if let Some(config) = &config {
                tf.context_window = context_window_for(config, &tf.current_model);
            }
        })
        .await;

    let tf = match updated {
        Ok(Some(tf)) => tf,
        Ok(None) => {
            warn!(%thread_id, "thread file missing on turn completion");
            return;
        }
        Err(e) => {
            warn!(%thread_id, %e, "failed to persist thread on turn completion");
            return;
        }
    };

    // Fold the same usage into the project + global ledgers via the single-writer actor (§10.2).
    if should_record {
        let date = Utc::now().format("%Y-%m-%d").to_string();
        ledger
            .record(project_id, date, provider, model, usage)
            .await;
    }

    // Push a thread-scoped token update to subscribers (§13.6).
    if let Ok(ledger_json) = serde_json::to_value(&tf.tokens) {
        hub.broadcast(
            thread_id,
            ServerMessage::TokenUpdate {
                scope: TokenScope::Thread,
                thread_id: Some(thread_id),
                ledger: ledger_json,
            },
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
    use giskard_core::error::HarnessError;
    use giskard_core::event::AgentEvent;
    use giskard_core::ids::{ApprovalId, ItemId, ProjectId, ServerRequestId, ThreadId, TurnId};
    use giskard_core::item::{CommandExecutionStart, Item, ItemKind, ItemPayload, ItemStart};
    use giskard_core::model::ModelRef;
    use giskard_core::server_request::ServerRequest;
    use giskard_core::token::{TokenLedger, TokenUsage};
    use giskard_core::turn::{ApprovalPolicy, Mode, TurnStatus, TurnStatusKind};
    use giskard_core::user_input::UserInput;
    use giskard_harness::AgentEventStream;
    use giskard_persist::PersistStore;
    use giskard_persist::store::ThreadFile;
    use giskard_proto::{ServerMessage, WireAgentEvent};
    use tokio::sync::{Mutex, broadcast, mpsc};

    use super::{
        ThreadTurnGate, TurnContext, TurnContextKind, command_completion_is_normal_success,
        command_status_is_running, forward_events,
    };
    use crate::hub::Hub;
    use crate::ledger;
    use crate::live_buffer::LiveBufferStore;
    use crate::running_commands::RunningTaskStore;

    #[test]
    fn command_completion_success_requires_success_status_and_zero_exit() {
        assert!(command_completion_is_normal_success("completed", Some(0)));
        assert!(command_completion_is_normal_success("succeeded", Some(0)));
        assert!(command_completion_is_normal_success("success", Some(0)));

        assert!(!command_completion_is_normal_success(
            "completed",
            Some(143)
        ));
        assert!(!command_completion_is_normal_success("failed", Some(0)));
        assert!(!command_completion_is_normal_success("interrupted", None));
    }

    #[test]
    fn command_status_running_accepts_codex_variants() {
        assert!(command_status_is_running("in_progress"));
        assert!(command_status_is_running("in-progress"));
        assert!(command_status_is_running("running"));

        assert!(!command_status_is_running("completed"));
        assert!(!command_status_is_running("interrupted"));
    }

    #[tokio::test]
    async fn live_turn_forwarders_do_not_persist_later_turns() {
        let tmp = tempfile::TempDir::new().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test", model.clone())
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    approval_policy: ApprovalPolicy::Ask,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let live_buffers = Arc::new(LiveBufferStore::new());
        let running_commands = Arc::new(RunningTaskStore::new());
        let approvals = Arc::new(Mutex::new(Default::default()));
        let server_requests = Arc::new(Mutex::new(Default::default()));
        let ledger = ledger::spawn(store.clone());

        spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub.clone(),
            live_buffers.clone(),
            running_commands.clone(),
            store.clone(),
            approvals.clone(),
            server_requests.clone(),
            ledger.clone(),
            model.clone(),
            "first",
        );
        let first_turn = TurnId::new();
        for event in turn_events(
            thread_id,
            first_turn,
            "first",
            "one",
            TokenUsage::new(10, 1),
        ) {
            tx.send(event).unwrap();
        }
        wait_for_turn_count(&store, project_id, thread_id, 1).await;

        spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            live_buffers,
            running_commands,
            store.clone(),
            approvals,
            server_requests,
            ledger,
            model,
            "second",
        );
        let second_turn = TurnId::new();
        for event in turn_events(
            thread_id,
            second_turn,
            "second",
            "two",
            TokenUsage::new(20, 2),
        ) {
            tx.send(event).unwrap();
        }
        wait_for_turn_count(&store, project_id, thread_id, 2).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let raw_history = tokio::fs::read_to_string(
            data_dir
                .join("projects")
                .join(project_id.to_string())
                .join("threads")
                .join(format!("{thread_id}.jsonl")),
        )
        .await
        .unwrap();
        assert_eq!(raw_history.lines().count(), 2);

        let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(saved.len(), 2);
        assert_eq!(saved[0].id, first_turn);
        assert_eq!(saved[0].user_input, UserInput::text("first"));
        assert_eq!(saved[1].id, second_turn);
        assert_eq!(saved[1].user_input, UserInput::text("second"));
    }

    #[tokio::test]
    async fn live_turn_forwarder_ignores_events_for_other_threads() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let other_thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test", model.clone())
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "target".into(),
                    harness_thread_id: "th_target".into(),
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    approval_policy: ApprovalPolicy::Ask,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let live_buffers = Arc::new(LiveBufferStore::new());
        let running_commands = Arc::new(RunningTaskStore::new());
        let approvals = Arc::new(Mutex::new(Default::default()));
        let server_requests = Arc::new(Mutex::new(Default::default()));
        let ledger = ledger::spawn(store.clone());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        hub.subscribe(thread_id, 1, client_tx).await;

        spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            live_buffers.clone(),
            running_commands,
            store.clone(),
            approvals,
            server_requests,
            ledger,
            model,
            "target",
        );
        let foreign_turn = TurnId::new();
        for event in turn_events(
            other_thread_id,
            foreign_turn,
            "foreign",
            "wrong",
            TokenUsage::new(99, 1),
        ) {
            tx.send(event).unwrap();
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert!(
            saved.is_empty(),
            "events for another thread must not be persisted into the target thread"
        );
        assert!(
            live_buffers.snapshot(thread_id).await.is_none(),
            "events for another thread must not create a live snapshot"
        );
        assert!(
            matches!(
                client_rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "events for another thread must not be broadcast to target-thread subscribers"
        );
    }

    #[tokio::test]
    async fn live_turn_forwarder_rejects_foreign_side_effect_events() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let other_thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test", model.clone())
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "target".into(),
                    harness_thread_id: "th_target".into(),
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    approval_policy: ApprovalPolicy::Ask,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let live_buffers = Arc::new(LiveBufferStore::new());
        let running_commands = Arc::new(RunningTaskStore::new());
        let approvals = Arc::new(Mutex::new(Default::default()));
        let server_requests = Arc::new(Mutex::new(Default::default()));
        let ledger = ledger::spawn(store.clone());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        hub.subscribe(thread_id, 1, client_tx).await;

        spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            live_buffers.clone(),
            running_commands.clone(),
            store.clone(),
            approvals.clone(),
            server_requests.clone(),
            ledger,
            model,
            "target",
        );

        let foreign_turn = TurnId::new();
        let foreign_item = ItemId::new();
        let approval_id = ApprovalId("foreign_approval".into());
        let server_request_id = ServerRequestId("foreign_request".into());
        let foreign_events = vec![
            AgentEvent::Notice {
                thread: other_thread_id,
                turn: None,
                message: "wrong thread notice".into(),
            },
            AgentEvent::Error {
                thread: other_thread_id,
                turn: None,
                error: HarnessError::Protocol("wrong thread error".into()),
            },
            AgentEvent::ApprovalRequested {
                thread: other_thread_id,
                turn: foreign_turn,
                request: ApprovalRequest {
                    id: approval_id.clone(),
                    kind: ApprovalKind::CommandExecution {
                        command: "sleep 60".into(),
                        cwd: "/tmp/test".into(),
                    },
                    reason: Some("wrong thread approval".into()),
                    metadata: Vec::new(),
                    available: vec![ApprovalDecision::Accept, ApprovalDecision::Cancel],
                },
            },
            AgentEvent::ServerRequestReceived {
                thread: other_thread_id,
                turn: Some(foreign_turn),
                request: ServerRequest {
                    id: server_request_id.clone(),
                    method: "tool/request_user_input".into(),
                    params: serde_json::json!({"message": "wrong thread request"}),
                    received_at: Utc::now(),
                },
            },
            AgentEvent::ItemStarted {
                thread: other_thread_id,
                turn: foreign_turn,
                item: ItemStart {
                    id: foreign_item,
                    harness_item_id: "foreign_command".into(),
                    kind: ItemKind::CommandExecution,
                    command: Some(CommandExecutionStart {
                        command: "sleep 60".into(),
                        cwd: "/tmp/test".into(),
                        status: Some("running".into()),
                        process_id: Some("foreign_process".into()),
                        started_at_ms: Some(1),
                    }),
                    tool: None,
                },
            },
        ];

        for event in foreign_events {
            tx.send(event).unwrap();
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(
            store
                .load_all_turns(project_id, thread_id)
                .await
                .unwrap()
                .is_empty(),
            "foreign events must not be persisted into the target thread"
        );
        assert!(
            live_buffers.snapshot(thread_id).await.is_none(),
            "foreign events must not create target-thread live state"
        );
        assert!(
            running_commands.snapshot(thread_id).await.is_empty(),
            "foreign running commands must not appear in the target-thread task list"
        );
        assert!(
            approvals.lock().await.get(&approval_id).is_none(),
            "foreign approvals must not register against the target thread"
        );
        assert!(
            server_requests
                .lock()
                .await
                .get(&server_request_id)
                .is_none(),
            "foreign server requests must not register against the target thread"
        );
        assert!(
            matches!(
                client_rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "foreign notices/errors must not be broadcast to target-thread subscribers"
        );
    }

    #[tokio::test]
    async fn forwarder_deduplicates_identical_notices_in_one_turn() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test", model.clone())
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    approval_policy: ApprovalPolicy::Ask,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        hub.subscribe(thread_id, 1, client_tx).await;
        let live_buffers = Arc::new(LiveBufferStore::new());
        let running_commands = Arc::new(RunningTaskStore::new());
        let approvals = Arc::new(Mutex::new(Default::default()));
        let server_requests = Arc::new(Mutex::new(Default::default()));
        let ledger = ledger::spawn(store.clone());

        spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            live_buffers,
            running_commands,
            store,
            approvals,
            server_requests,
            ledger,
            model,
            "compact",
        );

        let turn = TurnId::new();
        tx.send(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        })
        .unwrap();
        for _ in 0..2 {
            tx.send(AgentEvent::Notice {
                thread: thread_id,
                turn: Some(turn),
                message: "Heads up: Long threads and multiple compactions can cause drift.".into(),
            })
            .unwrap();
        }
        tx.send(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        })
        .unwrap();

        let mut notice_count = 0;
        let mut completed = false;
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline && !completed {
            match tokio::time::timeout(tokio::time::Duration::from_secs(1), client_rx.recv()).await
            {
                Ok(Some(ServerMessage::Event { agent_event, .. })) => match agent_event {
                    WireAgentEvent::Notice { .. } => notice_count += 1,
                    WireAgentEvent::TurnCompleted { .. } => completed = true,
                    _ => {}
                },
                Ok(Some(_)) => {}
                _ => {}
            }
        }

        assert!(completed, "turn should complete");
        assert_eq!(notice_count, 1);
    }

    #[tokio::test]
    async fn manual_compaction_item_completes_turn_and_releases_gate() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test", model.clone())
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    approval_policy: ApprovalPolicy::Ask,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        hub.subscribe(thread_id, 1, client_tx).await;
        let live_buffers = Arc::new(LiveBufferStore::new());
        let running_commands = Arc::new(RunningTaskStore::new());
        let approvals = Arc::new(Mutex::new(Default::default()));
        let server_requests = Arc::new(Mutex::new(Default::default()));
        let ledger = ledger::spawn(store.clone());
        let gate = ThreadTurnGate::default();
        let lease = gate.reserve(thread_id).unwrap();
        let stream = AgentEventStream::new(tx.subscribe());
        let ctx = TurnContext {
            user_input: UserInput::text("/compact"),
            model: model.clone(),
            mode: Mode::Build,
            kind: TurnContextKind::ManualCompaction,
        };

        tokio::spawn({
            let hub = hub.clone();
            let live_buffers = live_buffers.clone();
            let running_commands = running_commands.clone();
            let store = store.clone();
            async move {
                forward_events(
                    thread_id,
                    project_id,
                    stream,
                    hub,
                    live_buffers,
                    running_commands,
                    store,
                    approvals,
                    server_requests,
                    ledger,
                    ctx,
                    Some(lease),
                )
                .await;
            }
        });

        let turn = TurnId::new();
        tx.send(AgentEvent::ItemCompleted {
            thread: thread_id,
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
        })
        .unwrap();

        let mut completed = false;
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline && !completed {
            match tokio::time::timeout(tokio::time::Duration::from_secs(1), client_rx.recv()).await
            {
                Ok(Some(ServerMessage::Event { agent_event, .. })) => {
                    if matches!(agent_event, WireAgentEvent::TurnCompleted { .. }) {
                        completed = true;
                    }
                }
                Ok(Some(_)) => {}
                _ => {}
            }
        }
        assert!(
            completed,
            "compaction marker should synthesize turn completion"
        );

        wait_for_turn_count(&store, project_id, thread_id, 1).await;
        let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(saved[0].id, turn);
        assert_eq!(saved[0].user_input.as_text(), Some("/compact"));
        assert!(matches!(saved[0].status.kind, TurnStatusKind::Completed));
        assert!(saved[0].items.iter().any(|item| matches!(
            &item.payload,
            ItemPayload::Activity { title, .. } if title == "Context compacted"
        )));
        assert!(
            gate.reserve(thread_id).is_ok(),
            "manual compaction completion should release the turn gate"
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_forwarder(
        thread_id: ThreadId,
        project_id: ProjectId,
        stream: AgentEventStream,
        hub: Arc<Hub>,
        live_buffers: Arc<LiveBufferStore>,
        running_commands: Arc<RunningTaskStore>,
        store: Arc<PersistStore>,
        approvals: super::ApprovalMap,
        server_requests: super::ServerRequestMap,
        ledger: ledger::LedgerHandle,
        model: ModelRef,
        user_input: &str,
    ) {
        let ctx = TurnContext {
            user_input: UserInput::text(user_input),
            model,
            mode: Mode::Build,
            kind: TurnContextKind::User,
        };
        tokio::spawn(async move {
            forward_events(
                thread_id,
                project_id,
                stream,
                hub,
                live_buffers,
                running_commands,
                store,
                approvals,
                server_requests,
                ledger,
                ctx,
                None,
            )
            .await;
        });
    }

    fn turn_events(
        thread: ThreadId,
        turn: TurnId,
        input: &str,
        output: &str,
        usage: TokenUsage,
    ) -> Vec<AgentEvent> {
        let now = Utc::now();
        vec![
            AgentEvent::TurnStarted { thread, turn },
            AgentEvent::ItemCompleted {
                thread,
                turn,
                item: Item {
                    id: ItemId::new(),
                    harness_item_id: format!("user_{input}"),
                    payload: ItemPayload::UserMessage { text: input.into() },
                    created_at: now,
                },
            },
            AgentEvent::ItemCompleted {
                thread,
                turn,
                item: Item {
                    id: ItemId::new(),
                    harness_item_id: format!("agent_{output}"),
                    payload: ItemPayload::AgentMessage {
                        text: output.into(),
                    },
                    created_at: now,
                },
            },
            AgentEvent::TurnCompleted {
                thread,
                turn,
                usage,
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
            },
        ]
    }

    async fn wait_for_turn_count(
        store: &PersistStore,
        project_id: ProjectId,
        thread_id: ThreadId,
        count: usize,
    ) {
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        loop {
            let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
            if saved.len() >= count {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timed out waiting for {count} persisted turns");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }
}
