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
use giskard_core::model::{ModelDescriptor, ModelRef};
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::turn::{Mode, Turn, TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{AgentHarness, HarnessCapabilities, OpenThreadOptions, ThreadHandle};
use giskard_persist::PersistStore;
use giskard_persist::store::ProjectConfig;
use giskard_proto::{RunningTask, ServerMessage, ThreadActivity, ThreadActivityKind, TokenScope};

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

#[derive(Clone, Copy)]
enum ForwarderExitReason {
    NormalTurnCompleted,
    SyntheticCompactionCompleted,
    AfterTurnCommandsDrained,
    StreamEndedRecovered,
    StreamEndedWithoutTurn,
}

fn forwarder_exit_reason_label(reason: ForwarderExitReason) -> &'static str {
    match reason {
        ForwarderExitReason::NormalTurnCompleted => "normal_turn_completed",
        ForwarderExitReason::SyntheticCompactionCompleted => "synthetic_compaction_completed",
        ForwarderExitReason::AfterTurnCommandsDrained => "after_turn_commands_drained",
        ForwarderExitReason::StreamEndedRecovered => "stream_ended_recovered",
        ForwarderExitReason::StreamEndedWithoutTurn => "stream_ended_without_turn",
    }
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
    active: Arc<StdMutex<HashMap<ThreadId, ActiveTurnOwner>>>,
}

#[derive(Clone)]
struct ActiveTurnOwner {
    project_id: ProjectId,
    acknowledged_turn: Option<TurnId>,
    harness_thread_id: String,
    mode: Mode,
    provider: String,
    model: String,
    context_kind: &'static str,
    reserved_at: Instant,
}

impl ActiveTurnOwner {
    fn new(project_id: ProjectId, handle: &ThreadHandle, ctx: &TurnContext) -> Self {
        Self {
            project_id,
            acknowledged_turn: None,
            harness_thread_id: handle.harness_thread_id.clone(),
            mode: ctx.mode,
            provider: ctx.model.provider.clone(),
            model: ctx.model.model.clone(),
            context_kind: turn_context_kind_label(ctx.kind),
            reserved_at: Instant::now(),
        }
    }
}

impl ThreadTurnGate {
    fn reserve(
        &self,
        thread_id: ThreadId,
        owner: ActiveTurnOwner,
    ) -> Result<ThreadTurnLease, HarnessError> {
        let mut active = self.active_threads();
        if let Some(existing) = active.get(&thread_id) {
            warn!(
                %thread_id,
                owner_project_id = %existing.project_id,
                owner_turn_id = ?existing.acknowledged_turn,
                owner_harness_thread_id = %existing.harness_thread_id,
                owner_context_kind = existing.context_kind,
                owner_mode = ?existing.mode,
                owner_provider = %existing.provider,
                owner_model = %existing.model,
                owner_elapsed_ms = existing.reserved_at.elapsed().as_millis(),
                rejected_project_id = %owner.project_id,
                rejected_context_kind = owner.context_kind,
                rejected_mode = ?owner.mode,
                rejected_provider = %owner.provider,
                rejected_model = %owner.model,
                "rejecting turn start because thread turn gate is already active"
            );
            return Err(HarnessError::ThreadBusy { thread: thread_id });
        }
        debug!(
            %thread_id,
            project_id = %owner.project_id,
            harness_thread_id = %owner.harness_thread_id,
            context_kind = owner.context_kind,
            mode = ?owner.mode,
            provider = %owner.provider,
            model = %owner.model,
            "reserved active thread turn"
        );
        active.insert(thread_id, owner);
        Ok(ThreadTurnLease {
            gate: self.clone(),
            thread_id,
            released: false,
        })
    }

    fn active_threads(&self) -> StdMutexGuard<'_, HashMap<ThreadId, ActiveTurnOwner>> {
        match self.active.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("thread turn gate lock was poisoned; recovering active-turn state");
                poisoned.into_inner()
            }
        }
    }

    fn acknowledge_turn(&self, thread_id: ThreadId, turn_id: TurnId) {
        let mut active = self.active_threads();
        let Some(owner) = active.get_mut(&thread_id) else {
            warn!(
                %thread_id,
                %turn_id,
                "turn acknowledgement observed but no active turn gate owner was registered"
            );
            return;
        };
        owner.acknowledged_turn = Some(turn_id);
        debug!(
            %thread_id,
            %turn_id,
            project_id = %owner.project_id,
            harness_thread_id = %owner.harness_thread_id,
            context_kind = owner.context_kind,
            elapsed_ms = owner.reserved_at.elapsed().as_millis(),
            "recorded active turn owner"
        );
    }

    fn release(&self, thread_id: ThreadId) -> Option<ActiveTurnOwner> {
        let mut active = self.active_threads();
        let owner = active.remove(&thread_id);
        if let Some(owner) = &owner {
            debug!(
                %thread_id,
                project_id = %owner.project_id,
                turn_id = ?owner.acknowledged_turn,
                harness_thread_id = %owner.harness_thread_id,
                context_kind = owner.context_kind,
                mode = ?owner.mode,
                provider = %owner.provider,
                model = %owner.model,
                elapsed_ms = owner.reserved_at.elapsed().as_millis(),
                "released active thread turn"
            );
        } else {
            warn!(
                %thread_id,
                "active thread turn release requested but no owner was registered"
            );
        }
        owner
    }

    fn is_active(&self, thread_id: ThreadId) -> bool {
        self.active_threads().contains_key(&thread_id)
    }
}

struct ThreadTurnLease {
    gate: ThreadTurnGate,
    thread_id: ThreadId,
    released: bool,
}

impl ThreadTurnLease {
    fn acknowledge_turn(&mut self, turn_id: TurnId) {
        if self.released {
            warn!(
                thread_id = %self.thread_id,
                %turn_id,
                "attempted to acknowledge turn after active turn gate was released"
            );
            return;
        }
        self.gate.acknowledge_turn(self.thread_id, turn_id);
    }

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

        let ctx = TurnContext {
            user_input: input.clone(),
            model: effective_model,
            mode: overrides.mode,
            kind: TurnContextKind::User,
        };
        let request_started = Instant::now();
        let mut turn_gate = self
            .turn_gate
            .reserve(thread_id, ActiveTurnOwner::new(project_id, &handle, &ctx))?;

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
                turn_gate.acknowledge_turn(turn_id);
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
    ) -> Result<ThreadId, HarnessError> {
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
        harness.respond_approval(request_id, decision).await?;
        Ok(thread_id)
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
        let started = Instant::now();
        info!(
            %project_id,
            %thread_id,
            harness_thread_id = %handle.harness_thread_id,
            "sending interrupt request to harness"
        );
        let result = harness.interrupt(&handle).await;
        match &result {
            Ok(()) => info!(
                %project_id,
                %thread_id,
                harness_thread_id = %handle.harness_thread_id,
                elapsed_ms = started.elapsed().as_millis(),
                "harness interrupt request completed"
            ),
            Err(error) => warn!(
                %project_id,
                %thread_id,
                harness_thread_id = %handle.harness_thread_id,
                elapsed_ms = started.elapsed().as_millis(),
                %error,
                "harness interrupt request failed"
            ),
        }
        result
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
        let ctx = TurnContext {
            user_input: UserInput::text("/compact"),
            model: effective_model,
            mode,
            kind: TurnContextKind::ManualCompaction,
        };
        let turn_gate = self
            .turn_gate
            .reserve(thread_id, ActiveTurnOwner::new(project_id, &handle, &ctx))?;

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
        let started = Instant::now();
        info!(
            %project_id,
            %thread_id,
            harness_thread_id = %handle.harness_thread_id,
            process_id = %process_id,
            "sending terminate command request to harness"
        );
        let result = harness.terminate_command(&handle, &process_id).await;
        match &result {
            Ok(()) => info!(
                %project_id,
                %thread_id,
                harness_thread_id = %handle.harness_thread_id,
                process_id = %process_id,
                elapsed_ms = started.elapsed().as_millis(),
                "harness terminate command request completed"
            ),
            Err(error) => warn!(
                %project_id,
                %thread_id,
                harness_thread_id = %handle.harness_thread_id,
                process_id = %process_id,
                elapsed_ms = started.elapsed().as_millis(),
                %error,
                "harness terminate command request failed"
            ),
        }
        result
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

    /// List the models the project's harness advertises (e.g. Codex's `model/list` catalog). Used to
    /// overlay friendly display names onto the configured model list.
    pub async fn list_models(
        &self,
        config: &ProjectConfig,
    ) -> Result<Vec<ModelDescriptor>, HarnessError> {
        let harness = self.get_or_create_harness(config.id, config).await?;
        harness.list_models().await
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

    pub async fn delete_project(&self, project_id: ProjectId) -> Result<(), HarnessError> {
        let harness = self.harnesses.lock().await.get(&project_id).cloned();
        if let Some(harness) = harness {
            harness.shutdown().await?;
            self.harnesses.lock().await.remove(&project_id);
        }

        let thread_ids = {
            let mut threads = self.threads.lock().await;
            let thread_ids = threads
                .iter()
                .filter_map(|(thread_id, binding)| {
                    (binding.project == project_id).then_some(*thread_id)
                })
                .collect::<HashSet<_>>();
            threads.retain(|_, binding| binding.project != project_id);
            thread_ids
        };

        if !thread_ids.is_empty() {
            let mut approvals = self.approvals.lock().await;
            approvals.retain(|_, thread_id| !thread_ids.contains(thread_id));

            let mut server_requests = self.server_requests.lock().await;
            server_requests.retain(|_, thread_id| !thread_ids.contains(thread_id));
        }

        Ok(())
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
    let mut current_turn_items = CurrentTurnItems::default();
    let mut diffs: Vec<giskard_core::FileDiff> = Vec::new();
    let mut seen_turn_ids = persisted_turn_ids(&store, project_id, thread_id).await;
    let mut seen_notices = HashSet::new();
    let mut item_ids_by_harness: HashMap<(TurnId, String), ItemId> = HashMap::new();
    let forwarder_started = Instant::now();
    let mut saw_context_compaction_marker = false;
    let mut stream_error: Option<String> = None;
    debug!(
        %project_id,
        %thread_id,
        context_kind = turn_context_kind_label(ctx.kind),
        mode = ?ctx.mode,
        provider = %ctx.model.provider,
        model = %ctx.model.model,
        turn_gate_held = turn_gate.as_ref().is_some_and(|lease| !lease.is_released()),
        persisted_turn_count = seen_turn_ids.len(),
        "event forwarder started"
    );

    let exit_reason = loop {
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

                if let Some((event_turn, harness_item_id, existing_item_id, conflicting_item_id)) =
                    track_item_identity(&mut item_ids_by_harness, &event)
                {
                    error!(
                        %project_id,
                        %thread_id,
                        turn_id = %event_turn,
                        event_kind = event_kind(&event),
                        harness_item_id,
                        existing_item_id = %existing_item_id,
                        conflicting_item_id = %conflicting_item_id,
                        "dropping harness event because a native item id remapped to a different Giskard item id"
                    );
                    continue;
                }

                let event_turn = event_turn_id(&event);
                if let Some(owned) = owned_turn {
                    if let Some(turn) = event_turn {
                        if turn != owned {
                            if !owned_turn_completed {
                                warn!(
                                    %project_id,
                                    %thread_id,
                                    owned_turn = %owned,
                                    event_turn = %turn,
                                    event_kind = event_kind(&event),
                                    elapsed_ms = forwarder_started.elapsed().as_millis(),
                                    "dropping harness event for a different turn on the same thread"
                                );
                            }
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
                            apply_seen_turn_running_command_event(&running_commands, &event).await;
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
                                    break ForwarderExitReason::AfterTurnCommandsDrained;
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
                            broadcast_thread_activity(&hub, thread_id, &event, false).await;
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
                            broadcast_thread_activity(&hub, thread_id, &event, true).await;
                            hub.broadcast_event(thread_id, event.clone()).await;
                        }
                        AgentEvent::ServerRequestReceived { request, .. } => {
                            warn!(
                                %project_id,
                                %thread_id,
                                request_id = %request.id,
                                method = %request.method,
                                context_kind = turn_context_kind_label(ctx.kind),
                                turn_gate_held = turn_gate
                                    .as_ref()
                                    .is_some_and(|lease| !lease.is_released()),
                                elapsed_ms = forwarder_started.elapsed().as_millis(),
                                "turnless server request received before turn ownership"
                            );
                            server_requests
                                .lock()
                                .await
                                .insert(request.id.clone(), thread_id);
                            broadcast_thread_activity(&hub, thread_id, &event, true).await;
                            hub.broadcast_event(thread_id, event.clone()).await;
                        }
                        _ => {}
                    }
                    continue;
                }

                let command_state_changed =
                    apply_running_command_event(&running_commands, &event).await;

                match &event {
                    AgentEvent::TurnStarted { turn, .. } => {
                        turn_id = Some(*turn);
                        started_at = Utc::now();
                        current_turn_items.rebuild_indexes();
                        if let Some(turn_gate) = turn_gate.as_mut() {
                            turn_gate.acknowledge_turn(*turn);
                        }
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
                                items_buffered_after = current_turn_items.len() + 1,
                                elapsed_ms = forwarder_started.elapsed().as_millis(),
                                "context compaction marker received"
                            );
                        }
                        if !owned_turn_completed && current_turn_items.upsert(item) {
                            error!(
                                %project_id,
                                %thread_id,
                                %turn,
                                item_id = %item.id,
                                harness_item_id = %item.harness_item_id,
                                "recovered stale current-turn item index"
                            );
                        }
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
                    info!(
                        %project_id,
                        %thread_id,
                        turn = %completed_turn,
                        started_turn = ?turn_id,
                        status = ?status.kind,
                        context_kind = turn_context_kind_label(ctx.kind),
                        items_buffered = current_turn_items.len(),
                        diffs_buffered = diffs.len(),
                        elapsed_ms = forwarder_started.elapsed().as_millis(),
                        "turn completion event received"
                    );
                    if ctx.kind == TurnContextKind::ManualCompaction {
                        info!(
                            %project_id,
                            %thread_id,
                            turn = %completed_turn,
                            status = ?status.kind,
                            items_buffered = current_turn_items.len(),
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
                        &mut current_turn_items,
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
                    broadcast_thread_activity(&hub, thread_id, &event, false).await;
                    hub.broadcast_event(thread_id, event).await;
                    if command_state_changed {
                        broadcast_running_commands(&hub, &running_commands, thread_id).await;
                    }
                    if running_commands.has_running_for_turn(thread_id, tid).await {
                        info!(
                            %project_id,
                            %thread_id,
                            turn = %tid,
                            elapsed_ms = forwarder_started.elapsed().as_millis(),
                            "event forwarder monitoring after-turn running commands"
                        );
                        continue;
                    }
                    break ForwarderExitReason::NormalTurnCompleted;
                }

                broadcast_thread_activity(&hub, thread_id, &event, true).await;
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
                        items_buffered = current_turn_items.len(),
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
                        &mut current_turn_items,
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
                    broadcast_thread_activity(&hub, thread_id, &completion_event, false).await;
                    hub.broadcast_event(thread_id, completion_event).await;
                    if running_commands.has_running_for_turn(thread_id, tid).await {
                        info!(
                            %project_id,
                            %thread_id,
                            turn = %tid,
                            elapsed_ms = forwarder_started.elapsed().as_millis(),
                            "event forwarder monitoring after-turn running commands"
                        );
                        continue;
                    }
                    break ForwarderExitReason::SyntheticCompactionCompleted;
                }
            }
            Err(e) => {
                stream_error = Some(e.to_string());
                if ctx.kind == TurnContextKind::ManualCompaction && !owned_turn_completed {
                    let live_buffer_active = live_buffers.is_active(thread_id).await;
                    warn!(
                        %project_id,
                        %thread_id,
                        ?e,
                        ?owned_turn,
                        ?turn_id,
                        saw_context_compaction_marker,
                        items_buffered = current_turn_items.len(),
                        live_buffer_active,
                        turn_gate_held = turn_gate.is_some(),
                        elapsed_ms = forwarder_started.elapsed().as_millis(),
                        "context compaction event stream ended before completion"
                    );
                } else {
                    debug!(%thread_id, ?e, "event stream ended");
                }
                if let Some(incomplete_turn) = turn_id.or(owned_turn) {
                    let live_buffer_active = live_buffers.is_active(thread_id).await;
                    let turn_gate_held =
                        turn_gate.as_ref().is_some_and(|lease| !lease.is_released());
                    let status = TurnStatus {
                        kind: TurnStatusKind::Interrupted,
                        message: Some("Harness event stream ended before turn completion".into()),
                    };
                    warn!(
                        %project_id,
                        %thread_id,
                        turn = %incomplete_turn,
                        context_kind = turn_context_kind_label(ctx.kind),
                        mode = ?ctx.mode,
                        provider = %ctx.model.provider,
                        model = %ctx.model.model,
                        ?owned_turn,
                        ?turn_id,
                        stream_error = ?stream_error,
                        items_buffered = current_turn_items.len(),
                        diffs_buffered = diffs.len(),
                        live_buffer_active,
                        turn_gate_held,
                        elapsed_ms = forwarder_started.elapsed().as_millis(),
                        "persisting incomplete turn after event stream ended"
                    );
                    let completion_event = AgentEvent::TurnCompleted {
                        thread: thread_id,
                        turn: incomplete_turn,
                        usage: giskard_core::token::TokenUsage::default(),
                        status: status.clone(),
                    };
                    let command_state_changed =
                        apply_running_command_event(&running_commands, &completion_event).await;
                    if live_buffer_active {
                        live_buffers
                            .append(thread_id, completion_event.clone())
                            .await;
                    }
                    complete_forwarded_turn(
                        thread_id,
                        project_id,
                        incomplete_turn,
                        giskard_core::token::TokenUsage::default(),
                        status,
                        &ctx,
                        &mut current_turn_items,
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
                    broadcast_thread_activity(&hub, thread_id, &completion_event, false).await;
                    hub.broadcast_event(thread_id, completion_event).await;
                    if command_state_changed {
                        broadcast_running_commands(&hub, &running_commands, thread_id).await;
                    }
                    break ForwarderExitReason::StreamEndedRecovered;
                } else {
                    break ForwarderExitReason::StreamEndedWithoutTurn;
                }
            }
        }
    };
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
            exit_reason = forwarder_exit_reason_label(exit_reason),
            stream_error = ?stream_error,
            items_buffered = current_turn_items.len(),
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
            exit_reason = forwarder_exit_reason_label(exit_reason),
            stream_error = ?stream_error,
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
    current_turn_items: &mut CurrentTurnItems,
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
    let item_count = current_turn_items.len();
    let diff_count = diffs.len();
    let has_context_compaction_marker = current_turn_items.iter().any(is_context_compaction_item);
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
        items: current_turn_items.take(),
        model: ctx.model.clone(),
        mode: ctx.mode,
        status: status.clone(),
        usage,
        diffs: std::mem::take(diffs),
        started_at,
        completed_at: Some(Utc::now()),
    };
    let persist_outcome = persist_turn(store, hub, ledger, project_id, thread_id, turn).await;
    if ctx.kind == TurnContextKind::ManualCompaction {
        info!(
            %project_id,
            %thread_id,
            turn = %tid,
            item_count,
            has_context_compaction_marker,
            history_appended = persist_outcome.history_appended,
            metadata_updated = persist_outcome.metadata_updated,
            "context compaction persistence path finished"
        );
    }
    live_buffers.clear_turn(thread_id).await;
    if let Some(turn_gate) = turn_gate {
        turn_gate.release();
    }
    info!(
        %project_id,
        %thread_id,
        turn = %tid,
        completed_turn = %completed_turn,
        status = ?status.kind,
        context_kind = turn_context_kind_label(ctx.kind),
        item_count,
        diff_count,
        history_appended = persist_outcome.history_appended,
        metadata_updated = persist_outcome.metadata_updated,
        "completed turn cleanup finished"
    );
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

fn event_item_identity(event: &AgentEvent) -> Option<(TurnId, &str, ItemId)> {
    match event {
        AgentEvent::ItemStarted { turn, item, .. } if !item.harness_item_id.is_empty() => {
            Some((*turn, item.harness_item_id.as_str(), item.id))
        }
        AgentEvent::ItemCompleted { turn, item, .. } if !item.harness_item_id.is_empty() => {
            Some((*turn, item.harness_item_id.as_str(), item.id))
        }
        _ => None,
    }
}

fn track_item_identity(
    item_ids_by_harness: &mut HashMap<(TurnId, String), ItemId>,
    event: &AgentEvent,
) -> Option<(TurnId, String, ItemId, ItemId)> {
    let (turn, harness_item_id, item_id) = event_item_identity(event)?;
    let identity_key = (turn, harness_item_id.to_owned());
    match item_ids_by_harness.get(&identity_key) {
        Some(existing_item_id) if *existing_item_id != item_id => {
            Some((turn, harness_item_id.to_owned(), *existing_item_id, item_id))
        }
        Some(_) => None,
        None => {
            item_ids_by_harness.insert(identity_key, item_id);
            None
        }
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

async fn broadcast_thread_activity(
    hub: &Hub,
    thread_id: ThreadId,
    event: &AgentEvent,
    fallback_active_turn: bool,
) {
    let Some(activity) = thread_activity_from_event(thread_id, event, fallback_active_turn) else {
        return;
    };
    hub.broadcast_all(ServerMessage::ThreadActivity(activity))
        .await;
}

fn thread_activity_from_event(
    thread_id: ThreadId,
    event: &AgentEvent,
    fallback_active_turn: bool,
) -> Option<ThreadActivity> {
    let mut activity = ThreadActivity {
        thread_id,
        kind: ThreadActivityKind::Progress,
        active_turn: fallback_active_turn,
        summary: None,
    };

    match event {
        AgentEvent::ThreadOpened { .. } => return None,
        AgentEvent::TurnStarted { .. } => {
            activity.kind = ThreadActivityKind::TurnStarted;
            activity.active_turn = true;
            activity.summary = Some("Turn started".into());
        }
        AgentEvent::ItemStarted { item, .. } => {
            activity.active_turn = true;
            activity.summary = Some(match &item.kind {
                giskard_core::item::ItemKind::CommandExecution => item
                    .command
                    .as_ref()
                    .map(|cmd| format!("Running {}", cmd.command))
                    .unwrap_or_else(|| "Command started".into()),
                giskard_core::item::ItemKind::ToolCall => item
                    .tool
                    .as_ref()
                    .map(|tool| format!("Tool {}", tool.name))
                    .unwrap_or_else(|| "Tool started".into()),
                giskard_core::item::ItemKind::FileChange => "File change started".into(),
                giskard_core::item::ItemKind::Activity => "Activity started".into(),
                giskard_core::item::ItemKind::Reasoning => "Reasoning".into(),
                giskard_core::item::ItemKind::AgentMessage => "Agent message".into(),
                giskard_core::item::ItemKind::UserMessage => "User message".into(),
            });
        }
        AgentEvent::ItemDelta { .. } => return None,
        AgentEvent::ItemCompleted { item, .. } => {
            activity.active_turn = true;
            activity.summary = Some(match &item.payload {
                ItemPayload::CommandExecution { command, .. } => {
                    format!("Command finished {command}")
                }
                ItemPayload::ToolCall { name, .. } => format!("Tool finished {name}"),
                ItemPayload::FileChange { path, .. } => {
                    format!("Changed {}", path.to_string_lossy())
                }
                ItemPayload::Activity { title, .. } => title.clone(),
                ItemPayload::AgentMessage { .. } => "Agent replied".into(),
                ItemPayload::Reasoning { .. } => "Reasoning updated".into(),
                ItemPayload::UserMessage { .. } => "User message recorded".into(),
            });
        }
        AgentEvent::DiffUpdated { diff, .. } => {
            activity.active_turn = true;
            activity.summary = Some(format!("Diff updated {}", diff.path.to_string_lossy()));
        }
        AgentEvent::ApprovalRequested { request, .. } => {
            activity.kind = ThreadActivityKind::ApprovalRequested {
                approval_id: request.id.to_string(),
            };
            activity.active_turn = true;
            activity.summary = Some("Approval requested".into());
        }
        AgentEvent::ServerRequestReceived { request, .. } => {
            activity.kind = ThreadActivityKind::ServerRequestReceived {
                server_request_id: request.id.to_string(),
            };
            activity.active_turn = true;
            activity.summary = Some(format!("{} request", request.method));
        }
        AgentEvent::ServerRequestResolved { .. } => {
            activity.summary = Some("Request resolved".into());
        }
        AgentEvent::TurnCompleted { status, .. } => {
            activity.kind = ThreadActivityKind::TurnCompleted;
            activity.active_turn = false;
            activity.summary = status
                .message
                .clone()
                .or_else(|| Some("Turn completed".into()));
        }
        AgentEvent::Error { error, .. } => {
            activity.kind = ThreadActivityKind::Error;
            activity.active_turn = false;
            activity.summary = Some(error.to_string());
        }
        AgentEvent::Notice { message, .. } => {
            activity.kind = ThreadActivityKind::Notice;
            activity.summary = Some(message.clone());
        }
    }

    Some(activity)
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

async fn apply_seen_turn_running_command_event(
    running_commands: &RunningTaskStore,
    event: &AgentEvent,
) -> bool {
    if !is_terminal_command_completion(event) {
        log_ignored_seen_turn_running_task_start(event);
        return false;
    }
    apply_running_command_event(running_commands, event).await
}

fn log_ignored_seen_turn_running_task_start(event: &AgentEvent) {
    let AgentEvent::ItemStarted { thread, turn, item } = event else {
        return;
    };
    let Some(command) = &item.command else {
        return;
    };
    let status = command.status.as_deref().unwrap_or("in_progress");
    if !command_status_is_running(status) {
        return;
    }
    warn!(
        thread_id = %thread,
        turn_id = %turn,
        item_id = %item.id,
        harness_item_id = %item.harness_item_id,
        process_id = ?command.process_id,
        command = %command.command,
        status,
        "ignoring running command start for already-persisted turn"
    );
}

async fn terminating_command_before_terminal_completion(
    running_commands: &RunningTaskStore,
    event: &AgentEvent,
) -> Option<RunningTask> {
    let AgentEvent::ItemCompleted { thread, turn, item } = event else {
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

    let command = running_commands
        .get_by_item(*thread, *turn, item.id)
        .await?;
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

/// Owns the current turn's completed items and their authoritative `ItemId` index.
/// Keeping both in one type ensures draining a completed turn cannot leave indexes pointing into
/// the previous vector. Native item ids are validated separately and are never used to re-key an
/// item whose Giskard identity is already known.
#[derive(Default)]
struct CurrentTurnItems {
    items: Vec<Item>,
    indexes: HashMap<ItemId, usize>,
}

impl CurrentTurnItems {
    fn len(&self) -> usize {
        self.items.len()
    }

    fn iter(&self) -> impl Iterator<Item = &Item> {
        self.items.iter()
    }

    fn rebuild_indexes(&mut self) {
        self.indexes.clear();
        for (idx, item) in self.items.iter().enumerate() {
            self.indexes.insert(item.id, idx);
        }
    }

    /// Returns true when an inconsistent stale index was detected and repaired.
    fn upsert(&mut self, item: &Item) -> bool {
        if let Some(&idx) = self.indexes.get(&item.id) {
            if let Some(existing) = self
                .items
                .get_mut(idx)
                .filter(|existing| existing.id == item.id)
            {
                *existing = item.clone();
                return false;
            }
            self.rebuild_indexes();
            if let Some(&repaired_idx) = self.indexes.get(&item.id) {
                self.items[repaired_idx] = item.clone();
                return true;
            }
            self.append_indexed(item);
            return true;
        }
        self.append_indexed(item);
        false
    }

    fn append_indexed(&mut self, item: &Item) {
        let idx = self.items.len();
        self.items.push(item.clone());
        self.indexes.insert(item.id, idx);
    }

    fn take(&mut self) -> Vec<Item> {
        self.indexes.clear();
        std::mem::take(&mut self.items)
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

/// Append a completed `Turn` to the thread file, fold its usage into the thread ledger, recompute
/// the cached context window, persist atomically (§7.1), and hand the usage delta to the global +
/// project ledger actor (§10.2). Best-effort: logs on failure.
#[derive(Clone, Copy, Debug, Default)]
struct PersistTurnOutcome {
    history_appended: bool,
    metadata_updated: bool,
}

async fn persist_turn(
    store: &PersistStore,
    hub: &Hub,
    ledger: &LedgerHandle,
    project_id: ProjectId,
    thread_id: ThreadId,
    turn: Turn,
) -> PersistTurnOutcome {
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
    let turn_id = turn.id;
    let item_count = turn.items.len();
    let diff_count = turn.diffs.len();
    let status_kind = turn.status.kind;
    let started_at = turn.started_at;
    let completed_at = turn.completed_at;

    // H3 ordering: append the turn to the authoritative JSONL history FIRST, then update the
    // metadata aggregates. A crash between the two leaves the turn in history but not yet in the
    // aggregates cache — recoverable via `recompute_aggregates`.
    if let Err(e) = store.append_turn(project_id, thread_id, &turn).await {
        warn!(
            %project_id,
            %thread_id,
            turn = %turn_id,
            status = ?status_kind,
            item_count,
            diff_count,
            %e,
            "failed to append turn to history; skipping metadata update"
        );
        return PersistTurnOutcome::default();
    }
    info!(
        %project_id,
        %thread_id,
        turn = %turn_id,
        status = ?status_kind,
        item_count,
        diff_count,
        started_at = %started_at,
        completed_at = ?completed_at,
        "appended completed turn to history"
    );

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
            warn!(
                %project_id,
                %thread_id,
                turn = %turn_id,
                "thread file missing on turn completion after history append"
            );
            return PersistTurnOutcome {
                history_appended: true,
                metadata_updated: false,
            };
        }
        Err(e) => {
            warn!(
                %project_id,
                %thread_id,
                turn = %turn_id,
                %e,
                "failed to persist thread metadata on turn completion after history append"
            );
            return PersistTurnOutcome {
                history_appended: true,
                metadata_updated: false,
            };
        }
    };
    info!(
        %project_id,
        %thread_id,
        turn = %turn_id,
        status = ?status_kind,
        should_record_usage = should_record,
        "updated thread metadata for completed turn"
    );

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

    PersistTurnOutcome {
        history_appended: true,
        metadata_updated: true,
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
    use giskard_core::turn::{ApprovalPolicy, Mode, Turn, TurnStatus, TurnStatusKind};
    use giskard_core::user_input::UserInput;
    use giskard_harness::{AgentEventStream, ThreadHandle};
    use giskard_persist::PersistStore;
    use giskard_persist::store::ThreadFile;
    use giskard_proto::{ServerMessage, ThreadActivityKind, WireAgentEvent};
    use tokio::sync::{Mutex, broadcast, mpsc};
    use tokio::task::JoinHandle;

    use super::{
        ActiveTurnOwner, CurrentTurnItems, ThreadTurnGate, TurnContext, TurnContextKind,
        command_completion_is_normal_success, command_status_is_running, forward_events,
        thread_activity_from_event, track_item_identity,
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
    fn current_turn_items_take_clears_indexes_for_reused_item_id() {
        let item_id = ItemId::new();
        let mut buffer = CurrentTurnItems::default();
        let first = Item {
            id: item_id,
            harness_item_id: "native_first".into(),
            payload: ItemPayload::AgentMessage {
                text: "first".into(),
            },
            created_at: Utc::now(),
        };
        assert!(!buffer.upsert(&first));
        assert_eq!(buffer.take(), vec![first]);
        assert!(buffer.indexes.is_empty());

        let second = Item {
            id: item_id,
            harness_item_id: "native_second".into(),
            payload: ItemPayload::AgentMessage {
                text: "second".into(),
            },
            created_at: Utc::now(),
        };
        assert!(!buffer.upsert(&second));
        assert_eq!(buffer.take(), vec![second]);
    }

    #[test]
    fn current_turn_items_repairs_stale_index_without_panicking() {
        let mut buffer = CurrentTurnItems::default();
        let item_id = ItemId::new();
        buffer.indexes.insert(item_id, 7);
        let item = Item {
            id: item_id,
            harness_item_id: "stale_item".into(),
            payload: ItemPayload::AgentMessage {
                text: "recovered".into(),
            },
            created_at: Utc::now(),
        };

        assert!(buffer.upsert(&item));
        assert_eq!(buffer.items, vec![item]);
        assert_eq!(buffer.indexes.get(&item_id), Some(&0));
    }

    #[test]
    fn current_turn_items_repairs_in_range_stale_index() {
        let first_id = ItemId::new();
        let second_id = ItemId::new();
        let first = Item {
            id: first_id,
            harness_item_id: "first".into(),
            payload: ItemPayload::AgentMessage {
                text: "first".into(),
            },
            created_at: Utc::now(),
        };
        let second = Item {
            id: second_id,
            harness_item_id: "second".into(),
            payload: ItemPayload::AgentMessage {
                text: "second".into(),
            },
            created_at: Utc::now(),
        };
        let replacement = Item {
            payload: ItemPayload::AgentMessage {
                text: "updated second".into(),
            },
            ..second.clone()
        };
        let mut buffer = CurrentTurnItems::default();
        assert!(!buffer.upsert(&first));
        assert!(!buffer.upsert(&second));
        buffer.indexes.insert(second_id, 0);

        assert!(buffer.upsert(&replacement));
        assert_eq!(buffer.items, vec![first, replacement]);
        assert_eq!(buffer.indexes.get(&first_id), Some(&0));
        assert_eq!(buffer.indexes.get(&second_id), Some(&1));
    }

    #[test]
    fn current_turn_items_upserts_empty_native_id_by_item_id() {
        let item_id = ItemId::new();
        let mut buffer = CurrentTurnItems::default();
        let first = Item {
            id: item_id,
            harness_item_id: String::new(),
            payload: ItemPayload::AgentMessage {
                text: "partial".into(),
            },
            created_at: Utc::now(),
        };
        let completed = Item {
            payload: ItemPayload::AgentMessage {
                text: "complete".into(),
            },
            ..first.clone()
        };

        assert!(!buffer.upsert(&first));
        assert!(!buffer.upsert(&completed));
        assert_eq!(buffer.items, vec![completed]);
    }

    #[test]
    fn command_status_running_accepts_codex_variants() {
        assert!(command_status_is_running("in_progress"));
        assert!(command_status_is_running("in-progress"));
        assert!(command_status_is_running("running"));

        assert!(!command_status_is_running("completed"));
        assert!(!command_status_is_running("interrupted"));
    }

    #[test]
    fn item_identity_tracking_rejects_native_id_remapping_within_a_turn() {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let original_item = ItemId::new();
        let conflicting_item = ItemId::new();
        let mut identities = Default::default();

        let started = AgentEvent::ItemStarted {
            thread,
            turn,
            item: ItemStart {
                id: original_item,
                harness_item_id: "cmd_1".into(),
                kind: ItemKind::CommandExecution,
                command: None,
                tool: None,
            },
        };
        assert!(track_item_identity(&mut identities, &started).is_none());

        let repeated = AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: original_item,
                harness_item_id: "cmd_1".into(),
                payload: ItemPayload::AgentMessage {
                    text: "same identity".into(),
                },
                created_at: Utc::now(),
            },
        };
        assert!(track_item_identity(&mut identities, &repeated).is_none());

        let conflicting = AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: conflicting_item,
                harness_item_id: "cmd_1".into(),
                payload: ItemPayload::AgentMessage {
                    text: "different identity".into(),
                },
                created_at: Utc::now(),
            },
        };
        assert_eq!(
            track_item_identity(&mut identities, &conflicting),
            Some((turn, "cmd_1".into(), original_item, conflicting_item))
        );
    }

    #[test]
    fn thread_activity_mapper_covers_request_and_terminal_events() {
        let thread_id = ThreadId::new();
        let turn_id = TurnId::new();
        let approval_id = ApprovalId("approval_1".into());
        let server_request_id = ServerRequestId("server_request_1".into());

        let approval = thread_activity_from_event(
            thread_id,
            &AgentEvent::ApprovalRequested {
                thread: thread_id,
                turn: turn_id,
                request: ApprovalRequest {
                    id: approval_id.clone(),
                    kind: ApprovalKind::Permission {
                        detail: "network".into(),
                    },
                    reason: Some("needs network".into()),
                    metadata: Vec::new(),
                    available: vec![ApprovalDecision::Accept, ApprovalDecision::Decline],
                },
            },
            true,
        )
        .expect("approval should map to thread activity");
        assert!(approval.active_turn);
        match approval.kind {
            ThreadActivityKind::ApprovalRequested { approval_id: id } => {
                assert_eq!(id, approval_id.0);
            }
            other => panic!("expected approval activity, got {other:?}"),
        }

        let request = thread_activity_from_event(
            thread_id,
            &AgentEvent::ServerRequestReceived {
                thread: thread_id,
                turn: Some(turn_id),
                request: ServerRequest {
                    id: server_request_id.clone(),
                    method: "item/tool/requestUserInput".into(),
                    params: serde_json::json!({ "question": "Continue?" }),
                    received_at: Utc::now(),
                },
            },
            true,
        )
        .expect("server request should map to thread activity");
        assert!(request.active_turn);
        match request.kind {
            ThreadActivityKind::ServerRequestReceived {
                server_request_id: id,
            } => {
                assert_eq!(id, server_request_id.0);
            }
            other => panic!("expected server request activity, got {other:?}"),
        }

        let completed = thread_activity_from_event(
            thread_id,
            &AgentEvent::TurnCompleted {
                thread: thread_id,
                turn: turn_id,
                usage: TokenUsage::default(),
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: Some("done".into()),
                },
            },
            true,
        )
        .expect("turn completion should map to thread activity");
        assert_eq!(completed.kind, ThreadActivityKind::TurnCompleted);
        assert!(!completed.active_turn);
        assert_eq!(completed.summary.as_deref(), Some("done"));

        let error = thread_activity_from_event(
            thread_id,
            &AgentEvent::Error {
                thread: thread_id,
                turn: Some(turn_id),
                error: HarnessError::Protocol("bad frame".into()),
            },
            true,
        )
        .expect("errors should map to thread activity");
        assert_eq!(error.kind, ThreadActivityKind::Error);
        assert!(!error.active_turn);
        assert!(
            error
                .summary
                .as_deref()
                .is_some_and(|summary| summary.contains("bad frame"))
        );
    }

    #[test]
    fn thread_activity_mapper_skips_high_volume_deltas() {
        let thread_id = ThreadId::new();
        let turn_id = TurnId::new();
        let item_id = ItemId::new();
        let activity = thread_activity_from_event(
            thread_id,
            &AgentEvent::ItemDelta {
                thread: thread_id,
                turn: turn_id,
                item_id,
                delta: giskard_core::item::ItemDelta::Text {
                    text: "streaming".into(),
                },
            },
            true,
        );
        assert!(
            activity.is_none(),
            "text/output deltas should not become cross-thread activity"
        );
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
    async fn completed_turn_forwarder_exits_after_after_turn_command_completion() {
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
        let live_buffers = Arc::new(LiveBufferStore::new());
        let running_commands = Arc::new(RunningTaskStore::new());
        let approvals = Arc::new(Mutex::new(Default::default()));
        let server_requests = Arc::new(Mutex::new(Default::default()));
        let ledger = ledger::spawn(store.clone());
        let handle = spawn_forwarder_handle(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            live_buffers,
            running_commands.clone(),
            store.clone(),
            approvals,
            server_requests,
            ledger,
            model,
            "first",
        );

        let turn = TurnId::new();
        let command_item = ItemId::new();
        tx.send(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        })
        .unwrap();
        tx.send(AgentEvent::ItemStarted {
            thread: thread_id,
            turn,
            item: ItemStart {
                id: command_item,
                harness_item_id: "long_running_command".into(),
                kind: ItemKind::CommandExecution,
                command: Some(CommandExecutionStart {
                    command: "sleep 600".into(),
                    cwd: "/tmp/test".into(),
                    status: Some("running".into()),
                    process_id: Some("proc_after_turn".into()),
                    started_at_ms: Some(1),
                }),
                tool: None,
            },
        })
        .unwrap();
        tx.send(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn,
            item: Item {
                id: command_item,
                harness_item_id: "long_running_command".into(),
                payload: ItemPayload::CommandExecution {
                    command: "sleep 600".into(),
                    cwd: "/tmp/test".into(),
                    output: "still running".into(),
                    exit_code: None,
                    status: Some("running".into()),
                    process_id: Some("proc_after_turn".into()),
                    duration_ms: None,
                },
                created_at: Utc::now(),
            },
        })
        .unwrap();
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

        wait_for_turn_count(&store, project_id, thread_id, 1).await;
        let tasks = running_commands.snapshot(thread_id).await;
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].after_turn);

        tx.send(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn,
            item: Item {
                id: command_item,
                harness_item_id: "long_running_command".into(),
                payload: ItemPayload::CommandExecution {
                    command: "sleep 600".into(),
                    cwd: "/tmp/test".into(),
                    output: "done".into(),
                    exit_code: Some(0),
                    status: Some("completed".into()),
                    process_id: Some("proc_after_turn".into()),
                    duration_ms: Some(60_000),
                },
                created_at: Utc::now(),
            },
        })
        .unwrap();

        tokio::time::timeout(tokio::time::Duration::from_secs(2), handle)
            .await
            .expect("forwarder should exit after after-turn command completion")
            .unwrap();

        assert!(running_commands.snapshot(thread_id).await.is_empty());
    }

    #[tokio::test]
    async fn stream_end_before_completion_persists_interrupted_turn() {
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
        let live_buffers = Arc::new(LiveBufferStore::new());
        let running_commands = Arc::new(RunningTaskStore::new());
        let approvals = Arc::new(Mutex::new(Default::default()));
        let server_requests = Arc::new(Mutex::new(Default::default()));
        let ledger = ledger::spawn(store.clone());
        let handle = spawn_forwarder_handle(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            live_buffers.clone(),
            running_commands.clone(),
            store.clone(),
            approvals,
            server_requests,
            ledger,
            model,
            "incomplete",
        );

        let turn = TurnId::new();
        tx.send(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        })
        .unwrap();
        tx.send(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn,
            item: Item {
                id: ItemId::new(),
                harness_item_id: "agent_partial".into(),
                payload: ItemPayload::AgentMessage {
                    text: "partial answer".into(),
                },
                created_at: Utc::now(),
            },
        })
        .unwrap();
        tx.send(AgentEvent::ItemStarted {
            thread: thread_id,
            turn,
            item: ItemStart {
                id: ItemId::new(),
                harness_item_id: "partial_command".into(),
                kind: ItemKind::CommandExecution,
                command: Some(CommandExecutionStart {
                    command: "sleep 600".into(),
                    cwd: "/tmp/test".into(),
                    status: Some("running".into()),
                    process_id: Some("proc_partial".into()),
                    started_at_ms: Some(1),
                }),
                tool: None,
            },
        })
        .unwrap();
        drop(tx);

        tokio::time::timeout(tokio::time::Duration::from_secs(2), handle)
            .await
            .expect("forwarder should exit when stream closes")
            .unwrap();

        let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].id, turn);
        assert!(matches!(saved[0].status.kind, TurnStatusKind::Interrupted));
        assert_eq!(saved[0].items.len(), 1);
        assert!(
            live_buffers.snapshot(thread_id).await.is_none(),
            "synthetic completion should clear live state"
        );

        let tasks = running_commands.snapshot(thread_id).await;
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].after_turn);
    }

    #[tokio::test]
    async fn persisted_turn_command_starts_do_not_recreate_running_tasks() {
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

        let turn = TurnId::new();
        let item_id = ItemId::new();
        let harness_item_id = "cmd_1".to_string();
        store
            .append_turn(
                project_id,
                thread_id,
                &Turn {
                    id: turn,
                    user_input: UserInput::text("already done"),
                    items: vec![Item {
                        id: item_id,
                        harness_item_id: harness_item_id.clone(),
                        payload: ItemPayload::CommandExecution {
                            command: "sleep 1".into(),
                            cwd: "/tmp/test".into(),
                            output: "done".into(),
                            exit_code: Some(0),
                            status: Some("completed".into()),
                            process_id: Some("proc_1".into()),
                            duration_ms: Some(1_000),
                        },
                        created_at: now,
                    }],
                    model: model.clone(),
                    mode: Mode::Build,
                    status: TurnStatus {
                        kind: TurnStatusKind::Completed,
                        message: None,
                    },
                    usage: TokenUsage::default(),
                    diffs: Vec::new(),
                    started_at: now,
                    completed_at: Some(now),
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(16);
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
            hub,
            live_buffers,
            running_commands.clone(),
            store,
            approvals,
            server_requests,
            ledger,
            model,
            "next",
        );

        tx.send(AgentEvent::ItemStarted {
            thread: thread_id,
            turn,
            item: ItemStart {
                id: item_id,
                harness_item_id,
                kind: ItemKind::CommandExecution,
                command: Some(CommandExecutionStart {
                    command: "sleep 1".into(),
                    cwd: "/tmp/test".into(),
                    status: Some("in_progress".into()),
                    process_id: Some("proc_1".into()),
                    started_at_ms: Some(1),
                }),
                tool: None,
            },
        })
        .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(
            running_commands.snapshot(thread_id).await.is_empty(),
            "historical starts for already-persisted turns must not create stale running tasks"
        );
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
    async fn forwarder_broadcasts_turnless_server_request_before_turn_start() {
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
            server_requests.clone(),
            ledger,
            model,
            "target",
        );

        let request_id = ServerRequestId("turnless_request".into());
        tx.send(AgentEvent::ServerRequestReceived {
            thread: thread_id,
            turn: None,
            request: ServerRequest {
                id: request_id.clone(),
                method: "mcpServer/elicitation/request".into(),
                params: serde_json::json!({
                    "message": "Allow cf-mcp to run tool \"wiki_search\"?"
                }),
                received_at: Utc::now(),
            },
        })
        .unwrap();

        let received = tokio::time::timeout(tokio::time::Duration::from_secs(2), client_rx.recv())
            .await
            .expect("broadcast")
            .expect("message");
        match received {
            ServerMessage::Event {
                agent_event: WireAgentEvent::ServerRequestReceived { turn, request, .. },
                ..
            } => {
                assert!(turn.is_none());
                assert_eq!(request.id, request_id);
                assert_eq!(request.method, "mcpServer/elicitation/request");
            }
            other => panic!("expected turnless server request event, got {other:?}"),
        }

        assert_eq!(
            server_requests.lock().await.get(&request_id).copied(),
            Some(thread_id)
        );
        assert!(
            store
                .load_all_turns(project_id, thread_id)
                .await
                .unwrap()
                .is_empty(),
            "turnless request alone must not persist a turn"
        );
        assert!(
            live_buffers.snapshot(thread_id).await.is_none(),
            "turnless request alone must not create target-thread live turn state"
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
        let stream = AgentEventStream::new(tx.subscribe());
        let ctx = TurnContext {
            user_input: UserInput::text("/compact"),
            model: model.clone(),
            mode: Mode::Build,
            kind: TurnContextKind::ManualCompaction,
        };
        let gate = ThreadTurnGate::default();
        let handle = ThreadHandle {
            thread: thread_id,
            harness_thread_id: "native-test-thread".into(),
            resumed_model: None,
            warning: None,
        };
        let lease = gate
            .reserve(thread_id, ActiveTurnOwner::new(project_id, &handle, &ctx))
            .unwrap();
        let ctx_for_second_reserve = ctx.clone();

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
            gate.reserve(
                thread_id,
                ActiveTurnOwner::new(project_id, &handle, &ctx_for_second_reserve)
            )
            .is_ok(),
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
        std::mem::drop(spawn_forwarder_handle(
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
            model,
            user_input,
        ));
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_forwarder_handle(
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
    ) -> JoinHandle<()> {
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
        })
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

    #[tokio::test]
    async fn forwarder_upserts_items_and_drops_conflicting_native_identity() {
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

        let first_turn = TurnId::new();
        let reused_harness = "agent_reply".to_string();
        let first_item_id = ItemId::new();
        let second_item_id = ItemId::new();
        let conflicting_item_id = ItemId::new();

        store
            .append_turn(
                project_id,
                thread_id,
                &Turn {
                    id: first_turn,
                    user_input: UserInput::text("first"),
                    items: vec![Item {
                        id: first_item_id,
                        harness_item_id: reused_harness.clone(),
                        payload: ItemPayload::AgentMessage {
                            text: "first answer".into(),
                        },
                        created_at: now,
                    }],
                    model: model.clone(),
                    mode: Mode::Build,
                    status: TurnStatus {
                        kind: TurnStatusKind::Completed,
                        message: None,
                    },
                    usage: TokenUsage::new(1, 1),
                    diffs: vec![],
                    started_at: now,
                    completed_at: Some(now),
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(64);
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
            store.clone(),
            approvals,
            server_requests,
            ledger,
            model,
            "second",
        );

        let second_turn = TurnId::new();
        tx.send(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: second_turn,
        })
        .unwrap();
        // Two ItemCompleted events for the same harness id within the new turn: this should
        // upsert to a single persisted item carrying the latest payload, while the earlier
        // persisted turn keeps its own distinct item.
        tx.send(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn: second_turn,
            item: Item {
                id: second_item_id,
                harness_item_id: reused_harness.clone(),
                payload: ItemPayload::AgentMessage {
                    text: "first version in second turn".into(),
                },
                created_at: now,
            },
        })
        .unwrap();
        tx.send(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn: second_turn,
            item: Item {
                id: second_item_id,
                harness_item_id: reused_harness.clone(),
                payload: ItemPayload::AgentMessage {
                    text: "second version in second turn".into(),
                },
                created_at: now,
            },
        })
        .unwrap();
        tx.send(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn: second_turn,
            item: Item {
                id: conflicting_item_id,
                harness_item_id: reused_harness.clone(),
                payload: ItemPayload::AgentMessage {
                    text: "conflicting identity".into(),
                },
                created_at: now,
            },
        })
        .unwrap();
        tx.send(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: second_turn,
            usage: TokenUsage::new(2, 2),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        })
        .unwrap();

        wait_for_turn_count(&store, project_id, thread_id, 2).await;
        let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(saved.len(), 2);
        assert_eq!(saved[0].id, first_turn);
        assert_eq!(saved[1].id, second_turn);
        assert_eq!(saved[0].items.len(), 1);
        assert_eq!(
            saved[1].items.len(),
            1,
            "repeated harness id in same turn should upsert to one item"
        );
        assert_eq!(saved[1].items[0].id, second_item_id);
        assert!(
            matches!(
                &saved[1].items[0].payload,
                ItemPayload::AgentMessage { text } if text == "second version in second turn"
            ),
            "upsert should keep the latest occurrence within the turn"
        );
        assert!(
            saved[0].items[0].id == first_item_id,
            "earlier turn item must remain untouched"
        );
        while let Ok(message) = client_rx.try_recv() {
            if let ServerMessage::Event {
                agent_event: WireAgentEvent::ItemCompleted { item, .. },
                ..
            } = message
            {
                assert_ne!(
                    item.id, conflicting_item_id,
                    "conflicting native identity must not be broadcast"
                );
            }
        }
    }

    #[tokio::test]
    async fn forwarder_forwards_item_started_and_delta_for_harness_id_reused_across_turns() {
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

        let first_turn = TurnId::new();
        let reused_harness = "agent_stream".to_string();
        let first_item_id = ItemId::new();

        store
            .append_turn(
                project_id,
                thread_id,
                &Turn {
                    id: first_turn,
                    user_input: UserInput::text("first"),
                    items: vec![Item {
                        id: first_item_id,
                        harness_item_id: reused_harness.clone(),
                        payload: ItemPayload::AgentMessage {
                            text: "first answer".into(),
                        },
                        created_at: now,
                    }],
                    model: model.clone(),
                    mode: Mode::Build,
                    status: TurnStatus {
                        kind: TurnStatusKind::Completed,
                        message: None,
                    },
                    usage: TokenUsage::new(1, 1),
                    diffs: vec![],
                    started_at: now,
                    completed_at: Some(now),
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(64);
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
            store.clone(),
            approvals,
            server_requests,
            ledger,
            model,
            "second",
        );

        let second_turn = TurnId::new();
        let second_item_id = ItemId::new();
        tx.send(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: second_turn,
        })
        .unwrap();
        tx.send(AgentEvent::ItemStarted {
            thread: thread_id,
            turn: second_turn,
            item: ItemStart {
                id: second_item_id,
                harness_item_id: reused_harness.clone(),
                kind: ItemKind::AgentMessage,
                command: None,
                tool: None,
            },
        })
        .unwrap();
        tx.send(AgentEvent::ItemDelta {
            thread: thread_id,
            turn: second_turn,
            item_id: second_item_id,
            delta: giskard_core::item::ItemDelta::Text {
                text: "streaming".into(),
            },
        })
        .unwrap();
        tx.send(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn: second_turn,
            item: Item {
                id: second_item_id,
                harness_item_id: reused_harness.clone(),
                payload: ItemPayload::AgentMessage {
                    text: "second answer".into(),
                },
                created_at: now,
            },
        })
        .unwrap();
        tx.send(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: second_turn,
            usage: TokenUsage::new(2, 2),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        })
        .unwrap();

        wait_for_turn_count(&store, project_id, thread_id, 2).await;

        // Collect broadcast events for the new turn and ensure the reused harness id did not
        // cause ItemStarted/ItemDelta/ItemCompleted to be suppressed.
        let mut saw_started = false;
        let mut saw_delta = false;
        let mut saw_completed = false;
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            if let Ok(Some(ServerMessage::Event { agent_event, .. })) =
                tokio::time::timeout(tokio::time::Duration::from_millis(100), client_rx.recv())
                    .await
            {
                match agent_event {
                    WireAgentEvent::ItemStarted { item, .. }
                        if item.harness_item_id == reused_harness =>
                    {
                        saw_started = true;
                    }
                    WireAgentEvent::ItemDelta { item_id, .. } if item_id == second_item_id => {
                        saw_delta = true;
                    }
                    WireAgentEvent::ItemCompleted { item, .. }
                        if item.harness_item_id == reused_harness =>
                    {
                        saw_completed = true;
                    }
                    _ => {}
                }
            }
        }

        assert!(
            saw_started,
            "ItemStarted for reused harness id must be forwarded"
        );
        assert!(
            saw_delta,
            "ItemDelta for reused harness id must be forwarded"
        );
        assert!(
            saw_completed,
            "ItemCompleted for reused harness id must be forwarded"
        );

        let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(saved[1].items.len(), 1);
        assert_eq!(saved[1].items[0].id, second_item_id);
        assert!(
            saved[0].items[0].id == first_item_id,
            "earlier turn item must remain untouched"
        );
    }

    #[tokio::test]
    async fn forwarder_upserts_item_deltas_for_repeated_item_id_within_turn() {
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
        let (client_tx, mut client_rx) = mpsc::channel(64);
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
            live_buffers.clone(),
            running_commands,
            store.clone(),
            approvals,
            server_requests,
            ledger,
            model,
            "delta-upsert",
        );

        let turn = TurnId::new();
        let item_id = ItemId::new();
        let harness = "agent_text";
        tx.send(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        })
        .unwrap();
        tx.send(AgentEvent::ItemStarted {
            thread: thread_id,
            turn,
            item: ItemStart {
                id: item_id,
                harness_item_id: harness.into(),
                kind: ItemKind::AgentMessage,
                command: None,
                tool: None,
            },
        })
        .unwrap();
        tx.send(AgentEvent::ItemDelta {
            thread: thread_id,
            turn,
            item_id,
            delta: giskard_core::item::ItemDelta::Text {
                text: "first".into(),
            },
        })
        .unwrap();
        tx.send(AgentEvent::ItemDelta {
            thread: thread_id,
            turn,
            item_id,
            delta: giskard_core::item::ItemDelta::Text {
                text: " second".into(),
            },
        })
        .unwrap();
        tx.send(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn,
            item: Item {
                id: item_id,
                harness_item_id: harness.into(),
                payload: ItemPayload::AgentMessage {
                    text: "final".into(),
                },
                created_at: now,
            },
        })
        .unwrap();
        tx.send(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn,
            usage: TokenUsage::new(3, 3),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        })
        .unwrap();

        wait_for_turn_count(&store, project_id, thread_id, 1).await;

        // Collect broadcast events before querying persistence; the live buffer may already have
        // been cleared by the time the persisted turn is visible.
        let mut delta_texts = Vec::new();
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            if let Ok(Some(ServerMessage::Event {
                agent_event:
                    WireAgentEvent::ItemDelta {
                        delta: giskard_proto::ItemDelta::Text { text },
                        ..
                    },
                ..
            })) = tokio::time::timeout(tokio::time::Duration::from_millis(100), client_rx.recv())
                .await
            {
                delta_texts.push(text);
            }
        }
        assert_eq!(
            delta_texts.len(),
            2,
            "both deltas for the same item id must be forwarded"
        );
        assert_eq!(delta_texts.concat(), "first second");

        let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].items.len(), 1);
        assert_eq!(saved[0].items[0].id, item_id);
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
