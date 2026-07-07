use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ProjectId, ThreadId, TurnId};
use giskard_core::item::Item;
use giskard_core::model::ModelRef;
use giskard_core::turn::{Mode, Turn, TurnOverrides, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{AgentHarness, OpenThreadOptions, ThreadHandle};
use giskard_persist::PersistStore;
use giskard_persist::store::ProjectConfig;
use giskard_proto::{ServerMessage, TokenScope};

use crate::hub::Hub;
use crate::live_buffer::LiveBufferStore;
use crate::models::context_window_for;

pub trait HarnessFactory: Send + Sync {
    fn create(&self, config: &ProjectConfig) -> Result<Arc<dyn AgentHarness>, HarnessError>;
}

/// Context describing the turn being started, used to persist a `Turn` on completion (§7.1).
#[derive(Clone)]
struct TurnContext {
    user_input: UserInput,
    model: ModelRef,
    mode: Mode,
}

/// Shared handle to the pending-approvals map (`ApprovalId -> ThreadId`), cloneable into the
/// spawned event forwarder so it can register approvals as they stream in.
type ApprovalMap = Arc<Mutex<HashMap<ApprovalId, ThreadId>>>;

pub struct HarnessRegistry {
    harnesses: Mutex<HashMap<ProjectId, Arc<dyn AgentHarness>>>,
    threads: Mutex<HashMap<ThreadId, (ProjectId, ThreadHandle)>>,
    /// Which thread a pending approval belongs to, so `ApprovalDecision { request_id }` (which
    /// carries no thread id, §13.6) can be routed to the right harness (§9.2).
    approvals: ApprovalMap,
    factory: Arc<dyn HarnessFactory>,
    hub: Arc<Hub>,
    live_buffers: Arc<LiveBufferStore>,
    store: Arc<PersistStore>,
}

impl HarnessRegistry {
    pub fn new(
        factory: Arc<dyn HarnessFactory>,
        hub: Arc<Hub>,
        live_buffers: Arc<LiveBufferStore>,
        store: Arc<PersistStore>,
    ) -> Self {
        Self {
            harnesses: Mutex::new(HashMap::new()),
            threads: Mutex::new(HashMap::new()),
            approvals: Arc::new(Mutex::new(HashMap::new())),
            factory,
            hub,
            live_buffers,
            store,
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
        let h = self.factory.create(config)?;
        harnesses.insert(project, h.clone());
        Ok(h)
    }

    pub async fn open_thread(
        &self,
        config: &ProjectConfig,
        workspace_root: &str,
        resume: Option<String>,
        initial_model: ModelRef,
    ) -> Result<ThreadHandle, HarnessError> {
        let harness = self.get_or_create_harness(config.id, config).await?;

        let handle = harness
            .open_thread(OpenThreadOptions {
                project: config.id,
                workspace_root: workspace_root.into(),
                resume,
                initial_model: initial_model.clone(),
            })
            .await?;

        let mut threads = self.threads.lock().await;
        threads.insert(handle.thread, (config.id, handle.clone()));

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
        let (project_id, handle) = threads
            .get(&thread_id)
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let project_id = *project_id;
        let handle = handle.clone();
        drop(threads);

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
        };

        let hub = self.hub.clone();
        let live_buffers = self.live_buffers.clone();
        let store = self.store.clone();
        let approvals_map = self.approvals.clone();

        let stream = harness.subscribe(&handle);
        let turn_id = harness.start_turn(&handle, input, overrides).await?;

        tokio::spawn(async move {
            forward_events(
                thread_id,
                project_id,
                stream,
                hub,
                live_buffers,
                store,
                approvals_map,
                ctx,
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

    pub async fn get_thread_handle(&self, thread_id: ThreadId) -> Option<ThreadHandle> {
        let threads = self.threads.lock().await;
        threads.get(&thread_id).map(|(_, h)| h.clone())
    }

    pub async fn get_project_for_thread(&self, thread_id: ThreadId) -> Option<ProjectId> {
        let threads = self.threads.lock().await;
        threads.get(&thread_id).map(|(p, _)| *p)
    }
}

#[allow(clippy::too_many_arguments)]
async fn forward_events(
    thread_id: ThreadId,
    project_id: ProjectId,
    mut stream: giskard_harness::AgentEventStream,
    hub: Arc<Hub>,
    live_buffers: Arc<LiveBufferStore>,
    store: Arc<PersistStore>,
    approvals: ApprovalMap,
    ctx: TurnContext,
) {
    let mut turn_id: Option<TurnId> = None;
    let mut started_at = Utc::now();
    let mut items: Vec<Item> = Vec::new();

    loop {
        match stream.recv().await {
            Ok(event) => {
                match &event {
                    AgentEvent::TurnStarted { turn, .. } => {
                        turn_id = Some(*turn);
                        started_at = Utc::now();
                    }
                    AgentEvent::ItemCompleted { item, .. } => items.push(item.clone()),
                    AgentEvent::ApprovalRequested { request, .. } => {
                        approvals.lock().await.insert(request.id.clone(), thread_id);
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

                if is_turn_start {
                    live_buffers.start_turn(thread_id).await;
                }
                if live_buffers.is_active(thread_id).await {
                    live_buffers.append(thread_id, event.clone()).await;
                }

                hub.broadcast_event(thread_id, event).await;

                if let Some((completed_turn, usage, status)) = completed {
                    let tid = turn_id.unwrap_or(completed_turn);
                    let turn = Turn {
                        id: tid,
                        user_input: ctx.user_input.clone(),
                        items: std::mem::take(&mut items),
                        model: ctx.model.clone(),
                        mode: ctx.mode,
                        status: status.clone(),
                        usage,
                        started_at,
                        completed_at: Some(Utc::now()),
                    };
                    persist_turn(&store, &hub, project_id, thread_id, turn).await;
                    live_buffers.clear_turn(thread_id).await;
                    turn_id = None;
                }
            }
            Err(e) => {
                debug!(%thread_id, ?e, "event stream ended");
                break;
            }
        }
    }
}

/// Append a completed `Turn` to the thread file, fold its usage into the thread ledger, recompute
/// the cached context window, and persist atomically (§7.1). Best-effort: logs on failure.
async fn persist_turn(
    store: &PersistStore,
    hub: &Hub,
    project_id: ProjectId,
    thread_id: ThreadId,
    turn: Turn,
) {
    let mut tf = match store.load_thread(project_id, thread_id).await {
        Ok(Some(tf)) => tf,
        Ok(None) => {
            warn!(%thread_id, "thread file missing on turn completion");
            return;
        }
        Err(e) => {
            warn!(%thread_id, %e, "failed to load thread on turn completion");
            return;
        }
    };

    if turn.status.kind == TurnStatusKind::Completed
        || turn.status.kind == TurnStatusKind::Interrupted
    {
        tf.tokens
            .record(&turn.model.provider, &turn.model.model, &turn.usage);
    }
    tf.turns.push(turn);
    tf.updated_at = Utc::now();

    // C4: keep the cached context window in sync with the (possibly changed) current model.
    if let Ok(config) = store.load_config().await {
        tf.context_window = context_window_for(&config, &tf.current_model);
    }

    if let Err(e) = store.save_thread(project_id, &tf).await {
        warn!(%thread_id, %e, "failed to persist thread on turn completion");
        return;
    }

    // Push a thread-scoped token update to subscribers (§13.6).
    if let Ok(ledger) = serde_json::to_value(&tf.tokens) {
        hub.broadcast(
            thread_id,
            ServerMessage::TokenUpdate {
                scope: TokenScope::Thread,
                ledger,
            },
        )
        .await;
    }
}
