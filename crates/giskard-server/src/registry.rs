use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemPayload, command_status_is_running, normalized_command_status};
use giskard_core::model::ModelRef;
use giskard_core::turn::{Mode, Turn, TurnOverrides, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{AgentHarness, OpenThreadOptions, ThreadHandle};
use giskard_persist::PersistStore;
use giskard_persist::store::ProjectConfig;
use giskard_proto::{RunningCommand, ServerMessage, TokenScope};

use crate::hub::Hub;
use crate::ledger::LedgerHandle;
use crate::live_buffer::LiveBufferStore;
use crate::models::context_window_for;
use crate::running_commands::RunningCommandStore;

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
    running_commands: Arc<RunningCommandStore>,
    store: Arc<PersistStore>,
    ledger: LedgerHandle,
}

impl HarnessRegistry {
    pub fn new(
        factory: Arc<dyn HarnessFactory>,
        hub: Arc<Hub>,
        live_buffers: Arc<LiveBufferStore>,
        running_commands: Arc<RunningCommandStore>,
        store: Arc<PersistStore>,
        ledger: LedgerHandle,
    ) -> Self {
        Self {
            harnesses: Mutex::new(HashMap::new()),
            threads: Mutex::new(HashMap::new()),
            approvals: Arc::new(Mutex::new(HashMap::new())),
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

        let mut threads = self.threads.lock().await;
        threads.insert(handle.thread, (config.id, handle.clone()));
        debug!(
            project_id = %config.id,
            thread_id = %handle.thread,
            harness_thread_id = %handle.harness_thread_id,
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
        let (project_id, handle) = threads
            .get(&thread_id)
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let project_id = *project_id;
        let handle = handle.clone();
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
        };

        let hub = self.hub.clone();
        let live_buffers = self.live_buffers.clone();
        let running_commands = self.running_commands.clone();
        let store = self.store.clone();
        let approvals_map = self.approvals.clone();
        let ledger = self.ledger.clone();

        let stream = harness.subscribe(&handle);
        let turn_id = harness.start_turn(&handle, input, overrides).await?;

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
                ledger,
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
    running_commands: Arc<RunningCommandStore>,
    store: Arc<PersistStore>,
    approvals: ApprovalMap,
    ledger: LedgerHandle,
    ctx: TurnContext,
) {
    let mut turn_id: Option<TurnId> = None;
    let mut started_at = Utc::now();
    let mut items: Vec<Item> = Vec::new();
    let mut diffs: Vec<giskard_core::FileDiff> = Vec::new();
    let mut seen_turn_ids = persisted_turn_ids(&store, project_id, thread_id).await;
    let mut seen_harness_item_ids = persisted_harness_item_ids(&store, project_id, thread_id).await;
    let mut duplicate_item_ids = HashSet::new();

    loop {
        match stream.recv().await {
            Ok(event) => {
                if let Some(turn) = event_turn_id(&event) {
                    if seen_turn_ids.contains(&turn) {
                        let command_state_changed =
                            apply_running_command_event(&running_commands, &event).await;
                        if command_state_changed {
                            if is_terminal_command_completion(&event) {
                                hub.broadcast_event(thread_id, event).await;
                            }
                            broadcast_running_commands(&hub, &running_commands, thread_id).await;
                        }
                        continue;
                    }
                }
                if should_skip_duplicate_item(
                    &event,
                    &mut seen_harness_item_ids,
                    &mut duplicate_item_ids,
                ) {
                    continue;
                }

                let command_state_changed =
                    apply_running_command_event(&running_commands, &event).await;

                match &event {
                    AgentEvent::TurnStarted { turn, .. } => {
                        turn_id = Some(*turn);
                        started_at = Utc::now();
                    }
                    AgentEvent::ItemCompleted { item, .. } => items.push(item.clone()),
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

                if command_state_changed {
                    broadcast_running_commands(&hub, &running_commands, thread_id).await;
                }

                if let Some((completed_turn, usage, status)) = completed {
                    let tid = turn_id.unwrap_or(completed_turn);
                    seen_turn_ids.insert(tid);
                    let turn = Turn {
                        id: tid,
                        user_input: ctx.user_input.clone(),
                        items: std::mem::take(&mut items),
                        model: ctx.model.clone(),
                        mode: ctx.mode,
                        status: status.clone(),
                        usage,
                        diffs: std::mem::take(&mut diffs),
                        started_at,
                        completed_at: Some(Utc::now()),
                    };
                    persist_turn(&store, &hub, &ledger, project_id, thread_id, turn).await;
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

fn event_turn_id(event: &AgentEvent) -> Option<TurnId> {
    match event {
        AgentEvent::TurnStarted { turn, .. }
        | AgentEvent::ItemStarted { turn, .. }
        | AgentEvent::ItemDelta { turn, .. }
        | AgentEvent::ItemCompleted { turn, .. }
        | AgentEvent::DiffUpdated { turn, .. }
        | AgentEvent::ApprovalRequested { turn, .. }
        | AgentEvent::TurnCompleted { turn, .. } => Some(*turn),
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

async fn apply_running_command_event(
    running_commands: &RunningCommandStore,
    event: &AgentEvent,
) -> bool {
    let command_before_completion =
        terminating_command_before_terminal_completion(running_commands, event).await;
    let changed = running_commands.apply_event(event).await;
    log_command_completion_after_terminate(command_before_completion.as_ref(), event);
    changed
}

async fn terminating_command_before_terminal_completion(
    running_commands: &RunningCommandStore,
    event: &AgentEvent,
) -> Option<RunningCommand> {
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

fn log_command_completion_after_terminate(command: Option<&RunningCommand>, event: &AgentEvent) {
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
    running_commands: &RunningCommandStore,
    thread_id: ThreadId,
) {
    let commands = running_commands.snapshot(thread_id).await;
    hub.broadcast(
        thread_id,
        ServerMessage::RunningCommands {
            thread_id,
            commands,
        },
    )
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
    store
        .load_all_turns(project_id, thread_id)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|turn| turn.id)
        .collect()
}

async fn persisted_harness_item_ids(
    store: &PersistStore,
    project_id: ProjectId,
    thread_id: ThreadId,
) -> HashSet<String> {
    store
        .load_all_turns(project_id, thread_id)
        .await
        .unwrap_or_default()
        .into_iter()
        .flat_map(|turn| turn.items)
        .filter_map(|item| {
            if item.harness_item_id.is_empty() {
                None
            } else {
                Some(item.harness_item_id)
            }
        })
        .collect()
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
    let config = store.load_config().await.ok();

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
                ledger: ledger_json,
            },
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::{command_completion_is_normal_success, command_status_is_running};

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
}
