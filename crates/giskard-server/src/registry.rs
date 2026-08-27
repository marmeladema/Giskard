use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;
use futures::future::join_all;
use tokio::sync::{Mutex, Notify, OwnedMutexGuard, oneshot, watch};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ProjectId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{
    Item, ItemDelta, ItemPayload, SubagentAction, SubagentStatus, command_status_is_running,
    normalized_command_status, tool_status_is_running,
};
use giskard_core::mcp::{McpOauthStart, McpServerStatus};
use giskard_core::model::{ModelDescriptor, ModelRef};
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::text::trimmed_non_empty;
use giskard_core::thread::ThreadKind;
use giskard_core::turn::{Mode, Turn, TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentHarness, HarnessCapabilities, HarnessProvider, OpenThreadOptions, ResumePolicy,
    ThreadHandle, ThreadUpdate, thread_update_channel,
};
use giskard_persist::PersistStore;
use giskard_persist::store::{ProjectConfig, ThreadFile, ThreadMutation, TurnCommitOutcome};
use giskard_proto::{RunningTask, ServerMessage, WireAgentEvent, WireItem};

use crate::hub::Hub;
use crate::ledger::LedgerHandle;
use crate::log_fields::{display_opt, rfc3339, rfc3339_opt};
use crate::thread_graph::{
    ExistingLinkDisposition, classify_existing_link, load_thread_graph, parent_chain_is_valid,
    should_refresh_subagent_title,
};
use crate::thread_metadata::ThreadMetadataService;
use crate::thread_runtime::{
    AppliedRuntimeEvent, RequestResolution, RequestTransition, RestorePermit, RuntimeRequestId,
    ThreadRuntimeRegistry, ThreadTurnLease, TurnReservation,
};

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
    passive_input_is_fallback: bool,
    subagent_fallback: Option<SubagentFallbackTranscript>,
    passive_subagent_metadata: Option<PassiveSubagentMetadataMap>,
    passive_pre_turn_timeout: Option<Duration>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TurnContextKind {
    User,
    ManualCompaction,
    PassiveSubagent,
}

#[derive(Clone, Copy)]
enum ForwarderExitReason {
    NormalTurnCompleted,
    SyntheticCompactionCompleted,
    AfterTurnCommandsDrained,
    StreamEndedRecovered,
    StreamEndedWithoutTurn,
    DuplicateForwarder,
    PersistenceBlocked,
    EventPreparationFailed,
    RuntimeAuthorityReplaced,
}

fn forwarder_exit_reason_label(reason: ForwarderExitReason) -> &'static str {
    match reason {
        ForwarderExitReason::NormalTurnCompleted => "normal_turn_completed",
        ForwarderExitReason::SyntheticCompactionCompleted => "synthetic_compaction_completed",
        ForwarderExitReason::AfterTurnCommandsDrained => "after_turn_commands_drained",
        ForwarderExitReason::StreamEndedRecovered => "stream_ended_recovered",
        ForwarderExitReason::StreamEndedWithoutTurn => "stream_ended_without_turn",
        ForwarderExitReason::DuplicateForwarder => "duplicate_forwarder",
        ForwarderExitReason::PersistenceBlocked => "persistence_blocked",
        ForwarderExitReason::EventPreparationFailed => "event_preparation_failed",
        ForwarderExitReason::RuntimeAuthorityReplaced => "runtime_authority_replaced",
    }
}

fn turn_context_kind_label(kind: TurnContextKind) -> &'static str {
    match kind {
        TurnContextKind::User => "user",
        TurnContextKind::ManualCompaction => "manual_compaction",
        TurnContextKind::PassiveSubagent => "passive_subagent",
    }
}

fn turn_reservation(
    project_id: ProjectId,
    handle: &ThreadHandle,
    ctx: &TurnContext,
) -> TurnReservation {
    TurnReservation {
        project_id,
        harness_thread_id: handle.harness_thread_id.clone(),
        mode: ctx.mode,
        provider: ctx.model.provider.clone(),
        model: ctx.model.model.clone(),
        context_kind: turn_context_kind_label(ctx.kind),
    }
}

fn live_turn_user_input(ctx: &TurnContext) -> Option<UserInput> {
    if ctx.kind != TurnContextKind::PassiveSubagent {
        return None;
    }
    ctx.user_input
        .as_text()
        .and_then(trimmed_non_empty)
        .map(UserInput::text)
}

fn passive_subagent_prompt_text(ctx: &TurnContext) -> Option<String> {
    if ctx.kind != TurnContextKind::PassiveSubagent || ctx.passive_input_is_fallback {
        return None;
    }
    ctx.user_input
        .as_text()
        .and_then(trimmed_non_empty)
        .map(ToOwned::to_owned)
}

type PassiveSubagentMetadataMap = Arc<Mutex<HashMap<ThreadId, PassiveSubagentMetadata>>>;
type PassiveMonitorTasks = Arc<PassiveMonitorTaskTracker>;
type ProjectLifecycleLocks = Arc<Mutex<HashMap<ProjectId, Weak<Mutex<()>>>>>;
const ACTIVE_SUBAGENT_PRE_TURN_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const PASSIVE_MONITOR_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const HARNESS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);
const BACKGROUND_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const LEDGER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct RegistryTaskTracker {
    closed: AtomicBool,
    count: AtomicUsize,
    completion: Notify,
}

struct RegistryTaskPermit {
    tracker: Arc<RegistryTaskTracker>,
}

impl Drop for RegistryTaskPermit {
    fn drop(&mut self) {
        if self.tracker.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.tracker.completion.notify_waiters();
        }
    }
}

impl RegistryTaskTracker {
    fn register(self: &Arc<Self>) -> Option<RegistryTaskPermit> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        self.count.fetch_add(1, Ordering::AcqRel);
        if self.closed.load(Ordering::Acquire) {
            if self.count.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.completion.notify_waiters();
            }
            return None;
        }
        Some(RegistryTaskPermit {
            tracker: self.clone(),
        })
    }

    async fn close_and_wait(&self, wait: Duration) -> Result<(), HarnessError> {
        self.closed.store(true, Ordering::Release);
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            // Create the waiter before checking count so a concurrent final drop cannot be missed.
            let completion = self.completion.notified();
            if self.count.load(Ordering::Acquire) == 0 {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, completion).await.is_err() {
                return Err(HarnessError::Timeout(format!(
                    "registry background tasks did not drain within {} ms",
                    wait.as_millis()
                )));
            }
        }
    }
}

struct PassiveMonitorTaskTracker {
    counts: Mutex<HashMap<ThreadId, usize>>,
    completion: watch::Sender<u64>,
}

impl Default for PassiveMonitorTaskTracker {
    fn default() -> Self {
        let (completion, _) = watch::channel(0);
        Self {
            counts: Mutex::new(HashMap::new()),
            completion,
        }
    }
}

impl PassiveMonitorTaskTracker {
    async fn register(&self, thread_id: ThreadId) {
        *self.counts.lock().await.entry(thread_id).or_default() += 1;
    }

    async fn contains(&self, thread_id: ThreadId) -> bool {
        self.counts.lock().await.contains_key(&thread_id)
    }

    async fn finish(&self, thread_id: ThreadId) {
        let mut counts = self.counts.lock().await;
        match counts.get_mut(&thread_id) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                counts.remove(&thread_id);
            }
            None => {
                warn!(
                    %thread_id,
                    "passive sub-agent monitor task completed without a registered task"
                );
            }
        }
        drop(counts);
        self.completion.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.completion.subscribe()
    }
}

#[derive(Clone)]
struct ThreadBinding {
    project: ProjectId,
    handle: ThreadHandle,
    /// The model the harness reports this native thread is on. `None` when neither the caller nor
    /// the harness named one — callers already treat an unknown native model the same as an
    /// unbound thread.
    native_model: Option<ModelRef>,
}

#[derive(Clone, Default)]
struct PassiveSubagentMetadata {
    initial_prompt: Option<String>,
    fallback: Option<SubagentFallbackTranscript>,
    active_lifecycle_observed: bool,
    terminal_observed: bool,
    cancelled: bool,
    lifecycle_notify: Arc<Notify>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PassiveMonitorSignal {
    Continue,
    Terminal,
    Cancelled,
}

#[derive(Clone, Copy)]
enum LifecycleSignal {
    None,
    Active,
    Terminal,
}

#[derive(Clone)]

pub struct HarnessRegistry {
    shared: Arc<RegistryShared>,
    factory: Arc<dyn HarnessFactory>,
}

#[derive(Default)]
struct Harnesses {
    shutting_down: bool,
    by_project: HashMap<ProjectId, ProjectHarnessState>,
}

impl Harnesses {
    fn active(&self, project_id: ProjectId) -> Option<Arc<dyn AgentHarness>> {
        self.by_project
            .get(&project_id)
            .and_then(ProjectHarnessState::active)
            .cloned()
    }

    fn begin_delete(
        &mut self,
        project_id: ProjectId,
    ) -> Result<Option<Arc<dyn AgentHarness>>, HarnessError> {
        match self.by_project.get(&project_id) {
            Some(ProjectHarnessState::Active(harness)) => {
                let harness = harness.clone();
                self.by_project
                    .insert(project_id, ProjectHarnessState::Deleting(harness.clone()));
                Ok(Some(harness))
            }
            Some(ProjectHarnessState::Deleting(_)) => Err(HarnessError::Protocol(format!(
                "project {project_id} harness deletion is already in progress"
            ))),
            None => Ok(None),
        }
    }

    fn rollback_delete(&mut self, project_id: ProjectId, harness: Arc<dyn AgentHarness>) {
        if !self.shutting_down
            && matches!(
                self.by_project.get(&project_id),
                Some(ProjectHarnessState::Deleting(current)) if Arc::ptr_eq(current, &harness)
            )
        {
            self.by_project
                .insert(project_id, ProjectHarnessState::Active(harness));
        }
    }

    fn finish_delete(&mut self, project_id: ProjectId, harness: &Arc<dyn AgentHarness>) {
        if matches!(
            self.by_project.get(&project_id),
            Some(ProjectHarnessState::Deleting(current)) if Arc::ptr_eq(current, harness)
        ) {
            self.by_project.remove(&project_id);
        }
    }

    fn begin_shutdown(&mut self) -> HashMap<ProjectId, Arc<dyn AgentHarness>> {
        self.shutting_down = true;
        std::mem::take(&mut self.by_project)
            .into_iter()
            .map(|(project_id, state)| (project_id, state.into_harness()))
            .collect()
    }
}

enum ProjectHarnessState {
    Active(Arc<dyn AgentHarness>),
    Deleting(Arc<dyn AgentHarness>),
}

impl ProjectHarnessState {
    fn active(&self) -> Option<&Arc<dyn AgentHarness>> {
        match self {
            Self::Active(harness) => Some(harness),
            Self::Deleting(_) => None,
        }
    }

    fn into_harness(self) -> Arc<dyn AgentHarness> {
        match self {
            Self::Active(harness) | Self::Deleting(harness) => harness,
        }
    }
}

struct RegistryShared {
    harnesses: Arc<Mutex<Harnesses>>,
    threads: Arc<Mutex<HashMap<ThreadId, ThreadBinding>>>,
    passive_monitors: Arc<Mutex<HashSet<ThreadId>>>,
    passive_subagent_metadata: PassiveSubagentMetadataMap,
    /// Generation count spanning subscription and post-forwarder fallback persistence. A new
    /// monitor may start after an old subscription exits, so deletion waits for all generations.
    passive_monitor_tasks: PassiveMonitorTasks,
    background_tasks: Arc<RegistryTaskTracker>,
    /// Per-parent FIFO for linked lifecycle evidence. Harness events are ordered, so preserving
    /// that order here prevents a later terminal observation from racing ahead of an active one.
    subagent_materialization_queues:
        Arc<Mutex<HashMap<ThreadId, VecDeque<SubagentMaterializationJob>>>>,
    project_lifecycle_locks: ProjectLifecycleLocks,
    hub: Arc<Hub>,
    runtime: Arc<ThreadRuntimeRegistry>,
    store: Arc<PersistStore>,
    thread_metadata: Arc<ThreadMetadataService>,
    ledger: LedgerHandle,
}

/// The project's harness if one is running, or `None` if one should be created.
///
/// Errors on the states where creating is wrong: a server shutting down, or a harness midway
/// through deletion. Shared by both passes of `get_or_create_harness` so the second cannot drift
/// from the first.
fn harness_slot(
    harnesses: &Harnesses,
    project: ProjectId,
) -> Result<Option<Arc<dyn AgentHarness>>, HarnessError> {
    if harnesses.shutting_down {
        return Err(HarnessError::Protocol(
            "server is shutting down; refusing to start a harness".into(),
        ));
    }
    if let Some(harness) = harnesses.active(project) {
        return Ok(Some(harness));
    }
    if matches!(
        harnesses.by_project.get(&project),
        Some(ProjectHarnessState::Deleting(_))
    ) {
        return Err(HarnessError::Protocol(format!(
            "project {project} harness is being deleted"
        )));
    }
    Ok(None)
}

impl RegistryShared {
    async fn active_harness(&self, project_id: ProjectId) -> Option<Arc<dyn AgentHarness>> {
        self.harnesses.lock().await.active(project_id)
    }

    #[cfg(test)]
    fn new(hub: Arc<Hub>, store: Arc<PersistStore>, ledger: LedgerHandle) -> Self {
        Self::new_with_runtime(hub, Arc::new(ThreadRuntimeRegistry::new()), store, ledger)
    }

    fn new_with_runtime(
        hub: Arc<Hub>,
        runtime: Arc<ThreadRuntimeRegistry>,
        store: Arc<PersistStore>,
        ledger: LedgerHandle,
    ) -> Self {
        let thread_metadata = Arc::new(ThreadMetadataService::new(store.clone(), hub.clone()));
        Self {
            harnesses: Arc::new(Mutex::new(Harnesses::default())),
            threads: Arc::new(Mutex::new(HashMap::new())),
            passive_monitors: Arc::new(Mutex::new(HashSet::new())),
            passive_subagent_metadata: Arc::new(Mutex::new(HashMap::new())),
            passive_monitor_tasks: Arc::new(PassiveMonitorTaskTracker::default()),
            background_tasks: Arc::new(RegistryTaskTracker::default()),
            subagent_materialization_queues: Arc::new(Mutex::new(HashMap::new())),
            project_lifecycle_locks: Arc::new(Mutex::new(HashMap::new())),
            hub,
            runtime,
            store,
            thread_metadata,
            ledger,
        }
    }
}

fn prepare_thread_updates(
    shared: &RegistryShared,
    thread_id: ThreadId,
) -> (
    giskard_harness::ThreadUpdateSink,
    giskard_harness::ThreadUpdateStream,
    RestorePermit,
) {
    let (sink, stream) = thread_update_channel();
    let permit = shared.runtime.restoration_permit(thread_id);
    (sink, stream, permit)
}

fn spawn_thread_update_forwarder(
    shared: Arc<RegistryShared>,
    project_id: ProjectId,
    thread_id: ThreadId,
    mut updates: giskard_harness::ThreadUpdateStream,
    permit: RestorePermit,
) -> Option<tokio::task::JoinHandle<()>> {
    let Some(task_permit) = shared.background_tasks.register() else {
        warn!(
            %project_id,
            %thread_id,
            action = "restore_context_window",
            reason = "registry_shutting_down",
            "not starting thread update forwarder"
        );
        return None;
    };
    Some(tokio::spawn(async move {
        let _task_permit = task_permit;
        let Some(update) = updates.recv().await else {
            return;
        };
        let ThreadUpdate::ContextWindowRestored {
            model,
            context_window,
        } = update;
        let stored_model = model.clone();
        let runtime = shared.runtime.clone();
        let result = shared
            .thread_metadata
            .mutate(project_id, thread_id, move |thread| {
                if runtime.restoration_is_current(&permit) {
                    thread.record_model_context_window(&stored_model, context_window);
                }
            })
            .await;
        match result {
            Ok(ThreadMutation::Changed { after, .. }) => info!(%project_id, %thread_id,
                metadata_revision = after.revision, provider = %model.provider, model = %model.model,
                context_window, "restored resumed thread context window"),
            Ok(ThreadMutation::Unchanged { .. }) => debug!(%project_id, %thread_id,
                "resumed context-window restore was stale or already current"),
            Ok(ThreadMutation::Missing) => warn!(%project_id, %thread_id,
                "thread disappeared before resumed context window could be restored"),
            Err(error) => error!(%project_id, %thread_id, %error,
                "failed to persist resumed context window"),
        }
    }))
}

impl HarnessRegistry {
    #[cfg(test)]
    pub fn new(
        factory: Arc<dyn HarnessFactory>,
        hub: Arc<Hub>,
        store: Arc<PersistStore>,
        ledger: LedgerHandle,
    ) -> Self {
        Self {
            shared: Arc::new(RegistryShared::new(hub, store, ledger)),
            factory,
        }
    }

    pub fn new_with_runtime(
        factory: Arc<dyn HarnessFactory>,
        hub: Arc<Hub>,
        runtime: Arc<ThreadRuntimeRegistry>,
        store: Arc<PersistStore>,
        ledger: LedgerHandle,
    ) -> Self {
        Self {
            shared: Arc::new(RegistryShared::new_with_runtime(
                hub, runtime, store, ledger,
            )),
            factory,
        }
    }

    pub(crate) fn thread_metadata_service(&self) -> Arc<ThreadMetadataService> {
        self.shared.thread_metadata.clone()
    }

    /// Serialize persisted thread-graph mutations within one project. Child imports may originate
    /// from either an HTTP request or an asynchronously observed harness event, while subtree and
    /// project deletion mutate the same graph. One project-scoped lock makes each find/open/save
    /// or load/preflight/delete sequence atomic with respect to the others.
    pub async fn lock_project_lifecycle(&self, project_id: ProjectId) -> OwnedMutexGuard<()> {
        lock_project_lifecycle(&self.shared.project_lifecycle_locks, project_id).await
    }

    pub async fn lock_project_lifecycle_with_timeout(
        &self,
        project_id: ProjectId,
        wait: Duration,
    ) -> Result<OwnedMutexGuard<()>, HarnessError> {
        timeout(wait, self.lock_project_lifecycle(project_id))
            .await
            .map_err(|_| {
                HarnessError::Timeout(format!(
                    "timed out waiting for project {project_id} lifecycle lock"
                ))
            })
    }

    async fn get_or_create_harness(
        &self,
        project: ProjectId,
        config: &ProjectConfig,
    ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        // Fast path. This lock is a single global one guarding every project's harness and is
        // taken on ordinary per-event work, so the usual answer — "already running" — must not
        // wait behind anything slower than a map lookup.
        {
            let harnesses = self.shared.harnesses.lock().await;
            if let Some(harness) = harness_slot(&harnesses, project)? {
                return Ok(harness);
            }
        }

        // Nothing is running for this project yet, so read the bindings the new harness will need.
        //
        // The thread graph belongs to the project, not to any harness, so it is read with the
        // lock released: it is a directory scan plus a file per thread, and every other project's
        // work would queue behind it. A racing caller may create the harness while this runs, in
        // which case the re-check below returns theirs and this read is discarded — the cost of a
        // wasted scan on a path that runs once per project, against holding a global lock across
        // I/O on every path that does not.
        let bindings = self.known_thread_bindings(project).await;

        let mut harnesses = self.shared.harnesses.lock().await;
        if let Some(harness) = harness_slot(&harnesses, project)? {
            return Ok(harness);
        }
        let h = self.factory.create(config).await?;

        // Hand the bindings over *before* publishing the harness.
        //
        // Codex announces a sub-agent's thread as soon as it loads one, which for a child we
        // persisted in an earlier run happens before the parent's tool call names it. Without
        // these bindings the adapter meets a native id it has never seen and invents a ThreadId
        // for a thread that already has one.
        //
        // The ordering is the whole point, so it is enforced rather than assumed: the harness
        // enters the map only once bound, so no concurrent caller can take it out and open a
        // thread on it in between.
        if let Some(bindings) = bindings {
            debug!(
                project_id = %project,
                bindings = bindings.len(),
                "handing known thread bindings to a new harness"
            );
            h.bind_known_threads(bindings).await;
        }

        harnesses
            .by_project
            .insert(project, ProjectHarnessState::Active(h.clone()));
        Ok(h)
    }

    /// Every `(native id, ThreadId)` pair this project has already persisted.
    ///
    /// `None` when they could not be read: not fatal, because the harness still works and a thread
    /// Giskard opens itself registers on the way through. What is lost is the guarantee for
    /// children Codex announces before Giskard opens them, so it is worth a warning.
    ///
    /// Read from the same thread files the thread graph is built from; nothing else is loaded, and
    /// turn files are never touched.
    async fn known_thread_bindings(&self, project: ProjectId) -> Option<Vec<(String, ThreadId)>> {
        match load_thread_graph(&self.shared.store, project).await {
            Ok(graph) => Some(
                graph
                    .values()
                    .filter(|thread| !thread.harness_thread_id.is_empty())
                    .map(|thread| (thread.harness_thread_id.clone(), thread.id))
                    .collect(),
            ),
            Err(error) => {
                warn!(
                    project_id = %project,
                    %error,
                    "could not read known thread bindings; sub-agent threads announced before \
                     Giskard opens them may be routed under a fresh id"
                );
                None
            }
        }
    }

    pub async fn open_thread(
        &self,
        config: &ProjectConfig,
        workspace_root: &str,
        thread: Option<ThreadId>,
        resume: Option<String>,
        initial_model: Option<ModelRef>,
    ) -> Result<ThreadHandle, HarnessError> {
        self.open_thread_with_resume_policy(
            config,
            workspace_root,
            thread,
            resume,
            initial_model,
            ResumePolicy::AllowFreshFallback,
        )
        .await
    }

    pub async fn open_linked_thread(
        &self,
        config: &ProjectConfig,
        workspace_root: &str,
        thread: Option<ThreadId>,
        resume: String,
        initial_model: ModelRef,
    ) -> Result<ThreadHandle, HarnessError> {
        self.open_thread_with_resume_policy(
            config,
            workspace_root,
            thread,
            Some(resume),
            Some(initial_model),
            ResumePolicy::RequireExisting,
        )
        .await
    }

    async fn open_thread_with_resume_policy(
        &self,
        config: &ProjectConfig,
        workspace_root: &str,
        thread: Option<ThreadId>,
        resume: Option<String>,
        initial_model: Option<ModelRef>,
        resume_policy: ResumePolicy,
    ) -> Result<ThreadHandle, HarnessError> {
        debug!(
            project_id = %config.id,
            thread_id = display_opt(thread),
            resume = display_opt(resume.as_deref()),
            ?resume_policy,
            "opening harness thread"
        );
        let harness = self.get_or_create_harness(config.id, config).await?;
        let requested_native_id = resume.clone();
        let (updates, update_stream) = thread_update_channel();
        let restore_permit =
            thread.map(|thread_id| self.shared.runtime.restoration_permit(thread_id));

        let handle = harness
            .open_thread(OpenThreadOptions {
                project: config.id,
                thread,
                workspace_root: workspace_root.into(),
                resume,
                resume_policy,
                initial_model: initial_model.clone(),
                updates,
            })
            .await?;
        // A known thread can begin another lifecycle while its harness open is in flight, so its
        // permit was captured above. A newly imported thread is not exposed until after this
        // function returns, making the harness-returned identity safe to capture here.
        let restore_permit =
            restore_permit.unwrap_or_else(|| self.shared.runtime.restoration_permit(handle.thread));

        // This is the harness-neutral identity boundary. Individual adapters may enforce the same
        // contract internally, but the registry must not rely on adapter-specific validation.
        if resume_policy == ResumePolicy::RequireExisting
            && requested_native_id.as_deref() != Some(handle.harness_thread_id.as_str())
        {
            return Err(HarnessError::Protocol(format!(
                "linked-thread resume returned native thread {} instead of {}",
                handle.harness_thread_id,
                requested_native_id.as_deref().unwrap_or_default()
            )));
        }

        // Bind the model the harness reports as effective when it says so — Codex can ignore
        // resume overrides for a loaded thread, and the binding must reflect reality, not the
        // request (spec: model-provider-switching analysis).
        let native_model = handle
            .resumed_model
            .clone()
            .or_else(|| initial_model.clone());
        drop(spawn_thread_update_forwarder(
            self.shared.clone(),
            config.id,
            handle.thread,
            update_stream,
            restore_permit,
        ));
        let mut threads = self.shared.threads.lock().await;
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
            provider = initial_model.as_ref().map(|m| m.provider.as_str()).unwrap_or("<harness>"),
            model = initial_model.as_ref().map(|m| m.model.as_str()).unwrap_or("<harness>"),
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
        let threads = self.shared.threads.lock().await;
        let binding = threads
            .get(&thread_id)
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let project_id = binding.project;
        let handle = binding.handle.clone();
        drop(threads);
        if self.thread_has_passive_monitor(thread_id).await {
            warn!(
                %project_id,
                %thread_id,
                harness_thread_id = %handle.harness_thread_id,
                "refusing direct turn while passive sub-agent monitoring owns the thread"
            );
            return Err(HarnessError::ThreadBusy { thread: thread_id });
        }
        debug!(
            %project_id,
            %thread_id,
            harness_thread_id = %handle.harness_thread_id,
            mode = ?overrides.mode,
            provider = %effective_model.provider,
            model = %effective_model.model,
            "starting harness turn"
        );

        let harness = self
            .shared
            .active_harness(project_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;

        let ctx = TurnContext {
            user_input: input.clone(),
            model: effective_model,
            mode: overrides.mode,
            kind: TurnContextKind::User,
            passive_input_is_fallback: false,
            subagent_fallback: None,
            passive_subagent_metadata: None,
            passive_pre_turn_timeout: None,
        };
        let request_started = Instant::now();
        let mut turn_gate = self
            .shared
            .runtime
            .reserve_turn(thread_id, turn_reservation(project_id, &handle, &ctx))?;
        publish_runtime_overview(&self.shared).await;

        let shared = self.shared.clone();

        let stream = harness.subscribe(&handle);
        let Some(forwarder_permit) = shared.background_tasks.register() else {
            warn!(
                %project_id,
                %thread_id,
                action = "start_turn",
                reason = "registry_shutting_down",
                "refusing to start turn event forwarder"
            );
            if let Some(overview) = turn_gate.release() {
                self.shared.hub.publish_runtime_overview(overview).await;
            }
            return Err(HarnessError::Protocol(
                "server is shutting down; refusing to start turn forwarder".into(),
            ));
        };
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
                // Published by the unconditional `publish_runtime_overview` after this match.
                let _acknowledged = turn_gate.acknowledge_turn(turn_id);
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
                if let Some(overview) = turn_gate.release() {
                    self.shared.hub.publish_runtime_overview(overview).await;
                }
                return Err(error);
            }
        };
        publish_runtime_overview(&self.shared).await;

        launch_event_forwarder(
            shared,
            thread_id,
            project_id,
            stream,
            ctx,
            Some(turn_gate),
            forwarder_permit,
        );

        Ok(turn_id)
    }

    /// Route an approval decision to the harness that raised it (§9.2).
    pub async fn respond_approval(
        &self,
        thread_id: ThreadId,
        request_id: ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<ThreadId, HarnessError> {
        let project_id = self
            .get_project_for_thread(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;

        let harness = self
            .shared
            .active_harness(project_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;

        let (claim, transition) = self
            .shared
            .runtime
            .claim_request(thread_id, RuntimeRequestId::Approval(request_id.clone()))?;
        self.publish_request_transition(thread_id, transition).await;

        if let Err(error) = harness
            .respond_approval(request_id.clone(), decision.clone())
            .await
        {
            if let Some(transition) = claim.rollback() {
                self.publish_request_transition(thread_id, transition).await;
            }
            return Err(error);
        }
        let transition = match claim.commit(RequestResolution::Approval(decision.clone())) {
            Ok(transition) => transition,
            Err(failure) => {
                if let Some(transition) = failure.rollback {
                    self.publish_request_transition(thread_id, transition).await;
                }
                return Err(failure.error);
            }
        };
        // Record the resolution against the in-flight turn *before* publishing it, so a browser
        // that reloads the instant it sees the resolved state replays this approval as answered
        // rather than re-prompting (spec §13.6).
        self.shared
            .runtime
            .resolve_live_approval(thread_id, request_id.clone(), decision);
        debug!(
            %thread_id,
            request_id = %request_id.0,
            "recorded approval resolution in live buffer for reconnect"
        );
        self.publish_request_transition(thread_id, transition).await;
        Ok(thread_id)
    }

    /// Route a non-approval server-request response to the harness that raised it, returning the
    /// thread it belonged to so the caller can record the answer against that thread's live turn.
    pub async fn respond_server_request(
        &self,
        thread_id: ThreadId,
        request_id: ServerRequestId,
        response: ServerRequestResponse,
    ) -> Result<ThreadId, HarnessError> {
        let project_id = self
            .get_project_for_thread(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;

        let harness = self
            .shared
            .active_harness(project_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;

        let (claim, transition) = self
            .shared
            .runtime
            .claim_request(thread_id, RuntimeRequestId::Server(request_id.clone()))?;
        self.publish_request_transition(thread_id, transition).await;

        if let Err(error) = harness
            .respond_server_request(request_id.clone(), response.clone())
            .await
        {
            if let Some(transition) = claim.rollback() {
                self.publish_request_transition(thread_id, transition).await;
            }
            return Err(error);
        }
        let transition = match claim.commit(RequestResolution::Server(response)) {
            Ok(transition) => transition,
            Err(failure) => {
                if let Some(transition) = failure.rollback {
                    self.publish_request_transition(thread_id, transition).await;
                }
                return Err(failure.error);
            }
        };
        // Record the answer against the in-flight turn before publishing it. The harness emits its
        // own resolved event, but on its own schedule and not guaranteed at all; until then the
        // request still reads as outstanding in the replayed events, so a reload in that window
        // would re-prompt and re-answering routes a stale id to the harness (spec §13.6).
        self.shared
            .runtime
            .resolve_live_server_request(thread_id, request_id.clone());
        debug!(
            %thread_id,
            request_id = %request_id.0,
            "recorded server request resolution in live buffer for reconnect"
        );
        self.publish_request_transition(thread_id, transition).await;
        Ok(thread_id)
    }

    async fn publish_request_state(&self, thread_id: ThreadId, request_id: &RuntimeRequestId) {
        let Some(request) = self.shared.runtime.request_state(thread_id, request_id) else {
            return;
        };
        self.shared
            .hub
            .broadcast(request.thread_id, ServerMessage::RequestState(request))
            .await;
        publish_runtime_overview(&self.shared).await;
    }

    async fn publish_request_transition(&self, thread_id: ThreadId, transition: RequestTransition) {
        self.shared
            .hub
            .broadcast(
                thread_id,
                ServerMessage::RequestState(transition.request_state),
            )
            .await;
        if let Some(overview) = transition.overview_if_changed {
            self.shared.hub.publish_runtime_overview(overview).await;
        }
    }

    pub(crate) async fn republish_server_request_state(
        &self,
        thread_id: ThreadId,
        request_id: ServerRequestId,
    ) {
        self.publish_request_state(thread_id, &RuntimeRequestId::Server(request_id))
            .await;
    }

    pub(crate) async fn republish_approval_request_state(
        &self,
        thread_id: ThreadId,
        request_id: ApprovalId,
    ) {
        self.publish_request_state(thread_id, &RuntimeRequestId::Approval(request_id))
            .await;
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
            .shared
            .active_harness(project_id)
            .await
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
            .shared
            .active_harness(project_id)
            .await
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
            passive_input_is_fallback: false,
            subagent_fallback: None,
            passive_subagent_metadata: None,
            passive_pre_turn_timeout: None,
        };
        let mut turn_gate = self
            .shared
            .runtime
            .reserve_turn(thread_id, turn_reservation(project_id, &handle, &ctx))?;
        publish_runtime_overview(&self.shared).await;

        let shared = self.shared.clone();

        let stream = harness.subscribe(&handle);
        let Some(forwarder_permit) = shared.background_tasks.register() else {
            warn!(
                %project_id,
                %thread_id,
                action = "compact_context",
                reason = "registry_shutting_down",
                "refusing to start compaction event forwarder"
            );
            if let Some(overview) = turn_gate.release() {
                self.shared.hub.publish_runtime_overview(overview).await;
            }
            return Err(HarnessError::Protocol(
                "server is shutting down; refusing to start compaction forwarder".into(),
            ));
        };
        if let Err(error) = harness.compact_thread(&handle).await {
            if let Some(overview) = turn_gate.release() {
                self.shared.hub.publish_runtime_overview(overview).await;
            }
            return Err(error);
        }
        info!(
            %project_id,
            %thread_id,
            harness_thread_id = %handle.harness_thread_id,
            ack_elapsed_ms = request_started.elapsed().as_millis(),
            "harness accepted context compaction request"
        );

        launch_event_forwarder(
            shared,
            thread_id,
            project_id,
            stream,
            ctx,
            Some(turn_gate),
            forwarder_permit,
        );
        Ok(())
    }

    pub(crate) async fn open_subagent_link(
        &self,
        project_id: ProjectId,
        parent_thread_id: ThreadId,
        item_id: ItemId,
    ) -> Result<Option<ThreadId>, HarnessError> {
        let Some((spawned_by_turn_id, info)) =
            resolve_subagent_link_info(&self.shared, project_id, parent_thread_id, item_id).await?
        else {
            return Ok(None);
        };
        if let Some(parent_target) = resolve_reverse_subagent_target(
            &self.shared,
            project_id,
            parent_thread_id,
            &info.native_thread_id,
        )
        .await?
        {
            return Ok(Some(parent_target));
        }

        let (result, receiver) = tokio::sync::oneshot::channel();
        enqueue_subagent_materialization(
            parent_thread_id,
            SubagentMaterializationJob {
                project_id,
                spawned_by_turn_id,
                item_id,
                origin: "explicit_open",
                info,
                result: Some(result),
            },
            self.shared.clone(),
        )
        .await;
        receiver.await.map_err(|_| {
            HarnessError::Protocol(format!(
                "sub-agent materialization queue closed for item {item_id}"
            ))
        })?
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
            .shared
            .active_harness(project_id)
            .await
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
            .unwrap_or_else(|| ThreadHandle::detached(thread_id, harness_thread_id));
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
            .unwrap_or_else(|| ThreadHandle::detached(thread_id, harness_thread_id));
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

    pub async fn list_providers(
        &self,
        config: &ProjectConfig,
    ) -> Result<Vec<HarnessProvider>, HarnessError> {
        let harness = self.get_or_create_harness(config.id, config).await?;
        harness.list_providers().await
    }

    /// The harness's own version, for identifying it to a provider's `/models` endpoint (§8.3).
    pub async fn client_version(&self, config: &ProjectConfig) -> Option<String> {
        // A version is a nicety, not a reason to fail a catalog refresh: an unreachable harness is
        // already reported by the calls that need one.
        let harness = self.get_or_create_harness(config.id, config).await.ok()?;
        harness.client_version()
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
        self.stop_passive_subagent_monitor(thread_id).await?;
        let harness = self.get_or_create_harness(config.id, config).await?;
        let handle = self
            .get_thread_handle(thread_id)
            .await
            .unwrap_or_else(|| ThreadHandle::detached(thread_id, harness_thread_id));
        harness.delete_thread(&handle).await?;
        self.retire_thread(thread_id).await;
        Ok(())
    }

    pub async fn get_thread_handle(&self, thread_id: ThreadId) -> Option<ThreadHandle> {
        let threads = self.shared.threads.lock().await;
        threads
            .get(&thread_id)
            .map(|binding| binding.handle.clone())
    }

    pub async fn get_thread_native_model(&self, thread_id: ThreadId) -> Option<ModelRef> {
        let threads = self.shared.threads.lock().await;
        threads
            .get(&thread_id)
            .and_then(|binding| binding.native_model.clone())
    }

    pub async fn get_project_for_thread(&self, thread_id: ThreadId) -> Option<ProjectId> {
        let threads = self.shared.threads.lock().await;
        threads.get(&thread_id).map(|binding| binding.project)
    }

    pub async fn thread_has_active_turn(&self, thread_id: ThreadId) -> bool {
        self.shared.runtime.has_active_turn(thread_id)
    }

    pub async fn thread_has_passive_monitor(&self, thread_id: ThreadId) -> bool {
        if self
            .shared
            .passive_monitors
            .lock()
            .await
            .contains(&thread_id)
        {
            return true;
        }
        self.shared.passive_monitor_tasks.contains(thread_id).await
    }

    pub async fn stop_passive_subagent_monitor(
        &self,
        thread_id: ThreadId,
    ) -> Result<(), HarnessError> {
        let monitor_exists = {
            let monitors = self.shared.passive_monitors.lock().await;
            if !monitors.contains(&thread_id) {
                self.shared
                    .passive_subagent_metadata
                    .lock()
                    .await
                    .remove(&thread_id);
                false
            } else {
                let mut metadata = self.shared.passive_subagent_metadata.lock().await;
                let entry = metadata.entry(thread_id).or_default();
                entry.cancelled = true;
                entry.lifecycle_notify.notify_one();
                true
            }
        };
        if !monitor_exists && !self.shared.passive_monitor_tasks.contains(thread_id).await {
            return Ok(());
        }

        let deadline = tokio::time::Instant::now() + PASSIVE_MONITOR_STOP_TIMEOUT;
        let mut completions = self.shared.passive_monitor_tasks.subscribe();
        loop {
            if !self.shared.passive_monitor_tasks.contains(thread_id).await {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, completions.changed())
                .await
                .is_err()
            {
                return Err(HarnessError::Protocol(format!(
                    "timed out stopping passive sub-agent monitor for thread {thread_id}"
                )));
            }
        }
    }

    pub async fn forget_thread(&self, thread_id: ThreadId) {
        let mut threads = self.shared.threads.lock().await;
        threads.remove(&thread_id);
    }

    pub async fn retire_thread(&self, thread_id: ThreadId) {
        self.forget_thread(thread_id).await;
        self.shared
            .runtime
            .forget_threads(&HashSet::from([thread_id]));
        publish_runtime_overview(&self.shared).await;
    }

    /// Stop every project harness after HTTP traffic has drained.
    ///
    /// The shutdown flag and drained map share one mutex with harness creation, so a concurrent
    /// factory call either finishes before this snapshot or is refused after it. Individual
    /// harness failures are isolated: every project receives a shutdown request before an
    /// aggregate error is returned.
    pub async fn shutdown(&self) -> Result<(), HarnessError> {
        let harnesses = {
            let mut harnesses = self.shared.harnesses.lock().await;
            harnesses.begin_shutdown()
        };
        if harnesses.is_empty() {
            debug!("harness registry shutdown found no active harnesses");
        } else {
            info!(
                harness_count = harnesses.len(),
                "shutting down project harnesses"
            );
        }
        let results = join_all(
            harnesses
                .into_iter()
                .map(|(project_id, harness)| async move {
                    let started = Instant::now();
                    let result = match timeout(HARNESS_SHUTDOWN_TIMEOUT, harness.shutdown()).await {
                        Ok(result) => result,
                        Err(_) => Err(HarnessError::Timeout(format!(
                            "harness shutdown exceeded {} ms",
                            HARNESS_SHUTDOWN_TIMEOUT.as_millis()
                        ))),
                    };
                    match &result {
                        Ok(()) => info!(
                            %project_id,
                            elapsed_ms = started.elapsed().as_millis(),
                            "project harness shutdown completed"
                        ),
                        Err(error) => error!(
                            %project_id,
                            %error,
                            elapsed_ms = started.elapsed().as_millis(),
                            "project harness shutdown failed"
                        ),
                    }
                    (project_id, result)
                }),
        )
        .await;

        let mut failures = results
            .into_iter()
            .filter_map(|(project_id, result)| result.err().map(|error| (project_id, error)))
            .map(|(project_id, error)| format!("{project_id}: {error}"))
            .collect::<Vec<_>>();
        if let Err(error) = self
            .shared
            .background_tasks
            .close_and_wait(BACKGROUND_TASK_SHUTDOWN_TIMEOUT)
            .await
        {
            error!(%error, "registry background tasks did not drain during server shutdown");
            failures.push(error.to_string());
        }
        if timeout(LEDGER_SHUTDOWN_TIMEOUT, self.shared.ledger.shutdown())
            .await
            .is_err()
        {
            let error = format!(
                "token ledger did not shut down within {} ms",
                LEDGER_SHUTDOWN_TIMEOUT.as_millis()
            );
            error!(
                action = "shutdown_token_ledger",
                timeout_ms = LEDGER_SHUTDOWN_TIMEOUT.as_millis(),
                "{error}"
            );
            failures.push(error);
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(HarnessError::Protocol(format!(
                "one or more registry components failed to shut down: {}",
                failures.join("; ")
            )))
        }
    }

    pub async fn delete_project(&self, project_id: ProjectId) -> Result<(), HarnessError> {
        let thread_ids = self
            .shared
            .threads
            .lock()
            .await
            .iter()
            .filter_map(|(thread_id, binding)| {
                (binding.project == project_id).then_some(*thread_id)
            })
            .collect::<HashSet<_>>();
        for thread_id in &thread_ids {
            self.stop_passive_subagent_monitor(*thread_id).await?;
        }

        let harness = {
            let mut harnesses = self.shared.harnesses.lock().await;
            harnesses.begin_delete(project_id)?
        };
        if let Some(harness) = harness {
            if let Err(error) = harness.shutdown().await {
                let mut harnesses = self.shared.harnesses.lock().await;
                harnesses.rollback_delete(project_id, harness);
                return Err(error);
            }
            let mut harnesses = self.shared.harnesses.lock().await;
            harnesses.finish_delete(project_id, &harness);
        }

        let removed_thread_ids = {
            let mut threads = self.shared.threads.lock().await;
            let removed_thread_ids = threads
                .iter()
                .filter_map(|(thread_id, binding)| {
                    (binding.project == project_id).then_some(*thread_id)
                })
                .collect::<HashSet<_>>();
            threads.retain(|_, binding| binding.project != project_id);
            removed_thread_ids
        };

        self.shared.runtime.forget_threads(&removed_thread_ids);
        publish_runtime_overview(&self.shared).await;

        Ok(())
    }
}

async fn lock_project_lifecycle(
    locks: &ProjectLifecycleLocks,
    project_id: ProjectId,
) -> OwnedMutexGuard<()> {
    let lock = {
        let mut locks = locks.lock().await;
        locks.retain(|_, lock| lock.strong_count() > 0);
        match locks.get(&project_id).and_then(Weak::upgrade) {
            Some(lock) => lock,
            None => {
                let lock = Arc::new(Mutex::new(()));
                locks.insert(project_id, Arc::downgrade(&lock));
                lock
            }
        }
    };
    lock.lock_owned().await
}

async fn publish_runtime_overview(shared: &RegistryShared) {
    shared
        .hub
        .publish_runtime_overview(shared.runtime.current_overview())
        .await;
}

#[derive(Clone)]
struct SubagentActivityInfo {
    native_thread_id: String,
    agent_name: Option<String>,
    agent_path: Option<String>,
    initial_prompt: Option<String>,
    title: Option<String>,
    action: SubagentAction,
    status: Option<SubagentStatus>,
    fallback: Option<SubagentFallbackTranscript>,
}

type SubagentMaterializationResult = Result<Option<ThreadId>, HarnessError>;

struct SubagentMaterializationJob {
    project_id: ProjectId,
    spawned_by_turn_id: TurnId,
    item_id: ItemId,
    origin: &'static str,
    info: SubagentActivityInfo,
    result: Option<oneshot::Sender<SubagentMaterializationResult>>,
}

#[derive(Clone)]
struct SubagentFallbackTranscript {
    message: String,
    status: SubagentStatus,
}

struct FallbackTurnContext {
    user_input: UserInput,
    model: ModelRef,
    mode: Mode,
}

impl From<&TurnContext> for FallbackTurnContext {
    fn from(ctx: &TurnContext) -> Self {
        Self {
            user_input: ctx.user_input.clone(),
            model: ctx.model.clone(),
            mode: ctx.mode,
        }
    }
}

fn subagent_activity_info(item: &Item) -> Option<SubagentActivityInfo> {
    match &item.payload {
        ItemPayload::Activity {
            title, subagent, ..
        } => subagent_link_info(subagent.as_ref(), Some(title.clone()), None),
        ItemPayload::ToolCall {
            input, subagent, ..
        } => subagent_link_info(
            subagent.as_ref(),
            None,
            subagent_prompt_from_tool_input(input),
        ),
        _ => None,
    }
}

async fn resolve_subagent_link_info(
    shared: &RegistryShared,
    project_id: ProjectId,
    parent_thread_id: ThreadId,
    item_id: ItemId,
) -> Result<Option<(TurnId, SubagentActivityInfo)>, HarnessError> {
    let parent_exists = shared
        .store
        .load_thread(project_id, parent_thread_id)
        .await
        .map_err(|error| HarnessError::Protocol(error.to_string()))?
        .is_some();
    if !parent_exists {
        return Err(HarnessError::ThreadNotFound(parent_thread_id));
    }

    for event in shared
        .runtime
        .live_item_events(parent_thread_id, item_id)
        .into_iter()
        .rev()
    {
        match event {
            AgentEvent::ItemCompleted { turn, item, .. } => {
                if let Some(info) = subagent_activity_info(&item) {
                    return Ok(Some((turn, info)));
                }
            }
            AgentEvent::ItemStarted { turn, item, .. } => {
                if let Some(info) = subagent_start_info(&item) {
                    return Ok(Some((turn, info)));
                }
            }
            _ => {}
        }
    }

    let turns = shared
        .store
        .load_all_turns(project_id, parent_thread_id)
        .await
        .map_err(|error| HarnessError::Protocol(error.to_string()))?;
    for turn in turns.into_iter().rev() {
        if let Some(info) = turn
            .items
            .iter()
            .rev()
            .find(|item| item.id == item_id)
            .and_then(subagent_activity_info)
        {
            return Ok(Some((turn.id, info)));
        }
    }
    Ok(None)
}

async fn resolve_reverse_subagent_target(
    shared: &RegistryShared,
    project_id: ProjectId,
    source_thread_id: ThreadId,
    native_thread_id: &str,
) -> Result<Option<ThreadId>, HarnessError> {
    let graph = load_thread_graph(&shared.store, project_id)
        .await
        .map_err(|error| HarnessError::Protocol(error.to_string()))?;
    let Some(source) = graph.get(&source_thread_id) else {
        return Err(HarnessError::ThreadNotFound(source_thread_id));
    };
    let target = graph
        .values()
        .find(|thread| thread.harness_thread_id == native_thread_id);
    Ok(target
        .filter(|target| source.parent_thread_id == Some(target.id))
        .map(|target| target.id))
}

fn subagent_start_info(item: &giskard_core::item::ItemStart) -> Option<SubagentActivityInfo> {
    let tool = item.tool.as_ref()?;
    subagent_link_info(
        tool.subagent.as_ref(),
        None,
        subagent_prompt_from_tool_input(&tool.input),
    )
}

fn subagent_link_info(
    subagent: Option<&giskard_core::item::SubagentLink>,
    title: Option<String>,
    prompt_fallback: Option<String>,
) -> Option<SubagentActivityInfo> {
    let subagent = subagent?;
    let native_thread_id = trimmed_non_empty(&subagent.harness_thread_id)?;
    let agent_path = subagent
        .path
        .as_deref()
        .and_then(trimmed_non_empty)
        .map(ToOwned::to_owned);
    let initial_prompt = subagent
        .initial_prompt
        .as_deref()
        .and_then(trimmed_non_empty)
        .map(ToOwned::to_owned)
        .or(prompt_fallback);
    Some(SubagentActivityInfo {
        native_thread_id: native_thread_id.to_owned(),
        agent_name: None,
        agent_path,
        initial_prompt,
        title,
        action: subagent.action,
        status: subagent.status,
        fallback: subagent_fallback_transcript(subagent),
    })
}

fn subagent_prompt_from_tool_input(input: &serde_json::Value) -> Option<String> {
    for key in ["prompt", "message", "task", "instructions"] {
        if let Some(prompt) = input
            .get(key)
            .and_then(serde_json::Value::as_str)
            .and_then(trimmed_non_empty)
        {
            return Some(prompt.to_owned());
        }
    }
    input
        .get("items")
        .and_then(serde_json::Value::as_array)
        .and_then(|items| {
            items.iter().find_map(|item| {
                item.get("text")
                    .and_then(serde_json::Value::as_str)
                    .and_then(trimmed_non_empty)
                    .map(ToOwned::to_owned)
            })
        })
}

fn subagent_fallback_transcript(
    subagent: &giskard_core::item::SubagentLink,
) -> Option<SubagentFallbackTranscript> {
    terminal_subagent_fallback(subagent.status, subagent.message.as_deref())
}

fn terminal_subagent_fallback(
    status: Option<SubagentStatus>,
    message: Option<&str>,
) -> Option<SubagentFallbackTranscript> {
    let status = status?;
    if !matches!(
        status,
        SubagentStatus::Completed
            | SubagentStatus::Interrupted
            | SubagentStatus::Failed
            | SubagentStatus::Shutdown
            | SubagentStatus::NotFound
    ) {
        return None;
    }
    let message = message.and_then(trimmed_non_empty)?.to_owned();
    Some(SubagentFallbackTranscript { message, status })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SubagentMonitorPolicy {
    should_monitor: bool,
    terminal_observed: bool,
    active_observed: bool,
    pre_turn_timeout: Option<Duration>,
}

struct SubagentObservation {
    effective_model: ModelRef,
    mode: Mode,
    initial_prompt: Option<String>,
    policy: SubagentMonitorPolicy,
    fallback: Option<SubagentFallbackTranscript>,
}

fn subagent_monitor_policy(
    action: Option<SubagentAction>,
    status: Option<SubagentStatus>,
) -> SubagentMonitorPolicy {
    let terminal_observed = subagent_observation_is_terminal(action, status);
    let active_observed = !terminal_observed
        && (matches!(
            status,
            Some(SubagentStatus::Pending | SubagentStatus::Running)
        ) || matches!(
            action,
            Some(SubagentAction::Spawned | SubagentAction::Started | SubagentAction::Interacted)
        ));
    SubagentMonitorPolicy {
        should_monitor: active_observed,
        terminal_observed,
        active_observed,
        // Active evidence gets a generous no-event safety bound so a missed terminal event cannot
        // block direct follow-ups forever. Any stream event restarts the bound, and once a native
        // turn begins normal turn completion—not this pre-turn timeout—owns the lifecycle.
        pre_turn_timeout: active_observed.then_some(ACTIVE_SUBAGENT_PRE_TURN_IDLE_TIMEOUT),
    }
}

fn subagent_observation_is_terminal(
    action: Option<SubagentAction>,
    status: Option<SubagentStatus>,
) -> bool {
    action == Some(SubagentAction::Interrupted)
        || matches!(
            status,
            Some(
                SubagentStatus::Completed
                    | SubagentStatus::Interrupted
                    | SubagentStatus::Failed
                    | SubagentStatus::Shutdown
                    | SubagentStatus::NotFound
            )
        )
}

fn subagent_thread_title(info: &SubagentActivityInfo) -> String {
    let raw = info
        .agent_name
        .as_ref()
        .map(|name| format!("Sub-agent: {name}"))
        .or_else(|| {
            info.agent_path
                .as_deref()
                .and_then(subagent_path_leaf)
                .map(|path| format!("Sub-agent: {path}"))
        })
        .or_else(|| info.title.clone())
        .unwrap_or_else(|| "Sub-agent".to_string());
    normalize_subagent_title(raw)
}

fn subagent_path_leaf(path: &str) -> Option<&str> {
    path.rsplit('/')
        .map(str::trim)
        .find(|part| !part.is_empty())
}

fn normalize_subagent_title(raw: String) -> String {
    let title = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    let title = if title.is_empty() {
        "Sub-agent".to_string()
    } else {
        title
    };
    title.chars().take(120).collect()
}

fn subagent_info_with_agent_name(
    mut info: SubagentActivityInfo,
    agent_name: Option<String>,
) -> SubagentActivityInfo {
    if let Some(agent_name) = agent_name {
        info.agent_name = Some(agent_name);
    }
    info
}

async fn update_passive_subagent_metadata(
    map: &PassiveSubagentMetadataMap,
    thread_id: ThreadId,
    initial_prompt: Option<String>,
    fallback: Option<SubagentFallbackTranscript>,
    signal: LifecycleSignal,
) {
    let mut metadata = map.lock().await;
    let entry = metadata.entry(thread_id).or_default();
    merge_passive_subagent_metadata(entry, initial_prompt, fallback, signal);
}

fn merge_passive_subagent_metadata(
    entry: &mut PassiveSubagentMetadata,
    initial_prompt: Option<String>,
    fallback: Option<SubagentFallbackTranscript>,
    signal: LifecycleSignal,
) {
    if let Some(initial_prompt) = initial_prompt {
        entry.initial_prompt = Some(initial_prompt);
    }
    if let Some(fallback) = fallback {
        entry.fallback = Some(fallback);
    }
    match signal {
        LifecycleSignal::None => {}
        LifecycleSignal::Active => {
            entry.active_lifecycle_observed = true;
            entry.lifecycle_notify.notify_one();
        }
        LifecycleSignal::Terminal => {
            entry.terminal_observed = true;
            entry.lifecycle_notify.notify_one();
        }
    }
}

async fn register_passive_subagent_monitor(
    passive_monitors: &Arc<Mutex<HashSet<ThreadId>>>,
    passive_subagent_metadata: &PassiveSubagentMetadataMap,
    passive_monitor_tasks: &PassiveMonitorTasks,
    thread_id: ThreadId,
    initial_prompt: Option<String>,
    fallback: Option<SubagentFallbackTranscript>,
    signal: LifecycleSignal,
) -> bool {
    // Monitor ownership and metadata are published atomically under the same lock order used by
    // terminal recovery. A terminal observation therefore either updates this monitor or runs
    // fallback recovery itself; it cannot slip between metadata creation and monitor insertion.
    let mut monitors = passive_monitors.lock().await;
    let inserted = monitors.insert(thread_id);
    let mut metadata = passive_subagent_metadata.lock().await;
    let entry = metadata.entry(thread_id).or_default();
    merge_passive_subagent_metadata(entry, initial_prompt, fallback, signal);
    if inserted {
        passive_monitor_tasks.register(thread_id).await;
    }
    inserted
}

async fn finish_passive_subagent_monitor_task(
    passive_monitor_tasks: &PassiveMonitorTasks,
    thread_id: ThreadId,
) {
    passive_monitor_tasks.finish(thread_id).await;
}

async fn take_passive_subagent_monitor_metadata(
    passive_monitors: &Arc<Mutex<HashSet<ThreadId>>>,
    passive_subagent_metadata: &PassiveSubagentMetadataMap,
    thread_id: ThreadId,
) -> Option<PassiveSubagentMetadata> {
    // Keep monitor ownership and metadata removal under one lock order. Terminal observations use
    // the same order, so either the live monitor receives the fallback or teardown claims it for
    // immediate recovery; there is no gap where a result can be attached to an exited forwarder.
    let mut monitors = passive_monitors.lock().await;
    monitors.remove(&thread_id);
    passive_subagent_metadata.lock().await.remove(&thread_id)
}

async fn refresh_passive_subagent_context(
    thread_id: ThreadId,
    ctx: &mut TurnContext,
) -> PassiveMonitorSignal {
    if ctx.kind != TurnContextKind::PassiveSubagent {
        return PassiveMonitorSignal::Continue;
    }
    let Some(metadata_map) = ctx.passive_subagent_metadata.as_ref() else {
        return PassiveMonitorSignal::Continue;
    };
    let Some(metadata) = metadata_map.lock().await.get(&thread_id).cloned() else {
        return PassiveMonitorSignal::Continue;
    };
    if let Some(initial_prompt) = metadata
        .initial_prompt
        .as_deref()
        .and_then(trimmed_non_empty)
    {
        ctx.user_input = UserInput::text(initial_prompt);
        ctx.passive_input_is_fallback = false;
    }
    if metadata.fallback.is_some() {
        ctx.subagent_fallback = metadata.fallback;
    }
    if metadata.active_lifecycle_observed {
        ctx.passive_pre_turn_timeout = Some(ACTIVE_SUBAGENT_PRE_TURN_IDLE_TIMEOUT);
    }
    if metadata.cancelled {
        PassiveMonitorSignal::Cancelled
    } else if metadata.terminal_observed {
        PassiveMonitorSignal::Terminal
    } else {
        PassiveMonitorSignal::Continue
    }
}

async fn materialize_subagent_thread(
    parent_thread_id: ThreadId,
    project_id: ProjectId,
    spawned_by_turn_id: TurnId,
    info: SubagentActivityInfo,
    shared: Arc<RegistryShared>,
) -> Result<Option<ThreadId>, HarnessError> {
    let _lifecycle_guard =
        lock_project_lifecycle(&shared.project_lifecycle_locks, project_id).await;
    let Some(project_config) = shared
        .store
        .load_project(project_id)
        .await
        .map_err(|error| HarnessError::Protocol(error.to_string()))?
    else {
        return Err(HarnessError::Protocol(format!(
            "project {project_id} disappeared while importing sub-agent"
        )));
    };
    let parent_file = shared
        .store
        .load_thread(project_id, parent_thread_id)
        .await
        .map_err(|error| HarnessError::Protocol(error.to_string()))?
        .ok_or_else(|| {
            HarnessError::Protocol(format!(
                "parent thread {parent_thread_id} disappeared while importing sub-agent"
            ))
        })?;
    let live_existing_id = shared
        .threads
        .lock()
        .await
        .iter()
        .find_map(|(thread_id, binding)| {
            (binding.project == project_id
                && binding.handle.harness_thread_id == info.native_thread_id)
                .then_some(*thread_id)
        });
    let (mut graph, existing) = if let Some(existing_id) = live_existing_id {
        let existing = shared
            .store
            .load_thread(project_id, existing_id)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?;
        (None, existing)
    } else {
        let graph = load_thread_graph(&shared.store, project_id)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?;
        let existing = graph
            .values()
            .find(|thread| thread.harness_thread_id == info.native_thread_id)
            .cloned();
        (Some(graph), existing)
    };

    if let Some(existing) = existing {
        // A reverse link is uncommon and needs the complete graph to distinguish a valid direct
        // parent from the same direct fields inside a dangling or cyclic ownership chain. Keep the
        // hot path for repeated child activity cheap, but make reverse classification identical
        // before and after a restart.
        if graph.is_none() && parent_file.parent_thread_id == Some(existing.id) {
            graph = Some(
                load_thread_graph(&shared.store, project_id)
                    .await
                    .map_err(|error| HarnessError::Protocol(error.to_string()))?,
            );
        }
        // A live binding has already passed the full ownership validation while it was imported.
        // Repeated `interacted` activity can therefore use its immutable direct ownership fields
        // instead of re-reading every thread file on the parent forwarder's hot path.
        let disposition = match graph.as_ref() {
            Some(graph) => classify_existing_link(graph, parent_thread_id, &existing),
            None if existing.id == parent_thread_id => ExistingLinkDisposition::SelfLink,
            None if existing.kind == ThreadKind::Primary || existing.parent_thread_id.is_none() => {
                ExistingLinkDisposition::PrimaryThread
            }
            None if existing.parent_thread_id != Some(parent_thread_id) => {
                ExistingLinkDisposition::DifferentParent
            }
            None => ExistingLinkDisposition::OwnedChild,
        };
        if disposition == ExistingLinkDisposition::Parent {
            debug!(
                %project_id,
                source_thread_id = %parent_thread_id,
                parent_thread_id = %existing.id,
                linked_harness_thread_id = %info.native_thread_id,
                "recognized reverse sub-agent activity targeting the existing parent"
            );
            return Ok(None);
        }
        if disposition != ExistingLinkDisposition::OwnedChild {
            warn!(
                %project_id,
                %parent_thread_id,
                existing_thread_id = %existing.id,
                existing_kind = ?existing.kind,
                existing_parent_thread_id = display_opt(existing.parent_thread_id),
                linked_harness_thread_id = %info.native_thread_id,
                disposition = ?disposition,
                reason = disposition.reason(),
                "ignoring sub-agent materialization for an existing thread with incompatible ownership"
            );
            return Ok(None);
        }
        let policy = subagent_monitor_policy(Some(info.action), info.status);
        let opened_agent_name = if policy.should_monitor {
            ensure_subagent_thread_open(&project_config, &existing, &shared).await?
        } else {
            shared
                .threads
                .lock()
                .await
                .get(&existing.id)
                .and_then(|binding| binding.handle.agent_name.clone())
        };
        let refreshed_info = subagent_info_with_agent_name(info.clone(), opened_agent_name);
        let desired_title = subagent_thread_title(&refreshed_info);
        if should_refresh_subagent_title(&existing.title, &desired_title) {
            shared
                .thread_metadata
                .mutate(project_id, existing.id, |thread| {
                    if should_refresh_subagent_title(&thread.title, &desired_title) {
                        thread.title = desired_title.clone();
                    }
                })
                .await
                .map_err(|error| HarnessError::Protocol(error.to_string()))?;
        }
        observe_external_subagent_with_context(
            project_id,
            existing.id,
            SubagentObservation {
                effective_model: existing.current_model.clone(),
                mode: existing.mode,
                initial_prompt: refreshed_info.initial_prompt,
                policy,
                fallback: refreshed_info.fallback,
            },
            shared,
        )
        .await?;
        return Ok(Some(existing.id));
    }

    let graph = match graph {
        Some(graph) => graph,
        None => load_thread_graph(&shared.store, project_id)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?,
    };
    if !parent_chain_is_valid(&graph, parent_thread_id) {
        warn!(
            %project_id,
            %parent_thread_id,
            linked_harness_thread_id = %info.native_thread_id,
            "refusing to materialize a sub-agent under an invalid parent chain"
        );
        return Ok(None);
    }

    let model = parent_file.current_model.clone();
    let mode = parent_file.mode;
    let context_window = parent_file.context_window;
    let model_context_windows = parent_file.model_context_windows.clone();
    let permission_preset = parent_file.permission_preset;
    let model_efforts = parent_file.model_efforts.clone();

    let harness = shared
        .active_harness(project_id)
        .await
        .ok_or(HarnessError::ThreadNotFound(parent_thread_id))?;
    // The harness already runs this child inside its parent's turn, so its cwd is the parent's
    // workspace. Passing the project's checkout instead would be ignored while the child is live
    // and applied on its next cold resume, moving the thread out of the worktree its own earlier
    // work is in.
    let workspace_root = subagent_workspace_root(&shared, &project_config, &parent_file).await?;
    let (updates, update_stream) = thread_update_channel();
    let handle = harness
        .open_thread(OpenThreadOptions {
            project: project_id,
            thread: None,
            workspace_root: workspace_root.into(),
            resume: Some(info.native_thread_id.clone()),
            resume_policy: ResumePolicy::RequireExisting,
            initial_model: Some(model.clone()),
            updates,
        })
        .await?;
    let restore_permit = shared.runtime.restoration_permit(handle.thread);
    // This path calls the harness directly rather than `open_thread_with_resume_policy`, so retain
    // the registry's harness-neutral strict-resume check even when the adapter also validates it.
    if handle.harness_thread_id != info.native_thread_id {
        return Err(HarnessError::Protocol(format!(
            "linked-thread resume returned native thread {} instead of {}",
            handle.harness_thread_id, info.native_thread_id
        )));
    }
    if let Some(native_parent) = handle.parent_harness_thread_id.as_deref()
        && native_parent != parent_file.harness_thread_id
    {
        warn!(
            %project_id,
            %parent_thread_id,
            proposed_parent_harness_thread_id = %parent_file.harness_thread_id,
            reported_parent_harness_thread_id = %native_parent,
            linked_harness_thread_id = %handle.harness_thread_id,
            "refusing to materialize a native thread under a mismatched parent"
        );
        return Ok(None);
    }
    let current_model = handle.resumed_model.clone().unwrap_or(model);
    let info = subagent_info_with_agent_name(info, handle.agent_name.clone());
    let now = Utc::now();
    let thread_file = ThreadFile {
        revision: 0,
        version: 1,
        id: handle.thread,
        project_id,
        title: subagent_thread_title(&info),
        harness_thread_id: handle.harness_thread_id.clone(),
        parent_thread_id: Some(parent_thread_id),
        spawned_by_turn_id: Some(spawned_by_turn_id),
        kind: ThreadKind::Subagent,
        mode,
        current_model: current_model.clone(),
        context_window,
        model_context_windows,
        permission_preset,
        model_efforts,
        tokens: giskard_core::token::TokenLedger::default(),
        created_at: now,
        updated_at: now,
        archived: false,
        git_workspace: None,
    };
    let thread_file = shared
        .thread_metadata
        .create(project_id, thread_file)
        .await
        .map_err(|error| HarnessError::Protocol(error.to_string()))?;
    // The bounded channel retains an early replay while the thread is being created. Start the
    // forwarder only after metadata exists, so restoration cannot race creation and be lost.
    drop(spawn_thread_update_forwarder(
        shared.clone(),
        project_id,
        handle.thread,
        update_stream,
        restore_permit,
    ));
    let native_model = Some(current_model.clone());
    shared.threads.lock().await.insert(
        handle.thread,
        ThreadBinding {
            project: project_id,
            handle: handle.clone(),
            native_model,
        },
    );
    // The thread and binding are durable even if observation setup below fails. Publish the
    // creation now so a retry cannot leave the catalog unaware of an already-existing child.
    shared
        .thread_metadata
        .publish_created(project_id, &thread_file)
        .await;
    let policy = subagent_monitor_policy(Some(info.action), info.status);
    observe_external_subagent_with_context(
        project_id,
        handle.thread,
        SubagentObservation {
            effective_model: current_model,
            mode,
            initial_prompt: info.initial_prompt,
            policy,
            fallback: info.fallback,
        },
        shared.clone(),
    )
    .await?;
    Ok(Some(handle.thread))
}

async fn enqueue_subagent_materialization(
    parent_thread_id: ThreadId,
    mut job: SubagentMaterializationJob,
    shared: Arc<RegistryShared>,
) {
    let worker_permit = {
        let mut queues = shared.subagent_materialization_queues.lock().await;
        let permit = if queues.contains_key(&parent_thread_id) {
            None
        } else {
            match shared.background_tasks.register() {
                Some(permit) => Some(permit),
                None => {
                    warn!(
                        project_id = %job.project_id,
                        %parent_thread_id,
                        turn_id = %job.spawned_by_turn_id,
                        item_id = %job.item_id,
                        origin = %job.origin,
                        reason = "registry_shutting_down",
                        "rejecting sub-agent materialization job"
                    );
                    if let Some(result) = job.result.take() {
                        let _ = result.send(Err(HarnessError::Protocol(
                            "server is shutting down; refusing sub-agent materialization".into(),
                        )));
                    }
                    return;
                }
            }
        };
        queues.entry(parent_thread_id).or_default().push_back(job);
        permit
    };
    if let Some(permit) = worker_permit {
        tokio::spawn(async move {
            let _permit = permit;
            run_subagent_materialization_queue(parent_thread_id, shared).await;
        });
    }
}

async fn run_subagent_materialization_queue(
    parent_thread_id: ThreadId,
    shared: Arc<RegistryShared>,
) {
    loop {
        let job = {
            let mut queues = shared.subagent_materialization_queues.lock().await;
            let job = queues
                .get_mut(&parent_thread_id)
                .and_then(VecDeque::pop_front);
            if job.is_none() {
                queues.remove(&parent_thread_id);
            }
            job
        };
        let Some(job) = job else {
            return;
        };
        let result = materialize_subagent_thread(
            parent_thread_id,
            job.project_id,
            job.spawned_by_turn_id,
            job.info,
            shared.clone(),
        )
        .await;
        match &result {
            Ok(Some(subagent_thread_id)) => {
                info!(
                    project_id = %job.project_id,
                    %parent_thread_id,
                    %subagent_thread_id,
                    turn = %job.spawned_by_turn_id,
                    item_id = %job.item_id,
                    origin = %job.origin,
                    "materialized sub-agent thread from linked activity"
                );
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    project_id = %job.project_id,
                    %parent_thread_id,
                    turn = %job.spawned_by_turn_id,
                    item_id = %job.item_id,
                    origin = %job.origin,
                    error = %error,
                    "failed to materialize sub-agent thread from linked activity"
                );
            }
        }
        if let Some(sender) = job.result {
            let _ = sender.send(result);
        }
    }
}

/// The workspace a sub-agent's harness thread is opened against: the nearest worktree in its
/// ownership chain, the project's workspace otherwise.
///
/// Takes the parent's thread file when the child is not persisted yet, and the child's own once it
/// is; [`inherited_git_workspace`] answers the same for both.
async fn subagent_workspace_root(
    shared: &RegistryShared,
    project_config: &ProjectConfig,
    thread: &ThreadFile,
) -> Result<String, HarnessError> {
    let worktree =
        crate::thread_graph::inherited_git_workspace(&shared.store, project_config.id, thread)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?;
    Ok(worktree
        .map(|worktree| worktree.workspace_root().to_string())
        .unwrap_or_else(|| {
            project_config
                .workspace_root
                .as_deref()
                .unwrap_or(&project_config.dir)
                .to_owned()
        }))
}

async fn ensure_subagent_thread_open(
    project_config: &ProjectConfig,
    thread_file: &ThreadFile,
    shared: &Arc<RegistryShared>,
) -> Result<Option<String>, HarnessError> {
    if let Some(binding) = shared.threads.lock().await.get(&thread_file.id) {
        return Ok(binding.handle.agent_name.clone());
    }
    let harness = shared
        .active_harness(project_config.id)
        .await
        .ok_or(HarnessError::ThreadNotFound(thread_file.id))?;
    // Reopening a persisted sub-agent is that cold resume: resolve from the chain, not from the
    // child's own record, which never names a worktree.
    let workspace_root = subagent_workspace_root(shared, project_config, thread_file).await?;
    let (updates, update_stream, restore_permit) = prepare_thread_updates(shared, thread_file.id);
    let handle = harness
        .open_thread(OpenThreadOptions {
            project: project_config.id,
            thread: Some(thread_file.id),
            workspace_root: workspace_root.into(),
            resume: Some(thread_file.harness_thread_id.clone()),
            resume_policy: ResumePolicy::RequireExisting,
            initial_model: Some(thread_file.current_model.clone()),
            updates,
        })
        .await?;
    // This path calls the harness directly rather than `open_thread_with_resume_policy`, so retain
    // the registry's harness-neutral strict-resume check even when the adapter also validates it.
    if handle.harness_thread_id != thread_file.harness_thread_id {
        return Err(HarnessError::Protocol(format!(
            "linked-thread resume returned native thread {} instead of {}",
            handle.harness_thread_id, thread_file.harness_thread_id
        )));
    }
    drop(spawn_thread_update_forwarder(
        shared.clone(),
        project_config.id,
        handle.thread,
        update_stream,
        restore_permit,
    ));
    let native_model = Some(
        handle
            .resumed_model
            .clone()
            .unwrap_or_else(|| thread_file.current_model.clone()),
    );
    let agent_name = handle.agent_name.clone();
    shared.threads.lock().await.insert(
        handle.thread,
        ThreadBinding {
            project: project_config.id,
            handle,
            native_model,
        },
    );
    Ok(agent_name)
}

async fn start_passive_subagent_monitor(
    thread_id: ThreadId,
    observation: SubagentObservation,
    shared: Arc<RegistryShared>,
) -> Result<(), HarnessError> {
    let SubagentObservation {
        effective_model,
        mode,
        initial_prompt,
        policy,
        fallback,
    } = observation;
    if !register_passive_subagent_monitor(
        &shared.passive_monitors,
        &shared.passive_subagent_metadata,
        &shared.passive_monitor_tasks,
        thread_id,
        initial_prompt.clone(),
        fallback.clone(),
        if policy.active_observed {
            LifecycleSignal::Active
        } else {
            LifecycleSignal::None
        },
    )
    .await
    {
        return Ok(());
    }

    let error_cleanup_shared = shared.clone();
    let result = async {
        let threads = shared.threads.lock().await;
        let binding = threads
            .get(&thread_id)
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let project_id = binding.project;
        let handle = binding.handle.clone();
        drop(threads);

        let harness = shared
            .active_harness(project_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;

        let stream = harness.subscribe(&handle);
        let cleanup_model = effective_model.clone();
        let cleanup_mode = mode;
        let prompt_text = initial_prompt.as_deref().and_then(trimmed_non_empty);
        let ctx = TurnContext {
            user_input: UserInput::text(prompt_text.unwrap_or("Sub-agent turn")),
            model: effective_model,
            mode,
            kind: TurnContextKind::PassiveSubagent,
            passive_input_is_fallback: prompt_text.is_none(),
            subagent_fallback: fallback,
            passive_subagent_metadata: Some(shared.passive_subagent_metadata.clone()),
            passive_pre_turn_timeout: policy.pre_turn_timeout,
        };

        info!(
            %project_id,
            %thread_id,
            harness_thread_id = %handle.harness_thread_id,
            "starting passive monitor for external harness turn"
        );

        let cleanup_shared = shared.clone();
        let cleanup_tasks = shared.passive_monitor_tasks.clone();
        let Some(permit) = shared.background_tasks.register() else {
            warn!(
                %project_id,
                %thread_id,
                action = "start_passive_subagent_monitor",
                reason = "registry_shutting_down",
                "refusing to start passive sub-agent monitor forwarder"
            );
            return Err(HarnessError::Protocol(
                "server is shutting down; refusing passive monitor forwarder".into(),
            ));
        };
        tokio::spawn(async move {
            let _permit = permit;
            forward_events(shared, thread_id, project_id, stream, ctx, None).await;
            if let Some(metadata) = take_passive_subagent_monitor_metadata(
                &cleanup_shared.passive_monitors,
                &cleanup_shared.passive_subagent_metadata,
                thread_id,
            )
            .await
            {
                if metadata.cancelled {
                    debug!(
                        %project_id,
                        %thread_id,
                        "passive sub-agent monitor cleanup skipped fallback after cancellation"
                    );
                } else if let Some(fallback) = metadata.fallback {
                    persist_terminal_subagent_fallback(
                        project_id,
                        thread_id,
                        cleanup_model,
                        cleanup_mode,
                        metadata.initial_prompt,
                        fallback,
                        cleanup_shared,
                    )
                    .await;
                }
            }
            finish_passive_subagent_monitor_task(&cleanup_tasks, thread_id).await;
        });

        Ok(())
    }
    .await;

    if result.is_err() {
        take_passive_subagent_monitor_metadata(
            &error_cleanup_shared.passive_monitors,
            &error_cleanup_shared.passive_subagent_metadata,
            thread_id,
        )
        .await;
        finish_passive_subagent_monitor_task(
            &error_cleanup_shared.passive_monitor_tasks,
            thread_id,
        )
        .await;
    }
    result
}

async fn observe_external_subagent_with_context(
    project_id: ProjectId,
    thread_id: ThreadId,
    observation: SubagentObservation,
    shared: Arc<RegistryShared>,
) -> Result<(), HarnessError> {
    if observation.policy.should_monitor {
        // Setup is cancellation-shielded after the child record has been persisted. The detached
        // task owns monitor registration and cleanup even if its HTTP importer disconnects.
        let task = launch_passive_subagent_monitor(thread_id, observation, shared);
        return match task.await {
            Ok(result) => result,
            Err(error) => {
                error!(
                    %project_id,
                    %thread_id,
                    %error,
                    "passive sub-agent monitor setup task failed"
                );
                Err(HarnessError::Protocol(format!(
                    "passive sub-agent monitor setup task failed: {error}"
                )))
            }
        };
    }

    if observation.policy.terminal_observed {
        return recover_terminal_subagent_fallback(
            project_id,
            thread_id,
            observation.effective_model,
            observation.mode,
            observation.initial_prompt,
            observation.fallback,
            shared,
        )
        .await;
    }

    debug!(
        %thread_id,
        "skipping passive monitor for sub-agent observation without active work"
    );
    Ok(())
}

fn launch_passive_subagent_monitor(
    thread_id: ThreadId,
    observation: SubagentObservation,
    shared: Arc<RegistryShared>,
) -> tokio::task::JoinHandle<Result<(), HarnessError>> {
    tokio::spawn(start_passive_subagent_monitor(
        thread_id,
        observation,
        shared,
    ))
}

async fn recover_terminal_subagent_fallback(
    project_id: ProjectId,
    thread_id: ThreadId,
    effective_model: ModelRef,
    mode: Mode,
    initial_prompt: Option<String>,
    fallback: Option<SubagentFallbackTranscript>,
    shared: Arc<RegistryShared>,
) -> Result<(), HarnessError> {
    let attached_to_monitor = {
        let monitors = shared.passive_monitors.lock().await;
        if monitors.contains(&thread_id) {
            update_passive_subagent_metadata(
                &shared.passive_subagent_metadata,
                thread_id,
                initial_prompt.clone(),
                fallback.clone(),
                LifecycleSignal::Terminal,
            )
            .await;
            true
        } else {
            false
        }
    };
    if attached_to_monitor {
        debug!(
            %thread_id,
            "attached terminal fallback to active passive sub-agent monitor"
        );
        return Ok(());
    }

    let Some(fallback) = fallback else {
        debug!(
            %thread_id,
            "terminal sub-agent observation requires no monitor or fallback recovery"
        );
        return Ok(());
    };

    persist_terminal_subagent_fallback(
        project_id,
        thread_id,
        effective_model,
        mode,
        initial_prompt,
        fallback,
        shared,
    )
    .await;
    Ok(())
}

async fn persist_terminal_subagent_fallback(
    project_id: ProjectId,
    thread_id: ThreadId,
    effective_model: ModelRef,
    mode: Mode,
    initial_prompt: Option<String>,
    fallback: SubagentFallbackTranscript,
    shared: Arc<RegistryShared>,
) {
    let prompt_text = initial_prompt.as_deref().and_then(trimmed_non_empty);
    let ctx = FallbackTurnContext {
        user_input: UserInput::text(prompt_text.unwrap_or("Sub-agent turn")),
        model: effective_model,
        mode,
    };
    let mut seen_turn_ids = persisted_turn_ids(&shared.store, project_id, thread_id).await;
    persist_subagent_fallback_transcript(
        thread_id,
        project_id,
        &ctx,
        fallback,
        &mut seen_turn_ids,
        &shared,
    )
    .await;
}

async fn broadcast_event_with_context(
    hub: &Arc<Hub>,
    project_id: ProjectId,
    thread_id: ThreadId,
    event: AgentEvent,
    ctx: &TurnContext,
) {
    broadcast_event_with_user_input(hub, project_id, thread_id, event, live_turn_user_input(ctx))
        .await;
}

async fn broadcast_event_with_user_input(
    hub: &Arc<Hub>,
    project_id: ProjectId,
    thread_id: ThreadId,
    event: AgentEvent,
    user_input: Option<UserInput>,
) {
    let agent_event = match event {
        AgentEvent::TurnStarted { thread, turn } => WireAgentEvent::TurnStarted {
            thread,
            turn,
            user_input,
        },
        other => {
            let event_kind = event_kind(&other);
            let event_turn = event_turn_id(&other);
            let event_item = event_item_id(&other);
            let Some(agent_event) = WireAgentEvent::from_agent_event(other) else {
                log_metadata_only_event_rejection(
                    project_id, thread_id, event_kind, event_turn, event_item,
                );
                return;
            };
            agent_event
        }
    };
    hub.broadcast(
        thread_id,
        ServerMessage::Event {
            thread_id,
            agent_event: Box::new(agent_event),
        },
    )
    .await;
}

#[derive(Default)]
struct SyntheticSubagentPrompt {
    item_id: Option<ItemId>,
    text: Option<String>,
}

async fn synthesize_passive_subagent_prompt_item(
    project_id: ProjectId,
    thread_id: ThreadId,
    turn: TurnId,
    ctx: &TurnContext,
    current_turn_items: &mut CurrentTurnItems,
    prompt: &mut SyntheticSubagentPrompt,
    shared: &RegistryShared,
) {
    let Some(text) = passive_subagent_prompt_text(ctx) else {
        return;
    };
    if prompt.text.as_deref() == Some(text.as_str()) {
        return;
    }
    let item_id = *prompt.item_id.get_or_insert_with(ItemId::new);
    prompt.text = Some(text.clone());
    let item = Item {
        id: item_id,
        harness_item_id: format!("subagent_prompt:{turn}"),
        payload: ItemPayload::UserMessage { text },
        created_at: Utc::now(),
    };
    current_turn_items.upsert_first(&item);
    let event = AgentEvent::ItemCompleted {
        thread: thread_id,
        turn,
        item,
    };
    let applied = shared.runtime.apply_event(thread_id, &event, true);
    debug!(%thread_id, event_sequence = display_opt(applied.sequence), "applied synthetic prompt event");
    publish_applied_runtime_effects(&shared.hub, thread_id, applied).await;
    broadcast_event_with_context(&shared.hub, project_id, thread_id, event, ctx).await;
}

enum PassivePreTurnOutcome {
    Event(Box<Result<AgentEvent, tokio::sync::broadcast::error::RecvError>>),
    EvidenceAdopted,
    Stop(PassivePreTurnStop),
}

enum PassivePreTurnStop {
    Cancelled,
    Terminal,
    TimedOut { timeout: Option<Duration> },
}

async fn passive_pre_turn_recv(
    stream: &mut giskard_harness::AgentEventStream,
    lifecycle_notify: Option<&Arc<Notify>>,
    thread_id: ThreadId,
    ctx: &mut TurnContext,
) -> PassivePreTurnOutcome {
    let wait_for_event = async {
        if let Some(notify) = lifecycle_notify {
            tokio::select! {
                biased;
                result = stream.recv() => Some(result),
                _ = notify.notified() => None,
            }
        } else {
            Some(stream.recv().await)
        }
    };
    let wait_result = if let Some(pre_turn_timeout) = ctx.passive_pre_turn_timeout {
        timeout(pre_turn_timeout, wait_for_event).await.ok()
    } else {
        Some(wait_for_event.await)
    };

    match wait_result {
        Some(Some(result)) => PassivePreTurnOutcome::Event(Box::new(result)),
        Some(None) => match refresh_passive_subagent_context(thread_id, ctx).await {
            PassiveMonitorSignal::Continue => PassivePreTurnOutcome::EvidenceAdopted,
            PassiveMonitorSignal::Cancelled => {
                PassivePreTurnOutcome::Stop(PassivePreTurnStop::Cancelled)
            }
            PassiveMonitorSignal::Terminal => {
                PassivePreTurnOutcome::Stop(PassivePreTurnStop::Terminal)
            }
        },
        None => {
            let elapsed_timeout = ctx.passive_pre_turn_timeout;
            match refresh_passive_subagent_context(thread_id, ctx).await {
                PassiveMonitorSignal::Cancelled => {
                    PassivePreTurnOutcome::Stop(PassivePreTurnStop::Cancelled)
                }
                PassiveMonitorSignal::Terminal => {
                    PassivePreTurnOutcome::Stop(PassivePreTurnStop::Terminal)
                }
                PassiveMonitorSignal::Continue => {
                    PassivePreTurnOutcome::Stop(PassivePreTurnStop::TimedOut {
                        timeout: elapsed_timeout,
                    })
                }
            }
        }
    }
}

fn launch_event_forwarder(
    shared: Arc<RegistryShared>,
    thread_id: ThreadId,
    project_id: ProjectId,
    stream: giskard_harness::AgentEventStream,
    ctx: TurnContext,
    turn_gate: Option<ThreadTurnLease>,
    permit: RegistryTaskPermit,
) {
    tokio::spawn(async move {
        let _permit = permit;
        forward_events(shared, thread_id, project_id, stream, ctx, turn_gate).await;
    });
}

async fn forward_events(
    shared: Arc<RegistryShared>,
    thread_id: ThreadId,
    project_id: ProjectId,
    mut stream: giskard_harness::AgentEventStream,
    mut ctx: TurnContext,
    mut turn_gate: Option<ThreadTurnLease>,
) {
    let hub = shared.hub.clone();
    let runtime = shared.runtime.clone();
    // Establish the authority once. Per-event permits must only observe this entry, never recreate
    // it after retirement.
    drop(runtime.restoration_permit(thread_id));
    let store = shared.store.clone();
    let mut turn_id: Option<TurnId> = None;
    let mut owned_turn: Option<TurnId> = None;
    let mut owned_turn_completed = false;
    let mut started_at = Utc::now();
    let mut current_turn_items = CurrentTurnItems::default();
    let mut diffs: Vec<giskard_core::FileDiff> = Vec::new();
    let mut seen_turn_ids = persisted_turn_ids(&store, project_id, thread_id).await;
    let mut seen_notices = HashSet::new();
    let mut item_ids_by_harness: HashMap<(TurnId, String), ItemId> = HashMap::new();
    let mut synthetic_subagent_prompt = SyntheticSubagentPrompt::default();
    let forwarder_started = Instant::now();
    let mut saw_context_compaction_marker = false;
    let mut stream_error: Option<String> = None;
    let passive_lifecycle_notify = if ctx.kind == TurnContextKind::PassiveSubagent {
        match ctx.passive_subagent_metadata.as_ref() {
            Some(metadata) => metadata
                .lock()
                .await
                .get(&thread_id)
                .map(|entry| entry.lifecycle_notify.clone()),
            None => None,
        }
    } else {
        None
    };
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
        let recv_result = if ctx.kind == TurnContextKind::PassiveSubagent
            && owned_turn.is_none()
            && turn_id.is_none()
        {
            match passive_pre_turn_recv(
                &mut stream,
                passive_lifecycle_notify.as_ref(),
                thread_id,
                &mut ctx,
            )
            .await
            {
                PassivePreTurnOutcome::Event(result) => *result,
                PassivePreTurnOutcome::EvidenceAdopted => {
                    debug!(
                        %project_id,
                        %thread_id,
                        timeout_ms = ctx
                            .passive_pre_turn_timeout
                            .map(|value| tracing::field::display(value.as_millis())),
                        "passive subagent monitor adopted active lifecycle evidence"
                    );
                    continue;
                }
                PassivePreTurnOutcome::Stop(stop) => {
                    if !matches!(stop, PassivePreTurnStop::Cancelled)
                        && let Some(fallback) = ctx.subagent_fallback.clone()
                    {
                        let fallback_ctx = FallbackTurnContext::from(&ctx);
                        persist_subagent_fallback_transcript(
                            thread_id,
                            project_id,
                            &fallback_ctx,
                            fallback,
                            &mut seen_turn_ids,
                            &shared,
                        )
                        .await;
                    }
                    match stop {
                        PassivePreTurnStop::Cancelled => info!(
                            %project_id,
                            %thread_id,
                            elapsed_ms = forwarder_started.elapsed().as_millis(),
                            "passive subagent monitor cancelled before observing a turn"
                        ),
                        PassivePreTurnStop::Terminal => info!(
                            %project_id,
                            %thread_id,
                            elapsed_ms = forwarder_started.elapsed().as_millis(),
                            "passive subagent monitor stopped after terminal observation before a turn"
                        ),
                        PassivePreTurnStop::TimedOut { timeout } => info!(
                            %project_id,
                            %thread_id,
                            timeout_ms = timeout.map(|value| value.as_millis()).unwrap_or_default(),
                            elapsed_ms = forwarder_started.elapsed().as_millis(),
                            "passive subagent monitor timed out before observing a turn"
                        ),
                    }
                    break ForwarderExitReason::StreamEndedWithoutTurn;
                }
            }
        } else {
            stream.recv().await
        };
        match recv_result {
            Ok(event) => {
                if ctx.kind == TurnContextKind::PassiveSubagent
                    && turn_gate.is_none()
                    && event_turn_id(&event).is_none()
                    && shared.runtime.has_active_turn(thread_id)
                {
                    warn!(
                        %project_id,
                        %thread_id,
                        event_kind = event_kind(&event),
                        "passive sub-agent forwarder yielded turnless event to an active forwarder"
                    );
                    break ForwarderExitReason::DuplicateForwarder;
                }
                if ctx.kind == TurnContextKind::PassiveSubagent
                    && owned_turn.is_none()
                    && turn_id.is_none()
                    && refresh_passive_subagent_context(thread_id, &mut ctx).await
                        == PassiveMonitorSignal::Cancelled
                {
                    info!(
                        %project_id,
                        %thread_id,
                        "passive subagent monitor cancelled before processing a queued event"
                    );
                    break ForwarderExitReason::StreamEndedWithoutTurn;
                }
                let event_thread = event.thread_id();
                if event_thread != thread_id {
                    log_foreign_thread_event_drop(project_id, thread_id, event_thread, &event);
                    continue;
                }

                if should_skip_duplicate_notice(&event, &mut seen_notices) {
                    debug!(
                        %project_id,
                        %thread_id,
                        event_turn_id = display_opt(event_turn_id(&event)),
                        "skipping duplicate harness notice"
                    );
                    continue;
                }

                if ctx.kind == TurnContextKind::PassiveSubagent
                    && turn_gate.is_none()
                    && let Some(passive_turn) = event_turn_id(&event)
                    && !seen_turn_ids.contains(&passive_turn)
                {
                    let handle = shared
                        .threads
                        .lock()
                        .await
                        .get(&thread_id)
                        .map(|binding| binding.handle.clone());
                    let Some(handle) = handle else {
                        error!(
                            %project_id,
                            %thread_id,
                            %passive_turn,
                            "passive sub-agent forwarder lost its thread binding"
                        );
                        break ForwarderExitReason::DuplicateForwarder;
                    };
                    match shared
                        .runtime
                        .reserve_turn(thread_id, turn_reservation(project_id, &handle, &ctx))
                    {
                        Ok(mut lease) => {
                            // The reservation changed the projection as well, so the publish below
                            // carries both transitions.
                            let _acknowledged = lease.acknowledge_turn(passive_turn);
                            turn_gate = Some(lease);
                            publish_runtime_overview(&shared).await;
                        }
                        Err(HarnessError::ThreadBusy { .. }) => {
                            warn!(
                                %project_id,
                                %thread_id,
                                %passive_turn,
                                event_kind = event_kind(&event),
                                "passive subscriber yielded to the existing turn forwarder"
                            );
                            break ForwarderExitReason::DuplicateForwarder;
                        }
                        Err(error) => {
                            error!(
                                %project_id,
                                %thread_id,
                                %passive_turn,
                                %error,
                                "passive subscriber could not reserve turn ownership"
                            );
                            break ForwarderExitReason::DuplicateForwarder;
                        }
                    }
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
                                log_cross_turn_event_drop(
                                    project_id,
                                    thread_id,
                                    owned,
                                    turn,
                                    &event,
                                    forwarder_started.elapsed().as_millis(),
                                );
                            }
                            continue;
                        }
                    } else if owned_turn_completed {
                        continue;
                    }
                } else if let Some(turn) = event_turn
                    && !seen_turn_ids.contains(&turn)
                {
                    owned_turn = Some(turn);
                    if !matches!(event, AgentEvent::TurnStarted { .. }) {
                        debug!(
                            %thread_id,
                            %turn,
                            "event forwarder attached to turn before seeing turn start"
                        );
                    }
                }

                // Normalize every admitted completed-item payload once before runtime state, wire
                // projection, current-turn assembly, or persistence can observe it. Command
                // terminality is handled separately: providers may send a nonterminal
                // ItemCompleted followed by a later terminal replacement.
                let is_completed_addressable_output = matches!(
                    &event,
                    AgentEvent::ItemCompleted { item, .. }
                        if matches!(&item.payload, ItemPayload::CommandExecution { .. } | ItemPayload::ToolCall { .. })
                );
                let (event, prepared_item_output, preparation_permit) =
                    if is_completed_addressable_output {
                        let Some(permit) = runtime.event_application_permit(thread_id) else {
                            break ForwarderExitReason::RuntimeAuthorityReplaced;
                        };
                        let preparation_diagnostics = completed_item_diagnostics(&event);
                        let preparation_runtime = runtime.clone();
                        match tokio::task::spawn_blocking(move || {
                            preparation_runtime.prepare_item_output(event)
                        })
                        .await
                        {
                            Ok((event, prepared)) => (event, prepared, Some(permit)),
                            Err(error) => {
                                tracing::error!(
                                    %project_id,
                                    %thread_id,
                                    turn_id = preparation_diagnostics.as_ref().map(|value| tracing::field::display(value.turn_id)),
                                    item_id = preparation_diagnostics.as_ref().map(|value| tracing::field::display(value.item_id)),
                                    harness_item_id = preparation_diagnostics.as_ref().map(|value| value.harness_item_id.as_str()),
                                    item_payload_kind = preparation_diagnostics.as_ref().map(|value| value.payload_kind),
                                    error = %error,
                                    "addressable item-output event preparation task failed"
                                );
                                break ForwarderExitReason::EventPreparationFailed;
                            }
                        }
                    } else {
                        (event, None, None)
                    };

                if let Some(turn) = event_turn
                    && seen_turn_ids.contains(&turn)
                {
                    let command_state_changed = if is_terminal_command_completion(&event) {
                        let before =
                            terminating_command_before_terminal_completion(&runtime, &event).await;
                        let applied = match preparation_permit.as_ref() {
                            Some(permit) => match shared.runtime.apply_prepared_event_if_current(
                                permit,
                                &event,
                                false,
                                prepared_item_output,
                            ) {
                                Some(applied) => applied,
                                None => break ForwarderExitReason::RuntimeAuthorityReplaced,
                            },
                            None => shared.runtime.apply_prepared_event(
                                thread_id,
                                &event,
                                false,
                                prepared_item_output,
                            ),
                        };
                        if let AgentEvent::ItemCompleted { turn, item, .. } = &event {
                            shared
                                .runtime
                                .remove_command_output(thread_id, *turn, item.id);
                            warn!(
                                %project_id,
                                %thread_id,
                                %turn,
                                item_id = %item.id,
                                harness_item_id = %item.harness_item_id,
                                "deferred durable command-output update for already-persisted turn"
                            );
                        }
                        log_command_completion_after_terminate(project_id, before.as_ref(), &event);
                        debug!(
                            %thread_id,
                            event_sequence = display_opt(applied.sequence),
                            event_kind = event_kind(&event),
                            "applied late terminal event to thread runtime"
                        );
                        let changed = applied.tasks_changed;
                        publish_applied_runtime_effects(&hub, thread_id, applied).await;
                        changed
                    } else {
                        log_ignored_seen_turn_running_task_start(project_id, &event);
                        false
                    };
                    if is_terminal_command_completion(&event) {
                        if !command_state_changed
                            && let AgentEvent::ItemCompleted { turn, item, .. } = &event
                        {
                            warn!(
                                %project_id,
                                %thread_id,
                                %turn,
                                item_id = %item.id,
                                harness_item_id = %item.harness_item_id,
                                "broadcasting terminal command completion for a persisted turn without matching running-task state"
                            );
                        }
                        if let Some(message) =
                            late_command_completion_message(thread_id, event.clone())
                        {
                            hub.broadcast(thread_id, message).await;
                        }
                    }
                    if let AgentEvent::ItemCompleted { turn, item, .. } = &event
                        && let ItemPayload::ToolCall { name, server, .. } = &item.payload
                    {
                        shared.runtime.remove_tool_output(thread_id, *turn, item.id);
                        if completed_tool_has_terminal_output(item) {
                            warn!(
                                %project_id,
                                %thread_id,
                                %turn,
                                item_id = %item.id,
                                harness_item_id = %item.harness_item_id,
                                tool_name = %name,
                                tool_server = server.as_deref(),
                                "ignoring completed tool output for an already-persisted turn"
                            );
                        }
                    }
                    if owned_turn_completed
                        && let Some(owned) = owned_turn
                        && !runtime.has_running_for_turn(thread_id, owned)
                    {
                        break ForwarderExitReason::AfterTurnCommandsDrained;
                    }
                    continue;
                }

                if owned_turn.is_none() && event_turn.is_none() {
                    let applied = shared.runtime.apply_event(thread_id, &event, false);
                    debug!(
                        %thread_id,
                        event_sequence = display_opt(applied.sequence),
                        event_kind = event_kind(&event),
                        "applied turnless agent event to thread runtime"
                    );
                    publish_applied_runtime_effects(&hub, thread_id, applied).await;
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
                            hub.broadcast_event(thread_id, event.clone()).await;
                        }
                        _ => {}
                    }
                    continue;
                }

                // Only admitted events may mutate lazy diff storage. Extract bodies after the
                // wrong-turn and already-persisted-turn exits, but before reconnect state,
                // persistence assembly, or browser projection can observe the event.
                let event = runtime.capture_event_diffs(thread_id, event);

                if ctx.kind == TurnContextKind::PassiveSubagent {
                    refresh_passive_subagent_context(thread_id, &mut ctx).await;
                    if let Some(turn) = event_turn
                        && !matches!(event, AgentEvent::TurnStarted { .. })
                    {
                        synthesize_passive_subagent_prompt_item(
                            project_id,
                            thread_id,
                            turn,
                            &ctx,
                            &mut current_turn_items,
                            &mut synthetic_subagent_prompt,
                            &shared,
                        )
                        .await;
                    }
                }

                if let AgentEvent::ContextWindowUpdated {
                    turn,
                    model,
                    context_window,
                    ..
                } = &event
                {
                    if model.provider != ctx.model.provider || model.model != ctx.model.model {
                        error!(
                            %project_id,
                            %thread_id,
                            turn = %turn,
                            expected_provider = %ctx.model.provider,
                            expected_model = %ctx.model.model,
                            event_provider = %model.provider,
                            event_model = %model.model,
                            "dropping context-window update for the wrong turn model"
                        );
                        continue;
                    }
                    persist_model_context_window(
                        &shared.thread_metadata,
                        project_id,
                        thread_id,
                        *turn,
                        model,
                        *context_window,
                    )
                    .await;
                    continue;
                }

                match &event {
                    AgentEvent::TurnStarted { turn, .. } => {
                        turn_id = Some(*turn);
                        started_at = Utc::now();
                        current_turn_items.rebuild_indexes();
                        if let Some(turn_gate) = turn_gate.as_mut()
                            && let Some(overview) = turn_gate.acknowledge_turn(*turn)
                        {
                            // A lease reserved before the harness named its turn learns the id
                            // here. Nothing else publishes this transition, so a dropped overview
                            // would leave every tab on the superseded revision.
                            shared.hub.publish_runtime_overview(overview).await;
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
                    AgentEvent::ItemStarted { item, turn, .. } => {
                        if let Some(info) = subagent_start_info(item) {
                            enqueue_subagent_materialization(
                                thread_id,
                                SubagentMaterializationJob {
                                    project_id,
                                    spawned_by_turn_id: *turn,
                                    item_id: item.id,
                                    origin: "item_started",
                                    info,
                                    result: None,
                                },
                                shared.clone(),
                            )
                            .await;
                        }
                    }
                    AgentEvent::ItemCompleted { item, turn, .. } => {
                        if let Some(info) = subagent_activity_info(item) {
                            enqueue_subagent_materialization(
                                thread_id,
                                SubagentMaterializationJob {
                                    project_id,
                                    spawned_by_turn_id: *turn,
                                    item_id: item.id,
                                    origin: "item_completed",
                                    info,
                                    result: None,
                                },
                                shared.clone(),
                            )
                            .await;
                        }
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

                // A harness may deliver an item for an unseen turn before TurnStarted. Start the
                // reconnect buffer from the first turn-scoped event and reuse it when the delayed
                // start arrives, otherwise a reload in that window loses the already-visible item.
                let mut append_to_live_buffer = true;
                if let Some(buffer_turn) = event_turn
                    && let Err(existing_turn) =
                        runtime.ensure_live_turn(thread_id, buffer_turn, live_turn_user_input(&ctx))
                {
                    if matches!(event, AgentEvent::TurnStarted { .. }) {
                        warn!(
                            %project_id,
                            %thread_id,
                            %buffer_turn,
                            %existing_turn,
                            "replacing a stale live buffer when a new turn started"
                        );
                        runtime.replace_live_turn(
                            thread_id,
                            buffer_turn,
                            live_turn_user_input(&ctx),
                        );
                    } else {
                        error!(
                            %project_id,
                            %thread_id,
                            %buffer_turn,
                            %existing_turn,
                            event_kind = event_kind(&event),
                            "not buffering an event for a different turn; live delivery and persistence continue"
                        );
                        append_to_live_buffer = false;
                    }
                }
                if completed.is_none() {
                    let applied = match preparation_permit.as_ref() {
                        Some(permit) => match shared.runtime.apply_prepared_event_if_current(
                            permit,
                            &event,
                            append_to_live_buffer,
                            prepared_item_output,
                        ) {
                            Some(applied) => applied,
                            None => break ForwarderExitReason::RuntimeAuthorityReplaced,
                        },
                        None => shared.runtime.apply_prepared_event(
                            thread_id,
                            &event,
                            append_to_live_buffer,
                            prepared_item_output,
                        ),
                    };
                    debug!(
                        %thread_id,
                        event_sequence = display_opt(applied.sequence),
                        event_kind = event_kind(&event),
                        "applied agent event to thread runtime"
                    );
                    publish_applied_runtime_effects(&hub, thread_id, applied).await;
                }

                if let Some((completed_turn, usage, status)) = completed {
                    info!(
                        %project_id,
                        %thread_id,
                        turn = %completed_turn,
                        started_turn = display_opt(turn_id),
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
                    let Some(tid) = complete_forwarded_turn(
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
                        &shared,
                        turn_gate.as_mut(),
                    )
                    .await
                    else {
                        break ForwarderExitReason::PersistenceBlocked;
                    };
                    owned_turn_completed = true;
                    hub.broadcast_event(thread_id, event).await;
                    if runtime.has_running_for_turn(thread_id, tid) {
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

                broadcast_event_with_context(&hub, project_id, thread_id, event, &ctx).await;

                if is_turn_start && let Some(turn) = event_turn {
                    synthesize_passive_subagent_prompt_item(
                        project_id,
                        thread_id,
                        turn,
                        &ctx,
                        &mut current_turn_items,
                        &mut synthetic_subagent_prompt,
                        &shared,
                    )
                    .await;
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
                    let Some(tid) = complete_forwarded_turn(
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
                        &shared,
                        turn_gate.as_mut(),
                    )
                    .await
                    else {
                        break ForwarderExitReason::PersistenceBlocked;
                    };
                    owned_turn_completed = true;
                    hub.broadcast_event(thread_id, completion_event).await;
                    if runtime.has_running_for_turn(thread_id, tid) {
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
                    let live_buffer_active = runtime.live_is_active(thread_id);
                    warn!(
                        %project_id,
                        %thread_id,
                        ?e,
                        owned_turn = display_opt(owned_turn),
                        turn_id = display_opt(turn_id),
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
                    let live_buffer_active = runtime.live_is_active(thread_id);
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
                        owned_turn = display_opt(owned_turn),
                        turn_id = display_opt(turn_id),
                        stream_error = display_opt(stream_error.as_deref()),
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
                    let Some(_) = complete_forwarded_turn(
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
                        &shared,
                        turn_gate.as_mut(),
                    )
                    .await
                    else {
                        break ForwarderExitReason::PersistenceBlocked;
                    };
                    owned_turn_completed = true;
                    hub.broadcast_event(thread_id, completion_event).await;
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
            owned_turn = display_opt(owned_turn),
            turn_id = display_opt(turn_id),
            exit_reason = forwarder_exit_reason_label(exit_reason),
            stream_error = display_opt(stream_error.as_deref()),
            items_buffered = current_turn_items.len(),
            diffs_buffered = diffs.len(),
            saw_context_compaction_marker,
            elapsed_ms = forwarder_started.elapsed().as_millis(),
            "event forwarder exited without turn completion; releasing active-turn ownership"
        );
    } else {
        debug!(
            %project_id,
            %thread_id,
            context_kind = turn_context_kind_label(ctx.kind),
            owned_turn = display_opt(owned_turn),
            turn_id = display_opt(turn_id),
            owned_turn_completed,
            turn_gate_held,
            exit_reason = forwarder_exit_reason_label(exit_reason),
            stream_error = display_opt(stream_error.as_deref()),
            elapsed_ms = forwarder_started.elapsed().as_millis(),
            "event forwarder exited"
        );
    }
    if let Some(turn_gate) = turn_gate.as_mut()
        && let Some(overview) = turn_gate.release()
    {
        shared.hub.publish_runtime_overview(overview).await;
    }
}

async fn persist_subagent_fallback_transcript(
    thread_id: ThreadId,
    project_id: ProjectId,
    ctx: &FallbackTurnContext,
    fallback: SubagentFallbackTranscript,
    seen_turn_ids: &mut HashSet<TurnId>,
    shared: &RegistryShared,
) {
    if !seen_turn_ids.is_empty() {
        debug!(
            %project_id,
            %thread_id,
            persisted_turn_count = seen_turn_ids.len(),
            "skipping sub-agent fallback transcript because history already exists"
        );
        return;
    }

    let turn_id = TurnId::new();
    let item = Item {
        id: ItemId::new(),
        harness_item_id: format!("subagent_fallback:{turn_id}"),
        payload: ItemPayload::AgentMessage {
            text: fallback.message,
        },
        created_at: Utc::now(),
    };
    let status = TurnStatus {
        kind: subagent_status_turn_kind(fallback.status),
        message: None,
    };
    let started_at = Utc::now();
    let turn = Turn {
        id: turn_id,
        user_input: ctx.user_input.clone(),
        items: vec![item.clone()],
        model: ctx.model.clone(),
        mode: ctx.mode,
        status: status.clone(),
        usage: giskard_core::token::TokenUsage::default(),
        diffs: Vec::new(),
        started_at,
        completed_at: Some(Utc::now()),
    };
    let outcome = persist_turn(
        &shared.thread_metadata,
        &shared.ledger,
        project_id,
        thread_id,
        &turn,
        &[],
    )
    .await;
    if !outcome.history_appended {
        return;
    }
    seen_turn_ids.insert(turn_id);

    for event in [
        AgentEvent::TurnStarted {
            thread: thread_id,
            turn: turn_id,
        },
        AgentEvent::ItemCompleted {
            thread: thread_id,
            turn: turn_id,
            item,
        },
        AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: turn_id,
            usage: giskard_core::token::TokenUsage::default(),
            status,
        },
    ] {
        let applied = shared.runtime.apply_event(thread_id, &event, false);
        debug!(
            %thread_id,
            event_sequence = display_opt(applied.sequence),
            event_kind = event_kind(&event),
            "applied fallback transcript event to thread runtime"
        );
        publish_applied_runtime_effects(&shared.hub, thread_id, applied).await;
        broadcast_event_with_user_input(
            &shared.hub,
            project_id,
            thread_id,
            event,
            Some(ctx.user_input.clone()),
        )
        .await;
    }
    info!(
        %project_id,
        %thread_id,
        turn = %turn_id,
        "persisted fallback transcript for completed sub-agent"
    );
}

fn subagent_status_turn_kind(status: SubagentStatus) -> TurnStatusKind {
    match status {
        SubagentStatus::Interrupted | SubagentStatus::Shutdown => TurnStatusKind::Interrupted,
        SubagentStatus::Failed | SubagentStatus::NotFound => TurnStatusKind::Failed,
        SubagentStatus::Pending | SubagentStatus::Running | SubagentStatus::Completed => {
            TurnStatusKind::Completed
        }
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
    shared: &RegistryShared,
    turn_gate: Option<&mut ThreadTurnLease>,
) -> Option<TurnId> {
    let tid = turn_id.unwrap_or(completed_turn);
    let item_count = current_turn_items.len();
    let diff_count = diffs.len();
    let has_context_compaction_marker = current_turn_items.iter().any(is_context_compaction_item);
    if ctx.kind == TurnContextKind::ManualCompaction {
        info!(
            %project_id,
            %thread_id,
            turn = %tid,
            completed_turn = %completed_turn,
            started_turn = display_opt(turn_id),
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
    let captured_diffs = shared.runtime.captured_diff_records(thread_id, tid);
    let persist_outcome = persist_turn(
        &shared.thread_metadata,
        &shared.ledger,
        project_id,
        thread_id,
        &turn,
        &captured_diffs,
    )
    .await;
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
    let completion_event = AgentEvent::TurnCompleted {
        thread: thread_id,
        turn: completed_turn,
        usage,
        status: status.clone(),
    };
    if persist_outcome.history_appended {
        seen_turn_ids.insert(tid);
        let applied = match turn_gate {
            Some(turn_gate) => turn_gate.commit_after_persistence(&completion_event),
            None => shared
                .runtime
                .settle_completed_turn(thread_id, &completion_event, None),
        };
        publish_applied_runtime_effects(&shared.hub, thread_id, applied).await;
    } else {
        let error = persist_outcome
            .history_error
            .clone()
            .unwrap_or_else(|| "turn history append failed".into());
        let applied = match turn_gate {
            Some(turn_gate) => {
                turn_gate.retain_after_persistence_failure(&completion_event, turn, error)
            }
            None => shared.runtime.settle_completed_turn(
                thread_id,
                &completion_event,
                Some((turn, error)),
            ),
        };
        publish_applied_runtime_effects(&shared.hub, thread_id, applied).await;
        shared
            .hub
            .broadcast(
                thread_id,
                ServerMessage::Error {
                    error: giskard_proto::ErrorInfo {
                        code: "turn_persistence_blocked".into(),
                        severity: giskard_proto::ErrorSeverity::Error,
                        message:
                            "The completed turn could not be saved; this thread remains blocked."
                                .into(),
                        detail: persist_outcome.history_error,
                        thread_id: Some(thread_id),
                        action: Some("persist_turn".into()),
                        request_id: None,
                        process_id: None,
                    },
                },
            )
            .await;
        return None;
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
    Some(tid)
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
        | AgentEvent::ContextWindowUpdated { turn, .. }
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

fn event_kind(event: &AgentEvent) -> &'static str {
    match event {
        AgentEvent::ThreadOpened { .. } => "thread_opened",
        AgentEvent::TurnStarted { .. } => "turn_started",
        AgentEvent::ContextWindowUpdated { .. } => "context_window_updated",
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

fn event_item_delta_kind(event: &AgentEvent) -> Option<&'static str> {
    match event {
        AgentEvent::ItemDelta {
            delta: ItemDelta::Text { .. },
            ..
        } => Some("text"),
        AgentEvent::ItemDelta {
            delta: ItemDelta::CommandOutput { .. },
            ..
        } => Some("command_output"),
        _ => None,
    }
}

struct CompletedItemDiagnostics {
    turn_id: TurnId,
    item_id: ItemId,
    harness_item_id: String,
    payload_kind: &'static str,
}

fn completed_item_diagnostics(event: &AgentEvent) -> Option<CompletedItemDiagnostics> {
    let AgentEvent::ItemCompleted { turn, item, .. } = event else {
        return None;
    };
    Some(CompletedItemDiagnostics {
        turn_id: *turn,
        item_id: item.id,
        harness_item_id: item.harness_item_id.clone(),
        payload_kind: match item.payload {
            ItemPayload::CommandExecution { .. } => "command_execution",
            ItemPayload::ToolCall { .. } => "tool_call",
            _ => "other",
        },
    })
}

fn log_foreign_thread_event_drop(
    project_id: ProjectId,
    thread_id: ThreadId,
    event_thread_id: ThreadId,
    event: &AgentEvent,
) {
    error!(
        %project_id,
        %thread_id,
        %event_thread_id,
        event_kind = event_kind(event),
        event_turn_id = display_opt(event_turn_id(event)),
        event_item_id = display_opt(event_item_id(event)),
        item_delta_kind = event_item_delta_kind(event),
        "dropping harness event for a different thread"
    );
}

fn log_metadata_only_event_rejection(
    project_id: ProjectId,
    thread_id: ThreadId,
    event_kind: &'static str,
    event_turn_id: Option<TurnId>,
    event_item_id: Option<ItemId>,
) {
    warn!(
        %project_id,
        %thread_id,
        event_kind,
        event_turn_id = display_opt(event_turn_id),
        event_item_id = display_opt(event_item_id),
        "refusing to broadcast a metadata-only event on the transcript stream"
    );
}

fn log_cross_turn_event_drop(
    project_id: ProjectId,
    thread_id: ThreadId,
    owned_turn: TurnId,
    event_turn: TurnId,
    event: &AgentEvent,
    elapsed_ms: u128,
) {
    warn!(
        %project_id,
        %thread_id,
        %owned_turn,
        %event_turn,
        event_kind = event_kind(event),
        event_item_id = display_opt(event_item_id(event)),
        item_delta_kind = event_item_delta_kind(event),
        elapsed_ms,
        "dropping harness event for a different turn on the same thread"
    );
}

fn log_ignored_seen_turn_running_task_start(project_id: ProjectId, event: &AgentEvent) {
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
        %project_id,
        thread_id = %thread,
        turn_id = %turn,
        item_id = %item.id,
        harness_item_id = %item.harness_item_id,
        process_id = display_opt(command.process_id.as_deref()),
        command = %command.command,
        status,
        "ignoring running command start for already-persisted turn"
    );
}

async fn terminating_command_before_terminal_completion(
    runtime: &ThreadRuntimeRegistry,
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

    let command = runtime.task_by_item(*thread, *turn, item.id)?;
    command.terminating.then_some(command)
}

fn log_command_completion_after_terminate(
    project_id: ProjectId,
    command: Option<&RunningTask>,
    event: &AgentEvent,
) {
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
        %project_id,
        thread_id = %thread,
        turn_id = %turn,
        item_id = %item.id,
        harness_item_id = %item.harness_item_id,
        process_id = display_opt(command.process_id.as_deref()),
        command = %command.command,
        status = %status,
        exit_code = display_opt(exit_code.as_ref()),
        duration_ms = display_opt(duration_ms.as_ref()),
        "command completed normally after stop request; Codex did not terminate the process"
    );
}

async fn publish_applied_runtime_effects(
    hub: &Hub,
    thread_id: ThreadId,
    applied: AppliedRuntimeEvent,
) {
    if let Some(request) = applied.request_state {
        hub.broadcast(thread_id, ServerMessage::RequestState(request))
            .await;
    }
    if let Some(tasks) = applied.running_tasks_if_changed {
        hub.broadcast(
            thread_id,
            ServerMessage::RunningTasks {
                thread_id,
                revision: tasks.revision,
                tasks: tasks.tasks,
            },
        )
        .await;
    }
    if let Some(overview) = applied.overview_if_changed {
        hub.publish_runtime_overview(overview).await;
    }
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

fn completed_tool_has_terminal_output(item: &Item) -> bool {
    let ItemPayload::ToolCall { output, status, .. } = &item.payload else {
        return false;
    };
    output.is_some() && !status.as_deref().is_some_and(tool_status_is_running)
}

fn late_command_completion_message(
    thread_id: ThreadId,
    event: AgentEvent,
) -> Option<ServerMessage> {
    let AgentEvent::ItemCompleted { thread, turn, item } = event else {
        return None;
    };
    let descriptor = match &item.payload {
        ItemPayload::CommandExecution {
            output,
            output_truncated,
            output_original_bytes,
            output_original_lines,
            ..
        } => {
            let (original_bytes, original_lines) = giskard_core::resolve_command_output_counts(
                output,
                *output_truncated,
                *output_original_bytes,
                *output_original_lines,
            );
            Some(giskard_core::CommandOutputDescriptor::from_durable(
                output,
                *output_truncated,
                original_bytes,
                original_lines,
                false,
            ))
        }
        _ => None,
    };
    Some(ServerMessage::Event {
        thread_id,
        agent_event: Box::new(WireAgentEvent::ItemCompleted {
            thread,
            turn,
            item: WireItem::from_item_with_command_output(item, descriptor),
        }),
    })
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

    fn upsert_first(&mut self, item: &Item) {
        self.items.retain(|existing| existing.id != item.id);
        self.items.insert(0, item.clone());
        self.rebuild_indexes();
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

/// Persist an effective context window reported by the harness for a turn's model.
async fn persist_model_context_window(
    thread_metadata: &ThreadMetadataService,
    project_id: ProjectId,
    thread_id: ThreadId,
    turn_id: TurnId,
    model: &ModelRef,
    context_window: u32,
) {
    let provider = model.provider.clone();
    let model_id = model.model.clone();
    let stored_model = model.clone();
    match thread_metadata
        .mutate(project_id, thread_id, move |tf| {
            tf.record_model_context_window(&stored_model, context_window);
        })
        .await
    {
        Ok(ThreadMutation::Changed { after, .. }) => info!(
            %project_id,
            %thread_id,
            %turn_id,
            metadata_revision = after.revision,
            provider = %provider,
            model = %model_id,
            context_window,
            "persisted harness-reported model context window"
        ),
        Ok(ThreadMutation::Unchanged { current }) => debug!(
            %project_id,
            %thread_id,
            %turn_id,
            metadata_revision = current.revision,
            provider = %provider,
            model = %model_id,
            context_window,
            "harness-reported model context window was already current"
        ),
        Ok(ThreadMutation::Missing) => warn!(
            %project_id,
            %thread_id,
            %turn_id,
            provider = %provider,
            model = %model_id,
            context_window,
            "thread file missing while persisting model context window"
        ),
        Err(error) => error!(
            %project_id,
            %thread_id,
            %turn_id,
            provider = %provider,
            model = %model_id,
            context_window,
            %error,
            "failed to persist harness-reported model context window"
        ),
    }
}

/// Append a completed `Turn` to the thread file, fold its usage into the thread ledger, persist
/// atomically (§7.1), and hand the usage delta to the global + project ledger actor (§10.2).
/// Best-effort: logs on failure.
#[derive(Clone, Debug, Default)]
struct PersistTurnOutcome {
    history_appended: bool,
    metadata_updated: bool,
    history_error: Option<String>,
}

async fn persist_turn(
    thread_metadata: &ThreadMetadataService,
    ledger: &LedgerHandle,
    project_id: ProjectId,
    thread_id: ThreadId,
    turn: &Turn,
    captured_diffs: &[giskard_core::CapturedDiffRecord],
) -> PersistTurnOutcome {
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
    let commit = match thread_metadata
        .append_turn_with_diffs(project_id, thread_id, turn, captured_diffs)
        .await
    {
        Ok(commit) => commit,
        Err(e) => {
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
            return PersistTurnOutcome {
                history_error: Some(e.to_string()),
                ..PersistTurnOutcome::default()
            };
        }
    };
    info!(
        %project_id,
        %thread_id,
        turn = %turn_id,
        status = ?status_kind,
        item_count,
        diff_count,
        started_at = %rfc3339(&started_at),
        completed_at = rfc3339_opt(completed_at.as_ref()),
        "appended completed turn to history"
    );

    match commit {
        TurnCommitOutcome::MetadataMutation(
            ThreadMutation::Changed { .. } | ThreadMutation::Unchanged { .. },
        ) => {}
        TurnCommitOutcome::MetadataMutation(ThreadMutation::Missing) => {
            warn!(
                %project_id,
                %thread_id,
                turn = %turn_id,
                "thread file missing on turn completion after history append"
            );
            return PersistTurnOutcome {
                history_appended: true,
                metadata_updated: false,
                history_error: None,
            };
        }
        TurnCommitOutcome::MetadataFailed(e) => {
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
                history_error: None,
            };
        }
    }
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

    PersistTurnOutcome {
        history_appended: true,
        metadata_updated: true,
        history_error: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use chrono::Utc;
    use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
    use giskard_core::error::HarnessError;
    use giskard_core::event::AgentEvent;
    use giskard_core::ids::{ApprovalId, ItemId, ProjectId, ServerRequestId, ThreadId, TurnId};
    use giskard_core::item::{
        CommandExecutionStart, Item, ItemDelta, ItemKind, ItemPayload, ItemStart, SubagentAction,
        SubagentStatus,
    };
    use giskard_core::model::ModelRef;
    use giskard_core::server_request::ServerRequest;
    use giskard_core::token::{TokenLedger, TokenUsage};
    use giskard_core::turn::{Mode, PermissionPreset, Turn, TurnStatus, TurnStatusKind};
    use giskard_core::user_input::UserInput;
    use giskard_harness::{
        AgentEventStream, AgentHarness, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
        ThreadUpdate,
    };
    use giskard_persist::PersistStore;
    use giskard_persist::store::{ProjectConfig, ThreadFile};
    use giskard_proto::{ServerMessage, WireAgentEvent};
    use tokio::sync::{Mutex, broadcast, mpsc};
    use tokio::task::JoinHandle;

    use super::{
        CurrentTurnItems, ProjectHarnessState, TurnContext, TurnContextKind,
        command_completion_is_normal_success, command_status_is_running,
        completed_tool_has_terminal_output, event_item_delta_kind, event_item_id, event_turn_id,
        forward_events, late_command_completion_message, log_cross_turn_event_drop,
        log_foreign_thread_event_drop, log_metadata_only_event_rejection,
        passive_subagent_prompt_text, persist_subagent_fallback_transcript, prepare_thread_updates,
        should_refresh_subagent_title, spawn_thread_update_forwarder, subagent_monitor_policy,
        subagent_path_leaf, take_passive_subagent_monitor_metadata, track_item_identity,
        turn_reservation, update_passive_subagent_metadata,
    };
    use crate::hub::Hub;
    use crate::ledger;
    use crate::test_logs::CapturedLogWriter;
    use crate::thread_runtime::ThreadRuntimeRegistry;

    fn capture_logs(log: impl FnOnce()) -> String {
        let output = Arc::new(StdMutex::new(Vec::new()));
        let writer_output = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(move || CapturedLogWriter(writer_output.clone()))
            .finish();
        tracing::subscriber::with_default(subscriber, log);
        String::from_utf8(output.lock().unwrap().clone()).unwrap()
    }

    fn capture_cross_turn_warning(event: &AgentEvent) -> String {
        capture_logs(|| {
            log_cross_turn_event_drop(
                ProjectId::new(),
                ThreadId::new(),
                TurnId::new(),
                event_turn_id(event).unwrap(),
                event,
                42,
            );
        })
    }

    #[test]
    fn cross_turn_item_delta_warning_reports_bare_identity_without_content() {
        let item_id = ItemId::new();
        let event = AgentEvent::ItemDelta {
            thread: ThreadId::new(),
            turn: TurnId::new(),
            item_id,
            delta: ItemDelta::CommandOutput {
                chunk: "sensitive output".into(),
            },
        };

        assert_eq!(event_item_id(&event), Some(item_id));
        assert_eq!(event_item_delta_kind(&event), Some("command_output"));

        let output = capture_cross_turn_warning(&event);
        assert!(
            output.contains(&format!("event_item_id={item_id}")),
            "{output}"
        );
        assert!(
            output.contains("item_delta_kind=\"command_output\""),
            "{output}"
        );
        assert!(output.contains("elapsed_ms=42"), "{output}");
        assert!(!output.contains("Some("), "{output}");
        assert!(!output.contains("sensitive output"), "{output}");

        let text_event = AgentEvent::ItemDelta {
            thread: ThreadId::new(),
            turn: TurnId::new(),
            item_id: ItemId::new(),
            delta: ItemDelta::Text {
                text: "sensitive text".into(),
            },
        };
        assert_eq!(event_item_delta_kind(&text_event), Some("text"));
        let output = capture_cross_turn_warning(&text_event);
        assert!(output.contains("item_delta_kind=\"text\""), "{output}");
        assert!(!output.contains("sensitive text"), "{output}");

        let turn_event = AgentEvent::TurnStarted {
            thread: ThreadId::new(),
            turn: TurnId::new(),
        };
        let output = capture_cross_turn_warning(&turn_event);
        assert!(!output.contains("event_item_id"), "{output}");
        assert!(!output.contains("item_delta_kind"), "{output}");
    }

    #[test]
    fn dropped_and_rejected_event_logs_include_bare_identity_without_content() {
        let project_id = ProjectId::new();
        let expected_thread_id = ThreadId::new();
        let event_thread_id = ThreadId::new();
        let turn_id = TurnId::new();
        let item_id = ItemId::new();
        let event = AgentEvent::ItemDelta {
            thread: event_thread_id,
            turn: turn_id,
            item_id,
            delta: ItemDelta::Text {
                text: "foreign sensitive text".into(),
            },
        };

        let output = capture_logs(|| {
            log_foreign_thread_event_drop(project_id, expected_thread_id, event_thread_id, &event);
        });
        for expected in [
            format!("project_id={project_id}"),
            format!("thread_id={expected_thread_id}"),
            format!("event_thread_id={event_thread_id}"),
            format!("event_turn_id={turn_id}"),
            format!("event_item_id={item_id}"),
            "event_kind=\"item_delta\"".into(),
            "item_delta_kind=\"text\"".into(),
        ] {
            assert!(output.contains(&expected), "missing {expected}: {output}");
        }
        assert!(!output.contains("foreign sensitive text"), "{output}");
        assert!(!output.contains("Some("), "{output}");

        let output = capture_logs(|| {
            log_metadata_only_event_rejection(
                project_id,
                expected_thread_id,
                "context_window_updated",
                Some(turn_id),
                None,
            );
        });
        assert!(
            output.contains(&format!("project_id={project_id}")),
            "{output}"
        );
        assert!(
            output.contains(&format!("event_turn_id={turn_id}")),
            "{output}"
        );
        assert!(
            output.contains("event_kind=\"context_window_updated\""),
            "{output}"
        );
        assert!(!output.contains("event_item_id"), "{output}");
        assert!(!output.contains("Some("), "{output}");
    }

    #[test]
    fn late_tool_warning_requires_terminal_output() {
        let item = |status: Option<&str>, output: Option<serde_json::Value>| Item {
            id: ItemId::new(),
            harness_item_id: "tool-1".into(),
            payload: ItemPayload::ToolCall {
                name: "lookup".into(),
                input: serde_json::Value::Null,
                output,
                server: None,
                status: status.map(str::to_owned),
                metadata: None,
                subagent: None,
                error: None,
            },
            created_at: Utc::now(),
        };
        assert!(!completed_tool_has_terminal_output(&item(
            Some("completed"),
            None,
        )));
        assert!(!completed_tool_has_terminal_output(&item(
            Some("running"),
            Some(serde_json::json!({"partial": true})),
        )));
        assert!(completed_tool_has_terminal_output(&item(
            Some("completed"),
            Some(serde_json::Value::Null),
        )));
    }

    #[test]
    fn late_untruncated_command_completion_ignores_original_counts() {
        let thread_id = ThreadId::new();
        let message = late_command_completion_message(
            thread_id,
            AgentEvent::ItemCompleted {
                thread: thread_id,
                turn: TurnId::new(),
                item: Item {
                    id: ItemId::new(),
                    harness_item_id: "command-1".into(),
                    payload: ItemPayload::CommandExecution {
                        command: "printf ok".into(),
                        cwd: std::path::PathBuf::from("/tmp/project"),
                        output: "ok\n".into(),
                        output_truncated: false,
                        output_original_bytes: Some(999),
                        output_original_lines: Some(88),
                        exit_code: Some(0),
                        status: Some("completed".into()),
                        process_id: None,
                        duration_ms: None,
                    },
                    created_at: Utc::now(),
                },
            },
        )
        .unwrap();
        let ServerMessage::Event { agent_event, .. } = message else {
            panic!("expected event message");
        };
        let WireAgentEvent::ItemCompleted { item, .. } = *agent_event else {
            panic!("expected completed item");
        };
        let giskard_proto::WireItemPayload::CommandExecution { output, .. } = item.payload else {
            panic!("expected command execution payload");
        };
        assert_eq!(output.original_bytes, 3);
        assert_eq!(output.original_lines, 1);
    }

    struct UnusedHarnessFactory;

    struct ShutdownHarness {
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl AgentHarness for ShutdownHarness {
        fn capabilities(&self) -> HarnessCapabilities {
            HarnessCapabilities::default()
        }

        async fn list_models(&self) -> Result<Vec<giskard_core::ModelDescriptor>, HarnessError> {
            Ok(Vec::new())
        }

        async fn open_thread(
            &self,
            _opts: OpenThreadOptions,
        ) -> Result<ThreadHandle, HarnessError> {
            Err(HarnessError::Unsupported("unused".into()))
        }

        async fn start_turn(
            &self,
            _thread: &ThreadHandle,
            _input: UserInput,
            _overrides: giskard_core::turn::TurnOverrides,
        ) -> Result<TurnId, HarnessError> {
            Err(HarnessError::Unsupported("unused".into()))
        }

        fn subscribe(&self, _thread: &ThreadHandle) -> AgentEventStream {
            let (_, receiver) = broadcast::channel(1);
            AgentEventStream::new(receiver)
        }

        async fn respond_approval(
            &self,
            _req: ApprovalId,
            _decision: ApprovalDecision,
        ) -> Result<(), HarnessError> {
            Err(HarnessError::Unsupported("unused".into()))
        }

        async fn respond_server_request(
            &self,
            _req: ServerRequestId,
            _response: giskard_core::server_request::ServerRequestResponse,
        ) -> Result<(), HarnessError> {
            Err(HarnessError::Unsupported("unused".into()))
        }

        async fn interrupt(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
            Err(HarnessError::Unsupported("unused".into()))
        }

        async fn shutdown(&self) -> Result<(), HarnessError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(HarnessError::Protocol("injected shutdown failure".into()))
            } else {
                Ok(())
            }
        }
    }

    #[async_trait::async_trait]
    impl super::HarnessFactory for UnusedHarnessFactory {
        async fn create(
            &self,
            _config: &ProjectConfig,
        ) -> Result<Arc<dyn giskard_harness::AgentHarness>, HarnessError> {
            Err(HarnessError::Protocol(
                "unused test harness factory was called".into(),
            ))
        }
    }

    /// A harness that records whether it was bound, and how many threads were opened on it
    /// before that happened.
    struct BindingOrderHarness {
        bound: Arc<AtomicUsize>,
        opened_before_bound: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl AgentHarness for BindingOrderHarness {
        fn capabilities(&self) -> HarnessCapabilities {
            HarnessCapabilities::default()
        }

        async fn list_models(&self) -> Result<Vec<giskard_core::ModelDescriptor>, HarnessError> {
            Ok(Vec::new())
        }

        async fn bind_known_threads(&self, bindings: Vec<(String, ThreadId)>) {
            // Held open so a second caller is certainly inside the window between the harness
            // existing and its bindings landing. Without this the test only catches the bug when
            // the scheduler happens to interleave, which is not a test.
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            self.bound.store(bindings.len().max(1), Ordering::SeqCst);
        }

        async fn open_thread(
            &self,
            _opts: giskard_harness::OpenThreadOptions,
        ) -> Result<ThreadHandle, HarnessError> {
            if self.bound.load(Ordering::SeqCst) == 0 {
                self.opened_before_bound.fetch_add(1, Ordering::SeqCst);
            }
            Err(HarnessError::Protocol("not needed by this test".into()))
        }

        async fn start_turn(
            &self,
            _thread: &ThreadHandle,
            _input: UserInput,
            _overrides: giskard_core::turn::TurnOverrides,
        ) -> Result<TurnId, HarnessError> {
            Err(HarnessError::Protocol("not needed by this test".into()))
        }

        fn subscribe(&self, _thread: &ThreadHandle) -> giskard_harness::AgentEventStream {
            let (_, rx) = tokio::sync::broadcast::channel(1);
            giskard_harness::AgentEventStream::new(rx)
        }

        async fn respond_approval(
            &self,
            _req: ApprovalId,
            _decision: ApprovalDecision,
        ) -> Result<(), HarnessError> {
            Ok(())
        }

        async fn respond_server_request(
            &self,
            _req: ServerRequestId,
            _response: giskard_core::server_request::ServerRequestResponse,
        ) -> Result<(), HarnessError> {
            Ok(())
        }

        async fn interrupt(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
            Ok(())
        }

        async fn shutdown(&self) -> Result<(), HarnessError> {
            Ok(())
        }
    }

    struct BindingOrderFactory {
        harness: Arc<BindingOrderHarness>,
    }

    #[async_trait::async_trait]
    impl super::HarnessFactory for BindingOrderFactory {
        async fn create(
            &self,
            _config: &ProjectConfig,
        ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
            Ok(self.harness.clone())
        }
    }

    /// The bindings only prevent a second identity if they are in place before anything can open a
    /// thread. Publishing the harness first and binding afterwards would let a concurrent caller
    /// take it out of the map in between — so the harness must not be reachable until it is bound.
    #[tokio::test]
    async fn a_harness_is_not_reachable_until_its_bindings_are_registered() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let harness = Arc::new(BindingOrderHarness {
            bound: Arc::new(AtomicUsize::new(0)),
            opened_before_bound: Arc::new(AtomicUsize::new(0)),
        });
        let registry = Arc::new(super::HarnessRegistry::new(
            Arc::new(BindingOrderFactory {
                harness: harness.clone(),
            }),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let config = store
            .load_project(project_id)
            .await
            .unwrap()
            .expect("the project we just created");

        // One caller creates the harness; binding it takes 300ms.
        let creator = {
            let registry = registry.clone();
            let config = config.clone();
            tokio::spawn(async move { registry.get_or_create_harness(project_id, &config).await })
        };

        // Others arrive while that binding is still in flight — the exact window in which a
        // harness published too early would be handed out unbound.
        let mut racers = Vec::new();
        for _ in 0..4 {
            let registry = registry.clone();
            let config = config.clone();
            racers.push(tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                if let Ok(h) = registry.get_or_create_harness(project_id, &config).await {
                    let (updates, _) = giskard_harness::thread_update_channel();
                    let _ = h
                        .open_thread(giskard_harness::OpenThreadOptions {
                            project: project_id,
                            thread: None,
                            workspace_root: "/tmp/test".into(),
                            resume: Some("native-child".into()),
                            resume_policy: giskard_harness::ResumePolicy::AllowFreshFallback,
                            initial_model: None,
                            updates,
                        })
                        .await;
                }
            }));
        }
        creator.await.unwrap().expect("harness is created");
        for racer in racers {
            racer.await.unwrap();
        }

        assert!(harness.bound.load(Ordering::SeqCst) > 0, "bindings ran");
        assert_eq!(
            harness.opened_before_bound.load(Ordering::SeqCst),
            0,
            "no thread may be opened on a harness that has not been given its bindings"
        );
    }

    #[tokio::test]
    async fn registry_shutdown_attempts_every_harness_and_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let hub = Arc::new(Hub::new());
        let registry = super::HarnessRegistry::new(
            Arc::new(UnusedHarnessFactory),
            hub,
            store.clone(),
            ledger::spawn(store),
        );
        let successful_calls = Arc::new(AtomicUsize::new(0));
        let failing_calls = Arc::new(AtomicUsize::new(0));
        {
            let mut harnesses = registry.shared.harnesses.lock().await;
            harnesses.by_project.insert(
                ProjectId::new(),
                ProjectHarnessState::Active(Arc::new(ShutdownHarness {
                    calls: successful_calls.clone(),
                    fail: false,
                })),
            );
            harnesses.by_project.insert(
                ProjectId::new(),
                ProjectHarnessState::Active(Arc::new(ShutdownHarness {
                    calls: failing_calls.clone(),
                    fail: true,
                })),
            );
        }

        let error = registry.shutdown().await.unwrap_err();
        assert!(error.to_string().contains("injected shutdown failure"));
        assert_eq!(successful_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failing_calls.load(Ordering::SeqCst), 1);

        registry.shutdown().await.unwrap();
        assert_eq!(successful_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failing_calls.load(Ordering::SeqCst), 1);
        assert!(registry.shared.harnesses.lock().await.shutting_down);
    }

    #[tokio::test]
    async fn registry_task_shutdown_waits_and_refuses_late_registration() {
        let tracker = Arc::new(super::RegistryTaskTracker::default());
        let permit = tracker.register().unwrap();
        let waiting = tokio::spawn({
            let tracker = tracker.clone();
            async move {
                tracker
                    .close_and_wait(std::time::Duration::from_secs(1))
                    .await
            }
        });

        tokio::task::yield_now().await;
        assert!(tracker.register().is_none());
        assert!(!waiting.is_finished());
        drop(permit);
        waiting.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn closed_registry_rejects_materialization_without_stranding_queue() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let registry = super::HarnessRegistry::new(
            Arc::new(UnusedHarnessFactory),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        );
        registry
            .shared
            .background_tasks
            .close_and_wait(std::time::Duration::from_secs(1))
            .await
            .unwrap();
        let parent_thread_id = ThreadId::new();
        let (result, receiver) = tokio::sync::oneshot::channel();
        super::enqueue_subagent_materialization(
            parent_thread_id,
            super::SubagentMaterializationJob {
                project_id: ProjectId::new(),
                spawned_by_turn_id: TurnId::new(),
                item_id: ItemId::new(),
                origin: "test",
                info: super::SubagentActivityInfo {
                    native_thread_id: "native-child".into(),
                    agent_name: None,
                    agent_path: None,
                    initial_prompt: None,
                    title: None,
                    action: SubagentAction::Spawned,
                    status: None,
                    fallback: None,
                },
                result: Some(result),
            },
            registry.shared.clone(),
        )
        .await;
        assert!(receiver.await.unwrap().is_err());
        assert!(
            !registry
                .shared
                .subagent_materialization_queues
                .lock()
                .await
                .contains_key(&parent_thread_id)
        );
    }

    #[tokio::test]
    async fn deleting_harness_cannot_be_replaced() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let registry = super::HarnessRegistry::new(
            Arc::new(UnusedHarnessFactory),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        );
        let project_id = ProjectId::new();
        let harness: Arc<dyn AgentHarness> = Arc::new(ShutdownHarness {
            calls: Arc::new(AtomicUsize::new(0)),
            fail: false,
        });
        registry
            .shared
            .harnesses
            .lock()
            .await
            .by_project
            .insert(project_id, ProjectHarnessState::Deleting(harness));
        let config: ProjectConfig = serde_json::from_value(serde_json::json!({
            "version": 1,
            "id": project_id,
            "name": "test",
            "dir": "/tmp",
            "harness": "replay",
            "created_at": "2026-08-26T00:00:00Z",
            "updated_at": "2026-08-26T00:00:00Z"
        }))
        .unwrap();
        let error = match registry.get_or_create_harness(project_id, &config).await {
            Ok(_) => panic!("deleting harness must not be replaced"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("being deleted"));
    }

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
    fn active_subagent_monitor_uses_a_long_pre_turn_idle_timeout() {
        for action in [
            SubagentAction::Spawned,
            SubagentAction::Started,
            SubagentAction::Interacted,
        ] {
            let policy = subagent_monitor_policy(Some(action), None);
            assert!(policy.should_monitor);
            assert!(policy.active_observed);
            assert_eq!(
                policy.pre_turn_timeout,
                Some(super::ACTIVE_SUBAGENT_PRE_TURN_IDLE_TIMEOUT)
            );
        }
        assert!(
            subagent_monitor_policy(Some(SubagentAction::Spawned), Some(SubagentStatus::Pending))
                .should_monitor
        );
        assert!(
            subagent_monitor_policy(Some(SubagentAction::Spawned), Some(SubagentStatus::Running))
                .should_monitor
        );

        let ignored = subagent_monitor_policy(None, None);
        assert!(!ignored.should_monitor);

        let interrupted = subagent_monitor_policy(Some(SubagentAction::Interrupted), None);
        assert!(!interrupted.should_monitor);
        assert!(interrupted.terminal_observed);
        for status in [
            SubagentStatus::Completed,
            SubagentStatus::Interrupted,
            SubagentStatus::Failed,
            SubagentStatus::Shutdown,
            SubagentStatus::NotFound,
        ] {
            let policy = subagent_monitor_policy(Some(SubagentAction::Started), Some(status));
            assert!(!policy.should_monitor);
            assert!(policy.terminal_observed);
        }
    }

    #[test]
    fn real_prompt_equal_to_fallback_copy_is_not_suppressed() {
        let mut ctx = TurnContext {
            user_input: UserInput::text("Sub-agent turn"),
            model: ModelRef {
                provider: "openai".into(),
                model: "gpt-5.6-sol".into(),
                reasoning_effort: None,
            },
            mode: Mode::Build,
            kind: TurnContextKind::PassiveSubagent,
            passive_input_is_fallback: false,
            subagent_fallback: None,
            passive_subagent_metadata: None,
            passive_pre_turn_timeout: None,
        };
        assert_eq!(
            passive_subagent_prompt_text(&ctx).as_deref(),
            Some("Sub-agent turn")
        );

        ctx.passive_input_is_fallback = true;
        assert_eq!(passive_subagent_prompt_text(&ctx), None);
    }

    #[tokio::test]
    async fn passive_monitor_releases_after_pre_turn_idle_timeout() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.6-sol".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let (tx, _) = broadcast::channel(8);
        let hub = Arc::new(Hub::new());
        let ledger = ledger::spawn(store.clone());
        let shared = Arc::new(super::RegistryShared::new(hub, store, ledger));
        let ctx = TurnContext {
            user_input: UserInput::text("Sub-agent turn"),
            model,
            mode: Mode::Build,
            kind: TurnContextKind::PassiveSubagent,
            passive_input_is_fallback: true,
            subagent_fallback: None,
            passive_subagent_metadata: None,
            passive_pre_turn_timeout: Some(tokio::time::Duration::from_millis(20)),
        };

        let forwarder = tokio::spawn(forward_events(
            shared,
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            ctx,
            None,
        ));

        tokio::time::timeout(tokio::time::Duration::from_secs(1), forwarder)
            .await
            .expect("idle passive monitor should honor its pre-turn timeout")
            .unwrap();
        drop(tx);
    }

    #[tokio::test]
    async fn monitor_stop_waits_for_post_forwarder_cleanup() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let registry = Arc::new(super::HarnessRegistry::new(
            Arc::new(UnusedHarnessFactory),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));
        let thread_id = ThreadId::new();
        registry
            .shared
            .passive_monitor_tasks
            .register(thread_id)
            .await;

        let stopping = {
            let registry = registry.clone();
            tokio::spawn(async move { registry.stop_passive_subagent_monitor(thread_id).await })
        };
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        assert!(
            !stopping.is_finished(),
            "monitor stop returned before cleanup completed"
        );

        super::finish_passive_subagent_monitor_task(
            &registry.shared.passive_monitor_tasks,
            thread_id,
        )
        .await;
        tokio::time::timeout(tokio::time::Duration::from_secs(1), stopping)
            .await
            .expect("monitor stop should finish after cleanup")
            .unwrap()
            .unwrap();
    }

    #[test]
    fn generated_subagent_title_refresh_is_idempotent() {
        assert!(!should_refresh_subagent_title(
            "Sub-agent: Linnaeus",
            "Sub-agent: Linnaeus"
        ));
        assert!(should_refresh_subagent_title(
            "Sub-agent: server_lifecycle_audit",
            "Sub-agent: Linnaeus"
        ));
        assert!(!should_refresh_subagent_title(
            "My reviewer",
            "Sub-agent: Linnaeus"
        ));
    }

    #[test]
    fn subagent_thread_path_uses_its_final_non_empty_component() {
        assert_eq!(
            subagent_path_leaf("/root/nested_reload_parent"),
            Some("nested_reload_parent")
        );
        assert_eq!(subagent_path_leaf("///"), None);
    }

    #[tokio::test]
    async fn reverse_parent_materialization_is_navigation_only_when_bound_or_cold() {
        for parent_is_bound in [false, true] {
            let tmp = tempfile::TempDir::new().unwrap();
            let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
            let project_id = ProjectId::new();
            store
                .create_project(project_id, "project", "/tmp/project")
                .await
                .unwrap();
            let parent_id = ThreadId::new();
            let child_id = ThreadId::new();
            let model = ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            };
            let now = Utc::now();
            let thread = |id, harness_thread_id: &str, kind, parent_thread_id| ThreadFile {
                revision: 0,
                version: 1,
                id,
                project_id,
                title: "thread".into(),
                harness_thread_id: harness_thread_id.into(),
                parent_thread_id,
                spawned_by_turn_id: None,
                kind,
                mode: Mode::Build,
                current_model: model.clone(),
                context_window: 128_000,
                model_context_windows: Default::default(),
                permission_preset: PermissionPreset::AskFirst,
                model_efforts: Default::default(),
                tokens: TokenLedger::default(),
                created_at: now,
                updated_at: now,
                archived: false,
                git_workspace: None,
            };
            let parent = thread(
                parent_id,
                "native-parent",
                giskard_core::ThreadKind::Primary,
                None,
            );
            let child = thread(
                child_id,
                "native-child",
                giskard_core::ThreadKind::Subagent,
                Some(parent_id),
            );
            store.save_thread(project_id, &parent).await.unwrap();
            store.save_thread(project_id, &child).await.unwrap();

            let shared = Arc::new(super::RegistryShared::new(
                Arc::new(Hub::new()),
                store.clone(),
                ledger::spawn(store.clone()),
            ));
            if parent_is_bound {
                shared.threads.lock().await.insert(
                    parent_id,
                    super::ThreadBinding {
                        project: project_id,
                        handle: ThreadHandle::detached(parent_id, "native-parent".into()),
                        native_model: Some(model.clone()),
                    },
                );
            }

            let result = super::materialize_subagent_thread(
                child_id,
                project_id,
                TurnId::new(),
                super::SubagentActivityInfo {
                    native_thread_id: "native-parent".into(),
                    agent_name: None,
                    agent_path: Some("/root".into()),
                    initial_prompt: None,
                    title: Some("Sub-agent root interacted".into()),
                    action: SubagentAction::Interacted,
                    status: None,
                    fallback: None,
                },
                shared,
            )
            .await
            .unwrap();

            assert_eq!(result, None);
            let saved_parent = store
                .load_thread(project_id, parent_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(saved_parent.kind, giskard_core::ThreadKind::Primary);
            assert_eq!(saved_parent.parent_thread_id, None);
            let saved_child = store
                .load_thread(project_id, child_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(saved_child.kind, giskard_core::ThreadKind::Subagent);
            assert_eq!(saved_child.parent_thread_id, Some(parent_id));
        }
    }

    #[tokio::test]
    async fn monitor_teardown_claims_late_terminal_fallback() {
        let thread_id = ThreadId::new();
        let passive_monitors = Arc::new(Mutex::new(HashSet::from([thread_id])));
        let passive_subagent_metadata = Arc::new(Mutex::new(Default::default()));
        let fallback = super::SubagentFallbackTranscript {
            message: "late terminal result".into(),
            status: SubagentStatus::Completed,
        };

        update_passive_subagent_metadata(
            &passive_subagent_metadata,
            thread_id,
            Some("late prompt".into()),
            Some(fallback),
            super::LifecycleSignal::Terminal,
        )
        .await;

        let claimed = take_passive_subagent_monitor_metadata(
            &passive_monitors,
            &passive_subagent_metadata,
            thread_id,
        )
        .await
        .expect("teardown should claim monitor metadata");
        assert_eq!(claimed.initial_prompt.as_deref(), Some("late prompt"));
        assert_eq!(
            claimed
                .fallback
                .as_ref()
                .map(|value| value.message.as_str()),
            Some("late terminal result")
        );
        assert!(!passive_monitors.lock().await.contains(&thread_id));
        assert!(
            !passive_subagent_metadata
                .lock()
                .await
                .contains_key(&thread_id)
        );
    }

    #[tokio::test]
    async fn subagent_fallback_transcript_persists_when_history_is_empty() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.6-sol".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "Sub-agent".into(),
                    harness_thread_id: "native-child".into(),
                    parent_thread_id: Some(ThreadId::new()),
                    spawned_by_turn_id: Some(TurnId::new()),
                    kind: giskard_core::ThreadKind::Subagent,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(8);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let shared = Arc::new(super::RegistryShared::new(
            hub,
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let ctx = super::FallbackTurnContext {
            user_input: UserInput::text("Sub-agent turn"),
            model,
            mode: Mode::Build,
        };
        let mut seen_turn_ids = HashSet::new();

        persist_subagent_fallback_transcript(
            thread_id,
            project_id,
            &ctx,
            super::SubagentFallbackTranscript {
                message: "Completed child work".into(),
                status: SubagentStatus::Completed,
            },
            &mut seen_turn_ids,
            &shared,
        )
        .await;

        let turns = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].user_input.as_text(), Some("Sub-agent turn"));
        assert!(matches!(
            &turns[0].items[0].payload,
            ItemPayload::AgentMessage { text } if text == "Completed child work"
        ));
        assert_eq!(turns[0].status.kind, TurnStatusKind::Completed);

        let mut saw_item = false;
        while let Ok(message) = client_rx.try_recv() {
            if let ServerMessage::Event { agent_event, .. } = message
                && let WireAgentEvent::ItemCompleted { item, .. } = *agent_event
            {
                saw_item = matches!(
                    item.payload,
                    giskard_proto::WireItemPayload::AgentMessage { ref text }
                        if text == "Completed child work"
                );
            }
        }
        assert!(saw_item, "fallback transcript should be broadcast live");
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

    #[tokio::test]
    async fn resumed_context_window_uses_resumed_model_and_metadata_service() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "historical-model".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "native-thread".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();
        let hub = Arc::new(Hub::new());
        let shared = Arc::new(super::RegistryShared::new(
            hub,
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let (sink, stream, permit) = prepare_thread_updates(&shared, thread_id);
        let forwarder =
            spawn_thread_update_forwarder(shared.clone(), project_id, thread_id, stream, permit)
                .unwrap();
        sink.send(ThreadUpdate::ContextWindowRestored {
            model: model.clone(),
            context_window: 258_400,
        })
        .unwrap();
        forwarder.await.unwrap();
        assert_eq!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .unwrap()
                .context_window,
            258_400
        );

        for invalidate_with_turn in [true, false] {
            let stale_thread_id = ThreadId::new();
            let mut stale_thread = store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .unwrap();
            stale_thread.id = stale_thread_id;
            stale_thread.revision = 0;
            stale_thread.context_window = 128_000;
            stale_thread.model_context_windows.clear();
            store.save_thread(project_id, &stale_thread).await.unwrap();
            let (sink, stream, permit) = prepare_thread_updates(&shared, stale_thread_id);
            if invalidate_with_turn {
                let handle = ThreadHandle::detached(stale_thread_id, "native-stale".into());
                let ctx = TurnContext {
                    user_input: UserInput::text("newer turn"),
                    model: model.clone(),
                    mode: Mode::Build,
                    kind: TurnContextKind::User,
                    passive_input_is_fallback: false,
                    subagent_fallback: None,
                    passive_subagent_metadata: None,
                    passive_pre_turn_timeout: None,
                };
                let _lease = shared
                    .runtime
                    .reserve_turn(stale_thread_id, turn_reservation(project_id, &handle, &ctx))
                    .unwrap();
            } else {
                shared
                    .runtime
                    .forget_threads(&HashSet::from([stale_thread_id]));
            }
            let forwarder = spawn_thread_update_forwarder(
                shared.clone(),
                project_id,
                stale_thread_id,
                stream,
                permit,
            )
            .unwrap();
            sink.send(ThreadUpdate::ContextWindowRestored {
                model: model.clone(),
                context_window: 400_000,
            })
            .unwrap();
            forwarder.await.unwrap();
            assert_eq!(
                store
                    .load_thread(project_id, stale_thread_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .context_window,
                128_000
            );
        }
    }

    #[tokio::test]
    async fn forwarder_drops_context_window_update_for_mismatched_turn_model() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let turn_id = TurnId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.6-sol".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(16);
        let hub = Arc::new(Hub::new());
        let (client_tx, _client_rx) = mpsc::channel(16);
        let replacements = hub.register_client(1, client_tx.clone()).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());
        let handle = spawn_forwarder_handle(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store.clone(),
            ledger,
            model.clone(),
            "context window mismatch",
        );

        tx.send(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: turn_id,
        })
        .unwrap();
        tx.send(AgentEvent::ContextWindowUpdated {
            thread: thread_id,
            turn: turn_id,
            model: ModelRef {
                provider: model.provider.clone(),
                model: "gpt-5.6-pro".into(),
                reasoning_effort: None,
            },
            context_window: 400_000,
        })
        .unwrap();
        tx.send(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: turn_id,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        })
        .unwrap();

        tokio::time::timeout(tokio::time::Duration::from_secs(2), handle)
            .await
            .expect("forwarder should exit after turn completion")
            .unwrap();

        let persisted = store
            .load_thread(project_id, thread_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.context_window, 128_000);
        assert!(
            persisted.model_context_windows.is_empty(),
            "a mismatched turn model must not be persisted"
        );
        while let Some(message) = replacements.try_recv() {
            if let ServerMessage::ThreadState(state) = message {
                assert_ne!(state.metadata.context_window, 400_000);
            }
        }
    }

    #[tokio::test]
    async fn forwarder_persists_and_broadcasts_context_window_update_for_matching_turn_model() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let turn_id = TurnId::new();
        let model = ModelRef {
            provider: "openai".into(),
            model: "gpt-5.6-sol".into(),
            reasoning_effort: None,
        };
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(16);
        let hub = Arc::new(Hub::new());
        let (client_tx, _client_rx) = mpsc::channel(16);
        let replacements = hub.register_client(1, client_tx.clone()).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());
        let handle = spawn_forwarder_handle(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store.clone(),
            ledger,
            model.clone(),
            "context window match",
        );

        tx.send(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: turn_id,
        })
        .unwrap();
        tx.send(AgentEvent::ContextWindowUpdated {
            thread: thread_id,
            turn: turn_id,
            model: model.clone(),
            context_window: 258_400,
        })
        .unwrap();
        tx.send(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: turn_id,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        })
        .unwrap();

        tokio::time::timeout(tokio::time::Duration::from_secs(2), handle)
            .await
            .expect("forwarder should exit after turn completion")
            .unwrap();

        let persisted = store
            .load_thread(project_id, thread_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.context_window, 258_400);
        assert_eq!(
            persisted
                .model_context_windows
                .get("openai")
                .and_then(|models| models.get("gpt-5.6-sol")),
            Some(&258_400)
        );

        let mut matching_states = 0;
        while let Some(message) = replacements.try_recv() {
            if let ServerMessage::ThreadState(state) = message {
                assert_eq!(state.metadata.thread_id, thread_id);
                assert!(state.metadata.revision <= persisted.revision);
                assert_eq!(state.active_turn, None);
                if state.metadata.context_window == 258_400 {
                    matching_states += 1;
                    assert_eq!(state.metadata.revision, persisted.revision);
                    assert_eq!(state.metadata.current_model, model);
                }
            }
        }
        assert_eq!(
            matching_states, 1,
            "matching update must survive coalescing into the latest committed thread state"
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
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let ledger = ledger::spawn(store.clone());

        let runtime = spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub.clone(),
            store.clone(),
            ledger.clone(),
            model.clone(),
            "first",
        );
        let first_turn = TurnId::new();
        let second_turn = TurnId::new();
        let mut first_events = turn_events(
            thread_id,
            first_turn,
            "first",
            "one",
            TokenUsage::new(10, 1),
        );
        tx.send(first_events.remove(0)).unwrap();
        let rejected_diff = giskard_core::FileDiff {
            path: "src/rejected.rs".into(),
            change: giskard_core::FileChangeKind::Modified,
            old_text: Some("old".into()),
            new_text: Some("foreign".into()),
            hunks: Vec::new(),
            binary: false,
            captured: None,
        };
        let rejected_id = giskard_core::capture_structured_diff(rejected_diff.clone())
            .1
            .id;
        tx.send(AgentEvent::DiffUpdated {
            thread: thread_id,
            turn: second_turn,
            diff: rejected_diff,
        })
        .unwrap();
        for event in first_events {
            tx.send(event).unwrap();
        }
        wait_for_turn_count(&store, project_id, thread_id, 1).await;
        assert!(matches!(
            runtime.captured_diff(thread_id, second_turn, &rejected_id),
            crate::thread_runtime::RuntimeDiffLookup::Missing
        ));

        spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store.clone(),
            ledger,
            model,
            "second",
        );
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

        // The bounded index carries one record per turn, after its one-line header.
        let raw_history = tokio::fs::read_to_string(
            data_dir
                .join("projects")
                .join(project_id.to_string())
                .join("threads")
                .join(thread_id.to_string())
                .join("history.jsonl"),
        )
        .await
        .unwrap();
        assert_eq!(
            raw_history
                .lines()
                .filter(|line| line.contains(r#""kind":"turn""#))
                .count(),
            2
        );

        let saved = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(saved.len(), 2);
        assert_eq!(saved[0].id, first_turn);
        assert_eq!(saved[0].user_input, UserInput::text("first"));
        assert_eq!(saved[1].id, second_turn);
        assert_eq!(saved[1].user_input, UserInput::text("second"));
    }

    #[tokio::test]
    async fn completed_turn_forwarder_drains_processless_command_completion() {
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
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let ledger = ledger::spawn(store.clone());
        let (handle, runtime) = spawn_forwarder_handle_with_runtime(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store.clone(),
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
                    process_id: None,
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
                    output_truncated: false,
                    output_original_bytes: None,
                    output_original_lines: None,
                    exit_code: None,
                    status: Some("running".into()),
                    process_id: None,
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
        let tasks = runtime.tasks_snapshot(thread_id).1;
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].after_turn);
        assert!(tasks[0].process_id.is_none());

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
                    output_truncated: false,
                    output_original_bytes: None,
                    output_original_lines: None,
                    exit_code: Some(0),
                    status: Some("completed".into()),
                    process_id: None,
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

        assert!(runtime.tasks_snapshot(thread_id).1.is_empty());
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
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let ledger = ledger::spawn(store.clone());
        let (handle, runtime) = spawn_forwarder_handle_with_runtime(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store.clone(),
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
            runtime.live_snapshot(thread_id).is_none(),
            "synthetic completion should clear live state"
        );

        let tasks = runtime.tasks_snapshot(thread_id).1;
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
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "target".into(),
                    harness_thread_id: "th_target".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
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
                            output_truncated: false,
                            output_original_bytes: None,
                            output_original_lines: None,
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
        let ledger = ledger::spawn(store.clone());

        let runtime = spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store,
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
            runtime.tasks_snapshot(thread_id).1.is_empty(),
            "historical starts for already-persisted turns must not create stale running tasks"
        );
    }

    #[tokio::test]
    async fn persisted_turn_terminal_command_completion_is_broadcast_without_running_task() {
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
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "target".into(),
                    harness_thread_id: "th_target".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let turn = TurnId::new();
        let item_id = ItemId::new();
        let harness_item_id = "cmd_late".to_string();
        store
            .append_turn(
                project_id,
                thread_id,
                &Turn {
                    id: turn,
                    user_input: UserInput::text("already persisted"),
                    items: vec![Item {
                        id: item_id,
                        harness_item_id: harness_item_id.clone(),
                        payload: ItemPayload::CommandExecution {
                            command: "<command included NUL byte>".into(),
                            cwd: "/tmp/test".into(),
                            output: String::new(),
                            output_truncated: false,
                            output_original_bytes: None,
                            output_original_lines: None,
                            exit_code: None,
                            status: Some("in_progress".into()),
                            process_id: None,
                            duration_ms: None,
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
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());

        let runtime = spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store,
            ledger,
            model,
            "next",
        );

        assert!(runtime.tasks_snapshot(thread_id).1.is_empty());
        tx.send(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn,
            item: Item {
                id: item_id,
                harness_item_id,
                payload: ItemPayload::CommandExecution {
                    command: "<command included NUL byte>".into(),
                    cwd: "/tmp/test".into(),
                    output: "failed before spawn".into(),
                    output_truncated: false,
                    output_original_bytes: None,
                    output_original_lines: None,
                    exit_code: Some(1),
                    status: Some("failed".into()),
                    process_id: None,
                    duration_ms: Some(10),
                },
                created_at: now,
            },
        })
        .unwrap();

        let message = tokio::time::timeout(tokio::time::Duration::from_secs(1), async {
            loop {
                if let Some(message) = client_rx.recv().await
                    && matches!(
                        &message,
                        ServerMessage::Event {
                            agent_event,
                            ..
                        } if matches!(**agent_event, WireAgentEvent::ItemCompleted { .. })
                    )
                {
                    break message;
                }
            }
        })
        .await
        .expect("late terminal command completion should be broadcast");
        let ServerMessage::Event { agent_event, .. } = message else {
            panic!("expected event");
        };
        let WireAgentEvent::ItemCompleted { item, .. } = *agent_event else {
            panic!("expected item completion");
        };
        assert_eq!(item.id, item_id);
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
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "target".into(),
                    harness_thread_id: "th_target".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let ledger = ledger::spawn(store.clone());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);

        let runtime = spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store.clone(),
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
            runtime.live_snapshot(thread_id).is_none(),
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
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "target".into(),
                    harness_thread_id: "th_target".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let ledger = ledger::spawn(store.clone());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);

        let runtime = spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store.clone(),
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
            runtime.live_snapshot(thread_id).is_none(),
            "foreign events must not create target-thread live state"
        );
        assert!(
            runtime.tasks_snapshot(thread_id).1.is_empty(),
            "foreign running commands must not appear in the target-thread task list"
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
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "target".into(),
                    harness_thread_id: "th_target".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let ledger = ledger::spawn(store.clone());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);

        let runtime = spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store.clone(),
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

        tokio::time::timeout(tokio::time::Duration::from_secs(2), async {
            loop {
                match client_rx
                    .recv()
                    .await
                    .expect("subscriber should remain connected")
                {
                    ServerMessage::Event { agent_event, .. } => match *agent_event {
                        WireAgentEvent::ServerRequestReceived { turn, request, .. } => {
                            assert!(turn.is_none());
                            assert_eq!(request.id, request_id);
                            assert_eq!(request.method, "mcpServer/elicitation/request");
                            break;
                        }
                        other => panic!("expected turnless server request event, got {other:?}"),
                    },
                    ServerMessage::RequestState(_) => {}
                    other => panic!("expected turnless server request event, got {other:?}"),
                }
            }
        })
        .await
        .expect("normal forwarder should broadcast the turnless request");

        assert!(
            store
                .load_all_turns(project_id, thread_id)
                .await
                .unwrap()
                .is_empty(),
            "turnless request alone must not persist a turn"
        );
        assert!(
            runtime.live_snapshot(thread_id).is_none(),
            "turnless request alone must not create target-thread live turn state"
        );
    }

    #[tokio::test]
    async fn passive_forwarder_does_not_duplicate_turnless_event_owned_by_user_forwarder() {
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
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let (tx, _) = broadcast::channel(16);
        let user_stream = AgentEventStream::new(tx.subscribe());
        let passive_stream = AgentEventStream::new(tx.subscribe());
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let shared = Arc::new(super::RegistryShared::new(
            hub,
            store.clone(),
            ledger::spawn(store),
        ));
        let user_ctx = TurnContext {
            user_input: UserInput::text("user turn"),
            model: model.clone(),
            mode: Mode::Build,
            kind: TurnContextKind::User,
            passive_input_is_fallback: false,
            subagent_fallback: None,
            passive_subagent_metadata: None,
            passive_pre_turn_timeout: None,
        };
        let handle = ThreadHandle::detached(thread_id, "native-thread".into());
        let lease = shared
            .runtime
            .reserve_turn(thread_id, turn_reservation(project_id, &handle, &user_ctx))
            .unwrap();
        let user_forwarder = tokio::spawn(forward_events(
            shared.clone(),
            thread_id,
            project_id,
            user_stream,
            user_ctx,
            Some(lease),
        ));

        shared.passive_monitors.lock().await.insert(thread_id);
        shared.passive_monitor_tasks.register(thread_id).await;
        let passive_forwarder = tokio::spawn(forward_events(
            shared.clone(),
            thread_id,
            project_id,
            passive_stream,
            TurnContext {
                user_input: UserInput::text("Sub-agent turn"),
                model,
                mode: Mode::Build,
                kind: TurnContextKind::PassiveSubagent,
                passive_input_is_fallback: true,
                subagent_fallback: None,
                passive_subagent_metadata: Some(shared.passive_subagent_metadata.clone()),
                passive_pre_turn_timeout: Some(tokio::time::Duration::from_secs(1)),
            },
            None,
        ));

        tx.send(AgentEvent::Notice {
            thread: thread_id,
            turn: None,
            message: "one owner".into(),
        })
        .unwrap();

        tokio::time::timeout(tokio::time::Duration::from_secs(1), async {
            match client_rx
                .recv()
                .await
                .expect("subscriber should remain connected")
            {
                ServerMessage::Event { agent_event, .. }
                    if matches!(*agent_event, WireAgentEvent::Notice { .. }) => {}
                other => panic!("expected turnless notice event, got {other:?}"),
            }
        })
        .await
        .expect("normal forwarder should broadcast the turnless notice");
        let duplicate = tokio::time::timeout(tokio::time::Duration::from_millis(100), async {
            loop {
                match client_rx.recv().await {
                    Some(ServerMessage::Event { agent_event, .. })
                        if matches!(*agent_event, WireAgentEvent::Notice { .. }) =>
                    {
                        return true;
                    }
                    Some(_) => {}
                    None => return false,
                }
            }
        })
        .await;
        assert!(
            duplicate.is_err() || matches!(duplicate, Ok(false)),
            "passive and user forwarders must not both broadcast the same turnless event"
        );

        drop(tx);
        tokio::time::timeout(tokio::time::Duration::from_secs(1), passive_forwarder)
            .await
            .expect("passive duplicate forwarder should exit")
            .unwrap();
        tokio::time::timeout(tokio::time::Duration::from_secs(1), user_forwarder)
            .await
            .expect("user forwarder should exit after stream close")
            .unwrap();
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
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());

        spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store,
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
                Ok(Some(ServerMessage::Event { agent_event, .. })) => match *agent_event {
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
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());
        let stream = AgentEventStream::new(tx.subscribe());
        let ctx = TurnContext {
            user_input: UserInput::text("/compact"),
            model: model.clone(),
            mode: Mode::Build,
            kind: TurnContextKind::ManualCompaction,
            passive_input_is_fallback: false,
            subagent_fallback: None,
            passive_subagent_metadata: None,
            passive_pre_turn_timeout: None,
        };
        let gate = Arc::new(ThreadRuntimeRegistry::new());
        let handle = ThreadHandle::opened(
            thread_id,
            "native-test-thread".into(),
            std::path::PathBuf::from("/tmp/test-workspace"),
        );
        let lease = gate
            .reserve_turn(thread_id, turn_reservation(project_id, &handle, &ctx))
            .unwrap();
        let ctx_for_second_reserve = ctx.clone();
        let shared = super::RegistryShared::new_with_runtime(
            hub.clone(),
            gate.clone(),
            store.clone(),
            ledger,
        );
        let shared = Arc::new(shared);

        tokio::spawn({
            async move {
                forward_events(shared, thread_id, project_id, stream, ctx, Some(lease)).await;
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
                    subagent: None,
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
                    if matches!(*agent_event, WireAgentEvent::TurnCompleted { .. }) {
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
            gate.reserve_turn(
                thread_id,
                turn_reservation(project_id, &handle, &ctx_for_second_reserve)
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
        store: Arc<PersistStore>,
        ledger: ledger::LedgerHandle,
        model: ModelRef,
        user_input: &str,
    ) -> Arc<ThreadRuntimeRegistry> {
        let (handle, runtime) = spawn_forwarder_handle_with_runtime(
            thread_id, project_id, stream, hub, store, ledger, model, user_input,
        );
        std::mem::drop(handle);
        runtime
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_forwarder_handle(
        thread_id: ThreadId,
        project_id: ProjectId,
        stream: AgentEventStream,
        hub: Arc<Hub>,
        store: Arc<PersistStore>,
        ledger: ledger::LedgerHandle,
        model: ModelRef,
        user_input: &str,
    ) -> JoinHandle<()> {
        spawn_forwarder_handle_with_runtime(
            thread_id, project_id, stream, hub, store, ledger, model, user_input,
        )
        .0
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_forwarder_handle_with_runtime(
        thread_id: ThreadId,
        project_id: ProjectId,
        stream: AgentEventStream,
        hub: Arc<Hub>,
        store: Arc<PersistStore>,
        ledger: ledger::LedgerHandle,
        model: ModelRef,
        user_input: &str,
    ) -> (JoinHandle<()>, Arc<ThreadRuntimeRegistry>) {
        let ctx = TurnContext {
            user_input: UserInput::text(user_input),
            model,
            mode: Mode::Build,
            kind: TurnContextKind::User,
            passive_input_is_fallback: false,
            subagent_fallback: None,
            passive_subagent_metadata: None,
            passive_pre_turn_timeout: None,
        };
        let shared = super::RegistryShared::new(hub, store, ledger);
        let shared = Arc::new(shared);
        let runtime = shared.runtime.clone();
        let handle = tokio::spawn(async move {
            forward_events(shared, thread_id, project_id, stream, ctx, None).await;
        });
        (handle, runtime)
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
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
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
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());

        spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store.clone(),
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
            if let ServerMessage::Event { agent_event, .. } = message
                && let WireAgentEvent::ItemCompleted { item, .. } = *agent_event
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
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
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
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());

        spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store.clone(),
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
                match *agent_event {
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
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "t".into(),
                    harness_thread_id: "th".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: Mode::Build,
                    current_model: model.clone(),
                    context_window: 128_000,
                    model_context_windows: Default::default(),
                    permission_preset: PermissionPreset::AskFirst,
                    model_efforts: Default::default(),
                    tokens: TokenLedger::default(),
                    created_at: now,
                    updated_at: now,
                    archived: false,
                    git_workspace: None,
                },
            )
            .await
            .unwrap();

        let (tx, _) = broadcast::channel(64);
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(64);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());

        spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store.clone(),
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
            if let Ok(Some(ServerMessage::Event { agent_event, .. })) =
                tokio::time::timeout(tokio::time::Duration::from_millis(100), client_rx.recv())
                    .await
                && let WireAgentEvent::ItemDelta {
                    delta: giskard_proto::ItemDelta::Text { text },
                    ..
                } = *agent_event
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
