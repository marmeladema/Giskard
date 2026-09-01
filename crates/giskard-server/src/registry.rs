use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;
use futures::future::join_all;
use tokio::sync::{Mutex, Notify, OwnedMutexGuard, broadcast, oneshot, watch};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ProjectId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{
    Item, ItemDelta, ItemPayload, command_status_is_running, normalized_command_status,
    tool_status_is_running,
};
use giskard_core::mcp::{McpOauthStart, McpServerStatus};
use giskard_core::model::{ModelDescriptor, ModelRef};
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::text::trimmed_non_empty;
use giskard_core::thread::ThreadKind;
use giskard_core::turn::{
    Mode, Turn, TurnMode, TurnModel, TurnOverrides, TurnStatus, TurnStatusKind,
};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentHarness, HarnessBootstrap, HarnessCapabilities, HarnessProvider, KnownThreadBinding,
    OpenThreadOptions, ThreadHandle, ThreadUpdate, thread_update_channel,
};
use giskard_persist::PersistStore;
use giskard_persist::store::{ProjectConfig, ThreadFile, ThreadMutation, TurnCommitOutcome};
use giskard_proto::{RunningTask, ServerMessage, ThreadRuntimeOverview, WireAgentEvent, WireItem};

use crate::hub::Hub;
use crate::ledger::LedgerHandle;
use crate::log_fields::{display_opt, rfc3339, rfc3339_opt};
use crate::thread_graph::{
    ExistingLinkDisposition, classify_existing_link, effective_thread_workspace_root,
    load_thread_graph, parent_chain_is_valid, should_refresh_subagent_title,
};
use crate::thread_metadata::ThreadMetadataService;
use crate::thread_runtime::{
    AppliedRuntimeEvent, RequestResolution, RequestTransition, ResolvedThreadRuntime,
    RestorePermit, RuntimeRequestId, ThreadRuntimeSupport, ThreadTurnLease, TurnReservation,
};

mod project;
mod thread;

use project::{HarnessTransitions, LifecycleLock, ProjectAuthority, WeakLifecycleLock};
pub(crate) use thread::ThreadAuthority;
use thread::{
    ClassificationPhase, CoordinatorToken, EventOwnerControl, ExternalTurnDefaults, OwnerLock,
    ThreadBinding, ThreadCoordinator, WeakOwnerLock,
};

#[async_trait]
pub trait HarnessFactory: Send + Sync {
    /// Construct a harness with its complete durable identity table installed before it can
    /// dispatch ordinary events. The bootstrap is construction input, not a command sent to an
    /// already-running harness; returning success means every binding was validated and installed.
    async fn create(
        &self,
        config: &ProjectConfig,
        bootstrap: HarnessBootstrap,
    ) -> Result<Arc<dyn AgentHarness>, HarnessError>;
}

/// Context describing the turn being started, used to persist a `Turn` on completion (§7.1).
#[derive(Clone)]
struct TurnContext {
    user_input: UserInput,
    model: TurnModel,
    mode: TurnMode,
    kind: TurnContextKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TurnContextKind {
    User,
    ManualCompaction,
    ExternalSubagent,
    ExternalOrphan,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ForwarderExitReason {
    StreamEndedRecovered,
    StreamEndedWithoutTurn,
    DuplicateForwarder,
    PersistenceBlocked,
    EventPreparationFailed,
    RuntimeAuthorityReplaced,
}

fn forwarder_exit_reason_label(reason: ForwarderExitReason) -> &'static str {
    match reason {
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
        TurnContextKind::ExternalSubagent => "external_subagent",
        TurnContextKind::ExternalOrphan => "external_orphan",
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
        model: ctx.model.clone(),
        context_kind: turn_context_kind_label(ctx.kind),
    }
}

fn live_turn_user_input(ctx: &TurnContext) -> Option<UserInput> {
    if !matches!(
        ctx.kind,
        TurnContextKind::ExternalSubagent | TurnContextKind::ExternalOrphan
    ) {
        return None;
    }
    ctx.user_input
        .as_text()
        .and_then(trimmed_non_empty)
        .map(UserInput::text)
}

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

/// A loaded-thread identity sampled from one coordinator state.
///
/// Presence means the authority had a coordinator when sampled, not that the coordinator remains
/// installed or that its event owner is live. The cloned values retain neither the authority nor
/// the coordinator. A missing native model is distinct from a missing loaded binding.
#[derive(Clone)]
pub struct LoadedThreadBinding {
    project_id: ProjectId,
    handle: ThreadHandle,
    /// The model the harness reports this native thread is on. `None` when neither the caller nor
    /// the harness named one — callers already treat an unknown native model the same as an
    /// unbound thread.
    native_model: Option<ModelRef>,
}

impl LoadedThreadBinding {
    pub fn project_id(&self) -> ProjectId {
        self.project_id
    }

    pub fn handle(&self) -> &ThreadHandle {
        &self.handle
    }

    pub fn native_model(&self) -> Option<&ModelRef> {
        self.native_model.as_ref()
    }
}

struct ResolvedLoadedThread {
    authority: Arc<ThreadAuthority>,
    binding: LoadedThreadBinding,
}

#[derive(Debug, PartialEq, Eq)]
struct ThreadProjectMismatch {
    thread_id: ThreadId,
    existing_project_id: ProjectId,
    requested_project_id: ProjectId,
}

impl fmt::Display for ThreadProjectMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "thread {} is already associated with project {}, not {}",
            self.thread_id, self.existing_project_id, self.requested_project_id
        )
    }
}

impl std::error::Error for ThreadProjectMismatch {}

#[derive(Clone)]
pub struct HarnessRegistry {
    shared: Arc<RegistryShared>,
    factory: Arc<dyn HarnessFactory>,
}

#[derive(Default)]
struct ProjectIndex {
    // ENTITY-AUTHORITY-OWNER: sole process-local membership for ProjectAuthority.
    projects: HashMap<ProjectId, Arc<ProjectAuthority>>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Intern lifecycle locks before durable project existence is verified.
    // Source of truth: Published authorities own the adopted mutex; unpublished entries are weak.
    // Structural reason: Callers must lock an ID before a verified authority can be published.
    // Synchronization: The ProjectIndex mutex protects weak lookup and authority publication.
    // Invalidation/removal: Publication removes the weak entry; dead unpublished entries are pruned.
    unpublished_locks: HashMap<ProjectId, WeakLifecycleLock>,
}

#[derive(Default)]
struct ThreadIndex {
    // ENTITY-AUTHORITY-OWNER: sole process-local membership for ThreadAuthority.
    threads: HashMap<ThreadId, Arc<ThreadAuthority>>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Intern event-owner locks before a verified thread authority is published.
    // Source of truth: Published authorities own adopted mutexes; unpublished entries are weak.
    // Structural reason: Owner serialization begins before verified thread association is known.
    // Synchronization: The ThreadIndex mutex protects weak lookup and authority publication.
    // Invalidation/removal: Publication removes the weak entry; dead unpublished entries are pruned.
    unpublished_locks: HashMap<ThreadId, WeakOwnerLock>,
}

struct RegistryShared {
    projects: Arc<Mutex<ProjectIndex>>,
    harness_transitions: Arc<HarnessTransitions>,
    threads: Arc<Mutex<ThreadIndex>>,
    background_tasks: Arc<RegistryTaskTracker>,
    hub: Arc<Hub>,
    runtime: Arc<ThreadRuntimeSupport>,
    store: Arc<PersistStore>,
    thread_metadata: Arc<ThreadMetadataService>,
    ledger: LedgerHandle,
}

impl RegistryShared {
    async fn active_harness(&self, project_id: ProjectId) -> Option<Arc<dyn AgentHarness>> {
        let authority = self.project_authority(project_id).await?;
        let mut transitions = self.harness_transitions.lock().await;
        transitions.project(&authority).await.active()
    }

    async fn project_authority(&self, project_id: ProjectId) -> Option<Arc<ProjectAuthority>> {
        self.projects
            .lock()
            .await
            .projects
            .get(&project_id)
            .cloned()
    }

    async fn intern_project_authority(&self, project_id: ProjectId) -> Arc<ProjectAuthority> {
        let mut index = self.projects.lock().await;
        if let Some(authority) = index.projects.get(&project_id) {
            return authority.clone();
        }
        index
            .unpublished_locks
            .retain(|_, lock| lock.strong_count() > 0);
        let lifecycle = index
            .unpublished_locks
            .remove(&project_id)
            .and_then(|lock| lock.upgrade())
            .unwrap_or_else(LifecycleLock::new);
        let authority = Arc::new(ProjectAuthority::new(project_id, lifecycle));
        index.projects.insert(project_id, authority.clone());
        authority
    }

    async fn thread_authority(&self, thread_id: ThreadId) -> Option<Arc<ThreadAuthority>> {
        self.threads.lock().await.threads.get(&thread_id).cloned()
    }

    async fn intern_thread_authority(
        &self,
        thread_id: ThreadId,
        project_id: ProjectId,
    ) -> Result<Arc<ThreadAuthority>, ThreadProjectMismatch> {
        let mut index = self.threads.lock().await;
        if let Some(authority) = index.threads.get(&thread_id) {
            if authority.project_id() != project_id {
                return Err(ThreadProjectMismatch {
                    thread_id,
                    existing_project_id: authority.project_id(),
                    requested_project_id: project_id,
                });
            }
            return Ok(authority.clone());
        }
        index
            .unpublished_locks
            .retain(|_, lock| lock.strong_count() > 0);
        let owner = index
            .unpublished_locks
            .remove(&thread_id)
            .and_then(|lock| lock.upgrade())
            .unwrap_or_else(OwnerLock::new);
        let authority = Arc::new(ThreadAuthority::new(thread_id, project_id, owner));
        index.threads.insert(thread_id, authority.clone());
        Ok(authority)
    }

    async fn coordinator(&self, thread_id: ThreadId) -> Option<ThreadBinding> {
        let authority = self.thread_authority(thread_id).await?;
        authority.coordinator().await
    }

    async fn resolve_loaded_thread(&self, thread_id: ThreadId) -> Option<ResolvedLoadedThread> {
        let authority = self.thread_authority(thread_id).await?;
        let coordinator = authority.coordinator().await?;
        let binding = coordinator.binding().await;
        drop(coordinator);
        Some(ResolvedLoadedThread { authority, binding })
    }

    async fn coordinator_snapshot(&self) -> Vec<(ThreadId, ThreadBinding)> {
        let authorities = self
            .threads
            .lock()
            .await
            .threads
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut coordinators = Vec::new();
        for authority in authorities {
            if let Some(coordinator) = authority.coordinator().await {
                coordinators.push((authority.thread_id(), coordinator));
            }
        }
        coordinators
    }

    async fn abort_admitted_operation(
        &self,
        coordinator: &ThreadCoordinator,
        operation: CoordinatorToken,
    ) {
        if let Some(mut turn_gate) = coordinator.abort_operation(operation).await
            && let Some(overview) = turn_gate.release()
        {
            self.hub.publish_runtime_overview(overview).await;
        }
    }

    async fn admit_operation(
        &self,
        authority: &Arc<ThreadAuthority>,
        coordinator: &ThreadCoordinator,
        project_id: ProjectId,
        handle: &ThreadHandle,
        context: &TurnContext,
    ) -> Result<CoordinatorToken, HarnessError> {
        let turn_gate = match self
            .runtime
            .reserve_turn(authority, turn_reservation(project_id, handle, context))
        {
            Ok(turn_gate) => turn_gate,
            Err(error) => return Err(error),
        };
        let operation = match coordinator
            .prepare_operation(context.clone(), turn_gate)
            .await
        {
            Ok(operation) => operation,
            Err((error, mut turn_gate)) => {
                if let Some(overview) = turn_gate.release() {
                    self.hub.publish_runtime_overview(overview).await;
                }
                return Err(error);
            }
        };
        publish_runtime_overview(self).await;
        Ok(operation)
    }

    #[cfg(test)]
    fn new(hub: Arc<Hub>, store: Arc<PersistStore>, ledger: LedgerHandle) -> Self {
        Self::new_with_max_command_output_bytes(
            hub,
            giskard_persist::config::RetentionConfig::DEFAULT_MAX_COMMAND_OUTPUT_BYTES,
            store,
            ledger,
        )
    }

    fn new_with_max_command_output_bytes(
        hub: Arc<Hub>,
        max_command_output_bytes: usize,
        store: Arc<PersistStore>,
        ledger: LedgerHandle,
    ) -> Self {
        let thread_metadata = Arc::new(ThreadMetadataService::new(store.clone(), hub.clone()));
        Self {
            projects: Arc::new(Mutex::new(ProjectIndex::default())),
            harness_transitions: Arc::new(HarnessTransitions::new()),
            threads: Arc::new(Mutex::new(ThreadIndex::default())),
            background_tasks: Arc::new(RegistryTaskTracker::default()),
            hub,
            runtime: Arc::new(ThreadRuntimeSupport::with_max_command_output_bytes(
                max_command_output_bytes,
            )),
            store,
            thread_metadata,
            ledger,
        }
    }
}

#[cfg(test)]
async fn prepare_thread_updates(
    shared: &RegistryShared,
    project_id: ProjectId,
    thread_id: ThreadId,
) -> (
    giskard_harness::ThreadUpdateSink,
    giskard_harness::ThreadUpdateStream,
    RestorePermit,
) {
    let (sink, stream) = thread_update_channel();
    let authority = shared
        .intern_thread_authority(thread_id, project_id)
        .await
        .expect("test thread authority must be valid");
    let permit = shared.runtime.restoration_permit(&authority);
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
    /// Resolves a bound runtime view without interning a thread authority.
    pub async fn thread_runtime(&self, thread_id: ThreadId) -> Option<ResolvedThreadRuntime> {
        let authority = self.shared.thread_authority(thread_id).await?;
        Some(ResolvedThreadRuntime::new(
            self.shared.runtime.clone(),
            authority,
        ))
    }

    pub async fn loaded_thread_binding(&self, thread_id: ThreadId) -> Option<LoadedThreadBinding> {
        self.shared
            .resolve_loaded_thread(thread_id)
            .await
            .map(|resolved| resolved.binding)
    }

    /// Resolves or interns a project-verified authority and returns its bound runtime view.
    pub(crate) async fn verified_thread_runtime(
        &self,
        project_id: ProjectId,
        thread_id: ThreadId,
    ) -> Result<ResolvedThreadRuntime, HarnessError> {
        let authority = self
            .shared
            .intern_thread_authority(thread_id, project_id)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?;
        Ok(ResolvedThreadRuntime::new(
            self.shared.runtime.clone(),
            authority,
        ))
    }

    /// Returns the current cross-thread runtime overview projection.
    pub(crate) fn runtime_overview(&self) -> ThreadRuntimeOverview {
        self.shared.runtime.current_overview()
    }

    pub(crate) async fn ensure_thread_writable(
        &self,
        project_id: ProjectId,
        thread_id: ThreadId,
    ) -> Result<(), HarnessError> {
        let thread = self
            .shared
            .store
            .load_thread(project_id, thread_id)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        match thread.kind {
            ThreadKind::Primary => Ok(()),
            ThreadKind::Subagent | ThreadKind::Orphan => {
                Err(HarnessError::ThreadReadOnly { thread: thread_id })
            }
        }
    }

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

    /// Constructs the registry-owned runtime support with the configured output limit.
    pub(crate) fn new_with_max_command_output_bytes(
        factory: Arc<dyn HarnessFactory>,
        hub: Arc<Hub>,
        max_command_output_bytes: usize,
        store: Arc<PersistStore>,
        ledger: LedgerHandle,
    ) -> Self {
        Self {
            shared: Arc::new(RegistryShared::new_with_max_command_output_bytes(
                hub,
                max_command_output_bytes,
                store,
                ledger,
            )),
            factory,
        }
    }

    pub(crate) fn thread_metadata_service(&self) -> Arc<ThreadMetadataService> {
        self.shared.thread_metadata.clone()
    }

    pub(crate) async fn project_model_catalog(
        &self,
        project: &ProjectConfig,
    ) -> Option<Vec<ModelDescriptor>> {
        self.shared
            .intern_project_authority(project.id)
            .await
            .model_catalog()
            .await
    }

    pub(crate) async fn replace_project_model_catalog(
        &self,
        project: &ProjectConfig,
        models: Vec<ModelDescriptor>,
    ) {
        self.shared
            .intern_project_authority(project.id)
            .await
            .replace_model_catalog(models)
            .await;
    }

    pub(crate) async fn remove_project_model_catalog(&self, project_id: ProjectId) {
        if let Some(authority) = self.shared.project_authority(project_id).await {
            authority.clear_model_catalog().await;
        }
    }

    /// Serialize persisted thread-graph mutations within one project. Child imports may originate
    /// from either an HTTP request or an asynchronously observed harness event, while subtree and
    /// project deletion mutate the same graph. One project-scoped lock makes each find/open/save
    /// or load/preflight/delete sequence atomic with respect to the others.
    async fn lock_project_lifecycle(&self, project_id: ProjectId) -> OwnedMutexGuard<()> {
        lock_project_lifecycle(&self.shared.projects, project_id).await
    }

    pub(crate) async fn lock_project_lifecycle_with_timeout(
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
        let authority = self.shared.intern_project_authority(project).await;
        // Fast path. This lock is a single global one guarding every project's harness and is
        // taken on ordinary per-event work, so the usual answer — "already running" — must not
        // wait behind anything slower than a map lookup.
        {
            let mut transitions = self.shared.harness_transitions.lock().await;
            let slot = transitions.project(&authority).await;
            if let Some(harness) = slot.active_or_creatable()? {
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
        let bootstrap = self.known_thread_bindings(project).await?;

        let mut transitions = self.shared.harness_transitions.lock().await;
        let mut slot = transitions.project(&authority).await;
        if let Some(harness) = slot.active_or_creatable()? {
            return Ok(harness);
        }
        let binding_count = bootstrap.known_threads.len();
        let h = self.factory.create(config, bootstrap).await?;
        debug!(project_id = %project, bindings = binding_count,
            "created harness with durable thread bindings installed");

        slot.publish_active(h.clone());
        Ok(h)
    }

    /// Every `(native id, ThreadId)` pair this project has already persisted.
    ///
    /// Read from the same thread files the thread graph is built from; nothing else is loaded, and
    /// turn files are never touched. A failed/incomplete scan or a non-bijective identity table is
    /// fatal: publishing a harness without every durable binding would reintroduce the startup
    /// window this bootstrap exists to close.
    async fn known_thread_bindings(
        &self,
        project: ProjectId,
    ) -> Result<HarnessBootstrap, HarnessError> {
        let graph = load_thread_graph(&self.shared.store, project)
            .await
            .map_err(|error| {
                HarnessError::Protocol(format!(
                    "could not load durable thread bindings for project {project}: {error}"
                ))
            })?;
        let mut native_ids = HashSet::new();
        let mut thread_ids = HashSet::new();
        let mut known_threads = Vec::with_capacity(graph.len());
        for thread in graph.values() {
            if thread.harness_thread_id.is_empty() {
                return Err(HarnessError::Protocol(format!(
                    "thread {} has an empty native thread id",
                    thread.id
                )));
            }
            if !native_ids.insert(HarnessThreadId::new(thread.harness_thread_id.clone())) {
                return Err(HarnessError::Protocol(format!(
                    "native thread id {} is bound more than once",
                    thread.harness_thread_id
                )));
            }
            if !thread_ids.insert(thread.id) {
                return Err(HarnessError::Protocol(format!(
                    "thread id {} is bound more than once",
                    thread.id
                )));
            }
            known_threads.push(KnownThreadBinding {
                harness_thread_id: thread.harness_thread_id.clone(),
                thread_id: thread.id,
            });
        }
        Ok(HarnessBootstrap { known_threads })
    }

    pub async fn open_thread(
        &self,
        config: &ProjectConfig,
        workspace_root: &str,
        thread: Option<ThreadId>,
        resume: Option<String>,
        initial_model: Option<ModelRef>,
    ) -> Result<ThreadHandle, HarnessError> {
        self.open_primary_thread(config, workspace_root, thread, resume, initial_model)
            .await
    }

    /// Attach a persisted provider-owned child without resuming or nudging native work.
    pub async fn attach_subagent_thread(
        &self,
        config: &ProjectConfig,
        thread: &ThreadFile,
    ) -> Result<ThreadHandle, HarnessError> {
        if thread.kind != ThreadKind::Subagent {
            return Err(HarnessError::Protocol(format!(
                "thread {} is not a sub-agent",
                thread.id
            )));
        }
        self.get_or_create_harness(config.id, config).await?;
        ensure_subagent_thread_open(config, thread, &self.shared).await?;
        self.loaded_thread_binding(thread.id)
            .await
            .map(|binding| binding.handle)
            .ok_or(HarnessError::ThreadNotFound(thread.id))
    }

    async fn open_primary_thread(
        &self,
        config: &ProjectConfig,
        workspace_root: &str,
        thread: Option<ThreadId>,
        resume: Option<String>,
        initial_model: Option<ModelRef>,
    ) -> Result<ThreadHandle, HarnessError> {
        debug!(
            project_id = %config.id,
            thread_id = display_opt(thread),
            resume = display_opt(resume.as_deref()),
            "opening harness thread"
        );
        // Serialize the cold check and native open for an already-known thread. Locking only when
        // publishing the owner is too late: two callers could both open the native thread and the
        // losing open may invalidate the stream already owned by the winner.
        let owner_guard = if let Some(thread_id) = thread {
            Some(lock_thread_owner_after_drain(&self.shared, thread_id).await)
        } else {
            None
        };
        if let Some(thread_id) = thread
            && let Some(existing) = self.shared.coordinator(thread_id).await
        {
            return existing
                .reusable_handle(
                    config.id,
                    thread_id,
                    resume.as_deref(),
                    ClassificationPhase::Primary,
                )
                .await;
        }
        let harness = self.get_or_create_harness(config.id, config).await?;
        let (updates, update_stream) = thread_update_channel();
        let authority = match thread {
            Some(thread_id) => Some(
                self.shared
                    .intern_thread_authority(thread_id, config.id)
                    .await
                    .map_err(|error| HarnessError::Protocol(error.to_string()))?,
            ),
            None => None,
        };
        let restore_permit = authority
            .as_ref()
            .map(|authority| self.shared.runtime.restoration_permit(authority));

        let handle = harness
            .open_thread(OpenThreadOptions {
                project: config.id,
                thread,
                workspace_root: workspace_root.into(),
                resume,
                initial_model: initial_model.clone(),
                updates,
            })
            .await?;
        // A known thread can begin another lifecycle while its harness open is in flight, so its
        // permit was captured above. A newly imported thread is not exposed until after this
        // function returns, making the harness-returned identity safe to capture here.
        let authority = match authority {
            Some(authority) => authority,
            None => self
                .shared
                .intern_thread_authority(handle.thread, config.id)
                .await
                .map_err(|error| HarnessError::Protocol(error.to_string()))?,
        };
        let restore_permit =
            restore_permit.unwrap_or_else(|| self.shared.runtime.restoration_permit(&authority));

        // Bind the model the harness reports as effective when it says so — Codex can ignore
        // resume overrides for a loaded thread, and the binding must reflect reality, not the
        // request (spec: model-provider-switching analysis).
        let native_model = handle
            .resumed_model
            .clone()
            .or_else(|| initial_model.clone());
        let binding = LoadedThreadBinding {
            project_id: config.id,
            handle: handle.clone(),
            native_model,
        };
        let owner_installed = if owner_guard.is_some() {
            install_event_owner_locked(
                &self.shared,
                &harness,
                binding,
                ClassificationPhase::Primary,
            )
            .await?
        } else {
            install_event_owner(
                &self.shared,
                &harness,
                binding,
                ClassificationPhase::Primary,
            )
            .await?
        };
        if owner_installed {
            drop(spawn_thread_update_forwarder(
                self.shared.clone(),
                config.id,
                handle.thread,
                update_stream,
                restore_permit,
            ));
        }
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
        let coordinator = self
            .shared
            .coordinator(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let binding = coordinator.binding().await;
        let project_id = binding.project_id;
        let handle = binding.handle.clone();
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
            model: TurnModel::Known(effective_model),
            mode: TurnMode::Known(overrides.mode),
            kind: TurnContextKind::User,
        };
        let request_started = Instant::now();
        let authority = self
            .shared
            .thread_authority(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let operation = self
            .shared
            .admit_operation(&authority, &coordinator, project_id, &handle, &ctx)
            .await?;
        let Some(task_permit) = self.shared.background_tasks.register() else {
            self.shared
                .abort_admitted_operation(&coordinator, operation)
                .await;
            return Err(HarnessError::Protocol(
                "server is shutting down; refusing to start a turn".into(),
            ));
        };
        let task_coordinator = coordinator.clone();
        let task_shared = self.shared.clone();
        let task = tokio::spawn(async move {
            let _task_permit = task_permit;
            match harness.start_turn(&handle, input, overrides).await {
                Ok(turn_id) => {
                    info!(
                        %project_id,
                        %thread_id,
                        %turn_id,
                        harness_thread_id = %handle.harness_thread_id,
                        mode = ?ctx.mode,
                        model = ?ctx.model,
                        ack_elapsed_ms = request_started.elapsed().as_millis(),
                        "harness accepted turn start request"
                    );
                    task_coordinator
                        .acknowledge_operation_turn(operation, turn_id)
                        .await;
                    publish_runtime_overview(&task_shared).await;
                    Ok(turn_id)
                }
                Err(error) => {
                    warn!(
                        %project_id,
                        %thread_id,
                        harness_thread_id = %handle.harness_thread_id,
                        mode = ?ctx.mode,
                        model = ?ctx.model,
                        error = %error,
                        ack_elapsed_ms = request_started.elapsed().as_millis(),
                        "harness rejected turn start request"
                    );
                    task_shared
                        .abort_admitted_operation(&task_coordinator, operation)
                        .await;
                    Err(error)
                }
            }
        });
        match task.await {
            Ok(result) => result,
            Err(error) => {
                self.shared
                    .abort_admitted_operation(&coordinator, operation)
                    .await;
                Err(HarnessError::Protocol(format!(
                    "turn start task failed: {error}"
                )))
            }
        }
    }

    /// Route an approval decision to the harness that raised it (§9.2).
    pub async fn respond_approval(
        &self,
        thread_id: ThreadId,
        request_id: ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<ThreadId, HarnessError> {
        let resolved = self
            .shared
            .resolve_loaded_thread(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let project_id = resolved.binding.project_id;

        let harness = self
            .shared
            .active_harness(project_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;

        let (claim, transition) = self.shared.runtime.claim_request(
            &resolved.authority,
            RuntimeRequestId::Approval(request_id.clone()),
        )?;
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
        self.shared.runtime.resolve_live_approval(
            &resolved.authority,
            request_id.clone(),
            decision,
        );
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
        let resolved = self
            .shared
            .resolve_loaded_thread(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let project_id = resolved.binding.project_id;

        let harness = self
            .shared
            .active_harness(project_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;

        let (claim, transition) = self.shared.runtime.claim_request(
            &resolved.authority,
            RuntimeRequestId::Server(request_id.clone()),
        )?;
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
            .resolve_live_server_request(&resolved.authority, request_id.clone());
        debug!(
            %thread_id,
            request_id = %request_id.0,
            "recorded server request resolution in live buffer for reconnect"
        );
        self.publish_request_transition(thread_id, transition).await;
        Ok(thread_id)
    }

    async fn publish_request_state(&self, thread_id: ThreadId, request_id: &RuntimeRequestId) {
        let Some(authority) = self.shared.thread_authority(thread_id).await else {
            return;
        };
        let Some(request) = self.shared.runtime.request_state(&authority, request_id) else {
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
        let binding = self
            .loaded_thread_binding(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let project_id = binding.project_id;
        let handle = binding.handle;
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
        let coordinator = self
            .shared
            .coordinator(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let binding = coordinator.binding().await;
        let project_id = binding.project_id;
        let handle = binding.handle;
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
            model: TurnModel::Known(effective_model),
            mode: TurnMode::Known(mode),
            kind: TurnContextKind::ManualCompaction,
        };
        let operation = self
            .shared
            .admit_operation(
                &self
                    .shared
                    .thread_authority(thread_id)
                    .await
                    .ok_or(HarnessError::ThreadNotFound(thread_id))?,
                &coordinator,
                project_id,
                &handle,
                &ctx,
            )
            .await?;
        let Some(task_permit) = self.shared.background_tasks.register() else {
            self.shared
                .abort_admitted_operation(&coordinator, operation)
                .await;
            return Err(HarnessError::Protocol(
                "server is shutting down; refusing context compaction".into(),
            ));
        };
        let task_coordinator = coordinator.clone();
        let task_shared = self.shared.clone();
        let task = tokio::spawn(async move {
            let _task_permit = task_permit;
            match harness.compact_thread(&handle).await {
                Ok(()) => {
                    info!(
                        %project_id,
                        %thread_id,
                        harness_thread_id = %handle.harness_thread_id,
                        ack_elapsed_ms = request_started.elapsed().as_millis(),
                        "harness accepted context compaction request"
                    );
                    Ok(())
                }
                Err(error) => {
                    task_shared
                        .abort_admitted_operation(&task_coordinator, operation)
                        .await;
                    Err(error)
                }
            }
        });
        match task.await {
            Ok(result) => result,
            Err(error) => {
                self.shared
                    .abort_admitted_operation(&coordinator, operation)
                    .await;
                Err(HarnessError::Protocol(format!(
                    "context compaction task failed: {error}"
                )))
            }
        }
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
            project_id,
            SubagentMaterializationJob {
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
        let binding = self
            .loaded_thread_binding(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let project_id = binding.project_id;
        let handle = binding.handle;
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
        self.ensure_thread_writable(config.id, thread_id).await?;
        let harness = self.get_or_create_harness(config.id, config).await?;
        let handle = self
            .loaded_thread_binding(thread_id)
            .await
            .map(|binding| binding.handle)
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
        self.ensure_thread_writable(config.id, thread_id).await?;
        let harness = self.get_or_create_harness(config.id, config).await?;
        let handle = self
            .loaded_thread_binding(thread_id)
            .await
            .map(|binding| binding.handle)
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
        let harness = self.get_or_create_harness(config.id, config).await?;
        let handle = self
            .loaded_thread_binding(thread_id)
            .await
            .map(|binding| binding.handle)
            .unwrap_or_else(|| ThreadHandle::detached(thread_id, harness_thread_id));
        harness.delete_thread(&handle).await?;
        self.retire_thread(thread_id).await;
        Ok(())
    }

    pub async fn thread_has_active_turn(&self, thread_id: ThreadId) -> bool {
        let Some(authority) = self.shared.thread_authority(thread_id).await else {
            return false;
        };
        self.shared.runtime.has_active_turn(&authority)
    }

    pub async fn forget_thread(&self, thread_id: ThreadId) {
        let owner_guard = lock_thread_owner(&self.shared.threads, thread_id).await;
        let authority = self.shared.thread_authority(thread_id).await;
        let coordinator = match authority.as_ref() {
            Some(authority) => authority.coordinator().await,
            None => None,
        };
        let control = match coordinator.as_ref() {
            Some(coordinator) => coordinator.begin_retirement().await,
            None => None,
        };
        if let Some(control) = control.as_ref() {
            let _ = control.cancel.send(true);
        }
        drop(owner_guard);

        if let Some(mut control) = control {
            wait_for_owner_completion(&mut control).await;
        }

        let _owner_guard = lock_thread_owner(&self.shared.threads, thread_id).await;
        if let (Some(authority), Some(coordinator)) = (authority.as_ref(), coordinator) {
            authority.clear_coordinator_if(&coordinator).await;
            coordinator.finish_retirement().await;
        }
    }

    pub async fn retire_thread(&self, thread_id: ThreadId) {
        let authority = self.shared.thread_authority(thread_id).await;
        self.forget_thread(thread_id).await;
        if let Some(authority) = authority {
            self.shared.runtime.forget_threads(&[authority]);
        }
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
            let mut transitions = self.shared.harness_transitions.lock().await;
            transitions.begin_shutdown();
            let authorities = self
                .shared
                .projects
                .lock()
                .await
                .projects
                .values()
                .cloned()
                .collect::<Vec<_>>();
            let mut harnesses = HashMap::new();
            for authority in authorities {
                let mut harness = transitions.project(&authority).await;
                if let Some(harness) = harness.take_for_shutdown() {
                    harnesses.insert(authority.project_id(), harness);
                }
            }
            harnesses
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
        let coordinators = self.shared.coordinator_snapshot().await;
        let mut thread_ids = HashSet::new();
        for (thread_id, coordinator) in coordinators {
            if coordinator.binding().await.project_id == project_id {
                thread_ids.insert(thread_id);
            }
        }
        let authority = self.shared.project_authority(project_id).await;
        let harness = if let Some(authority) = authority.as_ref() {
            let mut transitions = self.shared.harness_transitions.lock().await;
            transitions.project(authority).await.begin_delete()?
        } else {
            None
        };
        if let Some(harness) = harness {
            if let Err(error) = harness.shutdown().await {
                let mut transitions = self.shared.harness_transitions.lock().await;
                if let Some(authority) = authority.as_ref() {
                    transitions
                        .project(authority)
                        .await
                        .rollback_delete_if_running(harness);
                }
                return Err(error);
            }
            let mut transitions = self.shared.harness_transitions.lock().await;
            if let Some(authority) = authority.as_ref() {
                transitions.project(authority).await.finish_delete(&harness);
            }
        }

        let mut thread_authorities = Vec::new();
        for thread_id in &thread_ids {
            if let Some(authority) = self.shared.thread_authority(*thread_id).await {
                thread_authorities.push(authority);
            }
            self.forget_thread(*thread_id).await;
        }
        self.shared.runtime.forget_threads(&thread_authorities);
        publish_runtime_overview(&self.shared).await;

        Ok(())
    }
}

async fn lock_project_lifecycle(
    projects: &Arc<Mutex<ProjectIndex>>,
    project_id: ProjectId,
) -> OwnedMutexGuard<()> {
    let lock = {
        let mut index = projects.lock().await;
        if let Some(authority) = index.projects.get(&project_id) {
            authority.lifecycle_lock()
        } else {
            index
                .unpublished_locks
                .retain(|_, lock| lock.strong_count() > 0);
            match index
                .unpublished_locks
                .get(&project_id)
                .and_then(WeakLifecycleLock::upgrade)
            {
                Some(lock) => lock,
                None => {
                    let lock = LifecycleLock::new();
                    index.unpublished_locks.insert(project_id, lock.downgrade());
                    lock
                }
            }
        }
    };
    lock.lock_owned().await
}

async fn lock_thread_owner(
    threads: &Arc<Mutex<ThreadIndex>>,
    thread_id: ThreadId,
) -> OwnedMutexGuard<()> {
    let lock = {
        let mut index = threads.lock().await;
        if let Some(authority) = index.threads.get(&thread_id) {
            authority.owner_lock()
        } else {
            index
                .unpublished_locks
                .retain(|_, lock| lock.strong_count() > 0);
            match index
                .unpublished_locks
                .get(&thread_id)
                .and_then(WeakOwnerLock::upgrade)
            {
                Some(lock) => lock,
                None => {
                    let lock = OwnerLock::new();
                    index.unpublished_locks.insert(thread_id, lock.downgrade());
                    lock
                }
            }
        }
    };
    lock.lock_owned().await
}

async fn wait_for_owner_completion(control: &mut EventOwnerControl) {
    while !*control.completed.borrow() {
        if control.completed.changed().await.is_err() {
            break;
        }
    }
}

/// Lock an owner slot only after its previous generation has finished draining. Waiting happens
/// without the slot lock, so retirement and owner-task completion can always make progress.
async fn lock_thread_owner_after_drain(
    shared: &RegistryShared,
    thread_id: ThreadId,
) -> OwnedMutexGuard<()> {
    loop {
        let owner_guard = lock_thread_owner(&shared.threads, thread_id).await;
        let Some(authority) = shared.thread_authority(thread_id).await else {
            return owner_guard;
        };
        let coordinator = authority.coordinator().await;
        let Some(coordinator) = coordinator else {
            return owner_guard;
        };
        if coordinator.is_retired().await {
            authority.clear_coordinator_if(&coordinator).await;
            return owner_guard;
        }
        let Some(mut control) = coordinator.draining_control().await else {
            return owner_guard;
        };
        if *control.completed.borrow() {
            drop(owner_guard);
            tokio::task::yield_now().await;
            continue;
        }
        if control.completed.has_changed().is_err() {
            authority.clear_coordinator_if(&coordinator).await;
            coordinator.finish_retirement().await;
            return owner_guard;
        }
        drop(owner_guard);
        wait_for_owner_completion(&mut control).await;
    }
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
    title: Option<String>,
}

type SubagentMaterializationResult = Result<Option<ThreadId>, HarnessError>;

struct SubagentMaterializationJob {
    spawned_by_turn_id: TurnId,
    item_id: ItemId,
    origin: &'static str,
    info: SubagentActivityInfo,
    result: Option<oneshot::Sender<SubagentMaterializationResult>>,
}

fn subagent_activity_info(item: &Item) -> Option<SubagentActivityInfo> {
    match &item.payload {
        ItemPayload::Activity {
            title, subagent, ..
        } => subagent_link_info(subagent.as_ref(), Some(title.clone())),
        ItemPayload::ToolCall { subagent, .. } => subagent_link_info(subagent.as_ref(), None),
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

    let live_events = match shared.thread_authority(parent_thread_id).await {
        Some(authority) => shared.runtime.live_item_events(&authority, item_id),
        None => Vec::new(),
    };
    for event in live_events.into_iter().rev() {
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
    subagent_link_info(tool.subagent.as_ref(), None)
}

fn subagent_link_info(
    subagent: Option<&giskard_core::item::SubagentLink>,
    title: Option<String>,
) -> Option<SubagentActivityInfo> {
    let subagent = subagent?;
    let native_thread_id = trimmed_non_empty(&subagent.harness_thread_id)?;
    let agent_path = subagent
        .path
        .as_deref()
        .and_then(trimmed_non_empty)
        .map(ToOwned::to_owned);
    Some(SubagentActivityInfo {
        native_thread_id: native_thread_id.to_owned(),
        agent_name: None,
        agent_path,
        title,
    })
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

async fn materialize_subagent_thread(
    parent_thread_id: ThreadId,
    project_id: ProjectId,
    spawned_by_turn_id: TurnId,
    info: SubagentActivityInfo,
    shared: Arc<RegistryShared>,
) -> Result<Option<ThreadId>, HarnessError> {
    let _lifecycle_guard = lock_project_lifecycle(&shared.projects, project_id).await;
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
    let live_bindings = shared.coordinator_snapshot().await;
    let mut live_existing_id = None;
    for (thread_id, coordinator) in live_bindings {
        let binding = coordinator.binding().await;
        if binding.project_id == project_id
            && binding.handle.harness_thread_id == info.native_thread_id
        {
            live_existing_id = Some(thread_id);
            break;
        }
    }
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

    if let Some(mut existing) = existing {
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
            None if existing.kind == ThreadKind::Primary => ExistingLinkDisposition::PrimaryThread,
            None if existing.kind == ThreadKind::Orphan => ExistingLinkDisposition::OwnedChild,
            None if existing.parent_thread_id.is_none() => ExistingLinkDisposition::DifferentParent,
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
        if existing.kind == ThreadKind::Orphan {
            let desired_title = subagent_thread_title(&info);
            let mutation = shared
                .thread_metadata
                .classify_orphan(
                    project_id,
                    existing.id,
                    existing.revision,
                    crate::thread_metadata::OrphanClassification {
                        parent_thread_id,
                        spawned_by_turn_id,
                        title: desired_title,
                        mode: parent_file.mode,
                        permission_preset: parent_file.permission_preset,
                    },
                )
                .await
                .map_err(|error| HarnessError::Protocol(error.to_string()))?;
            existing = mutation.into_current().ok_or_else(|| {
                HarnessError::Protocol(format!(
                    "orphan thread {} disappeared during classification",
                    existing.id
                ))
            })?;
            if existing.kind != ThreadKind::Subagent
                || existing.parent_thread_id != Some(parent_thread_id)
                || existing.spawned_by_turn_id != Some(spawned_by_turn_id)
            {
                return Err(HarnessError::Protocol(format!(
                    "orphan thread {} was classified concurrently with conflicting ownership",
                    existing.id
                )));
            }
            if let Some(coordinator) = shared.coordinator(existing.id).await {
                coordinator.classify_orphan_as_subagent().await?;
            }
            shared
                .thread_metadata
                .publish_created(project_id, &existing)
                .await;
        }
        let opened_agent_name =
            ensure_subagent_thread_open(&project_config, &existing, &shared).await?;
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

    let mode = parent_file.mode;
    let permission_preset = parent_file.permission_preset;

    let harness = shared
        .active_harness(project_id)
        .await
        .ok_or(HarnessError::ThreadNotFound(parent_thread_id))?;
    // The harness already runs this child inside its parent's turn, so its cwd is the parent's
    // workspace. Passing the project's checkout instead would be ignored while the child is live
    // and applied on its next cold resume, moving the thread out of the worktree its own earlier
    // work is in.
    let workspace_root =
        effective_thread_workspace_root(&shared.store, &project_config, &parent_file)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?;
    let child_thread_id = ThreadId::new();
    let handle = harness
        .claim_native_thread(
            child_thread_id,
            info.native_thread_id.clone(),
            workspace_root.into(),
        )
        .await?;
    if handle.harness_thread_id != info.native_thread_id {
        return Err(HarnessError::Protocol(format!(
            "linked-thread claim returned native thread {} instead of {}",
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
    let current_model = handle
        .resumed_model
        .clone()
        .map(TurnModel::Known)
        .unwrap_or(TurnModel::Unknown);
    let info = subagent_info_with_agent_name(info, handle.agent_name.clone());
    let now = Utc::now();
    let thread_file = ThreadFile {
        revision: 0,
        version: giskard_persist::store::THREAD_METADATA_VERSION,
        id: handle.thread,
        project_id,
        title: subagent_thread_title(&info),
        harness_thread_id: handle.harness_thread_id.clone(),
        parent_thread_id: Some(parent_thread_id),
        spawned_by_turn_id: Some(spawned_by_turn_id),
        kind: ThreadKind::Subagent,
        mode,
        current_model: current_model.clone(),
        context_window: 0,
        model_context_windows: HashMap::new(),
        permission_preset,
        model_efforts: HashMap::new(),
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
    let native_model = current_model.as_known().cloned();
    install_event_owner(
        &shared,
        &harness,
        LoadedThreadBinding {
            project_id,
            handle: handle.clone(),
            native_model,
        },
        ClassificationPhase::Subagent,
    )
    .await?;
    // The thread and binding are durable even if observation setup below fails. Publish the
    // creation now so a retry cannot leave the catalog unaware of an already-existing child.
    shared
        .thread_metadata
        .publish_created(project_id, &thread_file)
        .await;
    Ok(Some(handle.thread))
}

async fn enqueue_subagent_materialization(
    parent_thread_id: ThreadId,
    project_id: ProjectId,
    mut job: SubagentMaterializationJob,
    shared: Arc<RegistryShared>,
) {
    let establishment_permit = if shared.thread_authority(parent_thread_id).await.is_none() {
        match shared.background_tasks.register() {
            Some(permit) => Some(permit),
            None => {
                reject_materialization_during_shutdown(parent_thread_id, project_id, &mut job);
                return;
            }
        }
    } else {
        None
    };
    let authority = match shared
        .intern_thread_authority(parent_thread_id, project_id)
        .await
    {
        Ok(authority) => authority,
        Err(error) => {
            warn!(
                %project_id,
                %parent_thread_id,
                turn_id = %job.spawned_by_turn_id,
                item_id = %job.item_id,
                origin = %job.origin,
                error = %error,
                "rejecting sub-agent materialization job for mismatched thread authority"
            );
            if let Some(result) = job.result.take() {
                let _ = result.send(Err(HarnessError::Protocol(error.to_string())));
            }
            return;
        }
    };
    let worker_permit = match authority
        .enqueue_materialization_job(job, establishment_permit, &shared.background_tasks)
        .await
    {
        Ok(permit) => permit,
        Err(mut rejected) => {
            reject_materialization_during_shutdown(parent_thread_id, project_id, &mut rejected);
            return;
        }
    };
    if let Some(permit) = worker_permit {
        tokio::spawn(async move {
            let _permit = permit;
            run_subagent_materialization_queue(authority, shared).await;
        });
    }
}

fn reject_materialization_during_shutdown(
    parent_thread_id: ThreadId,
    project_id: ProjectId,
    job: &mut SubagentMaterializationJob,
) {
    warn!(
        %project_id,
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
}

async fn run_subagent_materialization_queue(
    authority: Arc<ThreadAuthority>,
    shared: Arc<RegistryShared>,
) {
    let parent_thread_id = authority.thread_id();
    let project_id = authority.project_id();
    loop {
        let job = authority.next_materialization_job().await;
        let Some(job) = job else {
            return;
        };
        let result = materialize_subagent_thread(
            parent_thread_id,
            project_id,
            job.spawned_by_turn_id,
            job.info,
            shared.clone(),
        )
        .await;
        match &result {
            Ok(Some(subagent_thread_id)) => {
                info!(
                    %project_id,
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
                    %project_id,
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

async fn ensure_subagent_thread_open(
    project_config: &ProjectConfig,
    thread_file: &ThreadFile,
    shared: &Arc<RegistryShared>,
) -> Result<Option<String>, HarnessError> {
    let harness = shared
        .active_harness(project_config.id)
        .await
        .ok_or(HarnessError::ThreadNotFound(thread_file.id))?;
    // A sub-agent is provider-owned and read-only. Reattach its durable identity to this harness
    // lifetime without issuing thread/resume or otherwise nudging native work.
    let workspace_root =
        effective_thread_workspace_root(&shared.store, project_config, thread_file)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?;
    let _owner_guard = lock_thread_owner_after_drain(shared, thread_file.id).await;
    if let Some(coordinator) = shared.coordinator(thread_file.id).await {
        let handle = coordinator
            .reusable_handle(
                project_config.id,
                thread_file.id,
                Some(&thread_file.harness_thread_id),
                ClassificationPhase::Subagent,
            )
            .await?;
        return Ok(handle.agent_name);
    }
    let handle = harness
        .claim_native_thread(
            thread_file.id,
            thread_file.harness_thread_id.clone(),
            workspace_root.into(),
        )
        .await?;
    if handle.harness_thread_id != thread_file.harness_thread_id {
        return Err(HarnessError::Protocol(format!(
            "linked-thread claim returned native thread {} instead of {}",
            handle.harness_thread_id, thread_file.harness_thread_id
        )));
    }
    let native_model = handle
        .resumed_model
        .clone()
        .or_else(|| thread_file.current_model.as_known().cloned());
    let agent_name = handle.agent_name.clone();
    install_event_owner_locked(
        shared,
        &harness,
        LoadedThreadBinding {
            project_id: project_config.id,
            handle,
            native_model,
        },
        ClassificationPhase::Subagent,
    )
    .await?;
    Ok(agent_name)
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

fn launch_event_forwarder(
    shared: Arc<RegistryShared>,
    authority: Arc<ThreadAuthority>,
    coordinator: Arc<ThreadCoordinator>,
    stream: giskard_harness::AgentEventStream,
    cancel: watch::Receiver<bool>,
    completed: watch::Sender<bool>,
    permit: RegistryTaskPermit,
) {
    tokio::spawn(async move {
        let _permit = permit;
        let cancellation_probe = cancel.clone();
        let thread_id = coordinator.binding().await.handle.thread;
        let exit_reason = forward_events(
            shared.clone(),
            authority.clone(),
            coordinator.clone(),
            stream,
            cancel,
        )
        .await;
        let cancelled = *cancellation_probe.borrow();
        if !cancelled && exit_reason != ForwarderExitReason::PersistenceBlocked {
            coordinator.owner_finished(false).await;
            if authority.clear_coordinator_if(&coordinator).await {
                warn!(
                    %thread_id,
                    exit_reason = forwarder_exit_reason_label(exit_reason),
                    "removed failed event owner so the thread can be reopened"
                );
            }
        } else {
            coordinator.owner_finished(cancelled).await;
        }
        let _ = completed.send(true);
    });
}

async fn install_event_owner(
    shared: &Arc<RegistryShared>,
    harness: &Arc<dyn AgentHarness>,
    binding: LoadedThreadBinding,
    classification: ClassificationPhase,
) -> Result<bool, HarnessError> {
    let thread_id = binding.handle.thread;
    let _owner_guard = lock_thread_owner_after_drain(shared, thread_id).await;
    install_event_owner_locked(shared, harness, binding, classification).await
}

async fn install_event_owner_locked(
    shared: &Arc<RegistryShared>,
    harness: &Arc<dyn AgentHarness>,
    binding: LoadedThreadBinding,
    classification: ClassificationPhase,
) -> Result<bool, HarnessError> {
    let thread_id = binding.handle.thread;
    let project_id = binding.project_id;
    let authority = shared
        .intern_thread_authority(thread_id, project_id)
        .await
        .map_err(|error| HarnessError::Protocol(error.to_string()))?;
    let existing = authority.coordinator().await;
    if let Some(existing) = existing {
        existing
            .reusable_handle(
                project_id,
                thread_id,
                Some(&binding.handle.harness_thread_id),
                classification,
            )
            .await?;
        debug!(%project_id, %thread_id, "reused existing long-lived native event owner");
        return Ok(false);
    }

    let stream = harness.subscribe(&binding.handle);
    let Some(permit) = shared.background_tasks.register() else {
        return Err(HarnessError::Protocol(
            "server is shutting down; refusing to install event owner".into(),
        ));
    };
    let coordinator = Arc::new(ThreadCoordinator::new(binding, classification));
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (completed_tx, completed_rx) = watch::channel(false);
    coordinator
        .activate_owner(EventOwnerControl {
            cancel: cancel_tx,
            completed: completed_rx,
        })
        .await?;
    if authority
        .install_coordinator_if_empty(coordinator.clone())
        .await
        .is_err()
    {
        return Err(HarnessError::Protocol(format!(
            "thread {thread_id} event owner installation conflicted with an existing coordinator"
        )));
    }
    launch_event_forwarder(
        shared.clone(),
        authority,
        coordinator.clone(),
        stream,
        cancel_rx,
        completed_tx,
        permit,
    );
    debug!(%project_id, %thread_id, "installed long-lived native event owner");
    Ok(true)
}

fn external_turn_defaults(
    binding: &LoadedThreadBinding,
    persisted: Option<&ThreadFile>,
) -> ExternalTurnDefaults {
    ExternalTurnDefaults {
        model: persisted
            .map(|thread| thread.current_model.clone())
            .or_else(|| binding.native_model.clone().map(TurnModel::Known))
            .unwrap_or(TurnModel::Unknown),
        mode: persisted
            .map(|thread| thread.mode)
            .unwrap_or(TurnMode::Unknown),
    }
}

async fn forward_events(
    shared: Arc<RegistryShared>,
    authority: Arc<ThreadAuthority>,
    coordinator: Arc<ThreadCoordinator>,
    mut stream: giskard_harness::AgentEventStream,
    mut cancel: watch::Receiver<bool>,
) -> ForwarderExitReason {
    let binding = coordinator.binding().await;
    let thread_id = binding.handle.thread;
    let project_id = binding.project_id;
    let persisted = shared
        .store
        .load_thread(project_id, thread_id)
        .await
        .ok()
        .flatten();
    let idle_defaults = external_turn_defaults(&binding, persisted.as_ref());
    let idle_context = TurnContext {
        user_input: UserInput::text(""),
        model: idle_defaults.model,
        mode: idle_defaults.mode,
        kind: TurnContextKind::User,
    };
    let mut ctx = idle_context.clone();
    let mut turn_gate: Option<ThreadTurnLease> = None;
    let hub = shared.hub.clone();
    let runtime = shared.runtime.clone();
    // Establish the authority once. Per-event permits must only observe this entry, never recreate
    // it after retirement.
    drop(runtime.restoration_permit(&authority));
    let store = shared.store.clone();
    let mut turn_id: Option<TurnId> = None;
    let mut owned_turn: Option<TurnId> = None;
    let mut owned_token: Option<CoordinatorToken> = None;
    let mut started_at = Utc::now();
    let mut current_turn_items = CurrentTurnItems::default();
    let mut diffs: Vec<giskard_core::FileDiff> = Vec::new();
    let mut seen_turn_ids = persisted_turn_ids(&store, project_id, thread_id).await;
    let mut seen_notices = HashSet::new();
    let mut item_ids_by_harness: HashMap<HarnessItemKey, ItemId> = HashMap::new();
    let forwarder_started = Instant::now();
    let mut saw_context_compaction_marker = false;
    let mut stream_error: Option<String> = None;
    debug!(
        %project_id,
        %thread_id,
        context_kind = turn_context_kind_label(ctx.kind),
        mode = ?ctx.mode,
        model = ?ctx.model,
        turn_gate_held = turn_gate.as_ref().is_some_and(|lease| !lease.is_released()),
        persisted_turn_count = seen_turn_ids.len(),
        "event forwarder started"
    );

    let exit_reason = loop {
        let recv_result = tokio::select! {
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    break ForwarderExitReason::StreamEndedWithoutTurn;
                }
                continue;
            }
            result = stream.recv() => result,
        };
        match recv_result {
            Ok(event) => {
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
                        // A command may outlive its persisted turn. Its terminal replacement must
                        // still reach the late-event path while a newer turn is active; it updates
                        // runtime task state only and cannot enter the newer turn's transcript.
                        // Events for any other non-owned, non-persisted turn remain a protocol
                        // violation and are dropped before they mutate runtime or persistence.
                        if turn != owned && !seen_turn_ids.contains(&turn) {
                            log_cross_turn_event_drop(
                                project_id,
                                thread_id,
                                owned,
                                turn,
                                &event,
                                forwarder_started.elapsed().as_millis(),
                            );
                            continue;
                        }
                    }
                } else if let Some(turn) = event_turn
                    && !seen_turn_ids.contains(&turn)
                {
                    let persisted = shared
                        .store
                        .load_thread(project_id, thread_id)
                        .await
                        .ok()
                        .flatten();
                    let external_defaults = external_turn_defaults(&binding, persisted.as_ref());
                    let claim = match coordinator.claim_native_turn(turn, external_defaults).await {
                        Ok(claim) => claim,
                        Err(error) => {
                            error!(%project_id, %thread_id, %turn, %error,
                                "event owner could not claim native turn");
                            break ForwarderExitReason::DuplicateForwarder;
                        }
                    };
                    ctx = claim.context;
                    if claim.external {
                        match runtime.reserve_turn(
                            &authority,
                            turn_reservation(project_id, &binding.handle, &ctx),
                        ) {
                            Ok(mut lease) => {
                                let _ = lease.acknowledge_turn(turn);
                                if let Err(mut lease) = coordinator
                                    .install_native_turn_gate(claim.token, turn, lease)
                                    .await
                                {
                                    if let Some(overview) = lease.release() {
                                        shared.hub.publish_runtime_overview(overview).await;
                                    }
                                    coordinator.finish_native_turn(claim.token, turn).await;
                                    break ForwarderExitReason::RuntimeAuthorityReplaced;
                                }
                                publish_runtime_overview(&shared).await;
                            }
                            Err(error) => {
                                error!(%project_id, %thread_id, %turn, %error,
                                    "event owner could not reserve an external native turn");
                                coordinator.finish_native_turn(claim.token, turn).await;
                                break ForwarderExitReason::RuntimeAuthorityReplaced;
                            }
                        }
                    }
                    owned_token = Some(claim.token);
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
                        let Some(permit) = runtime.event_application_permit(&authority) else {
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
                        let before = terminating_command_before_terminal_completion(
                            &runtime, &authority, &event,
                        )
                        .await;
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
                                &authority,
                                &event,
                                false,
                                prepared_item_output,
                            ),
                        };
                        if let AgentEvent::ItemCompleted { turn, item, .. } = &event {
                            shared
                                .runtime
                                .remove_command_output(&authority, *turn, item.id);
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
                        shared
                            .runtime
                            .remove_tool_output(&authority, *turn, item.id);
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
                    continue;
                }

                if owned_turn.is_none() && event_turn.is_none() {
                    let applied = shared.runtime.apply_event(&authority, &event, false);
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
                let event = runtime.capture_event_diffs(&authority, event);

                if let AgentEvent::ContextWindowUpdated {
                    turn,
                    model,
                    context_window,
                    ..
                } = &event
                {
                    if ctx.model.as_known().is_some_and(|expected| {
                        model.provider != expected.provider || model.model != expected.model
                    }) {
                        error!(
                            %project_id,
                            %thread_id,
                            turn = %turn,
                            expected_model = ?ctx.model,
                            event_provider = %model.provider,
                            event_model = %model.model,
                            "dropping context-window update for the wrong turn model"
                        );
                        continue;
                    }
                    if ctx.model.as_known().is_none() {
                        ctx.model = TurnModel::Known(model.clone());
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
                        if let Some(token) = owned_token
                            && let Some(overview) =
                                coordinator.acknowledge_native_turn(token, *turn).await
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
                                project_id,
                                SubagentMaterializationJob {
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
                                project_id,
                                SubagentMaterializationJob {
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
                        if current_turn_items.upsert(item) {
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

                // A harness may deliver an item for an unseen turn before TurnStarted. Start the
                // reconnect buffer from the first turn-scoped event and reuse it when the delayed
                // start arrives, otherwise a reload in that window loses the already-visible item.
                let mut append_to_live_buffer = true;
                if let Some(buffer_turn) = event_turn
                    && let Err(existing_turn) = runtime.ensure_live_turn(
                        &authority,
                        buffer_turn,
                        live_turn_user_input(&ctx),
                    )
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
                            &authority,
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
                            &authority,
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
                    if let Some(token) = owned_token {
                        turn_gate = coordinator
                            .take_native_turn_gate(token, completed_turn)
                            .await;
                    }
                    let Some(tid) = complete_forwarded_turn(
                        &authority,
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
                    hub.broadcast_event(thread_id, event).await;
                    if runtime.has_running_for_turn(&authority, tid) {
                        info!(
                            %project_id,
                            %thread_id,
                            turn = %tid,
                            elapsed_ms = forwarder_started.elapsed().as_millis(),
                            "event forwarder monitoring after-turn running commands"
                        );
                    }
                    if let Some(token) = owned_token.take() {
                        coordinator.finish_native_turn(token, tid).await;
                    }
                    turn_gate = None;
                    turn_id = None;
                    owned_turn = None;
                    ctx = idle_context.clone();
                    started_at = Utc::now();
                    current_turn_items = CurrentTurnItems::default();
                    diffs.clear();
                    seen_notices.clear();
                    item_ids_by_harness.clear();
                    saw_context_compaction_marker = false;
                    continue;
                }

                broadcast_event_with_context(&hub, project_id, thread_id, event, &ctx).await;
            }
            Err(e) => {
                let lagged = matches!(e, broadcast::error::RecvError::Lagged(_));
                stream_error = Some(e.to_string());
                if turn_gate.is_none()
                    && let (Some(token), Some(turn)) = (owned_token, owned_turn)
                {
                    turn_gate = coordinator.take_native_turn_gate(token, turn).await;
                }
                if ctx.kind == TurnContextKind::ManualCompaction {
                    let live_buffer_active = runtime.live_is_active(&authority);
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
                    let live_buffer_active = runtime.live_is_active(&authority);
                    let turn_gate_held =
                        turn_gate.as_ref().is_some_and(|lease| !lease.is_released());
                    let status = TurnStatus {
                        kind: TurnStatusKind::Interrupted,
                        message: Some(if lagged {
                            "Harness event stream lagged before turn completion".into()
                        } else {
                            "Harness event stream ended before turn completion".into()
                        }),
                    };
                    warn!(
                        %project_id,
                        %thread_id,
                        turn = %incomplete_turn,
                        context_kind = turn_context_kind_label(ctx.kind),
                        mode = ?ctx.mode,
                        model = ?ctx.model,
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
                        &authority,
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
                    hub.broadcast_event(thread_id, completion_event).await;
                    if let Some(token) = owned_token.take() {
                        coordinator.finish_native_turn(token, incomplete_turn).await;
                    }
                    if lagged {
                        turn_gate = None;
                        turn_id = None;
                        owned_turn = None;
                        ctx = idle_context.clone();
                        started_at = Utc::now();
                        current_turn_items = CurrentTurnItems::default();
                        diffs.clear();
                        seen_notices.clear();
                        item_ids_by_harness.clear();
                        saw_context_compaction_marker = false;
                        stream_error = None;
                        continue;
                    }
                    break ForwarderExitReason::StreamEndedRecovered;
                } else if lagged {
                    error!(
                        %project_id,
                        %thread_id,
                        ?e,
                        "event owner lagged while idle; continuing with retained events"
                    );
                    stream_error = None;
                    continue;
                } else {
                    break ForwarderExitReason::StreamEndedWithoutTurn;
                }
            }
        }
    };
    if turn_gate.is_none()
        && let (Some(token), Some(turn)) = (owned_token, owned_turn)
    {
        turn_gate = coordinator.take_native_turn_gate(token, turn).await;
    }
    if owned_turn.is_none() {
        turn_gate = coordinator.take_unclaimed_operation().await;
    }
    let turn_gate_held = turn_gate.as_ref().is_some_and(|lease| !lease.is_released());
    if turn_gate_held {
        warn!(
            %project_id,
            %thread_id,
            context_kind = turn_context_kind_label(ctx.kind),
            mode = ?ctx.mode,
            model = ?ctx.model,
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
    if let (Some(token), Some(turn)) = (owned_token, owned_turn) {
        coordinator.finish_native_turn(token, turn).await;
    }
    exit_reason
}

#[allow(clippy::too_many_arguments)]
async fn complete_forwarded_turn(
    authority: &Arc<ThreadAuthority>,
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
    let captured_diffs = shared.runtime.captured_diff_records(authority, tid);
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
                .settle_completed_turn(authority, &completion_event, None),
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
                authority,
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

/// Harness-neutral native thread identity used only for bootstrap uniqueness checks.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct HarnessThreadId(String);

impl HarnessThreadId {
    fn new(value: String) -> Self {
        Self(value)
    }
}

/// Harness-neutral native item identity retained by one thread's event forwarder.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct HarnessItemId(String);

impl HarnessItemId {
    fn new(value: String) -> Self {
        Self(value)
    }
}

/// Scoped harness item identity; native item IDs need only be unique within a turn.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct HarnessItemKey {
    turn_id: TurnId,
    harness_item_id: HarnessItemId,
}

impl HarnessItemKey {
    fn new(turn_id: TurnId, harness_item_id: HarnessItemId) -> Self {
        Self {
            turn_id,
            harness_item_id,
        }
    }
}

fn track_item_identity(
    item_ids_by_harness: &mut HashMap<HarnessItemKey, ItemId>,
    event: &AgentEvent,
) -> Option<(TurnId, String, ItemId, ItemId)> {
    let (turn, harness_item_id, item_id) = event_item_identity(event)?;
    let identity_key = HarnessItemKey::new(turn, HarnessItemId::new(harness_item_id.to_owned()));
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
    runtime: &ThreadRuntimeSupport,
    authority: &Arc<ThreadAuthority>,
    event: &AgentEvent,
) -> Option<RunningTask> {
    let AgentEvent::ItemCompleted { turn, item, .. } = event else {
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

    let command = runtime.task_by_item(authority, *turn, item.id)?;
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
    let model = turn.model.clone();
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
        match model.into_known() {
            Some(model) => {
                ledger
                    .record(project_id, date, model.provider, model.model, usage)
                    .await;
            }
            None => ledger.record_unattributed(project_id, date, usage).await,
        }
    }

    PersistTurnOutcome {
        history_appended: true,
        metadata_updated: true,
        history_error: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Weak};

    use chrono::Utc;
    use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
    use giskard_core::error::HarnessError;
    use giskard_core::event::AgentEvent;
    use giskard_core::ids::{ApprovalId, ItemId, ProjectId, ServerRequestId, ThreadId, TurnId};
    use giskard_core::item::{
        CommandExecutionStart, Item, ItemDelta, ItemKind, ItemPayload, ItemStart,
    };
    use giskard_core::model::{ModelDescriptor, ModelRef};
    use giskard_core::server_request::ServerRequest;
    use giskard_core::token::{TokenLedger, TokenUsage};
    use giskard_core::turn::{
        Mode, PermissionPreset, Turn, TurnMode, TurnModel, TurnStatus, TurnStatusKind,
    };
    use giskard_core::user_input::UserInput;
    use giskard_harness::{
        AgentEventStream, AgentHarness, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
        ThreadUpdate,
    };
    use giskard_persist::PersistStore;
    use giskard_persist::store::{ProjectConfig, ThreadFile};
    use giskard_proto::{ServerMessage, WireAgentEvent};
    use tokio::sync::{Notify, broadcast, mpsc};
    use tokio::task::JoinHandle;

    use super::{
        CurrentTurnItems, TurnContext, TurnContextKind, command_completion_is_normal_success,
        command_status_is_running, completed_tool_has_terminal_output, event_item_delta_kind,
        event_item_id, event_turn_id, forward_events, late_command_completion_message,
        log_cross_turn_event_drop, log_foreign_thread_event_drop,
        log_metadata_only_event_rejection, prepare_thread_updates, spawn_thread_update_forwarder,
        track_item_identity, turn_reservation,
    };
    use crate::hub::Hub;
    use crate::ledger;
    use crate::test_logs::CapturedLogWriter;
    use crate::thread_runtime::ThreadRuntimeSupport;

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
            _bootstrap: giskard_harness::HarnessBootstrap,
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
        authority_probe: StdMutex<Option<Weak<super::RegistryShared>>>,
        observed_runtime_before_return: AtomicBool,
    }

    #[async_trait::async_trait]
    impl AgentHarness for BindingOrderHarness {
        fn capabilities(&self) -> HarnessCapabilities {
            HarnessCapabilities::default()
        }

        async fn list_models(&self) -> Result<Vec<giskard_core::ModelDescriptor>, HarnessError> {
            Ok(Vec::new())
        }

        async fn open_thread(
            &self,
            opts: giskard_harness::OpenThreadOptions,
        ) -> Result<ThreadHandle, HarnessError> {
            if self.bound.load(Ordering::SeqCst) == 0 {
                self.opened_before_bound.fetch_add(1, Ordering::SeqCst);
            }
            let shared = self.authority_probe.lock().unwrap().clone();
            if let (Some(shared), Some(thread_id)) =
                (shared.and_then(|weak| weak.upgrade()), opts.thread)
                && let Some(authority) = shared.thread_authority(thread_id).await
                && authority.runtime_entry().is_some()
                && authority.coordinator().await.is_none()
            {
                self.observed_runtime_before_return
                    .store(true, Ordering::SeqCst);
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
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl super::HarnessFactory for BindingOrderFactory {
        async fn create(
            &self,
            _config: &ProjectConfig,
            bootstrap: giskard_harness::HarnessBootstrap,
        ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            // Held open so a second caller is certainly inside construction. The harness must not
            // become reachable until its complete bootstrap has been installed.
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            self.harness
                .bound
                .store(bootstrap.known_threads.len().max(1), Ordering::SeqCst);
            Ok(self.harness.clone())
        }
    }

    struct SerializedFactory {
        calls: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl super::HarnessFactory for SerializedFactory {
        async fn create(
            &self,
            _config: &ProjectConfig,
            _bootstrap: giskard_harness::HarnessBootstrap,
        ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            tokio::task::yield_now().await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(Arc::new(ShutdownHarness {
                calls: Arc::new(AtomicUsize::new(0)),
                fail: false,
            }))
        }
    }

    struct BlockingFactory {
        started: Arc<Notify>,
        release: Arc<Notify>,
        harness: Arc<ShutdownHarness>,
    }

    #[async_trait::async_trait]
    impl super::HarnessFactory for BlockingFactory {
        async fn create(
            &self,
            _config: &ProjectConfig,
            _bootstrap: giskard_harness::HarnessBootstrap,
        ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(self.harness.clone())
        }
    }

    async fn create_test_project(store: &PersistStore, name: &str) -> (ProjectId, ProjectConfig) {
        let project_id = ProjectId::new();
        store
            .create_project(project_id, name, "/tmp/test")
            .await
            .unwrap();
        let config = store
            .load_project(project_id)
            .await
            .unwrap()
            .expect("the project was just created");
        (project_id, config)
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
            authority_probe: StdMutex::new(None),
            observed_runtime_before_return: AtomicBool::new(false),
        });
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let registry = Arc::new(super::HarnessRegistry::new(
            Arc::new(BindingOrderFactory {
                harness: harness.clone(),
                calls: factory_calls.clone(),
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
        assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn first_message_runtime_exists_before_native_open_returns() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (_project_id, config) = create_test_project(&store, "first-message").await;
        let harness = Arc::new(BindingOrderHarness {
            bound: Arc::new(AtomicUsize::new(1)),
            opened_before_bound: Arc::new(AtomicUsize::new(0)),
            authority_probe: StdMutex::new(None),
            observed_runtime_before_return: AtomicBool::new(false),
        });
        let registry = super::HarnessRegistry::new(
            Arc::new(BindingOrderFactory {
                harness: harness.clone(),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        );
        *harness.authority_probe.lock().unwrap() = Some(Arc::downgrade(&registry.shared));
        let thread_id = ThreadId::new();

        let result = registry
            .open_thread(&config, "/tmp/test", Some(thread_id), None, None)
            .await;

        assert!(result.is_err());
        assert!(
            harness
                .observed_runtime_before_return
                .load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn runtime_entry_does_not_require_a_coordinator() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let shared =
            super::RegistryShared::new(Arc::new(Hub::new()), store.clone(), ledger::spawn(store));
        let authority = shared
            .intern_thread_authority(ThreadId::new(), ProjectId::new())
            .await
            .unwrap();

        let _permit = shared.runtime.restoration_permit(&authority);

        assert!(authority.runtime_entry().is_some());
        assert!(authority.coordinator().await.is_none());
    }

    #[tokio::test]
    async fn different_project_harness_creation_remains_globally_serialized() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (first_id, first) = create_test_project(&store, "first").await;
        let (second_id, second) = create_test_project(&store, "second").await;
        let calls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let registry = Arc::new(super::HarnessRegistry::new(
            Arc::new(SerializedFactory {
                calls: calls.clone(),
                active: active.clone(),
                max_active: max_active.clone(),
            }),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));

        let first_call = {
            let registry = registry.clone();
            tokio::spawn(async move { registry.get_or_create_harness(first_id, &first).await })
        };
        let second_call = {
            let registry = registry.clone();
            tokio::spawn(async move { registry.get_or_create_harness(second_id, &second).await })
        };
        first_call.await.unwrap().unwrap();
        second_call.await.unwrap().unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(max_active.load(Ordering::SeqCst), 1);
        assert!(registry.shared.active_harness(first_id).await.is_some());
        assert!(registry.shared.active_harness(second_id).await.is_some());
    }

    #[tokio::test]
    async fn harness_creation_cannot_publish_after_shutdown_fence() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, config) = create_test_project(&store, "project").await;
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let harness = Arc::new(ShutdownHarness {
            calls: Arc::new(AtomicUsize::new(0)),
            fail: false,
        });
        let registry = Arc::new(super::HarnessRegistry::new(
            Arc::new(BlockingFactory {
                started: started.clone(),
                release: release.clone(),
                harness: harness.clone(),
            }),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));

        let creating = {
            let registry = registry.clone();
            tokio::spawn(async move { registry.get_or_create_harness(project_id, &config).await })
        };
        started.notified().await;
        let shutting_down = {
            let registry = registry.clone();
            tokio::spawn(async move { registry.shutdown().await })
        };
        tokio::task::yield_now().await;
        release.notify_one();

        creating.await.unwrap().unwrap();
        shutting_down.await.unwrap().unwrap();
        let authority = registry.shared.project_authority(project_id).await.unwrap();
        assert!(authority.harness_is_empty().await);
        assert_eq!(harness.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn conflicting_durable_bindings_prevent_harness_publication() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        store
            .create_project(project_id, "proj", "/tmp/test")
            .await
            .unwrap();
        let now = Utc::now();
        for thread_id in [ThreadId::new(), ThreadId::new()] {
            store
                .save_thread(
                    project_id,
                    &ThreadFile {
                        revision: 0,
                        version: giskard_persist::store::THREAD_METADATA_VERSION,
                        id: thread_id,
                        project_id,
                        title: "conflict".into(),
                        harness_thread_id: "duplicate-native-id".into(),
                        parent_thread_id: None,
                        spawned_by_turn_id: None,
                        kind: giskard_core::ThreadKind::Primary,
                        mode: TurnMode::Known(Mode::Build),
                        current_model: TurnModel::Known(ModelRef {
                            provider: "openai".into(),
                            model: "test".into(),
                            reasoning_effort: None,
                        }),
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
        }
        let registry = super::HarnessRegistry::new(
            Arc::new(UnusedHarnessFactory),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        );
        let config = store.load_project(project_id).await.unwrap().unwrap();

        let error = match registry.get_or_create_harness(project_id, &config).await {
            Ok(_) => panic!("conflicting durable identities must prevent harness creation"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("bound more than once"));
        let authority = registry
            .shared
            .project_authority(project_id)
            .await
            .expect("verified project has an authority shell");
        assert!(
            authority.harness_is_empty().await,
            "a conflicting bootstrap must not publish any harness state"
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
        let successful = registry
            .shared
            .intern_project_authority(ProjectId::new())
            .await;
        {
            let mut transitions = registry.shared.harness_transitions.lock().await;
            transitions
                .project(&successful)
                .await
                .publish_active(Arc::new(ShutdownHarness {
                    calls: successful_calls.clone(),
                    fail: false,
                }));
        }
        let failing = registry
            .shared
            .intern_project_authority(ProjectId::new())
            .await;
        {
            let mut transitions = registry.shared.harness_transitions.lock().await;
            transitions
                .project(&failing)
                .await
                .publish_active(Arc::new(ShutdownHarness {
                    calls: failing_calls.clone(),
                    fail: true,
                }));
        }

        let error = registry.shutdown().await.unwrap_err();
        assert!(error.to_string().contains("injected shutdown failure"));
        assert_eq!(successful_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failing_calls.load(Ordering::SeqCst), 1);

        registry.shutdown().await.unwrap();
        assert_eq!(successful_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failing_calls.load(Ordering::SeqCst), 1);
        assert!(
            registry
                .shared
                .harness_transitions
                .lock()
                .await
                .is_shutting_down()
        );
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
        let project_id = ProjectId::new();
        let (result, receiver) = tokio::sync::oneshot::channel();
        super::enqueue_subagent_materialization(
            parent_thread_id,
            project_id,
            super::SubagentMaterializationJob {
                spawned_by_turn_id: TurnId::new(),
                item_id: ItemId::new(),
                origin: "test",
                info: super::SubagentActivityInfo {
                    native_thread_id: "native-child".into(),
                    agent_name: None,
                    agent_path: None,
                    title: None,
                },
                result: Some(result),
            },
            registry.shared.clone(),
        )
        .await;
        assert!(receiver.await.unwrap().is_err());
        assert!(
            registry
                .shared
                .thread_authority(parent_thread_id)
                .await
                .is_none()
        );
    }

    fn materialization_job(
        item_id: ItemId,
    ) -> (
        super::SubagentMaterializationJob,
        tokio::sync::oneshot::Receiver<super::SubagentMaterializationResult>,
    ) {
        let (result, receiver) = tokio::sync::oneshot::channel();
        (
            super::SubagentMaterializationJob {
                spawned_by_turn_id: TurnId::new(),
                item_id,
                origin: "test",
                info: super::SubagentActivityInfo {
                    native_thread_id: format!("native-{item_id}"),
                    agent_name: None,
                    agent_path: None,
                    title: None,
                },
                result: Some(result),
            },
            receiver,
        )
    }

    #[tokio::test]
    async fn parent_materialization_queues_are_fifo_and_independent() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, _) = create_test_project(&store, "queues").await;
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));
        let lifecycle = super::lock_project_lifecycle(&shared.projects, project_id).await;
        let first_parent = ThreadId::new();
        let second_parent = ThreadId::new();
        let first_item = ItemId::new();
        let second_item = ItemId::new();
        let other_item = ItemId::new();
        let (first, first_result) = materialization_job(first_item);
        let (second, second_result) = materialization_job(second_item);
        let (other, other_result) = materialization_job(other_item);

        super::enqueue_subagent_materialization(first_parent, project_id, first, shared.clone())
            .await;
        super::enqueue_subagent_materialization(first_parent, project_id, second, shared.clone())
            .await;
        super::enqueue_subagent_materialization(second_parent, project_id, other, shared.clone())
            .await;
        tokio::task::yield_now().await;

        let first_authority = shared.thread_authority(first_parent).await.unwrap();
        let second_authority = shared.thread_authority(second_parent).await.unwrap();
        assert!(!Arc::ptr_eq(&first_authority, &second_authority));
        while first_authority
            .materialization_job_ids()
            .await
            .is_some_and(|queue| queue.len() > 1)
        {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            first_authority.materialization_job_ids().await.unwrap(),
            vec![second_item]
        );
        assert!(second_authority.has_materialization_worker().await);

        drop(lifecycle);
        assert!(first_result.await.unwrap().is_err());
        assert!(second_result.await.unwrap().is_err());
        assert!(other_result.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn clearing_coordinator_does_not_replace_active_parent_worker() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, _) = create_test_project(&store, "active-worker").await;
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));
        let parent_thread_id = ThreadId::new();
        let authority = shared
            .intern_thread_authority(parent_thread_id, project_id)
            .await
            .unwrap();
        let coordinator = Arc::new(super::ThreadCoordinator::new(
            super::LoadedThreadBinding {
                project_id,
                handle: ThreadHandle::detached(parent_thread_id, "native-parent".into()),
                native_model: None,
            },
            super::ClassificationPhase::Primary,
        ));
        assert!(
            authority
                .install_coordinator_if_empty(coordinator)
                .await
                .is_ok()
        );
        let lifecycle = super::lock_project_lifecycle(&shared.projects, project_id).await;
        let (first, first_result) = materialization_job(ItemId::new());
        let (second, second_result) = materialization_job(ItemId::new());

        super::enqueue_subagent_materialization(
            parent_thread_id,
            project_id,
            first,
            shared.clone(),
        )
        .await;
        while authority
            .materialization_job_ids()
            .await
            .is_some_and(|queue| !queue.is_empty())
        {
            tokio::task::yield_now().await;
        }
        let coordinator = authority.coordinator().await.unwrap();
        assert!(authority.clear_coordinator_if(&coordinator).await);
        super::enqueue_subagent_materialization(
            parent_thread_id,
            project_id,
            second,
            shared.clone(),
        )
        .await;

        let retained = shared.thread_authority(parent_thread_id).await.unwrap();
        assert!(Arc::ptr_eq(&authority, &retained));
        assert_eq!(
            authority.materialization_job_ids().await.unwrap().len(),
            1,
            "the active worker dequeued one job and the second remains on its FIFO"
        );
        drop(lifecycle);
        assert!(first_result.await.unwrap().is_err());
        assert!(second_result.await.unwrap().is_err());
        while authority.has_materialization_worker().await {
            tokio::task::yield_now().await;
        }
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
        let authority = registry.shared.intern_project_authority(project_id).await;
        {
            let mut transitions = registry.shared.harness_transitions.lock().await;
            let mut slot = transitions.project(&authority).await;
            slot.publish_active(harness);
            slot.begin_delete().unwrap();
        }
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

    #[tokio::test]
    async fn failed_project_deletion_restores_the_same_active_harness() {
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
            fail: true,
        });
        let authority = registry.shared.intern_project_authority(project_id).await;
        {
            let mut transitions = registry.shared.harness_transitions.lock().await;
            transitions
                .project(&authority)
                .await
                .publish_active(harness.clone());
        }

        let error = registry.delete_project(project_id).await.unwrap_err();
        assert!(error.to_string().contains("injected shutdown failure"));
        let mut transitions = registry.shared.harness_transitions.lock().await;
        let restored = transitions
            .project(&authority)
            .await
            .active()
            .expect("failed deletion restores an active harness");
        assert!(Arc::ptr_eq(&restored, &harness));
    }

    #[tokio::test]
    async fn authority_catalog_preserves_absence_replace_and_removal() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let registry = super::HarnessRegistry::new(
            Arc::new(UnusedHarnessFactory),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        );
        let (project_id, config) = create_test_project(&store, "project").await;
        assert!(registry.project_model_catalog(&config).await.is_none());

        let first = vec![ModelDescriptor::conservative("provider", "first")];
        registry
            .replace_project_model_catalog(&config, first.clone())
            .await;
        assert_eq!(registry.project_model_catalog(&config).await, Some(first));

        let second = vec![ModelDescriptor::conservative("provider", "second")];
        registry
            .replace_project_model_catalog(&config, second.clone())
            .await;
        assert_eq!(registry.project_model_catalog(&config).await, Some(second));

        registry.remove_project_model_catalog(project_id).await;
        assert!(registry.project_model_catalog(&config).await.is_none());
    }

    #[tokio::test]
    async fn project_authority_adopts_the_contended_lifecycle_mutex() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let registry = super::HarnessRegistry::new(
            Arc::new(UnusedHarnessFactory),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        );
        let project_id = ProjectId::new();
        let guard = registry.lock_project_lifecycle(project_id).await;
        let interned_lock = registry
            .shared
            .projects
            .lock()
            .await
            .unpublished_locks
            .get(&project_id)
            .and_then(super::WeakLifecycleLock::upgrade)
            .expect("held lifecycle lock remains interned");

        let authority = registry.shared.intern_project_authority(project_id).await;
        assert!(authority.lifecycle_lock().ptr_eq(&interned_lock));
        assert!(
            !registry
                .shared
                .projects
                .lock()
                .await
                .unpublished_locks
                .contains_key(&project_id)
        );
        drop(guard);
    }

    #[tokio::test]
    async fn arbitrary_project_lookup_and_lock_do_not_intern_authority() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let registry = super::HarnessRegistry::new(
            Arc::new(UnusedHarnessFactory),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        );
        let project_id = ProjectId::new();

        assert!(
            registry
                .shared
                .project_authority(project_id)
                .await
                .is_none()
        );
        let guard = registry.lock_project_lifecycle(project_id).await;
        assert!(
            registry
                .shared
                .project_authority(project_id)
                .await
                .is_none()
        );
        drop(guard);
        assert!(
            registry
                .shared
                .project_authority(project_id)
                .await
                .is_none()
        );
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

    pub(super) fn test_coordinator(
        classification: super::ClassificationPhase,
    ) -> super::ThreadCoordinator {
        super::ThreadCoordinator::new(
            super::LoadedThreadBinding {
                project_id: ProjectId::new(),
                handle: ThreadHandle::detached(ThreadId::new(), "native-test".into()),
                native_model: None,
            },
            classification,
        )
    }

    async fn install_test_coordinator(
        shared: &super::RegistryShared,
        coordinator: Arc<super::ThreadCoordinator>,
    ) -> Arc<super::ThreadAuthority> {
        let binding = coordinator.binding().await;
        let authority = shared
            .intern_thread_authority(binding.handle.thread, binding.project_id)
            .await
            .unwrap();
        assert!(
            authority
                .install_coordinator_if_empty(coordinator)
                .await
                .is_ok()
        );
        authority
    }

    pub(super) fn test_authority(
        binding: &super::LoadedThreadBinding,
    ) -> Arc<super::ThreadAuthority> {
        Arc::new(super::ThreadAuthority::new_for_test(
            binding.handle.thread,
            binding.project_id,
        ))
    }

    pub(super) fn test_turn_context() -> TurnContext {
        TurnContext {
            user_input: UserInput::text("test"),
            model: TurnModel::Known(ModelRef {
                provider: "openai".into(),
                model: "test".into(),
                reasoning_effort: None,
            }),
            mode: TurnMode::Known(Mode::Build),
            kind: TurnContextKind::User,
        }
    }

    pub(super) async fn prepare_test_operation(
        coordinator: &super::ThreadCoordinator,
        runtime: &ThreadRuntimeSupport,
        context: TurnContext,
    ) -> super::CoordinatorToken {
        let binding = coordinator.binding().await;
        let authority = test_authority(&binding);
        let lease = runtime
            .reserve_turn(
                &authority,
                turn_reservation(binding.project_id, &binding.handle, &context),
            )
            .unwrap();
        match coordinator.prepare_operation(context, lease).await {
            Ok(operation) => operation,
            Err((error, _)) => panic!("test operation was rejected: {error}"),
        }
    }

    #[tokio::test]
    async fn closing_owner_cannot_deadlock_forget_thread_behind_owner_lock() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let registry = Arc::new(super::HarnessRegistry::new(
            Arc::new(UnusedHarnessFactory),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));
        let coordinator = Arc::new(test_coordinator(super::ClassificationPhase::Primary));
        let thread_id = coordinator.binding().await.handle.thread;
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let (completed_tx, completed_rx) = tokio::sync::watch::channel(false);
        coordinator
            .activate_owner(super::EventOwnerControl {
                cancel: cancel_tx,
                completed: completed_rx,
            })
            .await
            .unwrap();
        let authority = install_test_coordinator(&registry.shared, coordinator.clone()).await;
        let permit = registry.shared.background_tasks.register().unwrap();
        let (events, _) = broadcast::channel(2);
        super::launch_event_forwarder(
            registry.shared.clone(),
            authority,
            coordinator,
            AgentEventStream::new(events.subscribe()),
            cancel_rx,
            completed_tx,
            permit,
        );

        let owner_guard = super::lock_thread_owner(&registry.shared.threads, thread_id).await;
        let registry_forget = registry.clone();
        let forget = tokio::spawn(async move {
            registry_forget.forget_thread(thread_id).await;
        });
        tokio::task::yield_now().await;
        drop(events);
        tokio::task::yield_now().await;
        drop(owner_guard);

        tokio::time::timeout(std::time::Duration::from_secs(2), forget)
            .await
            .expect("forget_thread must not deadlock with a self-exiting owner")
            .unwrap();
    }

    #[tokio::test]
    async fn installer_waits_for_draining_owner_without_holding_owner_lock() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));
        let coordinator = Arc::new(test_coordinator(super::ClassificationPhase::Primary));
        let thread_id = coordinator.binding().await.handle.thread;
        let (cancel_tx, _) = tokio::sync::watch::channel(false);
        let (completed_tx, completed_rx) = tokio::sync::watch::channel(false);
        coordinator
            .activate_owner(super::EventOwnerControl {
                cancel: cancel_tx,
                completed: completed_rx,
            })
            .await
            .unwrap();
        coordinator.begin_retirement().await.unwrap();
        let authority = install_test_coordinator(&shared, coordinator.clone()).await;

        let waiter_shared = shared.clone();
        let waiter = tokio::spawn(async move {
            super::lock_thread_owner_after_drain(&waiter_shared, thread_id).await
        });
        tokio::task::yield_now().await;

        let independent_guard = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            super::lock_thread_owner(&shared.threads, thread_id),
        )
        .await
        .expect("an installer waiting for completion must release the owner lock");
        drop(independent_guard);
        coordinator.owner_finished(true).await;
        completed_tx.send(true).unwrap();
        let owner_guard = tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
            .await
            .expect("the installer should resume after the draining owner completes")
            .unwrap();

        assert!(authority.coordinator().await.is_none());
        assert!(coordinator.is_retired().await);
        drop(owner_guard);
    }

    #[tokio::test]
    async fn installer_retires_a_draining_owner_whose_completion_sender_closed() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));
        let coordinator = Arc::new(test_coordinator(super::ClassificationPhase::Primary));
        let thread_id = coordinator.binding().await.handle.thread;
        let (cancel_tx, _) = tokio::sync::watch::channel(false);
        let (completed_tx, completed_rx) = tokio::sync::watch::channel(false);
        coordinator
            .activate_owner(super::EventOwnerControl {
                cancel: cancel_tx,
                completed: completed_rx,
            })
            .await
            .unwrap();
        coordinator.begin_retirement().await.unwrap();
        let authority = install_test_coordinator(&shared, coordinator.clone()).await;
        drop(completed_tx);

        let owner_guard = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            super::lock_thread_owner_after_drain(&shared, thread_id),
        )
        .await
        .expect("a closed completion channel must terminate draining");

        assert!(authority.coordinator().await.is_none());
        assert!(coordinator.is_retired().await);
        drop(owner_guard);
    }

    #[tokio::test]
    async fn failed_forwarder_does_not_clear_a_replacement_coordinator() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));
        let coordinator = Arc::new(test_coordinator(super::ClassificationPhase::Primary));
        let binding = coordinator.binding().await;
        let thread_id = binding.handle.thread;
        let authority = install_test_coordinator(&shared, coordinator.clone()).await;
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let (completed_tx, mut completed_rx) = tokio::sync::watch::channel(false);
        coordinator
            .activate_owner(super::EventOwnerControl {
                cancel: cancel_tx,
                completed: completed_rx.clone(),
            })
            .await
            .unwrap();
        let replacement = Arc::new(super::ThreadCoordinator::new(
            binding,
            super::ClassificationPhase::Primary,
        ));
        assert!(authority.clear_coordinator_if(&coordinator).await);
        assert!(
            authority
                .install_coordinator_if_empty(replacement.clone())
                .await
                .is_ok()
        );
        let permit = shared.background_tasks.register().unwrap();
        let (events, _) = broadcast::channel(2);
        super::launch_event_forwarder(
            shared,
            authority.clone(),
            coordinator,
            AgentEventStream::new(events.subscribe()),
            cancel_rx,
            completed_tx,
            permit,
        );
        drop(events);
        while !*completed_rx.borrow() {
            completed_rx.changed().await.unwrap();
        }

        let installed = authority
            .coordinator()
            .await
            .expect("replacement coordinator remains installed");
        assert!(Arc::ptr_eq(&installed, &replacement));
        assert_eq!(authority.thread_id(), thread_id);
    }

    #[tokio::test]
    async fn forgetting_and_reopening_reuses_the_same_thread_authority() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let registry = super::HarnessRegistry::new(
            Arc::new(UnusedHarnessFactory),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        );
        let first = Arc::new(test_coordinator(super::ClassificationPhase::Primary));
        let binding = first.binding().await;
        let thread_id = binding.handle.thread;
        let authority = install_test_coordinator(&registry.shared, first).await;

        registry.forget_thread(thread_id).await;
        assert!(authority.coordinator().await.is_none());
        let retained = registry.shared.thread_authority(thread_id).await.unwrap();
        assert!(Arc::ptr_eq(&authority, &retained));

        let reopened = Arc::new(super::ThreadCoordinator::new(
            binding,
            super::ClassificationPhase::Primary,
        ));
        let reopened_authority = install_test_coordinator(&registry.shared, reopened).await;
        assert!(Arc::ptr_eq(&authority, &reopened_authority));
        assert!(authority.coordinator().await.is_some());
    }

    #[tokio::test]
    async fn thread_authority_rejects_a_second_project_association() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let shared =
            super::RegistryShared::new(Arc::new(Hub::new()), store.clone(), ledger::spawn(store));
        let thread_id = ThreadId::new();
        let first_project = ProjectId::new();
        let second_project = ProjectId::new();
        let authority = shared
            .intern_thread_authority(thread_id, first_project)
            .await
            .unwrap();

        let error = match shared
            .intern_thread_authority(thread_id, second_project)
            .await
        {
            Ok(_) => panic!("a stable thread authority cannot change projects"),
            Err(error) => error,
        };
        assert_eq!(
            error,
            super::ThreadProjectMismatch {
                thread_id,
                existing_project_id: first_project,
                requested_project_id: second_project,
            }
        );
        assert!(Arc::ptr_eq(
            &authority,
            &shared.thread_authority(thread_id).await.unwrap()
        ));
    }

    #[tokio::test]
    async fn thread_authority_adopts_the_contended_owner_mutex() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let shared =
            super::RegistryShared::new(Arc::new(Hub::new()), store.clone(), ledger::spawn(store));
        let thread_id = ThreadId::new();
        let owner_guard = super::lock_thread_owner(&shared.threads, thread_id).await;
        let interned_owner = shared
            .threads
            .lock()
            .await
            .unpublished_locks
            .get(&thread_id)
            .and_then(super::WeakOwnerLock::upgrade)
            .expect("held owner mutex remains weakly interned");

        let authority = shared
            .intern_thread_authority(thread_id, ProjectId::new())
            .await
            .unwrap();
        assert!(authority.owner_lock().ptr_eq(&interned_owner));
        assert!(!authority.owner_lock().is_unlocked());
        assert!(
            !shared
                .threads
                .lock()
                .await
                .unpublished_locks
                .contains_key(&thread_id)
        );
        drop(owner_guard);
    }

    #[tokio::test]
    async fn empty_thread_authority_is_absent_from_coordinator_lookups_and_scans() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let registry = super::HarnessRegistry::new(
            Arc::new(UnusedHarnessFactory),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        );
        let thread_id = ThreadId::new();
        registry
            .shared
            .intern_thread_authority(thread_id, ProjectId::new())
            .await
            .unwrap();

        assert!(registry.loaded_thread_binding(thread_id).await.is_none());
        assert!(
            registry
                .shared
                .coordinator_snapshot()
                .await
                .into_iter()
                .all(|(candidate, _)| candidate != thread_id)
        );
    }

    #[tokio::test]
    async fn runtime_facade_resolves_one_registry_owned_support() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let registry = super::HarnessRegistry::new(
            Arc::new(UnusedHarnessFactory),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        );
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();

        assert!(registry.thread_runtime(thread_id).await.is_none());
        assert!(registry.shared.thread_authority(thread_id).await.is_none());

        let first = registry
            .verified_thread_runtime(project_id, thread_id)
            .await
            .unwrap();
        let handle = ThreadHandle::detached(thread_id, "facade-native".into());
        let _lease = first.reserve_turn_for_test(turn_reservation(
            project_id,
            &handle,
            &test_turn_context(),
        ));

        let later = registry.thread_runtime(thread_id).await.unwrap();
        assert!(later.has_active_turn());
        assert!(
            registry
                .runtime_overview()
                .threads
                .iter()
                .any(|summary| summary.thread_id == thread_id)
        );
    }

    #[tokio::test]
    async fn loaded_thread_binding_is_coherent_across_coordinator_replacement() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let registry = super::HarnessRegistry::new(
            Arc::new(UnusedHarnessFactory),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        );
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let authority = registry
            .shared
            .intern_thread_authority(thread_id, project_id)
            .await
            .unwrap();
        let model_a = ModelRef {
            provider: "provider-a".into(),
            model: "model-a".into(),
            reasoning_effort: None,
        };
        let coordinator_a = Arc::new(super::ThreadCoordinator::new(
            super::LoadedThreadBinding {
                project_id,
                handle: ThreadHandle::detached(thread_id, "native-a".into()),
                native_model: Some(model_a.clone()),
            },
            super::ClassificationPhase::Primary,
        ));
        assert!(
            authority
                .install_coordinator_if_empty(coordinator_a.clone())
                .await
                .is_ok()
        );

        let snapshot_a = registry.loaded_thread_binding(thread_id).await.unwrap();
        let model_b = ModelRef {
            provider: "provider-b".into(),
            model: "model-b".into(),
            reasoning_effort: None,
        };
        let coordinator_b = Arc::new(super::ThreadCoordinator::new(
            super::LoadedThreadBinding {
                project_id,
                handle: ThreadHandle::detached(thread_id, "native-b".into()),
                native_model: Some(model_b.clone()),
            },
            super::ClassificationPhase::Primary,
        ));
        assert!(authority.clear_coordinator_if(&coordinator_a).await);
        assert!(
            authority
                .install_coordinator_if_empty(coordinator_b)
                .await
                .is_ok()
        );

        assert_eq!(snapshot_a.handle().harness_thread_id, "native-a");
        assert_eq!(snapshot_a.native_model(), Some(&model_a));
        let snapshot_b = registry.loaded_thread_binding(thread_id).await.unwrap();
        assert_eq!(snapshot_b.handle().harness_thread_id, "native-b");
        assert_eq!(snapshot_b.native_model(), Some(&model_b));
    }

    #[tokio::test]
    async fn loaded_thread_binding_distinguishes_absent_unknown_and_known_model() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let registry = super::HarnessRegistry::new(
            Arc::new(UnusedHarnessFactory),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        );
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let authority = registry
            .shared
            .intern_thread_authority(thread_id, project_id)
            .await
            .unwrap();
        assert!(registry.loaded_thread_binding(thread_id).await.is_none());

        let unknown_coordinator = Arc::new(super::ThreadCoordinator::new(
            super::LoadedThreadBinding {
                project_id,
                handle: ThreadHandle::detached(thread_id, "native-unknown".into()),
                native_model: None,
            },
            super::ClassificationPhase::Primary,
        ));
        assert!(
            authority
                .install_coordinator_if_empty(unknown_coordinator.clone())
                .await
                .is_ok()
        );
        let unknown = registry.loaded_thread_binding(thread_id).await.unwrap();
        assert!(unknown.native_model().is_none());

        let model = ModelRef {
            provider: "provider".into(),
            model: "model".into(),
            reasoning_effort: None,
        };
        let known = Arc::new(super::ThreadCoordinator::new(
            super::LoadedThreadBinding {
                project_id,
                handle: ThreadHandle::detached(thread_id, "native-known".into()),
                native_model: Some(model.clone()),
            },
            super::ClassificationPhase::Primary,
        ));
        assert!(authority.clear_coordinator_if(&unknown_coordinator).await);
        assert!(authority.install_coordinator_if_empty(known).await.is_ok());
        let known = registry.loaded_thread_binding(thread_id).await.unwrap();
        assert_eq!(known.native_model(), Some(&model));
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
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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
        let (sink, stream, permit) = prepare_thread_updates(&shared, project_id, thread_id).await;
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
            let (sink, stream, permit) =
                prepare_thread_updates(&shared, project_id, stale_thread_id).await;
            let stale_authority = shared.thread_authority(stale_thread_id).await.unwrap();
            if invalidate_with_turn {
                let handle = ThreadHandle::detached(stale_thread_id, "native-stale".into());
                let ctx = TurnContext {
                    user_input: UserInput::text("newer turn"),
                    model: TurnModel::Known(model.clone()),
                    mode: TurnMode::Known(Mode::Build),
                    kind: TurnContextKind::User,
                };
                let _lease = shared
                    .runtime
                    .reserve_turn(
                        &stale_authority,
                        turn_reservation(project_id, &handle, &ctx),
                    )
                    .unwrap();
            } else {
                shared.runtime.forget_threads(&[stale_authority]);
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
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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
        drop(tx);

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
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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
        drop(tx);

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
                    assert_eq!(
                        state.metadata.current_model,
                        TurnModel::Known(model.clone())
                    );
                }
            }
        }
        assert_eq!(
            matching_states, 1,
            "matching update must survive coalescing into the latest committed thread state"
        );
    }

    #[tokio::test]
    async fn one_long_lived_forwarder_persists_successive_native_turns() {
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
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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

        let (runtime, authority) = spawn_forwarder(
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
            runtime.captured_diff(&authority, second_turn, &rejected_id),
            crate::thread_runtime::RuntimeDiffLookup::Missing
        ));

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
        assert_eq!(saved[1].user_input, UserInput::text(""));
    }

    #[tokio::test]
    async fn long_lived_forwarder_uses_current_external_context_for_each_turn() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let parent_thread_id = ThreadId::new();
        let initial_model = ModelRef {
            provider: "openai".into(),
            model: "initial".into(),
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
                    title: "orphan".into(),
                    harness_thread_id: "native-orphan".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Orphan,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(initial_model.clone()),
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

        let (tx, rx) = broadcast::channel(32);
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let coordinator = Arc::new(super::ThreadCoordinator::new(
            super::LoadedThreadBinding {
                project_id,
                handle: ThreadHandle::detached(thread_id, "native-orphan".into()),
                native_model: Some(initial_model),
            },
            super::ClassificationPhase::Orphan,
        ));
        let authority = Arc::new(super::ThreadAuthority::new_for_test(thread_id, project_id));
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let (_completed_tx, completed_rx) = tokio::sync::watch::channel(false);
        coordinator
            .activate_owner(super::EventOwnerControl {
                cancel: cancel_tx,
                completed: completed_rx,
            })
            .await
            .unwrap();
        let forwarder = tokio::spawn(forward_events(
            shared.clone(),
            authority,
            coordinator.clone(),
            AgentEventStream::new(rx),
            cancel_rx,
        ));
        assert_eq!(tx.receiver_count(), 1);

        let first_turn = TurnId::new();
        for event in turn_events(
            thread_id,
            first_turn,
            "ignored",
            "first",
            TokenUsage::new(1, 1),
        ) {
            tx.send(event).unwrap();
        }
        wait_for_turn_count(&store, project_id, thread_id, 1).await;

        let orphan = store
            .load_thread(project_id, thread_id)
            .await
            .unwrap()
            .unwrap();
        shared
            .thread_metadata
            .classify_orphan(
                project_id,
                thread_id,
                orphan.revision,
                crate::thread_metadata::OrphanClassification {
                    parent_thread_id,
                    spawned_by_turn_id: first_turn,
                    title: "sub-agent".into(),
                    mode: TurnMode::Known(Mode::Plan),
                    permission_preset: PermissionPreset::AskFirst,
                },
            )
            .await
            .unwrap();
        coordinator.classify_orphan_as_subagent().await.unwrap();

        let second_turn = TurnId::new();
        for event in turn_events(
            thread_id,
            second_turn,
            "ignored",
            "second",
            TokenUsage::new(2, 2),
        ) {
            tx.send(event).unwrap();
        }
        wait_for_turn_count(&store, project_id, thread_id, 2).await;
        assert_eq!(tx.receiver_count(), 1);
        drop(tx);
        forwarder.await.unwrap();

        let turns = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].id, first_turn);
        assert_eq!(
            turns[0].user_input,
            UserInput::text("Unclassified native turn")
        );
        assert_eq!(turns[0].mode, TurnMode::Known(Mode::Build));
        assert_eq!(turns[1].id, second_turn);
        assert_eq!(turns[1].user_input, UserInput::text("Sub-agent turn"));
        assert_eq!(turns[1].mode, TurnMode::Known(Mode::Plan));
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
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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
        let (handle, runtime, _coordinator, authority) = spawn_forwarder_handle_with_runtime(
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
        let tasks = runtime.tasks_snapshot(&authority).1;
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].after_turn);
        assert!(tasks[0].process_id.is_none());

        // A newer turn may begin while a process from the persisted turn is still running. The
        // old process's terminal replacement must update running-task state without being mistaken
        // for an event belonging to the new turn.
        let next_turn = TurnId::new();
        tx.send(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: next_turn,
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

        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(2);
        while !runtime.tasks_snapshot(&authority).1.is_empty() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "old command completion was not applied while the next turn was active"
            );
            tokio::task::yield_now().await;
        }
        assert!(runtime.has_active_turn(&authority));

        tx.send(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: next_turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        })
        .unwrap();
        wait_for_turn_count(&store, project_id, thread_id, 2).await;
        let turns = store.load_all_turns(project_id, thread_id).await.unwrap();
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].id, turn);
        assert_eq!(turns[1].id, next_turn);
        assert!(turns[1].items.is_empty());
        drop(tx);

        tokio::time::timeout(tokio::time::Duration::from_secs(2), handle)
            .await
            .expect("forwarder should exit after the event stream closes")
            .unwrap();

        assert!(runtime.tasks_snapshot(&authority).1.is_empty());
        assert!(!runtime.has_active_turn(&authority));
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
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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
        let (handle, runtime, _coordinator, authority) = spawn_forwarder_handle_with_runtime(
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
            runtime.live_snapshot(&authority).is_none(),
            "synthetic completion should clear live state"
        );

        let tasks = runtime.tasks_snapshot(&authority).1;
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].after_turn);
    }

    #[tokio::test]
    async fn stream_end_before_turn_started_releases_prepared_operation() {
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

        let (tx, _) = broadcast::channel(8);
        let hub = Arc::new(Hub::new());
        let ledger = ledger::spawn(store.clone());
        let (handle, runtime, coordinator, authority) = spawn_forwarder_handle_with_runtime(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store,
            ledger,
            model.clone(),
            "never started",
        );
        drop(tx);

        tokio::time::timeout(tokio::time::Duration::from_secs(2), handle)
            .await
            .expect("forwarder should exit after the event stream closes")
            .unwrap();
        assert!(!runtime.has_active_turn(&authority));

        let replacement_context = TurnContext {
            user_input: UserInput::text("replacement"),
            model: TurnModel::Known(model),
            mode: TurnMode::Known(Mode::Build),
            kind: TurnContextKind::User,
        };
        let replacement = prepare_test_operation(&coordinator, &runtime, replacement_context).await;
        let _ = coordinator.abort_operation(replacement).await;
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
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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
                    model: TurnModel::Known(model.clone()),
                    mode: TurnMode::Known(Mode::Build),
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

        let (runtime, authority) = spawn_forwarder(
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
            runtime.tasks_snapshot(&authority).1.is_empty(),
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
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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
                    model: TurnModel::Known(model.clone()),
                    mode: TurnMode::Known(Mode::Build),
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

        let (runtime, authority) = spawn_forwarder(
            thread_id,
            project_id,
            AgentEventStream::new(tx.subscribe()),
            hub,
            store,
            ledger,
            model,
            "next",
        );

        assert!(runtime.tasks_snapshot(&authority).1.is_empty());
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
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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

        let (runtime, authority) = spawn_forwarder(
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
            runtime.live_snapshot(&authority).is_none(),
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
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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

        let (runtime, authority) = spawn_forwarder(
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
            runtime.live_snapshot(&authority).is_none(),
            "foreign events must not create target-thread live state"
        );
        assert!(
            runtime.tasks_snapshot(&authority).1.is_empty(),
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
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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

        let (runtime, authority) = spawn_forwarder(
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
            runtime.live_snapshot(&authority).is_none(),
            "turnless request alone must not create target-thread live turn state"
        );
    }

    #[tokio::test]
    async fn forwarder_deduplicates_notices_per_turn_and_resets_between_turns() {
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
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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

        let next_turn = TurnId::new();
        tx.send(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: next_turn,
        })
        .unwrap();
        tx.send(AgentEvent::Notice {
            thread: thread_id,
            turn: Some(next_turn),
            message: "Heads up: Long threads and multiple compactions can cause drift.".into(),
        })
        .unwrap();
        tx.send(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: next_turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        })
        .unwrap();

        let mut second_notice_count = 0;
        loop {
            if let ServerMessage::Event { agent_event, .. } =
                tokio::time::timeout(tokio::time::Duration::from_secs(5), client_rx.recv())
                    .await
                    .expect("second turn events should arrive")
                    .expect("subscriber should remain connected")
            {
                match *agent_event {
                    WireAgentEvent::Notice { .. } => second_notice_count += 1,
                    WireAgentEvent::TurnCompleted { turn, .. } if turn == next_turn => break,
                    _ => {}
                }
            }
        }
        assert_eq!(second_notice_count, 1);
    }

    #[tokio::test]
    async fn forwarder_lag_recovers_but_truncates_the_interrupted_native_turn() {
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
                    title: "lag test".into(),
                    harness_thread_id: "th-lag".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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

        let (tx, _) = broadcast::channel(2);
        let stream = AgentEventStream::new(tx.subscribe());
        for sequence in 0..3 {
            tx.send(AgentEvent::Notice {
                thread: thread_id,
                turn: None,
                message: format!("queued notice {sequence}"),
            })
            .unwrap();
        }
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = mpsc::channel(16);
        let _replacements = hub.register_client(1, client_tx).await;
        assert!(hub.subscribe(thread_id, 1).await);
        let ledger = ledger::spawn(store.clone());
        let (forwarder, _runtime, coordinator, _authority) = spawn_forwarder_handle_with_runtime(
            thread_id,
            project_id,
            stream,
            hub,
            store.clone(),
            ledger,
            model,
            "lag test",
        );
        drop(forwarder);

        tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                if matches!(
                    client_rx.recv().await,
                    Some(ServerMessage::Event { agent_event, .. })
                        if matches!(*agent_event, WireAgentEvent::Notice { .. })
                ) {
                    break;
                }
            }
        })
        .await
        .expect("the forwarder should continue after reporting lag");

        let turn = TurnId::new();
        tx.send(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
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

        tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                if store
                    .load_all_turns(project_id, thread_id)
                    .await
                    .unwrap()
                    .iter()
                    .any(|saved| saved.id == turn)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a turn after lag should be persisted normally");

        let lagged_turn = TurnId::new();
        tx.send(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: lagged_turn,
        })
        .unwrap();
        tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                if coordinator.owns_native_turn_for_test(lagged_turn).await {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the native turn should become active before forcing lag");

        for sequence in 0..3 {
            tx.send(AgentEvent::Notice {
                thread: thread_id,
                turn: Some(lagged_turn),
                message: format!("lagging active turn {sequence}"),
            })
            .unwrap();
        }
        tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                if store
                    .load_all_turns(project_id, thread_id)
                    .await
                    .unwrap()
                    .iter()
                    .any(|saved| {
                        saved.id == lagged_turn && saved.status.kind == TurnStatusKind::Interrupted
                    })
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("lag should persist the active native turn as interrupted");

        tx.send(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: lagged_turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        })
        .unwrap();
        tx.send(AgentEvent::Notice {
            thread: thread_id,
            turn: None,
            message: "completion fence after lag".into(),
        })
        .unwrap();
        tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                if matches!(
                    client_rx.recv().await,
                    Some(ServerMessage::Event { agent_event, .. })
                        if matches!(&*agent_event, WireAgentEvent::Notice { message, .. } if message == "completion fence after lag")
                ) {
                    break;
                }
            }
        })
        .await
        .expect("the owner should consume the real completion after lag");
        let following_turn = TurnId::new();
        tx.send(AgentEvent::TurnStarted {
            thread: thread_id,
            turn: following_turn,
        })
        .unwrap();
        tx.send(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn: following_turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        })
        .unwrap();
        tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                let turns = store.load_all_turns(project_id, thread_id).await.unwrap();
                if turns.iter().any(|saved| saved.id == following_turn) {
                    let lagged = turns.iter().find(|saved| saved.id == lagged_turn).unwrap();
                    assert_eq!(lagged.status.kind, TurnStatusKind::Interrupted);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the real completion is ignored and later native turns still proceed");
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
    ) -> (Arc<ThreadRuntimeSupport>, Arc<super::ThreadAuthority>) {
        let (handle, runtime, _coordinator, authority) = spawn_forwarder_handle_with_runtime(
            thread_id, project_id, stream, hub, store, ledger, model, user_input,
        );
        std::mem::drop(handle);
        (runtime, authority)
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
    ) -> (
        JoinHandle<()>,
        Arc<ThreadRuntimeSupport>,
        Arc<super::ThreadCoordinator>,
        Arc<super::ThreadAuthority>,
    ) {
        let ctx = TurnContext {
            user_input: UserInput::text(user_input),
            model: TurnModel::Known(model.clone()),
            mode: TurnMode::Known(Mode::Build),
            kind: TurnContextKind::User,
        };
        let shared = super::RegistryShared::new(hub, store, ledger);
        let shared = Arc::new(shared);
        let runtime = shared.runtime.clone();
        let native_handle = ThreadHandle::detached(thread_id, format!("native-{thread_id}"));
        let coordinator = Arc::new(super::ThreadCoordinator::new(
            super::LoadedThreadBinding {
                project_id,
                handle: native_handle.clone(),
                native_model: Some(model),
            },
            super::ClassificationPhase::Primary,
        ));
        let authority = Arc::new(super::ThreadAuthority::new_for_test(thread_id, project_id));
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let (_completed_tx, completed_rx) = tokio::sync::watch::channel(false);
        let coordinator_for_task = coordinator.clone();
        let task_authority = authority.clone();
        let handle = tokio::spawn(async move {
            coordinator_for_task
                .activate_owner(super::EventOwnerControl {
                    cancel: cancel_tx,
                    completed: completed_rx,
                })
                .await
                .unwrap();
            let lease = shared
                .runtime
                .reserve_turn(
                    &task_authority,
                    turn_reservation(project_id, &native_handle, &ctx),
                )
                .unwrap();
            match coordinator_for_task
                .prepare_operation(ctx.clone(), lease)
                .await
            {
                Ok(_) => {}
                Err((error, _)) => panic!("forwarder test operation was rejected: {error}"),
            }
            forward_events(
                shared,
                task_authority,
                coordinator_for_task,
                stream,
                cancel_rx,
            )
            .await;
        });
        (handle, runtime, coordinator, authority)
    }

    /// Drive an owner over a promptless externally started turn, the way a provider-owned thread
    /// arrives: no prepared operation, so the turn is claimed as `External` and labelled from the
    /// coordinator's classification alone.
    async fn persist_external_turn_input(classification: super::ClassificationPhase) -> UserInput {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let project_id = ProjectId::new();
        let thread_id = ThreadId::new();
        let turn = TurnId::new();
        let ledger = ledger::spawn(store.clone());
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger,
        ));
        let native_handle = ThreadHandle::detached(thread_id, format!("native-{thread_id}"));
        let coordinator = Arc::new(super::ThreadCoordinator::new(
            super::LoadedThreadBinding {
                project_id,
                handle: native_handle,
                native_model: Some(ModelRef {
                    provider: "openai".into(),
                    model: "test".into(),
                    reasoning_effort: None,
                }),
            },
            classification,
        ));
        let authority = Arc::new(super::ThreadAuthority::new_for_test(thread_id, project_id));
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let (_completed_tx, completed_rx) = tokio::sync::watch::channel(false);
        coordinator
            .activate_owner(super::EventOwnerControl {
                cancel: cancel_tx,
                completed: completed_rx,
            })
            .await
            .unwrap();
        let (tx, rx) = broadcast::channel(16);
        let forwarder = tokio::spawn(forward_events(
            shared,
            authority,
            coordinator,
            AgentEventStream::new(rx),
            cancel_rx,
        ));
        for event in turn_events(
            thread_id,
            turn,
            "ignored",
            "external output",
            TokenUsage::new(1, 1),
        ) {
            tx.send(event).unwrap();
        }
        drop(tx);
        forwarder.await.unwrap();

        let turns = store.load_all_turns(project_id, thread_id).await.unwrap();
        turns
            .into_iter()
            .find(|saved| saved.id == turn)
            .expect("the external turn should be persisted")
            .user_input
    }

    #[tokio::test]
    async fn an_unclassified_native_turn_does_not_claim_to_be_a_sub_agent() {
        let subagent = persist_external_turn_input(super::ClassificationPhase::Subagent).await;
        assert_eq!(subagent.as_text().unwrap(), "Sub-agent turn");
        let orphan = persist_external_turn_input(super::ClassificationPhase::Orphan).await;
        assert_eq!(orphan.as_text().unwrap(), "Unclassified native turn");
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
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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
                    model: TurnModel::Known(model.clone()),
                    mode: TurnMode::Known(Mode::Build),
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
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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
                    model: TurnModel::Known(model.clone()),
                    mode: TurnMode::Known(Mode::Build),
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
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Known(model.clone()),
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
