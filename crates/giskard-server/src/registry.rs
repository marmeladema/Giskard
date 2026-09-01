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
    Mode, PermissionPreset, Turn, TurnMode, TurnModel, TurnOverrides, TurnStatus, TurnStatusKind,
};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentHarness, DiscoveryTicket, HarnessBootstrap, HarnessCapabilities, HarnessProvider,
    HarnessThreadDiscoveryStream, KnownThreadBinding, OpenThreadOptions, ThreadAttachment,
    ThreadDeletion, ThreadHandle, ThreadUpdate, thread_update_channel,
};
use giskard_persist::PersistStore;
use giskard_persist::store::{
    ProjectConfig, ThreadFile, ThreadGitWorkspace, ThreadMutation, TurnCommitOutcome,
};
use giskard_proto::{
    GitStrategy, RunningTask, ServerMessage, ThreadRuntimeOverview, WireAgentEvent, WireItem,
};

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

mod event_forwarder;
mod owner;
mod primary;
mod project;
mod thread;

#[cfg(test)]
use event_forwarder::{ForwarderExitReason, ThreadEventForwarder, forwarder_exit_reason_label};
use event_forwarder::{
    event_item_id, event_kind, event_turn_id, log_metadata_only_event_rejection,
};
use owner::OwnerInstallation;
use project::{
    HarnessTransitions, LifecycleLock, ProjectAuthority, ProjectMaterializationPermit,
    WeakLifecycleLock,
};
pub(crate) use thread::ThreadAuthority;
use thread::{
    ClassificationPhase, CoordinatorToken, EventOwnerControl, ExternalTurnDefaults, OwnerLock,
    OwnerRetirement, PreparedTurnReservation, ThreadBinding, ThreadCoordinator, WeakOwnerLock,
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

    /// Close finite-operation admission and retain shutdown ownership until every admitted
    /// operation has committed or rolled back. Unlike background workers, these operations have
    /// bounded provider/persistence awaits of their own; advancing to harness shutdown would tear
    /// resources out from under rollback and is therefore not a valid timeout recovery.
    async fn close_and_wait_owned(&self) {
        self.closed.store(true, Ordering::Release);
        loop {
            let completion = self.completion.notified();
            if self.count.load(Ordering::Acquire) == 0 {
                return;
            }
            completion.await;
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

#[derive(Clone)]
pub(crate) struct NewPrimaryThread {
    pub(crate) title: String,
    pub(crate) mode: TurnMode,
    pub(crate) permission_preset: PermissionPreset,
    pub(crate) context_window: u32,
    pub(crate) git_workspace: Option<ThreadGitWorkspace>,
}

#[cfg(test)]
pub(crate) struct MaterializedPrimaryThread {
    pub(crate) handle: ThreadHandle,
}

pub(crate) struct StartedPrimaryThread {
    pub(crate) handle: ThreadHandle,
    pub(crate) turn_id: TurnId,
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
    primary_operations: Arc<RegistryTaskTracker>,
    hub: Arc<Hub>,
    runtime: Arc<ThreadRuntimeSupport>,
    store: Arc<PersistStore>,
    thread_metadata: Arc<ThreadMetadataService>,
    #[cfg(test)]
    discovery_create_fault: std::sync::Mutex<Option<DiscoveryCreateFault>>,
    #[cfg(test)]
    primary_create_committed_error: AtomicBool,
    #[cfg(test)]
    primary_delete_error: AtomicBool,
    ledger: LedgerHandle,
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum DiscoveryCreateFault {
    CommittedMatching,
    Absent,
    CommittedConflicting,
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
        operation: &PreparedTurnReservation,
    ) {
        if let Some(mut turn_gate) = coordinator.abort_operation(operation.token()).await
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
    ) -> Result<PreparedTurnReservation, HarnessError> {
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
            primary_operations: Arc::new(RegistryTaskTracker::default()),
            hub,
            runtime: Arc::new(ThreadRuntimeSupport::with_max_command_output_bytes(
                max_command_output_bytes,
            )),
            store,
            thread_metadata,
            #[cfg(test)]
            discovery_create_fault: std::sync::Mutex::new(None),
            #[cfg(test)]
            primary_create_committed_error: AtomicBool::new(false),
            #[cfg(test)]
            primary_delete_error: AtomicBool::new(false),
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
    async fn lock_project_lifecycle(&self, project_id: ProjectId) -> ProjectMaterializationPermit {
        lock_project_lifecycle(&self.shared.projects, project_id).await
    }

    pub(crate) async fn lock_project_lifecycle_with_timeout(
        &self,
        project_id: ProjectId,
        wait: Duration,
    ) -> Result<ProjectMaterializationPermit, HarnessError> {
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

        let discovery = match h.take_thread_discovery_stream() {
            Ok(discovery) => discovery,
            Err(error) => {
                let _ = h.shutdown().await;
                return Err(error);
            }
        };
        if let Some(discovery) = discovery {
            let Some(permit) = self.shared.background_tasks.register() else {
                let _ = h.shutdown().await;
                return Err(HarnessError::Protocol(
                    "server is shutting down; refusing native thread discovery".into(),
                ));
            };
            launch_thread_discovery_consumer(
                self.shared.clone(),
                project,
                h.clone(),
                discovery,
                permit,
            );
        }

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
        _workspace_root: &str,
        thread: ThreadId,
        resume: Option<String>,
        initial_model: ModelRef,
    ) -> Result<ThreadHandle, HarnessError> {
        let permit = self.shared.primary_operations.register().ok_or_else(|| {
            HarnessError::Protocol("server is shutting down; refusing Primary open".into())
        })?;
        let registry = self.clone();
        let config = config.clone();
        let (result_tx, result_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let result = async {
                let permit = registry.lock_project_lifecycle(config.id).await;
                let current_config = registry
                    .shared
                    .store
                    .load_project(config.id)
                    .await
                    .map_err(|error| HarnessError::Protocol(error.to_string()))?
                    .ok_or_else(|| {
                        HarnessError::Protocol(format!("project {} disappeared", config.id))
                    })?;
                let durable = registry
                    .shared
                    .store
                    .load_thread(config.id, thread)
                    .await
                    .map_err(|error| HarnessError::Protocol(error.to_string()))?
                    .ok_or(HarnessError::ThreadNotFound(thread))?;
                if durable.kind != ThreadKind::Primary
                    || resume.as_deref() != Some(durable.harness_thread_id.as_str())
                {
                    return Err(HarnessError::Protocol(format!(
                        "cold Primary open for {thread} did not match its durable native identity"
                    )));
                }
                if let Some(coordinator) = registry.shared.coordinator(thread).await {
                    return coordinator
                        .reusable_handle(
                            config.id,
                            thread,
                            Some(&durable.harness_thread_id),
                            ClassificationPhase::Primary,
                        )
                        .await;
                }
                let current_workspace = effective_thread_workspace_root(
                    &registry.shared.store,
                    &current_config,
                    &durable,
                )
                .await
                .map_err(|error| HarnessError::Protocol(error.to_string()))?;
                let app_config = registry
                    .shared
                    .store
                    .load_config()
                    .await
                    .map_err(|error| HarnessError::Protocol(error.to_string()))?;
                let catalog = registry
                    .project_model_catalog(&current_config)
                    .await
                    .unwrap_or_default();
                let mut current_model =
                    crate::models::normalize_model_ref(&app_config, &catalog, &initial_model);
                if !crate::models::resolve_catalog_descriptor(&catalog, &app_config, &current_model)
                    .supports_reasoning_effort
                {
                    current_model.reasoning_effort = None;
                }
                registry
                    .open_primary_thread_locked(
                        &permit,
                        &current_config,
                        &current_workspace,
                        thread,
                        resume,
                        current_model,
                    )
                    .await
            }
            .await;
            let _ = result_tx.send(result);
        });
        result_rx
            .await
            .map_err(|_| HarnessError::Transport("Primary open task dropped its result".into()))?
    }

    #[cfg(test)]
    pub(crate) async fn materialize_primary_thread(
        &self,
        config: &ProjectConfig,
        workspace_root: &str,
        thread: ThreadId,
        initial_model: ModelRef,
        metadata: NewPrimaryThread,
    ) -> Result<MaterializedPrimaryThread, HarnessError> {
        let started = self
            .create_primary_and_start(
                config.clone(),
                workspace_root.to_owned(),
                thread,
                initial_model,
                metadata.clone(),
                GitStrategy::Shared,
                UserInput::text("test Primary materialization"),
                TurnOverrides {
                    model: None,
                    mode: metadata.mode.as_known().unwrap_or(Mode::Build),
                    permission_preset: metadata.permission_preset,
                },
            )
            .await?;
        Ok(MaterializedPrimaryThread {
            handle: started.handle,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn create_primary_and_start(
        &self,
        config: ProjectConfig,
        project_workspace_root: String,
        thread: ThreadId,
        initial_model: ModelRef,
        metadata: NewPrimaryThread,
        git_strategy: GitStrategy,
        input: UserInput,
        overrides: TurnOverrides,
    ) -> Result<StartedPrimaryThread, HarnessError> {
        let creation = primary::Unadmitted::new(primary::Request {
            config,
            project_workspace_root,
            thread,
            initial_model,
            metadata,
            git_strategy,
            input,
            overrides,
            #[cfg(test)]
            phase_gate: None,
        });
        let permit = self.shared.primary_operations.register().ok_or_else(|| {
            HarnessError::Protocol("server is shutting down; refusing Primary creation".into())
        })?;
        let registry = self.clone();
        let (result_tx, result_rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = creation.run(&registry, permit).await;
            let _ = result_tx.send(result);
        });
        result_rx.await.map_err(|_| {
            HarnessError::Transport("Primary creation task dropped its result".into())
        })?
    }

    #[cfg(test)]
    async fn create_primary_with_phase_gate(
        &self,
        config: ProjectConfig,
        thread: ThreadId,
        initial_model: ModelRef,
        git_strategy: GitStrategy,
        gate: Arc<primary::PhaseGate>,
    ) -> Result<StartedPrimaryThread, HarnessError> {
        let creation = primary::Unadmitted::new(primary::Request {
            project_workspace_root: config
                .workspace_root
                .as_deref()
                .unwrap_or(&config.dir)
                .to_owned(),
            config,
            thread,
            initial_model,
            metadata: NewPrimaryThread {
                title: "phase-gated Primary".into(),
                mode: TurnMode::Known(Mode::Build),
                permission_preset: PermissionPreset::AskFirst,
                context_window: 0,
                git_workspace: None,
            },
            git_strategy,
            input: UserInput::text("phase gate"),
            overrides: TurnOverrides {
                model: None,
                mode: Mode::Build,
                permission_preset: PermissionPreset::AskFirst,
            },
            phase_gate: Some(gate),
        });
        let operation = self.shared.primary_operations.register().ok_or_else(|| {
            HarnessError::Protocol("server is shutting down; refusing Primary creation".into())
        })?;
        let registry = self.clone();
        let (result_tx, result_rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = creation.run(&registry, operation).await;
            let _ = result_tx.send(result);
        });
        result_rx.await.map_err(|_| {
            HarnessError::Transport("phase-gated Primary task dropped its result".into())
        })?
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
        let permit = self.lock_project_lifecycle(config.id).await;
        ensure_subagent_thread_open_locked(
            &permit,
            config,
            thread,
            &self.shared,
            SubagentRouteClaim::ExplicitReattach,
        )
        .await?;
        self.loaded_thread_binding(thread.id)
            .await
            .map(|binding| binding.handle)
            .ok_or(HarnessError::ThreadNotFound(thread.id))
    }

    #[allow(clippy::too_many_arguments)]
    async fn open_primary_thread_locked(
        &self,
        lifecycle_permit: &ProjectMaterializationPermit,
        config: &ProjectConfig,
        workspace_root: &str,
        thread: ThreadId,
        resume: Option<String>,
        initial_model: ModelRef,
    ) -> Result<ThreadHandle, HarnessError> {
        debug!(
            project_id = %config.id,
            thread_id = %thread,
            resume = display_opt(resume.as_deref()),
            "opening harness thread"
        );
        // Serialize every project materialization path through coordinator installation. This
        // deliberately spans the native open/resume: traffic discovery can observe the route
        // while `open_thread` is outstanding and must wait for its Primary coordinator instead of
        // creating an orphan. Project/thread deletion accepts this exclusion boundary and may
        // return Unavailable after its five-second lifecycle-lock wait rather than race partially
        // published ownership.
        debug_assert_eq!(lifecycle_permit.project_id(), config.id);
        // Preserve the global lock order: project lifecycle, then thread owner. Locking only when
        // publishing the owner is too late: two callers could both open the native thread and the
        // losing open may invalidate the stream already owned by the winner.
        let owner_guard = lock_thread_owner_after_drain(&self.shared, thread).await;
        if let Some(existing) = self.shared.coordinator(thread).await {
            let handle = existing
                .reusable_handle(
                    config.id,
                    thread,
                    resume.as_deref(),
                    ClassificationPhase::Primary,
                )
                .await?;
            return Ok(handle);
        }
        let harness = self.get_or_create_harness(config.id, config).await?;
        let (updates, update_stream) = thread_update_channel();
        let authority = self
            .shared
            .intern_thread_authority(thread, config.id)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?;
        let restore_permit = self.shared.runtime.restoration_permit(&authority);

        let attachment = harness
            .open_thread(OpenThreadOptions {
                project: config.id,
                thread,
                workspace_root: workspace_root.into(),
                resume,
                initial_model: initial_model.clone(),
                updates,
            })
            .await?;
        let handle = attachment.handle().clone();
        if handle.thread != thread {
            return Err(HarnessError::Protocol(format!(
                "harness opened thread {} instead of requested thread {thread}",
                handle.thread
            )));
        }

        // Bind the model the harness reports as effective when it says so — Codex can ignore
        // resume overrides for a loaded thread, and the binding must reflect reality, not the
        // request (spec: model-provider-switching analysis).
        let native_model = handle
            .resumed_model
            .clone()
            .unwrap_or_else(|| initial_model.clone());
        let owner_installed = install_event_owner_locked(
            &self.shared,
            owner_guard,
            attachment,
            config.id,
            Some(native_model),
            ClassificationPhase::Primary,
        )
        .await?;
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
                .abort_admitted_operation(&coordinator, &operation)
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
                        .acknowledge_operation_turn(&operation, turn_id)
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
                        .abort_admitted_operation(&task_coordinator, &operation)
                        .await;
                    Err(error)
                }
            }
        });
        match task.await {
            Ok(result) => result,
            Err(error) => Err(HarnessError::Protocol(format!(
                "turn start task failed: {error}"
            ))),
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
                .abort_admitted_operation(&coordinator, &operation)
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
                    task_coordinator.retain_accepted_operation(&operation).await;
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
                        .abort_admitted_operation(&task_coordinator, &operation)
                        .await;
                    Err(error)
                }
            }
        });
        match task.await {
            Ok(result) => result,
            Err(error) => Err(HarnessError::Protocol(format!(
                "context compaction task failed: {error}"
            ))),
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

    pub(crate) async fn delete_thread(
        &self,
        permit: &ProjectMaterializationPermit,
        config: &ProjectConfig,
        thread_id: ThreadId,
        harness_thread_id: String,
    ) -> Result<(), HarnessError> {
        if permit.project_id() != config.id {
            return Err(HarnessError::Protocol(format!(
                "project materialization permit belongs to {}, not {}",
                permit.project_id(),
                config.id
            )));
        }
        let harness = self.get_or_create_harness(config.id, config).await?;
        let handle = self
            .loaded_thread_binding(thread_id)
            .await
            .map(|binding| binding.handle)
            .unwrap_or_else(|| ThreadHandle::detached(thread_id, harness_thread_id));
        let retirement = harness.begin_delete_thread(&handle).await?;
        self.retire_thread(thread_id).await;
        let deletion = retirement.finish().await?;
        match deletion {
            ThreadDeletion::Retired => Ok(()),
            ThreadDeletion::RetiredWithProviderError(error) => Err(error),
        }
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
        if let Some(OwnerRetirement::Running(control)) = control.as_ref() {
            let _ = control.cancel.send(true);
        }
        drop(owner_guard);

        match control {
            Some(OwnerRetirement::Running(mut control)) => {
                wait_for_owner_completion(&mut control).await;
            }
            Some(OwnerRetirement::PersistenceBlocked(owner)) => drop(owner),
            None => {}
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
        let mut failures = Vec::new();
        self.shared.primary_operations.close_and_wait_owned().await;
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

        failures.extend(
            results
                .into_iter()
                .filter_map(|(project_id, result)| result.err().map(|error| (project_id, error)))
                .map(|(project_id, error)| format!("{project_id}: {error}")),
        );
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

    pub(crate) async fn delete_project(
        &self,
        permit: &ProjectMaterializationPermit,
        project_id: ProjectId,
    ) -> Result<(), HarnessError> {
        if permit.project_id() != project_id {
            return Err(HarnessError::Protocol(format!(
                "project materialization permit belongs to {}, not {project_id}",
                permit.project_id()
            )));
        }
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
) -> ProjectMaterializationPermit {
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
    ProjectMaterializationPermit::new(project_id, lock.lock_owned().await)
}

async fn remove_primary_worktree(
    worktree: Option<&giskard_persist::store::ThreadWorktree>,
    thread_id: ThreadId,
    failed_action: &str,
) -> Result<(), PrimaryWorktreeCleanupError> {
    let Some(worktree) = worktree else {
        return Ok(());
    };
    if let Err(error) = crate::worktree::remove(worktree, true).await {
        warn!(
            %thread_id,
            branch = %worktree.branch,
            path = %worktree.path,
            %failed_action,
            %error,
            "could not remove worktree during Primary rollback"
        );
        return Err(PrimaryWorktreeCleanupError {
            stage: "remove checkout",
            path: worktree.path.clone(),
            branch: worktree.branch.clone(),
            source: error.to_string(),
        });
    }
    if let Err(error) = crate::worktree::delete_branch(worktree).await {
        warn!(
            %thread_id,
            branch = %worktree.branch,
            %failed_action,
            %error,
            "could not remove branch during Primary rollback"
        );
        return Err(PrimaryWorktreeCleanupError {
            stage: "delete branch",
            path: worktree.path.clone(),
            branch: worktree.branch.clone(),
            source: error.to_string(),
        });
    }
    Ok(())
}

#[derive(Debug)]
struct PrimaryWorktreeCleanupError {
    stage: &'static str,
    path: String,
    branch: String,
    source: String,
}

impl fmt::Display for PrimaryWorktreeCleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} failed for checkout {} and branch {}: {}",
            self.stage, self.path, self.branch, self.source
        )
    }
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
    let lifecycle_permit = lock_project_lifecycle(&shared.projects, project_id).await;
    debug_assert_eq!(lifecycle_permit.project_id(), project_id);
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
        let opened_agent_name = ensure_subagent_thread_open_locked(
            &lifecycle_permit,
            &project_config,
            &existing,
            &shared,
            SubagentRouteClaim::ParentDiscovery,
        )
        .await?;
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
    let attachment = harness
        .claim_native_thread(
            child_thread_id,
            info.native_thread_id.clone(),
            workspace_root.into(),
        )
        .await?;
    let handle = attachment.handle().clone();
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
    let installation = install_event_owner(
        &shared,
        attachment,
        project_id,
        native_model,
        ClassificationPhase::Subagent,
    )
    .await;
    // Metadata is already durable even if owner installation failed. Publish exactly once from
    // the creating attempt so a later existing-Subagent retry cannot leave the catalog unaware of
    // the child or emit a duplicate creation notification.
    shared
        .thread_metadata
        .publish_created(project_id, &thread_file)
        .await;
    installation?;
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

#[derive(Clone, Copy)]
enum SubagentRouteClaim {
    ParentDiscovery,
    ExplicitReattach,
}

async fn ensure_subagent_thread_open_locked(
    permit: &ProjectMaterializationPermit,
    project_config: &ProjectConfig,
    thread_file: &ThreadFile,
    shared: &Arc<RegistryShared>,
    route_claim: SubagentRouteClaim,
) -> Result<Option<String>, HarnessError> {
    if permit.project_id() != project_config.id {
        return Err(HarnessError::Protocol(format!(
            "project materialization permit belongs to {}, not {}",
            permit.project_id(),
            project_config.id
        )));
    }
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
    let owner_guard = lock_thread_owner_after_drain(shared, thread_file.id).await;
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
    let attachment = match route_claim {
        SubagentRouteClaim::ParentDiscovery => {
            harness
                .claim_native_thread(
                    thread_file.id,
                    thread_file.harness_thread_id.clone(),
                    workspace_root.into(),
                )
                .await?
        }
        SubagentRouteClaim::ExplicitReattach => {
            harness
                .reattach_native_thread(
                    thread_file.id,
                    thread_file.harness_thread_id.clone(),
                    workspace_root.into(),
                )
                .await?
        }
    };
    let handle = attachment.handle().clone();
    if handle.thread != thread_file.id {
        return Err(HarnessError::Protocol(format!(
            "linked-thread claim returned thread {} instead of {}",
            handle.thread, thread_file.id
        )));
    }
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
        owner_guard,
        attachment,
        project_config.id,
        native_model,
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

#[cfg(test)]
fn launch_event_forwarder(
    shared: Arc<RegistryShared>,
    authority: Arc<ThreadAuthority>,
    coordinator: Arc<ThreadCoordinator>,
    owner: giskard_harness::ThreadEventOwner,
    cancel: watch::Receiver<bool>,
    completed: watch::Sender<bool>,
    permit: RegistryTaskPermit,
) {
    tokio::spawn(async move {
        let _permit = permit;
        let cancellation_probe = cancel.clone();
        let forwarder = ThreadEventForwarder::new(
            shared.clone(),
            authority.clone(),
            coordinator.clone(),
            owner,
            cancel,
        )
        .await;
        let thread_id = forwarder.thread_id();
        let (exit_reason, owner) = forwarder.run().await;
        let cancelled = *cancellation_probe.borrow();
        if exit_reason == ForwarderExitReason::PersistenceBlocked {
            if let Err((error, owner)) = coordinator.retain_persistence_blocked_owner(owner).await {
                warn!(
                    %thread_id,
                    %error,
                    "could not retain persistence-blocked native event owner"
                );
                drop(owner);
            }
        } else if !cancelled {
            drop(owner);
            coordinator.owner_finished(false).await;
            if authority.clear_coordinator_if(&coordinator).await {
                warn!(
                    %thread_id,
                    exit_reason = forwarder_exit_reason_label(exit_reason),
                    "removed failed event owner so the thread can be reopened"
                );
            }
        } else {
            drop(owner);
            coordinator.owner_finished(cancelled).await;
        }
        let _ = completed.send(true);
    });
}

fn launch_thread_discovery_consumer(
    shared: Arc<RegistryShared>,
    project_id: ProjectId,
    harness: Arc<dyn AgentHarness>,
    mut discoveries: HarnessThreadDiscoveryStream,
    permit: RegistryTaskPermit,
) {
    tokio::spawn(async move {
        let _permit = permit;
        while let Some(discovery) = discoveries.recv().await {
            let thread_id = discovery.thread_id();
            let harness_thread_id = discovery.harness_thread_id().to_owned();
            let result = ensure_discovered_thread_owner(
                project_id,
                discovery,
                harness.clone(),
                shared.clone(),
            )
            .await;
            if let Err(error) = &result {
                warn!(
                    %project_id,
                    %thread_id,
                    %harness_thread_id,
                    action = "materialize_discovered_thread",
                    %error,
                    "failed to establish an owner for a discovered native thread"
                );
            }
        }
        if shared.active_harness(project_id).await.is_some() {
            error!(
                %project_id,
                action = "consume_native_thread_discovery",
                "native thread discovery stream closed while the harness remained active"
            );
            let retired = if let Some(authority) = shared.project_authority(project_id).await {
                let mut transitions = shared.harness_transitions.lock().await;
                let mut slot = transitions.project(&authority).await;
                if slot
                    .active()
                    .is_some_and(|active| Arc::ptr_eq(&active, &harness))
                {
                    let retired = slot.begin_delete().ok().flatten();
                    if let Some(retired) = retired.as_ref() {
                        slot.finish_delete(retired);
                    }
                    retired
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(retired) = retired
                && let Err(error) = retired.shutdown().await
            {
                warn!(
                    %project_id,
                    %error,
                    "failed to shut down harness after discovery consumer closure"
                );
            }
        }
    });
}

async fn ensure_discovered_thread_owner(
    project_id: ProjectId,
    discovery: DiscoveryTicket,
    harness: Arc<dyn AgentHarness>,
    shared: Arc<RegistryShared>,
) -> Result<ThreadBinding, HarnessError> {
    let discovered_thread = discovery.thread_id();
    let discovered_native = discovery.harness_thread_id().to_owned();
    let lifecycle_permit = lock_project_lifecycle(&shared.projects, project_id).await;
    debug_assert_eq!(lifecycle_permit.project_id(), project_id);

    let project = shared
        .store
        .load_project(project_id)
        .await
        .map_err(|error| HarnessError::Protocol(error.to_string()))?
        .ok_or_else(|| HarnessError::Protocol(format!("project {project_id} disappeared")))?;
    let graph = load_thread_graph(&shared.store, project_id)
        .await
        .map_err(|error| HarnessError::Protocol(error.to_string()))?;
    if let Some(conflict) = graph.values().find(|thread| {
        thread.harness_thread_id == discovered_native && thread.id != discovered_thread
    }) {
        return Err(HarnessError::Protocol(format!(
            "native thread {} is durably owned by thread {} instead of discovered thread {}",
            discovered_native, conflict.id, discovered_thread
        )));
    }

    let existing = graph.get(&discovered_thread).cloned();
    if let Some(thread) = existing.as_ref()
        && thread.harness_thread_id != discovered_native
    {
        return Err(HarnessError::Protocol(format!(
            "discovered thread {} is durably bound to native thread {} instead of {}",
            discovered_thread, thread.harness_thread_id, discovered_native
        )));
    }

    if let Some(coordinator) = shared.coordinator(discovered_thread).await {
        let thread = existing.as_ref().ok_or_else(|| {
            HarnessError::Protocol(format!(
                "discovered thread {} has a coordinator before durable metadata",
                discovered_thread
            ))
        })?;
        coordinator
            .reusable_handle(
                project_id,
                discovered_thread,
                Some(&discovered_native),
                ClassificationPhase::from(thread.kind),
            )
            .await?;
        return Ok(coordinator);
    }

    let workspace_root = match existing.as_ref() {
        Some(thread) => effective_thread_workspace_root(&shared.store, &project, thread)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?,
        None => project
            .workspace_root
            .as_deref()
            .unwrap_or(&project.dir)
            .to_owned(),
    };
    let attachment = harness
        .claim_discovered_thread(discovery, workspace_root.into())
        .await?;
    let handle = attachment.handle().clone();
    if handle.thread != discovered_thread || handle.harness_thread_id != discovered_native {
        return Err(HarnessError::Protocol(format!(
            "discovered route claim returned thread {} / native {} instead of {} / {}",
            handle.thread, handle.harness_thread_id, discovered_thread, discovered_native
        )));
    }

    let (thread, created) = match existing {
        Some(thread) => (thread, false),
        None => {
            let now = Utc::now();
            let candidate = ThreadFile {
                revision: 0,
                version: giskard_persist::store::THREAD_METADATA_VERSION,
                id: discovered_thread,
                project_id,
                title: "Unclassified native thread".into(),
                harness_thread_id: discovered_native,
                parent_thread_id: None,
                spawned_by_turn_id: None,
                kind: ThreadKind::Orphan,
                mode: TurnMode::Unknown,
                current_model: TurnModel::Unknown,
                context_window: 0,
                model_context_windows: HashMap::new(),
                permission_preset: PermissionPreset::AskFirst,
                model_efforts: HashMap::new(),
                tokens: giskard_core::token::TokenLedger::default(),
                created_at: now,
                updated_at: now,
                archived: false,
                git_workspace: None,
            };
            let thread = match create_discovery_metadata(&shared, project_id, candidate.clone())
                .await
            {
                Ok(thread) => thread,
                Err(create_error) => {
                    // An atomic rename may have committed even when its caller observed an error.
                    // Reload before deciding that the attachment should be restored and retried.
                    match shared
                        .store
                        .load_thread(project_id, discovered_thread)
                        .await
                    {
                        Ok(Some(thread))
                            if thread.project_id == candidate.project_id
                                && thread.id == candidate.id
                                && thread.harness_thread_id == candidate.harness_thread_id
                                && thread.kind == candidate.kind =>
                        {
                            thread
                        }
                        Ok(Some(thread)) => {
                            return Err(HarnessError::Protocol(format!(
                                "discovery metadata create for {discovered_thread} failed ({create_error}); reload found conflicting native {} / kind {:?}",
                                thread.harness_thread_id, thread.kind
                            )));
                        }
                        Ok(None) => {
                            return Err(HarnessError::Protocol(create_error.to_string()));
                        }
                        Err(reload_error) => {
                            return Err(HarnessError::Protocol(format!(
                                "discovery metadata create for {discovered_thread} failed ({create_error}); reload also failed: {reload_error}"
                            )));
                        }
                    }
                }
            };
            (thread, true)
        }
    };
    let native_model = handle
        .resumed_model
        .clone()
        .or_else(|| thread.current_model.as_known().cloned());
    let installation = install_event_owner(
        &shared,
        attachment,
        project_id,
        native_model,
        ClassificationPhase::from(thread.kind),
    )
    .await;
    if created {
        shared
            .thread_metadata
            .publish_created(project_id, &thread)
            .await;
    }
    installation?;
    let coordinator = shared.coordinator(discovered_thread).await.ok_or_else(|| {
        HarnessError::Protocol(format!(
            "discovered thread {} owner disappeared during installation",
            discovered_thread
        ))
    })?;
    Ok(coordinator)
}

async fn create_discovery_metadata(
    shared: &RegistryShared,
    project_id: ProjectId,
    candidate: ThreadFile,
) -> Result<ThreadFile, giskard_core::PersistError> {
    #[cfg(test)]
    let discovery_create_fault = { shared.discovery_create_fault.lock().unwrap().take() };
    #[cfg(test)]
    if let Some(fault) = discovery_create_fault {
        let injected =
            giskard_core::PersistError::Io("injected uncertain discovery metadata create".into());
        match fault {
            DiscoveryCreateFault::CommittedMatching => {
                shared.thread_metadata.create(project_id, candidate).await?;
                return Err(injected);
            }
            DiscoveryCreateFault::Absent => return Err(injected),
            DiscoveryCreateFault::CommittedConflicting => {
                let mut conflicting = candidate;
                conflicting.harness_thread_id = "conflicting-native".into();
                shared.store.save_thread(project_id, &conflicting).await?;
                return Err(injected);
            }
        }
    }
    shared.thread_metadata.create(project_id, candidate).await
}

async fn install_event_owner(
    shared: &Arc<RegistryShared>,
    attachment: ThreadAttachment,
    project_id: ProjectId,
    native_model: Option<ModelRef>,
    classification: ClassificationPhase,
) -> Result<bool, HarnessError> {
    let thread_id = attachment.handle().thread;
    let owner_guard = lock_thread_owner_after_drain(shared, thread_id).await;
    install_event_owner_locked(
        shared,
        owner_guard,
        attachment,
        project_id,
        native_model,
        classification,
    )
    .await
}

async fn install_event_owner_locked(
    shared: &Arc<RegistryShared>,
    owner_guard: OwnedMutexGuard<()>,
    attachment: ThreadAttachment,
    project_id: ProjectId,
    native_model: Option<ModelRef>,
    classification: ClassificationPhase,
) -> Result<bool, HarnessError> {
    OwnerInstallation::prepare(
        shared,
        owner_guard,
        attachment,
        project_id,
        native_model,
        classification,
    )
    .await?
    .commit()?;
    Ok(true)
}

/// Harness-neutral native thread identity used only for bootstrap uniqueness checks.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct HarnessThreadId(String);

impl HarnessThreadId {
    fn new(value: String) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
    use std::sync::{Arc, Weak};

    use chrono::Utc;
    use giskard_core::approval::ApprovalDecision;
    use giskard_core::error::HarnessError;
    use giskard_core::event::AgentEvent;
    use giskard_core::ids::{ApprovalId, ItemId, ProjectId, ServerRequestId, ThreadId, TurnId};
    use giskard_core::model::{ModelDescriptor, ModelRef};
    use giskard_core::thread::ThreadKind;
    use giskard_core::token::TokenLedger;
    use giskard_core::turn::{Mode, PermissionPreset, TurnMode, TurnModel};
    use giskard_core::user_input::UserInput;
    use giskard_harness::{
        AgentEventStream, AgentHarness, DiscoveryTicket, HarnessCapabilities,
        HarnessThreadDiscoveryStream, OpenThreadOptions, ThreadAttachment, ThreadHandle,
    };
    use giskard_persist::PersistStore;
    use giskard_persist::store::{ProjectConfig, ThreadFile};
    use tokio::sync::{Notify, broadcast};

    use super::{TurnContext, TurnContextKind, turn_reservation};
    use crate::hub::Hub;
    use crate::ledger;
    use crate::thread_runtime::ThreadRuntimeSupport;

    struct UnusedHarnessFactory;

    struct ShutdownHarness {
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    #[derive(Clone)]
    struct TestRoute {
        receiver: Arc<StdMutex<Option<AgentEventStream>>>,
    }

    impl TestRoute {
        fn new(receiver: broadcast::Receiver<AgentEvent>) -> Self {
            Self {
                receiver: Arc::new(StdMutex::new(Some(AgentEventStream::new(receiver)))),
            }
        }

        fn attachment(&self, handle: ThreadHandle) -> Result<ThreadAttachment, HarnessError> {
            let stream = self.receiver.lock().unwrap().take().ok_or_else(|| {
                HarnessError::Protocol("test route receiver is already attached".into())
            })?;
            let attachment_return = self.receiver.clone();
            let owner_return = self.receiver.clone();
            Ok(ThreadAttachment::from_route(
                handle,
                stream,
                move || {
                    Ok(move |stream: AgentEventStream| {
                        *owner_return.lock().unwrap() = Some(stream);
                    })
                },
                move |stream| {
                    *attachment_return.lock().unwrap() = Some(stream);
                },
            ))
        }

        fn ticket(&self, handle: ThreadHandle) -> DiscoveryTicket {
            let thread_id = handle.thread;
            let native_id = handle.harness_thread_id.clone();
            let route = self.clone();
            DiscoveryTicket::from_route(
                thread_id,
                native_id,
                move |_| route.attachment(handle),
                || {},
            )
        }
    }

    struct DiscoveryOwnerHarness {
        route: TestRoute,
        subscriptions: AtomicUsize,
        routes: StdMutex<HashMap<String, ThreadId>>,
        discovery_tx: StdMutex<Option<tokio::sync::mpsc::Sender<DiscoveryTicket>>>,
        discovery_rx: StdMutex<Option<tokio::sync::mpsc::Receiver<DiscoveryTicket>>>,
    }

    #[async_trait::async_trait]
    impl AgentHarness for DiscoveryOwnerHarness {
        async fn begin_delete_thread<'a>(
            &'a self,
            thread: &'a ThreadHandle,
        ) -> Result<giskard_harness::ThreadRetirement<'a>, HarnessError> {
            giskard_harness::unsupported_thread_retirement(thread)
        }
        fn capabilities(&self) -> HarnessCapabilities {
            HarnessCapabilities::default()
        }

        fn take_thread_discovery_stream(
            &self,
        ) -> Result<Option<HarnessThreadDiscoveryStream>, HarnessError> {
            Ok(self
                .discovery_rx
                .lock()
                .unwrap()
                .take()
                .map(HarnessThreadDiscoveryStream::new))
        }

        async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError> {
            Ok(Vec::new())
        }

        async fn open_thread(
            &self,
            _opts: OpenThreadOptions,
        ) -> Result<ThreadAttachment, HarnessError> {
            Err(HarnessError::Unsupported("unused".into()))
        }

        async fn claim_native_thread(
            &self,
            thread: ThreadId,
            harness_thread_id: String,
            workspace_root: std::path::PathBuf,
        ) -> Result<ThreadAttachment, HarnessError> {
            if self.routes.lock().unwrap().contains_key("__tombstoned__") {
                return Err(HarnessError::Protocol(
                    "parent discovery cannot reactivate tombstoned route".into(),
                ));
            }
            if self.routes.lock().unwrap().contains_key("__busy__") {
                return Err(HarnessError::Protocol(
                    "active route has no compatible coordinator".into(),
                ));
            }
            let thread = *self
                .routes
                .lock()
                .unwrap()
                .entry(harness_thread_id.clone())
                .or_insert(thread);
            self.subscriptions.fetch_add(1, Ordering::SeqCst);
            self.route.attachment(ThreadHandle::opened(
                thread,
                harness_thread_id,
                workspace_root,
            ))
        }

        async fn reattach_native_thread(
            &self,
            thread: ThreadId,
            harness_thread_id: String,
            workspace_root: std::path::PathBuf,
        ) -> Result<ThreadAttachment, HarnessError> {
            let thread = {
                let mut routes = self.routes.lock().unwrap();
                let authoritative = routes.get(&harness_thread_id).copied().unwrap_or(thread);
                routes.insert("__reattached__".into(), authoritative);
                authoritative
            };
            self.subscriptions.fetch_add(1, Ordering::SeqCst);
            self.route.attachment(ThreadHandle::opened(
                thread,
                harness_thread_id,
                workspace_root,
            ))
        }

        async fn claim_discovered_thread(
            &self,
            ticket: DiscoveryTicket,
            workspace_root: std::path::PathBuf,
        ) -> Result<ThreadAttachment, HarnessError> {
            self.subscriptions.fetch_add(1, Ordering::SeqCst);
            ticket.claim(workspace_root)
        }

        async fn start_turn(
            &self,
            _thread: &ThreadHandle,
            _input: UserInput,
            _overrides: giskard_core::turn::TurnOverrides,
        ) -> Result<TurnId, HarnessError> {
            Err(HarnessError::Unsupported("unused".into()))
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
            self.discovery_tx.lock().unwrap().take();
            Ok(())
        }
    }

    struct DiscoveryOwnerFactory {
        harness: Arc<DiscoveryOwnerHarness>,
    }

    fn discovery_ticket(
        harness: &DiscoveryOwnerHarness,
        thread_id: ThreadId,
        native_id: &str,
    ) -> DiscoveryTicket {
        harness.route.ticket(ThreadHandle::opened(
            thread_id,
            native_id.into(),
            "/tmp".into(),
        ))
    }

    #[async_trait::async_trait]
    impl super::HarnessFactory for DiscoveryOwnerFactory {
        async fn create(
            &self,
            _config: &ProjectConfig,
            _bootstrap: giskard_harness::HarnessBootstrap,
        ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
            Ok(self.harness.clone())
        }
    }

    struct BlockingPrimaryHarness {
        open_started: Notify,
        open_release: Notify,
        turn_started: Notify,
        turn_release: Notify,
        turn_completed: Notify,
        start_success: AtomicBool,
        shutdown_started: AtomicBool,
        delete_mode: AtomicU8,
        sabotage_worktree_removal: AtomicBool,
        cleanup_started: Notify,
        cleanup_release: Notify,
        block_cleanup: AtomicBool,
        sender: StdMutex<Option<broadcast::Sender<AgentEvent>>>,
        route: TestRoute,
        claims: AtomicUsize,
        subscriptions: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl AgentHarness for BlockingPrimaryHarness {
        fn capabilities(&self) -> HarnessCapabilities {
            HarnessCapabilities::default()
        }

        async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError> {
            Ok(Vec::new())
        }

        async fn open_thread(
            &self,
            opts: OpenThreadOptions,
        ) -> Result<ThreadAttachment, HarnessError> {
            self.open_started.notify_one();
            self.open_release.notified().await;
            self.subscriptions.fetch_add(1, Ordering::SeqCst);
            self.route.attachment(ThreadHandle::opened(
                opts.thread,
                "native-primary".into(),
                opts.workspace_root,
            ))
        }

        async fn claim_native_thread(
            &self,
            _thread: ThreadId,
            _harness_thread_id: String,
            _workspace_root: std::path::PathBuf,
        ) -> Result<ThreadAttachment, HarnessError> {
            self.claims.fetch_add(1, Ordering::SeqCst);
            Err(HarnessError::Protocol(
                "discovery must reuse the Primary coordinator".into(),
            ))
        }

        async fn start_turn(
            &self,
            thread: &ThreadHandle,
            _input: UserInput,
            _overrides: giskard_core::turn::TurnOverrides,
        ) -> Result<TurnId, HarnessError> {
            self.turn_started.notify_one();
            self.turn_release.notified().await;
            self.turn_completed.notify_one();
            #[cfg(unix)]
            if self.sabotage_worktree_removal.load(Ordering::SeqCst)
                && let Some(parent) = std::path::Path::new(&thread.workspace_root).parent()
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o555))
                    .map_err(|error| HarnessError::Transport(error.to_string()))?;
            }
            if self.start_success.load(Ordering::SeqCst) {
                Ok(TurnId::new())
            } else {
                Err(HarnessError::Unsupported("unused".into()))
            }
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

        async fn begin_delete_thread<'a>(
            &'a self,
            _thread: &'a ThreadHandle,
        ) -> Result<giskard_harness::ThreadRetirement<'a>, HarnessError> {
            let mode = self.delete_mode.load(Ordering::SeqCst);
            if mode == 2 {
                return Err(HarnessError::Protocol(
                    "injected pre-invalidation rejection".into(),
                ));
            }
            Ok(giskard_harness::ThreadRetirement::new(Box::pin(
                async move {
                    self.cleanup_started.notify_one();
                    if self.block_cleanup.load(Ordering::SeqCst) {
                        self.cleanup_release.notified().await;
                    }
                    if mode == 3 {
                        Err(HarnessError::Transport(
                            "injected provider cleanup future failure".into(),
                        ))
                    } else if mode == 1 {
                        Ok(giskard_harness::ThreadDeletion::RetiredWithProviderError(
                            HarnessError::Transport("injected provider cleanup failure".into()),
                        ))
                    } else {
                        Ok(giskard_harness::ThreadDeletion::Retired)
                    }
                },
            )))
        }

        async fn shutdown(&self) -> Result<(), HarnessError> {
            self.shutdown_started.store(true, Ordering::SeqCst);
            self.sender.lock().unwrap().take();
            Ok(())
        }
    }

    struct BlockingPrimaryFactory {
        harness: Arc<BlockingPrimaryHarness>,
    }

    #[async_trait::async_trait]
    impl super::HarnessFactory for BlockingPrimaryFactory {
        async fn create(
            &self,
            _config: &ProjectConfig,
            _bootstrap: giskard_harness::HarnessBootstrap,
        ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
            Ok(self.harness.clone())
        }
    }

    #[async_trait::async_trait]
    impl AgentHarness for ShutdownHarness {
        async fn begin_delete_thread<'a>(
            &'a self,
            thread: &'a ThreadHandle,
        ) -> Result<giskard_harness::ThreadRetirement<'a>, HarnessError> {
            giskard_harness::unsupported_thread_retirement(thread)
        }
        fn capabilities(&self) -> HarnessCapabilities {
            HarnessCapabilities::default()
        }

        async fn list_models(&self) -> Result<Vec<giskard_core::ModelDescriptor>, HarnessError> {
            Ok(Vec::new())
        }

        async fn open_thread(
            &self,
            _opts: OpenThreadOptions,
        ) -> Result<ThreadAttachment, HarnessError> {
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
        async fn begin_delete_thread<'a>(
            &'a self,
            thread: &'a ThreadHandle,
        ) -> Result<giskard_harness::ThreadRetirement<'a>, HarnessError> {
            giskard_harness::unsupported_thread_retirement(thread)
        }
        fn capabilities(&self) -> HarnessCapabilities {
            HarnessCapabilities::default()
        }

        async fn list_models(&self) -> Result<Vec<giskard_core::ModelDescriptor>, HarnessError> {
            Ok(Vec::new())
        }

        async fn open_thread(
            &self,
            opts: giskard_harness::OpenThreadOptions,
        ) -> Result<ThreadAttachment, HarnessError> {
            if self.bound.load(Ordering::SeqCst) == 0 {
                self.opened_before_bound.fetch_add(1, Ordering::SeqCst);
            }
            let shared = self.authority_probe.lock().unwrap().clone();
            if let Some(shared) = shared.and_then(|weak| weak.upgrade())
                && let Some(authority) = shared.thread_authority(opts.thread).await
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

    fn durable_thread(
        project_id: ProjectId,
        thread_id: ThreadId,
        native_id: &str,
        kind: giskard_core::thread::ThreadKind,
    ) -> ThreadFile {
        let now = Utc::now();
        ThreadFile {
            revision: 0,
            version: giskard_persist::store::THREAD_METADATA_VERSION,
            id: thread_id,
            project_id,
            title: "durable".into(),
            harness_thread_id: native_id.into(),
            parent_thread_id: None,
            spawned_by_turn_id: None,
            kind,
            mode: TurnMode::Unknown,
            current_model: TurnModel::Unknown,
            context_window: 0,
            model_context_windows: HashMap::new(),
            permission_preset: PermissionPreset::AskFirst,
            model_efforts: HashMap::new(),
            tokens: TokenLedger::default(),
            created_at: now,
            updated_at: now,
            archived: false,
            git_workspace: None,
        }
    }

    #[tokio::test]
    async fn test_route_attachment_drop_restores_the_exact_buffered_receiver() {
        let thread_id = ThreadId::new();
        let handle = ThreadHandle::opened(thread_id, "native-linear".into(), "/tmp".into());
        let (sender, receiver) = broadcast::channel(4);
        let route = TestRoute::new(receiver);
        let attachment = route.attachment(handle.clone()).unwrap();
        let turn = TurnId::new();
        sender
            .send(AgentEvent::TurnStarted {
                thread: thread_id,
                turn,
            })
            .unwrap();

        drop(attachment);
        let mut owner = route.attachment(handle).unwrap().commit().unwrap();
        assert!(matches!(
            owner.recv().await.unwrap(),
            AgentEvent::TurnStarted { thread, turn: got }
                if thread == thread_id && got == turn
        ));
    }

    #[test]
    fn dropped_test_discovery_ticket_leaves_route_claimable() {
        let thread_id = ThreadId::new();
        let handle = ThreadHandle::opened(thread_id, "native-linear".into(), "/tmp".into());
        let (_sender, receiver) = broadcast::channel(1);
        let route = TestRoute::new(receiver);

        drop(route.ticket(handle.clone()));
        let attachment = route
            .ticket(handle)
            .claim(std::path::PathBuf::from("/tmp"))
            .unwrap();
        assert_eq!(attachment.handle().thread, thread_id);
    }

    #[tokio::test]
    async fn discovery_creates_one_orphan_publishes_catalog_and_installs_owner() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, _) = create_test_project(&store, "discovery").await;
        let hub = Arc::new(Hub::new());
        let (client_tx, mut client_rx) = tokio::sync::mpsc::channel(8);
        let replacements = hub.register_client(hub.next_client_id(), client_tx).await;
        let shared = Arc::new(super::RegistryShared::new(
            hub,
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let (sender, receiver) = broadcast::channel(4);
        let harness = Arc::new(DiscoveryOwnerHarness {
            route: TestRoute::new(receiver),
            subscriptions: AtomicUsize::new(0),
            routes: StdMutex::new(HashMap::new()),
            discovery_tx: StdMutex::new(None),
            discovery_rx: StdMutex::new(None),
        });
        let thread_id = ThreadId::new();
        let turn_id = TurnId::new();
        sender
            .send(AgentEvent::TurnStarted {
                thread: thread_id,
                turn: turn_id,
            })
            .unwrap();
        sender
            .send(AgentEvent::TurnCompleted {
                thread: thread_id,
                turn: turn_id,
                usage: giskard_core::token::TokenUsage::default(),
                status: giskard_core::turn::TurnStatus {
                    kind: giskard_core::turn::TurnStatusKind::Completed,
                    message: None,
                },
            })
            .unwrap();

        super::ensure_discovered_thread_owner(
            project_id,
            discovery_ticket(&harness, thread_id, "native-discovered"),
            harness.clone(),
            shared.clone(),
        )
        .await
        .unwrap();

        let thread = store
            .load_thread(project_id, thread_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(thread.kind, giskard_core::thread::ThreadKind::Orphan);
        assert_eq!(thread.title, "Unclassified native thread");
        assert_eq!(thread.mode, TurnMode::Unknown);
        assert_eq!(thread.current_model, TurnModel::Unknown);
        assert_eq!(thread.permission_preset, PermissionPreset::AskFirst);
        assert!(thread.parent_thread_id.is_none());
        assert!(thread.git_workspace.is_none());
        assert_eq!(harness.subscriptions.load(Ordering::SeqCst), 1);
        assert!(shared.coordinator(thread_id).await.is_some());
        while store
            .load_turn_records(project_id, thread_id)
            .await
            .unwrap()
            .is_empty()
        {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            replacements.recv().await,
            giskard_proto::ServerMessage::ThreadCatalogChanged
        ));
        assert!(
            client_rx.try_recv().is_err(),
            "catalog invalidation must use the replacement lane"
        );

        drop(sender);
        while shared.coordinator(thread_id).await.is_some() {
            tokio::task::yield_now().await;
        }
    }

    async fn uncertain_discovery_fixture(
        name: &str,
    ) -> (
        tempfile::TempDir,
        Arc<PersistStore>,
        ProjectId,
        Arc<super::RegistryShared>,
        Arc<DiscoveryOwnerHarness>,
        broadcast::Sender<AgentEvent>,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, _) = create_test_project(&store, name).await;
        let (sender, receiver) = broadcast::channel(4);
        let harness = Arc::new(DiscoveryOwnerHarness {
            route: TestRoute::new(receiver),
            subscriptions: AtomicUsize::new(0),
            routes: StdMutex::new(HashMap::new()),
            discovery_tx: StdMutex::new(None),
            discovery_rx: StdMutex::new(None),
        });
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        (tmp, store, project_id, shared, harness, sender)
    }

    #[tokio::test]
    async fn uncertain_discovery_create_with_matching_reload_continues_installation() {
        let (_tmp, store, project_id, shared, harness, _sender) =
            uncertain_discovery_fixture("uncertain-matching").await;
        let thread_id = ThreadId::new();
        *shared.discovery_create_fault.lock().unwrap() =
            Some(super::DiscoveryCreateFault::CommittedMatching);

        super::ensure_discovered_thread_owner(
            project_id,
            discovery_ticket(&harness, thread_id, "native-matching"),
            harness,
            shared.clone(),
        )
        .await
        .unwrap();

        assert_eq!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .unwrap()
                .harness_thread_id,
            "native-matching"
        );
        assert!(shared.coordinator(thread_id).await.is_some());
    }

    #[tokio::test]
    async fn uncertain_discovery_create_with_absent_reload_restores_and_retries() {
        let (_tmp, store, project_id, shared, harness, _sender) =
            uncertain_discovery_fixture("uncertain-absent").await;
        let thread_id = ThreadId::new();
        *shared.discovery_create_fault.lock().unwrap() = Some(super::DiscoveryCreateFault::Absent);

        assert!(
            super::ensure_discovered_thread_owner(
                project_id,
                discovery_ticket(&harness, thread_id, "native-absent"),
                harness.clone(),
                shared.clone(),
            )
            .await
            .is_err()
        );
        assert!(harness.route.receiver.lock().unwrap().is_some());
        assert!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .is_none()
        );

        super::ensure_discovered_thread_owner(
            project_id,
            discovery_ticket(&harness, thread_id, "native-absent"),
            harness,
            shared.clone(),
        )
        .await
        .unwrap();
        assert!(shared.coordinator(thread_id).await.is_some());
    }

    #[tokio::test]
    async fn uncertain_discovery_create_with_conflicting_reload_reports_corruption() {
        let (_tmp, _store, project_id, shared, harness, _sender) =
            uncertain_discovery_fixture("uncertain-conflict").await;
        let thread_id = ThreadId::new();
        *shared.discovery_create_fault.lock().unwrap() =
            Some(super::DiscoveryCreateFault::CommittedConflicting);

        let result = super::ensure_discovered_thread_owner(
            project_id,
            discovery_ticket(&harness, thread_id, "native-expected"),
            harness.clone(),
            shared,
        )
        .await;
        let Err(error) = result else {
            panic!("conflicting reload must fail discovery materialization")
        };
        assert!(
            error
                .to_string()
                .contains("reload found conflicting native")
        );
        assert!(harness.route.receiver.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn owner_install_admission_failure_publishes_durable_orphan_and_allows_retry() {
        let (_tmp, store, project_id, shared, harness, _sender) =
            uncertain_discovery_fixture("owner-install-retry").await;
        let (client_tx, _client_rx) = tokio::sync::mpsc::channel(4);
        let replacements = shared
            .hub
            .register_client(shared.hub.next_client_id(), client_tx)
            .await;
        let thread_id = ThreadId::new();
        shared
            .background_tasks
            .closed
            .store(true, Ordering::Release);

        assert!(
            super::ensure_discovered_thread_owner(
                project_id,
                discovery_ticket(&harness, thread_id, "native-install-retry"),
                harness.clone(),
                shared.clone(),
            )
            .await
            .is_err()
        );
        assert!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            replacements.recv().await,
            giskard_proto::ServerMessage::ThreadCatalogChanged
        ));
        assert!(
            harness.route.receiver.lock().unwrap().is_some(),
            "failed installation must restore the exact attachment receiver"
        );
        assert!(shared.coordinator(thread_id).await.is_none());

        // This test-only reopening models a later ordinary discovery attempt after the injected
        // admission failure; production shutdown never reopens a closed tracker.
        shared
            .background_tasks
            .closed
            .store(false, Ordering::Release);
        super::ensure_discovered_thread_owner(
            project_id,
            discovery_ticket(&harness, thread_id, "native-install-retry"),
            harness,
            shared.clone(),
        )
        .await
        .unwrap();
        assert!(shared.coordinator(thread_id).await.is_some());
    }

    #[tokio::test]
    async fn discovery_reattaches_dormant_primary_and_subagent_without_new_records() {
        for kind in [
            giskard_core::thread::ThreadKind::Primary,
            giskard_core::thread::ThreadKind::Subagent,
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
            let (project_id, _) = create_test_project(&store, "reattach").await;
            let thread_id = ThreadId::new();
            store
                .save_thread(
                    project_id,
                    &durable_thread(project_id, thread_id, "native-dormant", kind),
                )
                .await
                .unwrap();
            let shared = Arc::new(super::RegistryShared::new(
                Arc::new(Hub::new()),
                store.clone(),
                ledger::spawn(store.clone()),
            ));
            let (sender, receiver) = broadcast::channel(4);
            let harness = Arc::new(DiscoveryOwnerHarness {
                route: TestRoute::new(receiver),
                subscriptions: AtomicUsize::new(0),
                routes: StdMutex::new(HashMap::new()),
                discovery_tx: StdMutex::new(None),
                discovery_rx: StdMutex::new(None),
            });

            super::ensure_discovered_thread_owner(
                project_id,
                discovery_ticket(&harness, thread_id, "native-dormant"),
                harness.clone(),
                shared.clone(),
            )
            .await
            .unwrap();

            assert_eq!(
                store
                    .load_thread(project_id, thread_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .kind,
                kind
            );
            assert_eq!(harness.subscriptions.load(Ordering::SeqCst), 1);
            assert_eq!(
                super::load_thread_graph(&store, project_id)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            drop(sender);
            while shared.coordinator(thread_id).await.is_some() {
                tokio::task::yield_now().await;
            }
        }
    }

    #[tokio::test]
    async fn discovery_rejects_installing_coordinator_without_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, _) = create_test_project(&store, "explicit-race").await;
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let thread_id = ThreadId::new();
        let authority = shared
            .intern_thread_authority(thread_id, project_id)
            .await
            .unwrap();
        let installing = Arc::new(super::ThreadCoordinator::new(
            super::LoadedThreadBinding {
                project_id,
                handle: ThreadHandle::opened(thread_id, "native-explicit".into(), "/tmp".into()),
                native_model: None,
            },
            super::ClassificationPhase::Primary,
        ));
        assert!(
            authority
                .install_coordinator_if_empty(installing.clone())
                .await
                .is_ok()
        );
        let (_, receiver) = broadcast::channel(1);
        let harness = Arc::new(DiscoveryOwnerHarness {
            route: TestRoute::new(receiver),
            subscriptions: AtomicUsize::new(0),
            routes: StdMutex::new(HashMap::new()),
            discovery_tx: StdMutex::new(None),
            discovery_rx: StdMutex::new(None),
        });

        let error = super::ensure_discovered_thread_owner(
            project_id,
            discovery_ticket(&harness, thread_id, "native-explicit"),
            harness.clone(),
            shared.clone(),
        )
        .await
        .err()
        .unwrap();

        assert!(error.to_string().contains("before durable metadata"));
        assert!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(harness.subscriptions.load(Ordering::SeqCst), 0);

        assert!(authority.clear_coordinator_if(&installing).await);
        super::ensure_discovered_thread_owner(
            project_id,
            discovery_ticket(&harness, thread_id, "native-explicit"),
            harness.clone(),
            shared.clone(),
        )
        .await
        .expect("a later discovery must retry after the pre-claim failure");
        assert_eq!(harness.subscriptions.load(Ordering::SeqCst), 1);
        assert!(shared.coordinator(thread_id).await.is_some());
    }

    #[tokio::test]
    async fn dormant_subagent_rejects_a_claim_for_another_thread() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, config) = create_test_project(&store, "subagent-claim-mismatch").await;
        let thread_id = ThreadId::new();
        let returned_thread_id = ThreadId::new();
        let thread = durable_thread(
            project_id,
            thread_id,
            "native-subagent",
            giskard_core::thread::ThreadKind::Subagent,
        );
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));
        let (_, receiver) = broadcast::channel(1);
        let harness = Arc::new(DiscoveryOwnerHarness {
            route: TestRoute::new(receiver),
            subscriptions: AtomicUsize::new(0),
            routes: StdMutex::new(HashMap::from([(
                "native-subagent".into(),
                returned_thread_id,
            )])),
            discovery_tx: StdMutex::new(None),
            discovery_rx: StdMutex::new(None),
        });
        let authority = shared.intern_project_authority(project_id).await;
        {
            let mut transitions = shared.harness_transitions.lock().await;
            transitions
                .project(&authority)
                .await
                .publish_active(harness.clone());
        }

        let permit = super::lock_project_lifecycle(&shared.projects, project_id).await;
        let error = super::ensure_subagent_thread_open_locked(
            &permit,
            &config,
            &thread,
            &shared,
            super::SubagentRouteClaim::ExplicitReattach,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains(&returned_thread_id.to_string()));
        assert!(error.to_string().contains(&thread_id.to_string()));
        assert_eq!(harness.subscriptions.load(Ordering::SeqCst), 1);
        assert!(harness.route.receiver.lock().unwrap().is_some());
        assert!(shared.coordinator(thread_id).await.is_none());
        assert!(shared.coordinator(returned_thread_id).await.is_none());
    }

    #[tokio::test]
    async fn parent_materialization_cannot_reactivate_a_tombstoned_subagent() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, _) = create_test_project(&store, "parent-tombstone").await;
        let parent_id = ThreadId::new();
        let child_id = ThreadId::new();
        let spawning_turn = TurnId::new();
        store
            .save_thread(
                project_id,
                &durable_thread(
                    project_id,
                    parent_id,
                    "native-parent",
                    giskard_core::thread::ThreadKind::Primary,
                ),
            )
            .await
            .unwrap();
        let mut child = durable_thread(
            project_id,
            child_id,
            "native-child",
            giskard_core::thread::ThreadKind::Subagent,
        );
        child.parent_thread_id = Some(parent_id);
        child.spawned_by_turn_id = Some(spawning_turn);
        store.save_thread(project_id, &child).await.unwrap();

        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let (_, receiver) = broadcast::channel(1);
        let harness = Arc::new(DiscoveryOwnerHarness {
            route: TestRoute::new(receiver),
            subscriptions: AtomicUsize::new(0),
            routes: StdMutex::new(HashMap::from([
                ("native-child".into(), child_id),
                ("__tombstoned__".into(), child_id),
            ])),
            discovery_tx: StdMutex::new(None),
            discovery_rx: StdMutex::new(None),
        });
        let authority = shared.intern_project_authority(project_id).await;
        {
            let mut transitions = shared.harness_transitions.lock().await;
            transitions
                .project(&authority)
                .await
                .publish_active(harness.clone());
        }

        let error = super::materialize_subagent_thread(
            parent_id,
            project_id,
            spawning_turn,
            super::SubagentActivityInfo {
                native_thread_id: "native-child".into(),
                agent_name: None,
                agent_path: None,
                title: None,
            },
            shared.clone(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("cannot reactivate"));
        assert!(
            !harness
                .routes
                .lock()
                .unwrap()
                .contains_key("__reattached__")
        );
        assert_eq!(harness.subscriptions.load(Ordering::SeqCst), 0);
        assert!(shared.coordinator(child_id).await.is_none());
        assert_eq!(
            store
                .load_thread(project_id, child_id)
                .await
                .unwrap()
                .unwrap()
                .kind,
            giskard_core::thread::ThreadKind::Subagent
        );
    }

    #[tokio::test]
    async fn parent_materialization_rejects_active_route_without_compatible_coordinator() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, _) = create_test_project(&store, "parent-active-without-owner").await;
        let parent_id = ThreadId::new();
        let child_id = ThreadId::new();
        store
            .save_thread(
                project_id,
                &durable_thread(
                    project_id,
                    parent_id,
                    "native-parent",
                    giskard_core::thread::ThreadKind::Primary,
                ),
            )
            .await
            .unwrap();
        let mut child = durable_thread(
            project_id,
            child_id,
            "native-child",
            giskard_core::thread::ThreadKind::Subagent,
        );
        child.parent_thread_id = Some(parent_id);
        child.spawned_by_turn_id = Some(TurnId::new());
        store.save_thread(project_id, &child).await.unwrap();
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let (_, receiver) = broadcast::channel(1);
        let harness = Arc::new(DiscoveryOwnerHarness {
            route: TestRoute::new(receiver),
            subscriptions: AtomicUsize::new(0),
            routes: StdMutex::new(HashMap::from([
                ("native-child".into(), child_id),
                ("__busy__".into(), child_id),
            ])),
            discovery_tx: StdMutex::new(None),
            discovery_rx: StdMutex::new(None),
        });
        let authority = shared.intern_project_authority(project_id).await;
        {
            let mut transitions = shared.harness_transitions.lock().await;
            transitions
                .project(&authority)
                .await
                .publish_active(harness.clone());
        }

        let error = super::materialize_subagent_thread(
            parent_id,
            project_id,
            child.spawned_by_turn_id.unwrap(),
            super::SubagentActivityInfo {
                native_thread_id: "native-child".into(),
                agent_name: None,
                agent_path: None,
                title: Some("child".into()),
            },
            shared.clone(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("no compatible coordinator"));
        assert!(shared.coordinator(child_id).await.is_none());
        assert_eq!(harness.subscriptions.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.load_thread(project_id, child_id).await.unwrap(),
            Some(child)
        );
    }

    #[tokio::test]
    async fn parent_owner_install_failure_publishes_durable_child_once_and_retries() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, _) = create_test_project(&store, "parent-install-retry").await;
        let parent_id = ThreadId::new();
        store
            .save_thread(
                project_id,
                &durable_thread(
                    project_id,
                    parent_id,
                    "native-parent",
                    giskard_core::thread::ThreadKind::Primary,
                ),
            )
            .await
            .unwrap();
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let (client_tx, _client_rx) = tokio::sync::mpsc::channel(4);
        let replacements = shared
            .hub
            .register_client(shared.hub.next_client_id(), client_tx)
            .await;
        let (_sender, receiver) = broadcast::channel(4);
        let harness = Arc::new(DiscoveryOwnerHarness {
            route: TestRoute::new(receiver),
            subscriptions: AtomicUsize::new(0),
            routes: StdMutex::new(HashMap::new()),
            discovery_tx: StdMutex::new(None),
            discovery_rx: StdMutex::new(None),
        });
        let authority = shared.intern_project_authority(project_id).await;
        {
            let mut transitions = shared.harness_transitions.lock().await;
            transitions
                .project(&authority)
                .await
                .publish_active(harness.clone());
        }
        let spawning_turn = TurnId::new();
        let info = super::SubagentActivityInfo {
            native_thread_id: "native-child".into(),
            agent_name: None,
            agent_path: None,
            title: Some("child".into()),
        };
        shared
            .background_tasks
            .closed
            .store(true, Ordering::Release);

        assert!(
            super::materialize_subagent_thread(
                parent_id,
                project_id,
                spawning_turn,
                info.clone(),
                shared.clone(),
            )
            .await
            .is_err()
        );
        let child = super::load_thread_graph(&store, project_id)
            .await
            .unwrap()
            .into_values()
            .find(|thread| thread.harness_thread_id == "native-child")
            .expect("failed installation must retain durable Subagent metadata");
        assert_eq!(child.kind, giskard_core::thread::ThreadKind::Subagent);
        assert!(matches!(
            replacements.recv().await,
            giskard_proto::ServerMessage::ThreadCatalogChanged
        ));
        assert!(harness.route.receiver.lock().unwrap().is_some());
        assert!(shared.coordinator(child.id).await.is_none());

        // Test-only reopening models a later parent activity after the injected admission failure.
        shared
            .background_tasks
            .closed
            .store(false, Ordering::Release);
        assert_eq!(
            super::materialize_subagent_thread(
                parent_id,
                project_id,
                spawning_turn,
                info,
                shared.clone(),
            )
            .await
            .unwrap(),
            Some(child.id)
        );
        assert!(shared.coordinator(child.id).await.is_some());
        assert_eq!(harness.subscriptions.load(Ordering::SeqCst), 2);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), replacements.recv())
                .await
                .is_err(),
            "retrying an already-published child must not publish creation twice"
        );
    }

    #[tokio::test]
    async fn primary_open_excludes_discovery_through_owner_installation() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, config) = create_test_project(&store, "primary-discovery-race").await;
        let (sender, receiver) = broadcast::channel(4);
        let harness = Arc::new(BlockingPrimaryHarness {
            open_started: Notify::new(),
            open_release: Notify::new(),
            turn_started: Notify::new(),
            turn_release: Notify::new(),
            turn_completed: Notify::new(),
            start_success: AtomicBool::new(true),
            shutdown_started: AtomicBool::new(false),
            delete_mode: AtomicU8::new(0),
            sabotage_worktree_removal: AtomicBool::new(false),
            cleanup_started: Notify::new(),
            cleanup_release: Notify::new(),
            block_cleanup: AtomicBool::new(false),
            sender: StdMutex::new(Some(sender)),
            route: TestRoute::new(receiver),
            claims: AtomicUsize::new(0),
            subscriptions: AtomicUsize::new(0),
        });
        let registry = Arc::new(super::HarnessRegistry::new(
            Arc::new(BlockingPrimaryFactory {
                harness: harness.clone(),
            }),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let thread_id = ThreadId::new();
        let opening = {
            let registry = registry.clone();
            let config = config.clone();
            tokio::spawn(async move {
                registry
                    .materialize_primary_thread(
                        &config,
                        "/tmp/test",
                        thread_id,
                        ModelRef {
                            provider: "test".into(),
                            model: "test-model".into(),
                            reasoning_effort: None,
                        },
                        super::NewPrimaryThread {
                            title: "Primary".into(),
                            mode: TurnMode::Known(Mode::Build),
                            permission_preset: PermissionPreset::AskFirst,
                            context_window: 0,
                            git_workspace: None,
                        },
                    )
                    .await
            })
        };
        harness.open_started.notified().await;

        let lifecycle_error = registry
            .lock_project_lifecycle_with_timeout(project_id, std::time::Duration::from_millis(1))
            .await
            .unwrap_err();
        assert!(matches!(lifecycle_error, HarnessError::Timeout(_)));
        let discovery_started = Arc::new(Notify::new());
        let discovery = harness.route.ticket(ThreadHandle::opened(
            thread_id,
            "native-primary".into(),
            "/tmp".into(),
        ));
        let discovering = {
            let harness: Arc<dyn AgentHarness> = harness.clone();
            let shared = registry.shared.clone();
            let discovery_started = discovery_started.clone();
            tokio::spawn(async move {
                discovery_started.notify_one();
                super::ensure_discovered_thread_owner(project_id, discovery, harness, shared).await
            })
        };
        discovery_started.notified().await;
        tokio::task::yield_now().await;
        assert!(
            !discovering.is_finished(),
            "traffic discovery passed Primary materialization"
        );

        harness.open_release.notify_one();
        harness.turn_started.notified().await;
        harness.turn_release.notify_one();
        let materialized = opening.await.unwrap().unwrap();
        discovering.await.unwrap().unwrap();

        let handle = materialized.handle;
        assert_eq!(handle.thread, thread_id);
        let coordinator = registry.shared.coordinator(thread_id).await.unwrap();
        coordinator
            .reusable_handle(
                project_id,
                thread_id,
                Some("native-primary"),
                super::ClassificationPhase::Primary,
            )
            .await
            .unwrap();
        assert_eq!(harness.claims.load(Ordering::SeqCst), 0);
        assert_eq!(harness.subscriptions.load(Ordering::SeqCst), 1);
        let persisted = store
            .load_thread(project_id, thread_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.kind, super::ThreadKind::Primary);

        registry.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn provider_delete_failure_reopen_installs_a_new_coordinator() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, config) = create_test_project(&store, "delete-failure-reopen").await;
        let (sender, receiver) = broadcast::channel(4);
        let harness = Arc::new(BlockingPrimaryHarness {
            open_started: Notify::new(),
            open_release: Notify::new(),
            turn_started: Notify::new(),
            turn_release: Notify::new(),
            turn_completed: Notify::new(),
            start_success: AtomicBool::new(true),
            shutdown_started: AtomicBool::new(false),
            delete_mode: AtomicU8::new(0),
            sabotage_worktree_removal: AtomicBool::new(false),
            cleanup_started: Notify::new(),
            cleanup_release: Notify::new(),
            block_cleanup: AtomicBool::new(false),
            sender: StdMutex::new(Some(sender)),
            route: TestRoute::new(receiver),
            claims: AtomicUsize::new(0),
            subscriptions: AtomicUsize::new(0),
        });
        let registry = Arc::new(super::HarnessRegistry::new(
            Arc::new(BlockingPrimaryFactory {
                harness: harness.clone(),
            }),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));
        let thread_id = ThreadId::new();
        let model = ModelRef {
            provider: "test".into(),
            model: "test-model".into(),
            reasoning_effort: None,
        };
        let initial_open = {
            let harness = harness.clone();
            tokio::spawn(async move {
                harness.open_started.notified().await;
                harness.open_release.notify_one();
                harness.turn_release.notify_one();
            })
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            registry.materialize_primary_thread(
                &config,
                "/tmp/test",
                thread_id,
                model.clone(),
                super::NewPrimaryThread {
                    title: "Primary".into(),
                    mode: TurnMode::Known(Mode::Build),
                    permission_preset: PermissionPreset::AskFirst,
                    context_window: 0,
                    git_workspace: None,
                },
            ),
        )
        .await
        .expect("initial Primary materialization timed out")
        .unwrap();
        initial_open.await.unwrap();
        let original = registry.shared.coordinator(thread_id).await.unwrap();

        harness.delete_mode.store(1, Ordering::SeqCst);
        let permit = registry.lock_project_lifecycle(project_id).await;
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            registry.delete_thread(&permit, &config, thread_id, "native-primary".into()),
        )
        .await
        .expect("provider-failed deletion timed out")
        .unwrap_err();
        drop(permit);
        assert!(error.to_string().contains("provider cleanup failure"));
        assert!(registry.shared.coordinator(thread_id).await.is_none());

        harness.delete_mode.store(0, Ordering::SeqCst);
        let reopened_open = {
            let harness = harness.clone();
            tokio::spawn(async move {
                harness.open_started.notified().await;
                harness.open_release.notify_one();
            })
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            registry.open_thread(
                &config,
                "/tmp/test",
                thread_id,
                Some("native-primary".into()),
                model,
            ),
        )
        .await
        .expect("exact reopen timed out")
        .unwrap();
        reopened_open.await.unwrap();
        let reopened = registry.shared.coordinator(thread_id).await.unwrap();
        assert!(!Arc::ptr_eq(&original, &reopened));
        assert_eq!(harness.subscriptions.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cancelled_primary_caller_does_not_cancel_creation() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, config) = create_test_project(&store, "cancelled-primary").await;
        let (sender, receiver) = broadcast::channel(4);
        let harness = Arc::new(BlockingPrimaryHarness {
            open_started: Notify::new(),
            open_release: Notify::new(),
            turn_started: Notify::new(),
            turn_release: Notify::new(),
            turn_completed: Notify::new(),
            start_success: AtomicBool::new(true),
            shutdown_started: AtomicBool::new(false),
            delete_mode: AtomicU8::new(0),
            sabotage_worktree_removal: AtomicBool::new(false),
            cleanup_started: Notify::new(),
            cleanup_release: Notify::new(),
            block_cleanup: AtomicBool::new(false),
            sender: StdMutex::new(Some(sender)),
            route: TestRoute::new(receiver),
            claims: AtomicUsize::new(0),
            subscriptions: AtomicUsize::new(0),
        });
        let registry = Arc::new(super::HarnessRegistry::new(
            Arc::new(BlockingPrimaryFactory {
                harness: harness.clone(),
            }),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let thread_id = ThreadId::new();
        let held_permit = registry.lock_project_lifecycle(project_id).await;
        let caller = {
            let registry = registry.clone();
            let config = config.clone();
            tokio::spawn(async move {
                registry
                    .create_primary_and_start(
                        config,
                        "/tmp/test".into(),
                        thread_id,
                        ModelRef {
                            provider: "test".into(),
                            model: "test-model".into(),
                            reasoning_effort: None,
                        },
                        super::NewPrimaryThread {
                            title: "Primary".into(),
                            mode: TurnMode::Known(Mode::Build),
                            permission_preset: PermissionPreset::AskFirst,
                            context_window: 0,
                            git_workspace: None,
                        },
                        giskard_proto::GitStrategy::Shared,
                        UserInput::text("continue after cancellation"),
                        giskard_core::turn::TurnOverrides {
                            model: None,
                            mode: Mode::Build,
                            permission_preset: PermissionPreset::AskFirst,
                        },
                    )
                    .await
            })
        };
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(1),
                harness.open_started.notified()
            )
            .await
            .is_err(),
            "Primary provider I/O began while its project permit was unavailable"
        );
        drop(held_permit);
        harness.open_started.notified().await;
        harness.open_release.notify_one();
        harness.turn_started.notified().await;
        assert!(
            registry.shared.coordinator(thread_id).await.is_some(),
            "native response did not install the exact Primary owner before turn admission"
        );

        caller.abort();
        match caller.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("aborted Primary caller unexpectedly completed"),
        }
        let mut shutdown = {
            let registry = registry.clone();
            tokio::spawn(async move { registry.shutdown().await })
        };
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(1), &mut shutdown)
                .await
                .is_err(),
            "shutdown completed while an admitted Primary was awaiting turn acceptance"
        );
        assert!(
            !harness.shutdown_started.load(Ordering::SeqCst),
            "harness shutdown overtook an admitted Primary operation"
        );
        harness.turn_release.notify_one();
        harness.turn_completed.notified().await;
        shutdown.await.unwrap().unwrap();
        assert!(harness.shutdown_started.load(Ordering::SeqCst));
        let durable = store
            .load_thread(project_id, thread_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(durable.kind, super::ThreadKind::Primary);
        assert_eq!(durable.harness_thread_id, "native-primary");
    }

    #[tokio::test]
    async fn dropped_final_result_receiver_at_every_primary_phase_still_publishes() {
        use super::primary::{Phase, PhaseGate};

        for phase in [
            Phase::WaitingForPermit,
            Phase::WorkspaceCreation,
            Phase::NativeCommandAdmission,
            Phase::NativeResponse,
            Phase::MetadataRename,
            Phase::OwnerInstallation,
            Phase::TurnPreparation,
            Phase::StartTurn,
            Phase::TurnAccepted,
            Phase::Publication,
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
            let (project_id, config) = if phase == Phase::WorkspaceCreation {
                let repo = tmp.path().join("repo");
                std::fs::create_dir_all(&repo).unwrap();
                for args in [
                    &["init", "-q", "-b", "main"][..],
                    &["config", "user.email", "t@example.invalid"][..],
                    &["config", "user.name", "T"][..],
                    &["commit", "-q", "--allow-empty", "-m", "initial"][..],
                ] {
                    assert!(
                        std::process::Command::new("git")
                            .current_dir(&repo)
                            .args(args)
                            .status()
                            .unwrap()
                            .success()
                    );
                }
                let project_id = ProjectId::new();
                store
                    .create_project(project_id, "phase-cancellation", &repo.to_string_lossy())
                    .await
                    .unwrap();
                (
                    project_id,
                    store.load_project(project_id).await.unwrap().unwrap(),
                )
            } else {
                create_test_project(&store, "phase-cancellation").await
            };
            let (sender, receiver) = broadcast::channel(4);
            let harness = Arc::new(BlockingPrimaryHarness {
                open_started: Notify::new(),
                open_release: Notify::new(),
                turn_started: Notify::new(),
                turn_release: Notify::new(),
                turn_completed: Notify::new(),
                start_success: AtomicBool::new(true),
                shutdown_started: AtomicBool::new(false),
                delete_mode: AtomicU8::new(0),
                sabotage_worktree_removal: AtomicBool::new(false),
                cleanup_started: Notify::new(),
                cleanup_release: Notify::new(),
                block_cleanup: AtomicBool::new(false),
                sender: StdMutex::new(Some(sender)),
                route: TestRoute::new(receiver),
                claims: AtomicUsize::new(0),
                subscriptions: AtomicUsize::new(0),
            });
            let registry = Arc::new(super::HarnessRegistry::new(
                Arc::new(BlockingPrimaryFactory {
                    harness: harness.clone(),
                }),
                Arc::new(Hub::new()),
                store.clone(),
                ledger::spawn(store.clone()),
            ));
            if phase != Phase::NativeCommandAdmission {
                harness.open_release.notify_one();
            }
            if phase != Phase::StartTurn {
                harness.turn_release.notify_one();
            }
            if phase == Phase::MetadataRename {
                registry
                    .shared
                    .primary_create_committed_error
                    .store(true, Ordering::SeqCst);
            }
            let thread_id = ThreadId::new();
            let gate = PhaseGate::new(phase);
            let worktree_gate = if phase == Phase::WorkspaceCreation {
                Some(crate::worktree::gate_create(
                    crate::worktree::worktree_path(
                        store.data_dir(),
                        &project_id.to_string(),
                        thread_id,
                    ),
                ))
            } else {
                None
            };
            let held_permit = if phase == Phase::WaitingForPermit {
                Some(registry.lock_project_lifecycle(project_id).await)
            } else {
                None
            };
            let caller = {
                let registry = registry.clone();
                let gate = gate.clone();
                tokio::spawn(async move {
                    registry
                        .create_primary_with_phase_gate(
                            config,
                            thread_id,
                            ModelRef {
                                provider: "test".into(),
                                model: "test-model".into(),
                                reasoning_effort: None,
                            },
                            if phase == Phase::WorkspaceCreation {
                                giskard_proto::GitStrategy::Worktree
                            } else {
                                giskard_proto::GitStrategy::Shared
                            },
                            gate,
                        )
                        .await
                })
            };
            if let Some(worktree_gate) = worktree_gate.as_ref() {
                worktree_gate.wait_arrived().await;
            } else {
                gate.wait_arrived().await;
            }
            if phase == Phase::NativeCommandAdmission {
                gate.release();
                harness.open_started.notified().await;
            } else if phase == Phase::StartTurn {
                gate.release();
                harness.turn_started.notified().await;
            } else if phase == Phase::WaitingForPermit {
                gate.release();
                assert!(
                    tokio::time::timeout(
                        std::time::Duration::from_millis(1),
                        harness.open_started.notified(),
                    )
                    .await
                    .is_err(),
                    "Primary provider I/O began while the project permit was held"
                );
            } else if worktree_gate.is_some() {
                gate.release();
            }
            caller.abort();
            match caller.await {
                Err(error) => assert!(error.is_cancelled(), "phase {phase:?}"),
                Ok(_) => panic!("phase {phase:?} caller unexpectedly completed"),
            }
            if phase == Phase::NativeCommandAdmission {
                harness.open_release.notify_one();
            } else if phase == Phase::StartTurn {
                harness.turn_release.notify_one();
            } else if let Some(worktree_gate) = worktree_gate {
                worktree_gate.release();
            } else if phase != Phase::WaitingForPermit {
                gate.release();
            }
            drop(held_permit);
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                loop {
                    if let Some(coordinator) = registry.shared.coordinator(thread_id).await
                        && coordinator
                            .reusable_handle(
                                project_id,
                                thread_id,
                                None,
                                super::ClassificationPhase::Primary,
                            )
                            .await
                            .is_ok()
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("phase {phase:?} did not publish a Live Primary owner"));
            registry.shutdown().await.unwrap();
            let durable = store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("phase {phase:?} did not publish its Primary"));
            assert_eq!(durable.kind, ThreadKind::Primary, "phase {phase:?}");
        }
    }

    #[tokio::test]
    async fn committed_metadata_create_error_reloads_and_publishes_primary() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, config) = create_test_project(&store, "committed-metadata-error").await;
        let (sender, receiver) = broadcast::channel(4);
        let harness = Arc::new(BlockingPrimaryHarness {
            open_started: Notify::new(),
            open_release: Notify::new(),
            turn_started: Notify::new(),
            turn_release: Notify::new(),
            turn_completed: Notify::new(),
            start_success: AtomicBool::new(true),
            shutdown_started: AtomicBool::new(false),
            delete_mode: AtomicU8::new(0),
            sabotage_worktree_removal: AtomicBool::new(false),
            cleanup_started: Notify::new(),
            cleanup_release: Notify::new(),
            block_cleanup: AtomicBool::new(false),
            sender: StdMutex::new(Some(sender)),
            route: TestRoute::new(receiver),
            claims: AtomicUsize::new(0),
            subscriptions: AtomicUsize::new(0),
        });
        harness.open_release.notify_one();
        harness.turn_release.notify_one();
        let registry = super::HarnessRegistry::new(
            Arc::new(BlockingPrimaryFactory { harness }),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        );
        registry
            .shared
            .primary_create_committed_error
            .store(true, Ordering::SeqCst);
        let thread_id = ThreadId::new();

        registry
            .create_primary_and_start(
                config,
                "/tmp/test".into(),
                thread_id,
                ModelRef {
                    provider: "test".into(),
                    model: "test-model".into(),
                    reasoning_effort: None,
                },
                super::NewPrimaryThread {
                    title: "Primary".into(),
                    mode: TurnMode::Known(Mode::Build),
                    permission_preset: PermissionPreset::AskFirst,
                    context_window: 0,
                    git_workspace: None,
                },
                giskard_proto::GitStrategy::Shared,
                UserInput::text("recover committed metadata"),
                giskard_core::turn::TurnOverrides {
                    model: None,
                    mode: Mode::Build,
                    permission_preset: PermissionPreset::AskFirst,
                },
            )
            .await
            .unwrap();

        let durable = store
            .load_thread(project_id, thread_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(durable.kind, ThreadKind::Primary);
        assert_eq!(durable.harness_thread_id, "native-primary");
    }

    #[tokio::test]
    async fn dropped_cold_reopen_receiver_cannot_strand_the_attachment() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, config) = create_test_project(&store, "cancelled-cold-reopen").await;
        let thread_id = ThreadId::new();
        let mut durable =
            durable_thread(project_id, thread_id, "native-primary", ThreadKind::Primary);
        let model = ModelRef {
            provider: "test".into(),
            model: "test-model".into(),
            reasoning_effort: None,
        };
        durable.current_model = TurnModel::Known(model.clone());
        store.save_thread(project_id, &durable).await.unwrap();
        let (sender, receiver) = broadcast::channel(4);
        let harness = Arc::new(BlockingPrimaryHarness {
            open_started: Notify::new(),
            open_release: Notify::new(),
            turn_started: Notify::new(),
            turn_release: Notify::new(),
            turn_completed: Notify::new(),
            start_success: AtomicBool::new(true),
            shutdown_started: AtomicBool::new(false),
            delete_mode: AtomicU8::new(0),
            sabotage_worktree_removal: AtomicBool::new(false),
            cleanup_started: Notify::new(),
            cleanup_release: Notify::new(),
            block_cleanup: AtomicBool::new(false),
            sender: StdMutex::new(Some(sender)),
            route: TestRoute::new(receiver),
            claims: AtomicUsize::new(0),
            subscriptions: AtomicUsize::new(0),
        });
        let registry = Arc::new(super::HarnessRegistry::new(
            Arc::new(BlockingPrimaryFactory {
                harness: harness.clone(),
            }),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));
        let caller = {
            let registry = registry.clone();
            tokio::spawn(async move {
                registry
                    .open_thread(
                        &config,
                        "/ignored",
                        thread_id,
                        Some("native-primary".into()),
                        model,
                    )
                    .await
            })
        };
        harness.open_started.notified().await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        harness.open_release.notify_one();
        registry.shutdown().await.unwrap();
        assert_eq!(harness.subscriptions.load(Ordering::SeqCst), 1);
    }

    #[cfg(not(unix))]
    async fn failed_primary_rollback_fixture(
        delete_mode: u8,
        metadata_delete_error: bool,
    ) -> (Arc<PersistStore>, ProjectId, ThreadId, HarnessError) {
        let tmp = tempfile::tempdir().unwrap().keep();
        let store = Arc::new(PersistStore::new(tmp));
        let (project_id, config) = create_test_project(&store, "failed-primary").await;
        let (sender, receiver) = broadcast::channel(4);
        let harness = Arc::new(BlockingPrimaryHarness {
            open_started: Notify::new(),
            open_release: Notify::new(),
            turn_started: Notify::new(),
            turn_release: Notify::new(),
            turn_completed: Notify::new(),
            start_success: AtomicBool::new(false),
            shutdown_started: AtomicBool::new(false),
            delete_mode: AtomicU8::new(delete_mode),
            sabotage_worktree_removal: AtomicBool::new(false),
            cleanup_started: Notify::new(),
            cleanup_release: Notify::new(),
            block_cleanup: AtomicBool::new(false),
            sender: StdMutex::new(Some(sender)),
            route: TestRoute::new(receiver),
            claims: AtomicUsize::new(0),
            subscriptions: AtomicUsize::new(0),
        });
        harness.open_release.notify_one();
        harness.turn_release.notify_one();
        let registry = super::HarnessRegistry::new(
            Arc::new(BlockingPrimaryFactory { harness }),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        );
        registry
            .shared
            .primary_delete_error
            .store(metadata_delete_error, Ordering::SeqCst);
        let thread_id = ThreadId::new();
        let error = registry
            .create_primary_and_start(
                config,
                "/tmp/test".into(),
                thread_id,
                ModelRef {
                    provider: "test".into(),
                    model: "test-model".into(),
                    reasoning_effort: None,
                },
                super::NewPrimaryThread {
                    title: "failed Primary".into(),
                    mode: TurnMode::Known(Mode::Build),
                    permission_preset: PermissionPreset::AskFirst,
                    context_window: 0,
                    git_workspace: None,
                },
                giskard_proto::GitStrategy::Shared,
                UserInput::text("fail turn"),
                giskard_core::turn::TurnOverrides {
                    model: None,
                    mode: Mode::Build,
                    permission_preset: PermissionPreset::AskFirst,
                },
            )
            .await
            .err()
            .unwrap_or_else(|| panic!("failed Primary unexpectedly succeeded"));
        (store, project_id, thread_id, error)
    }

    #[cfg(unix)]
    async fn failed_primary_worktree_fixture(
        delete_mode: u8,
        metadata_delete_error: bool,
        sabotage_removal: bool,
        authority_conflict: bool,
    ) -> (
        Arc<PersistStore>,
        ProjectId,
        ThreadId,
        std::path::PathBuf,
        String,
        std::path::PathBuf,
        HarnessError,
    ) {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap().keep();
        let repo = tmp.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        for args in [
            &["init", "-q", "-b", "main"][..],
            &["config", "user.email", "t@example.invalid"][..],
            &["config", "user.name", "T"][..],
            &["commit", "-q", "--allow-empty", "-m", "initial"][..],
        ] {
            let output = std::process::Command::new("git")
                .current_dir(&repo)
                .args(args)
                .output()
                .unwrap();
            assert!(output.status.success(), "git {args:?} failed");
        }
        let store = Arc::new(PersistStore::new(tmp.join("data")));
        let project_id = ProjectId::new();
        store
            .create_project(
                project_id,
                "failed-worktree-primary",
                &repo.to_string_lossy(),
            )
            .await
            .unwrap();
        let config = store.load_project(project_id).await.unwrap().unwrap();
        let (sender, receiver) = broadcast::channel(4);
        let harness = Arc::new(BlockingPrimaryHarness {
            open_started: Notify::new(),
            open_release: Notify::new(),
            turn_started: Notify::new(),
            turn_release: Notify::new(),
            turn_completed: Notify::new(),
            start_success: AtomicBool::new(false),
            shutdown_started: AtomicBool::new(false),
            delete_mode: AtomicU8::new(delete_mode),
            sabotage_worktree_removal: AtomicBool::new(sabotage_removal),
            cleanup_started: Notify::new(),
            cleanup_release: Notify::new(),
            block_cleanup: AtomicBool::new(false),
            sender: StdMutex::new(Some(sender)),
            route: TestRoute::new(receiver),
            claims: AtomicUsize::new(0),
            subscriptions: AtomicUsize::new(0),
        });
        harness.open_release.notify_one();
        harness.turn_release.notify_one();
        let registry = super::HarnessRegistry::new(
            Arc::new(BlockingPrimaryFactory { harness }),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        );
        registry
            .shared
            .primary_delete_error
            .store(metadata_delete_error, Ordering::SeqCst);
        let thread_id = ThreadId::new();
        let worktree_path =
            crate::worktree::worktree_path(store.data_dir(), &project_id.to_string(), thread_id);
        let branch = crate::worktree::branch_name(thread_id);
        if authority_conflict {
            registry
                .shared
                .intern_thread_authority(thread_id, ProjectId::new())
                .await
                .unwrap();
        }
        let result = registry
            .create_primary_and_start(
                config,
                repo.to_string_lossy().into_owned(),
                thread_id,
                ModelRef {
                    provider: "test".into(),
                    model: "test-model".into(),
                    reasoning_effort: None,
                },
                super::NewPrimaryThread {
                    title: "failed worktree Primary".into(),
                    mode: TurnMode::Known(Mode::Build),
                    permission_preset: PermissionPreset::AskFirst,
                    context_window: 0,
                    git_workspace: None,
                },
                giskard_proto::GitStrategy::Worktree,
                UserInput::text("fail turn"),
                giskard_core::turn::TurnOverrides {
                    model: None,
                    mode: Mode::Build,
                    permission_preset: PermissionPreset::AskFirst,
                },
            )
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("failed worktree Primary unexpectedly succeeded"),
        };
        if let Some(parent) = worktree_path.parent() {
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        (
            store,
            project_id,
            thread_id,
            worktree_path,
            branch,
            repo,
            error,
        )
    }

    #[tokio::test]
    async fn metadata_delete_failure_preserves_visible_primary() {
        #[cfg(unix)]
        let (store, project_id, thread_id, worktree_path, _branch, _repo, error) =
            failed_primary_worktree_fixture(0, true, false, false).await;
        #[cfg(not(unix))]
        let (store, project_id, thread_id, error) = failed_primary_rollback_fixture(0, true).await;
        assert!(error.to_string().contains("metadata rollback failed"));
        assert!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .is_some()
        );
        #[cfg(unix)]
        assert!(
            worktree_path.is_dir(),
            "durable recovery worktree was removed"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn worktree_removal_failure_reports_retained_checkout_and_branch() {
        let (store, project_id, thread_id, worktree_path, branch, _repo, error) =
            failed_primary_worktree_fixture(0, false, true, false).await;
        let message = error.to_string();
        assert!(
            message.contains("orphan checkout"),
            "unexpected rollback error: {message}"
        );
        assert!(message.contains(&worktree_path.to_string_lossy().to_string()));
        assert!(message.contains(&branch));
        assert!(
            worktree_path.is_dir(),
            "failed removal did not retain checkout"
        );
        assert!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .is_none(),
            "metadata was recreated after worktree-only cleanup failure"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn authority_intern_failure_rolls_back_primary_worktree_and_branch() {
        let (_store, _project_id, _thread_id, worktree_path, branch, repo, error) =
            failed_primary_worktree_fixture(0, false, false, true).await;
        assert!(
            error
                .to_string()
                .contains("already associated with project"),
            "unexpected authority error: {error}"
        );
        assert!(!worktree_path.exists(), "authority failure leaked worktree");
        let branches = std::process::Command::new("git")
            .current_dir(repo)
            .args(["branch", "--list", &branch])
            .output()
            .unwrap();
        assert!(branches.status.success());
        assert!(
            String::from_utf8_lossy(&branches.stdout).trim().is_empty(),
            "authority failure leaked branch {branch}"
        );
    }

    #[tokio::test]
    async fn provider_cleanup_failure_preserves_visible_degraded_primary() {
        #[cfg(unix)]
        let (store, project_id, thread_id, worktree_path, _branch, _repo, error) =
            failed_primary_worktree_fixture(1, false, false, false).await;
        #[cfg(not(unix))]
        let (store, project_id, thread_id, error) = failed_primary_rollback_fixture(1, false).await;
        assert!(error.to_string().contains("provider cleanup failed"));
        assert!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .is_some()
        );
        #[cfg(unix)]
        assert!(
            worktree_path.is_dir(),
            "provider failure removed recovery worktree"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn provider_cleanup_future_error_preserves_metadata_and_worktree() {
        let (store, project_id, thread_id, worktree_path, _branch, _repo, error) =
            failed_primary_worktree_fixture(3, false, false, false).await;
        assert!(error.to_string().contains("cleanup future failure"));
        assert!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .is_some()
        );
        assert!(worktree_path.is_dir());
    }

    #[tokio::test]
    async fn primary_rollback_retires_owner_before_provider_cleanup() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, config) = create_test_project(&store, "retirement-order").await;
        let (sender, receiver) = broadcast::channel(4);
        let harness = Arc::new(BlockingPrimaryHarness {
            open_started: Notify::new(),
            open_release: Notify::new(),
            turn_started: Notify::new(),
            turn_release: Notify::new(),
            turn_completed: Notify::new(),
            start_success: AtomicBool::new(false),
            shutdown_started: AtomicBool::new(false),
            delete_mode: AtomicU8::new(0),
            sabotage_worktree_removal: AtomicBool::new(false),
            cleanup_started: Notify::new(),
            cleanup_release: Notify::new(),
            block_cleanup: AtomicBool::new(true),
            sender: StdMutex::new(Some(sender)),
            route: TestRoute::new(receiver),
            claims: AtomicUsize::new(0),
            subscriptions: AtomicUsize::new(0),
        });
        harness.open_release.notify_one();
        harness.turn_release.notify_one();
        let registry = Arc::new(super::HarnessRegistry::new(
            Arc::new(BlockingPrimaryFactory {
                harness: harness.clone(),
            }),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let thread_id = ThreadId::new();
        let creating = {
            let registry = registry.clone();
            tokio::spawn(async move {
                registry
                    .create_primary_and_start(
                        config,
                        "/tmp/test".into(),
                        thread_id,
                        ModelRef {
                            provider: "test".into(),
                            model: "test-model".into(),
                            reasoning_effort: None,
                        },
                        super::NewPrimaryThread {
                            title: "failed Primary".into(),
                            mode: TurnMode::Known(Mode::Build),
                            permission_preset: PermissionPreset::AskFirst,
                            context_window: 0,
                            git_workspace: None,
                        },
                        giskard_proto::GitStrategy::Shared,
                        UserInput::text("fail turn"),
                        giskard_core::turn::TurnOverrides {
                            model: None,
                            mode: Mode::Build,
                            permission_preset: PermissionPreset::AskFirst,
                        },
                    )
                    .await
            })
        };

        harness.cleanup_started.notified().await;
        assert!(
            registry.shared.coordinator(thread_id).await.is_none(),
            "provider cleanup began before the exact coordinator was retired"
        );
        assert!(
            store
                .load_thread(project_id, thread_id)
                .await
                .unwrap()
                .is_some(),
            "metadata rollback overtook provider cleanup"
        );
        harness.cleanup_release.notify_one();
        assert!(creating.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn registry_shutdown_closes_and_joins_discovery_consumer() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, config) = create_test_project(&store, "shutdown-discovery").await;
        let (_events_tx, events_rx) = broadcast::channel(1);
        let (discovery_tx, discovery_rx) = tokio::sync::mpsc::channel(1);
        let harness = Arc::new(DiscoveryOwnerHarness {
            route: TestRoute::new(events_rx),
            subscriptions: AtomicUsize::new(0),
            routes: StdMutex::new(HashMap::new()),
            discovery_tx: StdMutex::new(Some(discovery_tx)),
            discovery_rx: StdMutex::new(Some(discovery_rx)),
        });
        let registry = super::HarnessRegistry::new(
            Arc::new(DiscoveryOwnerFactory {
                harness: harness.clone(),
            }),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        );

        registry
            .get_or_create_harness(project_id, &config)
            .await
            .unwrap();
        registry.shutdown().await.unwrap();

        assert!(harness.discovery_tx.lock().unwrap().is_none());
        assert!(registry.shared.active_harness(project_id).await.is_none());
    }

    #[tokio::test]
    async fn unexpected_discovery_consumer_closure_retires_matching_published_harness() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, config) = create_test_project(&store, "closed-discovery").await;
        let (_events_tx, events_rx) = broadcast::channel(1);
        let (discovery_tx, discovery_rx) = tokio::sync::mpsc::channel(1);
        let harness = Arc::new(DiscoveryOwnerHarness {
            route: TestRoute::new(events_rx),
            subscriptions: AtomicUsize::new(0),
            routes: StdMutex::new(HashMap::new()),
            discovery_tx: StdMutex::new(Some(discovery_tx)),
            discovery_rx: StdMutex::new(Some(discovery_rx)),
        });
        let registry = super::HarnessRegistry::new(
            Arc::new(DiscoveryOwnerFactory {
                harness: harness.clone(),
            }),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        );
        let published = registry
            .get_or_create_harness(project_id, &config)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(
            &published,
            &(harness.clone() as Arc<dyn AgentHarness>)
        ));

        drop(harness.discovery_tx.lock().unwrap().take());
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while registry.shared.active_harness(project_id).await.is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("consumer closure must retire the matching published harness");

        registry.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn discovery_rejects_conflicting_durable_native_ownership() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, _) = create_test_project(&store, "conflict").await;
        let owner = ThreadId::new();
        store
            .save_thread(
                project_id,
                &durable_thread(
                    project_id,
                    owner,
                    "native-conflict",
                    giskard_core::thread::ThreadKind::Primary,
                ),
            )
            .await
            .unwrap();
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));
        let (_, receiver) = broadcast::channel(1);
        let harness = Arc::new(DiscoveryOwnerHarness {
            route: TestRoute::new(receiver),
            subscriptions: AtomicUsize::new(0),
            routes: StdMutex::new(HashMap::new()),
            discovery_tx: StdMutex::new(None),
            discovery_rx: StdMutex::new(None),
        });

        let discovered = ThreadId::new();
        let error = super::ensure_discovered_thread_owner(
            project_id,
            discovery_ticket(&harness, discovered, "native-conflict"),
            harness.clone(),
            shared,
        )
        .await
        .err()
        .unwrap();

        assert!(error.to_string().contains(&owner.to_string()));
        assert_eq!(harness.subscriptions.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn status_first_then_parent_materialization_preserves_route_and_owner() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, _) = create_test_project(&store, "status-first").await;
        let parent_id = ThreadId::new();
        store
            .save_thread(
                project_id,
                &durable_thread(
                    project_id,
                    parent_id,
                    "native-parent",
                    giskard_core::thread::ThreadKind::Primary,
                ),
            )
            .await
            .unwrap();
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let (sender, receiver) = broadcast::channel(4);
        let child_id = ThreadId::new();
        let harness = Arc::new(DiscoveryOwnerHarness {
            route: TestRoute::new(receiver),
            subscriptions: AtomicUsize::new(0),
            routes: StdMutex::new(HashMap::from([("native-child".into(), child_id)])),
            discovery_tx: StdMutex::new(None),
            discovery_rx: StdMutex::new(None),
        });
        let authority = shared.intern_project_authority(project_id).await;
        {
            let mut transitions = shared.harness_transitions.lock().await;
            transitions
                .project(&authority)
                .await
                .publish_active(harness.clone());
        }
        super::ensure_discovered_thread_owner(
            project_id,
            discovery_ticket(&harness, child_id, "native-child"),
            harness.clone(),
            shared.clone(),
        )
        .await
        .unwrap();
        let original = shared.coordinator(child_id).await.unwrap();

        let materialized = super::materialize_subagent_thread(
            parent_id,
            project_id,
            TurnId::new(),
            super::SubagentActivityInfo {
                native_thread_id: "native-child".into(),
                agent_name: None,
                agent_path: None,
                title: Some("child".into()),
            },
            shared.clone(),
        )
        .await
        .unwrap();

        assert_eq!(materialized, Some(child_id));
        assert_eq!(
            store
                .load_thread(project_id, child_id)
                .await
                .unwrap()
                .unwrap()
                .kind,
            giskard_core::thread::ThreadKind::Subagent
        );
        assert!(Arc::ptr_eq(
            &original,
            &shared.coordinator(child_id).await.unwrap()
        ));
        assert_eq!(harness.subscriptions.load(Ordering::SeqCst), 1);
        drop(sender);
    }

    #[tokio::test]
    async fn parent_first_then_discovery_keeps_one_subagent_and_owner() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, _) = create_test_project(&store, "parent-first").await;
        let parent_id = ThreadId::new();
        store
            .save_thread(
                project_id,
                &durable_thread(
                    project_id,
                    parent_id,
                    "native-parent",
                    giskard_core::thread::ThreadKind::Primary,
                ),
            )
            .await
            .unwrap();
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let (sender, receiver) = broadcast::channel(4);
        let harness = Arc::new(DiscoveryOwnerHarness {
            route: TestRoute::new(receiver),
            subscriptions: AtomicUsize::new(0),
            routes: StdMutex::new(HashMap::new()),
            discovery_tx: StdMutex::new(None),
            discovery_rx: StdMutex::new(None),
        });
        let authority = shared.intern_project_authority(project_id).await;
        {
            let mut transitions = shared.harness_transitions.lock().await;
            transitions
                .project(&authority)
                .await
                .publish_active(harness.clone());
        }
        let child_id = super::materialize_subagent_thread(
            parent_id,
            project_id,
            TurnId::new(),
            super::SubagentActivityInfo {
                native_thread_id: "native-child".into(),
                agent_name: None,
                agent_path: None,
                title: Some("child".into()),
            },
            shared.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let original = shared.coordinator(child_id).await.unwrap();

        super::ensure_discovered_thread_owner(
            project_id,
            discovery_ticket(&harness, child_id, "native-child"),
            harness.clone(),
            shared.clone(),
        )
        .await
        .unwrap();

        assert_eq!(
            super::load_thread_graph(&store, project_id)
                .await
                .unwrap()
                .values()
                .filter(|thread| thread.kind == giskard_core::thread::ThreadKind::Subagent)
                .count(),
            1
        );
        assert!(Arc::ptr_eq(
            &original,
            &shared.coordinator(child_id).await.unwrap()
        ));
        assert_eq!(harness.subscriptions.load(Ordering::SeqCst), 1);
        drop(sender);
    }

    #[tokio::test]
    async fn concurrent_parent_and_discovery_converge_on_one_durable_child() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project_id, _) = create_test_project(&store, "concurrent").await;
        let parent_id = ThreadId::new();
        store
            .save_thread(
                project_id,
                &durable_thread(
                    project_id,
                    parent_id,
                    "native-parent",
                    giskard_core::thread::ThreadKind::Primary,
                ),
            )
            .await
            .unwrap();
        let shared = Arc::new(super::RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let (sender, receiver) = broadcast::channel(4);
        let child_id = ThreadId::new();
        let harness = Arc::new(DiscoveryOwnerHarness {
            route: TestRoute::new(receiver),
            subscriptions: AtomicUsize::new(0),
            routes: StdMutex::new(HashMap::from([("native-child".into(), child_id)])),
            discovery_tx: StdMutex::new(None),
            discovery_rx: StdMutex::new(None),
        });
        let authority = shared.intern_project_authority(project_id).await;
        {
            let mut transitions = shared.harness_transitions.lock().await;
            transitions
                .project(&authority)
                .await
                .publish_active(harness.clone());
        }
        let lifecycle = super::lock_project_lifecycle(&shared.projects, project_id).await;
        let discovery = discovery_ticket(&harness, child_id, "native-child");
        let discovering = {
            let shared = shared.clone();
            let harness = harness.clone();
            tokio::spawn(async move {
                super::ensure_discovered_thread_owner(project_id, discovery, harness, shared).await
            })
        };
        let materializing = {
            let shared = shared.clone();
            tokio::spawn(async move {
                super::materialize_subagent_thread(
                    parent_id,
                    project_id,
                    TurnId::new(),
                    super::SubagentActivityInfo {
                        native_thread_id: "native-child".into(),
                        agent_name: None,
                        agent_path: None,
                        title: Some("child".into()),
                    },
                    shared,
                )
                .await
            })
        };
        tokio::task::yield_now().await;
        assert!(!discovering.is_finished());
        assert!(!materializing.is_finished());
        drop(lifecycle);

        discovering.await.unwrap().unwrap();
        assert_eq!(materializing.await.unwrap().unwrap(), Some(child_id));
        let graph = super::load_thread_graph(&store, project_id).await.unwrap();
        assert_eq!(graph.len(), 2);
        assert_eq!(
            graph.get(&child_id).unwrap().kind,
            giskard_core::thread::ThreadKind::Subagent
        );
        assert_eq!(harness.subscriptions.load(Ordering::SeqCst), 1);
        drop(sender);
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
                            thread: ThreadId::new(),
                            workspace_root: "/tmp/test".into(),
                            resume: Some("native-child".into()),
                            initial_model: ModelRef {
                                provider: "openai".into(),
                                model: "gpt-test".into(),
                                reasoning_effort: None,
                            },
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
        let (project_id, config) = create_test_project(&store, "first-message").await;
        let thread_id = ThreadId::new();
        let mut durable = durable_thread(
            project_id,
            thread_id,
            "native-first-message",
            ThreadKind::Primary,
        );
        durable.current_model = TurnModel::Known(ModelRef {
            provider: "openai".into(),
            model: "gpt-test".into(),
            reasoning_effort: None,
        });
        store.save_thread(project_id, &durable).await.unwrap();
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
        let result = registry
            .open_thread(
                &config,
                "/tmp/test",
                thread_id,
                Some("native-first-message".into()),
                ModelRef {
                    provider: "openai".into(),
                    model: "gpt-test".into(),
                    reasoning_effort: None,
                },
            )
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

        let permit = registry.lock_project_lifecycle(project_id).await;
        let error = registry
            .delete_project(&permit, project_id)
            .await
            .unwrap_err();
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
    ) -> super::PreparedTurnReservation {
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
        let (events, receiver) = broadcast::channel(2);
        let route = TestRoute::new(receiver);
        let owner = route
            .attachment(ThreadHandle::opened(
                thread_id,
                "native-test".into(),
                "/tmp".into(),
            ))
            .unwrap()
            .commit()
            .unwrap();
        super::launch_event_forwarder(
            registry.shared.clone(),
            authority,
            coordinator,
            owner,
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

        let restored = route
            .attachment(ThreadHandle::opened(
                thread_id,
                "native-test".into(),
                "/tmp".into(),
            ))
            .expect("requested retirement must return the exact route receiver");
        assert_eq!(restored.handle().thread, thread_id);
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
    async fn tombstoned_owner_stream_exit_clears_only_its_matching_coordinator() {
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
        let (events, receiver) = broadcast::channel(2);
        let route = TestRoute::new(receiver);
        let owner = route
            .attachment(ThreadHandle::opened(
                thread_id,
                "native-test".into(),
                "/tmp".into(),
            ))
            .unwrap()
            .commit()
            .unwrap();
        super::launch_event_forwarder(
            shared,
            authority.clone(),
            coordinator,
            owner,
            cancel_rx,
            completed_tx,
            permit,
        );
        // Codex tombstoning closes the route sender. Model that boundary directly: the old owner
        // observes a closed stream after a replacement coordinator has occupied the authority.
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
        assert!(route.receiver.lock().unwrap().is_some());
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
}
