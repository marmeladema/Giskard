use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;
use futures::future::join_all;
use tokio::sync::{Mutex, Notify, OwnedMutexGuard, mpsc, oneshot, watch};
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
    AgentHarness, EventStreamError, HarnessBootstrap, HarnessCapabilities, HarnessProvider,
    KnownThreadBinding, OpenThreadOptions, ThreadDiscovered, ThreadHandle, ThreadUpdate,
    thread_update_channel,
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

mod driver;
mod event_forwarder;
mod project;
mod thread;

use driver::{AttachOutcome, DriverHandle, spawn_project_event_driver};
use event_forwarder::{
    ForwarderExitReason, event_item_id, event_kind, event_turn_id, forwarder_exit_reason_label,
    log_metadata_only_event_rejection,
};
use project::{HarnessTransitions, LifecycleLock, ProjectAuthority, WeakLifecycleLock};
pub(crate) use thread::ThreadAuthority;
use thread::{
    ClassificationPhase, ExternalTurnDefaults, OwnerLock, ThreadBinding, ThreadCoordinator,
    TurnIntent, WeakOwnerLock, external_turn_input_label,
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
    #[cfg(test)]
    discovery_records_processed: AtomicUsize,
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

    async fn event_driver(&self, project_id: ProjectId) -> Option<DriverHandle> {
        let authority = self.project_authority(project_id).await?;
        let mut transitions = self.harness_transitions.lock().await;
        transitions.project(&authority).await.driver()
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
            #[cfg(test)]
            discovery_records_processed: AtomicUsize::new(0),
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

fn spawn_discovery_consumer(
    shared: Arc<RegistryShared>,
    project_id: ProjectId,
    harness: Arc<dyn AgentHarness>,
) {
    let Some(task_permit) = shared.background_tasks.register() else {
        warn!(
            %project_id,
            action = "consume_thread_discoveries",
            reason = "registry_shutting_down",
            "not starting harness discovery consumer"
        );
        return;
    };
    let mut discoveries = harness.discoveries();
    tokio::spawn(async move {
        let _task_permit = task_permit;
        loop {
            match discoveries.recv().await {
                Ok(record) => {
                    let result =
                        admit_discovered_thread(&shared, project_id, &harness, &record).await;
                    #[cfg(test)]
                    shared
                        .discovery_records_processed
                        .fetch_add(1, Ordering::SeqCst);
                    if let Err(error) = result {
                        error!(
                            %project_id,
                            thread_id = %record.thread,
                            harness_thread_id = %record.harness_thread_id,
                            %error,
                            "failed to admit a native thread discovered from traffic"
                        );
                    }
                }
                Err(EventStreamError::Closed) => return,
                Err(EventStreamError::Gap { dropped }) => {
                    error!(%project_id, dropped, "harness discovery stream overflowed");
                }
            }
        }
    });
}

async fn admit_discovered_thread(
    shared: &Arc<RegistryShared>,
    project_id: ProjectId,
    harness: &Arc<dyn AgentHarness>,
    record: &ThreadDiscovered,
) -> Result<(), HarnessError> {
    let _lifecycle_guard = lock_project_lifecycle(&shared.projects, project_id).await;
    let Some(project_config) = shared
        .store
        .load_project(project_id)
        .await
        .map_err(|error| HarnessError::Protocol(error.to_string()))?
    else {
        warn!(
            %project_id,
            thread_id = %record.thread,
            harness_thread_id = %record.harness_thread_id,
            "project disappeared before a discovered native thread could be admitted"
        );
        return Ok(());
    };

    let live_bindings = shared.coordinator_snapshot().await;
    let mut existing_id = None;
    for (thread_id, coordinator) in live_bindings {
        let binding = coordinator.binding().await;
        if binding.project_id == project_id
            && binding.handle.harness_thread_id == record.harness_thread_id
        {
            existing_id = Some(thread_id);
            break;
        }
    }
    let existing = if let Some(thread_id) = existing_id {
        shared
            .store
            .load_thread(project_id, thread_id)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?
    } else {
        load_thread_graph(&shared.store, project_id)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?
            .into_values()
            .find(|thread| thread.harness_thread_id == record.harness_thread_id)
    };

    if let Some(thread_file) = existing {
        if thread_file.kind == ThreadKind::Primary {
            warn!(
                %project_id,
                thread_id = %record.thread,
                existing_thread_id = %thread_file.id,
                harness_thread_id = %record.harness_thread_id,
                "ignoring traffic discovery for an already persisted primary thread"
            );
            return Ok(());
        }
        let classification = ClassificationPhase::from(thread_file.kind);
        ensure_subagent_thread_open(&project_config, &thread_file, shared, classification).await?;
        return Ok(());
    }

    let now = Utc::now();
    let thread_file = ThreadFile {
        revision: 0,
        version: giskard_persist::store::THREAD_METADATA_VERSION,
        id: record.thread,
        project_id,
        title: "Unclassified native thread".into(),
        harness_thread_id: record.harness_thread_id.clone(),
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
    let workspace_root =
        effective_thread_workspace_root(&shared.store, &project_config, &thread_file)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?;
    let thread_file = shared
        .thread_metadata
        .create(project_id, thread_file)
        .await
        .map_err(|error| HarnessError::Protocol(error.to_string()))?;
    let handle = ThreadHandle {
        parent_harness_thread_id: record.parent_harness_thread_id.clone(),
        ..ThreadHandle::opened(
            thread_file.id,
            thread_file.harness_thread_id.clone(),
            workspace_root.into(),
        )
    };
    install_event_owner(
        shared,
        harness,
        LoadedThreadBinding {
            project_id,
            handle,
            native_model: None,
        },
        ClassificationPhase::Orphan,
    )
    .await?;
    Ok(())
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

        let Some(driver_permit) = self.shared.background_tasks.register() else {
            let _ = h.shutdown().await;
            return Err(HarnessError::Protocol(
                "server is shutting down; refusing to start a project event driver".into(),
            ));
        };
        let driver = spawn_project_event_driver(project, self.shared.clone(), &h, driver_permit);
        slot.publish_active(h.clone(), driver);
        spawn_discovery_consumer(self.shared.clone(), project, h.clone());
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
        thread: ThreadId,
        resume: Option<String>,
        initial_model: ModelRef,
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
        ensure_subagent_thread_open(
            config,
            thread,
            &self.shared,
            ClassificationPhase::from(thread.kind),
        )
        .await?;
        self.loaded_thread_binding(thread.id)
            .await
            .map(|binding| binding.handle)
            .ok_or(HarnessError::ThreadNotFound(thread.id))
    }

    async fn open_primary_thread(
        &self,
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
        // Serialize the cold check and native open for an already-known thread. Locking only when
        // publishing the owner is too late: two callers could both open the native thread and the
        // losing open may invalidate the stream already owned by the winner.
        let _owner_guard = lock_thread_owner(&self.shared.threads, thread).await;
        if let Some(existing) = self.shared.coordinator(thread).await {
            return existing
                .reusable_handle(
                    config.id,
                    thread,
                    resume.as_deref(),
                    ClassificationPhase::Primary,
                )
                .await;
        }
        let harness = self.get_or_create_harness(config.id, config).await?;
        let (updates, update_stream) = thread_update_channel();
        let authority = self
            .shared
            .intern_thread_authority(thread, config.id)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?;
        let restore_permit = self.shared.runtime.restoration_permit(&authority);

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
        let binding = LoadedThreadBinding {
            project_id: config.id,
            handle: handle.clone(),
            native_model: Some(native_model),
        };
        let owner_installed = install_event_owner(
            &self.shared,
            &harness,
            binding,
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
        let intents = coordinator.intent_sender().await?;
        let ctx = TurnContext {
            user_input: input.clone(),
            model: TurnModel::Known(effective_model),
            mode: TurnMode::Known(overrides.mode),
            kind: TurnContextKind::User,
        };
        let (reply, response) = oneshot::channel();
        intents
            .send(TurnIntent::StartTurn {
                input,
                overrides,
                context: ctx,
                reply,
            })
            .await
            .map_err(|_| {
                HarnessError::Protocol(format!("thread {thread_id} has no live event owner"))
            })?;
        response.await.map_err(|_| {
            HarnessError::Protocol(format!(
                "thread {thread_id} event owner exited before answering"
            ))
        })?
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
        let intents = coordinator.intent_sender().await?;
        let ctx = TurnContext {
            user_input: UserInput::text("/compact"),
            model: TurnModel::Known(effective_model),
            mode: TurnMode::Known(mode),
            kind: TurnContextKind::ManualCompaction,
        };
        let (reply, response) = oneshot::channel();
        intents
            .send(TurnIntent::Compact {
                context: ctx,
                reply,
            })
            .await
            .map_err(|_| {
                HarnessError::Protocol(format!("thread {thread_id} has no live event owner"))
            })?;
        response.await.map_err(|_| {
            HarnessError::Protocol(format!(
                "thread {thread_id} event owner exited before answering"
            ))
        })?
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
        let Some(authority) = self.shared.thread_authority(thread_id).await else {
            return;
        };
        if let Some(driver) = self.shared.event_driver(authority.project_id()).await {
            driver.detach(thread_id).await;
        } else if let Some(coordinator) = authority.coordinator().await
            && coordinator.is_failed().await
        {
            authority.clear_coordinator_if(&coordinator).await;
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
        let harness_and_driver = if let Some(authority) = authority.as_ref() {
            let mut transitions = self.shared.harness_transitions.lock().await;
            transitions.project(authority).await.begin_delete()?
        } else {
            None
        };
        let mut retained_driver = None;
        if let Some((harness, driver)) = harness_and_driver {
            retained_driver = Some(driver);
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
            if let Some(driver) = retained_driver.as_ref() {
                driver.detach(*thread_id).await;
            } else {
                self.forget_thread(*thread_id).await;
            }
        }
        drop(retained_driver);
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
        let opened_agent_name = ensure_subagent_thread_open(
            &project_config,
            &existing,
            &shared,
            ClassificationPhase::Subagent,
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
    classification: ClassificationPhase,
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
    if let Some(coordinator) = shared.coordinator(thread_file.id).await {
        let handle = coordinator
            .reusable_handle(
                project_config.id,
                thread_file.id,
                Some(&thread_file.harness_thread_id),
                classification,
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
    install_event_owner(
        shared,
        &harness,
        LoadedThreadBinding {
            project_id: project_config.id,
            handle,
            native_model,
        },
        classification,
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

async fn install_event_owner(
    shared: &Arc<RegistryShared>,
    _harness: &Arc<dyn AgentHarness>,
    binding: LoadedThreadBinding,
    classification: ClassificationPhase,
) -> Result<bool, HarnessError> {
    let thread_id = binding.handle.thread;
    let project_id = binding.project_id;
    let driver = shared.event_driver(project_id).await.ok_or_else(|| {
        HarnessError::Protocol(format!("project {project_id} has no event driver"))
    })?;
    match driver.attach(binding, classification).await? {
        AttachOutcome::Installed => Ok(true),
        AttachOutcome::Reused(handle) => {
            drop(handle);
            debug!(%project_id, %thread_id, "reused existing long-lived native event owner");
            Ok(false)
        }
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Weak};

    use chrono::Utc;
    use giskard_core::approval::ApprovalDecision;
    use giskard_core::error::HarnessError;
    use giskard_core::event::AgentEvent;
    use giskard_core::ids::{ApprovalId, ItemId, ProjectId, ServerRequestId, ThreadId, TurnId};
    use giskard_core::item::{Item, ItemPayload};
    use giskard_core::model::{ModelDescriptor, ModelRef};
    use giskard_core::token::{TokenLedger, TokenUsage};
    use giskard_core::turn::{
        Mode, PermissionPreset, TurnMode, TurnModel, TurnStatus, TurnStatusKind,
    };
    use giskard_core::user_input::UserInput;
    use giskard_harness::{
        AgentEventStream, AgentHarness, DiscoveryStream, EventLog, HarnessBootstrap,
        HarnessCapabilities, OpenThreadOptions, ThreadDiscovered, ThreadHandle,
    };
    use giskard_persist::PersistStore;
    use giskard_persist::store::{ProjectConfig, ThreadFile};
    use tokio::sync::Notify;

    use super::{TurnContext, TurnContextKind, turn_reservation};
    use crate::hub::Hub;
    use crate::ledger;
    use crate::test_logs::CapturedLogWriter;

    struct UnusedHarnessFactory;

    struct DiscoveryHarness {
        discoveries: Arc<EventLog<ThreadDiscovered>>,
        routes: StdMutex<HashMap<String, ThreadHandle>>,
        logs: StdMutex<HashMap<ThreadId, Arc<EventLog>>>,
    }

    impl DiscoveryHarness {
        fn new(bootstrap: HarnessBootstrap) -> Self {
            let mut routes = HashMap::new();
            let mut logs = HashMap::new();
            for binding in bootstrap.known_threads {
                let handle = ThreadHandle::opened(
                    binding.thread_id,
                    binding.harness_thread_id.clone(),
                    PathBuf::from("/tmp/test"),
                );
                routes.insert(binding.harness_thread_id, handle);
                logs.insert(binding.thread_id, Arc::new(EventLog::new()));
            }
            Self {
                discoveries: Arc::new(EventLog::new()),
                routes: StdMutex::new(routes),
                logs: StdMutex::new(logs),
            }
        }

        fn announce(&self, record: ThreadDiscovered) {
            self.routes
                .lock()
                .unwrap()
                .entry(record.harness_thread_id.clone())
                .or_insert_with(|| ThreadHandle {
                    parent_harness_thread_id: record.parent_harness_thread_id.clone(),
                    ..ThreadHandle::opened(
                        record.thread,
                        record.harness_thread_id.clone(),
                        PathBuf::from("/tmp/test"),
                    )
                });
            self.logs
                .lock()
                .unwrap()
                .entry(record.thread)
                .or_insert_with(|| Arc::new(EventLog::new()));
            assert!(self.discoveries.append(record));
        }

        fn append_event(&self, thread: ThreadId, event: AgentEvent) {
            let log = self.logs.lock().unwrap().get(&thread).cloned().unwrap();
            assert!(log.append(event));
        }
    }

    #[async_trait::async_trait]
    impl AgentHarness for DiscoveryHarness {
        fn capabilities(&self) -> HarnessCapabilities {
            HarnessCapabilities::default()
        }

        async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError> {
            Ok(Vec::new())
        }

        async fn open_thread(
            &self,
            _opts: OpenThreadOptions,
        ) -> Result<ThreadHandle, HarnessError> {
            Err(HarnessError::Unsupported("unused".into()))
        }

        async fn claim_native_thread(
            &self,
            thread: ThreadId,
            harness_thread_id: String,
            workspace_root: PathBuf,
        ) -> Result<ThreadHandle, HarnessError> {
            let mut routes = self.routes.lock().unwrap();
            if let Some(existing) = routes.get(&harness_thread_id) {
                return Ok(existing.clone());
            }
            let handle = ThreadHandle::opened(thread, harness_thread_id.clone(), workspace_root);
            routes.insert(harness_thread_id, handle.clone());
            self.logs
                .lock()
                .unwrap()
                .entry(thread)
                .or_insert_with(|| Arc::new(EventLog::new()));
            Ok(handle)
        }

        async fn start_turn(
            &self,
            _thread: &ThreadHandle,
            _input: UserInput,
            _overrides: giskard_core::turn::TurnOverrides,
        ) -> Result<TurnId, HarnessError> {
            Err(HarnessError::Unsupported("unused".into()))
        }

        fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream {
            let log = self
                .logs
                .lock()
                .unwrap()
                .get(&thread.thread)
                .cloned()
                .unwrap_or_else(|| Arc::new(EventLog::new()));
            AgentEventStream::new(log.reader())
        }

        fn discoveries(&self) -> DiscoveryStream {
            DiscoveryStream::new(self.discoveries.reader())
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
            self.discoveries.close();
            for log in self.logs.lock().unwrap().values() {
                log.close();
            }
            Ok(())
        }
    }

    struct DiscoveryFactory {
        harness: StdMutex<Option<Arc<DiscoveryHarness>>>,
    }

    #[async_trait::async_trait]
    impl super::HarnessFactory for DiscoveryFactory {
        async fn create(
            &self,
            _config: &ProjectConfig,
            bootstrap: HarnessBootstrap,
        ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
            let harness = Arc::new(DiscoveryHarness::new(bootstrap));
            *self.harness.lock().unwrap() = Some(harness.clone());
            Ok(harness)
        }
    }

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
            AgentEventStream::closed()
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

        fn subscribe(&self, _thread: &ThreadHandle) -> giskard_harness::AgentEventStream {
            giskard_harness::AgentEventStream::closed()
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

    fn test_thread_file(
        project_id: ProjectId,
        id: ThreadId,
        native: &str,
        kind: giskard_core::ThreadKind,
    ) -> ThreadFile {
        let now = Utc::now();
        ThreadFile {
            revision: 0,
            version: giskard_persist::store::THREAD_METADATA_VERSION,
            id,
            project_id,
            title: "test thread".into(),
            harness_thread_id: native.into(),
            parent_thread_id: None,
            spawned_by_turn_id: None,
            kind,
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
        }
    }

    async fn discovery_registry(
        store: Arc<PersistStore>,
    ) -> (Arc<super::HarnessRegistry>, Arc<DiscoveryFactory>) {
        let factory = Arc::new(DiscoveryFactory {
            harness: StdMutex::new(None),
        });
        let registry = Arc::new(super::HarnessRegistry::new(
            factory.clone(),
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));
        (registry, factory)
    }

    async fn wait_for_thread(
        store: &PersistStore,
        project: ProjectId,
        thread: ThreadId,
    ) -> ThreadFile {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Some(file) = store.load_thread(project, thread).await.unwrap() {
                    return file;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("discovered thread was not persisted")
    }

    async fn wait_for_coordinator(
        registry: &super::HarnessRegistry,
        thread: ThreadId,
    ) -> Arc<super::ThreadCoordinator> {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Some(coordinator) = registry.shared.coordinator(thread).await {
                    return coordinator;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("discovered thread owner was not installed")
    }

    async fn wait_for_discovery_records(registry: &super::HarnessRegistry, expected: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while registry
                .shared
                .discovery_records_processed
                .load(Ordering::SeqCst)
                < expected
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("discovery consumer did not process the expected records");
    }

    #[tokio::test]
    async fn discovered_native_thread_becomes_a_hidden_orphan_with_an_owner() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project, config) = create_test_project(&store, "discovered-orphan").await;
        let (registry, factory) = discovery_registry(store.clone()).await;
        registry
            .get_or_create_harness(project, &config)
            .await
            .unwrap();
        let harness = factory.harness.lock().unwrap().clone().unwrap();
        let thread = ThreadId::new();
        harness.announce(ThreadDiscovered {
            thread,
            harness_thread_id: "native-child".into(),
            parent_harness_thread_id: None,
        });

        let file = wait_for_thread(&store, project, thread).await;
        assert_eq!(file.kind, giskard_core::ThreadKind::Orphan);
        assert_eq!(file.current_model, TurnModel::Unknown);
        assert_eq!(file.mode, TurnMode::Unknown);
        assert_eq!(file.harness_thread_id, "native-child");
        assert_eq!(file.parent_thread_id, None);
        assert_eq!(file.spawned_by_turn_id, None);
        // PersistStore reserves revision zero for legacy files and promotes every new record.
        assert_eq!(file.revision, 1);
        assert_eq!(
            file.version,
            giskard_persist::store::THREAD_METADATA_VERSION
        );
        assert_eq!(file.context_window, 0);
        assert!(file.model_context_windows.is_empty());
        assert_eq!(file.permission_preset, PermissionPreset::AskFirst);
        assert!(file.model_efforts.is_empty());
        assert_eq!(file.tokens, TokenLedger::default());
        assert_eq!(file.created_at, file.updated_at);
        assert!(!file.archived);
        assert!(file.git_workspace.is_none());
        wait_for_coordinator(&registry, thread).await;

        let turn = TurnId::new();
        harness.append_event(thread, AgentEvent::TurnStarted { thread, turn });
        harness.append_event(
            thread,
            AgentEvent::ItemCompleted {
                thread,
                turn,
                item: Item {
                    id: ItemId::new(),
                    harness_item_id: "native-agent-message".into(),
                    payload: ItemPayload::AgentMessage {
                        text: "hello".into(),
                    },
                    created_at: Utc::now(),
                },
            },
        );
        harness.append_event(
            thread,
            AgentEvent::TurnCompleted {
                thread,
                turn,
                usage: TokenUsage::default(),
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
            },
        );
        let turns = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let turns = store.load_all_turns(project, thread).await.unwrap();
                if turns.len() == 1 {
                    return turns;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("discovered thread turn was not persisted");
        assert_eq!(turns[0].id, turn);
        assert_eq!(
            turns[0].user_input,
            UserInput::text("Unclassified native turn")
        );
        registry.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn link_after_discovery_classifies_the_same_thread() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project, config) = create_test_project(&store, "link-after-discovery").await;
        let parent = ThreadId::new();
        store
            .save_thread(
                project,
                &test_thread_file(
                    project,
                    parent,
                    "native-parent",
                    giskard_core::ThreadKind::Primary,
                ),
            )
            .await
            .unwrap();
        let (registry, factory) = discovery_registry(store.clone()).await;
        registry
            .get_or_create_harness(project, &config)
            .await
            .unwrap();
        let harness = factory.harness.lock().unwrap().clone().unwrap();
        let child = ThreadId::new();
        harness.announce(ThreadDiscovered {
            thread: child,
            harness_thread_id: "native-child".into(),
            parent_harness_thread_id: None,
        });
        wait_for_thread(&store, project, child).await;
        let before = wait_for_coordinator(&registry, child).await;
        let spawned_by = TurnId::new();
        let result = super::materialize_subagent_thread(
            parent,
            project,
            spawned_by,
            super::SubagentActivityInfo {
                native_thread_id: "native-child".into(),
                agent_name: None,
                agent_path: None,
                title: None,
            },
            registry.shared.clone(),
        )
        .await
        .unwrap();
        assert_eq!(result, Some(child));
        let file = store.load_thread(project, child).await.unwrap().unwrap();
        assert_eq!(file.kind, giskard_core::ThreadKind::Subagent);
        assert_eq!(file.parent_thread_id, Some(parent));
        assert!(Arc::ptr_eq(
            &before,
            &registry.shared.coordinator(child).await.unwrap()
        ));
        registry.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn discovery_after_link_reuses_the_existing_thread() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project, config) = create_test_project(&store, "discovery-after-link").await;
        let parent = ThreadId::new();
        store
            .save_thread(
                project,
                &test_thread_file(
                    project,
                    parent,
                    "native-parent",
                    giskard_core::ThreadKind::Primary,
                ),
            )
            .await
            .unwrap();
        let (registry, factory) = discovery_registry(store.clone()).await;
        registry
            .get_or_create_harness(project, &config)
            .await
            .unwrap();
        let child = super::materialize_subagent_thread(
            parent,
            project,
            TurnId::new(),
            super::SubagentActivityInfo {
                native_thread_id: "native-child".into(),
                agent_name: None,
                agent_path: None,
                title: None,
            },
            registry.shared.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let harness = factory.harness.lock().unwrap().clone().unwrap();
        let rejected = ThreadId::new();
        harness.announce(ThreadDiscovered {
            thread: rejected,
            harness_thread_id: "native-child".into(),
            parent_harness_thread_id: Some("native-parent".into()),
        });
        wait_for_discovery_records(&registry, 1).await;
        assert!(
            store
                .load_thread(project, rejected)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            super::load_thread_graph(&store, project)
                .await
                .unwrap()
                .len(),
            2
        );
        assert!(registry.shared.coordinator(child).await.is_some());
        registry.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn discovery_for_a_primary_is_ignored() {
        let output = Arc::new(StdMutex::new(Vec::new()));
        let writer_output = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(move || CapturedLogWriter(writer_output.clone()))
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project, config) = create_test_project(&store, "primary-discovery").await;
        let primary = ThreadId::new();
        store
            .save_thread(
                project,
                &test_thread_file(
                    project,
                    primary,
                    "native-primary",
                    giskard_core::ThreadKind::Primary,
                ),
            )
            .await
            .unwrap();
        let (registry, factory) = discovery_registry(store.clone()).await;
        registry
            .get_or_create_harness(project, &config)
            .await
            .unwrap();
        let harness = factory.harness.lock().unwrap().clone().unwrap();
        let minted = ThreadId::new();
        harness.announce(ThreadDiscovered {
            thread: minted,
            harness_thread_id: "native-primary".into(),
            parent_harness_thread_id: None,
        });
        wait_for_discovery_records(&registry, 1).await;
        assert!(store.load_thread(project, minted).await.unwrap().is_none());
        assert!(registry.shared.coordinator(minted).await.is_none());
        let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(
            logs.contains("ignoring traffic discovery for an already persisted primary thread")
        );
        registry.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn discovery_consumer_survives_a_failed_record() {
        let output = Arc::new(StdMutex::new(Vec::new()));
        let writer_output = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(move || CapturedLogWriter(writer_output.clone()))
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project, config) = create_test_project(&store, "discovery-recovery").await;
        let (registry, factory) = discovery_registry(store.clone()).await;
        registry
            .get_or_create_harness(project, &config)
            .await
            .unwrap();
        let harness = factory.harness.lock().unwrap().clone().unwrap();

        store.delete_project(project).await.unwrap();
        let dropped = ThreadId::new();
        harness.announce(ThreadDiscovered {
            thread: dropped,
            harness_thread_id: "native-dropped".into(),
            parent_harness_thread_id: None,
        });
        wait_for_discovery_records(&registry, 1).await;
        assert!(store.load_thread(project, dropped).await.unwrap().is_none());
        assert!(
            String::from_utf8(output.lock().unwrap().clone())
                .unwrap()
                .contains(
                    "project disappeared before a discovered native thread could be admitted"
                )
        );

        store
            .create_project(project, "discovery-recovery", "/tmp/test")
            .await
            .unwrap();
        let admitted = ThreadId::new();
        harness.announce(ThreadDiscovered {
            thread: admitted,
            harness_thread_id: "native-admitted".into(),
            parent_harness_thread_id: None,
        });
        wait_for_discovery_records(&registry, 2).await;
        let file = wait_for_thread(&store, project, admitted).await;
        assert_eq!(file.kind, giskard_core::ThreadKind::Orphan);
        wait_for_coordinator(&registry, admitted).await;
        assert!(store.load_thread(project, dropped).await.unwrap().is_none());
        registry.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn discovery_consumer_stops_on_registry_shutdown() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let (project, config) = create_test_project(&store, "discovery-shutdown").await;
        let (registry, _) = discovery_registry(store).await;
        registry
            .get_or_create_harness(project, &config)
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), registry.shutdown())
            .await
            .expect("registry shutdown timed out")
            .unwrap();
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
            .open_thread(
                &config,
                "/tmp/test",
                thread_id,
                None,
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
            transitions.project(&successful).await.publish_active(
                Arc::new(ShutdownHarness {
                    calls: successful_calls.clone(),
                    fail: false,
                }),
                super::DriverHandle::disconnected(),
            );
        }
        let failing = registry
            .shared
            .intern_project_authority(ProjectId::new())
            .await;
        {
            let mut transitions = registry.shared.harness_transitions.lock().await;
            transitions.project(&failing).await.publish_active(
                Arc::new(ShutdownHarness {
                    calls: failing_calls.clone(),
                    fail: true,
                }),
                super::DriverHandle::disconnected(),
            );
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
            slot.publish_active(harness, super::DriverHandle::disconnected());
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
                .publish_active(harness.clone(), super::DriverHandle::disconnected());
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
        let authority = install_test_coordinator(&registry.shared, first.clone()).await;

        let _ = first
            .owner_exited(super::ForwarderExitReason::StreamEndedWithoutTurn)
            .await;

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
