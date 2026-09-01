//! Registry-owned Primary creation.
//!
//! Each state owns exactly the resources acquired in that phase.  The HTTP caller owns only the
//! result receiver; dropping it cannot cancel this finite operation.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use tokio::sync::OwnedMutexGuard;
use tracing::{debug, warn};

use crate::thread_runtime::RestorePermit;
use giskard_core::error::HarnessError;
use giskard_core::ids::{ThreadId, TurnId};
use giskard_core::model::ModelRef;
use giskard_core::thread::ThreadKind;
use giskard_core::turn::{TurnModel, TurnOverrides};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentHarness, OpenThreadOptions, ThreadAttachment, ThreadHandle, thread_update_channel,
};
use giskard_persist::store::{ProjectConfig, ThreadFile, ThreadGitWorkspace, ThreadWorktree};
use giskard_proto::GitStrategy;

use super::owner::OwnerInstallation;
use super::{
    ClassificationPhase, HarnessRegistry, NewPrimaryThread, PreparedTurnReservation,
    ProjectMaterializationPermit, RegistryTaskPermit, StartedPrimaryThread, ThreadBinding,
    TurnContext, TurnContextKind, lock_thread_owner_after_drain, remove_primary_worktree,
    spawn_thread_update_forwarder,
};

/// Request data before finite-operation admission. It cannot mutate project state.
pub(super) struct Unadmitted {
    request: Request,
}

impl Unadmitted {
    pub(super) fn new(request: Request) -> Self {
        Self { request }
    }

    pub(super) async fn run(
        self,
        registry: &HarnessRegistry,
        operation: RegistryTaskPermit,
    ) -> Result<StartedPrimaryThread, HarnessError> {
        let published = self
            .admit(registry, operation)
            .acquire_permit()
            .await?
            .prepare_workspace()
            .await?
            .open_native()
            .await?
            .persist()
            .await?
            .install_owner()
            .await?
            .prepare_turn()
            .await?
            .accept_turn()
            .await?
            .publish()
            .await;
        Ok(published.result)
    }
}

pub(super) struct Request {
    pub(super) config: ProjectConfig,
    pub(super) project_workspace_root: String,
    pub(super) thread: ThreadId,
    pub(super) initial_model: ModelRef,
    pub(super) metadata: NewPrimaryThread,
    pub(super) git_strategy: GitStrategy,
    pub(super) input: UserInput,
    pub(super) overrides: TurnOverrides,
    #[cfg(test)]
    pub(super) phase_gate: Option<Arc<PhaseGate>>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    WaitingForPermit,
    WorkspaceCreation,
    NativeCommandAdmission,
    NativeResponse,
    MetadataRename,
    OwnerInstallation,
    TurnPreparation,
    StartTurn,
    TurnAccepted,
    Publication,
}

#[cfg(test)]
pub(super) struct PhaseGate {
    target: Phase,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
impl PhaseGate {
    pub(super) fn new(target: Phase) -> Arc<Self> {
        Arc::new(Self {
            target,
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        })
    }

    pub(super) async fn wait_arrived(&self) {
        self.arrived.notified().await;
    }

    pub(super) fn release(&self) {
        self.release.notify_one();
    }

    async fn checkpoint(&self, phase: Phase) {
        if self.target == phase {
            self.arrived.notify_one();
            self.release.notified().await;
        }
    }
}

#[cfg(test)]
async fn checkpoint(request: &Request, phase: Phase) {
    if let Some(gate) = request.phase_gate.as_ref() {
        gate.checkpoint(phase).await;
    }
}

/// The operation has entered registry-owned execution but has not mutated external state.
struct Admitted<'a> {
    registry: &'a HarnessRegistry,
    request: Request,
    _operation: RegistryTaskPermit,
}

/// Project materialization is excluded until this operation publishes or rolls back.
struct PermitHeld<'a> {
    admitted: Admitted<'a>,
    _permit: ProjectMaterializationPermit,
}

/// A worktree may now exist and must be retained until native cleanup is known to have succeeded.
struct WorkspaceReady<'a> {
    permit: PermitHeld<'a>,
    worktree: Option<ThreadWorktree>,
}

/// Codex returned the exact route and its linear receiver, but metadata is not yet durable.
struct Attached<'a> {
    workspace: WorkspaceReady<'a>,
    harness: Arc<dyn AgentHarness>,
    attachment: ThreadAttachment,
    update_stream: giskard_harness::ThreadUpdateStream,
    owner_guard: OwnedMutexGuard<()>,
    restore_permit: RestorePermit,
}

/// Primary classification is durable; the receiver is still recoverably held by the attachment.
struct Durable<'a> {
    attached: Attached<'a>,
    thread_file: ThreadFile,
}

/// Metadata and the long-lived owner are both installed.
struct Live<'a> {
    workspace: WorkspaceReady<'a>,
    harness: Arc<dyn AgentHarness>,
    handle: ThreadHandle,
    thread_file: ThreadFile,
}

/// The exact coordinator operation and runtime turn lease are reserved, but Codex has not yet
/// accepted the request. Dropping the HTTP waiter cannot drop this state because it is task-owned.
struct TurnPrepared<'a> {
    live: Live<'a>,
    coordinator: ThreadBinding,
    operation: PreparedTurnReservation,
    harness: Arc<dyn AgentHarness>,
    _task_permit: RegistryTaskPermit,
    request_started: Instant,
}

/// Provider acceptance is the point after which rollback must never delete the Primary.
struct TurnAccepted<'a> {
    live: Live<'a>,
    turn_id: TurnId,
}

struct Published {
    result: StartedPrimaryThread,
}

/// Terminal state documenting that durable recovery resources intentionally remain visible.
#[must_use]
struct RetainedDegraded<'a> {
    _workspace: WorkspaceReady<'a>,
    _thread_file: Option<ThreadFile>,
    error: HarnessError,
}

/// Terminal state documenting that every removable resource was successfully rolled back.
#[must_use]
struct RolledBack<'a> {
    _workspace: WorkspaceReady<'a>,
    error: HarnessError,
}

impl<'a> RetainedDegraded<'a> {
    fn finish<T>(
        workspace: WorkspaceReady<'a>,
        thread_file: Option<ThreadFile>,
        error: HarnessError,
    ) -> Result<T, HarnessError> {
        let terminal = Self {
            _workspace: workspace,
            _thread_file: thread_file,
            error,
        };
        Err(terminal.error)
    }
}

impl<'a> RolledBack<'a> {
    fn finish<T>(workspace: WorkspaceReady<'a>, error: HarnessError) -> Result<T, HarnessError> {
        let terminal = Self {
            _workspace: workspace,
            error,
        };
        Err(terminal.error)
    }
}

impl Unadmitted {
    fn admit<'a>(
        self,
        registry: &'a HarnessRegistry,
        operation: RegistryTaskPermit,
    ) -> Admitted<'a> {
        Admitted {
            registry,
            request: self.request,
            _operation: operation,
        }
    }
}

impl<'a> Admitted<'a> {
    async fn acquire_permit(mut self) -> Result<PermitHeld<'a>, HarnessError> {
        #[cfg(test)]
        checkpoint(&self.request, Phase::WaitingForPermit).await;
        let permit = self
            .registry
            .lock_project_lifecycle(self.request.config.id)
            .await;
        let project_id = self.request.config.id;
        let reloaded = self
            .registry
            .shared
            .store
            .load_project(project_id)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?
            .ok_or_else(|| HarnessError::Protocol(format!("project {project_id} disappeared")))?;
        let app_config = self
            .registry
            .shared
            .store
            .load_config()
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?;
        let catalog = self
            .registry
            .project_model_catalog(&reloaded)
            .await
            .unwrap_or_default();
        let mut model =
            crate::models::normalize_model_ref(&app_config, &catalog, &self.request.initial_model);
        let descriptor = crate::models::resolve_catalog_descriptor(&catalog, &app_config, &model);
        if !descriptor.supports_reasoning_effort {
            model.reasoning_effort = None;
        }
        self.request.config = reloaded;
        self.request.project_workspace_root = self
            .request
            .config
            .workspace_root
            .as_deref()
            .unwrap_or(&self.request.config.dir)
            .to_owned();
        self.request.initial_model = model.clone();
        self.request.metadata.context_window = descriptor.context_window;
        self.request.overrides.model = Some(model);
        Ok(PermitHeld {
            admitted: self,
            _permit: permit,
        })
    }
}

impl<'a> PermitHeld<'a> {
    async fn prepare_workspace(self) -> Result<WorkspaceReady<'a>, HarnessError> {
        let request = &self.admitted.request;
        let worktree = match request.git_strategy {
            GitStrategy::Shared => None,
            GitStrategy::Worktree => {
                let path = crate::worktree::worktree_path(
                    self.admitted.registry.shared.store.data_dir(),
                    &request.config.id.to_string(),
                    request.thread,
                );
                let branch = crate::worktree::branch_name(request.thread);
                Some(
                    crate::worktree::create(
                        Path::new(&request.project_workspace_root),
                        &path,
                        &branch,
                    )
                    .await
                    .map_err(|error| match error {
                        crate::worktree::WorktreeError::Unavailable(message) => {
                            HarnessError::Timeout(message)
                        }
                        other => HarnessError::Unsupported(other.to_string()),
                    })?,
                )
            }
        };
        #[cfg(test)]
        checkpoint(&self.admitted.request, Phase::WorkspaceCreation).await;
        Ok(WorkspaceReady {
            permit: self,
            worktree,
        })
    }
}

impl<'a> WorkspaceReady<'a> {
    async fn open_native(mut self) -> Result<Attached<'a>, HarnessError> {
        let request = &mut self.permit.admitted.request;
        request.metadata.git_workspace = self.worktree.clone().map(ThreadGitWorkspace::Worktree);
        let workspace_root = self
            .worktree
            .as_ref()
            .map(ThreadWorktree::workspace_root)
            .unwrap_or(&request.project_workspace_root)
            .to_owned();
        let owner_guard =
            lock_thread_owner_after_drain(&self.permit.admitted.registry.shared, request.thread)
                .await;
        if self
            .permit
            .admitted
            .registry
            .shared
            .coordinator(request.thread)
            .await
            .is_some()
        {
            let error = HarnessError::Protocol(format!(
                "new Primary thread {} already has an event owner",
                request.thread
            ));
            return Err(with_worktree_cleanup(
                error,
                remove_primary_worktree(self.worktree.as_ref(), request.thread, "open_thread")
                    .await,
            ));
        }
        let harness = match self
            .permit
            .admitted
            .registry
            .get_or_create_harness(request.config.id, &request.config)
            .await
        {
            Ok(harness) => harness,
            Err(error) => {
                return Err(with_worktree_cleanup(
                    error,
                    remove_primary_worktree(
                        self.worktree.as_ref(),
                        request.thread,
                        "create_harness",
                    )
                    .await,
                ));
            }
        };
        let authority = match self
            .permit
            .admitted
            .registry
            .shared
            .intern_thread_authority(request.thread, request.config.id)
            .await
        {
            Ok(authority) => authority,
            Err(error) => {
                return Err(with_worktree_cleanup(
                    HarnessError::Protocol(error.to_string()),
                    remove_primary_worktree(
                        self.worktree.as_ref(),
                        request.thread,
                        "intern_thread_authority",
                    )
                    .await,
                ));
            }
        };
        let restore_permit = self
            .permit
            .admitted
            .registry
            .shared
            .runtime
            .restoration_permit(&authority);
        let (updates, update_stream) = thread_update_channel();
        #[cfg(test)]
        checkpoint(request, Phase::NativeCommandAdmission).await;
        let attachment = match harness
            .open_thread(OpenThreadOptions {
                project: request.config.id,
                thread: request.thread,
                workspace_root: workspace_root.into(),
                resume: None,
                initial_model: request.initial_model.clone(),
                updates,
            })
            .await
        {
            Ok(attachment) => attachment,
            Err(error) => {
                return Err(with_worktree_cleanup(
                    error,
                    remove_primary_worktree(self.worktree.as_ref(), request.thread, "open_thread")
                        .await,
                ));
            }
        };
        #[cfg(test)]
        checkpoint(request, Phase::NativeResponse).await;
        if attachment.handle().thread != request.thread {
            let handle = attachment.handle().clone();
            let cleanup = retire_unpublished_route(&*harness, &handle).await;
            let error = HarnessError::Protocol(format!(
                "harness opened thread {} instead of requested thread {}",
                handle.thread, request.thread
            ));
            if matches!(cleanup, Ok(giskard_harness::ThreadDeletion::Retired)) {
                return Err(with_worktree_cleanup(
                    error,
                    remove_primary_worktree(self.worktree.as_ref(), request.thread, "open_thread")
                        .await,
                ));
            }
            return Err(retained_with_location(
                error,
                cleanup,
                request.thread,
                self.worktree.as_ref(),
            ));
        }
        Ok(Attached {
            workspace: self,
            harness,
            attachment,
            update_stream,
            owner_guard,
            restore_permit,
        })
    }
}

impl<'a> Attached<'a> {
    async fn persist(self) -> Result<Durable<'a>, HarnessError> {
        let request = &self.workspace.permit.admitted.request;
        let handle = self.attachment.handle();
        let now = Utc::now();
        let candidate = ThreadFile {
            revision: 0,
            version: giskard_persist::store::THREAD_METADATA_VERSION,
            id: request.thread,
            project_id: request.config.id,
            title: request.metadata.title.clone(),
            harness_thread_id: handle.harness_thread_id.clone(),
            parent_thread_id: None,
            spawned_by_turn_id: None,
            kind: ThreadKind::Primary,
            mode: request.metadata.mode,
            current_model: TurnModel::Known(request.initial_model.clone()),
            context_window: request.metadata.context_window,
            model_context_windows: HashMap::new(),
            permission_preset: request.metadata.permission_preset,
            model_efforts: HashMap::new(),
            tokens: giskard_core::token::TokenLedger::default(),
            created_at: now,
            updated_at: now,
            archived: false,
            git_workspace: request.metadata.git_workspace.clone(),
        };
        let thread_file = match create_metadata(
            &self.workspace.permit.admitted.registry.shared,
            request.config.id,
            candidate,
        )
        .await
        {
            Ok(thread_file) => thread_file,
            Err(create_error) => {
                let reloaded = match self
                    .workspace
                    .permit
                    .admitted
                    .registry
                    .shared
                    .store
                    .load_thread(request.config.id, request.thread)
                    .await
                {
                    Ok(reloaded) => reloaded,
                    Err(reload_error) => {
                        // A failed reload cannot prove that the atomic metadata rename did not
                        // commit. Deleting the native route or worktree here could strand a
                        // durable Primary that becomes visible on the next successful read.
                        // Dropping `self` restores the attachment to its active route; retain the
                        // worktree as the only safe recovery state.
                        let terminal_error = with_recovery_location(
                            HarnessError::Protocol(format!(
                                "Primary metadata create failed ({create_error}); commit state \
                                 could not be resolved ({reload_error}); retained native route \
                                 and worktree for recovery"
                            )),
                            self.workspace.worktree.as_ref(),
                        );
                        let Attached { workspace, .. } = self;
                        return RetainedDegraded::finish(workspace, None, terminal_error);
                    }
                };
                match reloaded {
                    Some(thread_file)
                        if thread_file.id == request.thread
                            && thread_file.project_id == request.config.id
                            && thread_file.kind == ThreadKind::Primary
                            && thread_file.harness_thread_id == handle.harness_thread_id =>
                    {
                        warn!(
                            project_id = %request.config.id,
                            thread_id = %request.thread,
                            error = %create_error,
                            "Primary metadata create reported failure after committing"
                        );
                        thread_file
                    }
                    durable => {
                        let detail = if durable.is_some() {
                            "conflicting durable metadata"
                        } else {
                            "no durable metadata"
                        };
                        return self
                            .rollback_before_durable(HarnessError::Protocol(format!(
                                "Primary metadata create failed ({create_error}); reload found \
                                 {detail}"
                            )))
                            .await;
                    }
                }
            }
        };
        #[cfg(test)]
        checkpoint(request, Phase::MetadataRename).await;
        Ok(Durable {
            attached: self,
            thread_file,
        })
    }

    async fn rollback_before_durable<T>(self, error: HarnessError) -> Result<T, HarnessError> {
        let request = &self.workspace.permit.admitted.request;
        let deletion = retire_unpublished_route(&*self.harness, self.attachment.handle()).await;
        match deletion {
            Ok(giskard_harness::ThreadDeletion::Retired) => {
                return Err(with_worktree_cleanup(
                    error,
                    remove_primary_worktree(
                        self.workspace.worktree.as_ref(),
                        request.thread,
                        "persist_primary",
                    )
                    .await,
                ));
            }
            Ok(giskard_harness::ThreadDeletion::RetiredWithProviderError(cleanup_error)) => {
                warn!(
                    project_id = %request.config.id,
                    thread_id = %request.thread,
                    error = %cleanup_error,
                    "native cleanup failed; retaining Primary worktree for operator recovery"
                );
                Err(with_recovery_location(
                    HarnessError::Protocol(format!(
                        "{error}; Primary {} route was invalidated, but provider cleanup failed: \
                         {cleanup_error}",
                        request.thread
                    )),
                    self.workspace.worktree.as_ref(),
                ))
            }
            Err(cleanup_error) => {
                warn!(
                    project_id = %request.config.id,
                    thread_id = %request.thread,
                    error = %cleanup_error,
                    "native cleanup was rejected; retaining active Primary route and worktree"
                );
                Err(with_recovery_location(
                    HarnessError::Protocol(format!(
                        "{error}; Primary {} active native route was retained because cleanup was \
                         rejected before route invalidation: {cleanup_error}",
                        request.thread
                    )),
                    self.workspace.worktree.as_ref(),
                ))
            }
        }
    }
}

/// No coordinator has been published on these rollback paths, so route retirement can proceed
/// directly to provider cleanup without a server owner-retirement midpoint.
async fn retire_unpublished_route(
    harness: &dyn AgentHarness,
    handle: &ThreadHandle,
) -> Result<giskard_harness::ThreadDeletion, HarnessError> {
    harness.begin_delete_thread(handle).await?.finish().await
}

async fn create_metadata(
    shared: &super::RegistryShared,
    project_id: giskard_core::ids::ProjectId,
    candidate: ThreadFile,
) -> Result<ThreadFile, giskard_core::PersistError> {
    #[cfg(test)]
    if shared
        .primary_create_committed_error
        .swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        shared.thread_metadata.create(project_id, candidate).await?;
        return Err(giskard_core::PersistError::Io(
            "injected error after Primary metadata commit".into(),
        ));
    }
    shared.thread_metadata.create(project_id, candidate).await
}

async fn delete_metadata(
    shared: &super::RegistryShared,
    project_id: giskard_core::ids::ProjectId,
    thread_id: ThreadId,
) -> Result<(), giskard_core::PersistError> {
    #[cfg(test)]
    if shared
        .primary_delete_error
        .swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        return Err(giskard_core::PersistError::Io(
            "injected Primary metadata delete failure".into(),
        ));
    }
    shared.thread_metadata.delete(project_id, thread_id).await
}

impl<'a> Durable<'a> {
    async fn install_owner(self) -> Result<Live<'a>, HarnessError> {
        let request = &self.attached.workspace.permit.admitted.request;
        let project_id = request.config.id;
        let thread_id = request.thread;
        let initial_model = request.initial_model.clone();
        let handle = self.attached.attachment.handle().clone();
        let native_model = handle.resumed_model.clone().unwrap_or(initial_model);
        let registry = self.attached.workspace.permit.admitted.registry;
        let Durable {
            attached,
            thread_file,
        } = self;
        let Attached {
            workspace,
            harness,
            attachment,
            update_stream,
            owner_guard,
            restore_permit,
        } = attached;
        let installation = match OwnerInstallation::prepare(
            &registry.shared,
            owner_guard,
            attachment,
            project_id,
            Some(native_model),
            ClassificationPhase::Primary,
        )
        .await
        {
            Ok(installation) => installation,
            Err(error) => {
                return rollback_after_durable(workspace, harness, handle, thread_file, error)
                    .await;
            }
        };
        #[cfg(test)]
        checkpoint(&workspace.permit.admitted.request, Phase::OwnerInstallation).await;
        if let Err(error) = installation.commit() {
            return rollback_after_durable(workspace, harness, handle, thread_file, error).await;
        }
        drop(spawn_thread_update_forwarder(
            registry.shared.clone(),
            project_id,
            thread_id,
            update_stream,
            restore_permit,
        ));
        Ok(Live {
            workspace,
            harness,
            handle,
            thread_file,
        })
    }
}

async fn rollback_after_durable<T>(
    workspace: WorkspaceReady<'_>,
    harness: Arc<dyn AgentHarness>,
    handle: ThreadHandle,
    thread_file: ThreadFile,
    error: HarnessError,
) -> Result<T, HarnessError> {
    let request = &workspace.permit.admitted.request;
    let deletion = retire_unpublished_route(&*harness, &handle).await;
    match deletion {
        Err(cleanup_error) => {
            // Plain `Err` is explicitly pre-invalidation: the attachment drop restored the
            // receiver to the still-active route. Preserve the durable recovery record and do not
            // pretend that route retirement committed.
            workspace
                .permit
                .admitted
                .registry
                .shared
                .thread_metadata
                .publish_created(request.config.id, &thread_file)
                .await;
            let terminal_error = with_recovery_location(
                active_route_retained(error, cleanup_error, request.thread),
                workspace.worktree.as_ref(),
            );
            return RetainedDegraded::finish(workspace, Some(thread_file), terminal_error);
        }
        Ok(giskard_harness::ThreadDeletion::RetiredWithProviderError(cleanup_error)) => {
            workspace
                .permit
                .admitted
                .registry
                .shared
                .thread_metadata
                .publish_created(request.config.id, &thread_file)
                .await;
            let terminal_error = with_recovery_location(
                tombstoned_route_retained(error, cleanup_error, request.thread),
                workspace.worktree.as_ref(),
            );
            return RetainedDegraded::finish(workspace, Some(thread_file), terminal_error);
        }
        Ok(giskard_harness::ThreadDeletion::Retired) => {}
    }
    let metadata_deleted = delete_metadata(
        &workspace.permit.admitted.registry.shared,
        request.config.id,
        request.thread,
    )
    .await;
    if let Err(delete_error) = metadata_deleted {
        workspace
            .permit
            .admitted
            .registry
            .shared
            .thread_metadata
            .publish_created(request.config.id, &thread_file)
            .await;
        let terminal_error = with_recovery_location(
            HarnessError::Protocol(format!(
                "{error}; Primary {} was retained because metadata rollback failed: \
                 {delete_error}",
                request.thread
            )),
            workspace.worktree.as_ref(),
        );
        return RetainedDegraded::finish(workspace, Some(thread_file), terminal_error);
    }
    let terminal_error = with_worktree_cleanup(
        error,
        remove_primary_worktree(workspace.worktree.as_ref(), request.thread, "install_owner").await,
    );
    RolledBack::finish(workspace, terminal_error)
}

impl<'a> Live<'a> {
    async fn prepare_turn(self) -> Result<TurnPrepared<'a>, HarnessError> {
        let thread_id = self.workspace.permit.admitted.request.thread;
        let project_id = self.workspace.permit.admitted.request.config.id;
        let input = self.workspace.permit.admitted.request.input.clone();
        let initial_model = self.workspace.permit.admitted.request.initial_model.clone();
        let mode = self.workspace.permit.admitted.request.overrides.mode;
        let coordinator = self
            .workspace
            .permit
            .admitted
            .registry
            .shared
            .coordinator(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let authority = self
            .workspace
            .permit
            .admitted
            .registry
            .shared
            .thread_authority(thread_id)
            .await
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        let context = TurnContext {
            user_input: input,
            model: TurnModel::Known(initial_model),
            mode: giskard_core::turn::TurnMode::Known(mode),
            kind: TurnContextKind::User,
        };
        let operation = match self
            .workspace
            .permit
            .admitted
            .registry
            .shared
            .admit_operation(&authority, &coordinator, project_id, &self.handle, &context)
            .await
        {
            Ok(operation) => operation,
            Err(error) => return self.rollback(error).await,
        };
        let Some(task_permit) = self
            .workspace
            .permit
            .admitted
            .registry
            .shared
            .background_tasks
            .register()
        else {
            self.workspace
                .permit
                .admitted
                .registry
                .shared
                .abort_admitted_operation(&coordinator, &operation)
                .await;
            return self
                .rollback(HarnessError::Protocol(
                    "server is shutting down; refusing to start a turn".into(),
                ))
                .await;
        };
        #[cfg(test)]
        checkpoint(
            &self.workspace.permit.admitted.request,
            Phase::TurnPreparation,
        )
        .await;
        let harness = match self
            .workspace
            .permit
            .admitted
            .registry
            .shared
            .active_harness(project_id)
            .await
        {
            Some(harness) => harness,
            None => {
                self.workspace
                    .permit
                    .admitted
                    .registry
                    .shared
                    .abort_admitted_operation(&coordinator, &operation)
                    .await;
                return self.rollback(HarnessError::ThreadNotFound(thread_id)).await;
            }
        };
        Ok(TurnPrepared {
            live: self,
            coordinator,
            operation,
            harness,
            _task_permit: task_permit,
            request_started: Instant::now(),
        })
    }

    async fn rollback<T>(self, error: HarnessError) -> Result<T, HarnessError> {
        let request = &self.workspace.permit.admitted.request;
        let retirement = self.harness.begin_delete_thread(&self.handle).await;
        let retirement = match retirement {
            Ok(retirement) => retirement,
            Err(cleanup_error) => {
                // Invalidation was rejected, so the existing Live coordinator and its physical owner
                // remain the recovery authority. Retiring it would drop the receiver back onto an
                // active route and allow traffic discovery to reclassify this durable Primary.
                self.workspace
                    .permit
                    .admitted
                    .registry
                    .shared
                    .thread_metadata
                    .publish_created(request.config.id, &self.thread_file)
                    .await;
                let terminal_error = with_recovery_location(
                    active_route_retained(error, cleanup_error, request.thread),
                    self.workspace.worktree.as_ref(),
                );
                return RetainedDegraded::finish(
                    self.workspace,
                    Some(self.thread_file),
                    terminal_error,
                );
            }
        };
        self.workspace
            .permit
            .admitted
            .registry
            .retire_thread(request.thread)
            .await;
        let cleanup_error = match retirement.finish().await {
            Ok(giskard_harness::ThreadDeletion::Retired) => None,
            Ok(giskard_harness::ThreadDeletion::RetiredWithProviderError(cleanup_error))
            | Err(cleanup_error) => Some(cleanup_error),
        };
        if let Some(cleanup_error) = cleanup_error {
            self.workspace
                .permit
                .admitted
                .registry
                .shared
                .thread_metadata
                .publish_created(request.config.id, &self.thread_file)
                .await;
            let terminal_error = with_recovery_location(
                tombstoned_route_retained(error, cleanup_error, request.thread),
                self.workspace.worktree.as_ref(),
            );
            return RetainedDegraded::finish(
                self.workspace,
                Some(self.thread_file),
                terminal_error,
            );
        }
        if let Err(delete_error) = delete_metadata(
            &self.workspace.permit.admitted.registry.shared,
            request.config.id,
            request.thread,
        )
        .await
        {
            self.workspace
                .permit
                .admitted
                .registry
                .shared
                .thread_metadata
                .publish_created(request.config.id, &self.thread_file)
                .await;
            let terminal_error = with_recovery_location(
                HarnessError::Protocol(format!(
                    "{error}; Primary {} was retained because metadata rollback failed: \
                     {delete_error}",
                    request.thread
                )),
                self.workspace.worktree.as_ref(),
            );
            return RetainedDegraded::finish(
                self.workspace,
                Some(self.thread_file),
                terminal_error,
            );
        }
        let terminal_error = with_worktree_cleanup(
            error,
            remove_primary_worktree(
                self.workspace.worktree.as_ref(),
                request.thread,
                "start_turn",
            )
            .await,
        );
        RolledBack::finish(self.workspace, terminal_error)
    }
}

impl<'a> TurnPrepared<'a> {
    async fn accept_turn(self) -> Result<TurnAccepted<'a>, HarnessError> {
        let request = &self.live.workspace.permit.admitted.request;
        #[cfg(test)]
        checkpoint(request, Phase::StartTurn).await;
        let result = self
            .harness
            .start_turn(
                &self.live.handle,
                request.input.clone(),
                request.overrides.clone(),
            )
            .await;
        match result {
            Ok(turn_id) => {
                self.coordinator
                    .acknowledge_operation_turn(&self.operation, turn_id)
                    .await;
                super::publish_runtime_overview(
                    &self.live.workspace.permit.admitted.registry.shared,
                )
                .await;
                #[cfg(test)]
                checkpoint(request, Phase::TurnAccepted).await;
                debug!(
                    project_id = %request.config.id,
                    thread_id = %request.thread,
                    %turn_id,
                    ack_elapsed_ms = self.request_started.elapsed().as_millis(),
                    "harness accepted initial Primary turn"
                );
                Ok(TurnAccepted {
                    live: self.live,
                    turn_id,
                })
            }
            Err(error) => {
                self.live
                    .workspace
                    .permit
                    .admitted
                    .registry
                    .shared
                    .abort_admitted_operation(&self.coordinator, &self.operation)
                    .await;
                self.live.rollback(error).await
            }
        }
    }
}

impl TurnAccepted<'_> {
    async fn publish(self) -> Published {
        let request = &self.live.workspace.permit.admitted.request;
        #[cfg(test)]
        checkpoint(request, Phase::Publication).await;
        self.live
            .workspace
            .permit
            .admitted
            .registry
            .shared
            .thread_metadata
            .publish_created(request.config.id, &self.live.thread_file)
            .await;
        debug!(
            project_id = %request.config.id,
            thread_id = %request.thread,
            "published newly accepted Primary"
        );
        Published {
            result: StartedPrimaryThread {
                handle: self.live.handle,
                turn_id: self.turn_id,
            },
        }
    }
}

fn with_worktree_cleanup(
    original: HarnessError,
    cleanup: Result<(), super::PrimaryWorktreeCleanupError>,
) -> HarnessError {
    match cleanup {
        Ok(()) => original,
        Err(cleanup) => HarnessError::Protocol(format!(
            "{original}; Primary rollback left an orphan checkout: {cleanup}"
        )),
    }
}

fn with_recovery_location(
    original: HarnessError,
    worktree: Option<&ThreadWorktree>,
) -> HarnessError {
    match worktree {
        Some(worktree) => HarnessError::Protocol(format!(
            "{original}; retained checkout {} and branch {}",
            worktree.path, worktree.branch
        )),
        None => original,
    }
}

fn retained_with_location(
    original: HarnessError,
    cleanup: Result<giskard_harness::ThreadDeletion, HarnessError>,
    thread: ThreadId,
    worktree: Option<&ThreadWorktree>,
) -> HarnessError {
    let retained = match cleanup {
        Err(cleanup) => active_route_retained(original, cleanup, thread),
        Ok(giskard_harness::ThreadDeletion::RetiredWithProviderError(cleanup)) => {
            tombstoned_route_retained(original, cleanup, thread)
        }
        Ok(giskard_harness::ThreadDeletion::Retired) => original,
    };
    with_recovery_location(retained, worktree)
}

fn active_route_retained(
    original: HarnessError,
    cleanup: HarnessError,
    thread: ThreadId,
) -> HarnessError {
    HarnessError::Protocol(format!(
        "{original}; Primary {thread} metadata, worktree, and active native route were retained \
         because cleanup was rejected before route invalidation: {cleanup}"
    ))
}

fn tombstoned_route_retained(
    original: HarnessError,
    cleanup: HarnessError,
    thread: ThreadId,
) -> HarnessError {
    HarnessError::Protocol(format!(
        "{original}; Primary {thread} metadata and worktree were retained after route \
         invalidation because provider cleanup failed: {cleanup}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worktree_cleanup_failure_is_returned_with_recovery_coordinates() {
        let error = with_worktree_cleanup(
            HarnessError::Protocol("turn start failed".into()),
            Err(super::super::PrimaryWorktreeCleanupError {
                stage: "delete branch",
                path: "/data/worktrees/thread".into(),
                branch: "giskard/thread".into(),
                source: "branch is checked out".into(),
            }),
        )
        .to_string();

        assert!(error.contains("turn start failed"));
        assert!(error.contains("orphan checkout"));
        assert!(error.contains("/data/worktrees/thread"));
        assert!(error.contains("giskard/thread"));
        assert!(error.contains("branch is checked out"));
    }

    #[test]
    fn pre_invalidation_cleanup_error_reports_active_recovery_state() {
        let thread = ThreadId::new();
        let error = active_route_retained(
            HarnessError::Protocol("creation failed".into()),
            HarnessError::Protocol("identity mismatch".into()),
            thread,
        )
        .to_string();

        assert!(error.contains("active native route were retained"));
        assert!(error.contains("before route invalidation"));
        assert!(error.contains("identity mismatch"));
    }

    #[test]
    fn post_invalidation_cleanup_error_reports_tombstoned_recovery_state() {
        let thread = ThreadId::new();
        let error = tombstoned_route_retained(
            HarnessError::Protocol("creation failed".into()),
            HarnessError::Transport("provider unavailable".into()),
            thread,
        )
        .to_string();

        assert!(error.contains("retained after route invalidation"));
        assert!(error.contains("provider cleanup failed"));
        assert!(error.contains("provider unavailable"));
    }
}
