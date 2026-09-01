use std::sync::{Arc, Weak};

use giskard_core::error::HarnessError;
use giskard_core::ids::ProjectId;
use giskard_core::model::ModelDescriptor;
use giskard_harness::AgentHarness;
use tokio::sync::{Mutex, MutexGuard, OwnedMutexGuard, RwLock};

/// Role-specific handle for serializing one project's lifecycle operations.
#[derive(Clone)]
pub(super) struct LifecycleLock(Arc<Mutex<()>>);

impl LifecycleLock {
    /// Creates a lifecycle lock before an authority or weak interner entry exists.
    pub(super) fn new() -> Self {
        Self(Arc::new(Mutex::new(())))
    }

    /// Produces the weak handle retained for unpublished project IDs.
    pub(super) fn downgrade(&self) -> WeakLifecycleLock {
        WeakLifecycleLock(Arc::downgrade(&self.0))
    }

    /// Acquires the lock without exposing its raw mutex identity.
    pub(super) async fn lock_owned(self) -> OwnedMutexGuard<()> {
        self.0.lock_owned().await
    }

    #[cfg(test)]
    pub(super) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Weak lifecycle-lock handle used only while a project authority is unpublished.
pub(super) struct WeakLifecycleLock(Weak<Mutex<()>>);

impl WeakLifecycleLock {
    /// Recovers the exact interned lifecycle lock while a strong owner remains.
    pub(super) fn upgrade(&self) -> Option<LifecycleLock> {
        self.0.upgrade().map(LifecycleLock)
    }

    /// Reports whether pruning may discard this unpublished entry.
    pub(super) fn strong_count(&self) -> usize {
        self.0.strong_count()
    }
}

/// Owns all process-local state associated with one verified project ID.
pub(super) struct ProjectAuthority {
    project_id: ProjectId,
    lifecycle: LifecycleLock,
    harness: ProjectHarnessSlot,
    model_catalog: ProjectModelCatalogSlot,
}

impl ProjectAuthority {
    /// Publishes a project authority around the lifecycle lock adopted from the interner.
    pub(super) fn new(project_id: ProjectId, lifecycle: LifecycleLock) -> Self {
        Self {
            project_id,
            lifecycle,
            harness: ProjectHarnessSlot::default(),
            model_catalog: ProjectModelCatalogSlot::default(),
        }
    }

    /// Returns the authority's immutable project identity.
    pub(super) fn project_id(&self) -> ProjectId {
        self.project_id
    }

    /// Clones the role-specific lifecycle lock for acquisition outside the project index.
    pub(super) fn lifecycle_lock(&self) -> LifecycleLock {
        self.lifecycle.clone()
    }

    /// Returns a cloned catalog while preserving `None` as meaningful absence.
    pub(super) async fn model_catalog(&self) -> Option<Vec<ModelDescriptor>> {
        self.model_catalog.current.read().await.clone()
    }

    /// Atomically replaces the complete discovered model catalog.
    pub(super) async fn replace_model_catalog(&self, models: Vec<ModelDescriptor>) {
        *self.model_catalog.current.write().await = Some(models);
    }

    /// Restores meaningful catalog absence without removing the authority.
    pub(super) async fn clear_model_catalog(&self) {
        *self.model_catalog.current.write().await = None;
    }

    #[cfg(test)]
    pub(super) async fn harness_is_empty(&self) -> bool {
        self.harness.current.lock().await.is_none()
    }
}

/// Independently synchronized whole-catalog storage for one project.
#[derive(Default)]
struct ProjectModelCatalogSlot {
    current: RwLock<Option<Vec<ModelDescriptor>>>,
}

/// Harness storage acquired only through the root transition guard.
#[derive(Default)]
struct ProjectHarnessSlot {
    current: Mutex<Option<ProjectHarnessState>>,
}

/// Whether the installed harness accepts normal use or is being deleted.
enum ProjectHarnessState {
    Active(Arc<dyn AgentHarness>),
    Deleting(Arc<dyn AgentHarness>),
}

/// Root serialization point for harness creation, deletion, and shutdown.
pub(super) struct HarnessTransitions {
    gate: Mutex<HarnessTransitionState>,
}

#[derive(Default)]
struct HarnessTransitionState {
    shutting_down: bool,
}

impl HarnessTransitions {
    /// Creates an open transition gate with no shutdown fence.
    pub(super) fn new() -> Self {
        Self {
            gate: Mutex::new(HarnessTransitionState::default()),
        }
    }

    /// Acquires the root guard that must precede every project harness slot.
    pub(super) async fn lock(&self) -> HarnessTransitionGuard<'_> {
        HarnessTransitionGuard {
            state: self.gate.lock().await,
        }
    }
}

/// Held root harness-transition gate; project guards borrow this guard.
pub(super) struct HarnessTransitionGuard<'a> {
    state: MutexGuard<'a, HarnessTransitionState>,
}

impl<'a> HarnessTransitionGuard<'a> {
    /// Acquires a project's harness slot while retaining the root gate.
    pub(super) async fn project<'guard, 'authority>(
        &'guard mut self,
        authority: &'authority ProjectAuthority,
    ) -> ProjectHarnessGuard<'guard, 'a, 'authority> {
        ProjectHarnessGuard {
            transitions: self,
            project_id: authority.project_id,
            slot: authority.harness.current.lock().await,
        }
    }

    /// Fences future harness creation before shutdown drains project slots.
    pub(super) fn begin_shutdown(&mut self) {
        self.state.shutting_down = true;
    }

    #[cfg(test)]
    pub(super) fn is_shutting_down(&self) -> bool {
        self.state.shutting_down
    }
}

/// Access to one harness slot, structurally nested under the root transition guard.
pub(super) struct ProjectHarnessGuard<'guard, 'transition, 'authority> {
    transitions: &'guard mut HarnessTransitionGuard<'transition>,
    project_id: ProjectId,
    slot: MutexGuard<'authority, Option<ProjectHarnessState>>,
}

impl ProjectHarnessGuard<'_, '_, '_> {
    /// Clones an active harness; empty and deleting slots are not reachable.
    pub(super) fn active(&self) -> Option<Arc<dyn AgentHarness>> {
        match self.slot.as_ref() {
            Some(ProjectHarnessState::Active(harness)) => Some(harness.clone()),
            Some(ProjectHarnessState::Deleting(_)) | None => None,
        }
    }

    /// Returns the incumbent or confirms that creation may proceed under these guards.
    pub(super) fn active_or_creatable(
        &self,
    ) -> Result<Option<Arc<dyn AgentHarness>>, HarnessError> {
        if self.transitions.state.shutting_down {
            return Err(HarnessError::Protocol(
                "server is shutting down; refusing to start a harness".into(),
            ));
        }
        match self.slot.as_ref() {
            Some(ProjectHarnessState::Active(harness)) => Ok(Some(harness.clone())),
            Some(ProjectHarnessState::Deleting(_)) => Err(HarnessError::Protocol(format!(
                "project {} harness is being deleted",
                self.project_id
            ))),
            None => Ok(None),
        }
    }

    /// Publishes a newly created harness into the same slot checked for creation.
    pub(super) fn publish_active(&mut self, harness: Arc<dyn AgentHarness>) {
        *self.slot = Some(ProjectHarnessState::Active(harness));
    }

    /// Marks an active harness deleting and returns it for shutdown outside the guards.
    pub(super) fn begin_delete(&mut self) -> Result<Option<Arc<dyn AgentHarness>>, HarnessError> {
        match self.slot.as_ref() {
            Some(ProjectHarnessState::Active(harness)) => {
                let harness = harness.clone();
                *self.slot = Some(ProjectHarnessState::Deleting(harness.clone()));
                Ok(Some(harness))
            }
            Some(ProjectHarnessState::Deleting(_)) => Err(HarnessError::Protocol(format!(
                "project {} harness deletion is already in progress",
                self.project_id
            ))),
            None => Ok(None),
        }
    }

    /// Restores only the same deleting harness, and never after shutdown begins.
    pub(super) fn rollback_delete_if_running(&mut self, harness: Arc<dyn AgentHarness>) {
        if !self.transitions.state.shutting_down
            && matches!(
                self.slot.as_ref(),
                Some(ProjectHarnessState::Deleting(current)) if Arc::ptr_eq(current, &harness)
            )
        {
            *self.slot = Some(ProjectHarnessState::Active(harness));
        }
    }

    /// Clears only the pointer-identical harness whose deletion completed.
    pub(super) fn finish_delete(&mut self, harness: &Arc<dyn AgentHarness>) {
        if matches!(
            self.slot.as_ref(),
            Some(ProjectHarnessState::Deleting(current)) if Arc::ptr_eq(current, harness)
        ) {
            *self.slot = None;
        }
    }

    /// Drains either harness state while the global shutdown fence is held.
    pub(super) fn take_for_shutdown(&mut self) -> Option<Arc<dyn AgentHarness>> {
        self.slot.take().map(|state| match state {
            ProjectHarnessState::Active(harness) | ProjectHarnessState::Deleting(harness) => {
                harness
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::LifecycleLock;

    #[test]
    fn weak_lifecycle_lock_preserves_identity_and_expires() {
        let lock = LifecycleLock::new();
        let weak = lock.downgrade();
        let upgraded = weak.upgrade().unwrap();
        assert!(lock.ptr_eq(&upgraded));
        drop(lock);
        drop(upgraded);
        assert!(weak.upgrade().is_none());
    }
}
