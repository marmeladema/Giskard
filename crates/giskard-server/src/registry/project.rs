use std::collections::HashMap;
use std::sync::{Arc, Weak};

use giskard_core::error::HarnessError;
use giskard_core::ids::ProjectId;
use giskard_core::model::ModelDescriptor;
use giskard_harness::AgentHarness;
use indexmap::IndexMap;
use tokio::sync::{Mutex, MutexGuard, OwnedMutexGuard, RwLock};

use super::driver::DriverHandle;

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
    /// One installed instance per `[harnesses.<name>]` declaration this project has used.
    ///
    /// Lifetime class: entries are created on first use by `get_or_create_harness`, removed by
    /// `delete_project` and registry `shutdown` through the same transition guard, and never
    /// otherwise. Keyed by declaration name, which is configuration, not entity identity: this is
    /// the authority's own state, not a peer owning map.
    harnesses: Mutex<IndexMap<String, ProjectHarnessState>>,
    /// The composed catalog per declaration, keyed the same way and cleared by
    /// `clear_model_catalog` (project delete) or replaced whole by a refresh.
    model_catalogs: RwLock<HashMap<String, Vec<ModelDescriptor>>>,
}

impl ProjectAuthority {
    /// Publishes a project authority around the lifecycle lock adopted from the interner.
    pub(super) fn new(project_id: ProjectId, lifecycle: LifecycleLock) -> Self {
        Self {
            project_id,
            lifecycle,
            harnesses: Mutex::new(IndexMap::new()),
            model_catalogs: RwLock::new(HashMap::new()),
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

    /// Returns one declaration's cloned catalog while preserving `None` as meaningful absence.
    pub(super) async fn model_catalog(&self, harness: &str) -> Option<Vec<ModelDescriptor>> {
        self.model_catalogs.read().await.get(harness).cloned()
    }

    /// Atomically replaces one declaration's complete discovered model catalog.
    pub(super) async fn replace_model_catalog(&self, harness: &str, models: Vec<ModelDescriptor>) {
        self.model_catalogs
            .write()
            .await
            .insert(harness.to_string(), models);
    }

    /// Restores meaningful catalog absence for every declaration without removing the authority.
    pub(super) async fn clear_model_catalog(&self) {
        self.model_catalogs.write().await.clear();
    }

    #[cfg(test)]
    pub(super) async fn harness_is_empty(&self) -> bool {
        self.harnesses.lock().await.is_empty()
    }
}

/// Whether the installed harness accepts normal use or is being deleted.
enum ProjectHarnessState {
    Active(Arc<dyn AgentHarness>, DriverHandle),
    Deleting(Arc<dyn AgentHarness>, DriverHandle),
}

type HarnessAndDriver = (Arc<dyn AgentHarness>, DriverHandle);

/// One declaration's harness and driver, taken together by a whole-project transition.
pub(super) type NamedHarness = (String, HarnessAndDriver);

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
    /// Acquires one declaration's harness slot of a project while retaining the root gate.
    pub(super) async fn project<'guard, 'authority>(
        &'guard mut self,
        authority: &'authority ProjectAuthority,
        harness: &str,
    ) -> ProjectHarnessGuard<'guard, 'a, 'authority> {
        ProjectHarnessGuard {
            transitions: self,
            project_id: authority.project_id,
            name: harness.to_string(),
            slots: authority.harnesses.lock().await,
        }
    }

    /// Acquires every declaration slot of a project, for the whole-project paths: deletion and
    /// shutdown.
    pub(super) async fn project_all<'guard, 'authority>(
        &'guard mut self,
        authority: &'authority ProjectAuthority,
    ) -> ProjectHarnessesGuard<'guard, 'a, 'authority> {
        ProjectHarnessesGuard {
            _transitions: self,
            project_id: authority.project_id,
            slots: authority.harnesses.lock().await,
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

/// Access to one declaration's harness slot, structurally nested under the root transition guard.
pub(super) struct ProjectHarnessGuard<'guard, 'transition, 'authority> {
    transitions: &'guard mut HarnessTransitionGuard<'transition>,
    project_id: ProjectId,
    name: String,
    slots: MutexGuard<'authority, IndexMap<String, ProjectHarnessState>>,
}

impl ProjectHarnessGuard<'_, '_, '_> {
    fn slot(&self) -> Option<&ProjectHarnessState> {
        self.slots.get(&self.name)
    }

    /// Clones an active harness; empty and deleting slots are not reachable.
    pub(super) fn active(&self) -> Option<Arc<dyn AgentHarness>> {
        match self.slot() {
            Some(ProjectHarnessState::Active(harness, _)) => Some(harness.clone()),
            Some(ProjectHarnessState::Deleting(_, _)) | None => None,
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
        match self.slot() {
            Some(ProjectHarnessState::Active(harness, _)) => Ok(Some(harness.clone())),
            Some(ProjectHarnessState::Deleting(_, _)) => Err(HarnessError::Protocol(format!(
                "project {} harness {} is being deleted",
                self.project_id, self.name
            ))),
            None => Ok(None),
        }
    }

    /// Publishes a newly created harness into the same slot checked for creation.
    pub(super) fn publish_active(&mut self, harness: Arc<dyn AgentHarness>, driver: DriverHandle) {
        self.slots.insert(
            self.name.clone(),
            ProjectHarnessState::Active(harness, driver),
        );
    }

    pub(super) fn driver(&self) -> Option<DriverHandle> {
        match self.slot() {
            Some(ProjectHarnessState::Active(_, driver))
            | Some(ProjectHarnessState::Deleting(_, driver)) => Some(driver.clone()),
            None => None,
        }
    }

    /// Restores only the same deleting harness, and never after shutdown begins.
    pub(super) fn rollback_delete_if_running(&mut self, harness: Arc<dyn AgentHarness>) {
        if self.transitions.state.shutting_down {
            return;
        }
        let Some(state) = self.slots.get_mut(&self.name) else {
            return;
        };
        if let ProjectHarnessState::Deleting(current, driver) = state
            && Arc::ptr_eq(current, &harness)
        {
            let driver = driver.clone();
            *state = ProjectHarnessState::Active(harness, driver);
        }
    }

    /// Clears only the pointer-identical harness whose deletion completed.
    pub(super) fn finish_delete(&mut self, harness: &Arc<dyn AgentHarness>) {
        if matches!(
            self.slot(),
            Some(ProjectHarnessState::Deleting(current, _)) if Arc::ptr_eq(current, harness)
        ) {
            self.slots.shift_remove(&self.name);
        }
    }
}

/// Access to every declaration slot of one project, nested under the root transition guard.
pub(super) struct ProjectHarnessesGuard<'guard, 'transition, 'authority> {
    _transitions: &'guard mut HarnessTransitionGuard<'transition>,
    project_id: ProjectId,
    slots: MutexGuard<'authority, IndexMap<String, ProjectHarnessState>>,
}

impl ProjectHarnessesGuard<'_, '_, '_> {
    /// Marks every active harness of the project deleting and returns them, in declaration-use
    /// order, for shutdown outside the guards. Refuses without changing anything when any slot is
    /// already being deleted, so a second deletion never interleaves with the first.
    pub(super) fn begin_delete_all(&mut self) -> Result<Vec<NamedHarness>, HarnessError> {
        if let Some(name) = self.slots.iter().find_map(|(name, state)| {
            matches!(state, ProjectHarnessState::Deleting(_, _)).then_some(name)
        }) {
            return Err(HarnessError::Protocol(format!(
                "project {} harness {name} deletion is already in progress",
                self.project_id
            )));
        }
        let mut deleting = Vec::with_capacity(self.slots.len());
        for (name, state) in self.slots.iter_mut() {
            if let ProjectHarnessState::Active(harness, driver) = state {
                let harness = harness.clone();
                let driver = driver.clone();
                *state = ProjectHarnessState::Deleting(harness.clone(), driver.clone());
                deleting.push((name.clone(), (harness, driver)));
            }
        }
        Ok(deleting)
    }

    /// Drains every harness state of the project while the global shutdown fence is held.
    pub(super) fn take_all_for_shutdown(&mut self) -> Vec<NamedHarness> {
        self.slots
            .drain(..)
            .map(|(name, state)| match state {
                ProjectHarnessState::Active(harness, driver)
                | ProjectHarnessState::Deleting(harness, driver) => (name, (harness, driver)),
            })
            .collect()
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
