use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};

use giskard_core::error::HarnessError;
use giskard_core::ids::{ProjectId, ThreadId, TurnId};
use giskard_core::thread::ThreadKind;
use giskard_harness::ThreadHandle;
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard, watch};

use super::{
    LoadedThreadBinding, RegistryTaskPermit, RegistryTaskTracker, SubagentMaterializationJob,
    TurnContext,
};
use crate::thread_runtime::{ThreadRuntimeEntry, ThreadRuntimeSlot, ThreadTurnLease};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CoordinatorToken {
    generation: u64,
    sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ClassificationPhase {
    Primary,
    Subagent,
    Orphan,
}

impl From<ThreadKind> for ClassificationPhase {
    fn from(kind: ThreadKind) -> Self {
        match kind {
            ThreadKind::Primary => Self::Primary,
            ThreadKind::Subagent => Self::Subagent,
            ThreadKind::Orphan => Self::Orphan,
        }
    }
}

#[derive(Clone)]
pub(super) struct EventOwnerControl {
    pub(super) cancel: watch::Sender<bool>,
    pub(super) completed: watch::Receiver<bool>,
}

enum OwnerPhase {
    Installing,
    Live(EventOwnerControl),
    Draining(EventOwnerControl),
    Retired,
    Failed(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NativeActivity {
    Unknown,
    Idle,
    Active,
    Unloaded,
}

struct PreparedOperation {
    pub(super) token: CoordinatorToken,
    pub(super) context: TurnContext,
    turn_gate: ThreadTurnLease,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NativeTurnOrigin {
    Prepared(CoordinatorToken),
    External,
}

struct OwnedNativeTurn {
    pub(super) token: CoordinatorToken,
    turn_id: TurnId,
    origin: NativeTurnOrigin,
    pub(super) context: TurnContext,
    turn_gate: Option<ThreadTurnLease>,
}

pub(super) struct ClaimedNativeTurn {
    pub(super) token: CoordinatorToken,
    pub(super) context: TurnContext,
    pub(super) external: bool,
}

struct ThreadCoordinatorState {
    generation: u64,
    next_sequence: u64,
    binding: LoadedThreadBinding,
    classification: ClassificationPhase,
    owner: OwnerPhase,
    operation: Option<PreparedOperation>,
    native_turn: Option<OwnedNativeTurn>,
    native_activity: NativeActivity,
}

pub(super) struct ThreadCoordinator {
    state: AsyncMutex<ThreadCoordinatorState>,
    changed: Notify,
}

pub(super) type ThreadBinding = Arc<ThreadCoordinator>;
impl ThreadCoordinatorState {
    fn token(&mut self) -> CoordinatorToken {
        self.next_sequence = self.next_sequence.wrapping_add(1);
        CoordinatorToken {
            generation: self.generation,
            sequence: self.next_sequence,
        }
    }

    fn token_is_current(&self, token: CoordinatorToken) -> bool {
        self.generation == token.generation
    }
}

impl ThreadCoordinator {
    pub(super) fn new(binding: LoadedThreadBinding, classification: ClassificationPhase) -> Self {
        Self {
            state: AsyncMutex::new(ThreadCoordinatorState {
                generation: 1,
                next_sequence: 0,
                binding,
                classification,
                owner: OwnerPhase::Installing,
                operation: None,
                native_turn: None,
                native_activity: NativeActivity::Unknown,
            }),
            changed: Notify::new(),
        }
    }

    pub(super) async fn binding(&self) -> LoadedThreadBinding {
        self.state.lock().await.binding.clone()
    }

    pub(super) async fn classification(&self) -> ClassificationPhase {
        self.state.lock().await.classification
    }

    pub(super) async fn classify_orphan_as_subagent(&self) -> Result<(), HarnessError> {
        let mut state = self.state.lock().await;
        match state.classification {
            ClassificationPhase::Orphan => {
                state.classification = ClassificationPhase::Subagent;
                Ok(())
            }
            ClassificationPhase::Subagent => Ok(()),
            ClassificationPhase::Primary => Err(HarnessError::Protocol(format!(
                "primary thread {} cannot be classified as a sub-agent",
                state.binding.handle.thread
            ))),
        }
    }

    pub(super) async fn activate_owner(
        &self,
        control: EventOwnerControl,
    ) -> Result<(), HarnessError> {
        let mut state = self.state.lock().await;
        if !matches!(state.owner, OwnerPhase::Installing) {
            return Err(HarnessError::Protocol(format!(
                "thread {} event owner was installed more than once",
                state.binding.handle.thread
            )));
        }
        state.owner = OwnerPhase::Live(control);
        state.native_activity = NativeActivity::Idle;
        Ok(())
    }

    pub(super) async fn prepare_operation(
        &self,
        context: TurnContext,
        turn_gate: ThreadTurnLease,
    ) -> Result<CoordinatorToken, (HarnessError, ThreadTurnLease)> {
        let mut state = self.state.lock().await;
        if state.classification != ClassificationPhase::Primary {
            return Err((
                HarnessError::ThreadReadOnly {
                    thread: state.binding.handle.thread,
                },
                turn_gate,
            ));
        }
        if let OwnerPhase::Failed(reason) = &state.owner {
            return Err((
                HarnessError::Protocol(format!(
                    "thread {} event owner failed: {reason}",
                    state.binding.handle.thread
                )),
                turn_gate,
            ));
        }
        if !matches!(state.owner, OwnerPhase::Live(_)) {
            return Err((
                HarnessError::Protocol(format!(
                    "thread {} has no live event owner",
                    state.binding.handle.thread
                )),
                turn_gate,
            ));
        }
        if state.operation.is_some() || state.native_turn.is_some() {
            return Err((
                HarnessError::ThreadBusy {
                    thread: state.binding.handle.thread,
                },
                turn_gate,
            ));
        }
        let token = state.token();
        state.operation = Some(PreparedOperation {
            token,
            context,
            turn_gate,
        });
        Ok(token)
    }

    pub(super) async fn abort_operation(&self, token: CoordinatorToken) -> Option<ThreadTurnLease> {
        let mut state = self.state.lock().await;
        if state.operation.as_ref()?.token != token {
            return None;
        }
        let operation = state.operation.take()?;
        drop(state);
        self.changed.notify_waiters();
        Some(operation.turn_gate)
    }

    /// Clear an operation that never reached a native turn. The event owner calls this when its
    /// stream exits, so both gate-less admission and an installed runtime lease have one terminal
    /// cleanup path.
    pub(super) async fn take_unclaimed_operation(&self) -> Option<ThreadTurnLease> {
        let mut state = self.state.lock().await;
        let operation = state.operation.take();
        if operation.is_some() {
            drop(state);
            self.changed.notify_waiters();
        }
        operation.map(|operation| operation.turn_gate)
    }

    pub(super) async fn reusable_handle(
        &self,
        project: ProjectId,
        thread_id: ThreadId,
        native_thread_id: Option<&str>,
        classification: ClassificationPhase,
    ) -> Result<ThreadHandle, HarnessError> {
        let state = self.state.lock().await;
        if state.binding.project_id != project
            || state.binding.handle.thread != thread_id
            || native_thread_id.is_some_and(|native_id| {
                native_id != state.binding.handle.harness_thread_id.as_str()
            })
            || state.classification != classification
        {
            return Err(HarnessError::Protocol(format!(
                "thread {} already has an incompatible event owner",
                thread_id
            )));
        }
        match &state.owner {
            OwnerPhase::Live(_) => Ok(state.binding.handle.clone()),
            OwnerPhase::Failed(reason) => Err(HarnessError::Protocol(format!(
                "thread {} event owner failed: {reason}",
                thread_id
            ))),
            OwnerPhase::Installing | OwnerPhase::Draining(_) | OwnerPhase::Retired => Err(
                HarnessError::Protocol(format!("thread {} event owner is not reusable", thread_id)),
            ),
        }
    }

    pub(super) async fn acknowledge_operation_turn(
        &self,
        token: CoordinatorToken,
        turn_id: TurnId,
    ) {
        let mut state = self.state.lock().await;
        if let Some(operation) = state
            .operation
            .as_mut()
            .filter(|operation| operation.token == token)
        {
            let _ = operation.turn_gate.acknowledge_turn(turn_id);
            return;
        }
        if let Some(native_turn) = state.native_turn.as_mut()
            && native_turn.origin == NativeTurnOrigin::Prepared(token)
            && let Some(turn_gate) = native_turn.turn_gate.as_mut()
        {
            let _ = turn_gate.acknowledge_turn(turn_id);
        }
    }

    pub(super) async fn claim_native_turn(
        &self,
        turn_id: TurnId,
        external_context: TurnContext,
    ) -> Result<ClaimedNativeTurn, HarnessError> {
        let mut state = self.state.lock().await;
        if let Some(native_turn) = state.native_turn.as_ref() {
            if native_turn.turn_id != turn_id {
                return Err(HarnessError::Protocol(format!(
                    "native thread {} emitted turn {turn_id} while turn {} is active",
                    state.binding.handle.harness_thread_id, native_turn.turn_id
                )));
            }
            return Ok(ClaimedNativeTurn {
                token: native_turn.token,
                context: native_turn.context.clone(),
                external: native_turn.origin == NativeTurnOrigin::External,
            });
        }

        let (token, context, turn_gate, origin) = if let Some(operation) = state.operation.take() {
            (
                operation.token,
                operation.context,
                Some(operation.turn_gate),
                NativeTurnOrigin::Prepared(operation.token),
            )
        } else {
            let token = state.token();
            (token, external_context, None, NativeTurnOrigin::External)
        };
        let external = origin == NativeTurnOrigin::External;
        state.native_turn = Some(OwnedNativeTurn {
            token,
            turn_id,
            origin,
            context: context.clone(),
            turn_gate,
        });
        state.native_activity = NativeActivity::Active;
        Ok(ClaimedNativeTurn {
            token,
            context,
            external,
        })
    }

    pub(super) async fn install_native_turn_gate(
        &self,
        token: CoordinatorToken,
        turn_id: TurnId,
        turn_gate: ThreadTurnLease,
    ) -> Result<(), ThreadTurnLease> {
        let mut state = self.state.lock().await;
        let Some(native_turn) = state.native_turn.as_mut().filter(|turn| {
            turn.token == token && turn.turn_id == turn_id && turn.turn_gate.is_none()
        }) else {
            return Err(turn_gate);
        };
        native_turn.turn_gate = Some(turn_gate);
        Ok(())
    }

    pub(super) async fn acknowledge_native_turn(
        &self,
        token: CoordinatorToken,
        turn_id: TurnId,
    ) -> Option<giskard_proto::ThreadRuntimeOverview> {
        let mut state = self.state.lock().await;
        state
            .native_turn
            .as_mut()
            .filter(|turn| turn.token == token && turn.turn_id == turn_id)
            .and_then(|turn| turn.turn_gate.as_mut())
            .and_then(|turn_gate| turn_gate.acknowledge_turn(turn_id))
    }

    pub(super) async fn take_native_turn_gate(
        &self,
        token: CoordinatorToken,
        turn_id: TurnId,
    ) -> Option<ThreadTurnLease> {
        let mut state = self.state.lock().await;
        state
            .native_turn
            .as_mut()
            .filter(|turn| turn.token == token && turn.turn_id == turn_id)
            .and_then(|turn| turn.turn_gate.take())
    }

    pub(super) async fn finish_native_turn(&self, token: CoordinatorToken, turn_id: TurnId) {
        let mut state = self.state.lock().await;
        if state.native_turn.as_ref().is_some_and(|turn| {
            turn.token == token && turn.turn_id == turn_id && state.token_is_current(token)
        }) {
            state.native_turn = None;
            state.native_activity = NativeActivity::Idle;
            drop(state);
            self.changed.notify_waiters();
        }
    }

    pub(super) async fn owner_finished(&self, cancelled: bool) {
        let mut state = self.state.lock().await;
        let retired = match state.owner {
            OwnerPhase::Draining(_) if cancelled => {
                state.generation = state.generation.wrapping_add(1);
                state.owner = OwnerPhase::Retired;
                state.native_activity = NativeActivity::Unloaded;
                true
            }
            OwnerPhase::Live(_) => {
                state.owner = OwnerPhase::Failed("native event stream ended".into());
                state.native_activity = NativeActivity::Unknown;
                false
            }
            _ => false,
        };
        drop(state);
        if retired {
            self.changed.notify_waiters();
        }
    }

    pub(super) async fn begin_retirement(&self) -> Option<EventOwnerControl> {
        let mut state = self.state.lock().await;
        match &state.owner {
            OwnerPhase::Live(control) | OwnerPhase::Draining(control) => {
                let control = control.clone();
                state.owner = OwnerPhase::Draining(control.clone());
                Some(control)
            }
            OwnerPhase::Installing | OwnerPhase::Retired | OwnerPhase::Failed(_) => None,
        }
    }

    pub(super) async fn draining_control(&self) -> Option<EventOwnerControl> {
        let state = self.state.lock().await;
        match &state.owner {
            OwnerPhase::Draining(control) => Some(control.clone()),
            _ => None,
        }
    }

    pub(super) async fn is_retired(&self) -> bool {
        matches!(self.state.lock().await.owner, OwnerPhase::Retired)
    }

    pub(super) async fn finish_retirement(&self) {
        let mut state = self.state.lock().await;
        if matches!(state.owner, OwnerPhase::Retired) {
            return;
        }
        state.generation = state.generation.wrapping_add(1);
        state.owner = OwnerPhase::Retired;
        state.native_activity = NativeActivity::Unloaded;
        drop(state);
        self.changed.notify_waiters();
    }

    #[cfg(test)]
    pub(super) async fn owns_native_turn_for_test(&self, turn_id: TurnId) -> bool {
        self.state
            .lock()
            .await
            .native_turn
            .as_ref()
            .is_some_and(|native_turn| native_turn.turn_id == turn_id)
    }
}

#[cfg(test)]
use giskard_core::ids::ItemId;

/// Role-specific handle for serializing one thread's event-owner changes.
#[derive(Clone)]
pub(super) struct OwnerLock(Arc<AsyncMutex<()>>);

impl OwnerLock {
    /// Creates an owner lock before an authority or weak interner entry exists.
    pub(super) fn new() -> Self {
        Self(Arc::new(AsyncMutex::new(())))
    }

    /// Produces the weak handle retained for unpublished thread IDs.
    pub(super) fn downgrade(&self) -> WeakOwnerLock {
        WeakOwnerLock(Arc::downgrade(&self.0))
    }

    /// Acquires the lock without exposing its raw mutex identity.
    pub(super) async fn lock_owned(self) -> OwnedMutexGuard<()> {
        self.0.lock_owned().await
    }

    #[cfg(test)]
    pub(super) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    #[cfg(test)]
    pub(super) fn is_unlocked(&self) -> bool {
        self.0.try_lock().is_ok()
    }
}

/// Weak owner-lock handle used only while a thread authority is unpublished.
pub(super) struct WeakOwnerLock(Weak<AsyncMutex<()>>);

impl WeakOwnerLock {
    /// Recovers the exact interned owner lock while a strong owner remains.
    pub(super) fn upgrade(&self) -> Option<OwnerLock> {
        self.0.upgrade().map(OwnerLock)
    }

    /// Reports whether pruning may discard this unpublished entry.
    pub(super) fn strong_count(&self) -> usize {
        self.0.strong_count()
    }
}

/// Independently synchronized optional coordinator storage.
#[derive(Default)]
struct CoordinatorSlot {
    current: AsyncMutex<Option<ThreadBinding>>,
}

/// Per-parent FIFO storage whose presence also records a running worker.
#[derive(Default)]
struct MaterializationSlot {
    /// Queue presence is also the per-parent worker-running marker, including while empty.
    queue: AsyncMutex<Option<VecDeque<SubagentMaterializationJob>>>,
}

/// Owns immutable identity and all process-local state for one verified thread.
pub(crate) struct ThreadAuthority {
    thread_id: ThreadId,
    project_id: ProjectId,
    owner: OwnerLock,
    coordinator: CoordinatorSlot,
    runtime: ThreadRuntimeSlot,
    materialization: MaterializationSlot,
}

impl ThreadAuthority {
    /// Publishes an authority around the owner lock adopted from the interner.
    pub(super) fn new(thread_id: ThreadId, project_id: ProjectId, owner: OwnerLock) -> Self {
        Self {
            thread_id,
            project_id,
            owner,
            coordinator: CoordinatorSlot::default(),
            runtime: ThreadRuntimeSlot::new(),
            materialization: MaterializationSlot::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(thread_id: ThreadId, project_id: ProjectId) -> Self {
        Self::new(thread_id, project_id, OwnerLock::new())
    }

    /// Returns the authority's immutable thread identity.
    pub(crate) fn thread_id(&self) -> ThreadId {
        self.thread_id
    }

    /// Returns the authority's immutable project association.
    pub(super) fn project_id(&self) -> ProjectId {
        self.project_id
    }

    /// Clones the role-specific owner lock for acquisition outside the thread index.
    pub(super) fn owner_lock(&self) -> OwnerLock {
        self.owner.clone()
    }

    /// Clones the currently installed coordinator, if any.
    pub(super) async fn coordinator(&self) -> Option<ThreadBinding> {
        self.coordinator.current.lock().await.clone()
    }

    /// Installs only into an empty slot and returns the proposal unchanged on conflict.
    pub(super) async fn install_coordinator_if_empty(
        &self,
        coordinator: ThreadBinding,
    ) -> Result<(), ThreadBinding> {
        let mut current = self.coordinator.current.lock().await;
        if current.is_some() {
            return Err(coordinator);
        }
        *current = Some(coordinator);
        Ok(())
    }

    /// Clears only a pointer-identical installed coordinator.
    pub(super) async fn clear_coordinator_if(&self, expected: &ThreadBinding) -> bool {
        let mut current = self.coordinator.current.lock().await;
        if current
            .as_ref()
            .is_some_and(|coordinator| Arc::ptr_eq(coordinator, expected))
        {
            *current = None;
            true
        } else {
            false
        }
    }

    /// Returns the current runtime entry without creating one.
    pub(crate) fn runtime_entry(&self) -> Option<Arc<Mutex<ThreadRuntimeEntry>>> {
        self.runtime.current()
    }

    /// Returns the current runtime entry or installs a new empty entry.
    pub(crate) fn runtime_entry_or_create(&self) -> Arc<Mutex<ThreadRuntimeEntry>> {
        self.runtime.get_or_create()
    }

    /// Removes and returns the current runtime entry.
    pub(crate) fn take_runtime_entry(&self) -> Option<Arc<Mutex<ThreadRuntimeEntry>>> {
        self.runtime.take()
    }

    /// Runs a callback only when `expected` remains the exact installed runtime entry.
    pub(crate) fn with_exact_runtime_entry<R>(
        &self,
        expected: &Arc<Mutex<ThreadRuntimeEntry>>,
        callback: impl FnOnce(&mut ThreadRuntimeEntry) -> R,
    ) -> Option<R> {
        self.runtime.with_exact_current(expected, callback)
    }

    #[allow(clippy::result_large_err)]
    /// Enqueues FIFO work and returns a permit only when the caller must start the worker.
    pub(super) async fn enqueue_materialization_job(
        &self,
        job: SubagentMaterializationJob,
        establishment_permit: Option<RegistryTaskPermit>,
        tracker: &Arc<RegistryTaskTracker>,
    ) -> Result<Option<RegistryTaskPermit>, SubagentMaterializationJob> {
        let mut queue = self.materialization.queue.lock().await;
        let permit = if queue.is_some() {
            None
        } else if let Some(permit) = establishment_permit {
            Some(permit)
        } else {
            let Some(permit) = tracker.register() else {
                return Err(job);
            };
            Some(permit)
        };
        queue.get_or_insert_with(VecDeque::new).push_back(job);
        Ok(permit)
    }

    /// Pops the next job, clearing the worker marker only on a later empty poll.
    pub(super) async fn next_materialization_job(&self) -> Option<SubagentMaterializationJob> {
        let mut queue = self.materialization.queue.lock().await;
        let job = queue.as_mut().and_then(VecDeque::pop_front);
        if job.is_none() {
            *queue = None;
        }
        job
    }

    #[cfg(test)]
    pub(super) async fn materialization_job_ids(&self) -> Option<Vec<ItemId>> {
        self.materialization
            .queue
            .lock()
            .await
            .as_ref()
            .map(|queue| queue.iter().map(|job| job.item_id).collect())
    }

    #[cfg(test)]
    pub(super) async fn has_materialization_worker(&self) -> bool {
        self.materialization.queue.lock().await.is_some()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use giskard_core::ids::{ProjectId, ThreadId, TurnId};
    use giskard_harness::ThreadHandle;
    use giskard_persist::PersistStore;

    use super::*;
    use crate::hub::Hub;
    use crate::ledger;
    use crate::registry::tests::{
        prepare_test_operation, test_authority, test_coordinator, test_turn_context,
    };
    use crate::registry::{RegistryShared, turn_reservation};
    use crate::thread_runtime::ThreadRuntimeSupport;

    fn coordinator(thread_id: ThreadId, native_id: &str) -> Arc<ThreadCoordinator> {
        Arc::new(ThreadCoordinator::new(
            LoadedThreadBinding {
                project_id: ProjectId::new(),
                handle: ThreadHandle::detached(thread_id, native_id.into()),
                native_model: None,
            },
            ClassificationPhase::Primary,
        ))
    }

    #[test]
    fn weak_owner_lock_preserves_identity_and_expires() {
        let lock = OwnerLock::new();
        let weak = lock.downgrade();
        let upgraded = weak.upgrade().unwrap();
        assert!(lock.ptr_eq(&upgraded));
        drop(lock);
        drop(upgraded);
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn coordinator_slot_installs_and_clears_only_the_expected_entry() {
        let thread_id = ThreadId::new();
        let authority = ThreadAuthority::new_for_test(thread_id, ProjectId::new());
        let first = coordinator(thread_id, "first");
        let second = coordinator(thread_id, "second");

        assert!(
            authority
                .install_coordinator_if_empty(first.clone())
                .await
                .is_ok()
        );
        let rejected = authority
            .install_coordinator_if_empty(second.clone())
            .await
            .err()
            .unwrap();
        assert!(Arc::ptr_eq(&rejected, &second));
        assert!(!authority.clear_coordinator_if(&second).await);
        assert!(Arc::ptr_eq(&authority.coordinator().await.unwrap(), &first));
        assert!(authority.clear_coordinator_if(&first).await);
        assert!(authority.coordinator().await.is_none());
    }

    #[tokio::test]
    async fn subagent_coordinator_rejects_prepared_operations() {
        let coordinator = test_coordinator(ClassificationPhase::Subagent);
        let runtime = ThreadRuntimeSupport::new();
        let binding = coordinator.binding().await;
        let authority = test_authority(&binding);
        let context = test_turn_context();
        let lease = runtime
            .reserve_turn(
                &authority,
                turn_reservation(binding.project_id, &binding.handle, &context),
            )
            .unwrap();
        let error = match coordinator.prepare_operation(context, lease).await {
            Ok(_) => panic!("sub-agent operation must be rejected"),
            Err((error, _)) => error,
        };
        assert!(matches!(error, HarnessError::ThreadReadOnly { .. }));
    }

    #[tokio::test]
    async fn cancelling_operation_admission_cannot_leave_runtime_reserved() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(PersistStore::new(tmp.path().to_path_buf()));
        let shared = Arc::new(RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ));
        let coordinator = Arc::new(test_coordinator(ClassificationPhase::Primary));
        let binding = coordinator.binding().await;
        let thread_id = binding.handle.thread;
        let authority = shared
            .intern_thread_authority(thread_id, binding.project_id)
            .await
            .unwrap();
        let state_guard = coordinator.state.lock().await;
        let task_shared = shared.clone();
        let task_coordinator = coordinator.clone();
        let task_authority = authority.clone();
        let context = test_turn_context();
        let task = tokio::spawn(async move {
            task_shared
                .admit_operation(
                    &task_authority,
                    &task_coordinator,
                    binding.project_id,
                    &binding.handle,
                    &context,
                )
                .await
        });

        while !shared.runtime.has_active_turn(&authority) {
            tokio::task::yield_now().await;
        }
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(state_guard);

        assert!(!shared.runtime.has_active_turn(&authority));
        assert!(coordinator.state.lock().await.operation.is_none());
    }

    #[tokio::test]
    async fn stale_operation_token_cannot_abort_a_later_operation() {
        let coordinator = test_coordinator(ClassificationPhase::Primary);
        let (cancel, _) = tokio::sync::watch::channel(false);
        let (_, completed) = tokio::sync::watch::channel(false);
        coordinator
            .activate_owner(EventOwnerControl { cancel, completed })
            .await
            .unwrap();
        let runtime = ThreadRuntimeSupport::new();
        let stale = prepare_test_operation(&coordinator, &runtime, test_turn_context()).await;
        let mut stale_lease = coordinator.abort_operation(stale).await.unwrap();
        let _ = stale_lease.release();
        let current = prepare_test_operation(&coordinator, &runtime, test_turn_context()).await;

        let _ = coordinator.abort_operation(stale).await;

        let state = coordinator.state.lock().await;
        assert_eq!(state.operation.as_ref().map(|op| op.token), Some(current));
    }

    #[tokio::test]
    async fn stale_operation_acknowledgement_cannot_acknowledge_a_later_lease() {
        let coordinator = test_coordinator(ClassificationPhase::Primary);
        let binding = coordinator.binding().await;
        let authority = test_authority(&binding);
        let runtime = ThreadRuntimeSupport::new();
        let context = test_turn_context();
        let (cancel, _) = tokio::sync::watch::channel(false);
        let (_, completed) = tokio::sync::watch::channel(false);
        coordinator
            .activate_owner(EventOwnerControl { cancel, completed })
            .await
            .unwrap();

        let stale_lease = runtime
            .reserve_turn(
                &authority,
                turn_reservation(binding.project_id, &binding.handle, &context),
            )
            .unwrap();
        let stale = match coordinator
            .prepare_operation(context.clone(), stale_lease)
            .await
        {
            Ok(operation) => operation,
            Err((error, _)) => panic!("stale test operation was rejected: {error}"),
        };
        let mut stale_lease = coordinator.abort_operation(stale).await.unwrap();
        let _ = stale_lease.release();

        let current_lease = runtime
            .reserve_turn(
                &authority,
                turn_reservation(binding.project_id, &binding.handle, &context),
            )
            .unwrap();
        let current = match coordinator
            .prepare_operation(context.clone(), current_lease)
            .await
        {
            Ok(operation) => operation,
            Err((error, _)) => panic!("current test operation was rejected: {error}"),
        };

        coordinator
            .acknowledge_operation_turn(stale, TurnId::new())
            .await;
        let summary = runtime
            .current_overview()
            .threads
            .into_iter()
            .find(|summary| summary.thread_id == binding.handle.thread)
            .unwrap();
        assert!(matches!(
            summary.turn_state,
            giskard_proto::RuntimeTurnState::Active { turn_id: None }
        ));
        let mut current_lease = coordinator.abort_operation(current).await.unwrap();
        let _ = current_lease.release();
    }

    #[tokio::test]
    async fn stale_native_completion_cannot_clear_a_later_turn() {
        let coordinator = test_coordinator(ClassificationPhase::Subagent);
        let first_turn = TurnId::new();
        let first = coordinator
            .claim_native_turn(first_turn, test_turn_context())
            .await
            .unwrap();
        coordinator
            .finish_native_turn(first.token, first_turn)
            .await;

        let second_turn = TurnId::new();
        let second = coordinator
            .claim_native_turn(second_turn, test_turn_context())
            .await
            .unwrap();
        coordinator
            .finish_native_turn(first.token, first_turn)
            .await;

        let state = coordinator.state.lock().await;
        assert_eq!(
            state
                .native_turn
                .as_ref()
                .map(|turn| (turn.token, turn.turn_id)),
            Some((second.token, second_turn))
        );
    }

    #[tokio::test]
    async fn mismatched_native_start_preserves_the_active_turn() {
        let coordinator = test_coordinator(ClassificationPhase::Subagent);
        let active_turn = TurnId::new();
        let active = coordinator
            .claim_native_turn(active_turn, test_turn_context())
            .await
            .unwrap();

        let other_turn = TurnId::new();
        let error = match coordinator
            .claim_native_turn(other_turn, test_turn_context())
            .await
        {
            Ok(_) => panic!("a second native turn must not replace the active turn"),
            Err(error) => error,
        };
        assert!(matches!(error, HarnessError::Protocol(_)));
        let state = coordinator.state.lock().await;
        assert_eq!(
            state
                .native_turn
                .as_ref()
                .map(|turn| (turn.token, turn.turn_id)),
            Some((active.token, active_turn))
        );
    }

    #[tokio::test]
    async fn failed_owner_rejects_new_preparation_before_io() {
        let coordinator = test_coordinator(ClassificationPhase::Primary);
        let (cancel, _) = tokio::sync::watch::channel(false);
        let (_, completed) = tokio::sync::watch::channel(false);
        coordinator
            .activate_owner(EventOwnerControl { cancel, completed })
            .await
            .unwrap();
        coordinator.owner_finished(false).await;
        let runtime = ThreadRuntimeSupport::new();
        let binding = coordinator.binding().await;
        let authority = test_authority(&binding);
        let context = test_turn_context();
        let lease = runtime
            .reserve_turn(
                &authority,
                turn_reservation(binding.project_id, &binding.handle, &context),
            )
            .unwrap();
        let error = match coordinator.prepare_operation(context, lease).await {
            Ok(_) => panic!("failed owner must reject operation"),
            Err((error, _)) => error,
        };
        assert!(matches!(error, HarnessError::Protocol(_)));
        assert!(coordinator.state.lock().await.operation.is_none());
    }
}
