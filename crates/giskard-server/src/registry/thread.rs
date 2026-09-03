use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};

use giskard_core::error::HarnessError;
use giskard_core::ids::{ProjectId, ThreadId, TurnId};
use giskard_core::thread::ThreadKind;
use giskard_core::turn::{TurnMode, TurnModel, TurnOverrides};
use giskard_core::user_input::UserInput;
use giskard_harness::ThreadHandle;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, mpsc, oneshot, watch};

use super::{
    ForwarderExitReason, LoadedThreadBinding, RegistryTaskPermit, RegistryTaskTracker,
    SubagentMaterializationJob, TurnContext, forwarder_exit_reason_label,
};
use crate::thread_runtime::{ThreadRuntimeEntry, ThreadRuntimeSlot};

pub(super) const TURN_INTENT_CAPACITY: usize = 4;

pub(super) enum TurnIntent {
    StartTurn {
        input: UserInput,
        overrides: TurnOverrides,
        context: TurnContext,
        reply: oneshot::Sender<Result<TurnId, HarnessError>>,
    },
    Compact {
        context: TurnContext,
        reply: oneshot::Sender<Result<(), HarnessError>>,
    },
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

enum OwnerPhase {
    Live {
        cancel: watch::Sender<bool>,
        intents: mpsc::Sender<TurnIntent>,
    },
    Detaching {
        cancel: watch::Sender<bool>,
        waiters: Vec<oneshot::Sender<()>>,
    },
    Failed(String),
}

pub(super) enum OwnerExitOutcome {
    Detached(Vec<oneshot::Sender<()>>),
    RetainFailed,
    ClearFailed,
}

pub(super) enum DetachRequestOutcome {
    Pending,
    ClearFailed(oneshot::Sender<()>),
}

/// Persisted model and mode sampled outside the coordinator lock for one external claim.
#[derive(Clone)]
pub(super) struct ExternalTurnDefaults {
    pub(super) model: TurnModel,
    pub(super) mode: TurnMode,
}

struct ThreadCoordinatorState {
    binding: LoadedThreadBinding,
    classification: ClassificationPhase,
    owner: OwnerPhase,
}

pub(super) struct ThreadCoordinator {
    state: AsyncMutex<ThreadCoordinatorState>,
}

pub(super) type ThreadBinding = Arc<ThreadCoordinator>;

impl ThreadCoordinator {
    #[cfg(test)]
    pub(super) fn new(binding: LoadedThreadBinding, classification: ClassificationPhase) -> Self {
        let (cancel, _) = watch::channel(false);
        let (intents, _) = mpsc::channel(TURN_INTENT_CAPACITY);
        Self::new_live(binding, classification, cancel, intents)
    }

    pub(super) fn new_live(
        binding: LoadedThreadBinding,
        classification: ClassificationPhase,
        cancel: watch::Sender<bool>,
        intents: mpsc::Sender<TurnIntent>,
    ) -> Self {
        Self {
            state: AsyncMutex::new(ThreadCoordinatorState {
                binding,
                classification,
                owner: OwnerPhase::Live { cancel, intents },
            }),
        }
    }

    pub(super) async fn binding(&self) -> LoadedThreadBinding {
        self.state.lock().await.binding.clone()
    }

    pub(super) async fn classification(&self) -> ClassificationPhase {
        self.state.lock().await.classification
    }

    pub(super) async fn intent_sender(&self) -> Result<mpsc::Sender<TurnIntent>, HarnessError> {
        let state = self.state.lock().await;
        match &state.owner {
            OwnerPhase::Live { intents, .. } => Ok(intents.clone()),
            OwnerPhase::Failed(reason) => Err(HarnessError::Protocol(format!(
                "thread {} event owner failed: {reason}",
                state.binding.handle.thread
            ))),
            OwnerPhase::Detaching { .. } => Err(HarnessError::Protocol(format!(
                "thread {} has no live event owner",
                state.binding.handle.thread
            ))),
        }
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
            OwnerPhase::Live { .. } => Ok(state.binding.handle.clone()),
            OwnerPhase::Failed(reason) => Err(HarnessError::Protocol(format!(
                "thread {} event owner failed: {reason}",
                thread_id
            ))),
            OwnerPhase::Detaching { .. } => Err(HarnessError::Protocol(format!(
                "thread {} event owner is not reusable",
                thread_id
            ))),
        }
    }

    pub(super) async fn is_detaching(&self) -> bool {
        matches!(self.state.lock().await.owner, OwnerPhase::Detaching { .. })
    }

    pub(super) async fn is_failed(&self) -> bool {
        matches!(self.state.lock().await.owner, OwnerPhase::Failed(_))
    }

    pub(super) async fn request_detach(&self, reply: oneshot::Sender<()>) -> DetachRequestOutcome {
        let mut state = self.state.lock().await;
        match &mut state.owner {
            OwnerPhase::Live { cancel, .. } => {
                let cancel = cancel.clone();
                state.owner = OwnerPhase::Detaching {
                    cancel: cancel.clone(),
                    waiters: vec![reply],
                };
                let _ = cancel.send(true);
                DetachRequestOutcome::Pending
            }
            OwnerPhase::Detaching { cancel, waiters } => {
                let _ = cancel;
                waiters.push(reply);
                DetachRequestOutcome::Pending
            }
            OwnerPhase::Failed(_) => DetachRequestOutcome::ClearFailed(reply),
        }
    }

    pub(super) async fn owner_exited(&self, reason: ForwarderExitReason) -> OwnerExitOutcome {
        let mut state = self.state.lock().await;
        match &mut state.owner {
            OwnerPhase::Detaching { waiters, .. } => {
                let waiters = std::mem::take(waiters);
                state.owner = OwnerPhase::Failed("detached".into());
                OwnerExitOutcome::Detached(waiters)
            }
            OwnerPhase::Live { .. } if reason == ForwarderExitReason::PersistenceBlocked => {
                state.owner = OwnerPhase::Failed(forwarder_exit_reason_label(reason).into());
                OwnerExitOutcome::RetainFailed
            }
            OwnerPhase::Live { .. } => {
                state.owner = OwnerPhase::Failed(forwarder_exit_reason_label(reason).into());
                OwnerExitOutcome::ClearFailed
            }
            OwnerPhase::Failed(_) => OwnerExitOutcome::RetainFailed,
        }
    }
}

/// The visible input label for a native turn Giskard did not start.
///
/// A promptless external turn still gets one row of presentation metadata, and it may only name
/// what Giskard has actually established. A classified child is a sub-agent turn; an unclassified
/// native thread has no proven relationship yet, so its turns must not claim one. A `Primary`
/// thread never reaches this path with an empty label unless Giskard missed the start, so it keeps
/// the empty input it has always had.
///
/// A turn claimed while the thread was unclassified keeps this label after classification, for the
/// same reason its mode is not rewritten: the label records what was known when the turn was
/// claimed, not what is known now.
pub(super) fn external_turn_input_label(classification: ClassificationPhase) -> UserInput {
    match classification {
        ClassificationPhase::Primary => UserInput::text(""),
        ClassificationPhase::Subagent => UserInput::text("Sub-agent turn"),
        ClassificationPhase::Orphan => UserInput::text("Unclassified native turn"),
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

    use giskard_core::ids::{ProjectId, ThreadId};
    use giskard_harness::ThreadHandle;

    use super::*;
    use crate::registry::tests::test_coordinator;

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
    async fn intent_sender_follows_the_owner_phase() {
        let coordinator = test_coordinator(ClassificationPhase::Primary);
        let thread_id = coordinator.binding().await.handle.thread;
        assert!(coordinator.intent_sender().await.is_ok());
        let (reply, _) = oneshot::channel();
        assert!(matches!(
            coordinator.request_detach(reply).await,
            DetachRequestOutcome::Pending
        ));
        assert!(matches!(
            coordinator.intent_sender().await,
            Err(HarnessError::Protocol(message))
                if message == format!("thread {thread_id} has no live event owner")
        ));

        let coordinator = test_coordinator(ClassificationPhase::Primary);
        let thread_id = coordinator.binding().await.handle.thread;
        let _ = coordinator
            .owner_exited(ForwarderExitReason::PersistenceBlocked)
            .await;
        assert!(matches!(
            coordinator.intent_sender().await,
            Err(HarnessError::Protocol(message))
                if message == format!("thread {thread_id} event owner failed: persistence_blocked")
        ));
    }
}
