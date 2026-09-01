use std::sync::Arc;

use giskard_core::error::HarnessError;
use giskard_core::ids::ProjectId;
use giskard_core::model::ModelRef;
use giskard_harness::{ThreadAttachment, ThreadEventOwner};
use tokio::sync::{OwnedMutexGuard, oneshot, watch};
use tracing::{debug, warn};

use super::event_forwarder::{
    ForwarderExitReason, ThreadEventForwarder, forwarder_exit_reason_label,
};
use super::thread::{
    ClassificationPhase, EventOwnerControl, ThreadAuthority, ThreadBinding, ThreadCoordinator,
};
use super::{LoadedThreadBinding, RegistryShared, RegistryTaskPermit};

struct ForwarderStart {
    coordinator: ThreadBinding,
    owner: ThreadEventOwner,
}

/// Prepared, no-gap installation of one linear native event owner.
///
/// Construction acquires every fallible registry resource and starts a gated task before the
/// attachment is consumed. `commit` then performs attachment ownership, Live coordinator
/// publication, and task handoff synchronously while retaining the exact coordinator-slot guard.
/// The coordinator binding derives its complete native/local identity from the consumed owner;
/// callers supply only project and model metadata that the attachment does not contain.
/// Dropping an uncommitted value drops its attachment, returning the receiver to its route.
pub(super) struct OwnerInstallation {
    attachment: Option<ThreadAttachment>,
    project_id: ProjectId,
    native_model: Option<ModelRef>,
    classification: ClassificationPhase,
    control: Option<EventOwnerControl>,
    _owner_guard: OwnedMutexGuard<()>,
    coordinator_slot: OwnedMutexGuard<Option<ThreadBinding>>,
    start: Option<oneshot::Sender<ForwarderStart>>,
}

impl OwnerInstallation {
    pub(super) async fn prepare(
        shared: &Arc<RegistryShared>,
        owner_guard: OwnedMutexGuard<()>,
        attachment: ThreadAttachment,
        project_id: ProjectId,
        native_model: Option<ModelRef>,
        classification: ClassificationPhase,
    ) -> Result<Self, HarnessError> {
        let thread_id = attachment.handle().thread;
        let authority = shared
            .intern_thread_authority(thread_id, project_id)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?;
        let coordinator_slot = authority.coordinator_slot().await;
        if coordinator_slot.is_some() {
            return Err(HarnessError::Protocol(format!(
                "thread {thread_id} attachment conflicted with an existing event owner"
            )));
        }
        let Some(permit) = shared.background_tasks.register() else {
            return Err(HarnessError::Protocol(
                "server is shutting down; refusing to install event owner".into(),
            ));
        };
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (completed_tx, completed_rx) = watch::channel(false);
        let (start_tx, start_rx) = oneshot::channel();
        spawn_gated_forwarder(
            shared.clone(),
            authority,
            cancel_rx,
            completed_tx,
            permit,
            start_rx,
        );

        let control = EventOwnerControl {
            cancel: cancel_tx,
            completed: completed_rx,
        };
        Ok(Self {
            attachment: Some(attachment),
            project_id,
            native_model,
            classification,
            control: Some(control),
            _owner_guard: owner_guard,
            coordinator_slot,
            start: Some(start_tx),
        })
    }

    pub(super) fn commit(mut self) -> Result<ThreadBinding, HarnessError> {
        let start = self.start.take().ok_or_else(|| {
            HarnessError::Protocol("event forwarder start gate was not prepared".into())
        })?;
        let control = self
            .control
            .take()
            .ok_or_else(|| HarnessError::Protocol("event owner control was not prepared".into()))?;
        let attachment = self.attachment.take().ok_or_else(|| {
            HarnessError::Protocol("event owner attachment was already consumed".into())
        })?;
        let owner = attachment.commit()?;
        let handle = owner.handle().clone();
        let thread_id = handle.thread;
        let project_id = self.project_id;
        let binding = LoadedThreadBinding {
            project_id: self.project_id,
            handle,
            native_model: self.native_model,
        };
        let coordinator = Arc::new(ThreadCoordinator::new_live(
            binding,
            self.classification,
            control,
        ));
        *self.coordinator_slot = Some(coordinator.clone());
        if let Err(start) = start.send(ForwarderStart {
            coordinator: coordinator.clone(),
            owner,
        }) {
            *self.coordinator_slot = None;
            drop(start.owner);
            return Err(HarnessError::Protocol(format!(
                "thread {thread_id} event forwarder start gate closed before commit"
            )));
        }
        debug!(%project_id, %thread_id, "installed long-lived native event owner");
        Ok(coordinator)
    }

    #[cfg(test)]
    fn close_start_gate_for_test(&mut self) {
        let (closed, receiver) = oneshot::channel();
        drop(receiver);
        self.start = Some(closed);
    }
}

fn spawn_gated_forwarder(
    shared: Arc<RegistryShared>,
    authority: Arc<ThreadAuthority>,
    cancel: watch::Receiver<bool>,
    completed: watch::Sender<bool>,
    permit: RegistryTaskPermit,
    start: oneshot::Receiver<ForwarderStart>,
) {
    tokio::spawn(async move {
        let _permit = permit;
        let Ok(ForwarderStart { coordinator, owner }) = start.await else {
            return;
        };
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
                warn!(%thread_id, %error, "could not retain persistence-blocked native event owner");
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use giskard_core::ids::{ProjectId, ThreadId};
    use giskard_harness::{AgentEventStream, ThreadAttachment, ThreadHandle};
    use giskard_persist::PersistStore;
    use tokio::sync::broadcast;

    use super::*;
    use crate::hub::Hub;
    use crate::ledger;

    fn attachment(handle: ThreadHandle, returned: Arc<AtomicUsize>) -> ThreadAttachment {
        let (_sender, receiver) = broadcast::channel(4);
        let owner_returned = returned.clone();
        ThreadAttachment::from_route(
            handle,
            AgentEventStream::new(receiver),
            move || {
                let owner_returned = owner_returned.clone();
                Ok(move |_| {
                    owner_returned.fetch_add(1, Ordering::SeqCst);
                })
            },
            move |_| {
                returned.fetch_add(1, Ordering::SeqCst);
            },
        )
    }

    fn shared() -> Arc<RegistryShared> {
        let directory = tempfile::tempdir().unwrap().keep();
        let store = Arc::new(PersistStore::new(directory));
        Arc::new(RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store),
        ))
    }

    #[tokio::test]
    async fn admission_failure_returns_attachment_before_commit() {
        let shared = shared();
        shared
            .background_tasks
            .close_and_wait(std::time::Duration::from_secs(1))
            .await
            .unwrap();
        let returned = Arc::new(AtomicUsize::new(0));
        let handle = ThreadHandle::opened(ThreadId::new(), "native".into(), "/tmp".into());
        let binding = LoadedThreadBinding {
            project_id: ProjectId::new(),
            handle: handle.clone(),
            native_model: None,
        };
        let owner_guard = super::super::lock_thread_owner_after_drain(&shared, handle.thread).await;

        let error = match OwnerInstallation::prepare(
            &shared,
            owner_guard,
            attachment(handle, returned.clone()),
            binding.project_id,
            binding.native_model,
            ClassificationPhase::Primary,
        )
        .await
        {
            Ok(_) => panic!("closed admission unexpectedly prepared an owner"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("shutting down"), "{error}");
        assert_eq!(returned.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn coordinator_conflict_returns_attachment_before_commit() {
        let shared = shared();
        let project_id = ProjectId::new();
        let handle = ThreadHandle::opened(ThreadId::new(), "native".into(), "/tmp".into());
        let binding = LoadedThreadBinding {
            project_id,
            handle: handle.clone(),
            native_model: None,
        };
        let authority = shared
            .intern_thread_authority(handle.thread, project_id)
            .await
            .unwrap();
        let installed = authority
            .install_coordinator_if_empty(Arc::new(ThreadCoordinator::new(
                binding.clone(),
                ClassificationPhase::Primary,
            )))
            .await;
        assert!(installed.is_ok(), "test coordinator slot should be empty");
        let returned = Arc::new(AtomicUsize::new(0));
        let owner_guard = super::super::lock_thread_owner_after_drain(&shared, handle.thread).await;

        let error = match OwnerInstallation::prepare(
            &shared,
            owner_guard,
            attachment(handle, returned.clone()),
            binding.project_id,
            binding.native_model,
            ClassificationPhase::Primary,
        )
        .await
        {
            Ok(_) => panic!("occupied coordinator slot unexpectedly accepted an owner"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("conflicted"), "{error}");
        assert_eq!(returned.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn installation_retains_owner_serialization_through_commit() {
        let shared = shared();
        let project_id = ProjectId::new();
        let handle = ThreadHandle::opened(ThreadId::new(), "native".into(), "/tmp".into());
        let thread_id = handle.thread;
        let (sender, receiver) = broadcast::channel(4);
        let attachment = ThreadAttachment::from_route(
            handle.clone(),
            AgentEventStream::new(receiver),
            move || Ok(|_: AgentEventStream| {}),
            |_| {},
        );
        let binding = LoadedThreadBinding {
            project_id,
            handle,
            native_model: None,
        };
        let owner_guard = super::super::lock_thread_owner_after_drain(&shared, thread_id).await;
        let installation = OwnerInstallation::prepare(
            &shared,
            owner_guard,
            attachment,
            binding.project_id,
            binding.native_model,
            ClassificationPhase::Primary,
        )
        .await
        .unwrap();

        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                super::super::lock_thread_owner(&shared.threads, thread_id),
            )
            .await
            .is_err(),
            "prepared installation released owner serialization before commit"
        );
        let coordinator = installation.commit().unwrap();
        let installed = coordinator.binding().await;
        assert_eq!(installed.handle.thread, thread_id);
        assert_eq!(installed.handle.harness_thread_id, "native");
        let acquired = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            super::super::lock_thread_owner(&shared.threads, thread_id),
        )
        .await;
        assert!(
            acquired.is_ok(),
            "commit did not release owner serialization"
        );
        drop(sender);
    }

    #[tokio::test]
    async fn closed_start_gate_clears_coordinator_and_returns_exact_owner() {
        let shared = shared();
        let project_id = ProjectId::new();
        let handle = ThreadHandle::opened(ThreadId::new(), "native".into(), "/tmp".into());
        let thread_id = handle.thread;
        let returned = Arc::new(AtomicUsize::new(0));
        let binding = LoadedThreadBinding {
            project_id,
            handle: handle.clone(),
            native_model: None,
        };
        let owner_guard = super::super::lock_thread_owner_after_drain(&shared, thread_id).await;
        let mut installation = OwnerInstallation::prepare(
            &shared,
            owner_guard,
            attachment(handle, returned.clone()),
            binding.project_id,
            binding.native_model,
            ClassificationPhase::Primary,
        )
        .await
        .unwrap();
        installation.close_start_gate_for_test();
        let error = match installation.commit() {
            Err(error) => error,
            Ok(_) => panic!("closed start gate unexpectedly committed"),
        };
        assert!(error.to_string().contains("start gate closed"));
        assert!(shared.coordinator(thread_id).await.is_none());
        assert_eq!(returned.load(Ordering::SeqCst), 1);
    }
}
