use std::sync::{Arc, Weak};

use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use giskard_core::error::HarnessError;
use giskard_core::ids::{ProjectId, ThreadId};
use giskard_harness::{AgentHarness, ThreadHandle};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, warn};

use super::event_forwarder::{
    ForwarderExitReason, ThreadEventForwarder, forwarder_exit_reason_label,
};
use super::thread::{
    ClassificationPhase, DetachRequestOutcome, OwnerExitOutcome, TURN_INTENT_CAPACITY,
    ThreadAuthority, ThreadCoordinator,
};
use super::{LoadedThreadBinding, RegistryShared, RegistryTaskPermit};

const DRIVER_COMMAND_CAPACITY: usize = 64;

#[derive(Clone)]
pub(super) struct DriverHandle {
    tx: mpsc::Sender<DriverCommand>,
}

pub(super) enum AttachOutcome {
    Installed,
    Reused(Box<ThreadHandle>),
}

struct Attach {
    binding: LoadedThreadBinding,
    classification: ClassificationPhase,
    reply: oneshot::Sender<Result<AttachOutcome, HarnessError>>,
}

enum DriverCommand {
    Attach(Box<Attach>),
    Detach {
        thread_id: ThreadId,
        reply: oneshot::Sender<()>,
    },
}

struct OwnerExit {
    authority: Arc<ThreadAuthority>,
    coordinator: Arc<ThreadCoordinator>,
    reason: ForwarderExitReason,
}

struct ProjectEventDriver {
    project_id: ProjectId,
    rx: mpsc::Receiver<DriverCommand>,
    harness: Weak<dyn AgentHarness>,
    shared: Arc<RegistryShared>,
    owners: FuturesUnordered<BoxFuture<'static, OwnerExit>>,
    parked: Vec<Attach>,
}

impl DriverHandle {
    #[cfg(test)]
    pub(super) fn disconnected() -> Self {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        Self { tx }
    }

    pub(super) async fn attach(
        &self,
        binding: LoadedThreadBinding,
        classification: ClassificationPhase,
    ) -> Result<AttachOutcome, HarnessError> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(DriverCommand::Attach(Box::new(Attach {
                binding,
                classification,
                reply,
            })))
            .await
            .map_err(|_| HarnessError::Protocol("project event driver is gone".into()))?;
        response.await.map_err(|_| {
            HarnessError::Protocol("project event driver dropped attach reply".into())
        })?
    }

    pub(super) async fn detach(&self, thread_id: ThreadId) {
        let (reply, response) = oneshot::channel();
        if self
            .tx
            .send(DriverCommand::Detach { thread_id, reply })
            .await
            .is_ok()
        {
            let _ = response.await;
        }
    }
}

pub(super) fn spawn_project_event_driver(
    project_id: ProjectId,
    shared: Arc<RegistryShared>,
    harness: &Arc<dyn AgentHarness>,
    permit: RegistryTaskPermit,
) -> DriverHandle {
    let (tx, rx) = mpsc::channel(DRIVER_COMMAND_CAPACITY);
    let driver = ProjectEventDriver {
        project_id,
        rx,
        harness: Arc::downgrade(harness),
        shared,
        owners: FuturesUnordered::new(),
        parked: Vec::new(),
    };
    tokio::spawn(async move {
        let _permit = permit;
        driver.run().await;
    });
    DriverHandle { tx }
}

impl ProjectEventDriver {
    async fn run(mut self) {
        let mut closed = false;
        loop {
            tokio::select! {
                command = self.rx.recv(), if !closed => match command {
                    Some(DriverCommand::Attach(attach)) => self.attach(*attach).await,
                    Some(DriverCommand::Detach { thread_id, reply }) => {
                        self.detach(thread_id, reply).await;
                    }
                    None => closed = true,
                },
                Some(exit) = self.owners.next(), if !self.owners.is_empty() => {
                    self.owner_exited(exit).await;
                }
            }
            if closed && self.owners.is_empty() {
                for attach in self.parked.drain(..) {
                    let _ = attach.reply.send(Err(HarnessError::Protocol(
                        "project event driver is gone".into(),
                    )));
                }
                return;
            }
        }
    }

    async fn attach(&mut self, attach: Attach) {
        let thread_id = attach.binding.handle.thread;
        let project_id = attach.binding.project_id;
        if project_id != self.project_id {
            let _ = attach.reply.send(Err(HarnessError::Protocol(format!(
                "project {} event driver cannot attach thread {thread_id} from project {project_id}",
                self.project_id
            ))));
            return;
        }
        let authority = match self
            .shared
            .intern_thread_authority(thread_id, project_id)
            .await
        {
            Ok(authority) => authority,
            Err(error) => {
                let _ = attach
                    .reply
                    .send(Err(HarnessError::Protocol(error.to_string())));
                return;
            }
        };
        if let Some(existing) = authority.coordinator().await {
            if existing.is_detaching().await {
                self.parked.push(attach);
                return;
            }
            let result = existing
                .reusable_handle(
                    project_id,
                    thread_id,
                    Some(&attach.binding.handle.harness_thread_id),
                    attach.classification,
                )
                .await
                .map(Box::new)
                .map(AttachOutcome::Reused);
            let _ = attach.reply.send(result);
            return;
        }
        let Some(harness) = self.harness.upgrade() else {
            let _ = attach.reply.send(Err(HarnessError::Protocol(
                "project harness is gone".into(),
            )));
            return;
        };
        let stream = harness.subscribe(&attach.binding.handle);
        drop(harness);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (intent_tx, intent_rx) = mpsc::channel(TURN_INTENT_CAPACITY);
        let coordinator = Arc::new(ThreadCoordinator::new_live(
            attach.binding,
            attach.classification,
            cancel_tx,
            intent_tx,
        ));
        if authority
            .install_coordinator_if_empty(coordinator.clone())
            .await
            .is_err()
        {
            let _ = attach.reply.send(Err(HarnessError::Protocol(format!(
                "thread {thread_id} event owner installation conflicted with an existing coordinator"
            ))));
            return;
        }
        let shared = self.shared.clone();
        let owner_harness = self.harness.clone();
        let owner_authority = authority.clone();
        let owner_coordinator = coordinator.clone();
        self.owners.push(Box::pin(async move {
            let forwarder = ThreadEventForwarder::new(
                shared,
                owner_authority.clone(),
                owner_coordinator.clone(),
                owner_harness,
                stream,
                cancel_rx,
                intent_rx,
            )
            .await;
            let reason = forwarder.run().await;
            OwnerExit {
                authority: owner_authority,
                coordinator: owner_coordinator,
                reason,
            }
        }));
        debug!(%project_id, %thread_id, "installed long-lived native event owner");
        let _ = attach.reply.send(Ok(AttachOutcome::Installed));
    }

    async fn detach(&mut self, thread_id: ThreadId, reply: oneshot::Sender<()>) {
        let Some(authority) = self.shared.thread_authority(thread_id).await else {
            let _ = reply.send(());
            return;
        };
        let Some(coordinator) = authority.coordinator().await else {
            let _ = reply.send(());
            return;
        };
        if let DetachRequestOutcome::ClearFailed(reply) = coordinator.request_detach(reply).await {
            authority.clear_coordinator_if(&coordinator).await;
            let _ = reply.send(());
            self.retry_parked(thread_id).await;
        }
    }

    async fn owner_exited(&mut self, exit: OwnerExit) {
        let thread_id = exit.authority.thread_id();
        match exit.coordinator.owner_exited(exit.reason).await {
            OwnerExitOutcome::Detached(waiters) => {
                exit.authority.clear_coordinator_if(&exit.coordinator).await;
                for waiter in waiters {
                    let _ = waiter.send(());
                }
                self.retry_parked(thread_id).await;
            }
            OwnerExitOutcome::ClearFailed => {
                if exit.authority.clear_coordinator_if(&exit.coordinator).await {
                    warn!(
                        project_id = %self.project_id,
                        %thread_id,
                        exit_reason = forwarder_exit_reason_label(exit.reason),
                        "removed failed event owner so the thread can be reopened"
                    );
                }
            }
            OwnerExitOutcome::RetainFailed => {}
        }
    }

    async fn retry_parked(&mut self, thread_id: ThreadId) {
        let mut retry = Vec::new();
        let mut index = 0;
        while index < self.parked.len() {
            if self.parked[index].binding.handle.thread == thread_id {
                retry.push(self.parked.swap_remove(index));
            } else {
                index += 1;
            }
        }
        for attach in retry {
            self.attach(attach).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use giskard_core::approval::ApprovalDecision;
    use giskard_core::event::AgentEvent;
    use giskard_core::ids::{ApprovalId, ProjectId, ServerRequestId, ThreadId, TurnId};
    use giskard_core::model::{ModelDescriptor, ModelRef};
    use giskard_core::server_request::ServerRequestResponse;
    use giskard_core::token::{TokenLedger, TokenUsage};
    use giskard_core::turn::{
        Mode, PermissionPreset, TurnMode, TurnModel, TurnStatus, TurnStatusKind,
    };
    use giskard_core::user_input::UserInput;
    use giskard_harness::{AgentEventStream, EventLog, HarnessCapabilities, OpenThreadOptions};

    use super::*;
    use crate::hub::Hub;
    use crate::ledger;
    use crate::registry::{RegistryShared, TurnContext, TurnContextKind, TurnIntent};
    use giskard_persist::PersistStore;
    use giskard_persist::store::ThreadFile;

    struct TestHarness {
        logs: Mutex<HashMap<ThreadId, Arc<EventLog>>>,
        start_gate: tokio::sync::Notify,
        start_calls: AtomicUsize,
    }

    impl TestHarness {
        fn new() -> Self {
            Self {
                logs: Mutex::new(HashMap::new()),
                start_gate: tokio::sync::Notify::new(),
                start_calls: AtomicUsize::new(0),
            }
        }

        fn log(&self, thread_id: ThreadId) -> Arc<EventLog> {
            self.logs
                .lock()
                .unwrap()
                .entry(thread_id)
                .or_insert_with(|| Arc::new(EventLog::new()))
                .clone()
        }
    }

    #[async_trait]
    impl AgentHarness for TestHarness {
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

        async fn start_turn(
            &self,
            _thread: &ThreadHandle,
            _input: UserInput,
            _overrides: giskard_core::turn::TurnOverrides,
        ) -> Result<TurnId, HarnessError> {
            self.start_calls.fetch_add(1, Ordering::SeqCst);
            self.start_gate.notified().await;
            Ok(TurnId::new())
        }

        fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream {
            AgentEventStream::new(self.log(thread.thread).reader())
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
            _response: ServerRequestResponse,
        ) -> Result<(), HarnessError> {
            Ok(())
        }

        async fn interrupt(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
            Ok(())
        }

        async fn shutdown(&self) -> Result<(), HarnessError> {
            for log in self.logs.lock().unwrap().values() {
                log.close();
            }
            Ok(())
        }
    }

    fn binding(project_id: ProjectId, thread_id: ThreadId, native: &str) -> LoadedThreadBinding {
        LoadedThreadBinding {
            project_id,
            handle: ThreadHandle::opened(thread_id, native.into(), PathBuf::from("/tmp/test")),
            native_model: None,
        }
    }

    fn setup() -> (
        Arc<RegistryShared>,
        Arc<TestHarness>,
        DriverHandle,
        ProjectId,
        Arc<PersistStore>,
    ) {
        let temp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(PersistStore::new(temp.keep()));
        let shared = Arc::new(RegistryShared::new(
            Arc::new(Hub::new()),
            store.clone(),
            ledger::spawn(store.clone()),
        ));
        let harness = Arc::new(TestHarness::new());
        let project_id = ProjectId::new();
        let permit = shared.background_tasks.register().unwrap();
        let trait_harness: Arc<dyn AgentHarness> = harness.clone();
        let driver = spawn_project_event_driver(project_id, shared.clone(), &trait_harness, permit);
        (shared, harness, driver, project_id, store)
    }

    async fn persist_thread(store: &PersistStore, project_id: ProjectId, thread_id: ThreadId) {
        store
            .create_project(project_id, "project", "/tmp/test")
            .await
            .unwrap();
        let now = chrono::Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: 1,
                    id: thread_id,
                    project_id,
                    title: "thread".into(),
                    harness_thread_id: "native".into(),
                    parent_thread_id: None,
                    spawned_by_turn_id: None,
                    kind: giskard_core::thread::ThreadKind::Primary,
                    mode: TurnMode::Known(Mode::Build),
                    current_model: TurnModel::Unknown,
                    context_window: 0,
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

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn attach_installs_one_owner_and_a_second_attach_reuses_it() {
        let (shared, _harness, driver, project_id, _store) = setup();
        let thread_id = ThreadId::new();
        assert!(matches!(
            driver
                .attach(
                    binding(project_id, thread_id, "native"),
                    ClassificationPhase::Primary
                )
                .await
                .unwrap(),
            AttachOutcome::Installed
        ));
        let reused = driver
            .attach(
                binding(project_id, thread_id, "native"),
                ClassificationPhase::Primary,
            )
            .await
            .unwrap();
        assert!(matches!(reused, AttachOutcome::Reused(handle) if handle.thread == thread_id));
        assert!(shared.coordinator(thread_id).await.is_some());
    }

    #[tokio::test]
    async fn attach_with_an_incompatible_binding_is_rejected() {
        let (_shared, _harness, driver, project_id, _store) = setup();
        let thread_id = ThreadId::new();
        driver
            .attach(
                binding(project_id, thread_id, "native-a"),
                ClassificationPhase::Primary,
            )
            .await
            .unwrap();
        let error = driver
            .attach(
                binding(project_id, thread_id, "native-b"),
                ClassificationPhase::Primary,
            )
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("incompatible event owner"));
    }

    #[tokio::test]
    async fn detach_cancels_the_owner_and_clears_the_slot() {
        let (shared, harness, driver, project_id, _store) = setup();
        let thread_id = ThreadId::new();
        driver
            .attach(
                binding(project_id, thread_id, "native"),
                ClassificationPhase::Primary,
            )
            .await
            .unwrap();
        let authority = shared.thread_authority(thread_id).await.unwrap();
        let coordinator = authority.coordinator().await.unwrap();
        let context = TurnContext {
            user_input: UserInput::text("pending"),
            model: TurnModel::Known(ModelRef {
                provider: "test".into(),
                model: "test".into(),
                reasoning_effort: None,
            }),
            mode: TurnMode::Known(Mode::Build),
            kind: TurnContextKind::User,
        };
        let intents = coordinator.intent_sender().await.unwrap();
        let (reply, response) = oneshot::channel();
        intents
            .send(TurnIntent::StartTurn {
                input: context.user_input.clone(),
                overrides: giskard_core::turn::TurnOverrides {
                    model: None,
                    mode: Mode::Build,
                    permission_preset: PermissionPreset::AskFirst,
                },
                context,
                reply,
            })
            .await
            .unwrap();
        wait_until(|| harness.start_calls.load(Ordering::SeqCst) == 1).await;
        tokio::time::timeout(std::time::Duration::from_secs(1), driver.detach(thread_id))
            .await
            .expect("detach should not wait for the harness reply");
        assert!(shared.coordinator(thread_id).await.is_none());
        assert!(!shared.runtime.has_active_turn(&authority));
        assert!(matches!(
            response.await.unwrap(),
            Err(HarnessError::Protocol(message))
                if message == "event owner exited before the harness answered"
        ));
    }

    #[tokio::test]
    async fn detach_without_an_owner_replies_immediately() {
        let (_shared, _harness, driver, _project_id, _store) = setup();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            driver.detach(ThreadId::new()),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn attach_during_detach_is_parked_until_the_detach_completes() {
        let (shared, harness, _handle, project_id, _store) = setup();
        let permit = shared.background_tasks.register().unwrap();
        let trait_harness: Arc<dyn AgentHarness> = harness;
        let (_tx, rx) = mpsc::channel(1);
        let mut driver = ProjectEventDriver {
            project_id,
            rx,
            harness: Arc::downgrade(&trait_harness),
            shared: shared.clone(),
            owners: FuturesUnordered::new(),
            parked: Vec::new(),
        };
        let _permit = permit;
        let thread_id = ThreadId::new();
        let (first_reply, first_response) = oneshot::channel();
        driver
            .attach(Attach {
                binding: binding(project_id, thread_id, "native"),
                classification: ClassificationPhase::Primary,
                reply: first_reply,
            })
            .await;
        assert!(matches!(
            first_response.await.unwrap().unwrap(),
            AttachOutcome::Installed
        ));

        let (detach_reply, mut detach_response) = oneshot::channel();
        driver.detach(thread_id, detach_reply).await;
        let (second_reply, mut second_response) = oneshot::channel();
        driver
            .attach(Attach {
                binding: binding(project_id, thread_id, "native"),
                classification: ClassificationPhase::Primary,
                reply: second_reply,
            })
            .await;
        assert!(matches!(
            second_response.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        let exit = driver.owners.next().await.unwrap();
        driver.owner_exited(exit).await;
        assert!(detach_response.try_recv().is_ok());
        assert!(matches!(
            second_response.await.unwrap().unwrap(),
            AttachOutcome::Installed
        ));
        assert!(shared.coordinator(thread_id).await.is_some());
    }

    #[tokio::test]
    async fn a_persistence_blocked_exit_keeps_the_failed_coordinator() {
        let (shared, harness, driver, project_id, store) = setup();
        let thread_id = ThreadId::new();
        persist_thread(&store, project_id, thread_id).await;
        driver
            .attach(
                binding(project_id, thread_id, "native"),
                ClassificationPhase::Primary,
            )
            .await
            .unwrap();
        let authority = shared.thread_authority(thread_id).await.unwrap();
        let turn = TurnId::new();
        assert!(harness.log(thread_id).append(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        }));
        wait_until(|| shared.runtime.has_active_turn(&authority)).await;

        let history = store
            .data_dir()
            .join("projects")
            .join(project_id.to_string())
            .join("threads")
            .join(thread_id.to_string())
            .join("history.jsonl");
        tokio::fs::create_dir(&history).await.unwrap();
        assert!(harness.log(thread_id).append(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));
        wait_until(|| {
            authority
                .runtime_entry()
                .is_some_and(|_| shared.runtime.has_active_turn(&authority))
        })
        .await;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if authority.coordinator().await.unwrap().is_failed().await {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let error = driver
            .attach(
                binding(project_id, thread_id, "native"),
                ClassificationPhase::Primary,
            )
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("persistence_blocked"));
        driver.detach(thread_id).await;
        assert!(authority.coordinator().await.is_none());
    }

    #[tokio::test]
    async fn any_other_owner_failure_clears_the_slot_so_the_thread_can_reopen() {
        let (shared, harness, driver, project_id, _store) = setup();
        let thread_id = ThreadId::new();
        driver
            .attach(
                binding(project_id, thread_id, "native"),
                ClassificationPhase::Primary,
            )
            .await
            .unwrap();
        harness.log(thread_id).close();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while shared.coordinator(thread_id).await.is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            driver
                .attach(
                    binding(project_id, thread_id, "native"),
                    ClassificationPhase::Primary
                )
                .await
                .unwrap(),
            AttachOutcome::Installed
        ));
    }

    #[tokio::test]
    async fn the_driver_exits_after_its_handle_drops_and_its_owners_finish() {
        let (shared, harness, driver, project_id, _store) = setup();
        let thread_id = ThreadId::new();
        driver
            .attach(
                binding(project_id, thread_id, "native"),
                ClassificationPhase::Primary,
            )
            .await
            .unwrap();
        drop(driver);
        harness.log(thread_id).close();
        wait_until(|| {
            shared
                .background_tasks
                .count
                .load(std::sync::atomic::Ordering::Acquire)
                == 0
        })
        .await;
    }

    #[tokio::test]
    async fn the_driver_does_not_keep_the_harness_alive() {
        let (_shared, harness, driver, project_id, _store) = setup();
        let before = Arc::strong_count(&harness);
        driver
            .attach(
                binding(project_id, ThreadId::new(), "native"),
                ClassificationPhase::Primary,
            )
            .await
            .unwrap();
        assert_eq!(Arc::strong_count(&harness), before);
    }
}
