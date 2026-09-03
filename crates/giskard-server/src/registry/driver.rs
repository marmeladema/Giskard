use std::sync::{Arc, Weak};

use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use giskard_core::error::HarnessError;
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_harness::{AgentHarness, DiscoveryStream, EventStreamError, ThreadHandle};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, error, warn};

use super::SubagentActivityInfo;
use super::admission::{self, Admission, Admitted};

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
    tx: DriverSender,
}

#[derive(Clone)]
enum DriverSender {
    Strong(mpsc::Sender<DriverCommand>),
    Weak(mpsc::WeakSender<DriverCommand>),
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

pub(super) struct Link {
    pub(super) parent_thread_id: ThreadId,
    pub(super) spawned_by_turn_id: TurnId,
    pub(super) item_id: ItemId,
    pub(super) origin: &'static str,
    pub(super) info: SubagentActivityInfo,
    pub(super) reply: Option<oneshot::Sender<Result<Option<ThreadId>, HarnessError>>>,
}

enum DriverCommand {
    Attach(Box<Attach>),
    Detach {
        thread_id: ThreadId,
        reply: oneshot::Sender<()>,
    },
    Link(Box<Link>),
    Quiesce {
        reply: oneshot::Sender<()>,
    },
    Resume {
        reply: oneshot::Sender<()>,
    },
}

enum AdmissionSource {
    Discovery,
    Link {
        reply: Option<oneshot::Sender<Result<Option<ThreadId>, HarnessError>>>,
        parent_thread_id: ThreadId,
        item_id: ItemId,
        origin: &'static str,
    },
}

struct InflightAdmission {
    work: BoxFuture<'static, AdmissionOutcome>,
    source: AdmissionSource,
}

struct AdmissionOutcome {
    result: Result<Option<Admitted>, HarnessError>,
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
    weak_tx: mpsc::WeakSender<DriverCommand>,
    discoveries: DiscoveryStream,
    discoveries_closed: bool,
    admission: Option<InflightAdmission>,
    quiesced: bool,
}

impl DriverHandle {
    #[cfg(test)]
    pub(super) fn disconnected() -> Self {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        Self {
            tx: DriverSender::Strong(tx),
        }
    }

    #[cfg(test)]
    pub(super) fn responsive_for_test() -> Self {
        let (tx, mut rx) = mpsc::channel(4);
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    DriverCommand::Quiesce { reply } | DriverCommand::Resume { reply } => {
                        let _ = reply.send(());
                    }
                    DriverCommand::Detach { reply, .. } => {
                        let _ = reply.send(());
                    }
                    DriverCommand::Attach(attach) => {
                        let _ = attach.reply.send(Err(HarnessError::Protocol(
                            "test driver does not attach".into(),
                        )));
                    }
                    DriverCommand::Link(mut link) => {
                        if let Some(reply) = link.reply.take() {
                            let _ = reply.send(Ok(None));
                        }
                    }
                }
            }
        });
        Self {
            tx: DriverSender::Strong(tx),
        }
    }

    async fn send(&self, command: DriverCommand) -> Result<(), HarnessError> {
        let tx = match &self.tx {
            DriverSender::Strong(tx) => tx.clone(),
            DriverSender::Weak(tx) => tx
                .upgrade()
                .ok_or_else(|| HarnessError::Protocol("project event driver is gone".into()))?,
        };
        tx.send(command)
            .await
            .map_err(|_| HarnessError::Protocol("project event driver is gone".into()))
    }

    pub(super) async fn attach(
        &self,
        binding: LoadedThreadBinding,
        classification: ClassificationPhase,
    ) -> Result<AttachOutcome, HarnessError> {
        let (reply, response) = oneshot::channel();
        self.send(DriverCommand::Attach(Box::new(Attach {
            binding,
            classification,
            reply,
        })))
        .await?;
        response.await.map_err(|_| {
            HarnessError::Protocol("project event driver dropped attach reply".into())
        })?
    }

    pub(super) async fn detach(&self, thread_id: ThreadId) {
        let (reply, response) = oneshot::channel();
        if self
            .send(DriverCommand::Detach { thread_id, reply })
            .await
            .is_ok()
        {
            let _ = response.await;
        }
    }

    pub(super) async fn link(&self, link: Link) -> Result<(), HarnessError> {
        self.send(DriverCommand::Link(Box::new(link))).await
    }

    pub(super) async fn quiesce(&self) -> Result<(), HarnessError> {
        let (reply, response) = oneshot::channel();
        self.send(DriverCommand::Quiesce { reply }).await?;
        response.await.map_err(|_| {
            HarnessError::Protocol("project event driver dropped quiesce reply".into())
        })
    }

    pub(super) async fn resume(&self) -> Result<(), HarnessError> {
        let (reply, response) = oneshot::channel();
        self.send(DriverCommand::Resume { reply }).await?;
        response
            .await
            .map_err(|_| HarnessError::Protocol("project event driver dropped resume reply".into()))
    }
}

pub(super) fn spawn_project_event_driver(
    project_id: ProjectId,
    shared: Arc<RegistryShared>,
    harness: &Arc<dyn AgentHarness>,
    discoveries: DiscoveryStream,
    permit: RegistryTaskPermit,
) -> DriverHandle {
    let (tx, rx) = mpsc::channel(DRIVER_COMMAND_CAPACITY);
    let weak_tx = tx.downgrade();
    let driver = ProjectEventDriver {
        project_id,
        rx,
        harness: Arc::downgrade(harness),
        shared,
        owners: FuturesUnordered::new(),
        parked: Vec::new(),
        weak_tx,
        discoveries,
        discoveries_closed: false,
        admission: None,
        quiesced: false,
    };
    tokio::spawn(async move {
        let _permit = permit;
        driver.run().await;
    });
    DriverHandle {
        tx: DriverSender::Strong(tx),
    }
}

impl ProjectEventDriver {
    async fn run(mut self) {
        let mut closed = false;
        loop {
            tokio::select! {
                command = self.rx.recv(), if !closed && self.admission.is_none() => match command {
                    Some(DriverCommand::Attach(attach)) => self.attach(*attach).await,
                    Some(DriverCommand::Detach { thread_id, reply }) => {
                        self.detach(thread_id, reply).await;
                    }
                    Some(DriverCommand::Link(link)) => self.begin_link(*link).await,
                    Some(DriverCommand::Quiesce { reply }) => {
                        self.quiesced = true;
                        let _ = reply.send(());
                    }
                    Some(DriverCommand::Resume { reply }) => {
                        self.quiesced = false;
                        let _ = reply.send(());
                    }
                    None => closed = true,
                },
                record = self.discoveries.recv(),
                    if !self.discoveries_closed && self.admission.is_none() => match record {
                    Ok(record) => self.begin_discovery(record),
                    Err(EventStreamError::Closed) => self.discoveries_closed = true,
                    Err(EventStreamError::Gap { dropped }) => error!(
                        project_id = %self.project_id,
                        dropped,
                        "native thread discovery log dropped records"
                    ),
                },
                outcome = async {
                    match self.admission.as_mut() {
                        Some(admission) => admission.work.as_mut().await,
                        None => std::future::pending().await,
                    }
                }, if self.admission.is_some() => self.finish_admission(outcome).await,
                Some(exit) = self.owners.next(), if !self.owners.is_empty() => {
                    self.owner_exited(exit).await;
                }
            }
            if closed && self.owners.is_empty() && self.admission.is_none() {
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
        let owner_driver = DriverHandle {
            tx: DriverSender::Weak(self.weak_tx.clone()),
        };
        self.owners.push(Box::pin(async move {
            let forwarder = ThreadEventForwarder::new(
                shared,
                owner_authority.clone(),
                owner_coordinator.clone(),
                owner_harness,
                stream,
                cancel_rx,
                intent_rx,
                owner_driver,
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

    async fn begin_link(&mut self, mut link: Link) {
        if self.quiesced {
            if let Some(reply) = link.reply.take() {
                let _ = reply.send(Err(HarnessError::Protocol(
                    "project is being deleted".into(),
                )));
            }
            return;
        }
        let live = match self.shared.thread_authority(link.parent_thread_id).await {
            Some(authority) => match authority.coordinator().await {
                Some(coordinator) => {
                    !coordinator.is_detaching().await && !coordinator.is_failed().await
                }
                None => false,
            },
            None => false,
        };
        if !live {
            warn!(project_id = %self.project_id, parent_thread_id = %link.parent_thread_id,
                origin = link.origin, "refusing native identity link from a parent without a live owner");
            if let Some(reply) = link.reply.take() {
                let _ = reply.send(Ok(None));
            }
            return;
        }
        let source = AdmissionSource::Link {
            reply: link.reply.take(),
            parent_thread_id: link.parent_thread_id,
            item_id: link.item_id,
            origin: link.origin,
        };
        let shared = self.shared.clone();
        let Some(harness) = self.harness.upgrade() else {
            if let AdmissionSource::Link {
                reply: Some(reply), ..
            } = source
            {
                let _ = reply.send(Err(HarnessError::Protocol(
                    "project harness is gone".into(),
                )));
            }
            return;
        };
        let project_id = self.project_id;
        self.admission = Some(InflightAdmission {
            work: Box::pin(async move {
                AdmissionOutcome {
                    result: admission::admit(
                        shared,
                        harness,
                        project_id,
                        Admission::Link(Box::new(link)),
                    )
                    .await,
                }
            }),
            source,
        });
    }

    fn begin_discovery(&mut self, record: giskard_harness::ThreadDiscovered) {
        if self.quiesced {
            warn!(project_id = %self.project_id, thread_id = %record.thread,
                "dropping native thread discovery after project quiesce");
            #[cfg(test)]
            self.shared
                .discovery_records_processed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return;
        }
        let Some(harness) = self.harness.upgrade() else {
            warn!(project_id = %self.project_id, thread_id = %record.thread,
                "dropping native thread discovery because the project harness is gone");
            return;
        };
        let shared = self.shared.clone();
        let project_id = self.project_id;
        self.admission = Some(InflightAdmission {
            work: Box::pin(async move {
                AdmissionOutcome {
                    result: admission::admit(
                        shared,
                        harness,
                        project_id,
                        Admission::Discovered(record),
                    )
                    .await,
                }
            }),
            source: AdmissionSource::Discovery,
        });
    }

    async fn finish_admission(&mut self, outcome: AdmissionOutcome) {
        let Some(inflight) = self.admission.take() else {
            return;
        };
        let result = match outcome.result {
            Ok(Some(admitted)) => {
                let thread_id = admitted.thread_id;
                let link_result =
                    (admitted.classification != ClassificationPhase::Orphan).then_some(thread_id);
                let (reply, mut response) = oneshot::channel();
                self.attach(Attach {
                    binding: admitted.binding,
                    classification: admitted.classification,
                    reply,
                })
                .await;
                match response.try_recv() {
                    // A detach parks this attach inside the driver; the retry owns the reply and
                    // admission can report success because the binding is durably queued.
                    Ok(Ok(_)) | Err(oneshot::error::TryRecvError::Empty) => Ok(link_result),
                    Ok(Err(error)) => Err(error),
                    Err(oneshot::error::TryRecvError::Closed) => Err(HarnessError::Protocol(
                        "project event driver dropped admission attach reply".into(),
                    )),
                }
            }
            Ok(None) => Ok(None),
            Err(error) => Err(error),
        };
        self.finish_admission_reply(result, inflight.source);
    }

    fn finish_admission_reply(
        &self,
        result: Result<Option<ThreadId>, HarnessError>,
        source: AdmissionSource,
    ) {
        match source {
            AdmissionSource::Discovery => {
                #[cfg(test)]
                self.shared
                    .discovery_records_processed
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if let Err(error) = result {
                    warn!(project_id = %self.project_id, %error,
                        "failed to admit discovered native thread");
                }
            }
            AdmissionSource::Link {
                reply,
                parent_thread_id,
                item_id,
                origin,
            } => {
                if let Err(error) = &result {
                    warn!(project_id = %self.project_id, %parent_thread_id, %item_id, origin, %error,
                        "failed to admit linked native thread");
                }
                if let Some(reply) = reply {
                    let _ = reply.send(result);
                }
            }
        }
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
    use giskard_harness::{
        AgentEventStream, DiscoveryStream, EventLog, HarnessCapabilities, OpenThreadOptions,
        ThreadDiscovered,
    };

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
        claims: Mutex<HashMap<String, ThreadId>>,
        claim_calls: AtomicUsize,
        claim_gate: Mutex<Option<Arc<tokio::sync::Notify>>>,
        discoveries: Arc<EventLog<ThreadDiscovered>>,
    }

    impl TestHarness {
        fn new() -> Self {
            Self {
                logs: Mutex::new(HashMap::new()),
                start_gate: tokio::sync::Notify::new(),
                start_calls: AtomicUsize::new(0),
                claims: Mutex::new(HashMap::new()),
                claim_calls: AtomicUsize::new(0),
                claim_gate: Mutex::new(None),
                discoveries: Arc::new(EventLog::new()),
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

        fn bind(&self, native: &str, thread_id: ThreadId) {
            self.claims
                .lock()
                .unwrap()
                .insert(native.to_owned(), thread_id);
        }

        fn announce(&self, record: ThreadDiscovered) {
            self.bind(&record.harness_thread_id, record.thread);
            assert!(self.discoveries.append(record));
        }

        fn gate_claims(&self) -> Arc<tokio::sync::Notify> {
            let gate = Arc::new(tokio::sync::Notify::new());
            *self.claim_gate.lock().unwrap() = Some(gate.clone());
            gate
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

        fn discoveries(&self) -> DiscoveryStream {
            DiscoveryStream::new(self.discoveries.reader())
        }

        async fn claim_native_thread(
            &self,
            proposed: ThreadId,
            harness_thread_id: String,
            workspace_root: PathBuf,
        ) -> Result<ThreadHandle, HarnessError> {
            self.claim_calls.fetch_add(1, Ordering::SeqCst);
            let gate = self.claim_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.notified().await;
            }
            let thread = *self
                .claims
                .lock()
                .unwrap()
                .entry(harness_thread_id.clone())
                .or_insert(proposed);
            Ok(ThreadHandle::opened(
                thread,
                harness_thread_id,
                workspace_root,
            ))
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
            self.discoveries.close();
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
        let driver = spawn_project_event_driver(
            project_id,
            shared.clone(),
            &trait_harness,
            trait_harness.discoveries(),
            permit,
        );
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

    async fn save_thread(
        store: &PersistStore,
        project_id: ProjectId,
        thread_id: ThreadId,
        native: &str,
        kind: giskard_core::thread::ThreadKind,
        parent_thread_id: Option<ThreadId>,
    ) {
        let now = chrono::Utc::now();
        store
            .save_thread(
                project_id,
                &ThreadFile {
                    revision: 0,
                    version: giskard_persist::store::THREAD_METADATA_VERSION,
                    id: thread_id,
                    project_id,
                    title: "thread".into(),
                    harness_thread_id: native.into(),
                    parent_thread_id,
                    spawned_by_turn_id: parent_thread_id.map(|_| TurnId::new()),
                    kind,
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

    fn link(
        parent_thread_id: ThreadId,
        native: &str,
    ) -> (
        Link,
        oneshot::Receiver<Result<Option<ThreadId>, HarnessError>>,
    ) {
        let (reply, response) = oneshot::channel();
        (
            Link {
                parent_thread_id,
                spawned_by_turn_id: TurnId::new(),
                item_id: ItemId::new(),
                origin: "test",
                info: SubagentActivityInfo {
                    native_thread_id: native.into(),
                    agent_name: None,
                    agent_path: None,
                    title: Some("child".into()),
                },
                reply: Some(reply),
            },
            response,
        )
    }

    async fn load_thread(
        store: &PersistStore,
        project_id: ProjectId,
        thread_id: ThreadId,
    ) -> Option<ThreadFile> {
        store.load_thread(project_id, thread_id).await.unwrap()
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
        let (tx, rx) = mpsc::channel(1);
        let mut driver = ProjectEventDriver {
            project_id,
            rx,
            harness: Arc::downgrade(&trait_harness),
            shared: shared.clone(),
            owners: FuturesUnordered::new(),
            parked: Vec::new(),
            weak_tx: tx.downgrade(),
            discoveries: DiscoveryStream::closed(),
            discoveries_closed: false,
            admission: None,
            quiesced: false,
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
        let weak_harness = Arc::downgrade(&harness);
        driver
            .attach(
                binding(project_id, ThreadId::new(), "native"),
                ClassificationPhase::Primary,
            )
            .await
            .unwrap();
        drop(harness);
        assert!(weak_harness.upgrade().is_none());
    }

    async fn attach_primary(
        driver: &DriverHandle,
        project_id: ProjectId,
        thread_id: ThreadId,
        native: &str,
    ) {
        driver
            .attach(
                binding(project_id, thread_id, native),
                ClassificationPhase::Primary,
            )
            .await
            .unwrap();
    }

    async fn wait_for_thread(
        store: &PersistStore,
        project_id: ProjectId,
        thread_id: ThreadId,
    ) -> ThreadFile {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Some(file) = load_thread(store, project_id, thread_id).await {
                    return file;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_link_for_an_unknown_native_id_creates_a_subagent_and_its_owner() {
        let (shared, harness, driver, project_id, store) = setup();
        let parent = ThreadId::new();
        persist_thread(&store, project_id, parent).await;
        attach_primary(&driver, project_id, parent, "native").await;
        let (request, response) = link(parent, "child-native");
        let spawned_by_turn_id = request.spawned_by_turn_id;
        driver.link(request).await.unwrap();
        let child = response.await.unwrap().unwrap().unwrap();
        let file = load_thread(&store, project_id, child).await.unwrap();
        assert_eq!(file.revision, 1);
        assert_eq!(file.kind, giskard_core::thread::ThreadKind::Subagent);
        assert_eq!(file.parent_thread_id, Some(parent));
        assert_eq!(file.spawned_by_turn_id, Some(spawned_by_turn_id));
        assert!(shared.coordinator(child).await.is_some());
        assert_eq!(harness.claim_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_discovery_creates_a_hidden_orphan_and_its_owner() {
        let (shared, harness, _driver, project_id, store) = setup();
        store
            .create_project(project_id, "project", "/tmp/test")
            .await
            .unwrap();
        let thread = ThreadId::new();
        harness.announce(ThreadDiscovered {
            thread,
            harness_thread_id: "discovered".into(),
            parent_harness_thread_id: None,
        });
        let file = wait_for_thread(&store, project_id, thread).await;
        assert_eq!(file.kind, giskard_core::thread::ThreadKind::Orphan);
        wait_until(|| harness.claim_calls.load(Ordering::SeqCst) == 0).await;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while shared.coordinator(thread).await.is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_link_after_discovery_classifies_the_same_thread() {
        let (shared, harness, driver, project_id, store) = setup();
        let parent = ThreadId::new();
        persist_thread(&store, project_id, parent).await;
        attach_primary(&driver, project_id, parent, "native").await;
        let child = ThreadId::new();
        harness.announce(ThreadDiscovered {
            thread: child,
            harness_thread_id: "child-native".into(),
            parent_harness_thread_id: Some("native".into()),
        });
        wait_for_thread(&store, project_id, child).await;
        let before = loop {
            if let Some(coordinator) = shared.coordinator(child).await {
                break coordinator;
            }
            tokio::task::yield_now().await;
        };
        let (request, response) = link(parent, "child-native");
        let spawned_by_turn_id = request.spawned_by_turn_id;
        driver.link(request).await.unwrap();
        assert_eq!(response.await.unwrap().unwrap(), Some(child));
        let file = load_thread(&store, project_id, child).await.unwrap();
        assert_eq!(file.kind, giskard_core::thread::ThreadKind::Subagent);
        assert_eq!(file.parent_thread_id, Some(parent));
        assert_eq!(file.spawned_by_turn_id, Some(spawned_by_turn_id));
        assert!(Arc::ptr_eq(
            &before,
            &shared.coordinator(child).await.unwrap()
        ));
    }

    #[tokio::test]
    async fn a_discovery_after_link_reuses_the_existing_thread() {
        let (shared, harness, driver, project_id, store) = setup();
        let parent = ThreadId::new();
        persist_thread(&store, project_id, parent).await;
        attach_primary(&driver, project_id, parent, "native").await;
        let (request, response) = link(parent, "child-native");
        driver.link(request).await.unwrap();
        let child = response.await.unwrap().unwrap().unwrap();
        let before = shared.coordinator(child).await.unwrap();
        harness.announce(ThreadDiscovered {
            thread: child,
            harness_thread_id: "child-native".into(),
            parent_harness_thread_id: Some("native".into()),
        });
        wait_until(|| shared.discovery_records_processed.load(Ordering::SeqCst) == 1).await;
        assert!(Arc::ptr_eq(
            &before,
            &shared.coordinator(child).await.unwrap()
        ));
        assert_eq!(
            load_thread(&store, project_id, child)
                .await
                .unwrap()
                .parent_thread_id,
            Some(parent)
        );
    }

    #[tokio::test]
    async fn repeated_activity_on_a_classified_child_reads_no_graph() {
        let (_shared, harness, driver, project_id, store) = setup();
        let parent = ThreadId::new();
        persist_thread(&store, project_id, parent).await;
        attach_primary(&driver, project_id, parent, "native").await;
        let (first, first_response) = link(parent, "child-native");
        driver.link(first).await.unwrap();
        let child = first_response.await.unwrap().unwrap().unwrap();
        let unrelated = ThreadId::new();
        save_thread(
            &store,
            project_id,
            unrelated,
            "unrelated",
            giskard_core::thread::ThreadKind::Primary,
            None,
        )
        .await;
        let unrelated_metadata = store
            .data_dir()
            .join("projects")
            .join(project_id.to_string())
            .join("threads")
            .join(unrelated.to_string())
            .join("thread.json");
        tokio::fs::write(unrelated_metadata, b"not json")
            .await
            .unwrap();
        let (second, second_response) = link(parent, "child-native");
        driver.link(second).await.unwrap();
        assert_eq!(second_response.await.unwrap().unwrap(), Some(child));
        assert_eq!(harness.claim_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn an_invalid_link_still_records_the_claimed_identity_as_an_orphan() {
        let (shared, _harness, driver, project_id, store) = setup();
        store
            .create_project(project_id, "project", "/tmp/test")
            .await
            .unwrap();
        let parent = ThreadId::new();
        save_thread(
            &store,
            project_id,
            parent,
            "parent-native",
            giskard_core::thread::ThreadKind::Subagent,
            Some(ThreadId::new()),
        )
        .await;
        driver
            .attach(
                binding(project_id, parent, "parent-native"),
                ClassificationPhase::Subagent,
            )
            .await
            .unwrap();
        let (request, response) = link(parent, "child-native");
        driver.link(request).await.unwrap();
        assert_eq!(response.await.unwrap().unwrap(), None);
        let ids = store.list_threads(project_id).await.unwrap();
        let child_id = ids.into_iter().find(|id| *id != parent).unwrap();
        let child = load_thread(&store, project_id, child_id).await.unwrap();
        assert_eq!(child.revision, 1);
        assert_eq!(child.kind, giskard_core::thread::ThreadKind::Orphan);
        assert_eq!(child.harness_thread_id, "child-native");
        assert!(shared.coordinator(child_id).await.is_some());
    }

    #[tokio::test]
    async fn a_reverse_link_returns_none_and_creates_nothing() {
        let (_shared, harness, driver, project_id, store) = setup();
        store
            .create_project(project_id, "project", "/tmp/test")
            .await
            .unwrap();
        let root = ThreadId::new();
        save_thread(
            &store,
            project_id,
            root,
            "root-native",
            giskard_core::thread::ThreadKind::Primary,
            None,
        )
        .await;
        let child = ThreadId::new();
        save_thread(
            &store,
            project_id,
            child,
            "child-native",
            giskard_core::thread::ThreadKind::Subagent,
            Some(root),
        )
        .await;
        harness.bind("root-native", root);
        driver
            .attach(
                binding(project_id, child, "child-native"),
                ClassificationPhase::Subagent,
            )
            .await
            .unwrap();
        let (request, response) = link(child, "root-native");
        driver.link(request).await.unwrap();
        assert_eq!(response.await.unwrap().unwrap(), None);
        assert_eq!(store.list_threads(project_id).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn admissions_are_sequential_and_ordered_with_detach() {
        let (shared, harness, driver, project_id, store) = setup();
        let parent = ThreadId::new();
        persist_thread(&store, project_id, parent).await;
        attach_primary(&driver, project_id, parent, "native").await;
        let gate = harness.gate_claims();
        let (first_link, first_response) = link(parent, "first-child-native");
        driver.link(first_link).await.unwrap();
        wait_until(|| harness.claim_calls.load(Ordering::SeqCst) == 1).await;

        let ordered = tokio::spawn({
            let driver = driver.clone();
            async move {
                let (detach_reply, detach_response) = oneshot::channel();
                driver
                    .send(DriverCommand::Detach {
                        thread_id: parent,
                        reply: detach_reply,
                    })
                    .await
                    .unwrap();
                let (second_link, second_response) = link(parent, "second-child-native");
                driver.link(second_link).await.unwrap();
                (detach_response, second_response)
            }
        });
        let (mut detach_response, mut second_response) = ordered.await.unwrap();
        assert!(detach_response.try_recv().is_err());
        assert!(second_response.try_recv().is_err());

        gate.notify_one();
        let first_child = first_response.await.unwrap().unwrap().unwrap();
        detach_response.await.unwrap();
        assert_eq!(second_response.await.unwrap().unwrap(), None);

        let first_file = load_thread(&store, project_id, first_child).await.unwrap();
        assert_eq!(first_file.kind, giskard_core::thread::ThreadKind::Subagent);
        assert_eq!(first_file.parent_thread_id, Some(parent));
        assert!(shared.coordinator(parent).await.is_none());
        assert_eq!(harness.claim_calls.load(Ordering::SeqCst), 1);
        assert_eq!(store.list_threads(project_id).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn owners_keep_running_while_an_admission_is_in_flight() {
        let (_shared, harness, driver, project_id, store) = setup();
        let parent = ThreadId::new();
        persist_thread(&store, project_id, parent).await;
        attach_primary(&driver, project_id, parent, "native").await;
        let gate = harness.gate_claims();
        let (request, _response) = link(parent, "child-native");
        driver.link(request).await.unwrap();
        wait_until(|| harness.claim_calls.load(Ordering::SeqCst) == 1).await;
        let turn = TurnId::new();
        assert!(harness.log(parent).append(AgentEvent::TurnStarted {
            thread: parent,
            turn
        }));
        assert!(harness.log(parent).append(AgentEvent::TurnCompleted {
            thread: parent,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        }));
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if store
                    .load_turn_records(project_id, parent)
                    .await
                    .unwrap()
                    .iter()
                    .any(|record| record.turn_id == turn)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        gate.notify_one();
    }

    #[tokio::test]
    async fn command_channel_close_does_not_abandon_inflight_discovery() {
        let (shared, harness, spawned_driver, project_id, _store) = setup();
        drop(spawned_driver);
        let (tx, rx) = mpsc::channel(1);
        let gate = Arc::new(tokio::sync::Notify::new());
        let discovered = ThreadId::new();
        let admitted = Admitted {
            binding: binding(project_id, discovered, "discovered"),
            classification: ClassificationPhase::Orphan,
            thread_id: discovered,
        };
        let work_gate = gate.clone();
        let concrete_harness = harness.clone();
        let trait_harness: Arc<dyn AgentHarness> = harness;
        let driver = ProjectEventDriver {
            project_id,
            rx,
            harness: Arc::downgrade(&trait_harness),
            shared: shared.clone(),
            owners: FuturesUnordered::new(),
            parked: Vec::new(),
            weak_tx: tx.downgrade(),
            discoveries: DiscoveryStream::closed(),
            discoveries_closed: true,
            admission: Some(InflightAdmission {
                work: Box::pin(async move {
                    work_gate.notified().await;
                    AdmissionOutcome {
                        result: Ok(Some(admitted)),
                    }
                }),
                source: AdmissionSource::Discovery,
            }),
            quiesced: false,
        };
        let task = tokio::spawn(async move {
            driver.run().await;
        });

        drop(tx);
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        gate.notify_one();

        wait_until(|| shared.discovery_records_processed.load(Ordering::SeqCst) == 1).await;
        let authority = shared.thread_authority(discovered).await.unwrap();
        assert!(authority.coordinator().await.is_some());
        concrete_harness.log(discovered).close();
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn a_link_reports_owner_installation_failure() {
        let (_shared, harness, driver, project_id, store) = setup();
        let parent = ThreadId::new();
        persist_thread(&store, project_id, parent).await;
        attach_primary(&driver, project_id, parent, "native").await;
        let gate = harness.gate_claims();
        let (request, response) = link(parent, "child-native");
        driver.link(request).await.unwrap();
        wait_until(|| harness.claim_calls.load(Ordering::SeqCst) == 1).await;
        drop(harness);
        gate.notify_one();

        assert!(matches!(
            response.await.unwrap(),
            Err(HarnessError::Protocol(message)) if message == "project harness is gone"
        ));
    }

    #[tokio::test]
    async fn a_quiesced_driver_refuses_links_and_drops_discoveries() {
        let (shared, harness, driver, project_id, store) = setup();
        let parent = ThreadId::new();
        persist_thread(&store, project_id, parent).await;
        attach_primary(&driver, project_id, parent, "native").await;
        driver.quiesce().await.unwrap();
        let (request, response) = link(parent, "child-native");
        driver.link(request).await.unwrap();
        assert!(
            matches!(response.await.unwrap(), Err(HarnessError::Protocol(message)) if message == "project is being deleted")
        );
        let discovered = ThreadId::new();
        harness.announce(ThreadDiscovered {
            thread: discovered,
            harness_thread_id: "discovered".into(),
            parent_harness_thread_id: None,
        });
        wait_until(|| shared.discovery_records_processed.load(Ordering::SeqCst) == 1).await;
        assert!(load_thread(&store, project_id, discovered).await.is_none());
    }

    #[tokio::test]
    async fn a_resumed_driver_accepts_links_after_failed_deletion() {
        let (_shared, _harness, driver, project_id, store) = setup();
        let parent = ThreadId::new();
        persist_thread(&store, project_id, parent).await;
        attach_primary(&driver, project_id, parent, "native").await;
        driver.quiesce().await.unwrap();
        driver.resume().await.unwrap();

        let (request, response) = link(parent, "child-native");
        driver.link(request).await.unwrap();
        let child = response.await.unwrap().unwrap().unwrap();
        let file = load_thread(&store, project_id, child).await.unwrap();
        assert_eq!(file.kind, giskard_core::thread::ThreadKind::Subagent);
    }

    #[tokio::test]
    async fn a_link_from_a_parent_without_a_live_owner_is_refused() {
        let (_shared, harness, driver, project_id, store) = setup();
        let parent = ThreadId::new();
        persist_thread(&store, project_id, parent).await;
        let (request, response) = link(parent, "child-native");
        driver.link(request).await.unwrap();
        assert_eq!(response.await.unwrap().unwrap(), None);
        assert_eq!(harness.claim_calls.load(Ordering::SeqCst), 0);
        assert_eq!(store.list_threads(project_id).await.unwrap().len(), 1);
    }
}
