use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use giskard_core::approval::ApprovalDecision;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ServerRequestId, ThreadId, TurnId};
use giskard_core::model::{ModelDescriptor, ModelRef};
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::turn::{TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_core::{HarnessError, TokenUsage};
use giskard_harness::{
    AgentEventStream, AgentHarness, EventLog, HarnessBootstrap, HarnessCapabilities,
    HarnessProvider, OpenThreadOptions, ThreadHandle,
};
use giskard_persist::store::ProjectConfig;
use giskard_server::HarnessFactory;
use tokio::sync::Notify;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// One trait call the server made, recorded on entry, before the script runs.
#[derive(Debug, Clone)]
pub enum Call {
    Open {
        thread: ThreadId,
        resume: Option<String>,
        model: ModelRef,
        workspace_root: PathBuf,
    },
    Claim {
        thread: ThreadId,
        harness_thread_id: String,
        workspace_root: PathBuf,
    },
    StartTurn {
        thread: ThreadId,
        turn: TurnId,
        input: UserInput,
        overrides: TurnOverrides,
    },
    SteerTurn {
        thread: ThreadId,
        expected_turn: TurnId,
        text: String,
    },
    RespondApproval {
        request: ApprovalId,
        decision: ApprovalDecision,
    },
    RespondServerRequest {
        request: ServerRequestId,
        response: ServerRequestResponse,
    },
    Interrupt {
        thread: ThreadId,
    },
    TerminateCommand {
        thread: ThreadId,
        process_id: String,
    },
    CompactThread {
        thread: ThreadId,
    },
    DeleteThread {
        thread: ThreadId,
        harness_thread_id: String,
    },
    Shutdown,
}

#[derive(Default)]
pub struct FakeCore {
    threads: Mutex<HashMap<ThreadId, Arc<EventLog>>>,
    native_routes: Mutex<HashMap<String, ThreadId>>,
    calls: Mutex<Vec<Call>>,
    calls_changed: Notify,
}

impl FakeCore {
    pub fn log(&self, thread: ThreadId) -> Arc<EventLog> {
        self.ensure_log(thread).0
    }

    pub fn ensure_log(&self, thread: ThreadId) -> (Arc<EventLog>, bool) {
        let mut threads = lock(&self.threads);
        if let Some(log) = threads.get(&thread) {
            return (log.clone(), false);
        }
        let log = Arc::new(EventLog::new());
        threads.insert(thread, log.clone());
        (log, true)
    }

    pub fn try_log(&self, thread: ThreadId) -> Option<Arc<EventLog>> {
        lock(&self.threads).get(&thread).cloned()
    }

    pub fn remove_log(&self, thread: ThreadId) -> Option<Arc<EventLog>> {
        lock(&self.threads).remove(&thread)
    }

    pub fn append(&self, thread: ThreadId, event: AgentEvent) {
        self.log(thread).append(event);
    }

    pub fn complete_turn(&self, thread: ThreadId, turn: TurnId) {
        self.append(
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
    }

    pub fn route(&self, harness_thread_id: &str) -> Option<ThreadId> {
        lock(&self.native_routes).get(harness_thread_id).copied()
    }

    /// Claim a native id for `thread`, keeping any owner already bound to it. The winner is
    /// returned, so a second claim of the same native id resolves to the first thread — the
    /// "one native owner" rule `claim_native_thread` enforces.
    pub fn bind_route(&self, harness_thread_id: &str, thread: ThreadId) -> ThreadId {
        *lock(&self.native_routes)
            .entry(harness_thread_id.to_owned())
            .or_insert(thread)
    }

    /// Point a native id at `thread`, replacing any previous owner. An open is authoritative
    /// about its own thread, so it overwrites rather than deferring to an earlier binding.
    pub fn set_route(&self, harness_thread_id: &str, thread: ThreadId) {
        lock(&self.native_routes).insert(harness_thread_id.to_owned(), thread);
    }

    pub fn seed_routes(&self, bootstrap: &HarnessBootstrap) {
        for binding in &bootstrap.known_threads {
            self.set_route(&binding.harness_thread_id, binding.thread_id);
        }
    }

    pub fn opened(&self, opts: &OpenThreadOptions, fallback_native_id: String) -> ThreadHandle {
        self.log(opts.thread);
        let native_id = opts.resume.clone().unwrap_or(fallback_native_id);
        // The handle names the thread the server asked to open. Resolving it through the route
        // table instead would hand back an earlier thread whenever two threads share a native id
        // — as two projects seeded from the same fixture do — and stream this thread's turns
        // into that one's log.
        self.set_route(&native_id, opts.thread);
        ThreadHandle {
            resumed_model: Some(opts.initial_model.clone()),
            ..ThreadHandle::opened(opts.thread, native_id, opts.workspace_root.clone())
        }
    }

    pub fn claimed(
        &self,
        thread: ThreadId,
        harness_thread_id: &str,
        workspace_root: &Path,
    ) -> ThreadHandle {
        let thread = self.bind_route(harness_thread_id, thread);
        self.ensure_log(thread);
        ThreadHandle::opened(
            thread,
            harness_thread_id.to_owned(),
            workspace_root.to_path_buf(),
        )
    }

    fn record(&self, call: Call) {
        lock(&self.calls).push(call);
        self.calls_changed.notify_waiters();
    }

    pub fn calls(&self) -> Vec<Call> {
        lock(&self.calls).clone()
    }

    pub fn count(&self, pred: impl Fn(&Call) -> bool) -> usize {
        lock(&self.calls).iter().filter(|call| pred(call)).count()
    }

    pub fn clear_calls(&self) {
        lock(&self.calls).clear();
    }

    pub async fn wait_for_call<T>(&self, pick: impl Fn(&Call) -> Option<T>) -> T {
        let wait = async {
            loop {
                let notified = self.calls_changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Some(value) = lock(&self.calls).iter().find_map(&pick) {
                    return value;
                }
                notified.await;
            }
        };
        match tokio::time::timeout(Duration::from_secs(5), wait).await {
            Ok(value) => value,
            Err(_) => panic!(
                "harness call was not observed; recorded: {:?}",
                self.calls()
            ),
        }
    }

    pub async fn wait_for_calls(&self, pred: impl Fn(&Call) -> bool, at_least: usize) {
        let wait = async {
            loop {
                let notified = self.calls_changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if lock(&self.calls).iter().filter(|call| pred(call)).count() >= at_least {
                    return;
                }
                notified.await;
            }
        };
        let result = tokio::time::timeout(Duration::from_secs(5), wait).await;
        assert!(
            result.is_ok(),
            "expected at least {at_least} matching harness calls; recorded: {:?}",
            self.calls()
        );
    }

    /// Readers on `thread`'s log, counting a thread that was never opened as zero rather than
    /// creating a log for it. Asking how many readers a thread has is a question, not a reason
    /// to bring it into existence.
    pub fn readers(&self, thread: ThreadId) -> usize {
        self.try_log(thread).map_or(0, |log| log.reader_count())
    }

    pub async fn wait_for_readers(&self, thread: ThreadId, at_least: usize) {
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.readers(thread) >= at_least {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            result.is_ok(),
            "expected at least {at_least} readers for {thread}"
        );
    }

    pub async fn wait_for_reader_count(&self, thread: ThreadId, exactly: usize) {
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.readers(thread) == exactly {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            result.is_ok(),
            "expected exactly {exactly} readers for {thread}"
        );
    }
}

pub struct TurnCall {
    pub thread: ThreadId,
    pub harness_thread_id: String,
    pub turn: TurnId,
    pub input: UserInput,
    pub overrides: TurnOverrides,
    pub log: Arc<EventLog>,
}

pub struct SteerCall {
    pub thread: ThreadId,
    pub harness_thread_id: String,
    pub expected_turn: TurnId,
    pub text: String,
    pub log: Arc<EventLog>,
}

#[async_trait]
pub trait Script: Send + Sync + 'static {
    fn capabilities(&self) -> HarnessCapabilities {
        caps::TURNS
    }
    fn native_thread_id(&self, thread: ThreadId) -> String {
        format!("test_{thread}")
    }
    async fn open_thread(
        &self,
        core: &FakeCore,
        opts: &OpenThreadOptions,
    ) -> Result<ThreadHandle, HarnessError> {
        Ok(core.opened(opts, self.native_thread_id(opts.thread)))
    }
    async fn claim_native_thread(
        &self,
        core: &FakeCore,
        thread: ThreadId,
        harness_thread_id: &str,
        workspace_root: &Path,
    ) -> Result<ThreadHandle, HarnessError> {
        Ok(core.claimed(thread, harness_thread_id, workspace_root))
    }
    async fn start_turn(&self, _core: &FakeCore, _call: &TurnCall) -> Result<(), HarnessError> {
        Ok(())
    }
    async fn steer_turn(&self, _core: &FakeCore, _call: &SteerCall) -> Result<(), HarnessError> {
        Ok(())
    }
    async fn respond_approval(
        &self,
        _core: &FakeCore,
        _request: ApprovalId,
        _decision: ApprovalDecision,
    ) -> Result<(), HarnessError> {
        Ok(())
    }
    async fn respond_server_request(
        &self,
        _core: &FakeCore,
        _request: ServerRequestId,
        _response: ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        Ok(())
    }
    async fn interrupt(
        &self,
        _core: &FakeCore,
        _thread: &ThreadHandle,
    ) -> Result<(), HarnessError> {
        Ok(())
    }
    async fn terminate_command(
        &self,
        _core: &FakeCore,
        _thread: &ThreadHandle,
        process_id: &str,
    ) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(format!(
            "command termination is not supported for process {process_id}"
        )))
    }
    async fn compact_thread(
        &self,
        _core: &FakeCore,
        thread: &ThreadHandle,
    ) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(format!(
            "context compaction is not supported for thread {}",
            thread.harness_thread_id
        )))
    }
    async fn delete_thread(
        &self,
        core: &FakeCore,
        thread: &ThreadHandle,
    ) -> Result<(), HarnessError> {
        core.remove_log(thread.thread);
        Ok(())
    }
    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError> {
        Ok(Vec::new())
    }
    async fn list_providers(&self) -> Result<Vec<HarnessProvider>, HarnessError> {
        Err(HarnessError::Unsupported(
            "provider listing is not supported by this harness".into(),
        ))
    }
    async fn shutdown(&self, _core: &FakeCore) -> Result<(), HarnessError> {
        Ok(())
    }
}

#[async_trait]
impl Script for () {}

pub struct FakeHarness<S: Script> {
    pub script: S,
    pub core: FakeCore,
}

impl<S: Script> FakeHarness<S> {
    pub fn new(script: S) -> Arc<Self> {
        Arc::new(Self {
            script,
            core: FakeCore::default(),
        })
    }
}

#[async_trait]
impl<S: Script> AgentHarness for FakeHarness<S> {
    fn capabilities(&self) -> HarnessCapabilities {
        self.script.capabilities()
    }
    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError> {
        self.script.list_models().await
    }
    async fn list_providers(&self) -> Result<Vec<HarnessProvider>, HarnessError> {
        self.script.list_providers().await
    }
    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        self.core.record(Call::Open {
            thread: opts.thread,
            resume: opts.resume.clone(),
            model: opts.initial_model.clone(),
            workspace_root: opts.workspace_root.clone(),
        });
        self.script.open_thread(&self.core, &opts).await
    }
    async fn claim_native_thread(
        &self,
        thread: ThreadId,
        harness_thread_id: String,
        workspace_root: PathBuf,
    ) -> Result<ThreadHandle, HarnessError> {
        self.core.record(Call::Claim {
            thread,
            harness_thread_id: harness_thread_id.clone(),
            workspace_root: workspace_root.clone(),
        });
        self.script
            .claim_native_thread(&self.core, thread, &harness_thread_id, &workspace_root)
            .await
    }
    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        input: UserInput,
        overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let turn = TurnId::new();
        self.core.record(Call::StartTurn {
            thread: thread.thread,
            turn,
            input: input.clone(),
            overrides: overrides.clone(),
        });
        let call = TurnCall {
            thread: thread.thread,
            harness_thread_id: thread.harness_thread_id.clone(),
            turn,
            input,
            overrides,
            log: self.core.log(thread.thread),
        };
        self.script
            .start_turn(&self.core, &call)
            .await
            .map(|()| turn)
    }
    async fn steer_turn(
        &self,
        thread: &ThreadHandle,
        expected_turn: TurnId,
        text: String,
    ) -> Result<(), HarnessError> {
        self.core.record(Call::SteerTurn {
            thread: thread.thread,
            expected_turn,
            text: text.clone(),
        });
        let call = SteerCall {
            thread: thread.thread,
            harness_thread_id: thread.harness_thread_id.clone(),
            expected_turn,
            text,
            log: self.core.log(thread.thread),
        };
        self.script.steer_turn(&self.core, &call).await
    }
    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream {
        self.core
            .try_log(thread.thread)
            .map(|log| AgentEventStream::new(log.reader()))
            .unwrap_or_else(AgentEventStream::closed)
    }
    async fn respond_approval(
        &self,
        request: ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<(), HarnessError> {
        self.core.record(Call::RespondApproval {
            request: request.clone(),
            decision: decision.clone(),
        });
        self.script
            .respond_approval(&self.core, request, decision)
            .await
    }
    async fn respond_server_request(
        &self,
        request: ServerRequestId,
        response: ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        self.core.record(Call::RespondServerRequest {
            request: request.clone(),
            response: response.clone(),
        });
        self.script
            .respond_server_request(&self.core, request, response)
            .await
    }
    async fn interrupt(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        self.core.record(Call::Interrupt {
            thread: thread.thread,
        });
        self.script.interrupt(&self.core, thread).await
    }
    async fn terminate_command(
        &self,
        thread: &ThreadHandle,
        process_id: &str,
    ) -> Result<(), HarnessError> {
        self.core.record(Call::TerminateCommand {
            thread: thread.thread,
            process_id: process_id.to_owned(),
        });
        self.script
            .terminate_command(&self.core, thread, process_id)
            .await
    }
    async fn compact_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        self.core.record(Call::CompactThread {
            thread: thread.thread,
        });
        self.script.compact_thread(&self.core, thread).await
    }
    async fn delete_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        self.core.record(Call::DeleteThread {
            thread: thread.thread,
            harness_thread_id: thread.harness_thread_id.clone(),
        });
        self.script.delete_thread(&self.core, thread).await
    }
    async fn shutdown(&self) -> Result<(), HarnessError> {
        self.core.record(Call::Shutdown);
        self.script.shutdown(&self.core).await
    }
}

pub struct Gate {
    held: AtomicBool,
    blocked: AtomicBool,
    released: Notify,
    /// Carries the blocked signal on its own channel. Sharing `released` for it would make two
    /// callers waiting in `pass` wake each other: each block notifies, the other wakes, finds the
    /// gate still held, re-announces, and the pair spins until the release lands.
    blocked_changed: Notify,
}

impl Gate {
    pub fn open() -> Self {
        Self {
            held: AtomicBool::new(false),
            blocked: AtomicBool::new(false),
            released: Notify::new(),
            blocked_changed: Notify::new(),
        }
    }
    pub fn held() -> Self {
        Self {
            held: AtomicBool::new(true),
            blocked: AtomicBool::new(false),
            released: Notify::new(),
            blocked_changed: Notify::new(),
        }
    }
    pub fn hold(&self) {
        self.held.store(true, Ordering::SeqCst);
    }
    pub fn release(&self) {
        self.held.store(false, Ordering::SeqCst);
        self.released.notify_waiters();
    }
    pub async fn pass(&self) {
        while self.held.load(Ordering::SeqCst) {
            if !self.blocked.swap(true, Ordering::SeqCst) {
                self.blocked_changed.notify_waiters();
            }
            let notified = self.released.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.held.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
    pub async fn wait_blocked(&self) {
        let wait = async {
            loop {
                let notified = self.blocked_changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.blocked.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            }
        };
        assert!(
            tokio::time::timeout(Duration::from_secs(5), wait)
                .await
                .is_ok(),
            "gate was not blocked"
        );
    }
}

pub mod caps {
    use giskard_harness::HarnessCapabilities;
    pub const TURNS: HarnessCapabilities = HarnessCapabilities {
        turn_steering: false,
        live_approvals: true,
        plan_build_modes: true,
        per_turn_model: true,
        reasoning_effort: true,
        structured_diffs: true,
        resumable_threads: true,
        model_listing: false,
        provider_listing: false,
        token_usage: true,
        mcp_status: false,
        mcp_reload: false,
        mcp_oauth_login: false,
        context_compaction: false,
    };
    pub const ACTIVITY: HarnessCapabilities = HarnessCapabilities {
        structured_diffs: false,
        token_usage: false,
        ..TURNS
    };
    pub const STEERING: HarnessCapabilities = HarnessCapabilities {
        turn_steering: true,
        ..TURNS
    };
    pub const RESUMABLE: HarnessCapabilities = HarnessCapabilities {
        turn_steering: false,
        resumable_threads: true,
        live_approvals: false,
        plan_build_modes: false,
        per_turn_model: false,
        reasoning_effort: false,
        structured_diffs: false,
        model_listing: false,
        provider_listing: false,
        token_usage: false,
        mcp_status: false,
        mcp_reload: false,
        mcp_oauth_login: false,
        context_compaction: false,
    };
    pub const RESUMABLE_COMPACTION: HarnessCapabilities = HarnessCapabilities {
        turn_steering: false,
        resumable_threads: true,
        context_compaction: true,
        live_approvals: false,
        plan_build_modes: false,
        per_turn_model: false,
        reasoning_effort: false,
        structured_diffs: false,
        model_listing: false,
        provider_listing: false,
        token_usage: false,
        mcp_status: false,
        mcp_reload: false,
        mcp_oauth_login: false,
    };
    pub const REPLAY: HarnessCapabilities = HarnessCapabilities {
        turn_steering: false,
        live_approvals: true,
        plan_build_modes: true,
        per_turn_model: true,
        reasoning_effort: true,
        structured_diffs: true,
        resumable_threads: true,
        model_listing: false,
        provider_listing: false,
        token_usage: true,
        mcp_status: true,
        mcp_reload: true,
        mcp_oauth_login: false,
        context_compaction: true,
    };
}

struct FakeFactory<S: Script>(Arc<FakeHarness<S>>);

#[async_trait]
impl<S: Script> HarnessFactory for FakeFactory<S> {
    async fn create(
        &self,
        _config: &ProjectConfig,
        bootstrap: HarnessBootstrap,
    ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        self.0.core.seed_routes(&bootstrap);
        Ok(self.0.clone())
    }
}

pub fn factory<S: Script>(harness: Arc<FakeHarness<S>>) -> Arc<dyn HarnessFactory> {
    Arc::new(FakeFactory(harness))
}

#[cfg(test)]
mod tests {
    use super::*;
    use giskard_core::turn::{Mode, PermissionPreset};
    use giskard_harness::{EventStreamError, thread_update_channel};

    fn model() -> ModelRef {
        ModelRef {
            provider: "test".into(),
            model: "model".into(),
            reasoning_effort: None,
        }
    }

    fn overrides() -> TurnOverrides {
        TurnOverrides {
            model: Some(model()),
            mode: Mode::Build,
            permission_preset: PermissionPreset::AskFirst,
        }
    }

    struct Fails;

    #[async_trait]
    impl Script for Fails {
        async fn start_turn(&self, _core: &FakeCore, _call: &TurnCall) -> Result<(), HarnessError> {
            Err(HarnessError::Protocol("script failed".into()))
        }
    }

    struct FailsSteering;

    #[async_trait]
    impl Script for FailsSteering {
        async fn steer_turn(
            &self,
            _core: &FakeCore,
            _call: &SteerCall,
        ) -> Result<(), HarnessError> {
            Err(HarnessError::Protocol("steering failed".into()))
        }
    }

    #[tokio::test]
    async fn calls_are_recorded_on_entry_even_when_the_script_fails() {
        let harness = FakeHarness::new(Fails);
        let thread = ThreadHandle::opened(ThreadId::new(), "native".into(), PathBuf::from("/tmp"));
        let error = harness
            .start_turn(&thread, UserInput::text("hello"), overrides())
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            HarnessError::Protocol("script failed".into()).to_string()
        );
        assert_eq!(
            harness
                .core
                .count(|call| matches!(call, Call::StartTurn { .. })),
            1
        );
    }

    #[tokio::test]
    async fn exact_steering_call_is_recorded_before_the_script_runs() {
        let harness = FakeHarness::new(FailsSteering);
        let thread = ThreadHandle::opened(ThreadId::new(), "native".into(), PathBuf::from("/tmp"));
        let expected_turn = TurnId::new();
        let error = harness
            .steer_turn(&thread, expected_turn, "redirect now".into())
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            HarnessError::Protocol("steering failed".into()).to_string()
        );
        let call = harness
            .core
            .calls()
            .into_iter()
            .next()
            .expect("steering call must be recorded");
        assert!(matches!(
            call,
            Call::SteerTurn {
                thread: recorded_thread,
                expected_turn: recorded_turn,
                text,
            } if recorded_thread == thread.thread
                && recorded_turn == expected_turn
                && text == "redirect now"
        ));
    }

    #[tokio::test]
    async fn wait_for_call_wakes_without_polling() {
        let core = Arc::new(FakeCore::default());
        let recorder = core.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            recorder.record(Call::Shutdown);
        });
        core.wait_for_call(|call| matches!(call, Call::Shutdown).then_some(()))
            .await;
        let waiting = core.clone();
        let panic = tokio::spawn(async move {
            waiting
                .wait_for_call(|call| matches!(call, Call::Interrupt { .. }).then_some(()))
                .await
        })
        .await
        .unwrap_err();
        assert!(panic.is_panic());
    }

    #[tokio::test]
    async fn gate_release_before_pass_is_not_lost() {
        let gate = Gate::held();
        gate.release();
        gate.pass().await;

        let gate = Arc::new(Gate::held());
        let passing = gate.clone();
        let task = tokio::spawn(async move { passing.pass().await });
        gate.wait_blocked().await;
        gate.release();
        task.await.unwrap();
    }

    /// Two callers may be blocked at once. They must park until the release rather than wake each
    /// other: the blocked signal travels on its own channel precisely so that a pair waiting here
    /// cannot notify each other into a spin. A functional test cannot see the spin itself — the
    /// old code still finished on release — but it does pin the two-caller path.
    #[tokio::test]
    async fn two_blocked_callers_both_pass_on_one_release() {
        let gate = Arc::new(Gate::held());
        let first = tokio::spawn({
            let gate = gate.clone();
            async move { gate.pass().await }
        });
        let second = tokio::spawn({
            let gate = gate.clone();
            async move { gate.pass().await }
        });

        gate.wait_blocked().await;
        gate.release();

        first.await.unwrap();
        second.await.unwrap();
    }

    /// Counting readers must not conjure a log. A thread nobody opened has no readers, and
    /// asking about it leaves the core as it was.
    #[test]
    fn counting_readers_does_not_create_a_log() {
        let core = FakeCore::default();
        let thread = ThreadId::new();
        assert_eq!(core.readers(thread), 0);
        assert!(core.try_log(thread).is_none());
        assert!(core.ensure_log(thread).1, "the log must still be new");
    }

    #[test]
    fn claimed_binds_the_route_once() {
        let core = FakeCore::default();
        let first = ThreadId::new();
        let second = ThreadId::new();
        assert_eq!(
            core.claimed(first, "native", Path::new("/one")).thread,
            first
        );
        assert_eq!(
            core.claimed(second, "native", Path::new("/two")).thread,
            first
        );
        assert!(!core.ensure_log(first).1);
        assert!(core.ensure_log(second).1);
        assert!(!core.ensure_log(second).1);
    }

    #[tokio::test]
    async fn subscribe_is_closed_for_an_unknown_thread() {
        let harness = FakeHarness::new(());
        let thread = ThreadId::new();
        let handle = ThreadHandle::opened(thread, "native".into(), PathBuf::from("/tmp"));
        let mut closed = harness.subscribe(&handle);
        assert!(matches!(closed.recv().await, Err(EventStreamError::Closed)));

        let (updates, _) = thread_update_channel();
        harness
            .open_thread(OpenThreadOptions {
                project: giskard_core::ids::ProjectId::new(),
                thread,
                workspace_root: PathBuf::from("/tmp"),
                resume: None,
                initial_model: model(),
                updates,
            })
            .await
            .unwrap();
        let mut live = harness.subscribe(&handle);
        assert!(live.try_recv().is_none());
    }

    /// Two threads may carry the same native id — two projects seeded from one fixture both
    /// persist `th_test`. Each open still owns its own thread and its own log, and the later
    /// open takes the route; resolving the handle through the route table instead would stream
    /// the second thread's turns into the first thread's log.
    #[test]
    fn opening_a_shared_native_id_keeps_each_thread_its_own() {
        let core = FakeCore::default();
        let first = ThreadId::new();
        let second = ThreadId::new();
        let opts = |thread| OpenThreadOptions {
            project: giskard_core::ids::ProjectId::new(),
            thread,
            workspace_root: PathBuf::from("/tmp"),
            resume: Some("th_test".into()),
            initial_model: model(),
            updates: thread_update_channel().0,
        };

        let one = core.opened(&opts(first), "unused".into());
        let two = core.opened(&opts(second), "unused".into());

        assert_eq!(one.thread, first);
        assert_eq!(two.thread, second);
        assert_eq!(one.harness_thread_id, "th_test");
        assert_eq!(two.harness_thread_id, "th_test");
        assert!(!Arc::ptr_eq(&core.log(first), &core.log(second)));
        assert_eq!(core.route("th_test"), Some(second));
    }
}
