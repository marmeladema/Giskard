use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use futures_util::{SinkExt, StreamExt};
use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalRequest};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ProjectId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{
    CommandExecutionStart, Item, ItemDelta, ItemKind, ItemPayload, ItemStart,
};
use giskard_core::model::ModelRef;
use giskard_core::server_request::{ServerRequest, ServerRequestResponse};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{ApprovalPolicy, Mode, TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
};
use giskard_harness_replay::{ReplayFixture, ReplayHarness};
use giskard_persist::store::ProjectConfig;
use giskard_proto::{
    ClientMessage, ErrorSeverity, ServerMessage, ThreadActivity, ThreadActivityKind, WireAgentEvent,
};
use giskard_server::{AppState, HarnessFactory, build_app};

struct TestFactory {
    fixture: ReplayFixture,
}

#[async_trait::async_trait]
impl HarnessFactory for TestFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, HarnessError> {
        Ok(Arc::new(ReplayHarness::from_fixture(self.fixture.clone())))
    }
}

struct NoMcpFactory;
struct UnsupportedCompactionFactory;
struct SlowCompactionFactory;
struct SlowStartFactory {
    harness: Arc<SlowStartHarness>,
}
struct ActivityFactory {
    harness: Arc<ActivityHarness>,
}
struct CountingOpenFactory {
    harness: Arc<CountingOpenHarness>,
}

#[async_trait::async_trait]
impl HarnessFactory for NoMcpFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, HarnessError> {
        Ok(Arc::new(NoMcpHarness))
    }
}

#[async_trait::async_trait]
impl HarnessFactory for UnsupportedCompactionFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, HarnessError> {
        Ok(Arc::new(UnsupportedCompactionHarness::default()))
    }
}

#[async_trait::async_trait]
impl HarnessFactory for SlowCompactionFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, HarnessError> {
        Ok(Arc::new(SlowCompactionHarness::default()))
    }
}

#[async_trait::async_trait]
impl HarnessFactory for SlowStartFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, HarnessError> {
        Ok(self.harness.clone())
    }
}

#[async_trait::async_trait]
impl HarnessFactory for ActivityFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, HarnessError> {
        Ok(self.harness.clone())
    }
}

#[async_trait::async_trait]
impl HarnessFactory for CountingOpenFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn giskard_harness::AgentHarness>, HarnessError> {
        Ok(self.harness.clone())
    }
}

struct NoMcpHarness;

#[derive(Default)]
struct UnsupportedCompactionHarness {
    threads: tokio::sync::Mutex<HashMap<ThreadId, tokio::sync::broadcast::Sender<AgentEvent>>>,
}

#[derive(Default)]
struct SlowCompactionHarness {
    threads: tokio::sync::Mutex<HashMap<ThreadId, tokio::sync::broadcast::Sender<AgentEvent>>>,
}

struct SlowStartHarness {
    threads: tokio::sync::Mutex<HashMap<ThreadId, tokio::sync::broadcast::Sender<AgentEvent>>>,
    start_calls: AtomicUsize,
    hold_first_start: AtomicBool,
    release_first_start: AtomicBool,
}

#[derive(Default)]
struct ActivityHarness {
    threads: tokio::sync::Mutex<HashMap<ThreadId, tokio::sync::broadcast::Sender<AgentEvent>>>,
    approval_responses: tokio::sync::Mutex<Vec<(ApprovalId, ApprovalDecision)>>,
    server_responses: tokio::sync::Mutex<Vec<(ServerRequestId, ServerRequestResponse)>>,
    pending_approvals: tokio::sync::Mutex<HashMap<ApprovalId, (ThreadId, TurnId)>>,
    pending_server_requests: tokio::sync::Mutex<HashMap<ServerRequestId, (ThreadId, TurnId)>>,
}

#[derive(Default)]
struct CountingOpenHarness {
    threads: tokio::sync::Mutex<HashMap<ThreadId, tokio::sync::broadcast::Sender<AgentEvent>>>,
    open_calls: AtomicUsize,
    start_calls: AtomicUsize,
    delete_calls: AtomicUsize,
    shutdown_calls: AtomicUsize,
    opened_models: tokio::sync::Mutex<Vec<ModelRef>>,
    started_models: tokio::sync::Mutex<Vec<Option<ModelRef>>>,
    started_inputs: tokio::sync::Mutex<Vec<String>>,
    start_error: tokio::sync::Mutex<Option<HarnessError>>,
}

impl SlowStartHarness {
    fn new() -> Self {
        Self {
            threads: tokio::sync::Mutex::new(HashMap::new()),
            start_calls: AtomicUsize::new(0),
            hold_first_start: AtomicBool::new(true),
            release_first_start: AtomicBool::new(false),
        }
    }

    async fn wait_for_start_calls(&self, expected: usize) {
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        loop {
            if self.start_calls.load(Ordering::SeqCst) >= expected {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timed out waiting for {expected} start_turn calls");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    fn start_calls(&self) -> usize {
        self.start_calls.load(Ordering::SeqCst)
    }

    fn release_first_start(&self) {
        self.release_first_start.store(true, Ordering::SeqCst);
    }
}

impl ActivityHarness {
    async fn wait_for_approval_response(&self) -> (ApprovalId, ApprovalDecision) {
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        loop {
            if let Some(response) = self.approval_responses.lock().await.first().cloned() {
                return response;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("approval response did not reach harness");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_server_response(&self) -> (ServerRequestId, ServerRequestResponse) {
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        loop {
            if let Some(response) = self.server_responses.lock().await.first().cloned() {
                return response;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("server request response did not reach harness");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    async fn complete_turn(&self, thread: ThreadId, turn: TurnId) -> Result<(), HarnessError> {
        let Some(sender) = self.threads.lock().await.get(&thread).cloned() else {
            return Err(HarnessError::ThreadNotFound(thread));
        };
        let _ = sender.send(AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        });
        Ok(())
    }
}

impl CountingOpenHarness {
    fn open_calls(&self) -> usize {
        self.open_calls.load(Ordering::SeqCst)
    }

    async fn opened_models(&self) -> Vec<ModelRef> {
        self.opened_models.lock().await.clone()
    }

    fn start_calls(&self) -> usize {
        self.start_calls.load(Ordering::SeqCst)
    }

    fn delete_calls(&self) -> usize {
        self.delete_calls.load(Ordering::SeqCst)
    }

    fn shutdown_calls(&self) -> usize {
        self.shutdown_calls.load(Ordering::SeqCst)
    }

    async fn started_models(&self) -> Vec<Option<ModelRef>> {
        self.started_models.lock().await.clone()
    }

    async fn started_inputs(&self) -> Vec<String> {
        self.started_inputs.lock().await.clone()
    }

    async fn fail_start_with(&self, error: HarnessError) {
        *self.start_error.lock().await = Some(error);
    }
}

#[async_trait::async_trait]
impl AgentHarness for UnsupportedCompactionHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        HarnessCapabilities {
            live_approvals: false,
            plan_build_modes: false,
            per_turn_model: false,
            reasoning_effort: false,
            structured_diffs: false,
            resumable_threads: true,
            model_listing: false,
            token_usage: false,
            mcp_status: false,
            mcp_reload: false,
            mcp_oauth_login: false,
            context_compaction: false,
        }
    }

    async fn list_models(&self) -> Result<Vec<giskard_core::ModelDescriptor>, HarnessError> {
        Ok(Vec::new())
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        let thread = opts.thread.unwrap_or_default();
        let (tx, _) = tokio::sync::broadcast::channel(16);
        self.threads.lock().await.insert(thread, tx);
        Ok(ThreadHandle {
            thread,
            harness_thread_id: opts.resume.unwrap_or_else(|| format!("test_{thread}")),
            warning: None,
            resumed_model: Some(opts.initial_model.clone()),
        })
    }

    async fn start_turn(
        &self,
        _thread: &ThreadHandle,
        _input: UserInput,
        _overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        Err(HarnessError::Unsupported(
            "turns are not supported by this harness".into(),
        ))
    }

    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream {
        if let Ok(threads) = self.threads.try_lock() {
            if let Some(sender) = threads.get(&thread.thread) {
                return AgentEventStream::new(sender.subscribe());
            }
        }
        let (_, rx) = tokio::sync::broadcast::channel(1);
        AgentEventStream::new(rx)
    }

    async fn respond_approval(
        &self,
        _req: ApprovalId,
        _decision: ApprovalDecision,
    ) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(
            "approvals are not supported by this harness".into(),
        ))
    }

    async fn respond_server_request(
        &self,
        _req: ServerRequestId,
        _response: ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(
            "server requests are not supported by this harness".into(),
        ))
    }

    async fn interrupt(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(
            "interrupts are not supported by this harness".into(),
        ))
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl AgentHarness for SlowCompactionHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        HarnessCapabilities {
            live_approvals: false,
            plan_build_modes: false,
            per_turn_model: false,
            reasoning_effort: false,
            structured_diffs: false,
            resumable_threads: true,
            model_listing: false,
            token_usage: false,
            mcp_status: false,
            mcp_reload: false,
            mcp_oauth_login: false,
            context_compaction: true,
        }
    }

    async fn list_models(&self) -> Result<Vec<giskard_core::ModelDescriptor>, HarnessError> {
        Ok(Vec::new())
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        let thread = opts.thread.unwrap_or_default();
        let (tx, _) = tokio::sync::broadcast::channel(32);
        self.threads.lock().await.insert(thread, tx);
        Ok(ThreadHandle {
            thread,
            harness_thread_id: opts.resume.unwrap_or_else(|| format!("test_{thread}")),
            warning: None,
            resumed_model: Some(opts.initial_model.clone()),
        })
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        _input: UserInput,
        _overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let Some(sender) = self.threads.lock().await.get(&thread.thread).cloned() else {
            return Err(HarnessError::ThreadNotFound(thread.thread));
        };
        let thread_id = thread.thread;
        let turn = TurnId::new();
        tokio::spawn(async move {
            let item = Item {
                id: ItemId::new(),
                harness_item_id: format!("reply_{turn}"),
                payload: ItemPayload::AgentMessage {
                    text: "other thread reply".into(),
                },
                created_at: chrono::Utc::now(),
            };
            let _ = sender.send(AgentEvent::TurnStarted {
                thread: thread_id,
                turn,
            });
            tokio::task::yield_now().await;
            let _ = sender.send(AgentEvent::ItemCompleted {
                thread: thread_id,
                turn,
                item,
            });
            tokio::task::yield_now().await;
            let _ = sender.send(AgentEvent::TurnCompleted {
                thread: thread_id,
                turn,
                usage: TokenUsage::default(),
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
            });
        });
        Ok(turn)
    }

    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream {
        if let Ok(threads) = self.threads.try_lock() {
            if let Some(sender) = threads.get(&thread.thread) {
                return AgentEventStream::new(sender.subscribe());
            }
        }
        let (_, rx) = tokio::sync::broadcast::channel(1);
        AgentEventStream::new(rx)
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

    async fn compact_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        let Some(sender) = self.threads.lock().await.get(&thread.thread).cloned() else {
            return Err(HarnessError::ThreadNotFound(thread.thread));
        };
        let thread_id = thread.thread;
        tokio::spawn(async move {
            let turn = TurnId::new();
            let _ = sender.send(AgentEvent::TurnStarted {
                thread: thread_id,
                turn,
            });
            tokio::task::yield_now().await;
            let _ = sender.send(AgentEvent::ItemCompleted {
                thread: thread_id,
                turn,
                item: Item {
                    id: ItemId::new(),
                    harness_item_id: format!("compact_{turn}"),
                    payload: ItemPayload::Activity {
                        title: "Context compacted".into(),
                        detail: None,
                        metadata: None,
                    },
                    created_at: chrono::Utc::now(),
                },
            });
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            let _ = sender.send(AgentEvent::TurnCompleted {
                thread: thread_id,
                turn,
                usage: TokenUsage::default(),
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
            });
        });
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl AgentHarness for ActivityHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        HarnessCapabilities {
            live_approvals: true,
            plan_build_modes: true,
            per_turn_model: true,
            reasoning_effort: true,
            structured_diffs: false,
            resumable_threads: true,
            model_listing: false,
            token_usage: false,
            mcp_status: false,
            mcp_reload: false,
            mcp_oauth_login: false,
            context_compaction: false,
        }
    }

    async fn list_models(&self) -> Result<Vec<giskard_core::ModelDescriptor>, HarnessError> {
        Ok(Vec::new())
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        let thread = opts.thread.unwrap_or_default();
        let (tx, _) = tokio::sync::broadcast::channel(32);
        self.threads.lock().await.insert(thread, tx);
        Ok(ThreadHandle {
            thread,
            harness_thread_id: opts.resume.unwrap_or_else(|| format!("test_{thread}")),
            warning: None,
            resumed_model: Some(opts.initial_model.clone()),
        })
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        input: UserInput,
        _overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let Some(sender) = self.threads.lock().await.get(&thread.thread).cloned() else {
            return Err(HarnessError::ThreadNotFound(thread.thread));
        };
        let thread_id = thread.thread;
        let turn = TurnId::new();
        let text = input.as_text().unwrap_or_default().to_string();
        let _ = sender.send(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        });

        if text.contains("approval") {
            let approval_id = ApprovalId(format!("approval_{thread_id}"));
            self.pending_approvals
                .lock()
                .await
                .insert(approval_id.clone(), (thread_id, turn));
            let _ = sender.send(AgentEvent::ApprovalRequested {
                thread: thread_id,
                turn,
                request: ApprovalRequest {
                    id: approval_id,
                    kind: ApprovalKind::CommandExecution {
                        command: "cargo test".into(),
                        cwd: "/tmp".into(),
                    },
                    reason: Some("inactive approval".into()),
                    metadata: Vec::new(),
                    available: vec![ApprovalDecision::Accept, ApprovalDecision::Decline],
                },
            });
        } else if text.contains("server request") {
            let request_id = ServerRequestId(format!("server_request_{thread_id}"));
            self.pending_server_requests
                .lock()
                .await
                .insert(request_id.clone(), (thread_id, turn));
            let _ = sender.send(AgentEvent::ServerRequestReceived {
                thread: thread_id,
                turn: Some(turn),
                request: ServerRequest {
                    id: request_id,
                    method: "item/tool/requestUserInput".into(),
                    params: serde_json::json!({
                        "questions": [{
                            "id": "confirm",
                            "header": "Confirm",
                            "question": "Continue?",
                            "options": [{ "label": "Yes", "description": "Continue" }],
                        }]
                    }),
                    received_at: chrono::Utc::now(),
                },
            });
        } else {
            self.complete_turn(thread_id, turn).await?;
        }

        Ok(turn)
    }

    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream {
        if let Ok(threads) = self.threads.try_lock() {
            if let Some(sender) = threads.get(&thread.thread) {
                return AgentEventStream::new(sender.subscribe());
            }
        }
        let (_, rx) = tokio::sync::broadcast::channel(1);
        AgentEventStream::new(rx)
    }

    async fn respond_approval(
        &self,
        req: ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<(), HarnessError> {
        self.approval_responses
            .lock()
            .await
            .push((req.clone(), decision));
        let Some((thread, turn)) = self.pending_approvals.lock().await.remove(&req) else {
            return Err(HarnessError::Protocol(format!(
                "unknown approval response {req}"
            )));
        };
        self.complete_turn(thread, turn).await
    }

    async fn respond_server_request(
        &self,
        req: ServerRequestId,
        response: ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        self.server_responses
            .lock()
            .await
            .push((req.clone(), response));
        let Some((thread, turn)) = self.pending_server_requests.lock().await.remove(&req) else {
            return Err(HarnessError::Protocol(format!(
                "unknown server request response {req}"
            )));
        };
        self.complete_turn(thread, turn).await
    }

    async fn interrupt(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl AgentHarness for SlowStartHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        HarnessCapabilities {
            live_approvals: false,
            plan_build_modes: false,
            per_turn_model: false,
            reasoning_effort: false,
            structured_diffs: false,
            resumable_threads: true,
            model_listing: false,
            token_usage: false,
            mcp_status: false,
            mcp_reload: false,
            mcp_oauth_login: false,
            context_compaction: true,
        }
    }

    async fn list_models(&self) -> Result<Vec<giskard_core::ModelDescriptor>, HarnessError> {
        Ok(Vec::new())
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        let thread = opts.thread.unwrap_or_default();
        let (tx, _) = tokio::sync::broadcast::channel(32);
        self.threads.lock().await.insert(thread, tx);
        Ok(ThreadHandle {
            thread,
            harness_thread_id: opts.resume.unwrap_or_else(|| format!("test_{thread}")),
            warning: None,
            resumed_model: Some(opts.initial_model.clone()),
        })
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        input: UserInput,
        _overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let Some(sender) = self.threads.lock().await.get(&thread.thread).cloned() else {
            return Err(HarnessError::ThreadNotFound(thread.thread));
        };
        let call = self.start_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == 1 && self.hold_first_start.swap(false, Ordering::SeqCst) {
            while !self.release_first_start.load(Ordering::SeqCst) {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        }

        let thread_id = thread.thread;
        let turn = TurnId::new();
        let text = input.as_text().unwrap_or("message").to_owned();
        let item = Item {
            id: ItemId::new(),
            harness_item_id: format!("reply_{call}_{turn}"),
            payload: ItemPayload::AgentMessage {
                text: format!("reply to {text}"),
            },
            created_at: chrono::Utc::now(),
        };
        let _ = sender.send(AgentEvent::TurnStarted {
            thread: thread_id,
            turn,
        });
        tokio::task::yield_now().await;
        let _ = sender.send(AgentEvent::ItemCompleted {
            thread: thread_id,
            turn,
            item,
        });
        tokio::task::yield_now().await;
        let _ = sender.send(AgentEvent::TurnCompleted {
            thread: thread_id,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        });
        Ok(turn)
    }

    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream {
        if let Ok(threads) = self.threads.try_lock() {
            if let Some(sender) = threads.get(&thread.thread) {
                return AgentEventStream::new(sender.subscribe());
            }
        }
        let (_, rx) = tokio::sync::broadcast::channel(1);
        AgentEventStream::new(rx)
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

    async fn compact_thread(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl AgentHarness for CountingOpenHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        HarnessCapabilities {
            live_approvals: false,
            plan_build_modes: false,
            per_turn_model: false,
            reasoning_effort: false,
            structured_diffs: false,
            resumable_threads: true,
            model_listing: false,
            token_usage: false,
            mcp_status: false,
            mcp_reload: false,
            mcp_oauth_login: false,
            context_compaction: false,
        }
    }

    async fn list_models(&self) -> Result<Vec<giskard_core::ModelDescriptor>, HarnessError> {
        Ok(Vec::new())
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        let open_call = self.open_calls.fetch_add(1, Ordering::SeqCst) + 1;
        let thread = opts.thread.unwrap_or_default();
        self.opened_models
            .lock()
            .await
            .push(opts.initial_model.clone());
        let (tx, _) = tokio::sync::broadcast::channel(16);
        self.threads.lock().await.insert(thread, tx);
        Ok(ThreadHandle {
            thread,
            harness_thread_id: opts
                .resume
                .unwrap_or_else(|| format!("count_{thread}_{open_call}")),
            warning: None,
            resumed_model: Some(opts.initial_model.clone()),
        })
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        input: UserInput,
        overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        self.start_calls.fetch_add(1, Ordering::SeqCst);
        self.started_models.lock().await.push(overrides.model);
        self.started_inputs
            .lock()
            .await
            .push(input.as_text().unwrap_or_default().to_string());

        if let Some(error) = self.start_error.lock().await.clone() {
            return Err(error);
        }

        let turn = TurnId::new();
        let sender = {
            let threads = self.threads.lock().await;
            threads.get(&thread.thread).cloned()
        }
        .ok_or(HarnessError::ThreadNotFound(thread.thread))?;
        let _ = sender.send(AgentEvent::TurnStarted {
            thread: thread.thread,
            turn,
        });
        Ok(turn)
    }

    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream {
        if let Ok(threads) = self.threads.try_lock() {
            if let Some(sender) = threads.get(&thread.thread) {
                return AgentEventStream::new(sender.subscribe());
            }
        }
        let (_, rx) = tokio::sync::broadcast::channel(1);
        AgentEventStream::new(rx)
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

    async fn delete_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        self.delete_calls.fetch_add(1, Ordering::SeqCst);
        self.threads.lock().await.remove(&thread.thread);
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        self.shutdown_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait::async_trait]
impl AgentHarness for NoMcpHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        HarnessCapabilities {
            live_approvals: false,
            plan_build_modes: false,
            per_turn_model: false,
            reasoning_effort: false,
            structured_diffs: false,
            resumable_threads: false,
            model_listing: false,
            token_usage: false,
            mcp_status: false,
            mcp_reload: false,
            mcp_oauth_login: false,
            context_compaction: false,
        }
    }

    async fn list_models(&self) -> Result<Vec<giskard_core::ModelDescriptor>, HarnessError> {
        Ok(Vec::new())
    }

    async fn open_thread(&self, _opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        Err(HarnessError::Unsupported(
            "thread opening is not supported by this harness".into(),
        ))
    }

    async fn start_turn(
        &self,
        _thread: &ThreadHandle,
        _input: UserInput,
        _overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        Err(HarnessError::Unsupported(
            "turns are not supported by this harness".into(),
        ))
    }

    fn subscribe(&self, _thread: &ThreadHandle) -> AgentEventStream {
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        AgentEventStream::new(rx)
    }

    async fn respond_approval(
        &self,
        _req: ApprovalId,
        _decision: ApprovalDecision,
    ) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(
            "approvals are not supported by this harness".into(),
        ))
    }

    async fn respond_server_request(
        &self,
        _req: ServerRequestId,
        _response: ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(
            "server requests are not supported by this harness".into(),
        ))
    }

    async fn interrupt(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(
            "interrupts are not supported by this harness".into(),
        ))
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

fn make_fixture() -> ReplayFixture {
    let thread = ThreadId::new();
    let turn = TurnId::new();
    let it_1 = ItemId::new();
    let now = chrono::Utc::now();

    ReplayFixture::from_events(vec![
        AgentEvent::ThreadOpened {
            thread,
            harness_thread_id: "th_test".into(),
        },
        AgentEvent::TurnStarted { thread, turn },
        AgentEvent::ItemStarted {
            thread,
            turn,
            item: ItemStart {
                id: it_1,
                harness_item_id: "it_1".into(),
                kind: ItemKind::AgentMessage,
                command: None,
                tool: None,
            },
        },
        AgentEvent::ItemDelta {
            thread,
            turn,
            item_id: it_1,
            delta: ItemDelta::Text {
                text: "Hello from replay!".into(),
            },
        },
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: it_1,
                harness_item_id: "it_1".into(),
                payload: ItemPayload::AgentMessage {
                    text: "Hello from replay!".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::new(100, 50),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        },
    ])
}

fn duplicate_history_fixture(
    thread: ThreadId,
    old_turn: TurnId,
    new_turn: TurnId,
) -> ReplayFixture {
    let now = chrono::Utc::now();
    let old_user = ItemId::new();
    let old_agent = ItemId::new();
    let new_user = ItemId::new();
    let new_agent = ItemId::new();

    ReplayFixture::from_events(vec![
        AgentEvent::ThreadOpened {
            thread,
            harness_thread_id: "th_dupe".into(),
        },
        AgentEvent::TurnStarted {
            thread,
            turn: old_turn,
        },
        AgentEvent::ItemCompleted {
            thread,
            turn: old_turn,
            item: Item {
                id: old_user,
                harness_item_id: "old_user".into(),
                payload: ItemPayload::UserMessage {
                    text: "old input".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::ItemCompleted {
            thread,
            turn: old_turn,
            item: Item {
                id: old_agent,
                harness_item_id: "old_agent".into(),
                payload: ItemPayload::AgentMessage {
                    text: "old answer".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::TurnCompleted {
            thread,
            turn: old_turn,
            usage: TokenUsage::new(10, 10),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        },
        AgentEvent::TurnStarted {
            thread,
            turn: new_turn,
        },
        AgentEvent::ItemCompleted {
            thread,
            turn: new_turn,
            item: Item {
                id: new_user,
                harness_item_id: "new_user".into(),
                payload: ItemPayload::UserMessage {
                    text: "new input".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::ItemCompleted {
            thread,
            turn: new_turn,
            item: Item {
                id: new_agent,
                harness_item_id: "new_agent".into(),
                payload: ItemPayload::AgentMessage {
                    text: "new answer".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::TurnCompleted {
            thread,
            turn: new_turn,
            usage: TokenUsage::new(20, 20),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        },
    ])
}

/// A turn that starts, hits a fatal (non-retryable) error, and produces no agent output — the
/// sequence the Codex harness synthesizes for e.g. a quota rejection. The `Failed` `TurnCompleted`
/// carries the real message so it can be persisted to history rather than lost as a toast.
fn failed_turn_fixture(thread: ThreadId, turn: TurnId) -> ReplayFixture {
    let message = "usageLimitExceeded: Quota exceeded. Check your plan and billing details.";
    ReplayFixture::from_events(vec![
        AgentEvent::ThreadOpened {
            thread,
            harness_thread_id: "th_fail".into(),
        },
        AgentEvent::TurnStarted { thread, turn },
        AgentEvent::Error {
            thread,
            turn: Some(turn),
            error: HarnessError::Protocol(message.into()),
        },
        AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Failed,
                message: Some(message.into()),
            },
        },
    ])
}

/// A turn that emits a non-fatal advisory (a Codex warning) alongside normal agent output. The
/// notice must reach the client as a `Notice` event and must not fail the turn.
fn notice_fixture(thread: ThreadId, turn: TurnId) -> ReplayFixture {
    let item = ItemId::new();
    ReplayFixture::from_events(vec![
        AgentEvent::ThreadOpened {
            thread,
            harness_thread_id: "th_notice".into(),
        },
        AgentEvent::TurnStarted { thread, turn },
        AgentEvent::Notice {
            thread,
            turn: None,
            message: "Model metadata for `glm` not found. Using fallback.".into(),
        },
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: item,
                harness_item_id: "a1".into(),
                payload: ItemPayload::AgentMessage { text: "hi".into() },
                created_at: chrono::Utc::now(),
            },
        },
        AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::new(1, 1),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
        },
    ])
}

fn generate_password_hash(password: &str) -> String {
    use argon2::password_hash::SaltString;
    use argon2::{Argon2, PasswordHasher};
    use rand::rngs::OsRng;
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string()
}

async fn start_server_with_extra_config(
    port: u16,
    extra_config: &str,
) -> (tempfile::TempDir, Arc<AppState>) {
    start_server_with_fixture_and_extra_config(port, make_fixture(), extra_config).await
}

async fn start_server_with_fixture_and_extra_config(
    port: u16,
    fixture: ReplayFixture,
    extra_config: &str,
) -> (tempfile::TempDir, Arc<AppState>) {
    let tmp = tempfile::TempDir::new().unwrap();

    let hash = generate_password_hash("testpass");
    let config_toml = format!(
        r#"
[server]
bind = "127.0.0.1:{port}"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30

{extra_config}
"#
    );
    tokio::fs::write(tmp.path().join("config.toml"), config_toml)
        .await
        .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let session_key: Vec<u8> = (0..32u8).collect();
    let factory = Arc::new(TestFactory { fixture });

    let state = AppState::new(store, factory, session_key);

    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (tmp, Arc::new(state))
}

async fn start_server(port: u16) -> (tempfile::TempDir, Arc<AppState>) {
    start_server_with_extra_config(port, "").await
}

async fn start_server_with_extra_config_on_available_port(
    extra_config: &str,
) -> (tempfile::TempDir, Arc<AppState>, u16) {
    start_server_with_fixture_and_extra_config_on_available_port(make_fixture(), extra_config).await
}

async fn start_server_with_fixture_and_extra_config_on_available_port(
    fixture: ReplayFixture,
    extra_config: &str,
) -> (tempfile::TempDir, Arc<AppState>, u16) {
    let tmp = tempfile::TempDir::new().unwrap();

    let hash = generate_password_hash("testpass");
    let config_toml = format!(
        r#"
[server]
bind = "127.0.0.1:0"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30

{extra_config}
"#
    );
    tokio::fs::write(tmp.path().join("config.toml"), config_toml)
        .await
        .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let session_key: Vec<u8> = (0..32u8).collect();
    let factory = Arc::new(TestFactory { fixture });

    let state = AppState::new(store, factory, session_key);

    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (tmp, Arc::new(state), port)
}

async fn start_no_mcp_server_on_available_port() -> (tempfile::TempDir, Arc<AppState>, u16) {
    let tmp = tempfile::TempDir::new().unwrap();

    let hash = generate_password_hash("testpass");
    let config_toml = format!(
        r#"
[server]
bind = "127.0.0.1:0"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30
"#
    );
    tokio::fs::write(tmp.path().join("config.toml"), config_toml)
        .await
        .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let session_key: Vec<u8> = (0..32u8).collect();
    let state = AppState::new(store, Arc::new(NoMcpFactory), session_key);

    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (tmp, Arc::new(state), port)
}

async fn start_unsupported_compaction_server_on_available_port()
-> (tempfile::TempDir, Arc<AppState>, u16) {
    let tmp = tempfile::TempDir::new().unwrap();

    let hash = generate_password_hash("testpass");
    let config_toml = format!(
        r#"
[server]
bind = "127.0.0.1:0"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30
"#
    );
    tokio::fs::write(tmp.path().join("config.toml"), config_toml)
        .await
        .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let session_key: Vec<u8> = (0..32u8).collect();
    let state = AppState::new(store, Arc::new(UnsupportedCompactionFactory), session_key);

    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (tmp, Arc::new(state), port)
}

async fn start_slow_compaction_server_on_available_port() -> (tempfile::TempDir, Arc<AppState>, u16)
{
    start_custom_server_on_available_port(Arc::new(SlowCompactionFactory)).await
}

async fn start_slow_start_server_on_available_port(
    harness: Arc<SlowStartHarness>,
) -> (tempfile::TempDir, Arc<AppState>, u16) {
    start_custom_server_on_available_port(Arc::new(SlowStartFactory { harness })).await
}

async fn start_activity_server_on_available_port(
    harness: Arc<ActivityHarness>,
) -> (tempfile::TempDir, Arc<AppState>, u16) {
    start_custom_server_on_available_port(Arc::new(ActivityFactory { harness })).await
}

async fn start_custom_server_on_available_port(
    factory: Arc<dyn HarnessFactory>,
) -> (tempfile::TempDir, Arc<AppState>, u16) {
    start_custom_server_with_extra_config_on_available_port(factory, "").await
}

async fn start_custom_server_with_extra_config_on_available_port(
    factory: Arc<dyn HarnessFactory>,
    extra_config: &str,
) -> (tempfile::TempDir, Arc<AppState>, u16) {
    let tmp = tempfile::TempDir::new().unwrap();

    let hash = generate_password_hash("testpass");
    let config_toml = format!(
        r#"
[server]
bind = "127.0.0.1:0"
secure_cookies = false

[auth]
password_hash = "{hash}"
session_days = 30

{extra_config}
"#
    );
    tokio::fs::write(tmp.path().join("config.toml"), config_toml)
        .await
        .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let session_key: Vec<u8> = (0..32u8).collect();
    let state = AppState::new(store, factory, session_key);
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (tmp, Arc::new(state), port)
}

async fn login_cookie(client: &reqwest::Client, base: &str) -> String {
    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    resp.headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string()
}

async fn connect_ws(
    port: u16,
    cookie: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    use tokio_tungstenite::tungstenite::http::Request;

    let ws_request = Request::builder()
        .uri(format!("ws://127.0.0.1:{port}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect")
        .0
}

async fn create_project_and_thread(
    client: &reqwest::Client,
    base: &str,
    cookie: &str,
) -> (ProjectId, ThreadId) {
    let project_id = create_project_only(client, base, cookie).await;

    let thread_resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", cookie)
        .json(&serde_json::json!({"resume": "th_test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(thread_resp.status(), 200);
    let thread_id: ThreadId = thread_resp.json::<serde_json::Value>().await.unwrap()["thread_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    (project_id, thread_id)
}

async fn create_project_only(client: &reqwest::Client, base: &str, cookie: &str) -> ProjectId {
    let project_resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", cookie)
        .json(&serde_json::json!({
            "name": "thread-actions",
            "dir": "/tmp/thread-actions",
            "default_model": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(project_resp.status(), 200);
    let project_id: ProjectId = project_resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    project_id
}

async fn wait_for_ws_error(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    action: &str,
    code: &str,
) -> giskard_proto::ErrorInfo {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::Error { error }) = serde_json::from_str(&text) {
                    if error.action.as_deref() == Some(action) && error.code == code {
                        return error;
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("websocket error {code}/{action} was not observed");
}

async fn wait_for_turn_completed(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    thread_id: ThreadId,
) {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::Event {
                    thread_id: event_thread,
                    agent_event: WireAgentEvent::TurnCompleted { .. },
                }) = serde_json::from_str(&text)
                {
                    if event_thread == thread_id {
                        return;
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("turn completion for {thread_id} was not observed");
}

/// Wait for the `TurnStarted` event and return the turn id carried on the wire — the id the browser
/// stamps transcript rows with.
async fn wait_for_turn_started(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    thread_id: ThreadId,
) -> TurnId {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::Event {
                    thread_id: event_thread,
                    agent_event: WireAgentEvent::TurnStarted { turn, .. },
                }) = serde_json::from_str(&text)
                {
                    if event_thread == thread_id {
                        return turn;
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("turn start for {thread_id} was not observed");
}

async fn wait_for_approval_request(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    thread_id: ThreadId,
) -> String {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::Event {
                    thread_id: event_thread,
                    agent_event: WireAgentEvent::ApprovalRequested { request, .. },
                }) = serde_json::from_str(&text)
                {
                    if event_thread == thread_id {
                        return request.id.to_string();
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("approval request for {thread_id} was not observed");
}

async fn wait_for_approval_resolved(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    thread_id: ThreadId,
    request_id: &str,
) -> ApprovalDecision {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                if let Ok(ServerMessage::ApprovalResolved {
                    thread_id: resolved_thread,
                    request_id: resolved_request,
                    decision,
                }) = serde_json::from_str(&text)
                {
                    if resolved_thread == thread_id && resolved_request == request_id {
                        return decision;
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("approval resolution for {thread_id}/{request_id} was not observed");
}

async fn wait_for_thread_activity(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    active_thread: ThreadId,
    inactive_thread: ThreadId,
    expected: fn(&ThreadActivityKind) -> bool,
    expected_name: &str,
) -> ThreadActivity {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&text) else {
                    continue;
                };
                match server_msg {
                    ServerMessage::ThreadActivity(activity)
                        if activity.thread_id == inactive_thread && expected(&activity.kind) =>
                    {
                        return activity;
                    }
                    ServerMessage::ThreadActivity(_) => {}
                    ServerMessage::Event {
                        thread_id,
                        agent_event,
                    } if thread_id == inactive_thread => {
                        panic!(
                            "inactive thread full event leaked without subscription: {agent_event:?}"
                        );
                    }
                    ServerMessage::Event { thread_id, .. }
                    | ServerMessage::HistoryPage { thread_id, .. }
                    | ServerMessage::HistoryDelta { thread_id, .. }
                    | ServerMessage::RunningTasks { thread_id, .. } => {
                        assert_eq!(
                            thread_id, active_thread,
                            "thread-scoped message should belong to subscribed thread"
                        );
                    }
                    ServerMessage::LiveTurnSnapshot(snapshot) => {
                        assert_eq!(
                            snapshot.thread_id, active_thread,
                            "live snapshot should belong to subscribed thread"
                        );
                    }
                    ServerMessage::ApprovalRequest { thread_id, .. } => {
                        assert_eq!(
                            thread_id, active_thread,
                            "approval request card should belong to subscribed thread"
                        );
                    }
                    ServerMessage::ThreadState(_)
                    | ServerMessage::TokenUpdate { .. }
                    | ServerMessage::ApprovalResolved { .. }
                    | ServerMessage::Error { .. }
                    | ServerMessage::Pong => {}
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    panic!("thread activity {expected_name} for {inactive_thread} was not observed");
}

fn is_approval_requested_activity(kind: &ThreadActivityKind) -> bool {
    matches!(kind, ThreadActivityKind::ApprovalRequested { .. })
}

fn is_server_request_received_activity(kind: &ThreadActivityKind) -> bool {
    matches!(kind, ThreadActivityKind::ServerRequestReceived { .. })
}

fn is_turn_completed_activity(kind: &ThreadActivityKind) -> bool {
    matches!(kind, ThreadActivityKind::TurnCompleted)
}

#[tokio::test]
async fn send_input_rejects_second_turn_before_turn_started() {
    let harness = Arc::new(SlowStartHarness::new());
    let (_tmp, _state, port) = start_slow_start_server_on_available_port(harness.clone()).await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (_, thread_id) = create_project_and_thread(&client, &base, &cookie).await;
    let mut first = connect_ws(port, &cookie).await;
    let mut second = connect_ws(port, &cookie).await;

    for ws in [&mut first, &mut second] {
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&ClientMessage::Subscribe {
                thread_id,
                since: None,
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    }

    first
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&ClientMessage::SendInput {
                thread_id,
                text: "first message".into(),
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    harness.wait_for_start_calls(1).await;

    second
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&ClientMessage::SendInput {
                thread_id,
                text: "overlapping message".into(),
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();

    let error = wait_for_ws_error(&mut second, "send_input", "thread_turn_active").await;
    assert_eq!(error.severity, ErrorSeverity::Error);
    assert_eq!(error.thread_id, Some(thread_id));
    assert_eq!(harness.start_calls(), 1);

    harness.release_first_start();
    wait_for_turn_completed(&mut first, thread_id).await;

    second
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&ClientMessage::SendInput {
                thread_id,
                text: "after completion".into(),
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    harness.wait_for_start_calls(2).await;
    wait_for_turn_completed(&mut second, thread_id).await;
    assert_eq!(harness.start_calls(), 2);
}

#[tokio::test]
async fn send_input_rejects_same_thread_during_compaction() {
    let (_tmp, state, port) = start_slow_compaction_server_on_available_port().await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (_, thread_id) = create_project_and_thread(&client, &base, &cookie).await;
    let mut ws = connect_ws(port, &cookie).await;

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::CompactContext { thread_id })
            .unwrap()
            .into(),
    ))
    .await
    .unwrap();

    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while !state.live_buffers.is_active(thread_id).await {
        if tokio::time::Instant::now() >= deadline {
            panic!("compaction thread did not become active");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
    }

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id,
            text: "overlap compaction".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let error = wait_for_ws_error(&mut ws, "send_input", "thread_turn_active").await;
    assert_eq!(error.severity, ErrorSeverity::Error);
    assert_eq!(error.thread_id, Some(thread_id));
}

#[tokio::test]
async fn compact_context_streams_and_persists_compaction_turn() {
    let (_tmp, state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, thread_id) = create_project_and_thread(&client, &base, &cookie).await;
    let mut ws = connect_ws(port, &cookie).await;

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::CompactContext { thread_id })
            .unwrap()
            .into(),
    ))
    .await
    .unwrap();

    let mut saw_activity = false;
    let mut saw_completed = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline && !saw_completed {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                let server_msg: ServerMessage = serde_json::from_str(&text).unwrap();
                if let ServerMessage::Event { agent_event, .. } = server_msg {
                    match agent_event {
                        WireAgentEvent::ItemCompleted { item, .. } => {
                            if let giskard_proto::WireItemPayload::Activity { title, .. } =
                                item.payload
                            {
                                saw_activity = title == "Context compacted";
                            }
                        }
                        WireAgentEvent::TurnCompleted { .. } => saw_completed = true,
                        _ => {}
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }

    assert!(
        saw_activity,
        "compaction should stream a visible activity item"
    );
    assert!(saw_completed, "compaction should complete a turn");

    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    let saved_turns = loop {
        let turns = state
            .store
            .load_all_turns(project_id, thread_id)
            .await
            .unwrap();
        if turns
            .iter()
            .any(|turn| turn.user_input.as_text() == Some("/compact"))
        {
            break turns;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("compaction turn was not persisted");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
    };
    let compact_turn = saved_turns
        .iter()
        .find(|turn| turn.user_input.as_text() == Some("/compact"))
        .unwrap();
    assert!(
        compact_turn.items.iter().any(|item| matches!(
            &item.payload,
            ItemPayload::Activity { title, .. } if title == "Context compacted"
        )),
        "persisted compaction turn should contain the activity item"
    );
}

/// The turn id the browser receives on the wire (`TurnStarted` / `LiveTurnSnapshot`) is the id it
/// stamps transcript rows with. Incremental reconnect uses the *persisted* turn id as its "give me
/// turns after this" cursor, so the two must be the same value — otherwise a resync would re-render
/// the in-flight turn instead of skipping it. This is not tautological: the replay harness returns a
/// fresh id from `start_turn` that differs from the id it streams, so this also pins the registry to
/// the streamed/persisted id rather than the harness return value.
#[tokio::test]
async fn wire_turn_id_matches_persisted_turn_id() {
    let (_tmp, state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, thread_id) = create_project_and_thread(&client, &base, &cookie).await;
    let mut ws = connect_ws(port, &cookie).await;

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::CompactContext { thread_id })
            .unwrap()
            .into(),
    ))
    .await
    .unwrap();

    // The id the browser would stamp rows with.
    let wire_turn = wait_for_turn_started(&mut ws, thread_id).await;
    wait_for_turn_completed(&mut ws, thread_id).await;

    // The persisted turn — the resync cursor — must carry that same id.
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    let saved_turns = loop {
        let turns = state
            .store
            .load_all_turns(project_id, thread_id)
            .await
            .unwrap();
        if !turns.is_empty() {
            break turns;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("turn was not persisted");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
    };
    assert!(
        saved_turns.iter().any(|turn| turn.id == wire_turn),
        "persisted history must carry the turn under the same id the browser saw on the wire \
         (wire={wire_turn}, persisted={:?})",
        saved_turns.iter().map(|t| t.id).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn compact_context_does_not_block_turns_on_other_threads_or_projects() {
    let (_tmp, state, port) = start_slow_compaction_server_on_available_port().await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, compacting_thread) = create_project_and_thread(&client, &base, &cookie).await;
    let other_thread =
        open_thread_with_resume(&client, &base, &cookie, project_id, "other_thread").await;
    let (_, other_project_thread) = create_project_and_thread(&client, &base, &cookie).await;
    let mut ws = connect_ws(port, &cookie).await;

    for thread_id in [compacting_thread, other_thread, other_project_thread] {
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&ClientMessage::Subscribe {
                thread_id,
                since: None,
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    }

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::CompactContext {
            thread_id: compacting_thread,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while !state.live_buffers.is_active(compacting_thread).await {
        if tokio::time::Instant::now() >= deadline {
            panic!("compaction thread did not become active");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
    }

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id: other_thread,
            text: "work on another thread".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id: other_project_thread,
            text: "work on another project".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let mut other_completed = false;
    let mut other_project_completed = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline && (!other_completed || !other_project_completed) {
        match tokio::time::timeout(tokio::time::Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                let server_msg: ServerMessage = serde_json::from_str(&text).unwrap();
                if let ServerMessage::Event {
                    thread_id,
                    agent_event: WireAgentEvent::TurnCompleted { .. },
                } = server_msg
                {
                    if thread_id == other_thread {
                        other_completed = true;
                    } else if thread_id == other_project_thread {
                        other_project_completed = true;
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }

    assert!(
        other_completed,
        "a compaction turn must not block another thread from completing work"
    );
    assert!(
        other_project_completed,
        "a compaction turn must not block another project from completing work"
    );
    assert!(
        state.live_buffers.is_active(compacting_thread).await,
        "precondition check: compaction should still be active while the other thread completed"
    );
}

#[tokio::test]
async fn inactive_thread_progress_sends_activity_without_full_event_subscription() {
    let (_tmp, _state, port) = start_slow_compaction_server_on_available_port().await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, active_thread) = create_project_and_thread(&client, &base, &cookie).await;
    let inactive_thread =
        open_thread_with_resume(&client, &base, &cookie, project_id, "inactive_thread").await;
    let mut ws = connect_ws(port, &cookie).await;

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: active_thread,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id: inactive_thread,
            text: "work in inactive thread".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let mut saw_inactive_start = false;
    let mut saw_inactive_progress = false;
    let mut saw_inactive_completed = false;
    let mut saw_active_state = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text)))) => {
                let server_msg: ServerMessage = serde_json::from_str(&text).unwrap();
                match server_msg {
                    ServerMessage::ThreadState(state) => {
                        if state.thread_id == active_thread {
                            saw_active_state = true;
                        }
                    }
                    ServerMessage::ThreadActivity(activity) => {
                        if activity.thread_id == inactive_thread {
                            match activity.kind {
                                ThreadActivityKind::TurnStarted => {
                                    assert!(activity.active_turn);
                                    saw_inactive_start = true;
                                }
                                ThreadActivityKind::Progress => {
                                    assert!(activity.active_turn);
                                    saw_inactive_progress = true;
                                }
                                ThreadActivityKind::TurnCompleted => {
                                    assert!(!activity.active_turn);
                                    saw_inactive_completed = true;
                                }
                                other => panic!("unexpected inactive-thread activity: {other:?}"),
                            }
                        }
                    }
                    ServerMessage::Event {
                        thread_id,
                        agent_event,
                    } if thread_id == inactive_thread => {
                        panic!(
                            "inactive thread full event leaked without subscription: {agent_event:?}"
                        );
                    }
                    ServerMessage::Event { thread_id, .. } => {
                        assert_eq!(
                            thread_id, active_thread,
                            "only the subscribed thread may deliver full events"
                        );
                    }
                    ServerMessage::HistoryPage { thread_id, .. }
                    | ServerMessage::HistoryDelta { thread_id, .. }
                    | ServerMessage::RunningTasks { thread_id, .. } => {
                        assert_eq!(
                            thread_id, active_thread,
                            "snapshots should belong to the subscribed thread"
                        );
                    }
                    ServerMessage::LiveTurnSnapshot(snapshot) => {
                        assert_eq!(
                            snapshot.thread_id, active_thread,
                            "live snapshot should belong to the subscribed thread"
                        );
                    }
                    ServerMessage::TokenUpdate {
                        thread_id: Some(thread_id),
                        ..
                    } => {
                        assert_eq!(
                            thread_id, inactive_thread,
                            "inactive completed turn may update only lightweight token state"
                        );
                    }
                    ServerMessage::TokenUpdate {
                        thread_id: None, ..
                    }
                    | ServerMessage::ApprovalResolved { .. }
                    | ServerMessage::Error { .. }
                    | ServerMessage::ApprovalRequest { .. }
                    | ServerMessage::Pong => {}
                }
                if saw_inactive_start && saw_inactive_progress && saw_inactive_completed {
                    break;
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }

    assert!(
        saw_active_state,
        "subscribe should return active thread state"
    );
    assert!(
        saw_inactive_start,
        "inactive thread should announce turn start activity"
    );
    assert!(
        saw_inactive_progress,
        "inactive thread should announce progress activity"
    );
    assert!(
        saw_inactive_completed,
        "inactive thread should announce completion activity"
    );
}

#[tokio::test]
async fn inactive_thread_requests_send_activity_and_route_responses() {
    let harness = Arc::new(ActivityHarness::default());
    let (_tmp, _state, port) = start_activity_server_on_available_port(harness.clone()).await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, active_thread) = create_project_and_thread(&client, &base, &cookie).await;
    let inactive_thread =
        open_thread_with_resume(&client, &base, &cookie, project_id, "inactive_requests").await;
    let mut ws = connect_ws(port, &cookie).await;

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: active_thread,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id: inactive_thread,
            text: "approval please".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let approval_activity = wait_for_thread_activity(
        &mut ws,
        active_thread,
        inactive_thread,
        is_approval_requested_activity,
        "approval_requested",
    )
    .await;
    assert!(approval_activity.active_turn);
    let ThreadActivityKind::ApprovalRequested { approval_id } = approval_activity.kind else {
        panic!("approval activity should carry approval_id");
    };

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::ApprovalDecision {
            request_id: approval_id.clone(),
            decision: ApprovalDecision::Accept,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let (handled_approval, decision) = harness.wait_for_approval_response().await;
    assert_eq!(handled_approval.0, approval_id);
    assert_eq!(decision, ApprovalDecision::Accept);
    let completion_activity = wait_for_thread_activity(
        &mut ws,
        active_thread,
        inactive_thread,
        is_turn_completed_activity,
        "turn_completed",
    )
    .await;
    assert!(!completion_activity.active_turn);

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id: inactive_thread,
            text: "server request please".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let request_activity = wait_for_thread_activity(
        &mut ws,
        active_thread,
        inactive_thread,
        is_server_request_received_activity,
        "server_request_received",
    )
    .await;
    assert!(request_activity.active_turn);
    let ThreadActivityKind::ServerRequestReceived { server_request_id } = request_activity.kind
    else {
        panic!("server-request activity should carry server_request_id");
    };

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::ServerRequestResponse {
            request_id: server_request_id.clone(),
            response: ServerRequestResponse::result(serde_json::json!({
                "answers": { "confirm": "Yes" }
            })),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let (handled_request, response) = harness.wait_for_server_response().await;
    assert_eq!(handled_request.0, server_request_id);
    match response {
        ServerRequestResponse::Result { value } => {
            assert_eq!(value["answers"]["confirm"], "Yes");
        }
        other => panic!("expected result response, got {other:?}"),
    }
    let completion_activity = wait_for_thread_activity(
        &mut ws,
        active_thread,
        inactive_thread,
        is_turn_completed_activity,
        "turn_completed",
    )
    .await;
    assert!(!completion_activity.active_turn);
}

#[tokio::test]
async fn approval_decision_broadcasts_resolution_to_other_tabs() {
    let harness = Arc::new(ActivityHarness::default());
    let (_tmp, _state, port) = start_activity_server_on_available_port(harness.clone()).await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (_project_id, thread_id) = create_project_and_thread(&client, &base, &cookie).await;
    let mut first_ws = connect_ws(port, &cookie).await;
    let mut second_ws = connect_ws(port, &cookie).await;

    for ws in [&mut first_ws, &mut second_ws] {
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&ClientMessage::Subscribe {
                thread_id,
                since: None,
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    }

    first_ws
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&ClientMessage::SendInput {
                thread_id,
                text: "approval please".into(),
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();

    let first_approval_id = wait_for_approval_request(&mut first_ws, thread_id).await;
    let second_approval_id = wait_for_approval_request(&mut second_ws, thread_id).await;
    assert_eq!(second_approval_id, first_approval_id);

    first_ws
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&ClientMessage::ApprovalDecision {
                request_id: first_approval_id.clone(),
                decision: ApprovalDecision::Accept,
            })
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();

    let decision = wait_for_approval_resolved(&mut second_ws, thread_id, &first_approval_id).await;
    assert_eq!(decision, ApprovalDecision::Accept);
    let (handled_approval, handled_decision) = harness.wait_for_approval_response().await;
    assert_eq!(handled_approval.0, first_approval_id);
    assert_eq!(handled_decision, ApprovalDecision::Accept);
}

#[tokio::test]
async fn compact_context_unsupported_harness_returns_structured_error() {
    let (_tmp, _state, port) = start_unsupported_compaction_server_on_available_port().await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (_, thread_id) = create_project_and_thread(&client, &base, &cookie).await;
    let mut ws = connect_ws(port, &cookie).await;

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::CompactContext { thread_id })
            .unwrap()
            .into(),
    ))
    .await
    .unwrap();

    let msg = tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let tokio_tungstenite::tungstenite::Message::Text(text) = msg else {
        panic!("expected text WS frame");
    };
    let server_msg: ServerMessage = serde_json::from_str(&text).unwrap();
    match server_msg {
        ServerMessage::Error { error } => {
            assert_eq!(error.code, "harness_unsupported");
            assert_eq!(error.severity, ErrorSeverity::Error);
            assert_eq!(error.thread_id, Some(thread_id));
            assert_eq!(error.action.as_deref(), Some("compact_context"));
            assert!(
                error
                    .detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("context compaction is not supported"))
            );
        }
        other => panic!("expected structured compaction error, got {other:?}"),
    }
}

#[tokio::test]
async fn mcp_status_routes_surface_empty_replay_status_and_reload() {
    let (_tmp, _state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, _) = create_project_and_thread(&client, &base, &cookie).await;

    let status: serde_json::Value = client
        .get(format!("{base}/api/projects/{project_id}/mcp"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["servers"].as_array().unwrap().len(), 0);
    assert_eq!(status["capabilities"]["status"], true);
    assert_eq!(status["capabilities"]["reload"], true);
    assert_eq!(status["capabilities"]["oauth_login"], false);

    let reload: serde_json::Value = client
        .post(format!("{base}/api/projects/{project_id}/mcp/reload"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(reload["ok"], true);
}

#[tokio::test]
async fn mcp_status_routes_surface_unsupported_capabilities_without_failing() {
    let (_tmp, _state, port) = start_no_mcp_server_on_available_port().await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let project_id = create_project_only(&client, &base, &cookie).await;

    let status: serde_json::Value = client
        .get(format!("{base}/api/projects/{project_id}/mcp"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["servers"].as_array().unwrap().len(), 0);
    assert_eq!(status["capabilities"]["status"], false);
    assert_eq!(status["capabilities"]["reload"], false);
    assert_eq!(status["capabilities"]["oauth_login"], false);

    let reload = client
        .post(format!("{base}/api/projects/{project_id}/mcp/reload"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(reload.status(), 400);
    assert!(
        reload
            .text()
            .await
            .unwrap()
            .contains("MCP server reload is not supported")
    );
}

#[tokio::test]
async fn mcp_oauth_login_rejects_empty_and_unsupported_requests() {
    let (_tmp, _state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, _) = create_project_and_thread(&client, &base, &cookie).await;

    let empty = client
        .post(format!("{base}/api/projects/{project_id}/mcp/oauth-login"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"name": "   "}))
        .send()
        .await
        .unwrap();
    assert_eq!(empty.status(), 400);
    assert!(empty.text().await.unwrap().contains("cannot be empty"));

    let unsupported = client
        .post(format!("{base}/api/projects/{project_id}/mcp/oauth-login"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"name": "cf-mcp"}))
        .send()
        .await
        .unwrap();
    assert_eq!(unsupported.status(), 400);
    assert!(
        unsupported
            .text()
            .await
            .unwrap()
            .contains("MCP OAuth login is not supported")
    );
}

#[tokio::test]
async fn thread_archive_unarchive_updates_thread_summary() {
    let (_tmp, _state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, thread_id) = create_project_and_thread(&client, &base, &cookie).await;

    let archived: serde_json::Value = client
        .post(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}/archive"
        ))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"archived": true}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(archived["id"].as_str().unwrap(), thread_id.to_string());
    assert_eq!(archived["archived"].as_bool(), Some(true));

    let listed: serde_json::Value = client
        .get(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["threads"][0]["archived"].as_bool(), Some(true));

    let unarchived: serde_json::Value = client
        .post(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}/archive"
        ))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"archived": false}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(unarchived["archived"].as_bool(), Some(false));
}

#[tokio::test]
async fn thread_rename_updates_thread_summary_and_persistence() {
    let (_tmp, state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, thread_id) = create_project_and_thread(&client, &base, &cookie).await;

    let renamed = client
        .patch(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}/title"
        ))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"title": "  Better   title\nnow  "}))
        .send()
        .await
        .unwrap();
    assert_eq!(renamed.status(), 200);
    let renamed: serde_json::Value = renamed.json().await.unwrap();
    assert_eq!(renamed["id"].as_str().unwrap(), thread_id.to_string());
    assert_eq!(renamed["title"].as_str(), Some("Better title now"));

    let saved = state
        .store
        .load_thread(project_id, thread_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.title, "Better title now");

    let listed: serde_json::Value = client
        .get(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        listed["threads"][0]["title"].as_str(),
        Some("Better title now")
    );

    let empty = client
        .patch(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}/title"
        ))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"title": " \n\t "}))
        .send()
        .await
        .unwrap();
    assert_eq!(empty.status(), 400);
    let saved = state
        .store
        .load_thread(project_id, thread_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.title, "Better title now");
}

#[tokio::test]
async fn thread_delete_removes_native_and_persisted_thread() {
    let (_tmp, state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, thread_id) = create_project_and_thread(&client, &base, &cookie).await;
    assert_eq!(
        state.registry.get_project_for_thread(thread_id).await,
        Some(project_id)
    );

    let resp = client
        .delete(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    assert!(
        state
            .store
            .load_thread(project_id, thread_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(state.registry.get_project_for_thread(thread_id).await, None);
}

#[tokio::test]
async fn project_remove_shuts_down_harness_and_removes_giskard_data_only() {
    let harness = Arc::new(CountingOpenHarness::default());
    let (tmp, state, port) = start_custom_server_with_extra_config_on_available_port(
        Arc::new(CountingOpenFactory {
            harness: harness.clone(),
        }),
        "",
    )
    .await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let source_dir = tmp.path().join("source-project");
    tokio::fs::create_dir_all(&source_dir).await.unwrap();

    let project_resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "remove-me",
            "dir": source_dir.to_string_lossy(),
            "default_model": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(project_resp.status(), 200);
    let project_id: ProjectId = project_resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    let thread_resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"resume": "th_remove_project"}))
        .send()
        .await
        .unwrap();
    assert_eq!(thread_resp.status(), 200);
    let thread_id: ThreadId = thread_resp.json::<serde_json::Value>().await.unwrap()["thread_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        state.registry.get_project_for_thread(thread_id).await,
        Some(project_id)
    );

    let resp = client
        .delete(format!("{base}/api/projects/{project_id}"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    assert_eq!(harness.shutdown_calls(), 1);
    assert_eq!(state.registry.get_project_for_thread(thread_id).await, None);
    assert!(
        state
            .store
            .load_project(project_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        state
            .store
            .list_threads(project_id)
            .await
            .unwrap()
            .is_empty(),
        "project lifecycle data should be removed from Giskard storage"
    );
    assert!(
        source_dir.is_dir(),
        "removing a project from Giskard must not touch the source directory"
    );
}

#[tokio::test]
async fn project_remove_returns_not_found_for_missing_project() {
    let (_tmp, _state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;

    let resp = client
        .delete(format!("{base}/api/projects/{}", ProjectId::new()))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn thread_archive_and_delete_reject_active_turns() {
    let (_tmp, state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, thread_id) = create_project_and_thread(&client, &base, &cookie).await;
    state.live_buffers.start_turn(thread_id).await;

    let archive = client
        .post(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}/archive"
        ))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"archived": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(archive.status(), 409);

    let delete = client
        .delete(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(delete.status(), 409);
}

#[tokio::test]
async fn project_remove_rejects_active_turns() {
    let (_tmp, state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, thread_id) = create_project_and_thread(&client, &base, &cookie).await;
    state.live_buffers.start_turn(thread_id).await;

    let resp = client
        .delete(format!("{base}/api/projects/{project_id}"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
    assert!(
        state
            .store
            .load_project(project_id)
            .await
            .unwrap()
            .is_some()
    );
}

/// A running command must block archive/delete even when there is no live turn: the guard checks
/// the running-command registry independently of the live buffer (§7 / `reject_thread_mutation_if_live`).
#[tokio::test]
async fn thread_archive_and_delete_reject_running_commands() {
    let (_tmp, state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, thread_id) = create_project_and_thread(&client, &base, &cookie).await;

    // Track a running command without starting a turn, so only the running-command branch of the
    // guard can trip (not the live-turn branch).
    let tracked = state
        .running_commands
        .apply_event(&AgentEvent::ItemStarted {
            thread: thread_id,
            turn: TurnId::new(),
            item: ItemStart {
                id: ItemId::new(),
                harness_item_id: "cmd_guard".into(),
                kind: ItemKind::CommandExecution,
                command: Some(CommandExecutionStart {
                    command: "sleep 60".into(),
                    cwd: "/tmp/thread-actions".into(),
                    status: Some("in_progress".into()),
                    process_id: Some("proc_guard".into()),
                    started_at_ms: None,
                }),
                tool: None,
            },
        })
        .await;
    assert!(tracked, "command should be tracked as running");
    assert!(
        !state.live_buffers.is_active(thread_id).await,
        "precondition: no live turn — only a running command"
    );

    let archive = client
        .post(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}/archive"
        ))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"archived": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(archive.status(), 409);

    let delete = client
        .delete(format!(
            "{base}/api/projects/{project_id}/threads/{thread_id}"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(delete.status(), 409);
}

#[tokio::test]
async fn project_remove_rejects_running_commands() {
    let (_tmp, state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let (project_id, thread_id) = create_project_and_thread(&client, &base, &cookie).await;

    let tracked = state
        .running_commands
        .apply_event(&AgentEvent::ItemStarted {
            thread: thread_id,
            turn: TurnId::new(),
            item: ItemStart {
                id: ItemId::new(),
                harness_item_id: "cmd_project_guard".into(),
                kind: ItemKind::CommandExecution,
                command: Some(CommandExecutionStart {
                    command: "sleep 60".into(),
                    cwd: "/tmp/thread-actions".into(),
                    status: Some("in_progress".into()),
                    process_id: Some("proc_project_guard".into()),
                    started_at_ms: None,
                }),
                tool: None,
            },
        })
        .await;
    assert!(tracked, "command should be tracked as running");

    let resp = client
        .delete(format!("{base}/api/projects/{project_id}"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
    assert!(
        state
            .store
            .load_project(project_id)
            .await
            .unwrap()
            .is_some()
    );
}

/// AP2: approval policy is thread-scoped, so two threads in the same project keep independent
/// policies. Setting one thread's policy must not disturb the other's.
#[tokio::test]
async fn threads_in_a_project_keep_independent_approval_policies() {
    let (_tmp, state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let cookie = login_cookie(&client, &base).await;
    let project_id = create_project_only(&client, &base, &cookie).await;
    let thread_a =
        open_thread_with_resume(&client, &base, &cookie, project_id, "th_policy_a").await;
    let thread_b =
        open_thread_with_resume(&client, &base, &cookie, project_id, "th_policy_b").await;

    // New threads default to `ask`.
    assert_eq!(
        load_policy(&state, project_id, thread_a).await,
        ApprovalPolicy::Ask
    );
    assert_eq!(
        load_policy(&state, project_id, thread_b).await,
        ApprovalPolicy::Ask
    );

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    // Give the two threads different policies.
    for (thread_id, policy) in [
        (thread_a, ApprovalPolicy::ReadOnly),
        (thread_b, ApprovalPolicy::Auto),
    ] {
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&ClientMessage::SetApprovalPolicy { thread_id, policy })
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();
    }

    // Each thread retains its own policy; setting B did not overwrite A (which a project-scoped
    // policy would have done).
    wait_for_policy(&state, project_id, thread_a, ApprovalPolicy::ReadOnly).await;
    wait_for_policy(&state, project_id, thread_b, ApprovalPolicy::Auto).await;
}

async fn open_thread_with_resume(
    client: &reqwest::Client,
    base: &str,
    cookie: &str,
    project_id: ProjectId,
    resume: &str,
) -> ThreadId {
    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", cookie)
        .json(&serde_json::json!({ "resume": resume }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    resp.json::<serde_json::Value>().await.unwrap()["thread_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

async fn load_policy(
    state: &AppState,
    project_id: ProjectId,
    thread_id: ThreadId,
) -> ApprovalPolicy {
    state
        .store
        .load_thread(project_id, thread_id)
        .await
        .unwrap()
        .unwrap()
        .approval_policy
}

async fn wait_for_policy(
    state: &AppState,
    project_id: ProjectId,
    thread_id: ThreadId,
    expected: ApprovalPolicy,
) {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    loop {
        if load_policy(state, project_id, thread_id).await == expected {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("thread {thread_id} approval policy did not become {expected:?}");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn subscribe_unknown_thread_returns_structured_error() {
    let port = 18791;
    let (_tmp, _state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    let tid = ThreadId::new();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: tid,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let msg = tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let tokio_tungstenite::tungstenite::Message::Text(text) = msg else {
        panic!("expected text WS frame");
    };
    let server_msg: ServerMessage = serde_json::from_str(&text).unwrap();
    match server_msg {
        ServerMessage::Error { error } => {
            assert_eq!(error.code, "thread_not_found");
            assert_eq!(error.severity, ErrorSeverity::Error);
            assert_eq!(error.thread_id, Some(tid));
            assert_eq!(error.action.as_deref(), Some("subscribe"));
            assert!(!error.message.is_empty());
        }
        other => panic!("expected structured error, got {other:?}"),
    }
}

#[tokio::test]
async fn websocket_accepts_ticket_without_cookie_header() {
    let port = 18793;
    let (_tmp, _state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let ticket_resp: serde_json::Value = client
        .get(format!("{base}/api/ws-ticket"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ticket = ticket_resp["ticket"].as_str().unwrap();

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws?ticket={ticket}"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect with ticket");

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Ping).unwrap().into(),
    ))
    .await
    .unwrap();

    let msg = tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let tokio_tungstenite::tungstenite::Message::Text(text) = msg else {
        panic!("expected text WS frame");
    };
    let server_msg: ServerMessage = serde_json::from_str(&text).unwrap();
    assert!(matches!(server_msg, ServerMessage::Pong));
}

#[tokio::test]
async fn websocket_serializes_harness_error_events() {
    let port = 18794;
    let (_tmp, state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "test-project",
            "dir": "/tmp/test",
            "default_model": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let project_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"resume": "th_test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let tid: ThreadId = resp.json::<serde_json::Value>().await.unwrap()["thread_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: tid,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let msg = tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        serde_json::from_str::<ServerMessage>(msg.to_text().unwrap()).unwrap(),
        ServerMessage::ThreadState(_)
    ));

    state
        .hub
        .broadcast_event(
            tid,
            AgentEvent::Error {
                thread: tid,
                turn: None,
                error: HarnessError::Protocol("bad frame".into()),
            },
        )
        .await;

    // Skip the snapshot messages sent on subscribe and wait for the broadcast Error event.
    loop {
        let msg = tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match serde_json::from_str::<ServerMessage>(msg.to_text().unwrap()).unwrap() {
            ServerMessage::Event { agent_event, .. } => match agent_event {
                WireAgentEvent::Error { error, .. } => {
                    assert_eq!(error.code, "harness_protocol_error");
                    assert_eq!(error.message, "protocol error: bad frame");
                    break;
                }
                other => panic!("expected error event, got {other:?}"),
            },
            ServerMessage::HistoryPage { .. }
            | ServerMessage::LiveTurnSnapshot(_)
            | ServerMessage::RunningTasks { .. } => continue,
            other => panic!("expected event, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn subscribe_reopens_persisted_thread() {
    let port = 18792;
    let (_tmp, state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let model = ModelRef {
        provider: "openai".into(),
        model: "gpt-5.5".into(),
        reasoning_effort: None,
    };

    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "test-project",
            "dir": "/tmp/test",
            "default_model": model,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let project_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let pid: ProjectId = project_id.parse().unwrap();

    let tid = ThreadId::new();
    let now = chrono::Utc::now();
    state
        .store
        .save_thread(
            pid,
            &giskard_persist::store::ThreadFile {
                version: 1,
                id: tid,
                project_id: pid,
                title: "Saved thread".into(),
                harness_thread_id: "th_test".into(),
                mode: Mode::Build,
                current_model: model.clone(),
                context_window: 128_000,
                approval_policy: ApprovalPolicy::Ask,
                model_efforts: Default::default(),
                tokens: Default::default(),
                created_at: now,
                updated_at: now,
                archived: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(state.registry.get_project_for_thread(tid).await, None);

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: tid,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let mut got_thread_state = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(2), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                let server_msg: ServerMessage = serde_json::from_str(&t).unwrap();
                if let ServerMessage::ThreadState(state) = server_msg {
                    assert_eq!(state.thread_id, tid);
                    got_thread_state = true;
                    break;
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }

    assert!(got_thread_state, "subscribe should return ThreadState");
    assert_eq!(state.registry.get_project_for_thread(tid).await, Some(pid));
}

#[tokio::test]
async fn persisted_thread_can_be_reopened_before_ws_send() {
    let port = 18790;
    let (_tmp, state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let model = ModelRef {
        provider: "openai".into(),
        model: "gpt-5.5".into(),
        reasoning_effort: None,
    };

    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "test-project",
            "dir": "/tmp/test",
            "default_model": model,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let project_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let pid: ProjectId = project_id.parse().unwrap();

    let tid = ThreadId::new();
    let now = chrono::Utc::now();
    state
        .store
        .save_thread(
            pid,
            &giskard_persist::store::ThreadFile {
                version: 1,
                id: tid,
                project_id: pid,
                title: "Saved thread".into(),
                harness_thread_id: "th_test".into(),
                mode: Mode::Build,
                current_model: model.clone(),
                context_window: 128_000,
                approval_policy: ApprovalPolicy::Ask,
                model_efforts: Default::default(),
                tokens: Default::default(),
                created_at: now,
                updated_at: now,
                archived: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(state.registry.get_project_for_thread(tid).await, None);

    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"thread_id": tid, "resume": null}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["thread_id"].as_str().unwrap(), tid.to_string());
    assert_eq!(state.registry.get_project_for_thread(tid).await, Some(pid));

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: tid,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id: tid,
            text: "Hello".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let mut saw_completed = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                let server_msg: ServerMessage = serde_json::from_str(&t).unwrap();
                if let ServerMessage::Event { agent_event, .. } = server_msg {
                    if matches!(agent_event, WireAgentEvent::TurnCompleted { .. }) {
                        saw_completed = true;
                        break;
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }

    assert!(saw_completed, "reopened persisted thread should run a turn");
}

#[tokio::test]
async fn replayed_persisted_turn_events_are_not_duplicated() {
    let tid = ThreadId::new();
    let old_turn = TurnId::new();
    let new_turn = TurnId::new();
    let fixture = duplicate_history_fixture(tid, old_turn, new_turn);
    let (_tmp, state, port) =
        start_server_with_fixture_and_extra_config_on_available_port(fixture, "").await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let model = ModelRef {
        provider: "openai".into(),
        model: "gpt-5.5".into(),
        reasoning_effort: None,
    };

    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "test-project",
            "dir": "/tmp/test",
            "default_model": model,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let project_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let pid: ProjectId = project_id.parse().unwrap();

    let now = chrono::Utc::now();
    state
        .store
        .save_thread(
            pid,
            &giskard_persist::store::ThreadFile {
                version: 1,
                id: tid,
                project_id: pid,
                title: "Saved thread".into(),
                harness_thread_id: "th_dupe".into(),
                mode: Mode::Build,
                current_model: model.clone(),
                context_window: 128_000,
                approval_policy: ApprovalPolicy::Ask,
                model_efforts: Default::default(),
                tokens: Default::default(),
                created_at: now,
                updated_at: now,
                archived: false,
            },
        )
        .await
        .unwrap();
    // The persisted turn lives in the authoritative JSONL history (H1), not the metadata file.
    state
        .store
        .append_turn(
            pid,
            tid,
            &giskard_core::turn::Turn {
                id: old_turn,
                user_input: giskard_core::user_input::UserInput::text("old input"),
                items: vec![
                    Item {
                        id: ItemId::new(),
                        harness_item_id: "old_user".into(),
                        payload: ItemPayload::UserMessage {
                            text: "old input".into(),
                        },
                        created_at: now,
                    },
                    Item {
                        id: ItemId::new(),
                        harness_item_id: "old_agent".into(),
                        payload: ItemPayload::AgentMessage {
                            text: "old answer".into(),
                        },
                        created_at: now,
                    },
                ],
                model: model.clone(),
                mode: Mode::Build,
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
                usage: TokenUsage::new(10, 10),
                diffs: vec![],
                started_at: now,
                completed_at: Some(now),
            },
        )
        .await
        .unwrap();

    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"thread_id": tid, "resume": null}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: tid,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id: tid,
            text: "new input".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let mut seen_old = false;
    let mut seen_new = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline && !seen_new {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                let server_msg: ServerMessage = serde_json::from_str(&t).unwrap();
                if let ServerMessage::Event { agent_event, .. } = server_msg {
                    match agent_event {
                        WireAgentEvent::ItemCompleted { item, .. } => match item.payload {
                            giskard_proto::WireItemPayload::AgentMessage { text }
                            | giskard_proto::WireItemPayload::UserMessage { text } => {
                                if text.starts_with("old ") {
                                    seen_old = true;
                                }
                                if text == "new answer" {
                                    seen_new = true;
                                }
                            }
                            _ => {}
                        },
                        WireAgentEvent::TurnCompleted { turn, .. } if turn == new_turn => {
                            seen_new = true;
                        }
                        _ => {}
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }

    assert!(
        !seen_old,
        "persisted replay items should not be rebroadcast"
    );
    assert!(seen_new, "new turn should still be streamed");

    let saved = state.store.load_all_turns(pid, tid).await.unwrap();
    assert_eq!(saved.len(), 2);
    assert_eq!(saved[0].id, old_turn);
    assert_eq!(saved[1].id, new_turn);
    let old_item_count = saved
        .iter()
        .flat_map(|turn| &turn.items)
        .filter(|item| item.harness_item_id.starts_with("old_"))
        .count();
    assert_eq!(old_item_count, 2);
}

/// A turn that fails without producing agent output must still be persisted as a `Failed` turn
/// carrying the user's input and the real error message, so history explains why the message got
/// no response (rather than the error only flashing by as a transient toast).
#[tokio::test]
async fn failed_turn_is_persisted_with_error_message() {
    let tid = ThreadId::new();
    let turn = TurnId::new();
    let fixture = failed_turn_fixture(tid, turn);
    let (_tmp, state, port) =
        start_server_with_fixture_and_extra_config_on_available_port(fixture, "").await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "test-project",
            "dir": "/tmp/test",
            "default_model": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let project_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let pid: ProjectId = project_id.parse().unwrap();

    // Open the thread (resume triggers the replay fixture).
    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"resume": "th_fail"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let thread_id = resp.json::<serde_json::Value>().await.unwrap()["thread_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(thread_id, tid.to_string());

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: tid,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id: tid,
            text: "please summarize the repo".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    // Drive the WS until the failed turn completes, asserting the error surfaces live too.
    let mut saw_error = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                if let ServerMessage::Event { agent_event, .. } =
                    serde_json::from_str::<ServerMessage>(&t).unwrap()
                {
                    match agent_event {
                        WireAgentEvent::Error { error, .. } => {
                            assert!(error.message.contains("usageLimitExceeded"));
                            saw_error = true;
                        }
                        WireAgentEvent::TurnCompleted { .. } => break,
                        _ => {}
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }
    assert!(saw_error, "the error event should reach the client live");

    // The failed attempt is persisted: one turn, Failed, with the real message and the user input.
    let saved = state.store.load_all_turns(pid, tid).await.unwrap();
    assert_eq!(saved.len(), 1, "the failed turn should be persisted once");
    let failed = &saved[0];
    assert_eq!(failed.status.kind, TurnStatusKind::Failed);
    assert_eq!(
        failed.status.message.as_deref(),
        Some("usageLimitExceeded: Quota exceeded. Check your plan and billing details.")
    );
    assert_eq!(
        failed.user_input.as_text(),
        Some("please summarize the repo")
    );
    assert!(
        failed.items.is_empty(),
        "a turn that failed before output has no items"
    );
}

/// A non-fatal advisory reaches the client as a `Notice` event (not an `Error`) and the turn still
/// completes normally.
#[tokio::test]
async fn notice_event_is_delivered_to_client() {
    let tid = ThreadId::new();
    let turn = TurnId::new();
    let fixture = notice_fixture(tid, turn);
    let (_tmp, _state, port) =
        start_server_with_fixture_and_extra_config_on_available_port(fixture, "").await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "p",
            "dir": "/tmp/test",
            "default_model": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
        }))
        .send()
        .await
        .unwrap();
    let project_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"resume": "th_notice"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::Subscribe {
            thread_id: tid,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id: tid,
            text: "hi".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let mut saw_notice = false;
    let mut saw_error = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                if let ServerMessage::Event { agent_event, .. } =
                    serde_json::from_str::<ServerMessage>(&t).unwrap()
                {
                    match agent_event {
                        WireAgentEvent::Notice { message, .. } => {
                            assert!(message.contains("Model metadata"));
                            saw_notice = true;
                        }
                        WireAgentEvent::Error { .. } => saw_error = true,
                        WireAgentEvent::TurnCompleted { .. } => break,
                        _ => {}
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }
    assert!(saw_notice, "a notice event should reach the client");
    assert!(!saw_error, "a warning must not surface as an error");
}

#[tokio::test]
async fn open_thread_normalizes_stale_provider_from_configured_model() {
    let extra_config = r#"
[[providers]]
id = "proxy"
name = "proxy"
wire_api = "responses"

  [[providers.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = true
"#;
    let (_tmp, state, port) = start_server_with_extra_config_on_available_port(extra_config).await;
    let base = format!("http://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let stale_model = ModelRef {
        provider: "openai".into(),
        model: "gpt-5.5".into(),
        reasoning_effort: None,
    };

    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "name": "test-project",
            "dir": "/tmp/test",
            "default_model": stale_model,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let project_id = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let pid: ProjectId = project_id.parse().unwrap();

    let saved_project = state.store.load_project(pid).await.unwrap().unwrap();
    assert_eq!(saved_project.default_model.provider, "proxy");

    let tid = ThreadId::new();
    let now = chrono::Utc::now();
    state
        .store
        .save_thread(
            pid,
            &giskard_persist::store::ThreadFile {
                version: 1,
                id: tid,
                project_id: pid,
                title: "Saved thread".into(),
                harness_thread_id: "th_test".into(),
                mode: Mode::Build,
                current_model: ModelRef {
                    provider: "openai".into(),
                    model: "gpt-5.5".into(),
                    reasoning_effort: None,
                },
                context_window: 128_000,
                approval_policy: ApprovalPolicy::Ask,
                model_efforts: Default::default(),
                tokens: Default::default(),
                created_at: now,
                updated_at: now,
                archived: false,
            },
        )
        .await
        .unwrap();

    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"thread_id": tid, "resume": null}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let saved_thread = state.store.load_thread(pid, tid).await.unwrap().unwrap();
    assert_eq!(saved_thread.current_model.provider, "proxy");
    assert_eq!(saved_thread.context_window, 262_144);
}

#[tokio::test]
async fn open_thread_normalization_reuses_live_handle() {
    let extra_config = r#"
[[providers]]
id = "proxy"
name = "proxy"
wire_api = "responses"

  [[providers.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = true
"#;
    let harness = Arc::new(CountingOpenHarness::default());
    let (_tmp, state, port) = start_custom_server_with_extra_config_on_available_port(
        Arc::new(CountingOpenFactory {
            harness: harness.clone(),
        }),
        extra_config,
    )
    .await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let cookie = login_cookie(&client, &base).await;

    let stale_model = ModelRef {
        provider: "openai".into(),
        model: "gpt-5.5".into(),
        reasoning_effort: None,
    };
    let pid = ProjectId::new();
    state
        .store
        .create_project(pid, "test-project", "/tmp/test", stale_model.clone())
        .await
        .unwrap();
    let tid = ThreadId::new();
    let now = chrono::Utc::now();
    state
        .store
        .save_thread(
            pid,
            &giskard_persist::store::ThreadFile {
                version: 1,
                id: tid,
                project_id: pid,
                title: "Live stale thread".into(),
                harness_thread_id: "th_live".into(),
                mode: Mode::Build,
                current_model: stale_model.clone(),
                context_window: 128_000,
                approval_policy: ApprovalPolicy::Ask,
                model_efforts: Default::default(),
                tokens: Default::default(),
                created_at: now,
                updated_at: now,
                archived: false,
            },
        )
        .await
        .unwrap();

    let project_config = state.store.load_project(pid).await.unwrap().unwrap();
    state
        .registry
        .open_thread(
            &project_config,
            "/tmp/test",
            Some(tid),
            Some("th_live".into()),
            stale_model,
        )
        .await
        .unwrap();
    assert_eq!(harness.open_calls(), 1);

    let resp = client
        .post(format!("{base}/api/projects/{pid}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"thread_id": tid, "resume": null}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.json::<serde_json::Value>().await.unwrap();
    assert_eq!(body["harness_thread_id"], "th_live");
    assert_eq!(
        harness.open_calls(),
        1,
        "HTTP reopen must reuse the live registry handle"
    );

    let saved_thread = state.store.load_thread(pid, tid).await.unwrap().unwrap();
    assert_eq!(saved_thread.current_model.provider, "proxy");
    assert_eq!(saved_thread.context_window, 262_144);
}

#[tokio::test]
async fn blank_thread_creation_without_resume_is_rejected() {
    let harness = Arc::new(CountingOpenHarness::default());
    let (_tmp, _state, port) = start_custom_server_with_extra_config_on_available_port(
        Arc::new(CountingOpenFactory {
            harness: harness.clone(),
        }),
        "",
    )
    .await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let cookie = login_cookie(&client, &base).await;
    let pid = create_project_only(&client, &base, &cookie).await;

    let resp = client
        .post(format!("{base}/api/projects/{pid}/threads"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"resume": null}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(body.contains("creating a new thread requires an initial message"));
    assert_eq!(harness.open_calls(), 0);
}

#[tokio::test]
async fn start_thread_with_initial_message_uses_selected_provider_and_starts_turn() {
    let extra_config = r#"
[[providers]]
id = "openai"
name = "OpenAI"
wire_api = "responses"

  [[providers.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = true

[[providers]]
id = "proxy"
name = "Proxy"
wire_api = "responses"

  [[providers.models]]
  id = "glm-5.2-workers-ai"
  display_name = "GLM Workers"
  context_window = 131072
  supports_reasoning_effort = false
"#;
    let harness = Arc::new(CountingOpenHarness::default());
    let (_tmp, state, port) = start_custom_server_with_extra_config_on_available_port(
        Arc::new(CountingOpenFactory {
            harness: harness.clone(),
        }),
        extra_config,
    )
    .await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let cookie = login_cookie(&client, &base).await;
    let pid = create_project_only(&client, &base, &cookie).await;

    let proxy_model = ModelRef {
        provider: "proxy".into(),
        model: "glm-5.2-workers-ai".into(),
        reasoning_effort: None,
    };
    let resp = client
        .post(format!("{base}/api/projects/{pid}/threads/start"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "text": "Hello",
            "model_ref": proxy_model,
            "mode": "plan",
            "approval_policy": "read_only",
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.json::<serde_json::Value>().await.unwrap();
    let tid: ThreadId = body["thread_id"].as_str().unwrap().parse().unwrap();

    assert_eq!(harness.open_calls(), 1);
    assert_eq!(harness.start_calls(), 1);
    let opened = harness.opened_models().await;
    assert_eq!(opened[0].provider, "proxy");
    assert_eq!(opened[0].model, "glm-5.2-workers-ai");
    let started_models = harness.started_models().await;
    assert_eq!(started_models[0], Some(opened[0].clone()));
    assert_eq!(harness.started_inputs().await, vec!["Hello".to_string()]);

    let saved_thread = state.store.load_thread(pid, tid).await.unwrap().unwrap();
    assert_eq!(saved_thread.current_model.provider, "proxy");
    assert_eq!(saved_thread.current_model.model, "glm-5.2-workers-ai");
    assert_eq!(saved_thread.mode, Mode::Plan);
    assert_eq!(saved_thread.approval_policy, ApprovalPolicy::ReadOnly);
    assert_eq!(
        saved_thread.harness_thread_id,
        body["harness_thread_id"].as_str().unwrap()
    );
}

#[tokio::test]
async fn start_thread_turn_rejection_cleans_up_new_thread() {
    let harness = Arc::new(CountingOpenHarness::default());
    harness
        .fail_start_with(HarnessError::Unsupported(
            "turns are not supported by this harness".into(),
        ))
        .await;
    let (_tmp, state, port) = start_custom_server_with_extra_config_on_available_port(
        Arc::new(CountingOpenFactory {
            harness: harness.clone(),
        }),
        "",
    )
    .await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let cookie = login_cookie(&client, &base).await;
    let pid = create_project_only(&client, &base, &cookie).await;

    let resp = client
        .post(format!("{base}/api/projects/{pid}/threads/start"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({
            "text": "Hello",
            "model_ref": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
            "mode": "build",
            "approval_policy": "ask",
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(body.contains("turns are not supported"));
    assert_eq!(harness.open_calls(), 1);
    assert_eq!(harness.start_calls(), 1);
    assert_eq!(harness.delete_calls(), 1);
    assert!(state.store.list_threads(pid).await.unwrap().is_empty());
}

#[tokio::test]
async fn select_model_rejects_provider_change_on_non_empty_thread() {
    let extra_config = r#"
[[providers]]
id = "openai"
name = "OpenAI"
wire_api = "responses"

  [[providers.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = true

[[providers]]
id = "proxy"
name = "Proxy"
wire_api = "responses"

  [[providers.models]]
  id = "glm-5.2-workers-ai"
  display_name = "GLM Workers"
  context_window = 131072
  supports_reasoning_effort = false
"#;
    let harness = Arc::new(CountingOpenHarness::default());
    let (_tmp, state, port) = start_custom_server_with_extra_config_on_available_port(
        Arc::new(CountingOpenFactory {
            harness: harness.clone(),
        }),
        extra_config,
    )
    .await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let cookie = login_cookie(&client, &base).await;
    let (pid, tid) = create_project_and_thread(&client, &base, &cookie).await;
    let now = chrono::Utc::now();
    let openai_model = ModelRef {
        provider: "openai".into(),
        model: "gpt-5.5".into(),
        reasoning_effort: None,
    };
    state
        .store
        .append_turn(
            pid,
            tid,
            &giskard_core::turn::Turn {
                id: TurnId::new(),
                user_input: UserInput::text("previous"),
                items: vec![],
                model: openai_model,
                mode: Mode::Build,
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
                usage: TokenUsage::default(),
                diffs: vec![],
                started_at: now,
                completed_at: Some(now),
            },
        )
        .await
        .unwrap();

    let mut ws = connect_ws(port, &cookie).await;
    let proxy_model = ModelRef {
        provider: "proxy".into(),
        model: "glm-5.2-workers-ai".into(),
        reasoning_effort: None,
    };
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SelectModel {
            thread_id: tid,
            model_ref: proxy_model,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let error = wait_for_ws_error(&mut ws, "select_model", "thread_provider_locked").await;
    assert!(
        error
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("native provider: openai; selected provider: proxy")
    );
    assert_eq!(harness.open_calls(), 1);
    let saved_thread = state.store.load_thread(pid, tid).await.unwrap().unwrap();
    assert_eq!(saved_thread.current_model.provider, "openai");
}

#[tokio::test]
async fn send_input_rejects_persisted_provider_mismatch_on_non_empty_thread() {
    let extra_config = r#"
[[providers]]
id = "openai"
name = "OpenAI"
wire_api = "responses"

  [[providers.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = true

[[providers]]
id = "proxy"
name = "Proxy"
wire_api = "responses"

  [[providers.models]]
  id = "glm-5.2-workers-ai"
  display_name = "GLM Workers"
  context_window = 131072
  supports_reasoning_effort = false
"#;
    let harness = Arc::new(CountingOpenHarness::default());
    let (_tmp, state, port) = start_custom_server_with_extra_config_on_available_port(
        Arc::new(CountingOpenFactory {
            harness: harness.clone(),
        }),
        extra_config,
    )
    .await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let cookie = login_cookie(&client, &base).await;
    let (pid, tid) = create_project_and_thread(&client, &base, &cookie).await;
    let now = chrono::Utc::now();
    let openai_model = ModelRef {
        provider: "openai".into(),
        model: "gpt-5.5".into(),
        reasoning_effort: None,
    };
    state
        .store
        .append_turn(
            pid,
            tid,
            &giskard_core::turn::Turn {
                id: TurnId::new(),
                user_input: UserInput::text("previous"),
                items: vec![],
                model: openai_model,
                mode: Mode::Build,
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
                usage: TokenUsage::default(),
                diffs: vec![],
                started_at: now,
                completed_at: Some(now),
            },
        )
        .await
        .unwrap();
    state
        .store
        .update_thread(pid, tid, |thread| {
            thread.current_model = ModelRef {
                provider: "proxy".into(),
                model: "glm-5.2-workers-ai".into(),
                reasoning_effort: None,
            };
        })
        .await
        .unwrap()
        .unwrap();

    let mut ws = connect_ws(port, &cookie).await;
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id: tid,
            text: "Hello".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let error = wait_for_ws_error(&mut ws, "send_input", "thread_provider_locked").await;
    assert!(
        error
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("native provider: openai; selected provider: proxy")
    );
    assert_eq!(harness.open_calls(), 1);
}

#[tokio::test]
async fn login_project_thread_message() {
    let port = 18787;
    let (_tmp, _state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let ws_base = format!("ws://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    // 1. Login
    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Extract session cookie before consuming the body
    let cookie_header = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let cookie_val = cookie_header;

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], true);

    // 2. Create project
    let resp = client
        .post(format!("{base}/api/projects"))
        .header("cookie", &cookie_val)
        .json(&serde_json::json!({
            "name": "test-project",
            "dir": "/tmp/test",
            "default_model": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let project_resp: serde_json::Value = resp.json().await.unwrap();
    let project_id = project_resp["id"].as_str().unwrap().to_string();

    // 3. List projects
    let resp = client
        .get(format!("{base}/api/projects"))
        .header("cookie", &cookie_val)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let list: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(list["projects"].as_array().unwrap().len(), 1);

    // 4. Open thread (with resume to trigger replay fixture)
    let resp = client
        .post(format!("{base}/api/projects/{project_id}/threads"))
        .header("cookie", &cookie_val)
        .json(&serde_json::json!({"resume": "th_test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let thread_resp: serde_json::Value = resp.json().await.unwrap();
    let thread_id = thread_resp["thread_id"].as_str().unwrap().to_string();

    // 5. WebSocket: subscribe + send input + receive events
    use tokio_tungstenite::tungstenite::http::Request;
    let ws_request = Request::builder()
        .uri(format!("{ws_base}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", &cookie_val)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_request)
        .await
        .expect("WS connect");

    // Subscribe
    let subscribe = serde_json::to_string(&ClientMessage::Subscribe {
        thread_id: thread_id.parse().unwrap(),
        since: None,
    })
    .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        subscribe.into(),
    ))
    .await
    .unwrap();

    // Wait for ThreadState snapshot
    let mut got_thread_state = false;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while !got_thread_state && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(2), ws.next()).await {
            Ok(Some(Ok(msg))) => {
                if let tokio_tungstenite::tungstenite::Message::Text(t) = msg {
                    let server_msg: ServerMessage = serde_json::from_str(&t).unwrap();
                    if matches!(server_msg, ServerMessage::ThreadState(_)) {
                        got_thread_state = true;
                    }
                }
            }
            _ => break,
        }
    }
    assert!(got_thread_state, "should receive ThreadState");

    // Send input
    let send_input = serde_json::to_string(&ClientMessage::SendInput {
        thread_id: thread_id.parse().unwrap(),
        text: "Hello".into(),
    })
    .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        send_input.into(),
    ))
    .await
    .unwrap();

    // Collect events until TurnCompleted
    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(msg))) => {
                if let tokio_tungstenite::tungstenite::Message::Text(t) = msg {
                    let server_msg: ServerMessage = serde_json::from_str(&t).unwrap();
                    if let ServerMessage::Event { agent_event, .. } = server_msg {
                        let is_done = matches!(agent_event, WireAgentEvent::TurnCompleted { .. });
                        events.push(agent_event);
                        if is_done {
                            break;
                        }
                    }
                }
            }
            _ => break,
        }
    }

    assert!(!events.is_empty(), "should receive at least one event");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WireAgentEvent::TurnCompleted { .. })),
        "should receive TurnCompleted"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WireAgentEvent::ItemDelta { .. })),
        "should receive ItemDelta"
    );
}

#[tokio::test]
async fn auth_rejected_without_cookie() {
    let port = 18788;
    let (_tmp, _state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");

    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/api/projects"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn login_rejected_wrong_password() {
    let port = 18789;
    let (_tmp, _state) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "wrongpass"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ok"], false);
}

/// The directory picker's "New folder" endpoint creates a directory under the given parent and
/// rejects path-segment escapes (`..`, separators).
#[tokio::test]
async fn browse_mkdir_creates_directory_and_rejects_escapes() {
    let (_tmp, _state, port) = start_server_with_extra_config_on_available_port("").await;
    let base = format!("http://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": "testpass"}))
        .send()
        .await
        .unwrap();
    let cookie = resp
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let parent = tempfile::TempDir::new().unwrap();
    let parent_path = parent.path().to_string_lossy().to_string();

    // Happy path: a new folder is created and its path returned.
    let resp = client
        .post(format!("{base}/api/browse/mkdir"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"parent": parent_path, "name": "my-project"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let created = resp.json::<serde_json::Value>().await.unwrap()["path"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(created.ends_with("my-project"));
    assert!(parent.path().join("my-project").is_dir());

    // Escape attempts are rejected without touching the filesystem.
    for bad in ["../evil", "a/b", "..", "."] {
        let resp = client
            .post(format!("{base}/api/browse/mkdir"))
            .header("cookie", &cookie)
            .json(&serde_json::json!({"parent": parent_path, "name": bad}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400, "name {bad:?} should be rejected");
    }
    assert!(!parent.path().parent().unwrap().join("evil").exists());
}
