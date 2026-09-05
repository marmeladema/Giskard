//! End-to-end coverage: a running tool/MCP call surfaces in the `RunningTasks` snapshot through the
//! real server path (registry forward → broadcast → WebSocket), the same way commands do (TK1).

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ServerRequestId, TurnId};
use giskard_core::item::{ItemKind, ItemStart, ToolCallStart};
use giskard_core::model::ModelDescriptor;
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::turn::{TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, EventLog, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
};
use giskard_proto::{ClientMessage, ServerMessage, TaskKind};
use giskard_testenv::{TestServer, factory, ws};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

/// Harness that, on `start_turn`, emits `TurnStarted` + an in-progress tool `ItemStarted` and
/// leaves the turn open (the tool blocks the turn), so the server keeps a running tool task.
struct ToolHarness {
    tx: Arc<EventLog>,
    active_turn: Mutex<Option<TurnId>>,
}

impl ToolHarness {
    fn new() -> Self {
        let tx = Arc::new(EventLog::new());
        Self {
            tx,
            active_turn: Mutex::new(None),
        }
    }
}

#[async_trait]
impl AgentHarness for ToolHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        HarnessCapabilities {
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
        }
    }

    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError> {
        Ok(vec![])
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        let thread = opts.thread;
        Ok(ThreadHandle {
            resumed_model: Some(opts.initial_model.clone()),
            ..ThreadHandle::opened(
                thread,
                opts.resume.unwrap_or_else(|| "tool_harness".into()),
                opts.workspace_root.clone(),
            )
        })
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        _input: UserInput,
        _overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let turn = TurnId::new();
        let tid = thread.thread;
        *self.active_turn.lock().await = Some(turn);
        let _ = self
            .tx
            .append(AgentEvent::TurnStarted { thread: tid, turn });
        let _ = self.tx.append(AgentEvent::ItemStarted {
            thread: tid,
            turn,
            item: ItemStart {
                id: ItemId::new(),
                harness_item_id: "tool1".into(),
                kind: ItemKind::ToolCall,
                command: None,
                tool: Some(ToolCallStart {
                    name: "search".into(),
                    input: serde_json::json!({ "q": "cats" }),
                    server: Some("wiki".into()),
                    status: Some("in_progress".into()),
                    metadata: None,
                    subagent: None,
                    started_at_ms: Some(1_785_000_000_000),
                }),
            },
        });
        Ok(turn)
    }

    fn subscribe(&self, _thread: &ThreadHandle) -> AgentEventStream {
        AgentEventStream::new(self.tx.reader())
    }

    async fn respond_approval(
        &self,
        _req: ApprovalId,
        _decision: giskard_core::approval::ApprovalDecision,
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

    async fn interrupt(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        // Interrupting the turn ends it; the still-running tool is then dropped by the registry.
        let turn = self
            .active_turn
            .lock()
            .await
            .take()
            .unwrap_or_else(TurnId::new);
        let _ = self.tx.append(AgentEvent::TurnCompleted {
            thread: thread.thread,
            turn,
            usage: giskard_core::token::TokenUsage::default(),
            status: TurnStatus {
                kind: TurnStatusKind::Interrupted,
                message: None,
            },
        });
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

#[tokio::test]
async fn running_tool_call_surfaces_in_running_tasks_snapshot() {
    let server = TestServer::spawn(factory::from_fn(|_, _| Ok(Arc::new(ToolHarness::new())))).await;
    let project = server.create_project("tool-proj").await;
    let thread_id = server.register_thread(project.id, "th_tool").await;
    let mut ws = server.ws().await;

    ws.send(ws::text(&ClientMessage::Subscribe {
        thread_id,
        since: None,
    }))
    .await
    .unwrap();
    ws.send(ws::text(&ClientMessage::SendInput {
        thread_id,
        text: "search wikipedia".into(),
        attachments: Vec::new(),
    }))
    .await
    .unwrap();

    // Read snapshots until the running tool call appears.
    let deadline = Instant::now() + Duration::from_secs(5);
    let tool_item_id = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "running tool task was not observed");
        let Some(Ok(msg)) = tokio::time::timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for a running tool task"))
        else {
            continue;
        };
        let tokio_tungstenite::tungstenite::Message::Text(text) = msg else {
            continue;
        };
        if let ServerMessage::RunningTasks { tasks, .. } =
            serde_json::from_str::<ServerMessage>(&text).unwrap()
            && let Some(task) = tasks.iter().find(|t| t.kind == TaskKind::Tool)
        {
            assert_eq!(task.command, "search");
            assert_eq!(task.server.as_deref(), Some("wiki"));
            assert_eq!(task.process_id, None);
            assert_eq!(task.started_at_ms, 1_785_000_000_000);
            break task.item_id;
        }
    };

    ws.send(ws::text(&ClientMessage::Interrupt { thread_id }))
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "interrupted running tool task was not cleared"
        );
        let Some(Ok(msg)) = tokio::time::timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for the running tool task to clear"))
        else {
            continue;
        };
        let tokio_tungstenite::tungstenite::Message::Text(text) = msg else {
            continue;
        };
        if let ServerMessage::RunningTasks { tasks, .. } =
            serde_json::from_str::<ServerMessage>(&text).unwrap()
            && tasks.iter().all(|task| task.item_id != tool_item_id)
        {
            assert!(
                server
                    .state
                    .registry
                    .thread_runtime(thread_id)
                    .await
                    .unwrap()
                    .tasks_snapshot()
                    .1
                    .is_empty()
            );
            return;
        }
    }
}
