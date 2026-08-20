//! Deterministic, Codex-free giskard-server for end-to-end browser tests (Playwright).
//!
//! The production `giskard-server` binary spawns a real `codex app-server` per project, so it can
//! only run where Codex is installed and authenticated. Browser tests need a server that behaves
//! like the real one — same REST + WebSocket API, same static UI — but is fully self-contained and
//! deterministic. This binary provides exactly that:
//!
//! * a `ScriptedHarness` that never touches the network and emits a fixed, streamed agent reply on
//!   every turn (so the transcript/streaming UI can be asserted on);
//! * a fresh data directory, a known password, and one pre-seeded "Demo" project, so tests can log
//!   in and drive a thread without any host-side setup.
//!
//! It is a test/dev tool: it is not installed by `cargo install` (which targets `--bin
//! giskard-server`) and must never back a real user's data. Configure it with:
//!
//! * `GISKARD_DATA_DIR`   — data dir (created if missing; defaults to a fresh temp dir);
//! * `GISKARD_BIND`       — bind address (default `127.0.0.1:8787`);
//! * `GISKARD_REPLAY_PASSWORD` — the app password (default `giskard`);
//! * `GISKARD_REPLAY_WORKSPACE` — the demo project's workspace dir (created if missing).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use argon2::PasswordHasher;
use async_trait::async_trait;
use tokio::sync::broadcast;
use tracing::{info, warn};

use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalMetadata, ApprovalRequest};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ThreadId, TurnId};
use giskard_core::item::{
    Item, ItemDelta, ItemKind, ItemPayload, ItemStart, SubagentAction, SubagentLink,
};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
};
use giskard_persist::store::ProjectConfig;
use giskard_server::{AppState, HarnessFactory, build_app};

/// The scripted agent's fixed reply. Tests assert on this exact string, so keep it stable.
const SCRIPTED_REPLY: &str = "Hello from the scripted replay harness!";
const SCRIPTED_SUBAGENT_TRIGGER: &str = "Spawn the scripted linked sub-agent.";
const SCRIPTED_NESTED_SUBAGENT_TRIGGER: &str = "Spawn a scripted nested sub-agent.";
const SCRIPTED_SUBAGENT_PROMPT: &str = "Review the linked child task.";
const SCRIPTED_SUBAGENT_REPLY: &str = "Child replay output";
const SCRIPTED_SUBAGENT_PREFIX: &str = "scripted-subagent|";
const SCRIPTED_NESTED_SUBAGENT_PREFIX: &str = "scripted-nested-subagent|";
/// Prompt that spawns a linked child which raises an approval and then holds its turn open. The
/// parent turn completes normally, so browser tests can observe a blocked sub-agent while sitting on
/// the parent thread — the case where the child has no sidebar row of its own.
const SCRIPTED_SUBAGENT_APPROVAL_TRIGGER: &str = "Spawn a sub-agent that needs approval.";
const SCRIPTED_APPROVAL_SUBAGENT_PREFIX: &str = "scripted-approval-subagent|";
const SCRIPTED_SUBAGENT_APPROVAL_ID: &str = "scripted-subagent-approval-1";
const SCRIPTED_SUBAGENT_APPROVAL_COMMAND: &str = "rm -rf ./child-build";
const SCRIPTED_SUBAGENT_AGENT_NAME: &str = "Replay child";
const SCRIPTED_APPROVAL_SUBAGENT_AGENT_NAME: &str = "Approval child";
/// How long the approval-blocked child waits before raising its approval, so the browser's
/// WebSocket is attached and receives the live thread-activity broadcast.
const SCRIPTED_SUBAGENT_APPROVAL_DELAY: std::time::Duration =
    std::time::Duration::from_millis(1500);
/// Prompt that makes the harness raise an approval and then keep the turn in-flight (it never
/// completes). This lets browser tests answer the approval and reload mid-turn to assert the
/// answered card is not re-surfaced as actionable. The approval id is fixed so tests can target it.
const SCRIPTED_APPROVAL_TRIGGER: &str = "Trigger a scripted approval request.";
const SCRIPTED_APPROVAL_ID: &str = "scripted-approval-1";
/// Raises the path-free file approval emitted by current Codex versions when no structured patch
/// preview is available. Grant-root metadata remains available separately.
const SCRIPTED_EMPTY_FILE_APPROVAL_TRIGGER: &str = "Trigger a path-free file approval request.";
const SCRIPTED_EMPTY_FILE_APPROVAL_ID: &str = "scripted-empty-file-approval-1";
/// Raises an approval and then streams a harness error in the same still-open turn. The error is
/// the last activity-bearing event, so a reconnect that took the replayed events at face value
/// would land on "errored, no active turn" and lose the fact that the turn is still blocked on the
/// user. Re-asserting the outstanding set after the replay is what restores it.
const SCRIPTED_APPROVAL_THEN_ERROR_TRIGGER: &str = "Trigger an approval followed by an error.";
const SCRIPTED_APPROVAL_THEN_ERROR_MESSAGE: &str = "Scripted non-fatal harness error.";

/// Prompt that raises a `requestUserInput` server request and then keeps the turn in-flight. This
/// harness deliberately never emits `ServerRequestResolved` when the answer is routed — modelling a
/// harness whose resolved event is late or absent, which is the window a reload has to survive.
const SCRIPTED_SERVER_REQUEST_TRIGGER: &str = "Trigger a scripted user input request.";
const SCRIPTED_SERVER_REQUEST_ID: &str = "scripted-server-request-1";
const SCRIPTED_SERVER_REQUEST_QUESTION: &str = "Which branch should I use?";
/// Raises a server request and then streams a harness error in the same still-open turn. The error
/// is the last activity-bearing event, so a reconnect that took the replayed events at face value
/// would land on "errored, no active turn" and lose the fact that the turn is still blocked on the
/// user. Re-asserting the outstanding set after the replay is what restores it (SR11b).
const SCRIPTED_SERVER_REQUEST_THEN_ERROR_TRIGGER: &str =
    "Trigger a server request followed by an error.";
const SCRIPTED_SERVER_REQUEST_THEN_ERROR_MESSAGE: &str = "Scripted non-fatal harness error.";
/// How long a scripted turn waits for the server's event forwarder to subscribe before giving up.
const RECEIVER_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const RECEIVER_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

/// A harness that speaks the neutral protocol but has no backend: every turn streams the same
/// canned agent message, so the browser-visible transcript is fully deterministic.
struct ScriptedHarness {
    capabilities: HarnessCapabilities,
    threads: tokio::sync::Mutex<Vec<(ThreadId, broadcast::Sender<AgentEvent>)>>,
    /// Where each in-flight scripted approval was raised, so `respond_approval` can emit its
    /// confirmation item on the right still-open turn (the reload e2e test uses that ack to know the
    /// server has recorded the answer before it reconnects). Keyed by approval id rather than held
    /// as a single slot: a parent and a sub-agent can be blocked at the same time, and a shared slot
    /// would let the later one overwrite the earlier and misattribute its ack. Shared, because a
    /// sub-agent's approval is raised from the detached task that drives the child's turn.
    active_approvals: ActiveApprovals,
}

type ActiveApprovals = Arc<tokio::sync::Mutex<HashMap<ApprovalId, (ThreadId, TurnId)>>>;

impl ScriptedHarness {
    fn new() -> Self {
        Self {
            capabilities: HarnessCapabilities {
                live_approvals: true,
                plan_build_modes: true,
                per_turn_model: true,
                reasoning_effort: true,
                structured_diffs: true,
                resumable_threads: true,
                model_listing: false,
                // The scripted harness knows its one provider, so the picker exercises the same
                // id-validation path the real Codex harness does.
                provider_listing: true,
                token_usage: true,
                mcp_status: false,
                mcp_reload: false,
                mcp_oauth_login: false,
                context_compaction: false,
            },
            threads: tokio::sync::Mutex::new(Vec::new()),
            active_approvals: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Wait for the server's event forwarder to attach before scripting a turn. A `broadcast` sender
    /// drops anything sent with no receivers, so every scripted turn must gate on this.
    async fn wait_for_receiver(sender: &broadcast::Sender<AgentEvent>) -> bool {
        let deadline = tokio::time::Instant::now() + RECEIVER_WAIT_TIMEOUT;
        while sender.receiver_count() == 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(RECEIVER_POLL_INTERVAL).await;
        }
        sender.receiver_count() > 0
    }

    async fn sender_for(&self, thread: ThreadId) -> Option<broadcast::Sender<AgentEvent>> {
        let threads = self.threads.lock().await;
        threads
            .iter()
            .find(|(id, _)| *id == thread)
            .map(|(_, tx)| tx.clone())
    }

    fn subagent_parent(native_thread_id: &str) -> Option<String> {
        [
            SCRIPTED_SUBAGENT_PREFIX,
            SCRIPTED_NESTED_SUBAGENT_PREFIX,
            SCRIPTED_APPROVAL_SUBAGENT_PREFIX,
        ]
        .into_iter()
        .find_map(|prefix| native_thread_id.strip_prefix(prefix))
        .and_then(|value| value.rsplit_once('|'))
        .map(|(parent, _)| parent.to_owned())
    }

    /// Drive a child turn that blocks on an approval and never completes. The parent's own turn has
    /// already finished by the time this runs, so the browser is left with a blocked thread that has
    /// no sidebar row — the exact state the ancestor badge, the sub-agents button, and the approval
    /// notification have to surface.
    fn spawn_approval_subagent_turn(
        sender: broadcast::Sender<AgentEvent>,
        thread_id: ThreadId,
        active_approvals: ActiveApprovals,
    ) {
        tokio::spawn(async move {
            if !Self::wait_for_receiver(&sender).await {
                return;
            }

            // The forwarder is listening, but the browser opens its WebSocket a few milliseconds
            // after the HTTP call that started the parent turn. Thread activity is broadcast live
            // and never replayed on connect, so firing immediately would race the client and the
            // approval would be broadcast to nobody. Give the browser time to attach.
            tokio::time::sleep(SCRIPTED_SUBAGENT_APPROVAL_DELAY).await;

            let turn = TurnId::new();
            active_approvals.lock().await.insert(
                ApprovalId(SCRIPTED_SUBAGENT_APPROVAL_ID.into()),
                (thread_id, turn),
            );
            let _ = sender.send(AgentEvent::TurnStarted {
                thread: thread_id,
                turn,
            });
            tokio::task::yield_now().await;
            let _ = sender.send(AgentEvent::ApprovalRequested {
                thread: thread_id,
                turn,
                request: ApprovalRequest {
                    id: ApprovalId(SCRIPTED_SUBAGENT_APPROVAL_ID.into()),
                    kind: ApprovalKind::CommandExecution {
                        command: SCRIPTED_SUBAGENT_APPROVAL_COMMAND.into(),
                        cwd: "/tmp/demo".into(),
                    },
                    reason: Some("The sub-agent wants to remove its build directory.".into()),
                    metadata: vec![],
                    available: vec![ApprovalDecision::Accept, ApprovalDecision::Decline],
                },
            });
        });
    }

    fn spawn_nested_subagent_turn(
        sender: broadcast::Sender<AgentEvent>,
        thread_id: ThreadId,
        parent_harness_thread_id: String,
    ) {
        tokio::spawn(async move {
            if !Self::wait_for_receiver(&sender).await {
                return;
            }

            let turn = TurnId::new();
            // Mirror the collaboration-v2 race seen from Codex: a turn-scoped sub-agent activity
            // can arrive before the corresponding TurnStarted notification.
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            let _ = sender.send(AgentEvent::ItemCompleted {
                thread: thread_id,
                turn,
                item: Item {
                    id: ItemId::new(),
                    harness_item_id: format!("scripted_nested_subagent_link_{turn}"),
                    payload: ItemPayload::Activity {
                        title: "Sub-agent running".into(),
                        detail: Some("Nested replay child".into()),
                        metadata: None,
                        subagent: Some(SubagentLink {
                            harness_thread_id: format!(
                                "{SCRIPTED_SUBAGENT_PREFIX}{parent_harness_thread_id}|{turn}"
                            ),
                            path: Some("Nested replay child".into()),
                            initial_prompt: Some("Run the nested replay task.".into()),
                            action: SubagentAction::Started,
                            status: None,
                            message: None,
                        }),
                    },
                    created_at: chrono::Utc::now(),
                },
            });
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            let _ = sender.send(AgentEvent::TurnStarted {
                thread: thread_id,
                turn,
            });
            tokio::task::yield_now().await;
            let wait_item_id = ItemId::new();
            let _ = sender.send(AgentEvent::ItemStarted {
                thread: thread_id,
                turn,
                item: ItemStart {
                    id: wait_item_id,
                    harness_item_id: format!("scripted_nested_wait_{turn}"),
                    kind: ItemKind::ToolCall,
                    command: None,
                    tool: Some(giskard_core::item::ToolCallStart {
                        name: "wait".into(),
                        input: serde_json::json!({}),
                        server: Some("collab-agent".into()),
                        status: Some("in_progress".into()),
                        metadata: None,
                        subagent: None,
                        started_at_ms: None,
                    }),
                },
            });
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            let _ = sender.send(AgentEvent::TurnCompleted {
                thread: thread_id,
                turn,
                usage: TokenUsage::new(30, 6),
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
            });
        });
    }

    fn spawn_subagent_turn(
        sender: broadcast::Sender<AgentEvent>,
        thread_id: ThreadId,
        parent_harness_thread_id: String,
    ) {
        tokio::spawn(async move {
            if !Self::wait_for_receiver(&sender).await {
                return;
            }

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
                    harness_item_id: format!("scripted_child_reply_{turn}"),
                    payload: ItemPayload::AgentMessage {
                        text: SCRIPTED_SUBAGENT_REPLY.into(),
                    },
                    created_at: chrono::Utc::now(),
                },
            });
            tokio::task::yield_now().await;
            let _ = sender.send(AgentEvent::ItemCompleted {
                thread: thread_id,
                turn,
                item: Item {
                    id: ItemId::new(),
                    harness_item_id: format!("scripted_reverse_link_{turn}"),
                    payload: ItemPayload::Activity {
                        title: "Sub-agent interacted".into(),
                        detail: Some("Sent a result to the parent".into()),
                        metadata: None,
                        subagent: Some(SubagentLink {
                            harness_thread_id: parent_harness_thread_id,
                            path: Some("/root".into()),
                            initial_prompt: None,
                            action: SubagentAction::Interacted,
                            status: None,
                            message: None,
                        }),
                    },
                    created_at: chrono::Utc::now(),
                },
            });
            tokio::task::yield_now().await;
            let _ = sender.send(AgentEvent::TurnCompleted {
                thread: thread_id,
                turn,
                usage: TokenUsage::new(40, 12),
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
            });
        });
    }
}

#[async_trait]
impl AgentHarness for ScriptedHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        self.capabilities
    }

    async fn list_models(&self) -> Result<Vec<giskard_core::model::ModelDescriptor>, HarnessError> {
        Ok(vec![])
    }

    /// The scripted stand-in for Codex's `[model_providers]` table: one provider, no endpoint, so
    /// the seeded config's `replay` id validates while no discovery is attempted.
    async fn list_providers(&self) -> Result<Vec<giskard_harness::HarnessProvider>, HarnessError> {
        Ok(vec![giskard_harness::HarnessProvider {
            id: "replay".into(),
            name: Some("Replay (scripted)".into()),
            base_url: None,
            auth: None,
        }])
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        let thread = opts.thread.unwrap_or_default();
        let harness_thread_id = opts
            .resume
            .clone()
            .unwrap_or_else(|| format!("scripted_{thread}"));

        let (new_sender, _) = broadcast::channel(256);
        let mut threads = self.threads.lock().await;
        let (sender, is_new) =
            if let Some((_, existing)) = threads.iter().find(|(id, _)| *id == thread) {
                (existing.clone(), false)
            } else {
                threads.push((thread, new_sender.clone()));
                (new_sender, true)
            };
        drop(threads);

        let parent_harness_thread_id = Self::subagent_parent(&harness_thread_id);
        let blocks_on_approval = harness_thread_id.starts_with(SCRIPTED_APPROVAL_SUBAGENT_PREFIX);
        if is_new && let Some(parent) = parent_harness_thread_id.clone() {
            if harness_thread_id.starts_with(SCRIPTED_NESTED_SUBAGENT_PREFIX) {
                Self::spawn_nested_subagent_turn(sender, thread, harness_thread_id.clone());
            } else if blocks_on_approval {
                Self::spawn_approval_subagent_turn(sender, thread, self.active_approvals.clone());
            } else {
                Self::spawn_subagent_turn(sender, thread, parent);
            }
        }

        Ok(ThreadHandle {
            resumed_model: opts
                .initial_model
                .clone()
                .or_else(|| Some(fake_native_model())),
            agent_name: parent_harness_thread_id.as_ref().map(|_| {
                if blocks_on_approval {
                    SCRIPTED_APPROVAL_SUBAGENT_AGENT_NAME.to_string()
                } else {
                    SCRIPTED_SUBAGENT_AGENT_NAME.to_string()
                }
            }),
            parent_harness_thread_id,
            ..ThreadHandle::opened(thread, harness_thread_id, opts.workspace_root.clone())
        })
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        input: UserInput,
        _overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let turn = TurnId::new();
        let thread_id = thread.thread;
        let Some(sender) = self.sender_for(thread_id).await else {
            return Err(HarnessError::ThreadNotFound(thread_id));
        };

        let input_text = input.as_text();
        let subagent_native_thread_id = match input_text {
            Some(SCRIPTED_SUBAGENT_TRIGGER) => Some(format!(
                "{SCRIPTED_SUBAGENT_PREFIX}{}|{turn}",
                thread.harness_thread_id
            )),
            Some(SCRIPTED_NESTED_SUBAGENT_TRIGGER) => Some(format!(
                "{SCRIPTED_NESTED_SUBAGENT_PREFIX}{}|{turn}",
                thread.harness_thread_id
            )),
            Some(SCRIPTED_SUBAGENT_APPROVAL_TRIGGER) => Some(format!(
                "{SCRIPTED_APPROVAL_SUBAGENT_PREFIX}{}|{turn}",
                thread.harness_thread_id
            )),
            _ => None,
        };

        let raise_approval_then_error = input_text == Some(SCRIPTED_APPROVAL_THEN_ERROR_TRIGGER);
        let raise_empty_file_approval = input_text == Some(SCRIPTED_EMPTY_FILE_APPROVAL_TRIGGER);
        let raise_approval = input_text == Some(SCRIPTED_APPROVAL_TRIGGER)
            || raise_approval_then_error
            || raise_empty_file_approval;
        if raise_approval {
            let approval_id = if raise_empty_file_approval {
                SCRIPTED_EMPTY_FILE_APPROVAL_ID
            } else {
                SCRIPTED_APPROVAL_ID
            };
            self.active_approvals
                .lock()
                .await
                .insert(ApprovalId(approval_id.into()), (thread_id, turn));
        }

        let raise_server_request_then_error =
            input_text == Some(SCRIPTED_SERVER_REQUEST_THEN_ERROR_TRIGGER);
        let raise_server_request =
            input_text == Some(SCRIPTED_SERVER_REQUEST_TRIGGER) || raise_server_request_then_error;

        // Stream the canned reply the way a real harness would: start, incremental deltas, then a
        // completed item and a turn-completed with token usage. Emitted off-task with yields so the
        // WebSocket layer observes distinct frames (the transcript renders progressively).
        tokio::spawn(async move {
            if raise_server_request {
                // Raise a user-input request and leave the turn in-flight. Answering it routes a
                // response to `respond_server_request`, which deliberately stays silent: a browser
                // reload must still render the card resolved, from the server's recorded answer
                // rather than from a harness resolved event that never comes.
                let _ = sender.send(AgentEvent::TurnStarted {
                    thread: thread_id,
                    turn,
                });
                tokio::task::yield_now().await;
                let _ = sender.send(AgentEvent::ServerRequestReceived {
                    thread: thread_id,
                    turn: Some(turn),
                    request: giskard_core::server_request::ServerRequest {
                        id: giskard_core::ids::ServerRequestId(SCRIPTED_SERVER_REQUEST_ID.into()),
                        method: "item/tool/requestUserInput".into(),
                        params: serde_json::json!({
                            "questions": [{
                                "id": "branch",
                                "header": "Branch",
                                "question": SCRIPTED_SERVER_REQUEST_QUESTION,
                                "options": [
                                    { "label": "main", "description": "The default branch" },
                                    { "label": "develop", "description": "The integration branch" }
                                ]
                            }]
                        }),
                        received_at: chrono::Utc::now(),
                    },
                });
                if raise_server_request_then_error {
                    tokio::task::yield_now().await;
                    let _ = sender.send(AgentEvent::Error {
                        thread: thread_id,
                        turn: Some(turn),
                        error: giskard_core::error::HarnessError::Protocol(
                            SCRIPTED_SERVER_REQUEST_THEN_ERROR_MESSAGE.into(),
                        ),
                    });
                }
                return;
            }

            if raise_approval {
                // Raise an approval and deliberately leave the turn in-flight (no TurnCompleted), so
                // the live buffer keeps the answered state for reconnect assertions.
                let _ = sender.send(AgentEvent::TurnStarted {
                    thread: thread_id,
                    turn,
                });
                tokio::task::yield_now().await;
                let _ = sender.send(AgentEvent::ApprovalRequested {
                    thread: thread_id,
                    turn,
                    request: ApprovalRequest {
                        id: ApprovalId(
                            if raise_empty_file_approval {
                                SCRIPTED_EMPTY_FILE_APPROVAL_ID
                            } else {
                                SCRIPTED_APPROVAL_ID
                            }
                            .into(),
                        ),
                        kind: if raise_empty_file_approval {
                            ApprovalKind::FileChange {
                                path: std::path::PathBuf::new(),
                                change: giskard_core::item::FileChangeKind::Modified,
                            }
                        } else {
                            ApprovalKind::CommandExecution {
                                command: "rm -rf ./build".into(),
                                cwd: "/tmp/demo".into(),
                            }
                        },
                        reason: (!raise_empty_file_approval)
                            .then(|| "The agent wants to remove the build directory.".into()),
                        metadata: if raise_empty_file_approval {
                            vec![ApprovalMetadata::Path {
                                label: "Grant root".into(),
                                path: "/tmp/project".into(),
                                source_link: false,
                            }]
                        } else {
                            vec![]
                        },
                        available: vec![ApprovalDecision::Accept, ApprovalDecision::Decline],
                    },
                });
                if raise_approval_then_error {
                    tokio::task::yield_now().await;
                    let _ = sender.send(AgentEvent::Error {
                        thread: thread_id,
                        turn: Some(turn),
                        error: giskard_core::error::HarnessError::Protocol(
                            SCRIPTED_APPROVAL_THEN_ERROR_MESSAGE.into(),
                        ),
                    });
                }
                return;
            }

            if let Some(native_thread_id) = subagent_native_thread_id {
                let child_name = if native_thread_id.starts_with(SCRIPTED_APPROVAL_SUBAGENT_PREFIX)
                {
                    SCRIPTED_APPROVAL_SUBAGENT_AGENT_NAME
                } else {
                    SCRIPTED_SUBAGENT_AGENT_NAME
                };
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
                        harness_item_id: format!("scripted_subagent_link_{turn}"),
                        payload: ItemPayload::Activity {
                            title: "Sub-agent running".into(),
                            detail: Some(child_name.into()),
                            metadata: None,
                            subagent: Some(SubagentLink {
                                harness_thread_id: native_thread_id,
                                path: Some(child_name.into()),
                                initial_prompt: Some(SCRIPTED_SUBAGENT_PROMPT.into()),
                                action: SubagentAction::Started,
                                status: None,
                                message: None,
                            }),
                        },
                        created_at: chrono::Utc::now(),
                    },
                });
                tokio::task::yield_now().await;
                let _ = sender.send(AgentEvent::TurnCompleted {
                    thread: thread_id,
                    turn,
                    usage: TokenUsage::new(25, 5),
                    status: TurnStatus {
                        kind: TurnStatusKind::Completed,
                        message: None,
                    },
                });
                return;
            }

            let item_id = ItemId::new();
            let _ = sender.send(AgentEvent::TurnStarted {
                thread: thread_id,
                turn,
            });
            tokio::task::yield_now().await;
            let _ = sender.send(AgentEvent::ItemStarted {
                thread: thread_id,
                turn,
                item: ItemStart {
                    id: item_id,
                    harness_item_id: "scripted_1".into(),
                    kind: ItemKind::AgentMessage,
                    command: None,
                    tool: None,
                },
            });
            tokio::task::yield_now().await;
            for word in SCRIPTED_REPLY.split_inclusive(' ') {
                let _ = sender.send(AgentEvent::ItemDelta {
                    thread: thread_id,
                    turn,
                    item_id,
                    delta: ItemDelta::Text { text: word.into() },
                });
                tokio::task::yield_now().await;
            }
            let _ = sender.send(AgentEvent::ItemCompleted {
                thread: thread_id,
                turn,
                item: Item {
                    id: item_id,
                    harness_item_id: "scripted_1".into(),
                    payload: ItemPayload::AgentMessage {
                        text: SCRIPTED_REPLY.into(),
                    },
                    created_at: chrono::Utc::now(),
                },
            });
            tokio::task::yield_now().await;
            let _ = sender.send(AgentEvent::TurnCompleted {
                thread: thread_id,
                turn,
                usage: TokenUsage::new(120, 34),
                status: TurnStatus {
                    kind: TurnStatusKind::Completed,
                    message: None,
                },
            });
        });

        Ok(turn)
    }

    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream {
        if let Ok(threads) = self.threads.try_lock()
            && let Some((_, tx)) = threads.iter().find(|(id, _)| *id == thread.thread)
        {
            return AgentEventStream::new(tx.subscribe());
        }
        let (_, rx) = broadcast::channel(1);
        AgentEventStream::new(rx)
    }

    async fn respond_approval(
        &self,
        req: giskard_core::ids::ApprovalId,
        decision: giskard_core::approval::ApprovalDecision,
    ) -> Result<(), HarnessError> {
        // Emit a confirmation item on the still-open turn so tests have a deterministic signal that
        // the answer was routed. The turn stays in-flight (no TurnCompleted) so a reconnect still
        // replays the answered approval from the live buffer. Look the location up by the answered
        // id: several approvals can be pending at once, on different threads.
        let raised_at = self.active_approvals.lock().await.remove(&req);
        if let Some((thread_id, turn)) = raised_at
            && let Some(sender) = self.sender_for(thread_id).await
        {
            let label = match decision {
                ApprovalDecision::Accept => "accept",
                ApprovalDecision::AcceptForSession => "accept_for_session",
                ApprovalDecision::Decline => "decline",
                ApprovalDecision::Cancel => "cancel",
                ApprovalDecision::AcceptWithExecPolicyAmendment { .. } => "accept_amended",
            };
            let _ = sender.send(AgentEvent::ItemCompleted {
                thread: thread_id,
                turn,
                item: Item {
                    id: ItemId::new(),
                    harness_item_id: format!("scripted_approval_ack_{turn}"),
                    payload: ItemPayload::AgentMessage {
                        text: format!("Approval recorded: {label}"),
                    },
                    created_at: chrono::Utc::now(),
                },
            });
        }
        Ok(())
    }

    async fn respond_server_request(
        &self,
        _req: giskard_core::ids::ServerRequestId,
        _response: giskard_core::server_request::ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn interrupt(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn delete_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError> {
        self.threads
            .lock()
            .await
            .retain(|(thread_id, _)| *thread_id != thread.thread);
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

struct ScriptedFactory;

#[async_trait]
impl HarnessFactory for ScriptedFactory {
    async fn create(&self, _config: &ProjectConfig) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        Ok(Arc::new(ScriptedHarness::new()))
    }
}

fn env_path(var: &str) -> Option<PathBuf> {
    std::env::var_os(var).map(PathBuf::from)
}

/// Argon2 hash of the given password, in the PHC string form the login path expects.
fn hash_password(password: &str) -> Result<String, String> {
    use argon2::password_hash::SaltString;
    let salt = SaltString::generate(&mut rand::rngs::OsRng);
    argon2::Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| format!("failed to hash replay password: {e}"))
}

/// Write a `config.toml` into `data_dir` so the standard loader reads it back: this keeps the
/// replay server on the exact same config path as production instead of hand-building `Config`.
fn write_config(data_dir: &Path, bind: &str, password_hash: &str) -> Result<(), String> {
    let config = format!(
        r#"[server]
bind = "{bind}"
# Plain HTTP for local/CI tests: browsers refuse a Secure cookie over http://.
secure_cookies = false

[auth]
password_hash = "{password_hash}"

[harness]
kind = "replay"

[providers.replay]
model_listing = false
  [[providers.replay.models]]
  id = "replay-model"
  display_name = "Replay Model"
  context_window = 131072
  supports_reasoning_effort = true
"#
    );
    std::fs::write(data_dir.join("config.toml"), config)
        .map_err(|e| format!("cannot write config.toml in {}: {e}", data_dir.display()))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "giskard=info,tower_http=info".into()),
        )
        .init();

    if let Err(e) = run().await {
        eprintln!("giskard-server-replay: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let data_dir = env_path("GISKARD_DATA_DIR").unwrap_or_else(|| {
        std::env::temp_dir().join(format!("giskard-replay-{}", std::process::id()))
    });
    std::fs::create_dir_all(&data_dir)
        .map_err(|e| format!("cannot create data dir {}: {e}", data_dir.display()))?;

    let bind = std::env::var("GISKARD_BIND").unwrap_or_else(|_| "127.0.0.1:8787".to_string());
    let password =
        std::env::var("GISKARD_REPLAY_PASSWORD").unwrap_or_else(|_| "giskard".to_string());
    let password_hash = hash_password(&password)?;
    write_config(&data_dir, &bind, &password_hash)?;

    let store = Arc::new(giskard_persist::PersistStore::new(data_dir.clone()));
    let config = store
        .load_config()
        .await
        .map_err(|e| format!("cannot load generated config: {e}"))?;

    // Seed one project so tests have a thread to drive without exercising the folder picker. The
    // scripted harness ignores the workspace path, but we still create it so any file endpoints
    // resolve to a real directory.
    let workspace =
        env_path("GISKARD_REPLAY_WORKSPACE").unwrap_or_else(|| data_dir.join("demo-workspace"));
    std::fs::create_dir_all(&workspace)
        .map_err(|e| format!("cannot create workspace {}: {e}", workspace.display()))?;
    if let Err(error) = seed_git_workspace(&workspace) {
        warn!(workspace = %workspace.display(), %error, "could not seed replay workspace git repository");
    }

    let projects = store
        .load_project_index()
        .await
        .map_err(|e| format!("cannot read project index: {e}"))?;
    if projects.projects.is_empty() {
        store
            .create_project(
                giskard_core::ids::ProjectId::new(),
                "Demo",
                &workspace.to_string_lossy(),
            )
            .await
            .map_err(|e| format!("cannot seed demo project: {e}"))?;
        info!(workspace = %workspace.display(), "seeded demo project");
    }

    // A fresh random session key each boot is fine: the replay server holds no durable sessions.
    let mut session_key = [0u8; 32];
    {
        use rand::RngCore;
        rand::rngs::OsRng.fill_bytes(&mut session_key);
    }

    let factory = Arc::new(ScriptedFactory);
    let state = AppState::new_with_config(store, factory, session_key.to_vec(), Some(&config.viz));
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|e| format!("cannot bind {bind}: {e}"))?;
    info!(bind = %bind, data_dir = %data_dir.display(), "giskard-server-replay listening");
    axum::serve(listener, app)
        .await
        .map_err(|e| format!("server error: {e}"))?;
    Ok(())
}

fn seed_git_workspace(workspace: &Path) -> Result<(), String> {
    // `GISKARD_REPLAY_WORKSPACE` can point at a directory that outlives the process. Re-seeding one
    // would rewrite the demo source back to its committed content and then fail on an empty commit,
    // leaving a clean tree and a repository whose branch has been reset out from under whoever was
    // using it — so an existing repository is left exactly as it is.
    if workspace.join(".git").exists() {
        return Ok(());
    }
    std::fs::create_dir_all(workspace.join("src"))
        .map_err(|e| format!("cannot create demo source dir: {e}"))?;
    std::fs::write(
        workspace.join("README.md"),
        "# Demo workspace\n\nSeeded for Giskard screenshots.\n",
    )
    .map_err(|e| format!("cannot write demo README: {e}"))?;
    std::fs::write(
        workspace.join("src/main.rs"),
        "fn main() {\n    println!(\"hello from demo\");\n}\n",
    )
    .map_err(|e| format!("cannot write demo source: {e}"))?;

    run_git_seed(workspace, ["init"])?;
    run_git_seed(workspace, ["checkout", "-B", "main"])?;
    run_git_seed(
        workspace,
        ["config", "user.email", "giskard-replay@example.invalid"],
    )?;
    run_git_seed(workspace, ["config", "user.name", "Giskard Replay"])?;
    run_git_seed(workspace, ["add", "README.md", "src/main.rs"])?;
    run_git_seed(workspace, ["commit", "-m", "Seed demo workspace"])?;

    std::fs::write(
        workspace.join("src/main.rs"),
        "fn main() {\n    println!(\"hello from demo\");\n    println!(\"edited for status\");\n}\n",
    )
    .map_err(|e| format!("cannot modify demo source: {e}"))?;
    Ok(())
}

fn run_git_seed<const N: usize>(workspace: &Path, args: [&str; N]) -> Result<(), String> {
    let output = std::process::Command::new("git")
        .current_dir(workspace)
        .args(args)
        .output()
        .map_err(|e| format!("failed to run git: {e}"))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    Err(if stderr.is_empty() {
        "git command failed".into()
    } else {
        stderr
    })
}

/// The model a fake harness reports for a thread it is asked to import. A real harness answers this
/// from the thread itself; an import names no model, so the fake stands in with a fixed one rather
/// than claiming not to know.
fn fake_native_model() -> giskard_core::model::ModelRef {
    // The identity this server actually advertises, in `[providers.<id>]` and `list_providers` alike.
    // Reporting anything else would bind an imported thread to a provider the picker never offers.
    giskard_core::model::ModelRef {
        provider: "replay".into(),
        model: "replay-model".into(),
        reasoning_effort: None,
    }
}
