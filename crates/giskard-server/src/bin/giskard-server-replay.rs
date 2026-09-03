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
use tracing::{error, info, warn};

use giskard_core::approval::{ApprovalDecision, ApprovalKind, ApprovalMetadata, ApprovalRequest};
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ThreadId, TurnId};
use giskard_core::item::{
    FileChangeEntry, FileChangeKind, Item, ItemDelta, ItemKind, ItemPayload, ItemStart,
    SubagentAction, SubagentLink,
};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, EventLog, HarnessBootstrap, HarnessCapabilities,
    OpenThreadOptions, ThreadHandle,
};
use giskard_persist::store::ProjectConfig;
use giskard_server::{AppState, HarnessFactory, build_app};

mod common;

/// The scripted agent's fixed reply. Tests assert on this exact string, so keep it stable.
const SCRIPTED_REPLY: &str = "Hello from the scripted replay harness!";
const SCRIPTED_DIFF_TRIGGER: &str = "Trigger two scripted lazy diffs.";
const SCRIPTED_DIFF_PATH: &str = "src/lazy-diff.rs";
const SCRIPTED_DIFF_REPLACEMENT_DELAY: std::time::Duration = std::time::Duration::from_millis(1200);
const SCRIPTED_DIFF_COMPLETION_DELAY: std::time::Duration = std::time::Duration::from_millis(1000);
const HTTP_GRACEFUL_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
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
/// Prompt that makes the harness stream a reasoning note before the canned reply, so browser tests
/// can drive the collapsible "thinking" row (§7.3). The note is Markdown with a bold first line —
/// the collapsed row summarizes that line with its emphasis marks stripped.
const SCRIPTED_REASONING_TRIGGER: &str = "Think out loud before replying.";
const SCRIPTED_REASONING_SUMMARY: &str = "Weighing the scripted options";
const SCRIPTED_REASONING_DETAIL: &str = "Then answering with the deterministic scripted reply.";
/// A harness that speaks the neutral protocol but has no backend: every turn streams the same
/// canned agent message, so the browser-visible transcript is fully deterministic.
struct ScriptedHarness {
    capabilities: HarnessCapabilities,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Route scripted replay events to locally opened harness threads.
    // Source of truth: Scripted harness open/resume operations establish each sender.
    // Structural reason: This non-test-gated replay adapter cannot use server authorities.
    // Synchronization: The mutex protects linear lookup, insertion, and removal.
    // Invalidation/removal: Thread close removes state; dropping the harness removes all entries.
    threads: tokio::sync::Mutex<Vec<(ThreadId, Arc<EventLog>)>>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Translate scripted native thread identifiers to Giskard thread identifiers.
    // Source of truth: Bootstrap and import claims establish the bijective bindings.
    // Structural reason: Replay native-ID routing models a provider adapter boundary.
    // Synchronization: The mutex protects claim validation, lookup, and insertion.
    // Invalidation/removal: Bindings live for the scripted harness process and drop with it.
    native_bindings: tokio::sync::Mutex<Vec<(String, ThreadId)>>,
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
    fn new(bootstrap: HarnessBootstrap) -> Result<Self, HarnessError> {
        let mut native_bindings = Vec::with_capacity(bootstrap.known_threads.len());
        for binding in bootstrap.known_threads {
            Self::claim_binding(
                &mut native_bindings,
                binding.harness_thread_id,
                binding.thread_id,
            )?;
        }
        Ok(Self {
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
            native_bindings: tokio::sync::Mutex::new(native_bindings),
            active_approvals: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        })
    }

    fn claim_binding(
        bindings: &mut Vec<(String, ThreadId)>,
        harness_thread_id: String,
        thread: ThreadId,
    ) -> Result<(), HarnessError> {
        if harness_thread_id.trim().is_empty() {
            return Err(HarnessError::Protocol(
                "native thread identity must not be empty".into(),
            ));
        }
        if let Some((_, existing_thread)) = bindings
            .iter()
            .find(|(native, _)| native == &harness_thread_id)
        {
            return (*existing_thread == thread).then_some(()).ok_or_else(|| {
                HarnessError::Protocol(format!(
                    "native thread {harness_thread_id} is already bound to {existing_thread}"
                ))
            });
        }
        if let Some((existing_native, _)) = bindings.iter().find(|(_, local)| *local == thread) {
            return Err(HarnessError::Protocol(format!(
                "thread {thread} is already bound to native thread {existing_native}"
            )));
        }
        bindings.push((harness_thread_id, thread));
        Ok(())
    }

    async fn sender_for(&self, thread: ThreadId) -> Option<Arc<EventLog>> {
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

    async fn attach_thread(
        &self,
        thread: ThreadId,
        harness_thread_id: &str,
    ) -> (Option<String>, bool) {
        let new_sender = Arc::new(EventLog::new());
        let mut threads = self.threads.lock().await;
        let (sender, is_new) =
            if let Some((_, existing)) = threads.iter().find(|(id, _)| *id == thread) {
                (existing.clone(), false)
            } else {
                threads.push((thread, new_sender.clone()));
                (new_sender, true)
            };
        drop(threads);

        let parent = Self::subagent_parent(harness_thread_id);
        let blocks_on_approval = harness_thread_id.starts_with(SCRIPTED_APPROVAL_SUBAGENT_PREFIX);
        if is_new && let Some(parent_id) = parent.clone() {
            if harness_thread_id.starts_with(SCRIPTED_NESTED_SUBAGENT_PREFIX) {
                Self::spawn_nested_subagent_turn(sender, thread, harness_thread_id.to_owned());
            } else if blocks_on_approval {
                Self::spawn_approval_subagent_turn(sender, thread, self.active_approvals.clone());
            } else {
                Self::spawn_subagent_turn(sender, thread, parent_id);
            }
        }
        (parent, blocks_on_approval)
    }

    /// Drive a child turn that blocks on an approval and never completes. The parent's own turn has
    /// already finished by the time this runs, so the browser is left with a blocked thread that has
    /// no sidebar row — the exact state the ancestor badge, the sub-agents button, and the approval
    /// notification have to surface.
    fn spawn_approval_subagent_turn(
        sender: Arc<EventLog>,
        thread_id: ThreadId,
        active_approvals: ActiveApprovals,
    ) {
        tokio::spawn(async move {
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
            let _ = sender.append(AgentEvent::TurnStarted {
                thread: thread_id,
                turn,
            });
            tokio::task::yield_now().await;
            let _ = sender.append(AgentEvent::ApprovalRequested {
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
        sender: Arc<EventLog>,
        thread_id: ThreadId,
        parent_harness_thread_id: String,
    ) {
        tokio::spawn(async move {
            let turn = TurnId::new();
            // Mirror the collaboration-v2 race seen from Codex: a turn-scoped sub-agent activity
            // can arrive before the corresponding TurnStarted notification.
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            let _ = sender.append(AgentEvent::ItemCompleted {
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
            let _ = sender.append(AgentEvent::TurnStarted {
                thread: thread_id,
                turn,
            });
            tokio::task::yield_now().await;
            let wait_item_id = ItemId::new();
            let _ = sender.append(AgentEvent::ItemStarted {
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
            let _ = sender.append(AgentEvent::TurnCompleted {
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
        sender: Arc<EventLog>,
        thread_id: ThreadId,
        parent_harness_thread_id: String,
    ) {
        tokio::spawn(async move {
            let turn = TurnId::new();
            let _ = sender.append(AgentEvent::TurnStarted {
                thread: thread_id,
                turn,
            });
            tokio::task::yield_now().await;
            let _ = sender.append(AgentEvent::ItemCompleted {
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
            let _ = sender.append(AgentEvent::ItemCompleted {
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
            let _ = sender.append(AgentEvent::TurnCompleted {
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
        let thread = opts.thread;
        let harness_thread_id = opts
            .resume
            .clone()
            .unwrap_or_else(|| format!("scripted_{thread}"));

        {
            let mut bindings = self.native_bindings.lock().await;
            Self::claim_binding(&mut bindings, harness_thread_id.clone(), thread)?;
        }

        let (parent_harness_thread_id, blocks_on_approval) =
            self.attach_thread(thread, &harness_thread_id).await;

        Ok(ThreadHandle {
            resumed_model: Some(opts.initial_model.clone()),
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

    async fn claim_native_thread(
        &self,
        proposed_thread: ThreadId,
        harness_thread_id: String,
        workspace_root: PathBuf,
    ) -> Result<ThreadHandle, HarnessError> {
        let thread = {
            let mut bindings = self.native_bindings.lock().await;
            match bindings
                .iter()
                .find(|(native, _)| native == &harness_thread_id)
            {
                Some((_, existing_thread)) => *existing_thread,
                None => {
                    Self::claim_binding(&mut bindings, harness_thread_id.clone(), proposed_thread)?;
                    proposed_thread
                }
            }
        };

        let (parent_harness_thread_id, blocks_on_approval) =
            self.attach_thread(thread, &harness_thread_id).await;

        Ok(ThreadHandle {
            agent_name: parent_harness_thread_id.as_ref().map(|_| {
                if blocks_on_approval {
                    SCRIPTED_APPROVAL_SUBAGENT_AGENT_NAME.to_string()
                } else {
                    SCRIPTED_SUBAGENT_AGENT_NAME.to_string()
                }
            }),
            parent_harness_thread_id,
            ..ThreadHandle::opened(thread, harness_thread_id, workspace_root)
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
        let raise_lazy_diffs = input_text == Some(SCRIPTED_DIFF_TRIGGER);
        let stream_reasoning = input_text == Some(SCRIPTED_REASONING_TRIGGER);

        // Stream the canned reply the way a real harness would: start, incremental deltas, then a
        // completed item and a turn-completed with token usage. Emitted off-task with yields so the
        // WebSocket layer observes distinct frames (the transcript renders progressively).
        tokio::spawn(async move {
            if raise_lazy_diffs {
                let item_id = ItemId::new();
                let file_change = |diff: &str, status: &str| Item {
                    id: item_id,
                    harness_item_id: "scripted_lazy_diff".into(),
                    payload: ItemPayload::FileChange {
                        path: SCRIPTED_DIFF_PATH.into(),
                        change: FileChangeKind::Modified,
                        changes: vec![FileChangeEntry {
                            path: SCRIPTED_DIFF_PATH.into(),
                            change: FileChangeKind::Modified,
                            diff: Some(diff.into()),
                            captured_diff: None,
                        }],
                        status: Some(status.into()),
                    },
                    created_at: chrono::Utc::now(),
                };
                let _ = sender.append(AgentEvent::TurnStarted {
                    thread: thread_id,
                    turn,
                });
                tokio::task::yield_now().await;
                let _ = sender.append(AgentEvent::ItemCompleted {
                    thread: thread_id,
                    turn,
                    item: file_change("@@ -1 +1 @@\n-before\n+first version", "in_progress"),
                });
                tokio::time::sleep(SCRIPTED_DIFF_REPLACEMENT_DELAY).await;
                let _ = sender.append(AgentEvent::ItemCompleted {
                    thread: thread_id,
                    turn,
                    item: file_change("@@ -1 +1 @@\n-before\n+second version", "completed"),
                });
                let _ = sender.append(AgentEvent::DiffUpdated {
                    thread: thread_id,
                    turn,
                    diff: giskard_core::FileDiff {
                        path: "src/full-text-only.rs".into(),
                        change: FileChangeKind::Modified,
                        old_text: Some("fn old() {}\n".into()),
                        new_text: Some("fn new() {}\n".into()),
                        hunks: Vec::new(),
                        binary: false,
                        captured: None,
                    },
                });
                // Keep the replacement live long enough for browser tests to exercise the
                // superseded-id conflict before turn persistence releases runtime diff state.
                tokio::time::sleep(SCRIPTED_DIFF_COMPLETION_DELAY).await;
                let _ = sender.append(AgentEvent::TurnCompleted {
                    thread: thread_id,
                    turn,
                    usage: TokenUsage::new(20, 8),
                    status: TurnStatus {
                        kind: TurnStatusKind::Completed,
                        message: None,
                    },
                });
                return;
            }

            if raise_server_request {
                // Raise a user-input request and leave the turn in-flight. Answering it routes a
                // response to `respond_server_request`, which deliberately stays silent: a browser
                // reload must still render the card resolved, from the server's recorded answer
                // rather than from a harness resolved event that never comes.
                let _ = sender.append(AgentEvent::TurnStarted {
                    thread: thread_id,
                    turn,
                });
                tokio::task::yield_now().await;
                let _ = sender.append(AgentEvent::ServerRequestReceived {
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
                    let _ = sender.append(AgentEvent::Error {
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
                let _ = sender.append(AgentEvent::TurnStarted {
                    thread: thread_id,
                    turn,
                });
                tokio::task::yield_now().await;
                let _ = sender.append(AgentEvent::ApprovalRequested {
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
                    let _ = sender.append(AgentEvent::Error {
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
                let _ = sender.append(AgentEvent::TurnStarted {
                    thread: thread_id,
                    turn,
                });
                tokio::task::yield_now().await;
                let _ = sender.append(AgentEvent::ItemCompleted {
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
                let _ = sender.append(AgentEvent::TurnCompleted {
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
            let _ = sender.append(AgentEvent::TurnStarted {
                thread: thread_id,
                turn,
            });
            tokio::task::yield_now().await;
            if stream_reasoning {
                // Stream the note the way a real harness does — start, text deltas, completion — so
                // the browser exercises both the live "thinking" row and its persisted form.
                let reasoning_id = ItemId::new();
                let reasoning_text =
                    format!("**{SCRIPTED_REASONING_SUMMARY}**\n\n{SCRIPTED_REASONING_DETAIL}");
                let _ = sender.append(AgentEvent::ItemStarted {
                    thread: thread_id,
                    turn,
                    item: ItemStart {
                        id: reasoning_id,
                        harness_item_id: "scripted_reasoning_1".into(),
                        kind: ItemKind::Reasoning,
                        command: None,
                        tool: None,
                    },
                });
                tokio::task::yield_now().await;
                for chunk in reasoning_text.split_inclusive(' ') {
                    let _ = sender.append(AgentEvent::ItemDelta {
                        thread: thread_id,
                        turn,
                        item_id: reasoning_id,
                        delta: ItemDelta::Text { text: chunk.into() },
                    });
                    tokio::task::yield_now().await;
                }
                let _ = sender.append(AgentEvent::ItemCompleted {
                    thread: thread_id,
                    turn,
                    item: Item {
                        id: reasoning_id,
                        harness_item_id: "scripted_reasoning_1".into(),
                        payload: ItemPayload::Reasoning {
                            text: reasoning_text,
                        },
                        created_at: chrono::Utc::now(),
                    },
                });
                tokio::task::yield_now().await;
            }
            let _ = sender.append(AgentEvent::ItemStarted {
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
                let _ = sender.append(AgentEvent::ItemDelta {
                    thread: thread_id,
                    turn,
                    item_id,
                    delta: ItemDelta::Text { text: word.into() },
                });
                tokio::task::yield_now().await;
            }
            let _ = sender.append(AgentEvent::ItemCompleted {
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
            let _ = sender.append(AgentEvent::TurnCompleted {
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
            return AgentEventStream::new(tx.reader());
        }
        AgentEventStream::closed()
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
            let _ = sender.append(AgentEvent::ItemCompleted {
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
    async fn create(
        &self,
        _config: &ProjectConfig,
        bootstrap: HarnessBootstrap,
    ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        Ok(Arc::new(ScriptedHarness::new(bootstrap)?))
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

    let shutdown = common::shutdown::install_signal_handler();
    match common::shutdown::run_until_forced(run(shutdown.clone()), shutdown).await {
        common::shutdown::RunOutcome::Completed(Ok(())) => {}
        common::shutdown::RunOutcome::Completed(Err(error)) => {
            error!(%error, "replay server stopped with an error");
            eprintln!("giskard-server-replay: {error}");
            std::process::exit(1);
        }
        common::shutdown::RunOutcome::Forced(signal) => {
            error!(
                signal,
                "second shutdown signal received; forcing process exit"
            );
            eprintln!("giskard-server-replay: second {signal} received; forcing process exit");
            std::process::exit(1);
        }
    }
}

async fn run(
    shutdown: tokio::sync::watch::Receiver<common::shutdown::Phase>,
) -> Result<(), String> {
    let data_dir = env_path("GISKARD_DATA_DIR").unwrap_or_else(|| {
        std::env::temp_dir().join(format!("giskard-replay-{}", std::process::id()))
    });
    std::fs::create_dir_all(&data_dir)
        .map_err(|e| format!("cannot create data dir {}: {e}", data_dir.display()))?;

    // Locked for the same reason the real server is, and with more at stake: this binary
    // *overwrites* `config.toml` in whatever directory it is given, so being pointed at a live data
    // directory would clobber a real configuration. The default is a PID-suffixed temp directory,
    // so concurrent replay servers never contend. Held for the process lifetime — dropping the
    // guard releases the lock.
    let _data_dir_lock = match giskard_persist::DataDirLock::try_acquire(&data_dir) {
        Ok(Some(lock)) => lock,
        Ok(None) => {
            return Err(format!(
                "another Giskard process is using the data directory {}. Stop it, or set \
                 GISKARD_DATA_DIR to a directory of its own.",
                data_dir.display()
            ));
        }
        Err(e) => {
            return Err(format!(
                "cannot lock data directory {}: {e}",
                data_dir.display()
            ));
        }
    };

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
    let state = AppState::new_with_config(
        store,
        factory,
        session_key.to_vec(),
        Some(&config.viz),
        Some(&config.retention),
    );
    let registry = state.registry.clone();
    let app_shutdown = state.shutdown.clone();
    let app = build_app(state);

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|e| format!("cannot bind {bind}: {e}"))?;
    info!(bind = %bind, data_dir = %data_dir.display(), "giskard-server-replay listening");
    common::shutdown::serve_then_shutdown_registry(
        listener,
        app,
        app_shutdown,
        shutdown,
        HTTP_GRACEFUL_SHUTDOWN_TIMEOUT,
        "giskard-server-replay",
        &registry,
    )
    .await
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

#[cfg(test)]
mod scripted_harness_tests {
    use super::*;

    #[tokio::test]
    async fn native_claim_adopts_an_existing_binding() {
        let thread = ThreadId::new();
        let harness = ScriptedHarness::new(HarnessBootstrap {
            known_threads: vec![giskard_harness::KnownThreadBinding {
                harness_thread_id: "native-child".into(),
                thread_id: thread,
            }],
        })
        .unwrap();

        let claimed = harness
            .claim_native_thread(
                ThreadId::new(),
                "native-child".into(),
                PathBuf::from("/tmp"),
            )
            .await
            .unwrap();

        assert_eq!(claimed.thread, thread);
    }
}
