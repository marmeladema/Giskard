//! Deterministic replay harness for testing (spec §14.2).
//!
//! Reads a recorded fixture (JSONL of `AgentEvent`s) and replays them through the
//! `AgentHarness` trait with deterministic timing. No real LLM, no network.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use tokio::sync::{Mutex, broadcast};

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemPayload};
use giskard_core::mcp::McpServerStatus;
use giskard_core::model::{ModelDescriptor, ModelRef};
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::token::TokenUsage;
use giskard_core::turn::TurnOverrides;
use giskard_core::turn::{TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, HarnessCapabilities, HarnessProvider, OpenThreadOptions,
    ThreadHandle,
};

/// A recorded fixture: an ordered list of `AgentEvent`s to replay.
#[derive(Clone)]
pub struct ReplayFixture {
    pub events: Vec<AgentEvent>,
}

impl ReplayFixture {
    /// Load a fixture from a JSONL file (one `AgentEvent` per line).
    pub fn load(path: &Path) -> Result<Self, String> {
        let data = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let mut events = Vec::new();
        for (i, line) in data.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let event: AgentEvent =
                serde_json::from_str(line).map_err(|e| format!("line {}: {}", i + 1, e))?;
            events.push(event);
        }
        Ok(Self { events })
    }

    /// Create a fixture from a list of events.
    pub fn from_events(events: Vec<AgentEvent>) -> Self {
        Self { events }
    }

    /// Save as JSONL.
    pub fn save(&self, path: &Path) -> Result<(), String> {
        use std::io::Write;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let mut file = std::fs::File::create(path).map_err(|e| e.to_string())?;
        for event in &self.events {
            let json = serde_json::to_string(event).map_err(|e| e.to_string())?;
            writeln!(file, "{json}").map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

struct ThreadState {
    sender: broadcast::Sender<AgentEvent>,
    pending: Vec<AgentEvent>,
}

struct PreloadedFixture {
    thread_id: ThreadId,
    events: Vec<AgentEvent>,
}

/// A harness that replays recorded events deterministically.
pub struct ReplayHarness {
    capabilities: HarnessCapabilities,
    threads: Mutex<Vec<(ThreadId, ThreadState)>>,
    fixtures: Mutex<HashMap<String, PreloadedFixture>>,
    /// Catalog returned by `list_models` (empty unless set via [`ReplayHarness::with_models`]),
    /// standing in for a real harness's model catalog (e.g. Codex `model/list`).
    models: Vec<ModelDescriptor>,
    /// When set, `list_models` fails with this message instead of returning `models` — used to
    /// exercise the server's best-effort degradation when a harness catalog query errors.
    models_error: Option<String>,
    /// Provider table returned by `list_providers` (empty unless set via
    /// [`ReplayHarness::with_providers`]), standing in for a real harness's own provider config.
    providers: Vec<HarnessProvider>,
    /// When set, `list_providers` fails with this message instead of returning `providers`.
    providers_error: Option<String>,
    /// Version reported by `client_version`, standing in for a real harness's own. `None` by
    /// default: a harness that cannot say must not have one invented for it.
    client_version: Option<String>,
    /// The model reported for a thread imported by native id, where the caller names none and a
    /// real harness answers from the thread's own record. Defaults to `openai/gpt-5.5`; a test
    /// whose config does not offer that model sets its own with
    /// [`ReplayHarness::with_imported_model`].
    imported_model: ModelRef,
    shutdown_called: AtomicBool,
}

impl ReplayHarness {
    /// Create a new replay harness with full Codex-like capabilities.
    pub fn new() -> Self {
        Self::with_fixtures(HashMap::new())
    }

    fn with_fixtures(fixtures: HashMap<String, PreloadedFixture>) -> Self {
        Self {
            capabilities: HarnessCapabilities {
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
            },
            threads: Mutex::new(Vec::new()),
            fixtures: Mutex::new(fixtures),
            models: Vec::new(),
            models_error: None,
            providers: Vec::new(),
            providers_error: None,
            client_version: None,
            imported_model: ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
            shutdown_called: AtomicBool::new(false),
        }
    }

    /// Advertise a model catalog: sets the list returned by `list_models` and turns on the
    /// `model_listing` capability, so the server's per-project model overlay runs against it.
    pub fn with_models(mut self, models: Vec<ModelDescriptor>) -> Self {
        self.capabilities.model_listing = true;
        self.models = models;
        self
    }

    /// Advertise `model_listing` but make `list_models` fail, to exercise the server's best-effort
    /// degradation (the picker should still get the config + discovery list).
    pub fn with_failing_models(mut self, message: impl Into<String>) -> Self {
        self.capabilities.model_listing = true;
        self.models_error = Some(message.into());
        self
    }

    /// Report a harness version, as Codex does from its initialize handshake. Discovery sends it
    /// to a provider's `/models` endpoint as `client_version` (§8.3).
    pub fn with_client_version(mut self, version: impl Into<String>) -> Self {
        self.client_version = Some(version.into());
        self
    }

    /// Advertise a provider table: sets what `list_providers` returns and turns on the
    /// `provider_listing` capability, so the server resolves discovery endpoints and validates
    /// configured provider ids against it.
    pub fn with_providers(mut self, providers: Vec<HarnessProvider>) -> Self {
        self.capabilities.provider_listing = true;
        self.providers = providers;
        self
    }

    /// Advertise `provider_listing` but make `list_providers` fail, to exercise the server's
    /// best-effort degradation (the picker should still get the static config list).
    pub fn with_failing_providers(mut self, message: impl Into<String>) -> Self {
        self.capabilities.provider_listing = true;
        self.providers_error = Some(message.into());
        self
    }

    /// Set the model this harness reports for a thread imported by native id — the stand-in for
    /// the model that thread was already on. Tests whose configured providers do not include the
    /// default `openai/gpt-5.5` need this, or the imported thread lands on a provider their config
    /// never declared.
    pub fn with_imported_model(mut self, model: ModelRef) -> Self {
        self.imported_model = model;
        self
    }

    /// Load a fixture and create a harness pre-loaded with those events
    /// for a single thread.
    pub fn from_fixture(fixture: ReplayFixture) -> Self {
        let (thread_id, harness_thread_id) = fixture
            .events
            .iter()
            .find_map(|e| match e {
                AgentEvent::ThreadOpened {
                    thread,
                    harness_thread_id,
                } => Some((*thread, harness_thread_id.clone())),
                _ => None,
            })
            .unwrap_or_else(|| (ThreadId::new(), format!("replay_{}", ThreadId::new())));

        let mut fixtures = HashMap::new();
        fixtures.insert(
            harness_thread_id,
            PreloadedFixture {
                thread_id,
                events: fixture.events,
            },
        );
        Self::with_fixtures(fixtures)
    }
}

impl Default for ReplayHarness {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AgentHarness for ReplayHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        self.capabilities
    }

    fn client_version(&self) -> Option<String> {
        self.client_version.clone()
    }

    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError> {
        match &self.models_error {
            Some(message) => Err(HarnessError::Transport(message.clone())),
            None => Ok(self.models.clone()),
        }
    }

    async fn list_providers(&self) -> Result<Vec<HarnessProvider>, HarnessError> {
        match &self.providers_error {
            Some(message) => Err(HarnessError::Transport(message.clone())),
            None => Ok(self.providers.clone()),
        }
    }

    async fn list_mcp_servers(&self) -> Result<Vec<McpServerStatus>, HarnessError> {
        Ok(vec![])
    }

    async fn reload_mcp_servers(&self) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        let harness_thread_id = opts
            .resume
            .clone()
            .unwrap_or_else(|| format!("replay_{}", ThreadId::new()));

        let (thread_id, mut pending) = if let Some(resume) = &opts.resume {
            let mut fixtures = self.fixtures.lock().await;
            if let Some(fixture) = fixtures.remove(resume) {
                (opts.thread.unwrap_or(fixture.thread_id), fixture.events)
            } else {
                (opts.thread.unwrap_or_default(), Vec::new())
            }
        } else {
            (opts.thread.unwrap_or_default(), Vec::new())
        };
        for event in &mut pending {
            remap_event_thread(event, thread_id);
        }

        let (tx, _) = broadcast::channel(256);
        let mut threads = self.threads.lock().await;
        threads.push((
            thread_id,
            ThreadState {
                sender: tx,
                pending,
            },
        ));

        Ok(ThreadHandle {
            thread: thread_id,
            harness_thread_id,
            warning: None,
            // A deterministic replay applies exactly the requested model, so echo it as
            // effective — this is what lets server tests exercise verified provider switches.
            // An import names no model, and a harness answers that from the thread itself, so
            // the fake stands in with its configured one rather than reporting nothing.
            resumed_model: Some(
                opts.initial_model
                    .clone()
                    .unwrap_or_else(|| self.imported_model.clone()),
            ),
            agent_name: None,
            parent_harness_thread_id: None,
        })
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        _input: UserInput,
        _overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let turn_id = TurnId::new();

        // Emit all pending events for this thread into the broadcast channel.
        let mut threads = self.threads.lock().await;
        if let Some((_, state)) = threads.iter_mut().find(|(id, _)| *id == thread.thread) {
            let sender = state.sender.clone();
            let events = std::mem::take(&mut state.pending);
            drop(threads);

            // Send events asynchronously.
            tokio::spawn(async move {
                for event in events {
                    let _ = sender.send(event);
                    tokio::task::yield_now().await;
                }
            });
        }

        Ok(turn_id)
    }

    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream {
        // We need to get the sender synchronously. Use try_lock.
        let threads = self.threads.try_lock();
        if let Ok(threads) = threads
            && let Some((_, state)) = threads.iter().find(|(id, _)| *id == thread.thread)
        {
            return AgentEventStream::new(state.sender.subscribe());
        }
        // Fallback: create a dummy channel.
        let (_, rx) = broadcast::channel(1);
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
        let mut threads = self.threads.lock().await;
        let Some((_, state)) = threads.iter_mut().find(|(id, _)| *id == thread.thread) else {
            return Err(HarnessError::ThreadNotFound(thread.thread));
        };
        let sender = state.sender.clone();
        let thread_id = thread.thread;
        drop(threads);

        tokio::spawn(async move {
            let turn = TurnId::new();
            let item_id = ItemId::new();
            let _ = sender.send(AgentEvent::TurnStarted {
                thread: thread_id,
                turn,
            });
            tokio::task::yield_now().await;
            let _ = sender.send(AgentEvent::ItemCompleted {
                thread: thread_id,
                turn,
                item: Item {
                    id: item_id,
                    harness_item_id: "replay_context_compaction".into(),
                    payload: ItemPayload::Activity {
                        title: "Context compacted".into(),
                        detail: None,
                        metadata: None,
                        subagent: None,
                    },
                    created_at: chrono::Utc::now(),
                },
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
        Ok(())
    }

    async fn set_thread_name(
        &self,
        _thread: &ThreadHandle,
        _name: &str,
    ) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn set_thread_archived(
        &self,
        _thread: &ThreadHandle,
        _archived: bool,
    ) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn delete_thread(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        if self.shutdown_called.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        Ok(())
    }
}

fn remap_event_thread(event: &mut AgentEvent, thread_id: ThreadId) {
    match event {
        AgentEvent::ThreadOpened { thread, .. }
        | AgentEvent::TurnStarted { thread, .. }
        | AgentEvent::ContextWindowUpdated { thread, .. }
        | AgentEvent::ItemStarted { thread, .. }
        | AgentEvent::ItemDelta { thread, .. }
        | AgentEvent::ItemCompleted { thread, .. }
        | AgentEvent::DiffUpdated { thread, .. }
        | AgentEvent::ApprovalRequested { thread, .. }
        | AgentEvent::ServerRequestReceived { thread, .. }
        | AgentEvent::ServerRequestResolved { thread, .. }
        | AgentEvent::TurnCompleted { thread, .. }
        | AgentEvent::Error { thread, .. }
        | AgentEvent::Notice { thread, .. } => *thread = thread_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use giskard_core::ids::{ItemId, ThreadId, TurnId};
    use giskard_core::item::{Item, ItemDelta, ItemKind, ItemPayload, ItemStart};
    use giskard_core::model::ModelRef;
    use giskard_core::token::TokenUsage;
    use giskard_core::turn::{Mode, TurnStatus, TurnStatusKind};
    use std::sync::Arc;

    fn make_simple_fixture() -> ReplayFixture {
        let thread = ThreadId::new();
        let turn = TurnId::new();
        let item_id = ItemId::new();
        let now = Utc::now();

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
                    id: item_id,
                    harness_item_id: "it_1".into(),
                    kind: ItemKind::AgentMessage,
                    command: None,
                    tool: None,
                },
            },
            AgentEvent::ItemDelta {
                thread,
                turn,
                item_id,
                delta: ItemDelta::Text {
                    text: "Hello!".into(),
                },
            },
            AgentEvent::ItemCompleted {
                thread,
                turn,
                item: Item {
                    id: item_id,
                    harness_item_id: "it_1".into(),
                    payload: ItemPayload::AgentMessage {
                        text: "Hello!".into(),
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

    #[tokio::test]
    async fn replay_basic_turn() {
        let fixture = make_simple_fixture();
        let _thread_id = fixture
            .events
            .iter()
            .find_map(|e| match e {
                AgentEvent::ThreadOpened { thread, .. } => Some(*thread),
                _ => None,
            })
            .unwrap();

        let harness = Arc::new(ReplayHarness::from_fixture(fixture));

        let handle = harness
            .open_thread(giskard_harness::OpenThreadOptions {
                project: giskard_core::ProjectId::new(),
                thread: None,
                workspace_root: "/tmp".into(),
                resume: Some("th_test".into()),
                resume_policy: giskard_harness::ResumePolicy::AllowFreshFallback,
                initial_model: Some(ModelRef {
                    provider: "openai".into(),
                    model: "gpt-5.5".into(),
                    reasoning_effort: None,
                }),
            })
            .await
            .unwrap();

        // Subscribe before starting the turn.
        let mut stream = harness.subscribe(&handle);

        let _turn_id = harness
            .start_turn(
                &handle,
                UserInput::text("test"),
                TurnOverrides {
                    model: None,
                    mode: Mode::Build,
                    permission_preset: giskard_core::turn::PermissionPreset::AutoApprove,
                },
            )
            .await
            .unwrap();

        // Collect events.
        let mut events = Vec::new();
        while let Ok(event) = stream.recv().await {
            let is_completed = matches!(event, AgentEvent::TurnCompleted { .. });
            events.push(event);
            if is_completed {
                break;
            }
        }

        assert_eq!(events.len(), 6);
        assert!(matches!(events[0], AgentEvent::ThreadOpened { .. }));
        assert!(matches!(
            events[5],
            AgentEvent::TurnCompleted { ref status, .. } if status.kind == TurnStatusKind::Completed
        ));

        // Verify token usage.
        if let AgentEvent::TurnCompleted { usage, .. } = &events[5] {
            assert_eq!(usage.input, 100);
            assert_eq!(usage.output, 50);
            assert_eq!(usage.total, 150);
        }
    }

    #[tokio::test]
    async fn replay_resume_remaps_fixture_events_to_requested_thread() {
        let fixture = make_simple_fixture();
        let requested_thread = ThreadId::new();
        let harness = Arc::new(ReplayHarness::from_fixture(fixture));

        let handle = harness
            .open_thread(giskard_harness::OpenThreadOptions {
                project: giskard_core::ProjectId::new(),
                thread: Some(requested_thread),
                workspace_root: "/tmp".into(),
                resume: Some("th_test".into()),
                resume_policy: giskard_harness::ResumePolicy::AllowFreshFallback,
                initial_model: Some(ModelRef {
                    provider: "openai".into(),
                    model: "gpt-5.5".into(),
                    reasoning_effort: None,
                }),
            })
            .await
            .unwrap();
        assert_eq!(handle.thread, requested_thread);

        let mut stream = harness.subscribe(&handle);
        harness
            .start_turn(
                &handle,
                UserInput::text("test"),
                TurnOverrides {
                    model: None,
                    mode: Mode::Build,
                    permission_preset: giskard_core::turn::PermissionPreset::AutoApprove,
                },
            )
            .await
            .unwrap();

        let mut events = Vec::new();
        while let Ok(event) = stream.recv().await {
            let is_completed = matches!(event, AgentEvent::TurnCompleted { .. });
            events.push(event);
            if is_completed {
                break;
            }
        }

        assert_eq!(events.len(), 6);
        for event in events {
            assert_eq!(event_thread(&event), requested_thread);
        }
    }

    #[tokio::test]
    async fn replay_shutdown_idempotent() {
        let harness = ReplayHarness::new();
        harness.shutdown().await.unwrap();
        harness.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn replay_capabilities() {
        let harness = ReplayHarness::new();
        let caps = harness.capabilities();
        assert!(caps.live_approvals);
        assert!(caps.plan_build_modes);
        assert!(caps.token_usage);
    }

    #[tokio::test]
    async fn fixture_save_load_roundtrip() {
        let fixture = make_simple_fixture();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        fixture.save(tmp.path()).unwrap();
        let loaded = ReplayFixture::load(tmp.path()).unwrap();
        assert_eq!(loaded.events.len(), fixture.events.len());
    }

    fn event_thread(event: &AgentEvent) -> ThreadId {
        match event {
            AgentEvent::ThreadOpened { thread, .. }
            | AgentEvent::TurnStarted { thread, .. }
            | AgentEvent::ContextWindowUpdated { thread, .. }
            | AgentEvent::ItemStarted { thread, .. }
            | AgentEvent::ItemDelta { thread, .. }
            | AgentEvent::ItemCompleted { thread, .. }
            | AgentEvent::DiffUpdated { thread, .. }
            | AgentEvent::ApprovalRequested { thread, .. }
            | AgentEvent::ServerRequestReceived { thread, .. }
            | AgentEvent::ServerRequestResolved { thread, .. }
            | AgentEvent::TurnCompleted { thread, .. }
            | AgentEvent::Error { thread, .. }
            | AgentEvent::Notice { thread, .. } => *thread,
        }
    }
}
