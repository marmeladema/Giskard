//! Verified cold-resume provider switching (spec PS1): a thread whose provider was removed from
//! config loads read-only, and selecting a model from a configured provider re-resumes the native
//! thread under it — but only when the harness *confirms* the switch. An unconfirmed switch is
//! rejected with `thread_provider_switch_ignored` and persists nothing.

use std::sync::Arc;

use giskard_core::error::HarnessError;
use giskard_core::ids::{ProjectId, ThreadId, TurnId};
use giskard_core::model::{ModelDescriptor, ModelRef};
use giskard_harness::{
    AgentEventStream, AgentHarness, EventLog, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
};
use giskard_persist::store::{ThreadGitWorkspace, ThreadWorktree};
use giskard_proto::ClientMessage;
use giskard_testenv::{TestServer, factory, fixtures, ws};
use tokio::sync::Mutex;

const DEAD_PROVIDER: &str = "cloudflare-litellm";
const NEW_PROVIDER: &str = "opencodex";
const NEW_PROVIDER_TOML: &str = r#"[providers.opencodex]
model_listing = false
  [[providers.opencodex.models]]
  id = "glm-5.2"
  display_name = "GLM-5.2"
  context_window = 262144
  supports_reasoning_effort = false

[providers.openai]
model_listing = false
  [[providers.openai.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = true
"#;

/// Opens fail for the removed provider; for any other provider the open succeeds and the handle
/// reports an effective model — either an echo of the request, or `report_provider` to simulate
/// Codex ignoring the override (the loaded-thread rejoin behavior the verification must catch).
struct SwitchHarness {
    report_provider: Option<String>,
    opened_workspace_roots: Arc<Mutex<Vec<String>>>,
    events: Arc<EventLog>,
}

#[async_trait::async_trait]
impl AgentHarness for SwitchHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        HarnessCapabilities::default()
    }

    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError> {
        Ok(Vec::new())
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        self.opened_workspace_roots
            .lock()
            .await
            .push(opts.workspace_root.to_string_lossy().into_owned());
        // Echo the requested model as the effective model a real harness reports from its record.
        let mut effective = opts.initial_model.clone();
        if effective.provider == DEAD_PROVIDER {
            return Err(HarnessError::Transport(format!(
                "JSON-RPC error (-32600): failed to load configuration: Model provider \
                 {:?} not found",
                DEAD_PROVIDER
            )));
        }
        if let Some(provider) = &self.report_provider {
            effective.provider = provider.clone();
        }
        let thread = opts.thread;
        Ok(ThreadHandle {
            resumed_model: Some(effective),
            ..ThreadHandle::opened(
                thread,
                opts.resume.unwrap_or_else(|| "fresh".into()),
                opts.workspace_root.clone(),
            )
        })
    }

    async fn start_turn(
        &self,
        _thread: &ThreadHandle,
        _input: giskard_core::user_input::UserInput,
        _overrides: giskard_core::turn::TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        Err(HarnessError::Unsupported("no turns in this test".into()))
    }

    fn subscribe(&self, _thread: &ThreadHandle) -> AgentEventStream {
        AgentEventStream::new(self.events.reader())
    }

    async fn respond_approval(
        &self,
        _req: giskard_core::ids::ApprovalId,
        _decision: giskard_core::approval::ApprovalDecision,
    ) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported(
            "no approvals in this test".into(),
        ))
    }

    async fn respond_server_request(
        &self,
        _req: giskard_core::ids::ServerRequestId,
        _response: giskard_core::server_request::ServerRequestResponse,
    ) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported("no requests in this test".into()))
    }

    async fn interrupt(&self, _thread: &ThreadHandle) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported("no turns in this test".into()))
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

fn dead_model() -> ModelRef {
    ModelRef {
        provider: DEAD_PROVIDER.into(),
        model: "@cf/z-ai/glm-4.7".into(),
        reasoning_effort: None,
    }
}

fn new_model() -> ModelRef {
    ModelRef {
        provider: NEW_PROVIDER.into(),
        model: "glm-5.2".into(),
        reasoning_effort: None,
    }
}

struct Fixture {
    server: TestServer,
    pid: ProjectId,
    tid: ThreadId,
    opened_workspace_roots: Arc<Mutex<Vec<String>>>,
    _proj_dir: tempfile::TempDir,
    _worktree_dir: Option<tempfile::TempDir>,
}

/// Server whose config declares only the *new* providers; the thread is seeded under the dead one.
async fn start_server(report_provider: Option<String>) -> Fixture {
    start_server_inner(report_provider, false).await
}

async fn start_server_with_worktree(report_provider: Option<String>) -> Fixture {
    start_server_inner(report_provider, true).await
}

async fn start_server_inner(report_provider: Option<String>, seed_worktree: bool) -> Fixture {
    let pid = ProjectId::new();
    let tid = ThreadId::new();
    let proj_dir = tempfile::TempDir::new().unwrap();
    let worktree_dir = seed_worktree.then(|| tempfile::TempDir::new().unwrap());
    let git_workspace = worktree_dir.as_ref().map(|worktree_dir| {
        let path = worktree_dir.path().to_string_lossy().into_owned();
        ThreadGitWorkspace::Worktree(ThreadWorktree {
            path,
            workspace: None,
            branch: "giskard/worktree-test".into(),
            base_commit: None,
            repo_root: "/repo".into(),
            common_dir: "/repo/.git".into(),
            git_dir: "/repo/.git/worktrees/test".into(),
        })
    });
    let opened_workspace_roots = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(EventLog::new());
    let roots = opened_workspace_roots.clone();
    let factory_events = events.clone();
    let factory = factory::from_fn(move |_, _| {
        Ok(Arc::new(SwitchHarness {
            report_provider: report_provider.clone(),
            opened_workspace_roots: roots.clone(),
            events: factory_events.clone(),
        }))
    });
    let project_path = proj_dir.path().to_string_lossy().into_owned();
    let server = TestServer::builder(factory)
        .config(NEW_PROVIDER_TOML)
        .seed(move |store| async move {
            store
                .create_project(pid, "proj", &project_path)
                .await
                .unwrap();
            store
                .save_thread(
                    pid,
                    &fixtures::orphaned_thread(pid, tid, dead_model(), git_workspace),
                )
                .await
                .unwrap();
            store
                .append_turn(
                    pid,
                    tid,
                    &fixtures::completed_turn("hello from the old provider", dead_model()),
                )
                .await
                .unwrap();
        })
        .start()
        .await;

    Fixture {
        server,
        pid,
        tid,
        opened_workspace_roots,
        _proj_dir: proj_dir,
        _worktree_dir: worktree_dir,
    }
}

#[tokio::test]
async fn cold_provider_switch_succeeds_and_binds_the_thread() {
    let srv = start_server(None).await;
    let mut ws = srv.server.ws().await;

    // The thread loads read-only under the dead provider.
    ws::send(
        &mut ws,
        &ClientMessage::Subscribe {
            thread_id: srv.tid,
            since: None,
        },
    )
    .await;
    ws::next_matching(&mut ws, |v| v["code"] == "thread_read_only")
        .await
        .expect("read-only warning");

    // Selecting a model from a configured provider triggers the verified cold re-resume…
    ws::send(
        &mut ws,
        &ClientMessage::SelectModel {
            thread_id: srv.tid,
            request_id: "select-model-1".into(),
            model_ref: new_model(),
        },
    )
    .await;

    // …and the broadcast thread state reports the new provider.
    let state_msg = ws::next_matching(&mut ws, |v| {
        v["type"] == "thread_state" && v["current_model"]["provider"] == NEW_PROVIDER
    })
    .await
    .expect("thread state under the new provider");
    assert_eq!(state_msg["current_model"]["model"], "glm-5.2");

    // Persisted and natively bound.
    let tf = srv
        .server
        .state
        .store
        .load_thread(srv.pid, srv.tid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tf.current_model.as_known().unwrap().provider, NEW_PROVIDER);
    let native = srv
        .server
        .state
        .registry
        .loaded_thread_binding(srv.tid)
        .await
        .and_then(|binding| binding.native_model().cloned())
        .expect("thread must be warm after a confirmed switch");
    assert_eq!(native.provider, NEW_PROVIDER);

    // The thread is now provider-bound again: switching to yet another provider is rejected.
    ws::send(
        &mut ws,
        &ClientMessage::SelectModel {
            thread_id: srv.tid,
            request_id: "select-model-2".into(),
            model_ref: ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
        },
    )
    .await;
    let error = ws::next_matching(&mut ws, |v| v["code"] == "thread_provider_locked")
        .await
        .expect("warm thread rejects a second provider change");
    assert_eq!(error["request_id"], "select-model-2");
}

#[tokio::test]
async fn cold_provider_switch_reopens_worktree_thread_in_its_worktree() {
    let srv = start_server_with_worktree(None).await;
    let thread = srv
        .server
        .state
        .store
        .load_thread(srv.pid, srv.tid)
        .await
        .unwrap()
        .unwrap();
    let worktree_root = thread
        .git_workspace
        .as_ref()
        .unwrap()
        .workspace_root()
        .to_string();
    let project_root = srv._proj_dir.path().to_string_lossy().into_owned();
    let mut ws = srv.server.ws().await;

    ws::send(
        &mut ws,
        &ClientMessage::Subscribe {
            thread_id: srv.tid,
            since: None,
        },
    )
    .await;
    ws::next_matching(&mut ws, |v| v["code"] == "thread_read_only")
        .await
        .expect("read-only warning");

    ws::send(
        &mut ws,
        &ClientMessage::SelectModel {
            thread_id: srv.tid,
            request_id: "select-model-3".into(),
            model_ref: new_model(),
        },
    )
    .await;
    ws::next_matching(&mut ws, |v| {
        v["type"] == "thread_state" && v["current_model"]["provider"] == NEW_PROVIDER
    })
    .await
    .expect("thread state under the new provider");

    let opened_roots = srv.opened_workspace_roots.lock().await.clone();
    assert_eq!(
        opened_roots,
        vec![worktree_root.clone(), worktree_root],
        "both the failed dead-provider attach and the confirmed provider-switch reopen must use the worktree"
    );
    assert!(
        !opened_roots.contains(&project_root),
        "provider switch must not reopen a worktree thread in the project checkout"
    );
}

#[tokio::test]
async fn unconfirmed_provider_switch_is_rejected_and_persists_nothing() {
    // The harness claims the *old* provider stayed effective — simulating Codex ignoring the
    // resume overrides. Verification must fail the switch.
    let srv = start_server(Some(DEAD_PROVIDER.into())).await;
    let mut ws = srv.server.ws().await;

    ws::send(
        &mut ws,
        &ClientMessage::Subscribe {
            thread_id: srv.tid,
            since: None,
        },
    )
    .await;
    ws::next_matching(&mut ws, |v| v["code"] == "thread_read_only")
        .await
        .expect("read-only warning");

    ws::send(
        &mut ws,
        &ClientMessage::SelectModel {
            thread_id: srv.tid,
            request_id: "select-model-4".into(),
            model_ref: new_model(),
        },
    )
    .await;
    let err = ws::next_matching(&mut ws, |v| v["code"] == "thread_provider_switch_ignored")
        .await
        .expect("unconfirmed switch must surface a structured error");
    assert_eq!(err["severity"], "error");

    // Nothing persisted; the thread is still cold under the old provider.
    let tf = srv
        .server
        .state
        .store
        .load_thread(srv.pid, srv.tid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tf.current_model.as_known().unwrap().provider, DEAD_PROVIDER);
    assert!(
        srv.server
            .state
            .registry
            .loaded_thread_binding(srv.tid)
            .await
            .and_then(|binding| binding.native_model().cloned())
            .is_none(),
        "failed switch must leave the thread cold"
    );
}
