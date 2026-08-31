//! Starting a thread in its own Git worktree, end to end over HTTP.
//!
//! The unit of interest is where the harness is opened: a thread created with the `worktree` Git
//! strategy must be opened against the worktree rather than the project's checkout, and must still
//! be after a restart. Everything here therefore records the workspace root the harness was handed.

mod common;
#[path = "common/event_channel.rs"]
mod event_channel;

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ItemId, ProjectId, ServerRequestId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemPayload, SubagentAction, SubagentLink};
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::token::TokenUsage;
use giskard_core::turn::{TurnOverrides, TurnStatus, TurnStatusKind};
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, ClaimedNativeRoute, HarnessCapabilities, HarnessSignalStream,
    OpenThreadOptions, ThreadHandle,
};
use giskard_persist::store::{ProjectConfig, ThreadGitWorkspace};
use giskard_server::{AppState, HarnessFactory, build_app};
use tokio::sync::Mutex;

use common::fake_native_model;
use event_channel::{BoundedEventRoute, TestRouteContract, closed_event_stream};

/// Saving a plan writes a file into the workspace and then offers it back as a link. For an
/// isolated thread that has to be the worktree: writing to the project's checkout would put the
/// plan outside the tree the agent works in, and the link offered afterwards would read a file that
/// is not there.
#[tokio::test]
async fn a_plan_saved_from_an_isolated_thread_lands_in_its_worktree() {
    let server = start(/*git_repo*/ true).await;
    server
        .harness
        .agent_says
        .lock()
        .await
        .replace("## Step one\n\nRefactor the auth module.".to_string());
    let (_, body) = server
        .start_thread_in_mode("Plan the work", true, "plan")
        .await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree = server
        .state
        .store
        .load_thread(server.project_id, thread_id)
        .await
        .unwrap()
        .unwrap()
        .git_workspace
        .and_then(|ws| ws.as_worktree().cloned())
        .unwrap();
    server.wait_until_idle(thread_id).await;

    let port = server.base.rsplit(':').next().unwrap().to_string();
    let request = tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(format!("ws://127.0.0.1:{port}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", server.cookie.clone())
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(request)
        .await
        .expect("connect websocket");
    use futures_util::SinkExt;
    for message in [
        giskard_proto::ClientMessage::Subscribe {
            thread_id,
            since: None,
        },
        giskard_proto::ClientMessage::SavePlan {
            thread_id,
            path: "docs/plan.md".into(),
        },
    ] {
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&message).unwrap().into(),
        ))
        .await
        .unwrap();
    }

    let in_worktree = Path::new(&worktree.path).join("docs/plan.md");
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while !in_worktree.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the plan was never written into the thread's worktree"
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
    assert!(
        std::fs::read_to_string(&in_worktree)
            .unwrap()
            .contains("Refactor the auth module"),
        "the worktree's copy must hold the plan"
    );
    assert!(
        !server._project.path().join("docs/plan.md").exists(),
        "nothing may be written into the project's checkout"
    );
}

/// The file endpoints already name their thread; isolation is what makes that answer differ. A
/// worktree holds the same paths as the project's checkout with different contents in them, so a
/// read resolved against the wrong one succeeds and returns the wrong file — the `200` that looks
/// like it worked.
#[tokio::test]
async fn file_reads_come_from_the_threads_own_worktree() {
    let server = start(/*git_repo*/ true).await;
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree = server
        .state
        .store
        .load_thread(server.project_id, thread_id)
        .await
        .unwrap()
        .unwrap()
        .git_workspace
        .and_then(|ws| ws.as_worktree().cloned())
        .unwrap();

    // The same path in both trees, holding different text.
    std::fs::write(
        Path::new(&worktree.path).join("shared.rs"),
        "fn agent() {}\n",
    )
    .unwrap();
    std::fs::write(server._project.path().join("shared.rs"), "fn user() {}\n").unwrap();
    // And a file that exists only in the worktree, which must be readable rather than a 404.
    std::fs::write(
        Path::new(&worktree.path).join("agent-only.rs"),
        "fn only_here() {}\n",
    )
    .unwrap();

    let shared = server.file_response("raw", thread_id, "shared.rs").await;
    assert!(shared.status().is_success());
    assert_eq!(
        shared.text().await.unwrap(),
        "fn agent() {}\n",
        "the thread's own copy, not the project's"
    );

    let only = server
        .file_response("raw", thread_id, "agent-only.rs")
        .await;
    assert!(only.status().is_success());
    assert_eq!(only.text().await.unwrap(), "fn only_here() {}\n");

    // A path that exists only in the worktree is also linkifiable, which it cannot be when
    // existence is checked against the project.
    let links: serde_json::Value = server
        .client
        .post(format!(
            "{}/api/projects/{}/threads/{thread_id}/linkify",
            server.base, server.project_id
        ))
        .header("cookie", &server.cookie)
        .json(&serde_json::json!({ "text": "see agent-only.rs for the fix" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let paths: Vec<String> = links["links"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|l| l["path"].as_str().map(str::to_string))
        .collect();
    assert!(
        paths.contains(&"agent-only.rs".to_string()),
        "a file that exists only in the worktree must still linkify: {paths:?}"
    );
}

/// The point of an enum over a flag: a strategy this server does not implement is refused, not
/// quietly downgraded to the shared checkout. A thread started in the project's own tree when the
/// client asked for an isolated one looks like it worked, which is the failure mode worth paying an
/// error for. Omitting the field entirely is the other half — that is an ordinary thread, unchanged.
#[tokio::test]
async fn an_unknown_git_strategy_is_refused_and_an_absent_one_is_an_ordinary_thread() {
    let server = start(/*git_repo*/ true).await;

    let refused = server
        .start_thread_with_git_strategy("Isolate this work", serde_json::json!("git_isolate"))
        .await;
    assert!(
        refused.0.is_client_error(),
        "an unimplemented strategy must be refused, got {}: {}",
        refused.0,
        refused.1
    );

    let (status, body) = server
        .start_thread_with_git_strategy("Just work here", serde_json::Value::Null)
        .await;
    assert!(
        status.is_success(),
        "an absent strategy is the default: {body}"
    );
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    assert!(
        server
            .state
            .store
            .load_thread(server.project_id, thread_id)
            .await
            .unwrap()
            .unwrap()
            .git_workspace
            .and_then(|ws| ws.as_worktree().cloned())
            .is_none(),
        "no worktree is created for a thread that did not ask for one"
    );
}

/// Records the workspace root every thread is opened against, which is the whole point of the
/// feature: the cwd the agent works in.
#[derive(Default)]
struct RecordingHarness {
    route_contract: TestRouteContract,
    opened_workspace_roots: Mutex<Vec<String>>,
    /// A `std` mutex, not a `tokio` one: receiver claim is a synchronous trait method, and with an
    /// async mutex it could only `try_lock` — handing back a stream over a dropped sender whenever
    /// the map happened to be held. Every event for that thread would then vanish and the test would
    /// fail on an unexplained timeout. Nothing holds this guard across an await.
    threads: std::sync::Mutex<Vec<(ThreadId, BoundedEventRoute)>>,
    /// When set, the next turn reports having spawned a sub-agent with this native id, which is what
    /// drives the registry to materialize a linked thread the way Codex does.
    spawns_subagent: Mutex<Option<String>>,
    /// When set, every turn emits this as agent text. Plan extraction reads the persisted history,
    /// so a turn that streams nothing leaves nothing to save.
    agent_says: Mutex<Option<String>>,
}

#[async_trait::async_trait]
impl AgentHarness for RecordingHarness {
    fn capabilities(&self) -> HarnessCapabilities {
        HarnessCapabilities {
            resumable_threads: true,
            ..Default::default()
        }
    }

    async fn list_models(&self) -> Result<Vec<giskard_core::ModelDescriptor>, HarnessError> {
        Ok(Vec::new())
    }

    fn take_harness_signals(&self) -> Result<HarnessSignalStream, HarnessError> {
        self.route_contract.take_harness_signals()
    }

    async fn claim_native_route(
        &self,
        harness_thread_id: String,
        suggested_thread_id: ThreadId,
    ) -> Result<ClaimedNativeRoute, HarnessError> {
        self.route_contract
            .claim_native_route(harness_thread_id, suggested_thread_id)
    }

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        self.opened_workspace_roots
            .lock()
            .await
            .push(opts.workspace_root.to_string_lossy().into_owned());
        let thread = opts.thread.unwrap_or_default();
        self.threads
            .lock()
            .expect("recording harness thread-route lock poisoned")
            .push((thread, BoundedEventRoute::new(16)));
        let resumed_model = opts
            .initial_model
            .clone()
            .or_else(|| Some(fake_native_model()));
        let route = self
            .route_contract
            .activate_primary(
                opts.resume.unwrap_or_else(|| format!("native-{thread}")),
                thread,
                opts.identity_generation,
                resumed_model.clone(),
            )
            .await?;
        Ok(ThreadHandle {
            resumed_model,
            ..ThreadHandle::opened(
                route.thread_id,
                route.harness_thread_id,
                opts.workspace_root.clone(),
            )
        })
    }

    /// Binding a provider-owned child's identity: no resume, no native work, but the workspace it
    /// is bound against is still recorded — that is what these tests assert about inheritance.
    async fn claim_native_thread(
        &self,
        thread: ThreadId,
        harness_thread_id: String,
        workspace_root: std::path::PathBuf,
    ) -> Result<ThreadHandle, HarnessError> {
        self.opened_workspace_roots
            .lock()
            .await
            .push(workspace_root.to_string_lossy().into_owned());
        self.threads
            .lock()
            .expect("recording harness thread-route lock poisoned")
            .push((thread, BoundedEventRoute::new(16)));
        Ok(ThreadHandle::opened(
            thread,
            harness_thread_id,
            workspace_root,
        ))
    }

    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        _input: UserInput,
        _overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        let turn = TurnId::new();
        // Complete the turn immediately. A turn that never ends holds the thread's turn gate, and
        // every lifecycle operation these tests drive — delete above all — refuses a live thread.
        // Cloned out and the guard dropped before the awaits below: a `std` mutex must not be held
        // across one, and a `Sender` clone is all this needs.
        let sender = self
            .threads
            .lock()
            .expect("recording harness thread-route lock poisoned")
            .iter()
            .find(|(id, _)| *id == thread.thread)
            .map(|(_, events)| events.clone());
        if let Some(tx) = sender {
            let _ = tx
                .send(AgentEvent::TurnStarted {
                    thread: thread.thread,
                    turn,
                })
                .await;
            if let Some(text) = self.agent_says.lock().await.clone() {
                let _ = tx
                    .send(AgentEvent::ItemCompleted {
                        thread: thread.thread,
                        turn,
                        item: Item {
                            id: ItemId::new(),
                            harness_item_id: format!("say_{turn}"),
                            payload: ItemPayload::AgentMessage { text },
                            created_at: chrono::Utc::now(),
                        },
                    })
                    .await;
            }
            if let Some(child) = self.spawns_subagent.lock().await.take() {
                let _ = tx
                    .send(AgentEvent::ItemCompleted {
                        thread: thread.thread,
                        turn,
                        item: Item {
                            id: ItemId::new(),
                            harness_item_id: format!("spawn_{turn}"),
                            payload: ItemPayload::ToolCall {
                                name: "spawn_subagent".into(),
                                input: serde_json::json!({}),
                                output: None,
                                server: None,
                                status: Some("completed".into()),
                                metadata: None,
                                subagent: Some(SubagentLink {
                                    harness_thread_id: child,
                                    path: Some("reviewer".into()),
                                    initial_prompt: Some("review the change".into()),
                                    action: SubagentAction::Spawned,
                                    // Pending, not completed: a child that is still running is the one
                                    // the monitor attaches to, which is the route being covered.
                                    status: Some(giskard_core::item::SubagentStatus::Pending),
                                    message: None,
                                }),
                                error: None,
                            },
                            created_at: chrono::Utc::now(),
                        },
                    })
                    .await;
            }
            let _ = tx
                .send(AgentEvent::TurnCompleted {
                    thread: thread.thread,
                    turn,
                    usage: TokenUsage::new(0, 0),
                    status: TurnStatus {
                        kind: TurnStatusKind::Completed,
                        message: None,
                    },
                })
                .await;
        }
        Ok(turn)
    }

    fn claim_event_receiver(
        &self,
        route: &ClaimedNativeRoute,
    ) -> Result<AgentEventStream, HarnessError> {
        if let Some(stream) = self
            .threads
            .lock()
            .expect("recording harness thread-route lock poisoned")
            .iter()
            .find(|(id, _)| *id == route.thread_id)
            .and_then(|(_, events)| events.take_stream())
        {
            return Ok(stream);
        }
        Ok(closed_event_stream())
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
        self.threads
            .lock()
            .expect("recording harness thread-route lock poisoned")
            .retain(|(id, _)| *id != thread.thread);
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

struct RecordingFactory(Arc<RecordingHarness>);

#[async_trait::async_trait]
impl HarnessFactory for RecordingFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
        _bootstrap: giskard_harness::HarnessBootstrap,
    ) -> Result<Arc<dyn AgentHarness>, HarnessError> {
        Ok(self.0.clone())
    }
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

fn git(dir: &Path, args: &[&str]) {
    let output = Command::new("git")
        .current_dir(dir)
        .args(args)
        .env("LC_ALL", "C")
        .env("GIT_AUTHOR_NAME", "Test")
        .env("GIT_AUTHOR_EMAIL", "test@example.invalid")
        .env("GIT_COMMITTER_NAME", "Test")
        .env("GIT_COMMITTER_EMAIL", "test@example.invalid")
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

struct Harnessed {
    _data: tempfile::TempDir,
    _project: tempfile::TempDir,
    state: AppState,
    harness: Arc<RecordingHarness>,
    project_id: ProjectId,
    base: String,
    cookie: String,
    client: reqwest::Client,
}

/// A server whose single project is a real Git repository with one commit.
async fn start(git_repo: bool) -> Harnessed {
    let data = tempfile::TempDir::new().unwrap();
    let hash = generate_password_hash("testpass");
    // Bound before the config is written so the port is one the OS just handed out. A fixed port
    // collides with whatever else `cargo test` is running in parallel, which shows up as an
    // unrelated test failing on `AddrInUse`.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::fs::write(
        data.path().join("config.toml"),
        format!(
            "[server]\nbind = \"127.0.0.1:{port}\"\nsecure_cookies = false\n\n[auth]\npassword_hash = \"{hash}\"\nsession_days = 30\n"
        ),
    )
    .await
    .unwrap();

    let project = tempfile::TempDir::new().unwrap();
    if git_repo {
        git(project.path(), &["init", "-q", "-b", "main"]);
        std::fs::write(project.path().join("README.md"), "# project\n").unwrap();
        git(project.path(), &["add", "README.md"]);
        git(project.path(), &["commit", "-qm", "initial"]);
    }

    let store = Arc::new(giskard_persist::PersistStore::new(
        data.path().to_path_buf(),
    ));
    let harness = Arc::new(RecordingHarness::default());
    let state = AppState::new(
        store,
        Arc::new(RecordingFactory(harness.clone())),
        (0..32u8).collect(),
    );
    let app = build_app(state.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let project_id = ProjectId::new();
    state
        .store
        .create_project(
            project_id,
            "worktree-test",
            &project.path().to_string_lossy(),
        )
        .await
        .unwrap();

    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let login = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({ "password": "testpass" }))
        .send()
        .await
        .unwrap();
    let cookie = login
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    Harnessed {
        _data: data,
        _project: project,
        state,
        harness,
        project_id,
        base,
        cookie,
        client,
    }
}

impl Harnessed {
    /// Send whatever the caller wants as `git_strategy`, including a value the server should
    /// reject. `Null` omits the field, which is how an ordinary client starts an ordinary thread.
    async fn start_thread_with_git_strategy(
        &self,
        text: &str,
        git_strategy: serde_json::Value,
    ) -> (reqwest::StatusCode, String) {
        let mut body = serde_json::json!({
            "text": text,
            "model_ref": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
            "mode": "build",
            "permission_preset": "ask_first",
        });
        if !git_strategy.is_null() {
            body["git_strategy"] = git_strategy;
        }
        let response = self
            .client
            .post(format!(
                "{}/api/projects/{}/threads/start",
                self.base, self.project_id
            ))
            .header("cookie", &self.cookie)
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        (status, response.text().await.unwrap_or_default())
    }

    async fn start_thread_in_mode(
        &self,
        text: &str,
        worktree: bool,
        mode: &str,
    ) -> (reqwest::StatusCode, String) {
        let response = self
            .client
            .post(format!(
                "{}/api/projects/{}/threads/start",
                self.base, self.project_id
            ))
            .header("cookie", &self.cookie)
            .json(&serde_json::json!({
                "text": text,
                "model_ref": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
                "mode": mode,
                "permission_preset": "ask_first",
                "git_strategy": if worktree { "worktree" } else { "shared" },
            }))
            .send()
            .await
            .unwrap();
        let status = response.status();
        (status, response.text().await.unwrap_or_default())
    }

    async fn start_thread(&self, text: &str, worktree: bool) -> (reqwest::StatusCode, String) {
        self.start_thread_in_mode(text, worktree, "build").await
    }
    async fn file_response(
        &self,
        kind: &str,
        thread_id: ThreadId,
        path: &str,
    ) -> reqwest::Response {
        self.client
            .get(format!(
                "{}/api/projects/{}/threads/{thread_id}/{kind}?path={path}",
                self.base, self.project_id
            ))
            .header("cookie", &self.cookie)
            .send()
            .await
            .unwrap()
    }

    async fn workspace_roots(&self) -> Vec<String> {
        self.harness.opened_workspace_roots.lock().await.clone()
    }

    async fn thread_summaries(&self) -> serde_json::Value {
        self.client
            .get(format!(
                "{}/api/projects/{}/threads",
                self.base, self.project_id
            ))
            .header("cookie", &self.cookie)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    /// Persist a sub-agent under `parent`, the way the registry materializes one from linked
    /// activity: same project and model, no worktree record of its own.
    async fn persist_subagent(&self, parent: ThreadId) -> ThreadId {
        let mut file = self
            .state
            .store
            .load_thread(self.project_id, parent)
            .await
            .unwrap()
            .unwrap();
        let id = ThreadId::new();
        file.id = id;
        file.title = format!("Sub-agent of {parent}");
        file.harness_thread_id = format!("scripted_{id}");
        file.parent_thread_id = Some(parent);
        file.kind = giskard_core::thread::ThreadKind::Subagent;
        file.git_workspace = None;
        self.state
            .store
            .save_thread(self.project_id, &file)
            .await
            .unwrap();
        id
    }

    /// Wait for the turn started with the thread to finish, since every lifecycle operation
    /// refuses a live thread.
    async fn wait_until_idle(&self, thread_id: ThreadId) {
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        while self.state.registry.thread_has_active_turn(thread_id).await {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the thread's first turn never completed"
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
    }

    /// Rewrite the persisted worktree record, which is how these tests put Git in a state it cannot
    /// answer for without breaking the whole fixture.
    async fn repoint_worktree(&self, thread_id: ThreadId, path: &str, repo_root: &str) {
        let mut file = self
            .state
            .store
            .load_thread(self.project_id, thread_id)
            .await
            .unwrap()
            .unwrap();
        let mut worktree = file
            .git_workspace
            .as_ref()
            .and_then(|ws| ws.as_worktree().cloned())
            .expect("the thread records a worktree");
        worktree.path = path.to_string();
        worktree.repo_root = repo_root.to_string();
        file.git_workspace = Some(ThreadGitWorkspace::Worktree(worktree));
        self.state
            .store
            .save_thread(self.project_id, &file)
            .await
            .unwrap();
    }

    async fn worktree_of(&self, thread_id: ThreadId) -> giskard_persist::store::ThreadWorktree {
        self.state
            .store
            .load_thread(self.project_id, thread_id)
            .await
            .unwrap()
            .unwrap()
            .git_workspace
            .and_then(|ws| ws.as_worktree().cloned())
            .expect("the thread records its worktree")
    }

    async fn delete_thread(&self, thread_id: ThreadId, force: bool) -> reqwest::Response {
        let query = if force { "?force=true" } else { "" };
        self.client
            .delete(format!(
                "{}/api/projects/{}/threads/{thread_id}{query}",
                self.base, self.project_id
            ))
            .header("cookie", &self.cookie)
            .send()
            .await
            .unwrap()
    }

    async fn git_status_response(&self, thread_id: ThreadId) -> reqwest::Response {
        self.client
            .get(format!(
                "{}/api/projects/{}/git/status?thread_id={thread_id}",
                self.base, self.project_id
            ))
            .header("cookie", &self.cookie)
            .send()
            .await
            .unwrap()
    }

    async fn git_status(&self, thread_id: Option<ThreadId>) -> serde_json::Value {
        let scope = thread_id
            .map(|id| format!("?thread_id={id}"))
            .unwrap_or_default();
        self.client
            .get(format!(
                "{}/api/projects/{}/git/status{scope}",
                self.base, self.project_id
            ))
            .header("cookie", &self.cookie)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }
}

/// The sub-agent the registry materialized under `parent`, once it lands on disk.
async fn wait_for_subagent(server: &Harnessed, parent: ThreadId) -> ThreadId {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    loop {
        for thread_id in server
            .state
            .store
            .list_threads(server.project_id)
            .await
            .unwrap()
        {
            let Some(thread) = server
                .state
                .store
                .load_thread(server.project_id, thread_id)
                .await
                .unwrap()
            else {
                continue;
            };
            if thread.parent_thread_id == Some(parent) {
                return thread_id;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the sub-agent was never materialized"
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
}

fn branch_exists(repo: &Path, branch: &str) -> bool {
    !String::from_utf8_lossy(
        &Command::new("git")
            .current_dir(repo)
            .args(["branch", "--list", branch])
            .output()
            .expect("run git")
            .stdout,
    )
    .trim()
    .is_empty()
}

fn paths_in(status: &serde_json::Value) -> Vec<String> {
    status["files"]
        .as_array()
        .map(|files| {
            files
                .iter()
                .filter_map(|f| f["path"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn a_thread_started_with_worktree_runs_in_it() {
    let server = start(/*git_repo*/ true).await;

    let (status, body) = server.start_thread("Isolate this work", true).await;
    assert_eq!(status, 200, "start failed: {body}");
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();

    let thread = server
        .state
        .store
        .load_thread(server.project_id, thread_id)
        .await
        .unwrap()
        .unwrap();
    let worktree = thread
        .git_workspace
        .and_then(|ws| ws.as_worktree().cloned())
        .expect("the thread records its worktree");
    assert_eq!(
        worktree.branch,
        giskard_server::worktree::branch_name(thread_id)
    );
    assert!(Path::new(&worktree.path).join("README.md").is_file());

    // The whole point: the harness works in the worktree, not the project's checkout.
    assert_eq!(server.workspace_roots().await, vec![worktree.path.clone()]);
    let listed = server.thread_summaries().await;
    let thread_id_string = thread_id.to_string();
    let summary = listed["threads"]
        .as_array()
        .unwrap()
        .iter()
        .find(|thread| thread["id"].as_str() == Some(thread_id_string.as_str()))
        .expect("started thread appears in list");
    assert_eq!(
        summary["workspace_root"].as_str(),
        Some(worktree.path.as_str()),
        "thread summaries must expose the workspace root the browser strips from file-change paths"
    );
    assert!(
        !worktree
            .path
            .starts_with(&server._project.path().to_string_lossy().to_string()),
        "the worktree must not be a sibling of the project directory"
    );
}

#[tokio::test]
async fn a_thread_started_without_the_flag_is_unchanged() {
    let server = start(/*git_repo*/ true).await;

    let (status, body) = server.start_thread("Ordinary thread", false).await;
    assert_eq!(status, 200, "start failed: {body}");
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();

    let thread = server
        .state
        .store
        .load_thread(server.project_id, thread_id)
        .await
        .unwrap()
        .unwrap();
    assert!(thread.git_workspace.is_none());
    assert_eq!(
        server.workspace_roots().await,
        vec![server._project.path().to_string_lossy().to_string()]
    );
}

/// Isolation that quietly does not happen is worse than an error: the UI would show a worktree
/// thread that is editing the user's checkout.
#[tokio::test]
async fn a_project_that_is_not_a_repository_fails_instead_of_falling_back() {
    let server = start(/*git_repo*/ false).await;

    let (status, body) = server.start_thread("Isolate this work", true).await;

    assert_eq!(status, 400, "expected a refusal, got {status}: {body}");
    assert!(
        body.contains("not a Git repository"),
        "the error must explain why isolation was impossible, got {body}"
    );
    assert!(
        server.workspace_roots().await.is_empty(),
        "no thread may be opened when its worktree could not be created"
    );
    assert_eq!(
        server
            .state
            .store
            .list_threads(server.project_id)
            .await
            .unwrap()
            .len(),
        0,
        "a failed start must leave no thread behind"
    );
}

/// Resume is the path that matters after a restart: reading the project's workspace here would
/// silently drop the thread back into the user's checkout, with nothing in the UI to say so.
#[tokio::test]
async fn reopening_an_isolated_thread_returns_to_its_worktree() {
    let server = start(/*git_repo*/ true).await;
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree_path = server
        .state
        .store
        .load_thread(server.project_id, thread_id)
        .await
        .unwrap()
        .unwrap()
        .git_workspace
        .and_then(|ws| ws.as_worktree().cloned())
        .unwrap()
        .path;

    // A second server over the same data directory: the restart, with nothing in memory.
    let (restarted, base, cookie) = restart(&server).await;
    let response = server
        .client
        .post(format!("{base}/api/projects/{}/threads", server.project_id))
        .header("cookie", &cookie)
        .json(&serde_json::json!({ "thread_id": thread_id, "resume": null }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    assert_eq!(
        restarted.opened_workspace_roots.lock().await.clone(),
        vec![worktree_path],
        "the reopen must land in the thread's worktree, not the project's checkout"
    );
}

/// Deleting a thread takes its worktree and the branch it started on with it — that branch is
/// Giskard's own scaffolding, and leaving it would strand a ref nothing points to any more.
#[tokio::test]
async fn deleting_a_thread_removes_its_worktree_and_branch() {
    let server = start(/*git_repo*/ true).await;
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree = server.worktree_of(thread_id).await;
    server.wait_until_idle(thread_id).await;

    let response = server.delete_thread(thread_id, /*force*/ false).await;

    assert_eq!(response.status(), 204);
    assert!(!Path::new(&worktree.path).exists());
    assert!(
        !branch_exists(server._project.path(), &worktree.branch),
        "the thread's branch outlived the thread that owned it"
    );
}

/// A worktree the user removed by hand is still registered with Git, and a registered worktree
/// holds its branch. Deleting the thread has to finish the job rather than leave a branch nothing
/// can remove — which means still asking Git to remove a checkout that is already gone.
#[tokio::test]
async fn deleting_a_thread_whose_worktree_vanished_still_clears_its_branch() {
    let server = start(/*git_repo*/ true).await;
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree = server.worktree_of(thread_id).await;
    server.wait_until_idle(thread_id).await;
    std::fs::remove_dir_all(&worktree.path).unwrap();

    let response = server.delete_thread(thread_id, /*force*/ false).await;

    assert_eq!(response.status(), 204);
    assert!(
        !branch_exists(server._project.path(), &worktree.branch),
        "the branch survived a worktree that was already gone"
    );
}

/// A worktree holding work that exists nowhere else is not deleted on a plain request: the
/// confirmation has to happen first, and the refusal names what is at stake.
#[tokio::test]
async fn deleting_a_thread_refuses_to_discard_work_without_confirmation() {
    let server = start(/*git_repo*/ true).await;
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree = server.worktree_of(thread_id).await;
    server.wait_until_idle(thread_id).await;
    std::fs::write(
        Path::new(&worktree.path).join("in-progress.txt"),
        "not committed anywhere\n",
    )
    .unwrap();

    let refusal = server.delete_thread(thread_id, /*force*/ false).await;
    assert_eq!(refusal.status(), 409);
    let message = refusal.text().await.unwrap();
    assert!(
        message.contains("1 uncommitted change") && message.contains(&worktree.branch),
        "the refusal must name what would be destroyed, got: {message}"
    );
    assert!(
        Path::new(&worktree.path).exists(),
        "a refused deletion must leave the worktree alone"
    );

    // The card shows the same facts before asking, then confirms.
    let impact: serde_json::Value = server
        .client
        .get(format!(
            "{}/api/projects/{}/threads/{thread_id}/deletion-impact",
            server.base, server.project_id
        ))
        .header("cookie", &server.cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(impact["worktrees"][0]["uncommitted_changes"], 1);
    assert!(
        impact["worktrees"][0]["summary"]
            .as_str()
            .unwrap()
            .contains("uncommitted change")
    );

    let forced = server.delete_thread(thread_id, /*force*/ true).await;
    assert_eq!(forced.status(), 204);
    assert!(!Path::new(&worktree.path).exists());
}

/// Commits are the other way work is lost, and they are invisible in the working tree: the branch
/// looks clean while holding the only copy of an afternoon.
#[tokio::test]
async fn deleting_a_thread_refuses_to_discard_commits_on_no_other_branch() {
    let server = start(/*git_repo*/ true).await;
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree = server.worktree_of(thread_id).await;
    server.wait_until_idle(thread_id).await;
    let path = Path::new(&worktree.path);
    std::fs::write(path.join("committed.txt"), "work\n").unwrap();
    git(path, &["add", "committed.txt"]);
    git(path, &["commit", "-qm", "thread work"]);

    let refusal = server.delete_thread(thread_id, /*force*/ false).await;

    assert_eq!(refusal.status(), 409);
    let message = refusal.text().await.unwrap();
    assert!(
        message.contains("1 commit on no other ref"),
        "the refusal must name the commits at stake, got: {message}"
    );

    // Parked on a branch of the agent's own, the same commit is no longer at risk — and that branch
    // survives the deletion, because it is the user's now.
    git(path, &["branch", "agent/keep-this"]);
    let response = server.delete_thread(thread_id, /*force*/ false).await;
    assert_eq!(response.status(), 204);
    assert!(branch_exists(server._project.path(), "agent/keep-this"));
    assert!(!branch_exists(server._project.path(), &worktree.branch));
}

/// Deleting a project is a project-scope decision the user has already confirmed, so it sweeps the
/// worktrees rather than refusing over one thread's unfinished work.
#[tokio::test]
async fn deleting_a_project_sweeps_its_worktrees() {
    let server = start(/*git_repo*/ true).await;
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree = server.worktree_of(thread_id).await;
    server.wait_until_idle(thread_id).await;
    std::fs::write(
        Path::new(&worktree.path).join("in-progress.txt"),
        "not committed anywhere\n",
    )
    .unwrap();

    let response = server
        .client
        .delete(format!(
            "{}/api/projects/{}",
            server.base, server.project_id
        ))
        .header("cookie", &server.cookie)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 204);
    assert!(!Path::new(&worktree.path).exists());
    assert!(!branch_exists(server._project.path(), &worktree.branch));
}

/// The registry materializes a sub-agent the moment the parent's turn reports spawning one, and it
/// opens the child against a workspace of its own choosing. That has to be the parent's worktree:
/// the harness is already running the child there.
#[tokio::test]
async fn materializing_a_subagent_opens_it_in_its_parents_worktree() {
    let server = start(/*git_repo*/ true).await;
    server
        .harness
        .spawns_subagent
        .lock()
        .await
        .replace("native-child".to_string());
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let parent_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree = server.worktree_of(parent_id).await;

    wait_for_subagent(&server, parent_id).await;

    // The child's thread file lands before the harness open that records its workspace root, so
    // comparing straight away races the second open.
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    while server.workspace_roots().await.len() < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the sub-agent was never opened"
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        server.workspace_roots().await,
        vec![worktree.path.clone(), worktree.path],
        "the parent and the sub-agent it spawned must be opened in the same worktree"
    );
}

/// After a restart nothing is attached, so the next report of activity on a sub-agent has to reopen
/// it — the other registry route into a child's workspace, and a cold one, where the cwd Giskard
/// sends is the cwd the harness uses.
#[tokio::test]
async fn reattaching_a_subagent_after_a_restart_uses_its_parents_worktree() {
    let server = start(/*git_repo*/ true).await;
    server
        .harness
        .spawns_subagent
        .lock()
        .await
        .replace("native-child".to_string());
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let parent_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree = server.worktree_of(parent_id).await;
    let child = wait_for_subagent(&server, parent_id).await;

    // A second server over the same data directory: the child is persisted but nothing is attached.
    let (restarted, base, cookie) = restart(&server).await;
    restarted
        .spawns_subagent
        .lock()
        .await
        .replace("native-child".to_string());
    let port = base.rsplit(':').next().unwrap().to_string();
    let request = tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(format!("ws://127.0.0.1:{port}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(request)
        .await
        .expect("connect websocket");
    use futures_util::SinkExt;
    for message in [
        giskard_proto::ClientMessage::Subscribe {
            thread_id: parent_id,
            since: None,
        },
        giskard_proto::ClientMessage::SendInput {
            thread_id: parent_id,
            text: "Ask the reviewer again".into(),
            attachments: Vec::new(),
        },
    ] {
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&message).unwrap().into(),
        ))
        .await
        .unwrap();
    }

    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    loop {
        // Two opens: the parent on subscribe, then the child when its activity is reported.
        if restarted.opened_workspace_roots.lock().await.len() >= 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the sub-agent was never reattached"
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        restarted.opened_workspace_roots.lock().await.clone(),
        vec![worktree.path.clone(), worktree.path],
        "reattaching a sub-agent must use the worktree it works in"
    );
    // The reattach must not have invented a second child.
    assert_eq!(wait_for_subagent(&server, parent_id).await, child);
}

/// Opening a sub-agent is the cold path — the one where the harness stops ignoring the cwd Giskard
/// sends and applies it. It is the moment the child would silently move out of the worktree its own
/// earlier work is in.
#[tokio::test]
async fn opening_a_subagent_attaches_in_its_parents_worktree() {
    let server = start(/*git_repo*/ true).await;
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let parent_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree = server.worktree_of(parent_id).await;
    server.wait_until_idle(parent_id).await;
    let child = server.persist_subagent(parent_id).await;
    // Drop the parent's own open, so what is asserted below is the sub-agent's.
    server.harness.opened_workspace_roots.lock().await.clear();

    let response = server
        .client
        .post(format!(
            "{}/api/projects/{}/threads",
            server.base, server.project_id
        ))
        .header("cookie", &server.cookie)
        .json(&serde_json::json!({ "thread_id": child, "resume": null }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    assert_eq!(
        server.workspace_roots().await,
        vec![worktree.path],
        "opening a sub-agent must attach in the worktree it works in"
    );
}

/// A sub-agent inherits its parent's worktree and cannot be deleted independently. Rejecting that
/// operation must leave both the child record and the parent's checkout and branch untouched.
#[tokio::test]
async fn deleting_a_subagent_is_rejected_without_touching_its_parents_worktree() {
    let server = start(/*git_repo*/ true).await;
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let parent_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree = server.worktree_of(parent_id).await;
    server.wait_until_idle(parent_id).await;
    let child = server.persist_subagent(parent_id).await;

    let response = server.delete_thread(child, /*force*/ false).await;
    assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
    assert!(
        server
            .state
            .store
            .load_thread(server.project_id, child)
            .await
            .unwrap()
            .is_some(),
        "rejected deletion removed the sub-agent record"
    );

    assert!(
        Path::new(&worktree.path).exists(),
        "the parent's checkout was removed with its sub-agent"
    );
    assert!(
        branch_exists(server._project.path(), &worktree.branch),
        "the parent's branch was deleted with its sub-agent"
    );
}

/// The row above the composer has to describe the tree the agent is working in. Reading the
/// project's checkout for an isolated thread would report a branch and a change list belonging to
/// someone else, and would never move as the agent works.
#[tokio::test]
async fn git_status_reads_the_threads_own_workspace() {
    let server = start(/*git_repo*/ true).await;
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree = server
        .state
        .store
        .load_thread(server.project_id, thread_id)
        .await
        .unwrap()
        .unwrap()
        .git_workspace
        .and_then(|ws| ws.as_worktree().cloned())
        .unwrap();

    // Two workspaces, two different sets of changes.
    std::fs::write(
        Path::new(&worktree.path).join("agent-edit.txt"),
        "written by the agent\n",
    )
    .unwrap();
    std::fs::write(
        server._project.path().join("user-edit.txt"),
        "written by the user\n",
    )
    .unwrap();

    let scoped = server.git_status(Some(thread_id)).await;
    assert_eq!(scoped["branch"].as_str(), Some(worktree.branch.as_str()));
    let scoped_paths = paths_in(&scoped);
    assert!(
        scoped_paths.contains(&"agent-edit.txt".to_string()),
        "the thread's own change is missing: {scoped_paths:?}"
    );
    assert!(
        !scoped_paths.contains(&"user-edit.txt".to_string()),
        "the project's change must not appear in the thread's status: {scoped_paths:?}"
    );

    // Without a thread the project's own workspace still answers, which is what a draft reads.
    let project = server.git_status(None).await;
    assert_eq!(project["branch"].as_str(), Some("main"));
    assert!(paths_in(&project).contains(&"user-edit.txt".to_string()));
}

/// A probe that cannot run must not read as "nothing would be lost". Deletion is forced and takes
/// the branch with it, so that answer is the one that authorises destroying work — and the thread's
/// own guard is the last thing standing between a broken Git and an unrecoverable delete.
///
/// Both probes are broken separately, because either one alone answering `0` on failure is enough
/// to wave a deletion through: the checkout is asked about uncommitted files, the repository about
/// commits reachable from no other ref.
#[tokio::test]
async fn deleting_a_thread_refuses_when_git_cannot_report_what_would_be_lost() {
    for break_the in ["checkout", "repository"] {
        let server = start(/*git_repo*/ true).await;
        let (_, body) = server.start_thread("Isolate this work", true).await;
        let started: serde_json::Value = serde_json::from_str(&body).unwrap();
        let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
        let worktree = server.worktree_of(thread_id).await;
        server.wait_until_idle(thread_id).await;

        // A directory that exists but is not a repository: Git runs and refuses, rather than the
        // path simply being absent, which is a definite "no work here" and not a failure.
        let elsewhere = tempfile::TempDir::new().unwrap();
        let elsewhere = elsewhere.path().to_string_lossy().into_owned();
        let (path, repo_root) = match break_the {
            "checkout" => (elsewhere.clone(), worktree.repo_root.clone()),
            _ => (worktree.path.clone(), elsewhere.clone()),
        };
        server.repoint_worktree(thread_id, &path, &repo_root).await;

        let impact = server
            .client
            .get(format!(
                "{}/api/projects/{}/threads/{thread_id}/deletion-impact",
                server.base, server.project_id
            ))
            .header("cookie", &server.cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(
            impact.status(),
            503,
            "with the {break_the} unreadable the card must be told the question could not be \
             answered, not told there is no risk"
        );

        let refused = server.delete_thread(thread_id, /*force*/ false).await;
        assert_eq!(refused.status(), 409, "broken {break_the}");
        let message = refused.text().await.unwrap();
        assert!(
            message.contains("could not check"),
            "the refusal must say why it refused: {message}"
        );
        assert!(
            server
                .state
                .store
                .load_thread(server.project_id, thread_id)
                .await
                .unwrap()
                .is_some(),
            "a refused deletion must leave the thread intact"
        );

        // The user can still say "delete it anyway", which is the whole point of keeping this a
        // refusal rather than a hard error.
        let forced = server.delete_thread(thread_id, /*force*/ true).await;
        assert!(
            forced.status().is_success(),
            "forced delete, broken {break_the}"
        );
    }
}

/// Cleanup runs before the thread file does, because that file is the only record of where the
/// checkout and branch are. A cleanup that fails after it was deleted would strand both with
/// nothing left to find them by, so the deletion is refused and the thread kept for a retry.
#[tokio::test]
async fn a_thread_whose_worktree_cannot_be_removed_is_kept_so_the_delete_can_be_retried() {
    let server = start(/*git_repo*/ true).await;
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree = server.worktree_of(thread_id).await;
    server.wait_until_idle(thread_id).await;

    // Points at the project's own checkout: still a repository, so the impact probes answer
    // normally, but `git worktree remove` refuses a main working tree.
    let project_root = server._project.path().to_string_lossy().into_owned();
    server
        .repoint_worktree(thread_id, &project_root, &worktree.repo_root)
        .await;

    let refused = server.delete_thread(thread_id, /*force*/ false).await;
    assert_eq!(refused.status(), 409);
    let message = refused.text().await.unwrap();
    assert!(
        message.contains(&worktree.branch),
        "the refusal must name what was left behind: {message}"
    );
    assert!(
        server
            .state
            .store
            .load_thread(server.project_id, thread_id)
            .await
            .unwrap()
            .is_some(),
        "the thread must survive so the deletion can be retried"
    );
    assert!(
        Path::new(&project_root).exists(),
        "the refusal must not have removed anything"
    );

    let forced = server.delete_thread(thread_id, /*force*/ true).await;
    assert!(
        forced.status().is_success(),
        "force must still get rid of a thread whose worktree will not come down"
    );
    assert!(
        server
            .state
            .store
            .load_thread(server.project_id, thread_id)
            .await
            .unwrap()
            .is_none()
    );
}

/// A sub-agent records no worktree of its own: the harness spawns it inside its parent's turn, so
/// it works in the parent's checkout. The row above the composer has to agree with that, or it
/// describes a tree the sub-agent never touched.
#[tokio::test]
async fn a_subagent_reads_the_worktree_of_the_thread_that_owns_it() {
    let server = start(/*git_repo*/ true).await;
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let parent_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree = server
        .state
        .store
        .load_thread(server.project_id, parent_id)
        .await
        .unwrap()
        .unwrap()
        .git_workspace
        .and_then(|ws| ws.as_worktree().cloned())
        .unwrap();

    // A child and a grandchild, so the lookup has to walk the chain rather than peek at the
    // immediate parent.
    let child = server.persist_subagent(parent_id).await;
    let grandchild = server.persist_subagent(child).await;

    std::fs::write(
        Path::new(&worktree.path).join("agent-edit.txt"),
        "written in the worktree\n",
    )
    .unwrap();
    std::fs::write(
        server._project.path().join("user-edit.txt"),
        "written by the user\n",
    )
    .unwrap();

    for descendant in [child, grandchild] {
        let status = server.git_status(Some(descendant)).await;
        assert_eq!(
            status["branch"].as_str(),
            Some(worktree.branch.as_str()),
            "a sub-agent must report the branch of the worktree it works in"
        );
        let paths = paths_in(&status);
        assert!(
            paths.contains(&"agent-edit.txt".to_string()),
            "the worktree's change is missing for a sub-agent: {paths:?}"
        );
        assert!(
            !paths.contains(&"user-edit.txt".to_string()),
            "the project's change must not appear for a sub-agent: {paths:?}"
        );
    }
}

/// A sub-agent of an ordinary thread has no worktree anywhere in its chain, and must read the
/// project rather than inheriting someone else's.
#[tokio::test]
async fn a_subagent_of_an_ordinary_thread_reads_the_project() {
    let server = start(/*git_repo*/ true).await;
    let (_, body) = server.start_thread("Ordinary thread", false).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let parent_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let child = server.persist_subagent(parent_id).await;

    std::fs::write(
        server._project.path().join("user-edit.txt"),
        "written by the user\n",
    )
    .unwrap();

    let status = server.git_status(Some(child)).await;
    assert_eq!(status["branch"].as_str(), Some("main"));
    assert!(paths_in(&status).contains(&"user-edit.txt".to_string()));
}

/// An unknown thread is answered with an error, never with the project's workspace: handing back a
/// different tree under the name of the one that was asked for is the confusion isolation exists to
/// prevent.
#[tokio::test]
async fn git_status_refuses_a_thread_it_cannot_resolve() {
    let server = start(/*git_repo*/ true).await;

    let response = server.git_status_response(ThreadId::new()).await;

    assert_eq!(response.status(), 404);
}

/// `load_thread` is scoped to its project, so a thread from another project is unknown here — which
/// also stops this project's endpoints reading files out of that project's workspace.
#[tokio::test]
async fn git_status_refuses_a_thread_from_another_project() {
    let server = start(/*git_repo*/ true).await;
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();

    let other_project = ProjectId::new();
    server
        .state
        .store
        .create_project(
            other_project,
            "other",
            &server._project.path().to_string_lossy(),
        )
        .await
        .unwrap();

    let response = server
        .client
        .get(format!(
            "{}/api/projects/{other_project}/git/status?thread_id={thread_id}",
            server.base
        ))
        .header("cookie", &server.cookie)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
}

/// The other way back into a thread after a restart: the browser subscribes over the WebSocket
/// without opening the thread over HTTP first, so `ensure_thread_open` attaches the harness. Reading
/// the project's workspace there would silently un-isolate the thread, with nothing in the UI to
/// say so.
#[tokio::test]
async fn subscribing_after_a_restart_attaches_in_the_worktree() {
    let server = start(/*git_repo*/ true).await;
    let (_, body) = server.start_thread("Isolate this work", true).await;
    let started: serde_json::Value = serde_json::from_str(&body).unwrap();
    let thread_id: ThreadId = started["thread_id"].as_str().unwrap().parse().unwrap();
    let worktree_path = server
        .state
        .store
        .load_thread(server.project_id, thread_id)
        .await
        .unwrap()
        .unwrap()
        .git_workspace
        .and_then(|ws| ws.as_worktree().cloned())
        .unwrap()
        .path;

    let (restarted, base, cookie) = restart(&server).await;
    let port = base.rsplit(':').next().unwrap().to_string();
    let request = tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(format!("ws://127.0.0.1:{port}/api/ws"))
        .header("host", format!("127.0.0.1:{port}"))
        .header("cookie", cookie)
        .header("upgrade", "websocket")
        .header("connection", "upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .body(())
        .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(request)
        .await
        .expect("connect websocket");
    use futures_util::SinkExt;
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&giskard_proto::ClientMessage::Subscribe {
            thread_id,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    loop {
        if !restarted.opened_workspace_roots.lock().await.is_empty() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the subscribe never attached the harness"
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        restarted.opened_workspace_roots.lock().await.clone(),
        vec![worktree_path],
        "attaching on subscribe must use the thread's worktree"
    );
}

/// Build a second server over the same data directory, as a restart would leave it: the persisted
/// thread is all there is to go on.
async fn restart(server: &Harnessed) -> (Arc<RecordingHarness>, String, String) {
    let store = Arc::new(giskard_persist::PersistStore::new(
        server._data.path().to_path_buf(),
    ));
    let harness = Arc::new(RecordingHarness::default());
    let state = AppState::new(
        store,
        Arc::new(RecordingFactory(harness.clone())),
        (0..32u8).collect(),
    );
    let app = build_app(state.clone());
    // An ephemeral port, not a fixed one: these tests run concurrently with every other test in the
    // binary, and a hard-coded port makes a second restart in the same run fail with `AddrInUse`.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let login = server
        .client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({ "password": "testpass" }))
        .send()
        .await
        .unwrap();
    let cookie = login
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    (harness, base, cookie)
}
