//! Starting a thread in its own Git worktree, end to end over HTTP.
//!
//! The unit of interest is where the harness is opened: a thread created with the `worktree` Git
//! strategy must be opened against the worktree rather than the project's checkout, and must still
//! be after a restart. Everything here therefore records the workspace root the harness was handed.

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use giskard_core::approval::ApprovalDecision;
use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ApprovalId, ProjectId, ServerRequestId, ThreadId, TurnId};
use giskard_core::model::ModelRef;
use giskard_core::server_request::ServerRequestResponse;
use giskard_core::turn::TurnOverrides;
use giskard_core::user_input::UserInput;
use giskard_harness::{
    AgentEventStream, AgentHarness, HarnessCapabilities, OpenThreadOptions, ThreadHandle,
};
use giskard_persist::store::ProjectConfig;
use giskard_server::{AppState, HarnessFactory, build_app};
use tokio::sync::Mutex;

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
    opened_workspace_roots: Mutex<Vec<String>>,
    /// A `std` mutex, not a `tokio` one: `subscribe` is a synchronous trait method, and with an
    /// async mutex it could only `try_lock` — handing back a stream over a dropped sender whenever
    /// the map happened to be held. Every event for that thread would then vanish and the test would
    /// fail on an unexplained timeout. Nothing holds this guard across an await.
    threads: std::sync::Mutex<Vec<(ThreadId, tokio::sync::broadcast::Sender<AgentEvent>)>>,
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

    async fn open_thread(&self, opts: OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        self.opened_workspace_roots
            .lock()
            .await
            .push(opts.workspace_root.to_string_lossy().into_owned());
        let thread = opts.thread.unwrap_or_default();
        let (tx, _) = tokio::sync::broadcast::channel(16);
        self.threads.lock().unwrap().push((thread, tx));
        Ok(ThreadHandle {
            thread,
            harness_thread_id: opts.resume.unwrap_or_else(|| format!("native-{thread}")),
            warning: None,
            resumed_model: Some(opts.initial_model),
            agent_name: None,
            parent_harness_thread_id: None,
        })
    }

    async fn start_turn(
        &self,
        _thread: &ThreadHandle,
        _input: UserInput,
        _overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError> {
        Ok(TurnId::new())
    }

    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream {
        if let Some((_, tx)) = self
            .threads
            .lock()
            .unwrap()
            .iter()
            .find(|(id, _)| *id == thread.thread)
        {
            return AgentEventStream::new(tx.subscribe());
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

    async fn shutdown(&self) -> Result<(), HarnessError> {
        Ok(())
    }
}

struct RecordingFactory(Arc<RecordingHarness>);

#[async_trait::async_trait]
impl HarnessFactory for RecordingFactory {
    async fn create(&self, _config: &ProjectConfig) -> Result<Arc<dyn AgentHarness>, HarnessError> {
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
            ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
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

    async fn start_thread(&self, text: &str, worktree: bool) -> (reqwest::StatusCode, String) {
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
                "mode": "build",
                "permission_preset": "ask_first",
                "git_strategy": if worktree { "worktree" } else { "shared" },
            }))
            .send()
            .await
            .unwrap();
        let status = response.status();
        // Errors come back as plain text, successes as JSON, so read the body once and let each
        // test decide what it is looking at.
        (status, response.text().await.unwrap_or_default())
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
            ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
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
