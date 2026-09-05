//! Code-overlay endpoint integration tests: syntax highlighting, raw file download, image preview,
//! path linkification, and server-side Markdown rendering (spec §11.2 / §11.3).

use chrono::Utc;
use giskard_core::HarnessError;
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{Item, ItemPayload};
use giskard_core::token::TokenUsage;
use giskard_core::turn::{Mode, Turn, TurnStatus, TurnStatusKind};
use giskard_persist::store::{ThreadGitWorkspace, ThreadWorktree};
use giskard_testenv::{TestProject, TestServer, factory, fixtures};

struct Fixture {
    server: TestServer,
    project: TestProject,
    pid: ProjectId,
    tid: ThreadId,
}

/// The endpoints under test name a thread in their path, so the fixture persists one.
async fn start_server() -> Fixture {
    let server = TestServer::spawn(factory::failing(HarnessError::Spawn("dummy".into()))).await;
    let project = server.create_project("viz-test").await;
    let pid = project.id;

    tokio::fs::write(
        project.dir.path().join("main.rs"),
        "fn main() {\n    println!(\"hi\");\n}\n",
    )
    .await
    .unwrap();
    tokio::fs::write(project.dir.path().join("data.bin"), b"bin\x00ary\x00data")
        .await
        .unwrap();
    tokio::fs::write(
        project.dir.path().join("config.toml"),
        "[server]\nbind = \"127.0.0.1:0\"\nsecure_cookies = false\n",
    )
    .await
    .unwrap();
    tokio::fs::write(project.dir.path().join("image.png"), fixtures::TINY_PNG)
        .await
        .unwrap();
    tokio::fs::write(
        project.dir.path().join("vector.svg"),
        r#"<svg xmlns="http://www.w3.org/2000/svg"></svg>"#,
    )
    .await
    .unwrap();

    let tid = ThreadId::new();
    server
        .state
        .store
        .save_thread(pid, &thread_file(pid, tid))
        .await
        .unwrap();

    Fixture {
        server,
        project,
        pid,
        tid,
    }
}

fn thread_file(pid: ProjectId, tid: ThreadId) -> giskard_persist::store::ThreadFile {
    giskard_persist::store::ThreadFile {
        revision: 0,
        version: 1,
        id: tid,
        project_id: pid,
        title: "code overlay".into(),
        harness_thread_id: format!("native-{tid}"),
        parent_thread_id: None,
        spawned_by_turn_id: None,
        kind: giskard_core::thread::ThreadKind::Primary,
        mode: giskard_core::turn::TurnMode::Known(giskard_core::turn::Mode::Build),
        current_model: giskard_core::turn::TurnModel::Known(giskard_core::model::ModelRef {
            provider: "openai".into(),
            model: "gpt-5.5".into(),
            reasoning_effort: None,
        }),
        context_window: 1,
        model_context_windows: std::collections::HashMap::new(),
        permission_preset: giskard_core::turn::PermissionPreset::AskFirst,
        model_efforts: std::collections::HashMap::new(),
        tokens: giskard_core::token::TokenLedger::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        archived: false,
        git_workspace: None,
    }
}

fn command_turn(output: &str, status: Option<&str>) -> (Turn, ItemId) {
    let now = Utc::now();
    let item_id = ItemId::new();
    (
        Turn {
            id: TurnId::new(),
            user_input: giskard_core::user_input::UserInput::text("run command"),
            items: vec![Item {
                id: item_id,
                harness_item_id: format!("native-{item_id}"),
                payload: ItemPayload::CommandExecution {
                    command: "test command".into(),
                    cwd: ".".into(),
                    output: output.into(),
                    output_truncated: false,
                    output_original_bytes: None,
                    output_original_lines: None,
                    exit_code: Some(0),
                    status: status.map(str::to_owned),
                    process_id: None,
                    duration_ms: Some(1),
                },
                created_at: now,
            }],
            model: giskard_core::turn::TurnModel::Known(giskard_core::model::ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            }),
            mode: giskard_core::turn::TurnMode::Known(Mode::Build),
            status: TurnStatus {
                kind: TurnStatusKind::Completed,
                message: None,
            },
            usage: TokenUsage::new(1, 1),
            diffs: vec![],
            started_at: now,
            completed_at: Some(now),
        },
        item_id,
    )
}

fn command_output_url(
    base: &str,
    pid: ProjectId,
    tid: ThreadId,
    turn_id: TurnId,
    item_id: ItemId,
) -> String {
    format!(
        "{base}/api/projects/{pid}/threads/{tid}/turns/{turn_id}/items/{item_id}/command-output"
    )
}

#[tokio::test]
async fn highlight_rust_file() {
    let fixture = start_server().await;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "{base}/api/projects/{pid}/threads/{tid}/highlight?path=main.rs"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(!body["is_binary"].as_bool().unwrap());
    assert!(body["total_lines"].as_u64().unwrap() >= 3);
    assert!(body["file_size"].as_u64().unwrap() > 0);
    let html = body["html"].as_str().unwrap();
    assert!(!html.is_empty());
}

#[tokio::test]
async fn highlight_toml_file() {
    let fixture = start_server().await;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "{base}/api/projects/{pid}/threads/{tid}/highlight?path=config.toml"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["language"].as_str(), Some("TOML"));
    assert!(!body["is_binary"].as_bool().unwrap());
    let html = body["html"].as_str().unwrap();
    assert!(!html.is_empty());
    assert!(
        html.contains("<span"),
        "TOML should be syntax highlighted, got {html}"
    );
}

#[tokio::test]
async fn highlight_binary_file() {
    let fixture = start_server().await;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "{base}/api/projects/{pid}/threads/{tid}/highlight?path=data.bin"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["is_binary"].as_bool().unwrap());
    assert!(body["html"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn download_raw_file() {
    let fixture = start_server().await;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "{base}/api/projects/{pid}/threads/{tid}/raw?path=main.rs"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let content = resp.text().await.unwrap();
    assert!(content.contains("fn main"));
}

#[tokio::test]
async fn image_preview_serves_raster_image_inline() {
    let fixture = start_server().await;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "{base}/api/projects/{pid}/threads/{tid}/image?path=image.png"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("image/png")
    );
    assert_eq!(resp.bytes().await.unwrap().as_ref(), fixtures::TINY_PNG);
}

#[tokio::test]
async fn image_preview_rejects_svg() {
    let fixture = start_server().await;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "{base}/api/projects/{pid}/threads/{tid}/image?path=vector.svg"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn linkify_finds_paths() {
    let fixture = start_server().await;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/api/projects/{pid}/threads/{tid}/linkify"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"text": "see main.rs for the entry point"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let links = body["links"].as_array().unwrap();
    assert!(!links.is_empty(), "should find main.rs as a link");
    assert!(links[0]["path"].as_str().unwrap().contains("main.rs"));
}

#[tokio::test]
async fn render_endpoint_returns_sanitized_markdown_with_path_links() {
    let fixture = start_server().await;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let text = "See `main.rs` and **open** main.rs now.\n\n```rust\nfn main() {}\n```\n\n<img src=x onerror=alert(1)>";
    let resp = client
        .post(format!("{base}/api/projects/{pid}/threads/{tid}/render"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({ "text": text }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let html = body["html"].as_str().unwrap();

    // Markdown is rendered...
    assert!(
        html.contains("<strong>open</strong>"),
        "bold renders: {html}"
    );
    // ...prose paths become path-link buttons, but code spans stay literal...
    assert!(
        html.contains("class=\"path-link\" data-path=\"main.rs\""),
        "prose path is linkified: {html}"
    );
    assert!(
        html.contains("<code>main.rs</code>"),
        "code stays literal: {html}"
    );
    // ...fenced code blocks show their language and are highlighted server-side...
    assert!(
        html.contains("<div class=\"code-block-head\"><span>Rust</span></div>"),
        "code block language is visible: {html}"
    );
    assert!(
        html.contains("data-highlighted=\"true\""),
        "code block is highlighted: {html}"
    );
    // ...and raw HTML is escaped, never passed through.
    assert!(
        !html.contains("<img"),
        "raw HTML must not pass through: {html}"
    );
    assert!(html.contains("&lt;img"), "raw HTML is escaped: {html}");
}

#[tokio::test]
async fn linkify_endpoint_returns_only_existing_workspace_files() {
    let fixture = start_server().await;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let proj_dir = &fixture.project.dir;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    tokio::fs::create_dir_all(proj_dir.path().join("src"))
        .await
        .unwrap();
    tokio::fs::write(proj_dir.path().join("src/lib.rs"), "pub fn lib() {}\n")
        .await
        .unwrap();

    let absolute_main = proj_dir.path().join("main.rs");
    let text = format!(
        "Changed {absolute_main}. Also inspect ./src/lib.rs:2:4, but ignore missing.rs:4.",
        absolute_main = absolute_main.display()
    );

    let resp = client
        .post(format!("{base}/api/projects/{pid}/threads/{tid}/linkify"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({ "text": text }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let links = body["links"].as_array().unwrap();
    let paths = links
        .iter()
        .map(|link| link["path"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        paths,
        vec!["main.rs", "src/lib.rs"],
        "linkify should return only existing workspace files as workspace-relative paths"
    );
    assert_eq!(
        links[0].get("line"),
        None,
        "plain path should not carry a line target"
    );
    assert_eq!(
        links[1]["line"].as_u64(),
        Some(2),
        "colon line suffix should be returned as a line target"
    );

    for link in links {
        let start = link["start"].as_u64().unwrap() as usize;
        let end = link["end"].as_u64().unwrap() as usize;
        let slice = &text[start..end];
        assert!(
            slice == absolute_main.to_string_lossy() || slice == "./src/lib.rs:2:4",
            "span should point at the exact source text path, got {slice:?}"
        );
    }
}

#[tokio::test]
async fn command_output_links_linkifies_persisted_output_with_matching_version() {
    let fixture = start_server().await;
    let state = &fixture.server.state;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let (turn, item_id) = command_turn("compiler error at main.rs:2\n", Some("completed"));
    state.store.append_turn(pid, tid, &turn).await.unwrap();
    let output_url = command_output_url(&base, pid, tid, turn.id, item_id);

    let output = client
        .get(&output_url)
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(output.status(), 200);
    let version = output.headers().get("etag").unwrap().clone();

    let response = client
        .get(format!("{output_url}-links"))
        .header("cookie", &cookie)
        .header("if-output-match", version)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    let links = body["links"].as_array().unwrap();
    assert_eq!(
        links.len(),
        1,
        "persisted command output should be linkified"
    );
    assert_eq!(links[0]["path"], "main.rs");
    assert_eq!(links[0]["line"], 2);
}

#[tokio::test]
async fn command_output_links_enforces_output_version_precondition() {
    let fixture = start_server().await;
    let state = &fixture.server.state;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let (turn, item_id) = command_turn("main.rs:1\n", None);
    state.store.append_turn(pid, tid, &turn).await.unwrap();
    let links_url = format!(
        "{}-links",
        command_output_url(&base, pid, tid, turn.id, item_id)
    );

    let missing = client
        .get(&links_url)
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 428);

    let stale_version = format!("\"sha256_{}\"", "0".repeat(64));
    let stale = client
        .get(&links_url)
        .header("cookie", &cookie)
        .header("if-output-match", stale_version)
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), 412);

    let malformed = [
        "sha256_unquoted".to_owned(),
        format!("W/\"sha256_{}\"", "0".repeat(64)),
        "\"sha256_stale\"".to_owned(),
        format!("\"sha256_{}\"", "0".repeat(63)),
        format!("\"sha256_{}\"", "0".repeat(65)),
        format!("\"sha256_{}\"", "A".repeat(64)),
        format!("\"sha256_{}\"", "g".repeat(64)),
        format!(
            "\"sha256_{}\", \"sha256_{}\"",
            "0".repeat(64),
            "1".repeat(64)
        ),
        format!("\"sha256_{}\"extra", "0".repeat(64)),
        format!("\"sha256_{}", "0".repeat(64)),
    ];
    for malformed in malformed {
        let response = client
            .get(&links_url)
            .header("cookie", &cookie)
            .header("if-output-match", &malformed)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400, "header {malformed:?}");
    }
}

#[tokio::test]
async fn command_output_links_rejects_unreadable_items_and_uses_thread_workspace() {
    let fixture = start_server().await;
    let state = &fixture.server.state;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let proj_dir = &fixture.project.dir;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let isolated = tempfile::TempDir::new().unwrap();
    tokio::fs::write(isolated.path().join("isolated.rs"), "fn isolated() {}\n")
        .await
        .unwrap();
    state
        .store
        .update_thread(pid, tid, |thread| {
            let path = isolated.path().to_string_lossy().into_owned();
            thread.git_workspace = Some(ThreadGitWorkspace::Worktree(ThreadWorktree {
                path: path.clone(),
                workspace: None,
                branch: "giskard/test".into(),
                base_commit: None,
                repo_root: proj_dir.path().to_string_lossy().into_owned(),
                common_dir: proj_dir.path().join(".git").to_string_lossy().into_owned(),
                git_dir: isolated.path().join(".git").to_string_lossy().into_owned(),
            }));
        })
        .await
        .unwrap();

    let (turn, item_id) = command_turn("isolated.rs:1 main.rs:1\n", Some("completed"));
    state.store.append_turn(pid, tid, &turn).await.unwrap();
    let output_url = command_output_url(&base, pid, tid, turn.id, item_id);
    let output = client
        .get(&output_url)
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    let version = output.headers().get("etag").unwrap().clone();
    let response = client
        .get(format!("{output_url}-links"))
        .header("cookie", &cookie)
        .header("if-output-match", version)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    let paths: Vec<_> = body["links"]
        .as_array()
        .unwrap()
        .iter()
        .map(|link| link["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, ["isolated.rs"]);

    let unknown_url = command_output_url(&base, pid, tid, turn.id, ItemId::new());
    let unknown = client
        .get(format!("{unknown_url}-links"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status(), 404);

    let (running_turn, running_item) = command_turn("isolated.rs:1", Some("running"));
    state
        .store
        .append_turn(pid, tid, &running_turn)
        .await
        .unwrap();
    let running = client
        .get(format!(
            "{}-links",
            command_output_url(&base, pid, tid, running_turn.id, running_item)
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(running.status(), 404);

    let wrong_kind = ItemId::new();
    let mut wrong_turn = command_turn("unused", None).0;
    wrong_turn.items[0] = Item {
        id: wrong_kind,
        harness_item_id: "message".into(),
        payload: ItemPayload::AgentMessage {
            text: "isolated.rs:1".into(),
        },
        created_at: Utc::now(),
    };
    state
        .store
        .append_turn(pid, tid, &wrong_turn)
        .await
        .unwrap();
    let wrong = client
        .get(format!(
            "{}-links",
            command_output_url(&base, pid, tid, wrong_turn.id, wrong_kind)
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), 404);
}

#[cfg(unix)]
#[tokio::test]
async fn linkify_endpoint_rejects_symlink_escape() {
    let outside = tempfile::TempDir::new().unwrap();
    let outside_file = outside.path().join("outside.rs");
    tokio::fs::write(&outside_file, "pub fn outside() {}\n")
        .await
        .unwrap();

    let fixture = start_server().await;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let proj_dir = &fixture.project.dir;
    std::os::unix::fs::symlink(&outside_file, proj_dir.path().join("linked.rs")).unwrap();

    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/projects/{pid}/threads/{tid}/linkify"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"text": "linked.rs exists but points outside"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["links"].as_array().unwrap().is_empty(),
        "symlink escape must not become a browser link"
    );
}

#[tokio::test]
async fn highlight_rejects_path_escape() {
    let fixture = start_server().await;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "{base}/api/projects/{pid}/threads/{tid}/highlight?path=../../etc/passwd"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 403);
}

#[tokio::test]
async fn highlight_and_raw_reject_missing_files() {
    let fixture = start_server().await;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    for endpoint in ["highlight", "raw", "image"] {
        let resp = client
            .get(format!(
                "{base}/api/projects/{pid}/threads/{tid}/{endpoint}?path=missing.rs"
            ))
            .header("cookie", &cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            403,
            "{endpoint} should fail closed for missing files"
        );
    }
}

/// The root a thread works in is not always the directory its project lives in: `workspace_root`
/// (§6.3) moves the harness sandbox boundary — to a subdirectory, to narrow the agent's write
/// scope, or elsewhere entirely — and every thread in that project then works there. Reads have to
/// follow it.
///
/// Worth pinning because the resolution now has one home, shared by the file endpoints and the plan
/// write: a regression there moves all of them at once, and silently, since both directories exist
/// and both are readable.
#[tokio::test]
async fn thread_reads_follow_the_configured_workspace_root() {
    let fixture = start_server().await;
    let state = &fixture.server.state;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let proj_dir = &fixture.project.dir;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    // The same relative path in both trees, holding different text: reading the wrong root succeeds
    // and returns the wrong file, which is the failure this guards.
    let elsewhere = tempfile::TempDir::new().unwrap();
    tokio::fs::write(
        elsewhere.path().join("main.rs"),
        "fn from_workspace_root() {}\n",
    )
    .await
    .unwrap();
    tokio::fs::write(
        proj_dir.path().join("main.rs"),
        "fn from_project_dir() {}\n",
    )
    .await
    .unwrap();

    let mut project = state.store.load_project(pid).await.unwrap().unwrap();
    project.workspace_root = Some(elsewhere.path().to_string_lossy().into_owned());
    state.store.save_project(&project).await.unwrap();

    let raw = client
        .get(format!(
            "{base}/api/projects/{pid}/threads/{tid}/raw?path=main.rs"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert!(raw.status().is_success());
    assert_eq!(
        raw.text().await.unwrap(),
        "fn from_workspace_root() {}\n",
        "a read must come from the root the thread works in, not the directory its project lives in"
    );
}

/// The thread is in the route, so it is answered for or refused — never ignored. An id that does not
/// resolve within this project would otherwise be served from a workspace the caller never named.
#[tokio::test]
async fn code_overlay_endpoints_refuse_a_thread_they_cannot_resolve() {
    let fixture = start_server().await;
    let state = &fixture.server.state;
    let pid = fixture.pid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    // A thread of another project: `load_thread` is project-scoped, so it is unknown here — which is
    // also what stops one project's endpoints reading through another's workspace.
    let other_project = ProjectId::new();
    state
        .store
        .create_project(other_project, "other", "/tmp")
        .await
        .unwrap();
    let foreign = ThreadId::new();
    state
        .store
        .save_thread(other_project, &thread_file(other_project, foreign))
        .await
        .unwrap();

    for thread in [ThreadId::new(), foreign] {
        for (method, endpoint) in [
            ("GET", "highlight?path=main.rs"),
            ("GET", "raw?path=main.rs"),
            ("GET", "image?path=image.png"),
            ("POST", "linkify"),
            ("POST", "render"),
        ] {
            let url = format!("{base}/api/projects/{pid}/threads/{thread}/{endpoint}");
            let request = match method {
                "POST" => client
                    .post(url)
                    .json(&serde_json::json!({"text": "main.rs"})),
                _ => client.get(url),
            };
            let resp = request.header("cookie", &cookie).send().await.unwrap();
            assert_eq!(
                resp.status(),
                404,
                "{method} {endpoint} must refuse a thread it cannot resolve in this project"
            );
        }
    }
}

#[tokio::test]
async fn code_overlay_endpoints_return_not_found_for_missing_project() {
    let fixture = start_server().await;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let missing_project = ProjectId::new();

    for (method, endpoint) in [
        ("GET", "highlight?path=main.rs"),
        ("GET", "raw?path=main.rs"),
        ("GET", "image?path=image.png"),
        ("POST", "linkify"),
        ("POST", "render"),
    ] {
        let url = format!("{base}/api/projects/{missing_project}/threads/{tid}/{endpoint}");
        let request = match method {
            "POST" => client
                .post(url)
                .json(&serde_json::json!({"text": "main.rs"})),
            _ => client.get(url),
        };
        let resp = request.header("cookie", &cookie).send().await.unwrap();
        assert_eq!(
            resp.status(),
            404,
            "{method} {endpoint} should report missing project"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn highlight_and_raw_reject_symlink_escape() {
    let outside = tempfile::TempDir::new().unwrap();
    let outside_file = outside.path().join("outside.rs");
    tokio::fs::write(&outside_file, "pub fn outside() {}\n")
        .await
        .unwrap();

    let fixture = start_server().await;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let proj_dir = &fixture.project.dir;
    std::os::unix::fs::symlink(&outside_file, proj_dir.path().join("linked.rs")).unwrap();

    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    for endpoint in ["highlight", "raw", "image"] {
        let resp = client
            .get(format!(
                "{base}/api/projects/{pid}/threads/{tid}/{endpoint}?path=linked.rs"
            ))
            .header("cookie", &cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            403,
            "{endpoint} must reject symlinks that resolve outside the workspace"
        );
    }
}

/// Files exceeding the configured size threshold should return empty HTML
/// but still report `file_size` and `language` for the overlay metadata.
#[tokio::test]
async fn highlight_oversized_file_returns_metadata() {
    let fixture = start_server().await;
    let pid = fixture.pid;
    let tid = fixture.tid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let proj_dir = &fixture.project.dir;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let big_content = "x".repeat(20 * 1024 * 1024);
    tokio::fs::write(proj_dir.path().join("big.txt"), &big_content)
        .await
        .unwrap();

    let resp = client
        .get(format!(
            "{base}/api/projects/{pid}/threads/{tid}/highlight?path=big.txt"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(!body["is_binary"].as_bool().unwrap());
    assert!(body["html"].as_str().unwrap().is_empty());
    assert_eq!(body["file_size"].as_u64().unwrap(), 20 * 1024 * 1024);
    assert_eq!(body["language"].as_str().unwrap(), "txt");
}
