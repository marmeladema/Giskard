//! Phase 4 integration tests: syntax highlighting, file download, path linkification,
//! and diff accumulation.

use std::sync::Arc;

use chrono::Utc;
use futures_util::SinkExt;
use giskard_core::diff::{DiffHunk, DiffLine, FileDiff};
use giskard_core::event::AgentEvent;
use giskard_core::ids::{ItemId, ProjectId, ThreadId, TurnId};
use giskard_core::item::{FileChangeKind, Item, ItemKind, ItemPayload, ItemStart};
use giskard_core::model::ModelRef;
use giskard_core::token::TokenUsage;
use giskard_core::turn::{ApprovalPolicy, Mode, TurnStatus, TurnStatusKind};
use giskard_harness::AgentHarness;
use giskard_harness_replay::{ReplayFixture, ReplayHarness};
use giskard_persist::store::{ProjectConfig, ThreadFile};
use giskard_proto::ClientMessage;
use giskard_server::{AppState, HarnessFactory, build_app};

const TINY_PNG: &[u8] = &[
    0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, b'I', b'H', b'D', b'R',
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
    0x89, 0x00, 0x00, 0x00, 0x0a, b'I', b'D', b'A', b'T', 0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00,
    0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, b'I', b'E', b'N', b'D', 0xae,
    0x42, 0x60, 0x82,
];

struct DummyFactory;

#[async_trait::async_trait]
impl HarnessFactory for DummyFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn AgentHarness>, giskard_core::HarnessError> {
        Err(giskard_core::HarnessError::Spawn("dummy".into()))
    }
}

/// Harness factory that wraps a replay harness with a diff-containing fixture.
struct DiffFactory {
    fixture: ReplayFixture,
}

#[async_trait::async_trait]
impl HarnessFactory for DiffFactory {
    async fn create(
        &self,
        _config: &ProjectConfig,
    ) -> Result<Arc<dyn AgentHarness>, giskard_core::HarnessError> {
        Ok(Arc::new(ReplayHarness::from_fixture(self.fixture.clone())))
    }
}

/// Build a fixture that emits two `DiffUpdated` events for the same file
/// (simulating incremental diff updates) plus one for a second file.
fn make_diff_fixture() -> ReplayFixture {
    let thread = ThreadId::new();
    let turn = TurnId::new();
    let item = ItemId::new();
    let now = Utc::now();

    let diff1 = FileDiff {
        path: "src/main.rs".into(),
        change: FileChangeKind::Modified,
        old_text: Some("fn main() {}".into()),
        new_text: Some("fn main() {\n    println!(\"hi\");\n}".into()),
        hunks: vec![DiffHunk {
            old_start: 1,
            old_lines: 1,
            new_start: 1,
            new_lines: 3,
            lines: vec![
                DiffLine::Removed("fn main() {}".into()),
                DiffLine::Added("fn main() {".into()),
                DiffLine::Added("    println!(\"hi\");".into()),
                DiffLine::Added("}".into()),
            ],
        }],
        binary: false,
    };

    let diff2 = FileDiff {
        path: "src/main.rs".into(),
        change: FileChangeKind::Modified,
        old_text: Some("fn main() {\n    println!(\"hi\");\n}".into()),
        new_text: Some("fn main() {\n    println!(\"hello\");\n}".into()),
        hunks: vec![DiffHunk {
            old_start: 2,
            old_lines: 1,
            new_start: 2,
            new_lines: 1,
            lines: vec![
                DiffLine::Removed("    println!(\"hi\");".into()),
                DiffLine::Added("    println!(\"hello\");".into()),
            ],
        }],
        binary: false,
    };

    let diff3 = FileDiff {
        path: "src/lib.rs".into(),
        change: FileChangeKind::Created,
        old_text: None,
        new_text: Some("pub fn lib() {}".into()),
        hunks: vec![],
        binary: false,
    };

    ReplayFixture::from_events(vec![
        AgentEvent::ThreadOpened {
            thread,
            harness_thread_id: "th_diff".into(),
        },
        AgentEvent::TurnStarted { thread, turn },
        AgentEvent::ItemStarted {
            thread,
            turn,
            item: ItemStart {
                id: item,
                harness_item_id: "it_1".into(),
                kind: ItemKind::AgentMessage,
                command: None,
                tool: None,
            },
        },
        AgentEvent::DiffUpdated {
            thread,
            turn,
            diff: diff1,
        },
        AgentEvent::DiffUpdated {
            thread,
            turn,
            diff: diff2,
        },
        AgentEvent::DiffUpdated {
            thread,
            turn,
            diff: diff3,
        },
        AgentEvent::ItemCompleted {
            thread,
            turn,
            item: Item {
                id: item,
                harness_item_id: "it_1".into(),
                payload: ItemPayload::AgentMessage {
                    text: "Modified src/main.rs and created src/lib.rs".into(),
                },
                created_at: now,
            },
        },
        AgentEvent::TurnCompleted {
            thread,
            turn,
            usage: TokenUsage::new(200, 100),
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

async fn start_server(
    port: u16,
) -> (
    tempfile::TempDir,
    tempfile::TempDir,
    Arc<AppState>,
    ProjectId,
    String,
) {
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
"#
    );
    tokio::fs::write(tmp.path().join("config.toml"), config_toml)
        .await
        .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let session_key: Vec<u8> = (0..32u8).collect();
    let factory = Arc::new(DummyFactory);
    let state = AppState::new(store, factory, session_key);
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let proj_dir = tempfile::TempDir::new().unwrap();
    let proj_dir_path = proj_dir.path().to_string_lossy().to_string();
    let pid = ProjectId::new();
    state
        .store
        .create_project(
            pid,
            "viz-test",
            &proj_dir_path,
            giskard_core::model::ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
        )
        .await
        .unwrap();

    tokio::fs::write(
        proj_dir.path().join("main.rs"),
        "fn main() {\n    println!(\"hi\");\n}\n",
    )
    .await
    .unwrap();
    tokio::fs::write(proj_dir.path().join("data.bin"), b"bin\x00ary\x00data")
        .await
        .unwrap();
    tokio::fs::write(proj_dir.path().join("image.png"), TINY_PNG)
        .await
        .unwrap();
    tokio::fs::write(
        proj_dir.path().join("vector.svg"),
        r#"<svg xmlns="http://www.w3.org/2000/svg"></svg>"#,
    )
    .await
    .unwrap();

    let cookie = {
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://127.0.0.1:{port}/api/login"))
            .json(&serde_json::json!({"password": "testpass"}))
            .send()
            .await
            .unwrap();
        resp.headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string()
    };

    (tmp, proj_dir, Arc::new(state), pid, cookie)
}

#[tokio::test]
async fn highlight_rust_file() {
    let port = 19001;
    let (_data_dir, _proj_dir, _state, pid, cookie) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/api/projects/{pid}/highlight?path=main.rs"))
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
async fn highlight_binary_file() {
    let port = 19002;
    let (_data_dir, _proj_dir, _state, pid, cookie) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/api/projects/{pid}/highlight?path=data.bin"))
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
    let port = 19003;
    let (_data_dir, _proj_dir, _state, pid, cookie) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/api/projects/{pid}/raw?path=main.rs"))
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
    let port = 19027;
    let (_data_dir, _proj_dir, _state, pid, cookie) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/api/projects/{pid}/image?path=image.png"))
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
    assert_eq!(resp.bytes().await.unwrap().as_ref(), TINY_PNG);
}

#[tokio::test]
async fn image_preview_rejects_svg() {
    let port = 19028;
    let (_data_dir, _proj_dir, _state, pid, cookie) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{base}/api/projects/{pid}/image?path=vector.svg"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn linkify_finds_paths() {
    let port = 19004;
    let (_data_dir, _proj_dir, _state, pid, cookie) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/api/projects/{pid}/linkify"))
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
    let port = 19022;
    let (_data_dir, _proj_dir, _state, pid, cookie) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let text = "See `main.rs` and **open** main.rs now.\n\n```rust\nfn main() {}\n```\n\n<img src=x onerror=alert(1)>";
    let resp = client
        .post(format!("{base}/api/projects/{pid}/render"))
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
async fn thread_lifecycle_native_failure_preserves_local_thread() {
    let port = 19037;
    let (_data_dir, _proj_dir, state, pid, cookie) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let tid = ThreadId::new();
    let now = Utc::now();
    state
        .store
        .save_thread(
            pid,
            &ThreadFile {
                version: 1,
                id: tid,
                project_id: pid,
                title: "Local thread".into(),
                harness_thread_id: "native-thread".into(),
                mode: Mode::Build,
                current_model: ModelRef {
                    provider: "openai".into(),
                    model: "gpt-5.5".into(),
                    reasoning_effort: None,
                },
                context_window: 262_144,
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

    let rename = client
        .patch(format!("{base}/api/projects/{pid}/threads/{tid}/title"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"title": "Remote title"}))
        .send()
        .await
        .unwrap();
    assert_eq!(rename.status(), 500);
    let saved = state.store.load_thread(pid, tid).await.unwrap().unwrap();
    assert_eq!(saved.title, "Local thread");

    let archive = client
        .post(format!("{base}/api/projects/{pid}/threads/{tid}/archive"))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"archived": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(archive.status(), 500);
    let saved = state.store.load_thread(pid, tid).await.unwrap().unwrap();
    assert!(!saved.archived);

    let delete = client
        .delete(format!("{base}/api/projects/{pid}/threads/{tid}"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(delete.status(), 500);
    assert!(state.store.load_thread(pid, tid).await.unwrap().is_some());
}

#[tokio::test]
async fn linkify_endpoint_returns_only_existing_workspace_files() {
    let port = 19012;
    let (_data_dir, proj_dir, _state, pid, cookie) = start_server(port).await;
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
        .post(format!("{base}/api/projects/{pid}/linkify"))
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

#[cfg(unix)]
#[tokio::test]
async fn linkify_endpoint_rejects_symlink_escape() {
    let port = 19013;
    let outside = tempfile::TempDir::new().unwrap();
    let outside_file = outside.path().join("outside.rs");
    tokio::fs::write(&outside_file, "pub fn outside() {}\n")
        .await
        .unwrap();

    let (_data_dir, proj_dir, _state, pid, cookie) = start_server(port).await;
    std::os::unix::fs::symlink(&outside_file, proj_dir.path().join("linked.rs")).unwrap();

    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/projects/{pid}/linkify"))
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
    let port = 19005;
    let (_data_dir, _proj_dir, _state, pid, cookie) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let resp = client
        .get(format!(
            "{base}/api/projects/{pid}/highlight?path=../../etc/passwd"
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 403);
}

#[tokio::test]
async fn highlight_and_raw_reject_missing_files() {
    let port = 19015;
    let (_data_dir, _proj_dir, _state, pid, cookie) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    for endpoint in ["highlight", "raw", "image"] {
        let resp = client
            .get(format!(
                "{base}/api/projects/{pid}/{endpoint}?path=missing.rs"
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

#[tokio::test]
async fn code_overlay_endpoints_return_not_found_for_missing_project() {
    let port = 19016;
    let (_data_dir, _proj_dir, _state, _pid, cookie) = start_server(port).await;
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
        let url = format!("{base}/api/projects/{missing_project}/{endpoint}");
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
    let port = 19014;
    let outside = tempfile::TempDir::new().unwrap();
    let outside_file = outside.path().join("outside.rs");
    tokio::fs::write(&outside_file, "pub fn outside() {}\n")
        .await
        .unwrap();

    let (_data_dir, proj_dir, _state, pid, cookie) = start_server(port).await;
    std::os::unix::fs::symlink(&outside_file, proj_dir.path().join("linked.rs")).unwrap();

    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    for endpoint in ["highlight", "raw", "image"] {
        let resp = client
            .get(format!(
                "{base}/api/projects/{pid}/{endpoint}?path=linked.rs"
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

/// DiffUpdated events should be accumulated into Turn.diffs and persisted.
///
/// Two diffs for the same path (`src/main.rs`) should be deduplicated to the
/// most recent one, while the second file (`src/lib.rs`) should appear as a
/// separate entry.
#[tokio::test]
async fn diff_accumulation_persists_turn_diffs() {
    let port = 19010;
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
"#
    );
    tokio::fs::write(tmp.path().join("config.toml"), config_toml)
        .await
        .unwrap();

    let store = Arc::new(giskard_persist::PersistStore::new(tmp.path().to_path_buf()));
    let session_key: Vec<u8> = (0..32u8).collect();
    let factory = Arc::new(DiffFactory {
        fixture: make_diff_fixture(),
    });
    let state = AppState::new(store.clone(), factory, session_key);
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let proj_dir = tempfile::TempDir::new().unwrap();
    let proj_dir_path = proj_dir.path().to_string_lossy().to_string();
    let pid = ProjectId::new();
    state
        .store
        .create_project(
            pid,
            "diff-test",
            &proj_dir_path,
            ModelRef {
                provider: "openai".into(),
                model: "gpt-5.5".into(),
                reasoning_effort: None,
            },
        )
        .await
        .unwrap();

    let http_client = reqwest::Client::new();
    let cookie = {
        let resp = http_client
            .post(format!("http://127.0.0.1:{port}/api/login"))
            .json(&serde_json::json!({"password": "testpass"}))
            .send()
            .await
            .unwrap();
        resp.headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string()
    };

    let open_resp: serde_json::Value = http_client
        .post(format!(
            "http://127.0.0.1:{port}/api/projects/{pid}/threads"
        ))
        .header("cookie", &cookie)
        .json(&serde_json::json!({"resume": "th_diff"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let thread_id: ThreadId = serde_json::from_value(open_resp["thread_id"].clone()).unwrap();

    let ws_base = format!("ws://127.0.0.1:{port}");
    let ws_request = tokio_tungstenite::tungstenite::http::Request::builder()
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
            thread_id,
            since: None,
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&ClientMessage::SendInput {
            thread_id,
            text: "modify files".into(),
        })
        .unwrap()
        .into(),
    ))
    .await
    .unwrap();

    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    loop {
        if let Ok(turns) = state.store.load_all_turns(pid, thread_id).await {
            if !turns.is_empty() {
                let turn = &turns[0];
                assert_eq!(
                    turn.diffs.len(),
                    2,
                    "two distinct file paths should have diffs (dedup by path)"
                );

                let main_rs_diff = turn
                    .diffs
                    .iter()
                    .find(|d| d.path.to_string_lossy() == "src/main.rs")
                    .expect("src/main.rs diff should exist");
                assert_eq!(main_rs_diff.change, FileChangeKind::Modified);
                assert!(
                    main_rs_diff.new_text.as_ref().unwrap().contains("hello"),
                    "should contain the latest diff (hello, not hi)"
                );

                let lib_rs_diff = turn
                    .diffs
                    .iter()
                    .find(|d| d.path.to_string_lossy() == "src/lib.rs")
                    .expect("src/lib.rs diff should exist");
                assert_eq!(lib_rs_diff.change, FileChangeKind::Created);

                return;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("turn was not persisted within 10 seconds");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
}

/// Files exceeding the configured size threshold should return empty HTML
/// but still report `file_size` and `language` for the overlay metadata.
#[tokio::test]
async fn highlight_oversized_file_returns_metadata() {
    let port = 19011;
    let (_data_dir, proj_dir, _state, pid, cookie) = start_server(port).await;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let big_content = "x".repeat(20 * 1024 * 1024);
    tokio::fs::write(proj_dir.path().join("big.txt"), &big_content)
        .await
        .unwrap();

    let resp = client
        .get(format!("{base}/api/projects/{pid}/highlight?path=big.txt"))
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
