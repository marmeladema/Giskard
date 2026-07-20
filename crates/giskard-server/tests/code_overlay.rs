//! Code-overlay endpoint integration tests: syntax highlighting, raw file download, image preview,
//! path linkification, and server-side Markdown rendering (spec §11.2 / §11.3).

use std::sync::Arc;

use giskard_core::ids::ProjectId;
use giskard_harness::AgentHarness;
use giskard_persist::store::ProjectConfig;
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
