//! Thread-lifecycle integration test: when the native harness cannot apply a rename/archive/delete
//! (e.g. it fails to attach), the HTTP operation surfaces an error and the locally persisted thread
//! is left intact rather than being partially mutated.

use std::sync::Arc;

use chrono::Utc;
use giskard_core::ids::{ProjectId, ThreadId};
use giskard_core::model::ModelRef;
use giskard_core::turn::{ApprovalPolicy, Mode};
use giskard_harness::AgentHarness;
use giskard_persist::store::{ProjectConfig, ThreadFile};
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
                parent_thread_id: None,
                spawned_by_turn_id: None,
                kind: giskard_core::ThreadKind::Primary,
                mode: Mode::Build,
                current_model: ModelRef {
                    provider: "openai".into(),
                    model: "gpt-5.5".into(),
                    reasoning_effort: None,
                },
                context_window: 262_144,
                model_context_windows: Default::default(),
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
