//! Thread-lifecycle integration test: when the native harness cannot apply a rename/archive/delete
//! (e.g. it fails to attach), the HTTP operation surfaces an error and the locally persisted thread
//! is left intact rather than being partially mutated.

use chrono::Utc;
use giskard_core::HarnessError;
use giskard_core::ids::{ProjectId, ThreadId};
use giskard_core::model::ModelRef;
use giskard_core::turn::{Mode, PermissionPreset};
use giskard_persist::store::ThreadFile;
use giskard_testenv::{TestProject, TestServer, factory, fixtures};

struct Fixture {
    server: TestServer,
    _project: TestProject,
    pid: ProjectId,
}

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
    tokio::fs::write(project.dir.path().join("image.png"), fixtures::TINY_PNG)
        .await
        .unwrap();
    tokio::fs::write(
        project.dir.path().join("vector.svg"),
        r#"<svg xmlns="http://www.w3.org/2000/svg"></svg>"#,
    )
    .await
    .unwrap();

    Fixture {
        server,
        _project: project,
        pid,
    }
}

#[tokio::test]
async fn thread_lifecycle_native_failure_preserves_local_thread() {
    let fixture = start_server().await;
    let state = &fixture.server.state;
    let pid = fixture.pid;
    let cookie = fixture.server.cookie.clone();
    let port = fixture.server.addr.port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();

    let tid = ThreadId::new();
    let now = Utc::now();
    state
        .store
        .save_thread(
            pid,
            &ThreadFile {
                revision: 0,
                version: 1,
                id: tid,
                project_id: pid,
                title: "Local thread".into(),
                harness_thread_id: "native-thread".into(),
                parent_thread_id: None,
                spawned_by_turn_id: None,
                kind: giskard_core::ThreadKind::Primary,
                mode: giskard_core::turn::TurnMode::Known(Mode::Build),
                current_model: giskard_core::turn::TurnModel::Known(ModelRef {
                    provider: "openai".into(),
                    model: "gpt-5.5".into(),
                    reasoning_effort: None,
                }),
                context_window: 262_144,
                model_context_windows: Default::default(),
                permission_preset: PermissionPreset::AskFirst,
                model_efforts: Default::default(),
                tokens: Default::default(),
                created_at: now,
                updated_at: now,
                archived: false,
                git_workspace: None,
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
