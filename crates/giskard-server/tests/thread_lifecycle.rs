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
                harness: "codex".into(),
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

/// A server declaring `stable` (the default) and `nightly`, both constructing replay harnesses,
/// with a project on the default.
async fn start_two_declaration_server() -> (TestServer, TestProject) {
    let server = TestServer::spawn(factory::with_catalog(
        factory::from_fn(|_, _| {
            Ok(std::sync::Arc::new(
                giskard_harness_replay::ReplayHarness::new(),
            ))
        }),
        factory::catalog(
            r#"
[harnesses.stable]
kind = "codex"
default = true

[harnesses.nightly]
kind = "codex"
"#,
        ),
    ))
    .await;
    let project = server.create_project("two-declarations").await;
    (server, project)
}

async fn start_thread(
    server: &TestServer,
    pid: ProjectId,
    harness: Option<&str>,
) -> serde_json::Value {
    let mut body = serde_json::json!({
        "text": "hello",
        "model_ref": {"provider": "openai", "model": "gpt-5.5", "reasoning_effort": null},
        "mode": "build",
        "permission_preset": "ask_first",
    });
    if let Some(harness) = harness {
        body["harness"] = harness.into();
    }
    let response = server
        .client
        .post(server.url(&format!("/api/projects/{pid}/threads/start")))
        .header("cookie", &server.cookie)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    response.json().await.unwrap()
}

#[tokio::test]
async fn a_thread_is_created_on_the_chosen_declaration_or_the_project_default() {
    let (server, project) = start_two_declaration_server().await;
    let pid = project.id;

    let nightly = start_thread(&server, pid, Some("nightly")).await;
    let default = start_thread(&server, pid, None).await;
    assert_eq!(nightly["harness"], "nightly", "{nightly}");
    assert_eq!(default["harness"], "stable", "{default}");

    let nightly_id: ThreadId = nightly["thread_id"].as_str().unwrap().parse().unwrap();
    let default_id: ThreadId = default["thread_id"].as_str().unwrap().parse().unwrap();
    let store = server.store();
    let nightly_file = store.load_thread(pid, nightly_id).await.unwrap().unwrap();
    let default_file = store.load_thread(pid, default_id).await.unwrap().unwrap();
    assert_eq!(nightly_file.harness, "nightly");
    assert_eq!(default_file.harness, "stable");

    let listed: serde_json::Value = server
        .client
        .get(server.url(&format!("/api/projects/{pid}/threads")))
        .header("cookie", &server.cookie)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let harness_of = |id: ThreadId| {
        listed["threads"]
            .as_array()
            .unwrap()
            .iter()
            .find(|thread| thread["id"] == id.to_string())
            .map(|thread| thread["harness"].clone())
    };
    assert_eq!(harness_of(nightly_id), Some("nightly".into()), "{listed}");
    assert_eq!(harness_of(default_id), Some("stable".into()), "{listed}");

    let opened: serde_json::Value = server
        .client
        .post(server.url(&format!("/api/projects/{pid}/threads")))
        .header("cookie", &server.cookie)
        .json(&serde_json::json!({"thread_id": nightly_id}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(opened["harness"], "nightly", "{opened}");
}
