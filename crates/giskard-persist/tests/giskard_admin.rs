use std::collections::HashMap;
use std::process::Command;

use chrono::Utc;
use giskard_core::ids::{ProjectId, ThreadId};
use giskard_core::model::ModelRef;
use giskard_core::token::TokenLedger;
use giskard_core::turn::{ApprovalPolicy, Mode};
use giskard_persist::PersistStore;
use giskard_persist::store::ThreadFile;

fn test_model() -> ModelRef {
    ModelRef {
        provider: "openai".into(),
        model: "gpt-5.5".into(),
        reasoning_effort: None,
    }
}

fn test_thread(
    project_id: ProjectId,
    thread_id: ThreadId,
    title: &str,
    mode: Mode,
    archived: bool,
) -> ThreadFile {
    let now = Utc::now();
    ThreadFile {
        version: 1,
        id: thread_id,
        project_id,
        title: title.into(),
        harness_thread_id: format!("harness-{thread_id}"),
        mode,
        current_model: test_model(),
        context_window: 262_144,
        approval_policy: ApprovalPolicy::Ask,
        model_efforts: HashMap::new(),
        tokens: TokenLedger::default(),
        created_at: now,
        updated_at: now,
        archived,
    }
}

#[tokio::test]
async fn list_threads_prints_archived_status() {
    let tmp = tempfile::TempDir::new().unwrap();
    let store = PersistStore::new(tmp.path().to_path_buf());
    let project_id = ProjectId::new();
    let active_id = ThreadId::new();
    let archived_id = ThreadId::new();

    store
        .create_project(project_id, "proj", "/tmp/proj", test_model())
        .await
        .unwrap();
    store
        .save_thread(
            project_id,
            &test_thread(project_id, active_id, "Active thread", Mode::Build, false),
        )
        .await
        .unwrap();
    store
        .save_thread(
            project_id,
            &test_thread(project_id, archived_id, "Archived thread", Mode::Plan, true),
        )
        .await
        .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_giskard-admin"))
        .env("GISKARD_DATA_DIR", tmp.path())
        .arg("list-threads")
        .arg(project_id.to_string())
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains(&format!("{active_id}  Active thread  [Build]  active")),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains(&format!("{archived_id}  Archived thread  [Plan]  archived")),
        "stdout: {stdout}"
    );
}

#[test]
fn revoke_sessions_rotates_the_signing_key() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = tempfile::TempDir::new().unwrap();
    let key_path = tmp.path().join("session.key");
    let old_key = [7u8; 32];
    std::fs::write(&key_path, old_key).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_giskard-admin"))
        .env("GISKARD_DATA_DIR", tmp.path())
        .arg("revoke-sessions")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        stdout.contains("Restart giskard-server"),
        "stdout: {stdout}"
    );

    let new_key = std::fs::read(&key_path).unwrap();
    assert_eq!(new_key.len(), 32);
    assert_ne!(new_key.as_slice(), old_key.as_slice());
    let mode = std::fs::metadata(&key_path).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600, "session.key must be private");
}
