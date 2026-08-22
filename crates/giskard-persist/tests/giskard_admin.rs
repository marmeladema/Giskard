use std::collections::HashMap;
use std::process::Command;

use chrono::Utc;
use giskard_core::ids::{ProjectId, ThreadId};
use giskard_core::model::ModelRef;
use giskard_core::thread::ThreadKind;
use giskard_core::token::TokenLedger;
use giskard_core::turn::{Mode, PermissionPreset};
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
        revision: 0,
        version: 1,
        id: thread_id,
        project_id,
        title: title.into(),
        harness_thread_id: format!("harness-{thread_id}"),
        parent_thread_id: None,
        spawned_by_turn_id: None,
        kind: ThreadKind::Primary,
        mode,
        current_model: test_model(),
        context_window: 262_144,
        model_context_windows: Default::default(),
        permission_preset: PermissionPreset::AskFirst,
        model_efforts: HashMap::new(),
        tokens: TokenLedger::default(),
        created_at: now,
        updated_at: now,
        archived,
        git_workspace: None,
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
        .create_project(project_id, "proj", "/tmp/proj")
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

/// `migrate-storage` and `prune-legacy` operate on the storage layout in bulk: the migration is
/// non-destructive on its own, and deleting the retained originals is the separate, explicit step.
#[tokio::test]
async fn migrate_storage_converts_flat_threads_and_prune_legacy_removes_the_originals() {
    use giskard_core::turn::{Turn, TurnStatus, TurnStatusKind};

    let tmp = tempfile::TempDir::new().unwrap();
    let store = PersistStore::new(tmp.path().to_path_buf());
    let project_id = ProjectId::new();
    let thread_id = ThreadId::new();
    store
        .create_project(project_id, "proj", "/tmp/proj")
        .await
        .unwrap();

    // Lay the thread out the way the store did before the per-turn payload split.
    let now = Utc::now();
    let turn = Turn {
        id: giskard_core::ids::TurnId::new(),
        user_input: giskard_core::user_input::UserInput::text("migrate me"),
        items: vec![],
        model: test_model(),
        mode: Mode::Build,
        status: TurnStatus {
            kind: TurnStatusKind::Completed,
            message: None,
        },
        usage: giskard_core::token::TokenUsage::new(10, 1),
        diffs: vec![],
        started_at: now,
        completed_at: Some(now),
    };
    let threads_dir = tmp
        .path()
        .join("projects")
        .join(project_id.to_string())
        .join("threads");
    std::fs::create_dir_all(&threads_dir).unwrap();
    std::fs::write(
        threads_dir.join(format!("{thread_id}.json")),
        serde_json::to_vec_pretty(&test_thread(
            project_id,
            thread_id,
            "Flat thread",
            Mode::Build,
            false,
        ))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(
        threads_dir.join(format!("{thread_id}.jsonl")),
        format!("{}\n", serde_json::to_string(&turn).unwrap()),
    )
    .unwrap();

    let admin = |args: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_giskard-admin"))
            .env("GISKARD_DATA_DIR", tmp.path())
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    };

    let dry_run = admin(&["migrate-storage", "--dry-run"]);
    assert!(dry_run.contains("would migrate (format 1)"), "{dry_run}");
    assert!(
        threads_dir.join(format!("{thread_id}.json")).exists(),
        "a dry run changes nothing"
    );

    // A thread caught between the commit rename and the legacy move still has work to do, and the
    // dry run has to say so — a bare format check reads it as already current.
    let interrupted = ThreadId::new();
    std::fs::create_dir_all(threads_dir.join(interrupted.to_string())).unwrap();
    std::fs::write(
        threads_dir
            .join(interrupted.to_string())
            .join("history.jsonl"),
        format!(
            "{}\n",
            serde_json::json!({
                "kind": "history_header",
                "format": 2,
                "thread_id": interrupted.to_string(),
                "created_at": now,
            })
        ),
    )
    .unwrap();
    std::fs::write(
        threads_dir.join(format!("{interrupted}.json")),
        serde_json::to_vec_pretty(&test_thread(
            project_id,
            interrupted,
            "Interrupted",
            Mode::Build,
            false,
        ))
        .unwrap(),
    )
    .unwrap();
    let dry_run = admin(&["migrate-storage", "--dry-run"]);
    assert!(
        dry_run.contains(&format!(
            "{interrupted}  would finish an interrupted migration"
        )),
        "{dry_run}"
    );
    assert!(
        threads_dir.join(format!("{interrupted}.json")).exists(),
        "a dry run changes nothing"
    );
    assert!(dry_run.contains("would migrate 2 thread(s)"), "{dry_run}");

    let migrated = admin(&["migrate-storage"]);
    assert!(
        migrated.contains(&format!("{thread_id}  migrated")),
        "{migrated}"
    );
    assert!(
        migrated.contains(&format!("{interrupted}  finished an interrupted migration")),
        "{migrated}"
    );
    assert!(
        !threads_dir.join(format!("{interrupted}.json")).exists(),
        "the interrupted relocation is finished"
    );
    let thread_dir = threads_dir.join(thread_id.to_string());
    assert!(thread_dir.join("history.jsonl").exists());
    assert!(
        thread_dir
            .join("turns")
            .join(format!("{}.jsonl", turn.id))
            .exists()
    );
    assert!(!threads_dir.join(format!("{thread_id}.json")).exists());

    // Non-destructive: the originals are retained, and `validate` is happy with the new layout.
    assert!(thread_dir.join("legacy").join("history.jsonl").exists());
    assert!(admin(&["validate"]).contains("All files valid."));
    assert!(admin(&["migrate-storage"]).contains("migrated 0 thread(s)"));
    assert!(
        admin(&["migrate-storage", "--dry-run"]).contains("would migrate 0 thread(s)"),
        "the plan agrees with the run once there is nothing left to do"
    );

    // Reading the migrated thread yields the same turn.
    let loaded = store.load_all_turns(project_id, thread_id).await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].id, turn.id);
    assert_eq!(loaded[0].usage, turn.usage);

    let pruned = admin(&["prune-legacy"]);
    assert!(
        pruned.contains(&format!("{thread_id}  deleted legacy/")),
        "{pruned}"
    );
    assert!(!thread_dir.join("legacy").exists());
    assert_eq!(
        store
            .load_all_turns(project_id, thread_id)
            .await
            .unwrap()
            .len(),
        1,
        "pruning the originals must not touch the migrated history"
    );

    // Migration references every payload it wrote, so it leaves no orphan behind.
    assert!(admin(&["sweep-orphan-payloads", "--dry-run"]).contains("would delete 0"));
}

/// A thread whose index is missing means "these payloads may be the surviving history" — a reason
/// to leave that one alone, not to abandon the sweep of every other thread.
#[tokio::test]
async fn sweep_orphan_payloads_skips_a_thread_it_cannot_judge_and_keeps_going() {
    let tmp = tempfile::TempDir::new().unwrap();
    let store = PersistStore::new(tmp.path().to_path_buf());
    let project_id = ProjectId::new();
    store
        .create_project(project_id, "proj", "/tmp/proj")
        .await
        .unwrap();

    let healthy = ThreadId::new();
    let indexless = ThreadId::new();
    for tid in [healthy, indexless] {
        store
            .create_thread(
                project_id,
                test_thread(project_id, tid, "t", Mode::Build, false),
            )
            .await
            .unwrap();
    }
    let threads_dir = tmp
        .path()
        .join("projects")
        .join(project_id.to_string())
        .join("threads");
    let payload = threads_dir
        .join(indexless.to_string())
        .join("turns")
        .join(format!("{}.jsonl", giskard_core::ids::TurnId::new()));
    std::fs::create_dir_all(payload.parent().unwrap()).unwrap();
    std::fs::write(&payload, b"{\"kind\":\"turn_header\",\"format\":1}\n").unwrap();
    std::fs::remove_file(
        threads_dir
            .join(indexless.to_string())
            .join("history.jsonl"),
    )
    .unwrap();

    let sweep = |args: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_giskard-admin"))
            .env("GISKARD_DATA_DIR", tmp.path())
            .args(args)
            .output()
            .unwrap();
        (
            String::from_utf8(output.stdout).unwrap(),
            String::from_utf8(output.stderr).unwrap(),
            output.status,
        )
    };

    // The refusal tells the operator to inspect the files with --dry-run, so --dry-run has to be
    // the one run that still works: it names the refusal *and* lists what the refusal is about.
    let (stdout, stderr, status) = sweep(&["sweep-orphan-payloads", "--dry-run"]);
    assert!(
        stderr.contains(&format!("{indexless}  SKIPPED")),
        "stderr: {stderr}"
    );
    assert!(
        stdout.contains(&payload.display().to_string()),
        "a dry run must show the files the refusal names: {stdout}"
    );
    assert!(!status.success(), "a refusal still reaches the exit code");
    assert!(payload.exists(), "and a dry run deletes nothing");

    let (stdout, stderr, status) = sweep(&["sweep-orphan-payloads"]);
    assert!(
        stderr.contains(&format!("{indexless}  SKIPPED")),
        "stderr: {stderr}"
    );
    assert!(
        stdout.contains("skipped 1 thread(s)") && stdout.contains("deleted 0"),
        "the healthy thread was still visited: {stdout}"
    );
    assert!(!status.success(), "a refusal has to reach the exit code");
    assert!(payload.exists(), "nothing was deleted");
    assert!(
        threads_dir
            .join(healthy.to_string())
            .join("history.jsonl")
            .exists(),
        "the healthy thread is untouched"
    );
}

/// Every destructive command must refuse while another Giskard process holds the data directory,
/// and change nothing — the store's per-thread locks are in-process `Mutex`es, so they order
/// nothing between `giskard-admin` and a running `giskard-server`.
#[tokio::test]
async fn destructive_commands_refuse_and_change_nothing_while_the_data_dir_is_locked() {
    let tmp = tempfile::TempDir::new().unwrap();
    let store = PersistStore::new(tmp.path().to_path_buf());
    let project_id = ProjectId::new();
    let thread_id = ThreadId::new();
    store
        .create_project(project_id, "proj", "/tmp/proj")
        .await
        .unwrap();
    store
        .create_thread(
            project_id,
            test_thread(project_id, thread_id, "t", Mode::Build, false),
        )
        .await
        .unwrap();
    let thread_dir = tmp
        .path()
        .join("projects")
        .join(project_id.to_string())
        .join("threads")
        .join(thread_id.to_string());
    assert!(thread_dir.exists());

    let run = |args: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_giskard-admin"))
            .env("GISKARD_DATA_DIR", tmp.path())
            .args(args)
            .output()
            .unwrap();
        (
            String::from_utf8(output.stdout).unwrap(),
            String::from_utf8(output.stderr).unwrap(),
            output.status,
        )
    };

    let held = giskard_persist::DataDirLock::try_acquire(tmp.path())
        .unwrap()
        .expect("stand in for a running giskard-server");

    for args in [
        vec!["migrate-storage"],
        vec!["prune-legacy"],
        vec!["sweep-orphan-payloads"],
        vec![
            "delete-thread",
            &project_id.to_string(),
            &thread_id.to_string(),
        ],
        vec!["delete-project", &project_id.to_string()],
    ] {
        let (_, stderr, status) = run(&args);
        assert!(!status.success(), "{args:?} must refuse: {stderr}");
        assert!(
            stderr.contains("another Giskard process is using"),
            "{args:?} stderr: {stderr}"
        );
    }
    assert!(
        thread_dir.exists(),
        "a refused command must not have touched the store"
    );

    // A dry run takes no lock: it is the path an operator reaches for while the server is up, so it
    // has to work — and say that what it printed may already be stale.
    for args in [
        vec!["migrate-storage", "--dry-run"],
        vec!["prune-legacy", "--dry-run"],
        vec!["sweep-orphan-payloads", "--dry-run"],
    ] {
        let (_, stderr, status) = run(&args);
        assert!(status.success(), "{args:?} must succeed: {stderr}");
        assert!(
            stderr.contains("warning: another Giskard process is using"),
            "{args:?} must warn that its listing may be racy: {stderr}"
        );
    }
    // Read-only inspection is the same: allowed, warned about.
    let (stdout, stderr, status) = run(&["list-threads", &project_id.to_string()]);
    assert!(status.success(), "stderr: {stderr}");
    assert!(stdout.contains(&thread_id.to_string()), "stdout: {stdout}");
    assert!(
        stderr.contains("warning: another Giskard process is using"),
        "stderr: {stderr}"
    );

    // Once the holder is gone, the same command works.
    drop(held);
    let (_, stderr, status) = run(&["migrate-storage"]);
    assert!(status.success(), "stderr: {stderr}");
}

/// The kernel releases the lock when the holder dies, so a crashed server leaves nothing stale.
/// This is the property that makes a lock file preferable to a pidfile, which would have needed
/// liveness checks and PID-reuse handling to answer the same question.
#[test]
fn a_killed_holder_leaves_no_stale_lock() {
    let tmp = tempfile::TempDir::new().unwrap();

    // A child that takes the lock, says so, and then waits to be killed.
    let mut child = Command::new(env!("CARGO_BIN_EXE_giskard-admin"))
        .env("GISKARD_DATA_DIR", tmp.path())
        .arg("hold-lock-for-tests")
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut ready = String::new();
    {
        use std::io::{BufRead, BufReader};
        let stdout = child.stdout.as_mut().unwrap();
        BufReader::new(stdout).read_line(&mut ready).unwrap();
    }
    assert!(
        ready.contains("locked"),
        "child did not take the lock: {ready}"
    );
    assert!(
        giskard_persist::DataDirLock::try_acquire(tmp.path())
            .unwrap()
            .is_none(),
        "the child holds it"
    );

    child.kill().unwrap();
    child.wait().unwrap();

    assert!(
        giskard_persist::DataDirLock::try_acquire(tmp.path())
            .unwrap()
            .is_some(),
        "an abnormally terminated holder must not leave the directory locked"
    );
}

/// The sweep has no age condition: with the data directory locked there is no in-flight commit an
/// unreferenced payload could belong to, so a freshly written orphan is swept on sight. Pinned at
/// the command level because that is where the removed 24h threshold used to be described.
#[tokio::test]
async fn sweep_orphan_payloads_deletes_a_freshly_written_orphan() {
    let tmp = tempfile::TempDir::new().unwrap();
    let store = PersistStore::new(tmp.path().to_path_buf());
    let project_id = ProjectId::new();
    let thread_id = ThreadId::new();
    store
        .create_project(project_id, "proj", "/tmp/proj")
        .await
        .unwrap();
    store
        .create_thread(
            project_id,
            test_thread(project_id, thread_id, "t", Mode::Build, false),
        )
        .await
        .unwrap();

    // One committed turn, so the index references something and the guards stay quiet.
    let now = Utc::now();
    let committed = giskard_core::turn::Turn {
        id: giskard_core::ids::TurnId::new(),
        user_input: giskard_core::user_input::UserInput::text("kept"),
        items: vec![],
        model: test_model(),
        mode: Mode::Build,
        status: giskard_core::turn::TurnStatus {
            kind: giskard_core::turn::TurnStatusKind::Completed,
            message: None,
        },
        usage: giskard_core::token::TokenUsage::new(1, 1),
        diffs: vec![],
        started_at: now,
        completed_at: Some(now),
    };
    store
        .append_turn(project_id, thread_id, &committed)
        .await
        .unwrap();

    // A payload no turn record references, written just now.
    let turns_dir = tmp
        .path()
        .join("projects")
        .join(project_id.to_string())
        .join("threads")
        .join(thread_id.to_string())
        .join("turns");
    let orphan = turns_dir.join(format!("{}.jsonl", giskard_core::ids::TurnId::new()));
    std::fs::write(&orphan, b"{\"kind\":\"turn_header\",\"format\":1}\n").unwrap();

    let run = |args: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_giskard-admin"))
            .env("GISKARD_DATA_DIR", tmp.path())
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    };

    let previewed = run(&["sweep-orphan-payloads", "--dry-run"]);
    assert!(
        previewed.contains(&orphan.display().to_string()),
        "a fresh orphan is a candidate, with no waiting period: {previewed}"
    );
    assert!(orphan.exists(), "a dry run deletes nothing");

    let swept = run(&["sweep-orphan-payloads"]);
    assert!(
        swept.contains("deleted 1 unreferenced payload file(s)"),
        "{swept}"
    );
    assert!(!orphan.exists());
    assert!(
        turns_dir.join(format!("{}.jsonl", committed.id)).exists(),
        "the referenced payload is untouched"
    );
}
