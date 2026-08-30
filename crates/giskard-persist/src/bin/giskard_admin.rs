//! giskard-admin — maintenance/debug CLI (spec §5.5).
//!
//! Commands:
//!   giskard-admin set-password              Generate an Argon2 hash for the app password.
//!   giskard-admin revoke-sessions           Rotate the session signing key (logs out everyone).
//!   giskard-admin list-projects             List all projects.
//!   giskard-admin list-threads <project>    List threads in a project.
//!   giskard-admin dump-thread <project> <thread>   Pretty-print thread metadata JSON.
//!   giskard-admin delete-thread <id>        Delete a thread.
//!   giskard-admin delete-project <id>       Delete a project.
//!   giskard-admin validate                  Validate all files, report corruption.
//!   giskard-admin migrate-storage           Migrate every thread to the current storage layout.
//!   giskard-admin prune-legacy              Delete retained pre-migration originals.
//!   giskard-admin sweep-orphan-payloads     Delete payload files no turn record references.
//!
//! Every command that changes the store holds an advisory lock on the data directory
//! (`<data_dir>/.giskard.lock`) and refuses while `giskard-server` — or another `giskard-admin`
//! run — holds it. `--dry-run` and read-only inspection take no lock and warn instead.

use std::path::PathBuf;

use argon2::PasswordHasher;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        usage(&args[0]);
        std::process::exit(1);
    }

    let data_dir = std::env::var("GISKARD_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            dirs_next::data_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join("giskard")
        });

    let cmd = &args[1];
    match cmd.as_str() {
        "set-password" => {
            let password =
                rpassword::prompt_password("Enter password: ").map_err(|e| e.to_string())?;
            let hash = argon2::Argon2::default()
                .hash_password(
                    password.as_bytes(),
                    &argon2::password_hash::SaltString::generate(&mut rand::rngs::OsRng),
                )
                .map_err(|e| format!("hashing failed: {e}"))?
                .to_string();
            println!("{hash}");
            println!("\nAdd this to config.toml under [auth]: password_hash = \"{hash}\"");
        }
        "revoke-sessions" => {
            revoke_sessions(&data_dir)?;
        }
        "list-projects" => {
            let store = giskard_persist::PersistStore::new(data_dir);
            let index = store
                .load_project_index()
                .await
                .map_err(|e| format!("failed to load index: {e}"))?;
            if index.projects.is_empty() {
                println!("No projects.");
            } else {
                for p in &index.projects {
                    println!("{}  {}  {}", p.id, p.name, p.dir);
                }
            }
        }
        "list-threads" => {
            warn_if_locked(&data_dir);
            let pid = expect_arg(&args, 2, "project id");
            let pid = parse_project_id(pid)?;
            let store = giskard_persist::PersistStore::new(data_dir);
            let threads = store
                .list_threads(pid)
                .await
                .map_err(|e| format!("failed to list threads: {e}"))?;
            if threads.is_empty() {
                println!("No threads.");
            } else {
                for tid in threads {
                    if let Some(thread) = store
                        .load_thread(pid, tid)
                        .await
                        .map_err(|e| format!("failed to load thread {tid}: {e}"))?
                    {
                        println!(
                            "{}  {}  [{}]  {}",
                            tid,
                            thread.title,
                            match thread.mode.as_known() {
                                Some(giskard_core::turn::Mode::Plan) => "Plan",
                                Some(giskard_core::turn::Mode::Build) => "Build",
                                None => "Unknown",
                            },
                            thread_archive_status(thread.archived)
                        );
                    }
                }
            }
        }
        "dump-thread" => {
            warn_if_locked(&data_dir);
            let pid = expect_arg(&args, 2, "project id");
            let tid = expect_arg(&args, 3, "thread id");
            let pid = parse_project_id(pid)?;
            let tid = parse_thread_id(tid)?;
            let store = giskard_persist::PersistStore::new(data_dir);
            match store.load_thread(pid, tid).await {
                Ok(Some(thread)) => {
                    let json = serde_json::to_string_pretty(&thread)
                        .map_err(|e| format!("failed to serialize thread: {e}"))?;
                    println!("{json}");
                }
                Ok(None) => {
                    eprintln!("Thread not found.");
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
        }
        "delete-thread" => {
            let pid = expect_arg(&args, 2, "project id");
            let tid = expect_arg(&args, 3, "thread id");
            let pid = parse_project_id(pid)?;
            let tid = parse_thread_id(tid)?;
            let _lock = lock_data_dir(&data_dir)?;
            let store = giskard_persist::PersistStore::new(data_dir);
            store
                .delete_thread(pid, tid)
                .await
                .map_err(|e| format!("failed to delete thread: {e}"))?;
            println!("Deleted thread {tid}");
        }
        "delete-project" => {
            let pid = expect_arg(&args, 2, "project id");
            let pid = parse_project_id(pid)?;
            let _lock = lock_data_dir(&data_dir)?;
            let store = giskard_persist::PersistStore::new(data_dir);
            store
                .delete_project(pid)
                .await
                .map_err(|e| format!("failed to delete project: {e}"))?;
            println!("Deleted project {pid}");
        }
        "migrate-storage" => {
            let dry_run = has_flag(&args, "--dry-run");
            let _lock = if dry_run {
                warn_if_locked(&data_dir);
                None
            } else {
                Some(lock_data_dir(&data_dir)?)
            };
            let store = giskard_persist::PersistStore::new(data_dir);
            let mut migrated = 0usize;
            let mut failed = 0usize;
            for (pid, tid) in all_threads(&store).await? {
                // A dry run previews through the migration's own classifier rather than a
                // reimplementation of it, so the two cannot disagree about what a thread needs.
                let outcome = if dry_run {
                    Ok(store.planned_migration(pid, tid).await)
                } else {
                    store.migrate_thread_layout(pid, tid).await
                };
                match outcome {
                    Ok(giskard_persist::MigrationOutcome::Migrated) => {
                        println!(
                            "{pid}  {tid}  {} (format 1)",
                            verb(dry_run, "migrate", "migrated")
                        );
                        migrated += 1;
                    }
                    Ok(giskard_persist::MigrationOutcome::FinishedLegacyMove) => {
                        println!(
                            "{pid}  {tid}  {} an interrupted migration",
                            verb(dry_run, "finish", "finished")
                        );
                        migrated += 1;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("{pid}  {tid}  FAILED: {e}");
                        failed += 1;
                    }
                }
            }
            println!(
                "{} {migrated} thread(s); {failed} failed.",
                verb(dry_run, "migrate", "migrated")
            );
            if failed > 0 {
                std::process::exit(1);
            }
        }
        "prune-legacy" => {
            let dry_run = has_flag(&args, "--dry-run");
            let _lock = if dry_run {
                warn_if_locked(&data_dir);
                None
            } else {
                Some(lock_data_dir(&data_dir)?)
            };
            let store = giskard_persist::PersistStore::new(data_dir);
            let mut pruned = 0usize;
            let mut failed = 0usize;
            for (pid, tid) in all_threads(&store).await? {
                if !store.has_legacy_data(pid, tid).await {
                    continue;
                }
                if dry_run {
                    println!("{pid}  {tid}  would delete legacy/");
                    pruned += 1;
                    continue;
                }
                // Per-thread, like the other two bulk commands: one thread whose `legacy/` cannot
                // be removed must not leave every later thread unprocessed and its retained data
                // silently still there.
                match store.prune_legacy_data(pid, tid).await {
                    Ok(true) => {
                        println!("{pid}  {tid}  deleted legacy/");
                        pruned += 1;
                    }
                    Ok(false) => {}
                    Err(e) => {
                        eprintln!("{pid}  {tid}  FAILED: {e}");
                        failed += 1;
                    }
                }
            }
            println!(
                "{} retained pre-migration data for {pruned} thread(s); {failed} failed.",
                verb(dry_run, "delete", "deleted")
            );
            if failed > 0 {
                std::process::exit(1);
            }
        }
        "sweep-orphan-payloads" => {
            let dry_run = has_flag(&args, "--dry-run");
            let _lock = if dry_run {
                warn_if_locked(&data_dir);
                None
            } else {
                Some(lock_data_dir(&data_dir)?)
            };
            let store = giskard_persist::PersistStore::new(data_dir);
            let mut swept = 0usize;
            let mut refused = 0usize;
            for (pid, tid) in all_threads(&store).await? {
                // Reported per thread and never fatal to the run: the refusals this can raise mean
                // "this thread's index is missing, so its payloads may be the surviving history" —
                // a reason to leave that one alone, not to stop sweeping every other thread.
                match store.sweep_orphan_payloads(pid, tid, dry_run).await {
                    Ok(sweep) => match sweep.refusal {
                        // A dry run exists to show what a refusal is talking about, so it prints
                        // the list; a real run keeps its output to the reason and the count, and
                        // points at the dry run for the files themselves.
                        Some(reason) => {
                            if dry_run {
                                for path in &sweep.payloads {
                                    println!("{pid}  {tid}  {}", path.display());
                                }
                            }
                            eprintln!(
                                "{pid}  {tid}  SKIPPED: {reason} — inspect them with --dry-run"
                            );
                            refused += 1;
                        }
                        None => {
                            for path in &sweep.payloads {
                                println!("{pid}  {tid}  {}", path.display());
                            }
                            swept += sweep.payloads.len();
                        }
                    },
                    Err(e) => {
                        eprintln!("{pid}  {tid}  SKIPPED: {e}");
                        refused += 1;
                    }
                }
            }
            println!(
                "{} {swept} unreferenced payload file(s); skipped {refused} thread(s).",
                verb(dry_run, "delete", "deleted")
            );
            if refused > 0 {
                std::process::exit(1);
            }
        }
        "validate" => {
            warn_if_locked(&data_dir);
            let store = giskard_persist::PersistStore::new(data_dir);
            let errors = store.validate_all().await;
            if errors.is_empty() {
                println!("All files valid.");
            } else {
                for (path, err) in &errors {
                    eprintln!("{}: {}", path.display(), err);
                }
                std::process::exit(1);
            }
        }
        // Takes the lock and waits to be killed. Exists so a test can prove the kernel releases
        // the lock when a holder dies abnormally — the property that makes this preferable to a
        // pidfile. Undocumented in `usage` because it is not an operator command.
        "hold-lock-for-tests" => {
            let _lock = lock_data_dir(&data_dir)?;
            println!("locked");
            use std::io::Write;
            std::io::stdout().flush().map_err(|e| e.to_string())?;
            std::thread::sleep(std::time::Duration::from_secs(600));
        }
        "help" | "--help" | "-h" => {
            usage(&args[0]);
        }
        _ => {
            eprintln!("Unknown command: {cmd}\n");
            usage(&args[0]);
            std::process::exit(1);
        }
    }
    Ok(())
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

/// Take the data-directory lock for a command that is about to change the store.
///
/// `giskard-admin` is a separate binary from `giskard-server`, so the store's per-thread `Mutex`es
/// order nothing between them. Every command that rewrites or deletes has to hold this instead, and
/// fail rather than proceed alongside a running server. The returned guard must outlive the work:
/// dropping it releases the lock.
fn lock_data_dir(data_dir: &std::path::Path) -> Result<giskard_persist::DataDirLock, String> {
    match giskard_persist::DataDirLock::try_acquire(data_dir) {
        Ok(Some(lock)) => Ok(lock),
        Ok(None) => Err(format!(
            "another Giskard process is using {} — stop giskard-server (or the other \
             giskard-admin run) and try again. Read-only commands and --dry-run work either way.",
            data_dir.display()
        )),
        Err(e) => Err(format!("cannot lock {}: {e}", data_dir.display())),
    }
}

/// Warn that a read-only listing may be racing a live writer.
///
/// A momentary probe, so it is already stale when it returns — which is fine for a warning and
/// would not be fine for a decision. Anything destructive holds the lock instead.
fn warn_if_locked(data_dir: &std::path::Path) {
    if giskard_persist::DataDirLock::is_held(data_dir) {
        eprintln!(
            "warning: another Giskard process is using {} — this listing may be out of date \
             before it finishes printing.",
            data_dir.display()
        );
    }
}

/// `"would migrate"` / `"migrated"`, so a dry run's lines read as the plan they are.
///
/// Both tenses are spelled out rather than derived: not every verb here takes a bare `-d`.
fn verb(dry_run: bool, present: &str, past: &str) -> String {
    if dry_run {
        format!("would {present}")
    } else {
        past.to_string()
    }
}

/// Every `(project, thread)` pair in the store, for the bulk maintenance commands.
async fn all_threads(
    store: &giskard_persist::PersistStore,
) -> Result<Vec<(giskard_core::ProjectId, giskard_core::ThreadId)>, String> {
    let index = store
        .load_project_index()
        .await
        .map_err(|e| format!("failed to load index: {e}"))?;
    let mut pairs = vec![];
    for project in &index.projects {
        let threads = store
            .list_threads(project.id)
            .await
            .map_err(|e| format!("failed to list threads for {}: {e}", project.id))?;
        pairs.extend(threads.into_iter().map(|tid| (project.id, tid)));
    }
    Ok(pairs)
}

/// Rotate `session.key`: sessions are stateless HMAC tokens, so replacing the signing key is the
/// only way to invalidate every outstanding browser session (and WebSocket ticket) at once —
/// e.g. after losing a logged-in device. The running server keeps the old key in memory, so it
/// must be restarted to pick up the new one.
fn revoke_sessions(data_dir: &std::path::Path) -> Result<(), String> {
    use rand::RngCore;
    use std::os::unix::fs::PermissionsExt;

    let key_path = data_dir.join("session.key");
    let mut key = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut key);
    std::fs::create_dir_all(data_dir)
        .map_err(|e| format!("cannot create data dir {}: {e}", data_dir.display()))?;
    std::fs::write(&key_path, key)
        .map_err(|e| format!("cannot write {}: {e}", key_path.display()))?;
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| format!("cannot set permissions on {}: {e}", key_path.display()))?;
    println!("Rotated session signing key at {}.", key_path.display());
    println!("All existing sessions are now invalid. Restart giskard-server to load the new key.");
    Ok(())
}

fn expect_arg<'a>(args: &'a [String], idx: usize, name: &str) -> &'a str {
    args.get(idx).unwrap_or_else(|| {
        eprintln!("Missing {name} argument");
        usage(&args[0]);
        std::process::exit(1);
    })
}

fn parse_project_id(raw: &str) -> Result<giskard_core::ProjectId, String> {
    raw.parse::<ulid::Ulid>()
        .map(giskard_core::ProjectId)
        .map_err(|e| format!("invalid project id {raw}: {e}"))
}

fn parse_thread_id(raw: &str) -> Result<giskard_core::ThreadId, String> {
    raw.parse::<ulid::Ulid>()
        .map(giskard_core::ThreadId)
        .map_err(|e| format!("invalid thread id {raw}: {e}"))
}

fn thread_archive_status(archived: bool) -> &'static str {
    if archived { "archived" } else { "active" }
}

fn usage(prog: &str) {
    eprintln!(
        "Usage: {prog} <command> [args]

Commands:
  set-password              Generate an Argon2 hash for the app password
  revoke-sessions           Rotate the session signing key, invalidating all sessions
                            (restart giskard-server afterwards)
  list-projects             List all projects
  list-threads <project>    List threads in a project
  dump-thread <project> <thread>   Pretty-print thread metadata JSON
  delete-thread <project> <thread> Delete a thread
  delete-project <project>         Delete a project
  validate                  Validate all files, report corruption
  migrate-storage [--dry-run]      Migrate every thread from the flat <id>.json/<id>.jsonl pair
                            to the <id>/ directory layout (index + per-turn payload files).
                            Runs automatically per thread on open; this only does it in bulk.
                            Never deletes: the originals are retained under <id>/legacy/.
  prune-legacy [--dry-run]  Delete the retained pre-migration originals under <id>/legacy/.
                            This destroys transcript history — run it only once the new layout
                            is trusted.
  sweep-orphan-payloads [--dry-run]
                            Delete turn payload files no history record references. Such files are
                            invisible to every read path, so this only
                            reclaims space. Skips (and exits non-zero for) any thread whose index
                            is missing, references no turns, or would have more than a handful
                            swept at once: a large sweep set is evidence of a truncated index
                            beside recoverable payloads, not of orphaned commits. --dry-run reports
                            those refusals too, and lists the files each one is about.

Commands that change the store (delete-thread, delete-project, migrate-storage, prune-legacy,
sweep-orphan-payloads) hold an advisory lock on the data directory and refuse while giskard-server
is running — stop it first. --dry-run and read-only commands work either way, and warn that what
they print may already be stale.

Environment:
  GISKARD_DATA_DIR          Override the data directory (default: ~/.local/share/giskard)"
    );
}
