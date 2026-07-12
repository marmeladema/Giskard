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
                            "{}  {}  [{:?}]  {}",
                            tid,
                            thread.title,
                            thread.mode,
                            thread_archive_status(thread.archived)
                        );
                    }
                }
            }
        }
        "dump-thread" => {
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
            let store = giskard_persist::PersistStore::new(data_dir);
            store
                .delete_project(pid)
                .await
                .map_err(|e| format!("failed to delete project: {e}"))?;
            println!("Deleted project {pid}");
        }
        "validate" => {
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

Environment:
  GISKARD_DATA_DIR          Override the data directory (default: ~/.local/share/giskard)"
    );
}
