//! giskard-admin — maintenance/debug CLI (spec §5.5).
//!
//! Commands:
//!   giskard-admin set-password              Generate an Argon2 hash for the app password.
//!   giskard-admin list-projects             List all projects.
//!   giskard-admin list-threads <project>    List threads in a project.
//!   giskard-admin dump-thread <id>          Pretty-print a thread's JSON.
//!   giskard-admin delete-thread <id>        Delete a thread.
//!   giskard-admin delete-project <id>       Delete a project.
//!   giskard-admin validate                  Validate all files, report corruption.

use std::path::PathBuf;

use argon2::PasswordHasher;

#[tokio::main]
async fn main() {
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
                rpassword::prompt_password("Enter password: ").expect("failed to read password");
            let hash = argon2::Argon2::default()
                .hash_password(
                    password.as_bytes(),
                    &argon2::password_hash::SaltString::generate(&mut rand::rngs::OsRng),
                )
                .expect("hashing failed")
                .to_string();
            println!("{hash}");
            println!("\nAdd this to config.toml under [auth]: password_hash = \"{hash}\"");
        }
        "list-projects" => {
            let store = giskard_persist::PersistStore::new(data_dir);
            let index = store
                .load_project_index()
                .await
                .expect("failed to load index");
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
            let pid: giskard_core::ProjectId = pid
                .parse::<ulid::Ulid>()
                .map(giskard_core::ProjectId)
                .expect("invalid project id");
            let store = giskard_persist::PersistStore::new(data_dir);
            let threads = store
                .list_threads(pid)
                .await
                .expect("failed to list threads");
            if threads.is_empty() {
                println!("No threads.");
            } else {
                for tid in threads {
                    if let Some(thread) = store.load_thread(pid, tid).await.unwrap() {
                        println!("{}  {}  [{:?}]", tid, thread.title, thread.mode);
                    }
                }
            }
        }
        "dump-thread" => {
            let pid = expect_arg(&args, 2, "project id");
            let tid = expect_arg(&args, 3, "thread id");
            let pid = pid
                .parse::<ulid::Ulid>()
                .map(giskard_core::ProjectId)
                .expect("invalid project id");
            let tid = tid
                .parse::<ulid::Ulid>()
                .map(giskard_core::ThreadId)
                .expect("invalid thread id");
            let store = giskard_persist::PersistStore::new(data_dir);
            match store.load_thread(pid, tid).await {
                Ok(Some(thread)) => {
                    let json = serde_json::to_string_pretty(&thread).unwrap();
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
            let pid = pid
                .parse::<ulid::Ulid>()
                .map(giskard_core::ProjectId)
                .expect("invalid project id");
            let tid = tid
                .parse::<ulid::Ulid>()
                .map(giskard_core::ThreadId)
                .expect("invalid thread id");
            let store = giskard_persist::PersistStore::new(data_dir);
            store
                .delete_thread(pid, tid)
                .await
                .expect("failed to delete thread");
            println!("Deleted thread {tid}");
        }
        "delete-project" => {
            let pid = expect_arg(&args, 2, "project id");
            let pid = pid
                .parse::<ulid::Ulid>()
                .map(giskard_core::ProjectId)
                .expect("invalid project id");
            let store = giskard_persist::PersistStore::new(data_dir);
            store
                .delete_project(pid)
                .await
                .expect("failed to delete project");
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
}

fn expect_arg<'a>(args: &'a [String], idx: usize, name: &str) -> &'a str {
    args.get(idx).unwrap_or_else(|| {
        eprintln!("Missing {name} argument");
        usage(&args[0]);
        std::process::exit(1);
    })
}

fn usage(prog: &str) {
    eprintln!(
        "Usage: {prog} <command> [args]

Commands:
  set-password              Generate an Argon2 hash for the app password
  list-projects             List all projects
  list-threads <project>    List threads in a project
  dump-thread <project> <thread>   Pretty-print a thread's JSON
  delete-thread <project> <thread> Delete a thread
  delete-project <project>         Delete a project
  validate                  Validate all files, report corruption

Environment:
  GISKARD_DATA_DIR          Override the data directory (default: ~/.local/share/giskard)"
    );
}
