use std::path::Path;
use std::process::{Command, Output};

pub fn run(dir: &Path, args: &[&str]) -> Output {
    let output = Command::new("git")
        .current_dir(dir)
        .args(args)
        .env("LC_ALL", "C")
        .env("GIT_AUTHOR_NAME", "Test")
        .env("GIT_AUTHOR_EMAIL", "test@example.invalid")
        .env("GIT_COMMITTER_NAME", "Test")
        .env("GIT_COMMITTER_EMAIL", "test@example.invalid")
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

pub fn init_repo_with_commit(dir: &Path) {
    run(dir, &["init", "-q", "-b", "main"]);
    std::fs::write(dir.join("README.md"), "# project\n").unwrap();
    run(dir, &["add", "README.md"]);
    run(dir, &["commit", "-qm", "initial"]);
}
