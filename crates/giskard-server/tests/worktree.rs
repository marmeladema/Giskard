//! Per-thread Git worktrees, driven against real repositories.
//!
//! Everything here is behaviour Giskard depends on but does not implement: what `git worktree add`
//! carries across, what it refuses, and which of those refusals are recoverable. Mocking Git would
//! only assert our assumptions back at us.

use std::path::Path;
use std::process::Command;

use giskard_persist::store::ThreadWorktree;
use giskard_server::worktree;
use giskard_testenv::git;
use tempfile::TempDir;

fn stdout(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// A repository with one commit on `main`.
fn repo_with_commit(tmp: &TempDir) -> std::path::PathBuf {
    let repo = tmp.path().join("project");
    std::fs::create_dir_all(&repo).unwrap();
    git::run(&repo, &["init", "-q", "-b", "main"]);
    std::fs::write(repo.join("tracked.txt"), "committed\n").unwrap();
    git::run(&repo, &["add", "tracked.txt"]);
    git::run(&repo, &["commit", "-qm", "initial"]);
    repo
}

#[tokio::test]
async fn create_checks_out_the_branch_and_records_the_git_directories() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    let head = stdout(&git::run(&repo, &["rev-parse", "HEAD"]));
    let path = tmp.path().join("wt");

    let wt = worktree::create(&repo, &path, "giskard/worktree-01test")
        .await
        .expect("create worktree");

    assert!(path.join("tracked.txt").is_file());
    assert_eq!(
        stdout(&git::run(&path, &["branch", "--show-current"])),
        "giskard/worktree-01test"
    );
    assert_eq!(wt.base_commit.as_deref(), Some(head.as_str()));
    // Recorded, not derived: removal, branch deletion and the impact probes all run against these,
    // and a project that is itself a linked worktree makes the derived paths wrong.
    assert_eq!(
        Path::new(&wt.common_dir).canonicalize().unwrap(),
        repo.join(".git").canonicalize().unwrap()
    );
    assert!(
        Path::new(&wt.git_dir).ends_with("worktrees/wt"),
        "git_dir should be this worktree's private directory, got {}",
        wt.git_dir
    );
}

/// A fresh `git init` is a legitimate project. Git creates the worktree on an orphan branch, and
/// there is simply no base commit to record.
#[tokio::test]
async fn create_succeeds_in_a_repository_with_no_commits() {
    let tmp = TempDir::new().unwrap();
    let repo = tmp.path().join("empty");
    std::fs::create_dir_all(&repo).unwrap();
    git::run(&repo, &["init", "-q", "-b", "main"]);

    let wt = worktree::create(&repo, &tmp.path().join("wt"), "giskard/worktree-01empty")
        .await
        .expect("worktree in an unborn repository");

    assert_eq!(wt.base_commit, None);
}

#[tokio::test]
async fn create_refuses_a_directory_that_is_not_a_repository() {
    let tmp = TempDir::new().unwrap();
    let plain = tmp.path().join("plain");
    std::fs::create_dir_all(&plain).unwrap();

    let error = worktree::create(&plain, &tmp.path().join("wt"), "giskard/worktree-01plain")
        .await
        .expect_err("a directory with no repository cannot be isolated");

    assert!(
        matches!(error, worktree::WorktreeError::NotARepository(_)),
        "expected NotARepository, got {error:?}"
    );
}

/// Collisions hard-fail rather than picking another name: two threads whose branches differ by a
/// suffix look alike everywhere they are read, and a retry gets a fresh thread id anyway.
#[tokio::test]
async fn create_fails_and_names_the_branch_when_it_already_exists() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    git::run(&repo, &["branch", "giskard/worktree-01taken"]);

    let error = worktree::create(&repo, &tmp.path().join("wt"), "giskard/worktree-01taken")
        .await
        .expect_err("an existing branch must not be reused");

    let message = error.to_string();
    assert!(
        message.contains("giskard/worktree-01taken"),
        "the error must name the branch that collided, got: {message}"
    );
}

/// The worktree starts from HEAD, so work in progress in the project's checkout stays there. This
/// is the behaviour most likely to be mistaken for a bug, so it is pinned.
#[tokio::test]
async fn create_carries_committed_state_only() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    std::fs::write(repo.join("tracked.txt"), "committed\nuncommitted edit\n").unwrap();
    std::fs::write(repo.join("untracked.txt"), "scratch\n").unwrap();
    let path = tmp.path().join("wt");

    worktree::create(&repo, &path, "giskard/worktree-01clean")
        .await
        .unwrap();

    assert_eq!(
        std::fs::read_to_string(path.join("tracked.txt")).unwrap(),
        "committed\n",
        "the worktree holds the committed content, not the checkout's edit"
    );
    assert!(!path.join("untracked.txt").exists());
    assert_eq!(stdout(&git::run(&path, &["status", "--porcelain"])), "");
}

#[tokio::test]
async fn removal_refuses_uncommitted_work_and_proceeds_when_forced() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    let path = tmp.path().join("wt");
    let wt = worktree::create(&repo, &path, "giskard/worktree-01dirty")
        .await
        .unwrap();
    std::fs::write(path.join("tracked.txt"), "agent edit\n").unwrap();

    assert_eq!(worktree::uncommitted_changes(&wt).await.unwrap(), 1);
    worktree::remove(&wt, false)
        .await
        .expect_err("a dirty worktree must not vanish silently");
    worktree::remove(&wt, true).await.expect("forced removal");
    assert!(!path.exists());
}

/// A rename is one change but two records in `--porcelain=v2 -z`: the entry, then the original path
/// as a record of its own. Counting records rather than parsing them would report two, and the
/// deletion warning would overstate what is at stake. This is why the count goes through
/// `giskard-git-parser` rather than tallying output here.
#[tokio::test]
async fn a_rename_counts_as_one_uncommitted_change() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    let path = tmp.path().join("wt");
    let wt = worktree::create(&repo, &path, "giskard/worktree-01rename")
        .await
        .unwrap();
    git::run(&path, &["mv", "tracked.txt", "renamed.txt"]);

    assert_eq!(
        stdout(&git::run(&path, &["status", "--porcelain=v2"]))
            .lines()
            .count(),
        1,
        "the fixture must produce exactly one rename entry"
    );
    assert_eq!(worktree::uncommitted_changes(&wt).await.unwrap(), 1);
}

/// A path that `--porcelain` v1 would return quoted comes back verbatim under `-z`, so it is one
/// record either way — but only the parser knows that the quoting is absent.
#[tokio::test]
async fn a_path_with_spaces_and_quotes_counts_once() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    let path = tmp.path().join("wt");
    let wt = worktree::create(&repo, &path, "giskard/worktree-01quoted")
        .await
        .unwrap();
    std::fs::write(path.join(r#"a file "with quotes".txt"#), "scratch\n").unwrap();

    assert_eq!(worktree::uncommitted_changes(&wt).await.unwrap(), 1);
}

/// `git worktree remove` ignores ignored files, so a thread whose only leftovers are build
/// artifacts stays deletable in one click.
#[tokio::test]
async fn ignored_files_do_not_block_removal_or_count_as_changes() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    std::fs::write(repo.join(".gitignore"), "target/\n").unwrap();
    git::run(&repo, &["add", ".gitignore"]);
    git::run(&repo, &["commit", "-qm", "ignore build output"]);
    let path = tmp.path().join("wt");
    let wt = worktree::create(&repo, &path, "giskard/worktree-01ignored")
        .await
        .unwrap();
    std::fs::create_dir_all(path.join("target")).unwrap();
    std::fs::write(path.join("target/out.bin"), "build\n").unwrap();

    assert_eq!(worktree::uncommitted_changes(&wt).await.unwrap(), 0);
    worktree::remove(&wt, false)
        .await
        .expect("ignored files must not block removal");
}

#[tokio::test]
async fn delete_branch_reports_the_commit_it_pointed_at() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    let path = tmp.path().join("wt");
    let wt = worktree::create(&repo, &path, "giskard/worktree-01gone")
        .await
        .unwrap();
    worktree::remove(&wt, false).await.unwrap();

    let sha = worktree::delete_branch(&wt)
        .await
        .expect("delete the thread branch")
        .expect("git reports the commit");

    assert!(
        stdout(&git::run(&repo, &["rev-parse", "HEAD"])).starts_with(&sha),
        "the reported sha must be the branch tip, got {sha}"
    );
    assert_eq!(
        stdout(&git::run(&repo, &["branch", "--list", &wt.branch])),
        ""
    );
}

/// Branches the agent made are the user's now: deleting the thread's own branch leaves them alone,
/// and they were visible from the project's checkout all along.
#[tokio::test]
async fn deleting_the_thread_branch_leaves_agent_branches_alone() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    let path = tmp.path().join("wt");
    let wt = worktree::create(&repo, &path, "giskard/worktree-01keep")
        .await
        .unwrap();
    git::run(&path, &["switch", "-qc", "agent/idea"]);
    std::fs::write(path.join("idea.txt"), "work\n").unwrap();
    git::run(&path, &["add", "idea.txt"]);
    git::run(&path, &["commit", "-qm", "agent work"]);

    // `+` is git marking a branch checked out in another worktree — which is exactly what this
    // asserts: the branch exists in the shared repository, seen from the project's checkout.
    assert_eq!(
        stdout(&git::run(&repo, &["branch", "--list", "agent/idea"])),
        "+ agent/idea",
        "a branch created inside the worktree is visible from the project checkout immediately"
    );

    worktree::remove(&wt, false).await.unwrap();
    worktree::delete_branch(&wt).await.unwrap();

    assert!(!stdout(&git::run(&repo, &["branch", "--list", "agent/idea"])).is_empty());
    assert_eq!(
        stdout(&git::run(
            &repo,
            &["log", "-1", "--format=%s", "agent/idea"]
        )),
        "agent work"
    );
}

#[tokio::test]
async fn lost_work_counts_only_commits_that_exist_nowhere_else() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);

    // A branch holding a commit of its own: deleting it would destroy that commit.
    let unique_path = tmp.path().join("wt-unique");
    let unique = worktree::create(&repo, &unique_path, "giskard/worktree-01unique")
        .await
        .unwrap();
    std::fs::write(unique_path.join("only-here.txt"), "unique\n").unwrap();
    git::run(&unique_path, &["add", "only-here.txt"]);
    git::run(&unique_path, &["commit", "-qm", "unique work"]);

    // A branch whose commit the agent also parked on a branch of its own: nothing would be lost.
    let parked_path = tmp.path().join("wt-parked");
    let parked = worktree::create(&repo, &parked_path, "giskard/worktree-01parked")
        .await
        .unwrap();
    std::fs::write(parked_path.join("kept.txt"), "kept\n").unwrap();
    git::run(&parked_path, &["add", "kept.txt"]);
    git::run(&parked_path, &["commit", "-qm", "work the user keeps"]);
    git::run(&parked_path, &["branch", "agent/keep-this"]);

    assert_eq!(
        worktree::commits_reachable_from_nowhere_else(&unique)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        worktree::commits_reachable_from_nowhere_else(&parked)
            .await
            .unwrap(),
        0,
        "work parked on another branch is not lost, whatever `git branch -d` says about it"
    );
    // The verdict this replaces: `-d` compares against HEAD and upstream only.
    let refusal = Command::new("git")
        .current_dir(&repo)
        .args(["branch", "-d", &parked.branch])
        .env("LC_ALL", "C")
        .output()
        .unwrap();
    assert!(!refusal.status.success());
}

/// The two halves of deletion have to agree about a branch that is already gone. `delete_branch`
/// treats it as nothing left to remove, so the impact probe must too — otherwise a thread whose
/// branch the agent renamed can be deleted but cannot say what deleting it would cost, and the
/// preflight refuses a deletion that would have succeeded.
#[tokio::test]
async fn a_branch_the_agent_renamed_costs_nothing_rather_than_failing_the_probe() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    let path = tmp.path().join("wt");
    let wt = worktree::create(&repo, &path, "giskard/worktree-01renamed")
        .await
        .unwrap();
    std::fs::write(path.join("work.txt"), "work\n").unwrap();
    git::run(&path, &["add", "work.txt"]);
    git::run(&path, &["commit", "-qm", "work"]);
    // What the agent does when it gives its work a name of its own.
    git::run(&path, &["branch", "-m", "agent/renamed"]);

    assert_eq!(
        worktree::commits_reachable_from_nowhere_else(&wt)
            .await
            .unwrap(),
        0,
        "a recorded branch that no longer exists cannot lose commits"
    );
    // And the other half agrees rather than erroring, which is the point of the pairing.
    assert_eq!(worktree::delete_branch(&wt).await.unwrap(), None);
}

/// A third outcome sits between "deleted, here is the commit" and "there was nothing to delete":
/// the deletion happens but Git's report names no commit. Deleting a symbolic ref reaches it for
/// real — Git answers `(was refs/heads/…)` — and the branch is gone all the same, so the deletion
/// must succeed while reporting nothing rather than passing a ref name off as a commit.
#[tokio::test]
async fn a_deletion_that_reports_no_commit_still_succeeds() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    let path = tmp.path().join("wt");
    let created = worktree::create(&repo, &path, "giskard/worktree-01symref")
        .await
        .unwrap();
    worktree::remove(&created, false).await.unwrap();
    // A branch cannot be a symbolic ref and a checkout at once, so the recorded name is swapped for
    // one after the fact. Everything else about the worktree is what Git actually produced.
    let branch = "giskard/worktree-01symlink";
    git::run(
        &repo,
        &[
            "symbolic-ref",
            &format!("refs/heads/{branch}"),
            "refs/heads/main",
        ],
    );
    let wt = ThreadWorktree {
        branch: branch.to_string(),
        ..created
    };

    assert_eq!(
        worktree::delete_branch(&wt).await.unwrap(),
        None,
        "the ref a symbolic branch pointed at is not the commit it pointed at"
    );
    assert_eq!(stdout(&git::run(&repo, &["branch", "--list", branch])), "");
    assert!(
        !stdout(&git::run(&repo, &["branch", "--list", "main"])).is_empty(),
        "deleting the symbolic ref must not take the branch it pointed at"
    );
}

/// Removing a worktree does not touch the commits made in it — they are in the shared object store
/// and the branch still points at them. This is what makes deleting a thread a decision about the
/// *checkout* rather than about history, and why the deletion impact asks only about work that
/// exists nowhere else.
#[tokio::test]
async fn removing_a_worktree_leaves_the_commits_made_in_it_on_their_branch() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    let path = tmp.path().join("wt");
    let wt = worktree::create(&repo, &path, "giskard/worktree-01archive")
        .await
        .unwrap();
    std::fs::write(path.join("kept.txt"), "committed in the thread\n").unwrap();
    git::run(&path, &["add", "kept.txt"]);
    git::run(&path, &["commit", "-qm", "thread work"]);
    let committed = stdout(&git::run(&path, &["rev-parse", "HEAD"]));

    worktree::remove(&wt, false).await.expect("remove");

    assert!(!path.exists());
    assert_eq!(
        stdout(&git::run(&repo, &["rev-parse", &wt.branch])),
        committed,
        "the branch still points at the commit made in the removed checkout"
    );
    assert_eq!(
        stdout(&git::run(
            &repo,
            &["show", &format!("{}:kept.txt", wt.branch)]
        )),
        "committed in the thread",
        "and the content is readable from the project's own checkout"
    );
}

/// A project can be rooted in a *subdirectory* of its repository — a package inside a monorepo is
/// the ordinary case. Git can only check out a whole repository, so the worktree is the repository
/// root; the thread has to work in the same subdirectory beneath it. Resolving to the worktree root
/// instead would put the agent above the directory the project scoped it to, and would make the
/// same relative path name a different file than it does for an ordinary thread.
#[tokio::test]
async fn a_project_inside_a_repository_works_in_the_matching_subdirectory() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    let project = repo.join("packages/api");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join("service.rs"), "fn serve() {}\n").unwrap();
    git::run(&repo, &["add", "packages"]);
    git::run(&repo, &["commit", "-qm", "add the package"]);
    let path = tmp.path().join("wt");

    let wt = worktree::create(&project, &path, "giskard/worktree-01sub")
        .await
        .expect("a project inside a repository can be isolated");

    assert_eq!(
        Path::new(wt.workspace_root()),
        path.join("packages/api"),
        "the thread works in its own subdirectory, not at the checkout root"
    );
    assert_eq!(
        wt.path,
        path.to_string_lossy(),
        "while the checkout Git manages is still the whole repository"
    );
    // The distinction is only worth anything if a path resolves the same way it does without
    // isolation: `service.rs` means the package's file in both.
    assert!(Path::new(wt.workspace_root()).join("service.rs").is_file());
    // And removal still works, because it runs against the checkout rather than the workspace.
    worktree::remove(&wt, false).await.expect("remove");
    assert!(!path.exists());
}

/// A project directory that is not in the repository's committed content has no counterpart in a
/// fresh checkout, so there is nowhere for the thread to run. Better to say so than to invent an
/// empty directory and let the agent work in it.
#[tokio::test]
async fn an_untracked_project_directory_cannot_be_isolated() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    let project = repo.join("scratch");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join("notes.txt"), "not committed\n").unwrap();
    let path = tmp.path().join("wt");

    let error = worktree::create(&project, &path, "giskard/worktree-01scratch")
        .await
        .expect_err("an untracked project directory has no worktree counterpart");

    assert!(
        error.to_string().contains("scratch"),
        "the error must name the directory that is missing, got: {error}"
    );
    // Nothing is left behind, exactly as for any other failure after the checkout exists.
    assert!(
        !path.exists(),
        "the half-created checkout must be rolled back"
    );
    assert!(
        stdout(&git::run(
            &repo,
            &["branch", "--list", "giskard/worktree-01scratch"]
        ))
        .is_empty(),
        "and so must its branch"
    );
}

/// The project directory can itself be a linked worktree, in which case its `.git` is a pointer
/// file. Anything built from `<project>/.git` would then target a file rather than a repository, so
/// the recorded directories have to come from `git rev-parse`.
#[tokio::test]
async fn worktree_of_a_worktree_records_the_canonical_repository() {
    let tmp = TempDir::new().unwrap();
    let canonical = repo_with_commit(&tmp);
    let project = tmp.path().join("project-as-worktree");
    git::run(
        &canonical,
        &[
            "worktree",
            "add",
            "-q",
            "-b",
            "feature",
            &project.to_string_lossy(),
        ],
    );
    assert!(
        project.join(".git").is_file(),
        "the project's .git must be a pointer file for this test to mean anything"
    );

    let wt = worktree::create(
        &project,
        &tmp.path().join("wt"),
        "giskard/worktree-01nested",
    )
    .await
    .expect("create from a project that is itself a worktree");

    assert_eq!(
        Path::new(&wt.common_dir).canonicalize().unwrap(),
        canonical.join(".git").canonicalize().unwrap(),
        "the shared repository is the canonical checkout's, not the project's pointer file"
    );
    assert!(
        !Path::new(&wt.git_dir).starts_with(project.join(".git")),
        "no recorded Git directory may be derived from the pointer file, got {}",
        wt.git_dir
    );
}

/// A worktree the user deleted by hand is still *registered* with Git, and a registered worktree
/// holds its branch. Removal is therefore attempted even when the directory is gone: it is what
/// de-registers the checkout, and the branch cannot be deleted until it is.
#[tokio::test]
async fn removal_de_registers_a_worktree_whose_directory_disappeared() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    let path = tmp.path().join("wt");
    let wt = worktree::create(&repo, &path, "giskard/worktree-01vanished")
        .await
        .unwrap();
    std::fs::remove_dir_all(&path).unwrap();

    assert!(
        !git_status(&repo, &["branch", "-D", &wt.branch]).success(),
        "the registration should still be holding the branch"
    );

    // Unforced on purpose: a directory that is already gone holds nothing to lose, so an ordinary
    // thread deletion has to clear the registration on its own rather than bouncing the user into a
    // confirmation about work that no longer exists.
    worktree::remove(&wt, /*force*/ false)
        .await
        .expect("remove a worktree whose directory is already gone");
    worktree::delete_branch(&wt)
        .await
        .expect("delete the branch");

    assert_eq!(
        stdout(&git::run(&repo, &["branch", "--list", &wt.branch])),
        ""
    );
}

fn git_status(dir: &Path, args: &[&str]) -> std::process::ExitStatus {
    Command::new("git")
        .current_dir(dir)
        .args(args)
        .env("LC_ALL", "C")
        .output()
        .expect("run git")
        .status
}

#[tokio::test]
async fn prune_forgets_a_worktree_whose_directory_was_deleted() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_commit(&tmp);
    let path = tmp.path().join("wt");
    let wt: ThreadWorktree = worktree::create(&repo, &path, "giskard/worktree-01pruned")
        .await
        .unwrap();
    std::fs::remove_dir_all(&path).unwrap();

    // Without this the check below could pass for a path Git never printed in the first place —
    // it compares a string, and a temporary directory behind a symlink resolves differently.
    assert!(
        stdout(&git::run(&repo, &["worktree", "list"])).contains(&*path.to_string_lossy()),
        "the fixture must leave the worktree registered, or the assertion below proves nothing"
    );

    worktree::prune(Path::new(&wt.repo_root)).await;

    // The exact path, not a substring: `worktree list` prints the main checkout too, and the
    // temporary directory's name is random, so a looser match decides on whatever it happens to
    // contain.
    assert!(
        !stdout(&git::run(&repo, &["worktree", "list"])).contains(&*path.to_string_lossy()),
        "a worktree whose directory is gone must not stay listed"
    );
}

/// A repository that ignores the usual local files, with them present in the checkout, and a
/// `.worktreeinclude` naming some of them.
fn repo_with_local_files(tmp: &TempDir, include: &str) -> std::path::PathBuf {
    let repo = repo_with_commit(tmp);
    std::fs::write(
        repo.join(".gitignore"),
        ".env\n*.log\ntarget/\nlocal/\nsecrets/\n",
    )
    .unwrap();
    git::run(&repo, &["add", ".gitignore"]);
    git::run(&repo, &["commit", "-qm", "ignore local files"]);
    for (file, content) in [
        (".env", "TOKEN=1\n"),
        ("local/sub/dev.toml", "port = 1\n"),
        ("pkg/debug.log", "log\n"),
        ("pkg/.env", "NESTED=1\n"),
        ("target/debug/build", "artifact\n"),
        ("secrets/key.pem", "key\n"),
        ("scratch.txt", "work in progress\n"),
    ] {
        let path = repo.join(file);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }
    if !include.is_empty() {
        std::fs::write(repo.join(worktree::WORKTREE_INCLUDE_FILE), include).unwrap();
    }
    repo
}

/// The include file selects among *ignored* files, by `.gitignore` rules: a bare name matches at
/// any depth, a directory pattern takes everything under it, `**/` reaches into subdirectories and
/// `!` takes a match back. An ignored file it does not name stays behind, and so does an untracked
/// file that is not ignored even when the include file names it — that is work in progress, not
/// local configuration.
#[tokio::test]
async fn create_copies_the_ignored_files_the_include_file_names() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_local_files(&tmp, ".env\n!pkg/.env\nlocal/\n**/*.log\nscratch.txt\n");
    let path = tmp.path().join("wt");

    worktree::create(&repo, &path, "giskard/worktree-01seed")
        .await
        .expect("create worktree");

    assert_eq!(
        std::fs::read_to_string(path.join(".env")).unwrap(),
        "TOKEN=1\n"
    );
    assert!(path.join("local/sub/dev.toml").is_file());
    assert!(path.join("pkg/debug.log").is_file());
    assert!(!path.join("pkg/.env").exists(), "`!` takes the match back");
    assert!(
        !path.join("target").exists(),
        "an ignored file not named stays behind"
    );
    assert!(
        !path.join("secrets").exists(),
        "an ignored file not named stays behind"
    );
    assert!(
        !path.join("scratch.txt").exists(),
        "an untracked file that is not ignored is never copied"
    );
    // Copied files are ignored in the worktree just as in the checkout, so seeding leaves the
    // thread's Git status row clean.
    assert_eq!(stdout(&git::run(&path, &["status", "--porcelain"])), "");
}

#[tokio::test]
async fn without_an_include_file_no_ignored_file_comes_across() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_local_files(&tmp, "");
    let path = tmp.path().join("wt");

    worktree::create(&repo, &path, "giskard/worktree-01noseed")
        .await
        .expect("create worktree");

    for absent in [".env", "local", "pkg", "target", "secrets", "scratch.txt"] {
        assert!(!path.join(absent).exists(), "{absent} came across");
    }
}

/// `ls-files -z` prints names verbatim, so a path with a space or a newline in it is copied to the
/// same name rather than to a quoted or split one.
#[tokio::test]
async fn an_included_path_with_a_space_or_a_newline_is_copied_verbatim() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_local_files(&tmp, "local/\n");
    std::fs::write(repo.join("local/dev config.toml"), "a\n").unwrap();
    std::fs::write(repo.join("local/odd\nname"), "b\n").unwrap();
    let path = tmp.path().join("wt");

    worktree::create(&repo, &path, "giskard/worktree-01spaces")
        .await
        .expect("create worktree");

    assert!(path.join("local/dev config.toml").is_file());
    assert!(path.join("local/odd\nname").is_file());
}

/// A file the checkout has stopped tracking can still be in the commit the worktree starts from.
/// The committed content wins: seeding adds files the checkout cannot have, and never replaces
/// one it does.
#[tokio::test]
async fn an_included_file_never_overwrites_committed_content() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_local_files(&tmp, ".env\n");
    std::fs::write(repo.join(".env"), "COMMITTED=1\n").unwrap();
    git::run(&repo, &["add", "-f", ".env"]);
    git::run(&repo, &["commit", "-qm", "commit a .env by mistake"]);
    git::run(&repo, &["rm", "-q", "--cached", ".env"]);
    std::fs::write(repo.join(".env"), "LOCAL=1\n").unwrap();
    let path = tmp.path().join("wt");

    worktree::create(&repo, &path, "giskard/worktree-01keep")
        .await
        .expect("create worktree");

    assert_eq!(
        std::fs::read_to_string(path.join(".env")).unwrap(),
        "COMMITTED=1\n"
    );
}

/// A symlink is copied as the link, not as what it points at — which may be anywhere on disk.
/// The permission bits come across too, so a local script stays runnable.
#[cfg(unix)]
#[tokio::test]
async fn an_included_symlink_stays_a_symlink_and_modes_are_kept() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = TempDir::new().unwrap();
    let outside = tmp.path().join("outside.toml");
    std::fs::write(&outside, "elsewhere\n").unwrap();
    let repo = repo_with_local_files(&tmp, "local/\n");
    std::os::unix::fs::symlink(&outside, repo.join("local/linked.toml")).unwrap();
    let script = repo.join("local/run.sh");
    std::fs::write(&script, "#!/bin/sh\n").unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = tmp.path().join("wt");

    worktree::create(&repo, &path, "giskard/worktree-01links")
        .await
        .expect("create worktree");

    let link = path.join("local/linked.toml");
    assert!(link.symlink_metadata().unwrap().file_type().is_symlink());
    assert_eq!(std::fs::read_link(&link).unwrap(), outside);
    let mode = std::fs::metadata(path.join("local/run.sh"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o111, 0o111, "the script lost its executable bits");
}

/// The include file belongs to the repository, like the checkout it fills: a project rooted in a
/// subdirectory still reads it from the top level, and its paths stay relative to the top level.
#[tokio::test]
async fn a_project_in_a_subdirectory_reads_the_include_file_at_the_top_level() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_local_files(&tmp, ".env\n");
    let project = repo.join("packages/api");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join("service.rs"), "fn serve() {}\n").unwrap();
    std::fs::write(project.join(".env"), "API=1\n").unwrap();
    git::run(&repo, &["add", "packages"]);
    git::run(&repo, &["commit", "-qm", "add the package"]);
    let path = tmp.path().join("wt");

    let wt = worktree::create(&project, &path, "giskard/worktree-01subseed")
        .await
        .expect("create worktree");

    assert!(path.join(".env").is_file(), "the top level's .env");
    assert_eq!(
        std::fs::read_to_string(Path::new(wt.workspace_root()).join(".env")).unwrap(),
        "API=1\n",
        "and the package's own, beneath the workspace"
    );
}

/// A file the project lists but the server cannot copy fails the creation, like every other
/// failure after the checkout exists: a thread missing the file it was promised would fail later,
/// further from the cause. Nothing is left behind.
#[cfg(unix)]
#[tokio::test]
async fn a_file_that_cannot_be_copied_fails_the_creation_and_rolls_it_back() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = TempDir::new().unwrap();
    let repo = repo_with_local_files(&tmp, ".env\n");
    let env = repo.join(".env");
    std::fs::set_permissions(&env, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read(&env).is_ok() {
        // Running as root, which reads through any mode: there is no failure to provoke.
        return;
    }
    let path = tmp.path().join("wt");
    let branch = "giskard/worktree-01unreadable";

    let error = worktree::create(&repo, &path, branch)
        .await
        .expect_err("an unreadable included file must fail the creation");

    assert!(error.to_string().contains(".env"), "{error}");
    assert!(!path.exists(), "the checkout survived the failed creation");
    assert_eq!(stdout(&git::run(&repo, &["branch", "--list", branch])), "");
}

/// A Unix file name is bytes. One that is not UTF-8 is still a local file the project named, and is
/// copied to exactly the same name.
#[cfg(unix)]
#[tokio::test]
async fn an_included_path_that_is_not_utf8_is_copied_byte_for_byte() {
    use std::os::unix::ffi::OsStrExt;

    let tmp = TempDir::new().unwrap();
    let repo = repo_with_local_files(&tmp, "local/\n");
    let name = std::ffi::OsStr::from_bytes(b"caf\xe9.toml");
    std::fs::write(repo.join("local").join(name), "latin-1\n").unwrap();
    let path = tmp.path().join("wt");

    worktree::create(&repo, &path, "giskard/worktree-01bytes")
        .await
        .expect("create worktree");

    assert_eq!(
        std::fs::read_to_string(path.join("local").join(name)).unwrap(),
        "latin-1\n"
    );
}

/// The directories seeding creates take their checkout's mode and timestamps, not the server's
/// defaults: a `0700` secrets directory stays private. A directory with no write permission is the
/// case that decides the order — its files have to be written before its mode is applied.
#[cfg(unix)]
#[tokio::test]
async fn created_directories_and_files_keep_their_modes_and_times() {
    use std::os::unix::fs::PermissionsExt;
    use std::time::{Duration, SystemTime};

    let tmp = TempDir::new().unwrap();
    let repo = repo_with_local_files(&tmp, "secrets/\nlocal/\n");
    let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o7777;
    let set_mode = |path: &Path, mode: u32| {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap()
    };
    let past = SystemTime::UNIX_EPOCH + Duration::from_secs(1_600_000_000);
    let key = repo.join("secrets/key.pem");
    std::fs::File::options()
        .write(true)
        .open(&key)
        .unwrap()
        .set_modified(past)
        .unwrap();
    set_mode(&key, 0o600);
    set_mode(&repo.join("secrets"), 0o700);
    // `local/sub` holds a file and has no write permission of its own.
    set_mode(&repo.join("local/sub"), 0o555);
    std::fs::File::open(repo.join("local/sub"))
        .unwrap()
        .set_modified(past)
        .unwrap();
    let path = tmp.path().join("wt");

    let created = worktree::create(&repo, &path, "giskard/worktree-01modes").await;

    // Restore before asserting, so the temporary directory can be cleaned up either way.
    set_mode(&repo.join("local/sub"), 0o755);
    if path.join("local/sub").exists() {
        let copied_mode = mode(&path.join("local/sub"));
        set_mode(&path.join("local/sub"), 0o755);
        assert_eq!(copied_mode, 0o555, "the read-only directory's mode");
    }
    created.expect("a read-only directory in the checkout must not stop the copy");
    assert!(path.join("local/sub/dev.toml").is_file());
    assert_eq!(mode(&path.join("secrets")), 0o700);
    assert_eq!(mode(&path.join("secrets/key.pem")), 0o600);
    let modified = |path: &Path| std::fs::metadata(path).unwrap().modified().unwrap();
    assert_eq!(modified(&path.join("secrets/key.pem")), past);
    assert_eq!(
        modified(&path.join("local/sub")),
        past,
        "a directory's time is set after its files are written into it"
    );
}

/// A relative link that stays inside the repository means the same file in the worktree. One that
/// climbs out of it would land somewhere else from the worktree's location, so it is rewritten to
/// the absolute path it reaches from the checkout.
#[cfg(unix)]
#[tokio::test]
async fn a_relative_link_out_of_the_repository_is_made_absolute() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_local_files(&tmp, "local/\n");
    std::os::unix::fs::symlink("../../shared.env", repo.join("local/out.env")).unwrap();
    std::os::unix::fs::symlink("sub/dev.toml", repo.join("local/in.toml")).unwrap();
    let path = tmp.path().join("wt");

    worktree::create(&repo, &path, "giskard/worktree-01rellinks")
        .await
        .expect("create worktree");

    let toplevel = repo.canonicalize().unwrap();
    assert_eq!(
        std::fs::read_link(path.join("local/out.env")).unwrap(),
        toplevel.parent().unwrap().join("shared.env")
    );
    assert_eq!(
        std::fs::read_link(path.join("local/in.toml")).unwrap(),
        Path::new("sub/dev.toml"),
        "a link inside the repository is left relative"
    );
}

/// `ls-files` reports a nested repository as a directory entry, not its files. It is someone else's
/// checkout, and is skipped rather than copied or failed on.
#[tokio::test]
async fn a_nested_repository_the_include_file_matches_is_skipped() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_local_files(&tmp, "local/\n");
    let nested = repo.join("local/nested");
    std::fs::create_dir_all(&nested).unwrap();
    git::run(&nested, &["init", "-q", "-b", "main"]);
    std::fs::write(nested.join("file"), "x\n").unwrap();
    let path = tmp.path().join("wt");

    worktree::create(&repo, &path, "giskard/worktree-01nested")
        .await
        .expect("a nested repository must not fail the creation");

    assert!(path.join("local/sub/dev.toml").is_file());
    assert!(!path.join("local/nested").exists());
}

/// The repository and file set both runs below were measured on, against Claude Code 2.1.316.
const REACH_FILES: [&str; 13] = [
    ".env",
    "sub/.env",
    "config/app.local.toml",
    "target/.env",
    "target/deep/.env",
    "target/cfg/app.local.toml",
    "target/keep/x.txt",
    "target/notes.md",
    "node_modules/pkg/.env",
    "node_modules/pkg/app.local.toml",
    "vendor/a/config.json",
    "vendor/b/config.json",
    "cache/all/of/it.bin",
];

/// Seed a worktree of the measured repository with `include`, and list what came across.
async fn files_copied_with(include: &str, branch: &str) -> Vec<&'static str> {
    let tmp = TempDir::new().unwrap();
    let repo = tmp.path().join("project");
    std::fs::create_dir_all(&repo).unwrap();
    git::run(&repo, &["init", "-q", "-b", "main"]);
    std::fs::write(
        repo.join(".gitignore"),
        "target/\nnode_modules/\nvendor/\ncache/\n.env\n*.local.toml\n",
    )
    .unwrap();
    git::run(&repo, &["add", ".gitignore"]);
    git::run(&repo, &["commit", "-qm", "initial"]);
    for file in REACH_FILES {
        let path = repo.join(file);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, file).unwrap();
    }
    std::fs::write(repo.join(worktree::WORKTREE_INCLUDE_FILE), include).unwrap();
    let path = tmp.path().join("wt");
    worktree::create(&repo, &path, branch)
        .await
        .expect("create worktree");
    REACH_FILES
        .into_iter()
        .filter(|file| path.join(file).exists())
        .collect()
}

/// Inside a directory the repository ignores as a whole, nothing is copied unless a pattern opens
/// the directory — Claude Code's rule, and both expectations below are what Claude Code 2.1.316
/// produced for the same repository and include file. A bare `.env` does not empty `target/` and
/// `node_modules/` of their `.env` files, and `**/keep/x.txt` does not open `target/`, because
/// `keep` is not a name in `target`'s path. `vendor/**/…` names the directory it goes into.
#[tokio::test]
async fn a_directory_ignored_as_a_whole_stays_closed_unless_a_pattern_opens_it() {
    let copied = files_copied_with(
        ".env\n*.local.toml\n**/keep/x.txt\nvendor/**/config.json\n",
        "giskard/worktree-01closed",
    )
    .await;
    assert_eq!(
        copied,
        vec![
            ".env",
            "sub/.env",
            "config/app.local.toml",
            "vendor/a/config.json",
            "vendor/b/config.json",
        ]
    );
}

/// The decision is per directory. `**/target/notes.md` opens `target/` — `target` is a name in its
/// path — and once it is open, every pattern applies inside it: the bare `.env` and
/// `*.local.toml` now copy from `target/` too, and so does `**/keep/x.txt`. `cache/` matches the
/// directory itself. `node_modules/` stays closed. Again Claude Code 2.1.316's own result.
#[tokio::test]
async fn a_pattern_that_opens_a_directory_lets_every_pattern_apply_inside_it() {
    let copied = files_copied_with(
        ".env\n*.local.toml\n**/keep/x.txt\nvendor/**/config.json\n**/target/notes.md\ncache/\n",
        "giskard/worktree-01opened",
    )
    .await;
    assert_eq!(
        copied,
        vec![
            ".env",
            "sub/.env",
            "config/app.local.toml",
            "target/.env",
            "target/deep/.env",
            "target/cfg/app.local.toml",
            "target/keep/x.txt",
            "target/notes.md",
            "vendor/a/config.json",
            "vendor/b/config.json",
            "cache/all/of/it.bin",
        ]
    );
}

/// A path line opens the directory its literal text leads into, and a line leading somewhere else
/// opens nothing. A trailing `**` is a way in as well.
#[tokio::test]
async fn a_path_line_opens_only_the_directory_it_leads_into() {
    assert_eq!(
        files_copied_with("vend*/a/config.json\n", "giskard/worktree-01glob").await,
        vec!["vendor/a/config.json"]
    );
    assert_eq!(
        files_copied_with("vendor/**\n", "giskard/worktree-01tail").await,
        vec!["vendor/a/config.json", "vendor/b/config.json"]
    );
    assert!(
        files_copied_with("elsewhere/a/config.json\n", "giskard/worktree-01away")
            .await
            .is_empty()
    );
}

/// `check-ignore` reads its input as pathspecs, so a file named like pathspec magic must reach it
/// as a plain path: one such name would otherwise fail the whole batch, and the thread's start.
#[tokio::test]
async fn a_file_named_like_pathspec_magic_is_copied() {
    let tmp = TempDir::new().unwrap();
    let repo = repo_with_local_files(&tmp, "*.log\n");
    let names = [
        ":!weird.log",
        ":(x)y.log",
        ":plain.log",
        "a*b.log",
        "q?z.log",
    ];
    for name in names {
        std::fs::write(repo.join(name), name).unwrap();
    }
    std::fs::create_dir_all(repo.join(":dir")).unwrap();
    std::fs::write(repo.join(":dir/f.log"), "f").unwrap();
    let path = tmp.path().join("wt");

    worktree::create(&repo, &path, "giskard/worktree-01magic")
        .await
        .expect("a file's name must not fail the creation");

    for name in names {
        assert!(path.join(name).is_file(), "{name} did not come across");
    }
    assert!(path.join(":dir/f.log").is_file());
}

/// Each row is one repository run through Claude Code (2.1.316 or 2.1.317) with `claude -p
/// --worktree`, and what it copied: which lines open a directory the repository ignores as a whole.
/// Claude Code documents only part of this, so the measurements are the specification.
#[tokio::test]
async fn which_lines_open_a_directory_matches_claude_code() {
    const VENDOR: &[&str] = &["vendor/a/config.json"];
    const WITH_ENV: &[&str] = &["vendor/a/config.json", "vendor/.env"];
    const NESTED: &[&str] = &["x/vendor/a/config.json"];
    #[rustfmt::skip]
    let rows: &[(&str, &str, &[&str], &[&str])] = &[
        // A first name opens by the literal text before any wildcard, and not without any.
        ("vendor/\n", "**/vendor/a/config.json\n", VENDOR, VENDOR),
        ("vendor/\n", "**/vend*/a/config.json\n", VENDOR, VENDOR),
        ("vendor/\n", "**/v*/a/config.json\n", VENDOR, VENDOR),
        ("vendor/\n", "**/vend?r/a/config.json\n", VENDOR, VENDOR),
        ("vendor/\n", "**/vendor*/a/config.json\n", VENDOR, VENDOR),
        ("vendor/\n", "**/*/a/config.json\n", VENDOR, &[]),
        ("vendor/\n", "**/*dor/a/config.json\n", VENDOR, &[]),
        ("vendor/\n", "**/*end*/a/config.json\n", VENDOR, &[]),
        ("vendor/\n", "**/?endor/a/config.json\n", VENDOR, &[]),
        ("vendor/\n", "**/[v]endor/a/config.json\n", VENDOR, &[]),
        ("vendor/\n", "**/*/config.json\n", VENDOR, &[]),
        ("vendor/\n", "**/config.json\n", VENDOR, &[]),
        // So does a path, and a leading wildcard opens nothing there either.
        ("vendor/\n", "vend*/a/config.json\n", VENDOR, VENDOR),
        ("vendor/\n", "*/a/config.json\n", VENDOR, &[]),
        // Only the literal text counts. These open `vendor`, which `.env` shows, without the line
        // itself selecting anything there.
        ("vendor/\n.env\n", "**/v*x/a/config.json\n.env\n", WITH_ENV, &["vendor/.env"]),
        ("vendor/\n.env\n", "v*x/a/config.json\n.env\n", WITH_ENV, &["vendor/.env"]),
        ("vendor/\n.env\n", "vendor/nope/missing\n.env\n", WITH_ENV, &["vendor/.env"]),
        ("vendor/\n.env\n", "w*/a/config.json\n.env\n", WITH_ENV, &[]),
        // A directory further down, where its parent is not ignored.
        ("x/vendor/\n", "x/*/a/config.json\n", NESTED, NESTED),
        ("x/vendor/\n", "x/v*/a/config.json\n", NESTED, NESTED),
        ("x/vendor/\n", "x/**/config.json\n", NESTED, NESTED),
        ("x/vendor/\n", "*/vendor/a/config.json\n", NESTED, &[]),
        ("x/vendor/\n", "**/x/*/a/config.json\n", NESTED, NESTED),
        ("x/vendor/\n", "**/vendor/a/config.json\n", NESTED, NESTED),
        ("x/vendor/\n", "**/x/config.json\n", NESTED, &[]),
        // A directory whose contents are ignored is not itself ignored, so it was never closed.
        ("logs/*\n", ".env\n", &["logs/.env"], &["logs/.env"]),
        ("star/**\n", ".env\n", &["star/.env"], &["star/.env"]),
        // A negation still takes back what an open directory would have given.
        (
            "vendor/\n",
            "vendor/**/config.json\n!vendor/b/config.json\n",
            &["vendor/a/config.json", "vendor/b/config.json"],
            &["vendor/a/config.json"],
        ),
    ];
    for (gitignore, include, files, expected) in rows {
        let tmp = TempDir::new().unwrap();
        let repo = tmp.path().join("project");
        std::fs::create_dir_all(&repo).unwrap();
        git::run(&repo, &["init", "-q", "-b", "main"]);
        std::fs::write(repo.join(".gitignore"), gitignore).unwrap();
        git::run(&repo, &["add", ".gitignore"]);
        git::run(&repo, &["commit", "-qm", "initial"]);
        for file in *files {
            let path = repo.join(file);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, file).unwrap();
        }
        std::fs::write(repo.join(worktree::WORKTREE_INCLUDE_FILE), include).unwrap();
        let path = tmp.path().join("wt");

        worktree::create(&repo, &path, "giskard/worktree-01row")
            .await
            .expect("create worktree");

        let copied: Vec<&str> = files
            .iter()
            .copied()
            .filter(|file| path.join(file).exists())
            .collect();
        assert_eq!(
            &copied, expected,
            "gitignore {gitignore:?}, include {include:?}"
        );
    }
}
