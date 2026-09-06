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
