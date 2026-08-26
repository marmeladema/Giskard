//! Per-thread Git worktrees (spec §7.1).
//!
//! A thread can be started in its own linked worktree so its file changes are isolated from the
//! project's checkout and from every other thread. This module owns the Git side of that: creating
//! and removing the worktree, and answering what a removal would destroy.
//!
//! What it deliberately does **not** own is isolation of the repository itself. A linked worktree
//! shares one object store and one set of refs with the checkout that owns it, so a thread's commits
//! and branches are visible from the project's checkout immediately — that is the point, since it is
//! how work gets out. See `docs/git-worktrees.md`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use giskard_core::ids::ThreadId;
use giskard_persist::store::ThreadWorktree;
use tokio::process::Command as TokioCommand;
use tracing::{debug, warn};

/// Read-only queries answer in milliseconds; `worktree add` writes a whole working tree and
/// `worktree remove` deletes one, so both scale with the repository rather than with the request.
/// A checkout of a large repository on a cold cache takes far longer than the 3s the status polls
/// allow, and timing it out would leave a half-created worktree behind.
const GIT_CHECKOUT_TIMEOUT: Duration = Duration::from_secs(300);
const GIT_QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// How many characters of the thread ULID name the branch.
///
/// Both bounds on this number are real, which is why it is not simply "all of them".
///
/// The lower bound is collision. A ULID's first ten characters decode to *exactly* its 48-bit
/// millisecond timestamp — every bit that separates two ids minted in the same millisecond lives
/// after them. At ten, two threads started in that millisecond would be handed the same branch and
/// `git worktree add -b` would fail for whichever lost the race. Thirteen keeps the timestamp, so
/// `giskard/worktree-*` still lists in creation order, and adds three characters of randomness —
/// 15 bits, which turns a certainty into roughly one in 32 000 for a same-millisecond pair. What
/// remains fails loudly at creation, carrying Git's own message; it cannot corrupt anything.
///
/// The upper bound is the Git status row on a phone. Its shortener sheds the leading path first and
/// keeps the tail, so once the tail outgrows the row's budget the `worktree-` marker is what goes:
/// the branch renders as a bare `…76cpvefa9bx2f6dhph711`. Measured at 360px with the row at its
/// most crowded — ahead/behind, file count and diffstat all present — thirteen is the widest that
/// still renders as `…/worktree-<id>`. The full 26-character ULID fits without clipping too; it
/// just stops saying what kind of branch it is.
const BRANCH_ID_CHARS: usize = 13;

#[derive(Debug)]
pub enum WorktreeError {
    /// The workspace is not a Git repository, so there is nothing to branch from.
    NotARepository(String),
    /// Git refused the operation. Carries git's own message, which names the branch or path that
    /// collided and is what the user needs to act on.
    Git(String),
    /// Git could not be run at all, or took long enough that something is wrong.
    Unavailable(String),
}

impl std::fmt::Display for WorktreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorktreeError::NotARepository(msg)
            | WorktreeError::Git(msg)
            | WorktreeError::Unavailable(msg) => f.write_str(msg),
        }
    }
}

/// The branch a thread's worktree starts on.
///
/// Deliberately opaque. The readable name of a thread is its title, which the user can change at
/// any time; a ref cannot follow that without either going stale or being renamed underneath
/// commits that already exist. Lowercased because refs are case-sensitive in Git but not on macOS
/// or Windows filesystems.
pub fn branch_name(thread_id: ThreadId) -> String {
    let id = thread_id.to_string().to_lowercase();
    let short: String = id.chars().take(BRANCH_ID_CHARS).collect();
    format!("giskard/worktree-{short}")
}

/// Where a thread's worktree lives: inside Giskard's own data directory, never beside the project.
/// A dozen sibling worktrees would bury the project directory they belong to.
pub fn worktree_path(data_dir: &Path, project_id: &str, thread_id: ThreadId) -> PathBuf {
    data_dir
        .join("projects")
        .join(project_id)
        .join("worktrees")
        .join(thread_id.to_string())
}

/// Create the worktree and its branch, and record what cleanup will need later.
///
/// Fails rather than falling back to the project's checkout: a thread that silently runs
/// unisolated looks isolated in the UI, which is worse than an error the user can act on.
pub async fn create(
    repo_root: &Path,
    path: &Path,
    branch: &str,
) -> Result<ThreadWorktree, WorktreeError> {
    if !is_git_repository(repo_root).await? {
        return Err(WorktreeError::NotARepository(format!(
            "{} is not a Git repository, so this thread cannot be isolated in a worktree",
            repo_root.display()
        )));
    }
    // Where the project sits inside its repository. Git can only check out a whole repository, so
    // for a project rooted in a subdirectory the worktree is the repository root and the thread
    // works in the same subdirectory beneath it. Resolved before anything is created, because a
    // project directory that is not in the repository's committed content has no counterpart in a
    // worktree and the thread would have nowhere to run.
    let relative_root = project_root_within_repository(repo_root).await?;

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|error| {
            WorktreeError::Unavailable(format!("could not create {}: {error}", parent.display()))
        })?;
    }

    // A repository with no commits is a supported case, not a failure: Git creates the worktree on
    // an orphan branch ("No possible source branch, inferring '--orphan'"). Older Git errors here
    // instead, which surfaces as the hard failure below with Git's own wording.
    let add = run_git(
        repo_root,
        &["worktree", "add", "-b", branch, &path.to_string_lossy()],
        GIT_CHECKOUT_TIMEOUT,
    )
    .await?;
    if !add.status.success() {
        return Err(WorktreeError::Git(stderr_or(
            &add,
            "git worktree add failed",
        )));
    }

    // Resolved from inside the new worktree rather than assembled from `repo_root`: when the
    // project directory is itself a linked worktree its `.git` is a pointer file, not the
    // repository, and every path built from it would be wrong.
    //
    // The worktree exists from here on, so a failure below has to take it back down rather than
    // leave an orphan checkout and a branch belonging to a thread that was never created.
    let resolved = async {
        Ok::<_, WorktreeError>((
            rev_parse(path, "--git-common-dir").await?,
            rev_parse(path, "--git-dir").await?,
            // An unborn repository has no `HEAD` and is a supported case, so this failure is not
            // fatal — but Git failing to answer at all lands here too, and the two are
            // indistinguishable in the record. Logged so the empty `base_commit` is diagnosable.
            match rev_parse(path, "HEAD").await {
                Ok(sha) => Some(sha),
                Err(error) => {
                    debug!(
                        worktree = %path.display(),
                        branch,
                        %error,
                        "could not resolve the worktree's base commit"
                    );
                    None
                }
            },
        ))
    }
    .await;
    let (common_dir, git_dir, base_commit) = match resolved {
        Ok(resolved) => resolved,
        Err(error) => {
            rollback_worktree(repo_root, path, branch).await;
            return Err(error);
        }
    };

    // The subdirectory has to be there for the thread to run in it. It will not be if the project
    // directory is untracked or ignored in its repository, since a worktree carries committed
    // content only — so this is a real condition to report, not one to paper over by creating an
    // empty directory the agent would then work in.
    let workspace = match &relative_root {
        None => None,
        Some(relative) => {
            let workspace = path.join(relative);
            if !workspace.is_dir() {
                rollback_worktree(repo_root, path, branch).await;
                return Err(WorktreeError::Git(format!(
                    "{} is not in the committed content of its repository, so an isolated thread \
                     has no directory to work in ({} does not exist in a fresh checkout)",
                    repo_root.display(),
                    relative.display()
                )));
            }
            Some(workspace.to_string_lossy().into_owned())
        }
    };

    debug!(
        worktree = %path.display(),
        branch,
        base_commit = base_commit.as_deref().unwrap_or("(unborn)"),
        workspace = workspace.as_deref().unwrap_or("(worktree root)"),
        "created thread worktree"
    );

    Ok(ThreadWorktree {
        path: path.to_string_lossy().into_owned(),
        workspace,
        branch: branch.to_string(),
        base_commit,
        repo_root: repo_root.to_string_lossy().into_owned(),
        common_dir,
        git_dir,
    })
}

/// Where the project directory sits inside its repository, or `None` when it *is* the top level.
///
/// Both sides are canonicalized before subtracting: `--show-toplevel` resolves symlinks, and a
/// project path that reaches its repository through one would otherwise fail to match a prefix it
/// genuinely has.
async fn project_root_within_repository(
    repo_root: &Path,
) -> Result<Option<PathBuf>, WorktreeError> {
    let toplevel = PathBuf::from(rev_parse(repo_root, "--show-toplevel").await?);
    let canonical_toplevel = toplevel.canonicalize().unwrap_or(toplevel);
    let canonical_project = repo_root
        .canonicalize()
        .unwrap_or_else(|_| repo_root.to_path_buf());
    match canonical_project.strip_prefix(&canonical_toplevel) {
        Ok(relative) if relative.as_os_str().is_empty() => Ok(None),
        Ok(relative) => Ok(Some(relative.to_path_buf())),
        // Not a prefix at all. Nothing sane produces this — `--show-toplevel` answered for this
        // very directory — so treat the pair as unusable rather than guessing a layout.
        Err(_) => Err(WorktreeError::Git(format!(
            "{} is not inside its own repository root {}",
            canonical_project.display(),
            canonical_toplevel.display()
        ))),
    }
}

/// Take a half-created worktree back down, so a failed creation leaves nothing behind.
async fn rollback_worktree(repo_root: &Path, path: &Path, branch: &str) {
    let path = path.to_string_lossy().into_owned();
    match run_git(
        repo_root,
        &["worktree", "remove", "--force", &path],
        GIT_CHECKOUT_TIMEOUT,
    )
    .await
    {
        Ok(output) if output.status.success() => {}
        Ok(output) => {
            warn!(
                repo = %repo_root.display(),
                worktree = %path,
                branch,
                action = "rollback_worktree",
                stderr = %stderr_or(&output, ""),
                "could not roll back a half-created worktree"
            )
        }
        Err(error) => {
            warn!(
                repo = %repo_root.display(),
                worktree = %path,
                branch,
                action = "rollback_worktree",
                %error,
                "could not roll back a half-created worktree"
            )
        }
    }
    // `worktree remove` above de-registers a checkout even when its directory is already gone, but
    // a rollback runs against state that is by definition half-built: prune so a registration it
    // could not remove cannot keep the branch alive.
    prune(repo_root).await;
    // The branch outlives `worktree remove`, and nothing has run in it.
    match run_git(repo_root, &["branch", "-D", branch], GIT_QUERY_TIMEOUT).await {
        Ok(output) if output.status.success() => {}
        Ok(output) => {
            warn!(
                repo = %repo_root.display(),
                worktree = %path,
                branch,
                action = "rollback_branch",
                stderr = %stderr_or(&output, ""),
                "could not roll back a half-created branch"
            )
        }
        Err(error) => warn!(
            repo = %repo_root.display(),
            worktree = %path,
            branch,
            action = "rollback_branch",
            %error,
            "could not roll back a half-created branch"
        ),
    }
}

/// Remove the worktree directory, leaving the branch alone.
///
/// Git refuses while the tree holds modified or untracked files unless `force` is set; ignored
/// files never block it, so a `target/` or `node_modules/` left behind does not make a thread
/// undeletable.
pub async fn remove(worktree: &ThreadWorktree, force: bool) -> Result<(), WorktreeError> {
    let mut args = vec!["worktree", "remove"];
    if force {
        args.push("--force");
    }
    args.push(&worktree.path);
    let output = run_git(Path::new(&worktree.repo_root), &args, GIT_CHECKOUT_TIMEOUT).await?;
    if !output.status.success() {
        let message = stderr_or(&output, "git worktree remove failed");
        // Cleanup is two commands — this one, then `branch -D` — and the second can fail after this
        // one has succeeded. Nothing persists that half-state, so the only way back is to run the
        // pair again; a removal that reported "already not a worktree" as a failure would make the
        // retry stop here forever and strand the branch. Being already gone is the outcome asked
        // for. Note this is *not* the same as pointing at a checkout Git refuses to remove — "is a
        // main working tree" stays an error, because that means the record is wrong.
        if message.contains("is not a working tree") {
            debug!(
                worktree = %worktree.path,
                "worktree was already removed; treating the removal as done"
            );
            return Ok(());
        }
        return Err(WorktreeError::Git(message));
    }
    Ok(())
}

/// Delete the branch the thread started on, returning the commit it pointed at so the reflog window
/// remains usable. Nothing else is touched: branches the agent created during the thread live in
/// the shared repository and are the user's now.
pub async fn delete_branch(worktree: &ThreadWorktree) -> Result<Option<String>, WorktreeError> {
    let output = run_git(
        Path::new(&worktree.repo_root),
        &["branch", "-D", &worktree.branch],
        GIT_QUERY_TIMEOUT,
    )
    .await?;
    if !output.status.success() {
        let message = stderr_or(&output, "git branch -D failed");
        // A branch the agent renamed or deleted itself is not an error worth failing a deletion
        // over; there is simply nothing of ours left to remove.
        if message.contains("not found") {
            warn!(
                repo = %worktree.repo_root,
                worktree = %worktree.path,
                branch = %worktree.branch,
                action = "delete_thread_branch",
                "thread branch was already gone"
            );
            return Ok(None);
        }
        return Err(WorktreeError::Git(message));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let was = giskard_git_parser::parse_deleted_branch_sha(&stdout);
    if was.is_none() {
        // The branch is gone either way, so this is not a failure — but it is the one outcome where
        // nothing records where it pointed, and it must not be confused with the already-gone case
        // above, which never had a commit to report. Git deleting a symbolic ref, a reworded
        // report, or a locale that is not `C` all land here.
        warn!(
            repo = %worktree.repo_root,
            worktree = %worktree.path,
            branch = %worktree.branch,
            action = "delete_thread_branch",
            report = %stdout.trim(),
            "deleted the thread's branch but could not read the commit it pointed at"
        );
    }
    Ok(was)
}

/// Drop Git's metadata for worktrees whose directories are gone, so a thread deleted from under us
/// does not leave the repository listing a path that no longer exists.
pub async fn prune(repo_root: &Path) {
    match run_git(repo_root, &["worktree", "prune"], GIT_QUERY_TIMEOUT).await {
        Ok(output) if output.status.success() => {}
        Ok(output) => warn!(
            repo = %repo_root.display(),
            stderr = %stderr_or(&output, ""),
            "git worktree prune failed"
        ),
        Err(error) => {
            warn!(repo = %repo_root.display(), %error, "could not run git worktree prune")
        }
    }
}

/// Whether the worktree holds changes that removing it would discard. Ignored files are excluded,
/// exactly as `git worktree remove` itself excludes them.
///
/// Fallible on purpose. This number is read as "how much work is at stake", and the caller deletes
/// without asking when it is zero — so a Git failure answered with `0` would not be a missing
/// answer, it would be a confident wrong one, and the deletion that follows is forced and
/// irreversible.
pub async fn uncommitted_changes(worktree: &ThreadWorktree) -> Result<usize, WorktreeError> {
    // A checkout that is not there holds no uncommitted work. That is a definite zero rather than a
    // failure to look, and the distinction matters: a user who removed the directory by hand must
    // still be able to delete the thread without forcing. What deleting would destroy in that case
    // lives on the branch, which is probed separately and against the repository, not the checkout.
    if !Path::new(&worktree.path).exists() {
        return Ok(0);
    }
    // The same request the Git status row makes, parsed by the same parser (spec §5: parsing lives
    // in `giskard-git-parser`). Counting lines of `--porcelain` v1 here instead would be a second,
    // subtly different reading of the tree: v1 encodes every state in one two-character code, and
    // without `-z` a path with a space or a newline comes back quoted rather than verbatim. It
    // would also let the number in the deletion warning disagree with the row the user is looking
    // at, which is the one place the two must match.
    //
    // `--untracked-files=normal` for the same reason: an untracked directory counts once, as the
    // row shows it and as `git worktree remove` treats it.
    let output = run_git(
        Path::new(&worktree.path),
        &[
            "status",
            "--porcelain=v2",
            "-z",
            "--untracked-files=normal",
            "--",
            ".",
        ],
        GIT_QUERY_TIMEOUT,
    )
    .await?;
    if !output.status.success() {
        return Err(WorktreeError::Git(stderr_or(
            &output,
            "git status --porcelain=v2 failed",
        )));
    }
    Ok(giskard_git_parser::parse_git_status(&output.stdout)
        .files
        .len())
}

/// How many commits on the thread's branch exist nowhere else — what deleting the branch would
/// actually destroy.
///
/// `git branch -d`'s verdict is the wrong question: it compares against HEAD and upstream only, so
/// an agent that parked its work on a branch of its own still reports "not fully merged" when
/// nothing at all would be lost.
/// Fallible for the same reason as [`uncommitted_changes`]: zero is the answer that authorises an
/// unconfirmed deletion, so it has to mean "Git looked and found none", never "Git could not look".
pub async fn commits_reachable_from_nowhere_else(
    worktree: &ThreadWorktree,
) -> Result<usize, WorktreeError> {
    let exclude = format!("--exclude=refs/heads/{}", worktree.branch);
    let output = run_git(
        Path::new(&worktree.repo_root),
        &[
            "rev-list",
            "--count",
            &worktree.branch,
            "--not",
            &exclude,
            // `--all` examines every worktree's HEAD by default, and this branch is checked out in
            // the worktree we are about to remove — so without this its own commits count as
            // "reachable from somewhere else" and the confirmation would always report that nothing
            // would be lost.
            "--single-worktree",
            "--all",
        ],
        GIT_QUERY_TIMEOUT,
    )
    .await?;
    if !output.status.success() {
        let message = stderr_or(&output, "git rev-list --count failed");
        // A branch the agent renamed or deleted itself holds nothing this deletion could destroy,
        // and `delete_branch` already treats that state as "nothing of ours left to remove". The
        // two have to agree: a thread that can be deleted must also be able to say what deleting it
        // would cost, or the preflight refuses a deletion the deletion itself would allow.
        if message.contains("unknown revision") || message.contains("bad revision") {
            warn!(
                branch = %worktree.branch,
                "thread branch is already gone; reporting no unreachable commits"
            );
            return Ok(0);
        }
        return Err(WorktreeError::Git(message));
    }
    let counted = String::from_utf8_lossy(&output.stdout);
    counted.trim().parse().map_err(|_| {
        WorktreeError::Git(format!(
            "git rev-list --count returned {:?}, which is not a number",
            counted.trim()
        ))
    })
}

async fn is_git_repository(dir: &Path) -> Result<bool, WorktreeError> {
    repository_probe(rev_parse(dir, "--git-dir").await)
}

/// Decide what a `rev-parse --git-dir` result says about the directory.
///
/// Only Git's own refusal means "not a repository". A failure to run Git at all, or one that never
/// finished, answers nothing — and reporting it as "not a repository" would send the user to fix
/// their project when the problem is the server's Git, and would turn a `503` into a client error.
///
/// Split out from [`is_git_repository`] so the distinction can be tested without an unrunnable Git.
fn repository_probe(result: Result<String, WorktreeError>) -> Result<bool, WorktreeError> {
    match result {
        Ok(_) => Ok(true),
        Err(WorktreeError::Git(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

async fn rev_parse(dir: &Path, what: &str) -> Result<String, WorktreeError> {
    let output = run_git(dir, &["rev-parse", what], GIT_QUERY_TIMEOUT).await?;
    if !output.status.success() {
        return Err(WorktreeError::Git(stderr_or(
            &output,
            &format!("git rev-parse {what} failed"),
        )));
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if value.is_empty() {
        return Err(WorktreeError::Git(format!(
            "git rev-parse {what} was empty"
        )));
    }
    // `--git-dir` and `--git-common-dir` answer relative to the working directory when they can, so
    // resolve them against it before storing paths that outlive this call.
    let path = Path::new(&value);
    if path.is_relative() && what.starts_with("--git") {
        return Ok(dir.join(path).to_string_lossy().into_owned());
    }
    Ok(value)
}

async fn run_git(
    dir: &Path,
    args: &[&str],
    timeout: Duration,
) -> Result<std::process::Output, WorktreeError> {
    let mut cmd = TokioCommand::new("git");
    cmd.current_dir(dir)
        .args(args)
        // Tokio leaves a child running when the future owning it is dropped, so without this a
        // timed-out checkout would keep writing into a directory we are about to abandon.
        .kill_on_drop(true)
        // Git's `fatal:` messages are translated; the callers below match on them.
        .env("LC_ALL", "C");
    tokio::time::timeout(timeout, cmd.output())
        .await
        .map_err(|_| {
            WorktreeError::Unavailable(format!(
                "git {} timed out in {}",
                args.first().copied().unwrap_or("command"),
                dir.display()
            ))
        })?
        .map_err(|error| WorktreeError::Unavailable(format!("failed to run git: {error}")))
}

fn stderr_or(output: &std::process::Output, fallback: &str) -> String {
    let text = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if text.is_empty() {
        fallback.to_string()
    } else {
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two failures look alike at the call site and must not be answered alike: one is the
    /// user's project, the other is the server's Git. Collapsing them tells the user to fix a
    /// repository that is fine.
    #[test]
    fn only_gits_own_refusal_means_the_directory_is_not_a_repository() {
        assert!(repository_probe(Ok("/repo/.git".into())).unwrap());
        assert!(
            !repository_probe(Err(WorktreeError::Git("not a git repository".into()))).unwrap(),
            "git ran and refused, which is the one answer that means it is not a repository"
        );
        for unanswered in [
            WorktreeError::Unavailable("could not run git".into()),
            WorktreeError::NotARepository("already decided".into()),
        ] {
            assert!(
                repository_probe(Err(unanswered)).is_err(),
                "a probe that never got an answer must not report one"
            );
        }
    }

    #[test]
    fn branch_name_is_lowercase_and_prefixed() {
        let id: ThreadId = "01KYX76CPVEFA9BX2F6DHPH711".parse().unwrap();
        assert_eq!(branch_name(id), "giskard/worktree-01kyx76cpvefa");
    }

    /// Two threads whose ids share a timestamp is the case worth pinning: a ULID's first ten
    /// characters *are* that timestamp, so a name built from them alone would be identical for both
    /// and the second thread could never be created. Ids that differ earlier — the easy case — prove
    /// nothing about the width, since any width tells those apart.
    #[test]
    fn branch_names_differ_for_threads_created_in_the_same_millisecond() {
        let a: ThreadId = "01KYX76CPVEFA9BX2F6DHPH711".parse().unwrap();
        let b: ThreadId = "01KYX76CPVZZZZZZZZZZZZZZZZ".parse().unwrap();
        assert_eq!(
            a.to_string()[..10],
            b.to_string()[..10],
            "the fixture must share a millisecond, or it tests nothing"
        );
        assert_ne!(branch_name(a), branch_name(b));
    }

    #[test]
    fn worktree_path_is_under_the_data_dir_not_the_project() {
        let id: ThreadId = "01KYX76CPVEFA9BX2F6DHPH711".parse().unwrap();
        let path = worktree_path(Path::new("/var/lib/giskard"), "01PROJECT", id);
        assert_eq!(
            path,
            PathBuf::from(
                "/var/lib/giskard/projects/01PROJECT/worktrees/01KYX76CPVEFA9BX2F6DHPH711"
            )
        );
    }

    /// Everything after `git worktree add` runs against a checkout that already exists, so a
    /// failure there has to take both the checkout and its branch back down — otherwise a thread
    /// that was never created leaves a stray worktree and a ref nothing points to.
    #[tokio::test]
    async fn rollback_removes_both_the_checkout_and_the_branch() {
        let tmp = tempfile::TempDir::new().unwrap();
        let repo = tmp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        for args in [
            vec!["init", "-q", "-b", "main"],
            vec!["config", "user.email", "t@example.invalid"],
            vec!["config", "user.name", "T"],
            vec!["commit", "-q", "--allow-empty", "-m", "initial"],
        ] {
            let out = std::process::Command::new("git")
                .current_dir(&repo)
                .args(&args)
                .output()
                .expect("run git");
            assert!(out.status.success(), "git {args:?} failed");
        }
        let path = tmp.path().join("wt");
        let branch = "giskard/worktree-01rollback";
        let worktree = create(&repo, &path, branch).await.expect("create");

        rollback_worktree(&repo, Path::new(&worktree.path), branch).await;

        assert!(!path.exists(), "the checkout survived the rollback");
        let branches = std::process::Command::new("git")
            .current_dir(&repo)
            .args(["branch", "--list", branch])
            .output()
            .expect("run git");
        assert!(
            String::from_utf8_lossy(&branches.stdout).trim().is_empty(),
            "the branch survived the rollback"
        );
    }
}
