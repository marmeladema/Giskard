# Per-thread Git worktrees

A thread can be started in a Git worktree of its own, so what it edits is invisible to the project's
checkout and to every other thread. The choice is made once, on the draft, from the **Git checkout**
dropdown at the right of the **Git status row** above the composer: **Shared** (the default) or
**Worktree**. It sits on that row because that row describes the very tree the choice changes — the
branch a worktree would start from, and the changed files it would leave behind. It is not a project
setting, it does not carry over to the next draft, and it cannot be changed afterwards, because a
thread's working directory is fixed the moment the thread exists.

Hovering the dropdown says what the selected option *is* — a worktree starts from the last commit, on
a branch of its own. What it would *cost* is printed on a line under the row instead of hovered:
choose **Worktree** while the project has uncommitted work and it says how much of that work stays
in the project's checkout. That one is printed because it is the fact you need before sending, and a
tooltip cannot be read on a phone. It appears only when something is actually at stake, so the row
stays quiet on a clean tree or a shared checkout.

A project whose workspace is not a Git repository has no status row and so no dropdown: there is
nothing to branch from, and such a thread simply uses the project's directory.

It is a picker rather than a switch because where a thread's working tree comes from has more than
two possible answers — a checkout the thread genuinely owns, rather than a second view of the
project's repository, is a different strategy again, and a strategy the server does not implement is
refused rather than quietly downgraded. Today the list is these two.

This document covers what that isolation is and is not, how far it reaches, what does not come
across, where worktrees and their branches live, what the agent may do to Git and what still needs
your approval, how to get the work out, what archiving and deleting do, and where the v1 edges are.

## What isolation means, and what it does not

A linked worktree is a second working directory attached to the *same* repository. Files are
isolated. The repository is not.

Shared with the project's checkout, immediately and by design:

- the object store — every commit the agent writes is present in the main checkout the instant it
  exists;
- refs — branches, tags and stashes are one namespace, so a branch the agent creates shows up in
  `git branch` in the main checkout with no push, no fetch and no copying;
- config, hooks, remotes.

Isolated:

- the working tree itself — the checked-out files;
- `HEAD`, the index, the reflog and in-progress rebase/merge state, which live in the worktree's own
  private directory under `.git/worktrees/<thread_id>/`.

The practical consequence cuts both ways. The agent cannot break your working state — it cannot stage
over your index, and it cannot check out a branch on top of your edits. It *can* create refs you will
see. That is not a leak; it is the delivery mechanism (see [Getting work out](#getting-work-out)).

Your *working* state, though, is not the whole repository, and a worktree does not make it one. What
the boundary does and does not cover is set out in
[What this does not protect](#what-this-does-not-protect).

### The unit of isolation is the thread and everything under it

A sub-agent works in the worktree of the thread that spawned it, and never gets one of its own. That
is what you want: a sub-agent is sent to do part of the parent's task, so it has to see the parent's
uncommitted work and the parent has to see what it produced. Giving each child a checkout of its own
would isolate them from the very work they were asked to help with.

So the isolation boundary encloses a thread *and its whole sub-agent tree*. Everything Giskard
resolves for a child resolves to that same worktree: its Git status row, the cwd handed to the
harness when the child is opened or resumed, and the Git directories its own turns may write.

Ownership is separate from use. The worktree belongs to the thread that created it, and only that
thread's deletion removes it — deleting a sub-agent leaves its parent's checkout and branch alone.

Everything the UI reads for a thread reads that workspace: the Git status row and its diffs, and the
file views behind the paths in the transcript — syntax-highlighted source, raw download, image
preview. A path the agent mentions opens the agent's copy, and a file that exists only in the
worktree is still a working link rather than plain text.

## What does not come across

A new worktree is `HEAD`, exactly. Nothing else follows it:

(A repository with no commits yet is supported and is the one case with no `HEAD` to start from:
Git puts the worktree on an orphan branch, and the checkout is simply empty. Everything below still
holds — nothing comes across, because there is nothing committed to come across.)

| in the project's checkout | in a new thread worktree |
|---|---|
| tracked file, committed | present |
| tracked file, edited but not committed | present **as committed** — your edit is not there |
| tracked file, staged but not committed | present as committed — the staged version is not there |
| untracked file (a scratch script, a fixture) | absent |
| ignored file (`.env`, `node_modules/`, `target/`, `.venv/`) | absent |

The toggle says so at the point of decision: with uncommitted work in the project, the hint reads
*"Starts from the last commit, on a branch of its own. Your 3 uncommitted changes stay in the
project's checkout."* If the agent needs an edit you have not committed, commit it — or stash and
re-apply it in the worktree — before starting the thread.

### The first build is cold, on purpose

Because ignored files do not come across, build output does not either. A worked example, a Rust
project:

```text
~/dev/myproject         target/  4.1 GB, 380 crates compiled
~/.giskard/projects/01J…/worktrees/01K…   target/  absent
```

The thread's first `cargo build` compiles all 380 dependencies. Likewise a Node project needs
`npm ci` before anything runs, and a service that reads `.env` will fail to start until the agent is
given the values it needs.

This is deliberate and is not a gap waiting to be filled. Copying build state across would make every
thread's build depend on the state of your checkout at the moment the thread happened to start —
which is precisely the class of "works here, not there" failure the isolation exists to remove. The
first build in a worktree is hermetic, and a hermetic build that takes four minutes is a better
report than a fast one you cannot trust.

Speed belongs one level down, in a cache shared by every worktree rather than copied into each:
`sccache`, a shared `CARGO_HOME`/`CARGO_TARGET_DIR`, an npm or pnpm store, a `ccache`. These live
outside the workspace, so the sandbox has to be told about them — in Codex's config:

```toml
[sandbox_workspace_write]
writable_roots = ["/home/you/.cache/sccache", "/home/you/.cargo"]
```

Giskard reads that list when it starts the project's harness and passes it through with every turn,
so a cache declared there is writable from the worktree. **A cache that is not declared fails
quietly**: `sccache` treats an unwritable cache directory as a miss and compiles anyway, so the only
symptom is that nothing ever gets faster. If a shared cache seems to be doing nothing, check that its
directory is in `writable_roots` before looking anywhere else.

Note that these roots are only sent under the **Auto approve** preset, which is Codex behaviour
Giskard does not change for isolated threads. Under **Ask first** the turn is read-only until you
approve each command, so there is nothing to widen; under **⚠ Full Access** there is no sandbox to
widen either.

## Where worktrees and branches live

The checkout goes under Giskard's data directory, never beside the project:

```text
$GISKARD_DATA_DIR/projects/<project_id>/worktrees/<thread_id>/
```

Sibling worktrees (`../myproject-thread-3/`) were rejected deliberately: a project with a dozen
threads would bury the directory it belongs to, and every editor's file picker and every
`rg` across the parent directory would fill with copies.

**If your project is a subdirectory of its repository** — a package inside a monorepo — the checkout
is still the whole repository, because that is the only thing Git can check out. The thread works in
the matching subdirectory beneath it:

```text
project      ~/dev/monorepo/packages/api
checkout     $GISKARD_DATA_DIR/projects/<project_id>/worktrees/<thread_id>/
thread works …/worktrees/<thread_id>/packages/api
```

So a path means the same file with isolation as without it, and the thread starts where the project
does. Two consequences worth knowing: the checkout on disk is the size of the whole repository, not
of your package, and the rest of the repository is present in it — the agent's own copy, not yours,
but it is there if the agent walks up out of its directory. A project directory that is *not* in the
repository's committed content — untracked or ignored — cannot be isolated at all, since a fresh
checkout has no such directory to work in; starting the thread fails and says so.

The branch is named from the thread's ULID:

```text
giskard/worktree-01k9x2m4qpz8v
```

Thirteen characters of the ULID, lowercased (refs are case-sensitive in Git but not on macOS or
Windows filesystems). The first ten of those are the thread's millisecond timestamp, so these
branches sort in creation order under `git branch --list 'giskard/worktree-*'`; the three after it
are random, so two threads started in the same millisecond almost certainly get different branches —
about one such pair in 32 000 collides, and that fails loudly at creation rather than quietly.
Thirteen rather than the whole ULID because the Git status row on a phone drops the leading path first: a
longer name renders there as a bare `…76cpvefa9bx2f6dhph711`, with nothing left to say it is a
worktree branch.

The name is deliberately opaque rather than derived from the thread's title. A title is editable at
any time; a ref that followed it would either go stale the first time you renamed the thread or would
have to be renamed underneath commits that already exist. So renaming a thread never touches its
branch: the two are unlinked on purpose, and the branch is stable for as long as the commits on it
are.

**The recorded branch is only the starting point.** Giskard remembers it because it is the branch it
created and therefore the branch it may remove. What the thread does afterwards is the agent's and
yours: if the agent branches off, switches, or renames, Giskard does not follow it and does not
manage those refs.

## What the agent may and may not do to Git

**An isolated thread's permissions are exactly an ordinary thread's.** Isolation decides *where* a
thread works; the permission preset still decides what it may do there. Choosing a worktree grants
nothing extra and takes nothing away.

In practice that means editing files in the worktree runs freely, and Git commands that write the
repository do not. Codex keeps `.git` read-only inside a writable root, and a linked worktree's real
Git directory lives under the project's `.git/` — outside the workspace root entirely — so neither is
writable. Under **Auto approve**, `commit`, `branch`, `switch`, `merge`, `rebase`, `stash` and `tag`
therefore escalate to an approval prompt, exactly as they would for a thread with no worktree.
Under **Ask first** nothing changes at all, since every command was already prompted.

This is worth being clear about, because it is the main thing isolation does *not* buy: you get
parallel threads that cannot collide on each other's files, not an agent that commits unattended.

Anything needing the network — `fetch`, `pull`, `push` to a remote — follows the preset too, and
worth stating exactly because it is easy to assume otherwise:

| preset | Git writes | network |
|---|---|---|
| **Ask first** | prompts | prompts |
| **Auto approve** | prompts | prompts |
| **⚠ Full Access** | runs | **runs** — no sandbox, no prompt |

Full Access is `:danger-full-access` with approvals off, so nothing is denied and nothing is asked;
an agent can push to a remote. That is what the preset means, and isolation does not restrain it —
a worktree is a directory, not a boundary.

### What Git itself refuses

Approval is not the only limit. Once you *do* approve a command, Git still refuses every route to a
branch checked out somewhere else. Measured from a thread worktree with `main` checked out in the
project:

```console
$ git switch main
fatal: 'main' is already used by worktree at '/home/you/dev/myproject'

$ git branch -f main HEAD
fatal: cannot force update the branch 'main' used by worktree at '/home/you/dev/myproject'

$ git push . HEAD:main
 ! [remote rejected] HEAD -> main (branch is currently checked out)

$ git fetch . HEAD:main
fatal: refusing to fetch into branch 'refs/heads/main' checked out at '/home/you/dev/myproject'
```

Those are the four commands an agent reaches for when told "merge this into main", and it will hit
one of them and report it. Only raw plumbing (`git update-ref`) gets through, and only after those
four refusals — not something anyone arrives at by accident.

A branch that is *not* checked out anywhere is fair game, on purpose. An agent asked to work on
`feature/x` should be able to check it out.

### What this does not protect

Stated plainly, because a worktree looks like more of a boundary than it is: **it isolates files,
not the repository.** Approving a Git command in an isolated thread has the same reach as approving
one in any other thread — it runs against the shared object store and the shared refs, because the
worktree shares both.

So the risk that remains is the ordinary one: what you agree to. If you approve a command that
forces a ref your checkout is sitting on, the ref is recoverable from the reflog, but your
`git status` starts reporting changes you never made — and the instinctive cleanup for a confusing
dirty state (`git reset --hard`, `git checkout .`) destroys uncommitted work. Git's four refusals
above bound that, and **Ask first** keeps every command in front of you.

## Getting work out

Whatever the agent committed — with your approval, since commits prompt — is already in your
repository. There is nothing to transfer:

```bash
# from the project's checkout — the branch is simply there
git log giskard/worktree-01k9x2m4qpz8v
git merge giskard/worktree-01k9x2m4qpz8v
git cherry-pick <sha>
```

Other routes, when you want them:

- **Push from the worktree.** Whether the agent can push is the preset's call, not the worktree's
  (see the table above): prompted under **Ask first** and **Auto approve**, unprompted under
  **⚠ Full Access**. You can always do it yourself from the worktree directory, or from the main
  checkout since the branch is local either way.
- **A branch of your own.** `git branch mywork giskard/worktree-01k9x2m4qpz8v` gives the work a name
  that survives deleting the thread — see below.
- **Uncommitted work in the worktree** is only in the worktree. `git -C <worktree-path> diff` reads
  it from anywhere, but if it matters, commit it before deleting the thread.

## Lifecycle

| action | worktree | branch | uncommitted work in it |
|---|---|---|---|
| thread runs | in place | in place | in place |
| **archive** | left in place (v1) | kept | kept |
| **delete thread** | removed | **deleted** | **lost** |
| **delete project** | removed | deleted | lost |
| creation fails partway | rolled back | rolled back | — |

Deleting a thread takes its worktree and the branch Giskard recorded when it created it — if that ref
is still there. Rename it and Giskard no longer has anything to delete: the recorded name is gone,
the deletion says so and moves on, and the renamed branch is yours to keep or remove. Branches the
agent made during the thread are *not* touched either: they live in the shared repository and are
yours now.

Before deleting, Giskard asks the repository what would actually be destroyed — uncommitted changes
in the worktree, and commits on the thread's branch reachable from no other ref — across the thread
and every sub-agent under it. The confirmation card names it while the question is still open:

> This also destroys 2 uncommitted changes and 5 commits on no other ref in worktree
> giskard/worktree-01k9x2m4qpz8v.

Confirming then deletes anyway; that is where the decision is made. The server keeps its own guard
for the case where the card could not ask — it refuses with *"deleting this thread would destroy …"*
and the card asks you to confirm once more.

"Commits on no other ref" is the question the confirmation asks, and it is not Git's `branch -d`
verdict. Those measure different things: `branch -d` decides whether a *branch* may be deleted, by
comparing against `HEAD` and its upstream only, while the confirmation asks whether any *commit*
would stop being reachable. So `branch -d` refuses a branch whose commits another ref keeps
perfectly safe — an agent that parked its work on a branch of its own hits exactly that.

*Ref*, not *branch*, is deliberate: a tag or a stash keeps commits just as well as a branch does, so
a thread whose tip is only tagged is reported as losing nothing, and it loses nothing — even though
`git branch -d` on that same branch still refuses it as unmerged.

Deleting a **project** sweeps every thread worktree in it without per-thread confirmation. That
confirmation is project-scoped by design — a single thread holding work must not be able to leave a
project half-deleted.

If a thread's worktree directory has been removed from underneath Giskard, deletion still works: the
removal is attempted anyway, because that is what de-registers the stale entry Git is still holding —
and while it holds one, the branch cannot be deleted.

## Troubleshooting

**The toggle is greyed out: "This project is not a Git repository, so there is nothing to branch
from."** The project's workspace is not a repository. `git init` it, or start the thread without
isolation.

**Starting the thread failed and nothing was created.** Isolation fails loudly rather than falling
back to the project's checkout: a thread that silently ran unisolated would look isolated in the UI,
which is worse than an error. The failure carries Git's own message, which names what went wrong —
usually a permission problem on the data directory, or — far more rarely — a branch that already
exists, which needs two threads started in the same millisecond *and* the same three random
characters. Nothing is left behind: a failure after the checkout exists takes both
the checkout and the branch back down. Retry — a new thread gets a new id and a new branch name, so
a retry usually just works. If the same name collides again, look at what is on the colliding branch
before doing anything to it: the `giskard/worktree-` prefix says Giskard made it, not that it is
empty, and it may hold the only copy of an earlier thread's work. Rename it if you want to keep it,
and delete it only once you have checked there is nothing on it you want.

**"'main' is already used by worktree at …"** Expected, and explained above. The branch is checked
out in your main checkout. Either work on a different branch, or switch your own checkout off it.

**The agent asks for approval on `git commit` or `git switch`.** Expected under **Ask first** and
**Auto approve**; under **⚠ Full Access** it simply runs. A worktree isolates files, not the
repository, and grants no permission an ordinary thread lacks — so repository writes prompt here
exactly as they do anywhere else, and are exempted here exactly as they are anywhere else.

**The agent asks for approval on `git fetch`, or it fails.** Expected under **Ask first** and
**Auto approve**, which put network behind an approval; under **⚠ Full Access** it simply runs. None
of that is worktree-specific — it is the preset.

**The first build in a thread is slow, or a shared cache is not being used.** Expected for the first;
for the second, check the cache directory is in Codex's `sandbox_workspace_write.writable_roots`, and
that the thread runs under **Auto approve**.

**The Git status row above the composer disagrees with the project.** It should: for an isolated
thread the row, its file list and its diffs are scoped to that thread's worktree, not the project's
checkout.

**Something referenced the worktree path and it is gone.** Giskard removes the checkout when the
thread is deleted. Commits on branches that still exist are unaffected; they are in the shared object
store.

**A sub-agent's status row shows its parent's branch.** Correct: a sub-agent works in the worktree of
the thread that spawned it, and its own turns run there too. Only the thread that created the
worktree owns it, so deleting the sub-agent leaves it alone.

## v1 limits

- **No seeding.** Uncommitted, untracked and ignored files do not come across, and there is no
  opt-in to carry them. Ignored files are a permanent non-goal (see above); carrying uncommitted
  tracked edits may become an opt-in later.
- **No submodule initialisation.** A repository with submodules gets an empty submodule directory in
  the worktree.
- **No merge or PR affordance.** Giskard will not merge the thread's branch or open a pull request;
  use Git.
- **Network follows the preset, not the worktree.** Under **Ask first** and **Auto approve** a
  `fetch` waits for your approval, so an unattended thread cannot rebase onto an updated upstream;
  under **⚠ Full Access** it runs unprompted. Isolation neither grants nor withholds it.
- **Git writes still prompt.** Isolation grants no extra permission, so committing under **Auto
  approve** raises an approval prompt just as it does without a worktree. Letting a thread do
  isolate-local Git work unprompted needs a repository the thread genuinely owns, not a worktree
  sharing the project's; that is a separate design.
- **Archiving does not remove the worktree.** Archiving a thread leaves its checkout on disk;
  deleting the thread is what reclaims it.
- **No disk cap.** Each isolated thread is a full checkout; nothing watches the total.
