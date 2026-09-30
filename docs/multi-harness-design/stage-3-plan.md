# Stage 3 implementation plan: thread operations resolve their own harness target

A follow-up to Stage 2 of [`../multi-harness-design.md`](../multi-harness-design.md). This plan
is written for an implementing agent. Every file, symbol, and string below was verified against
`main` at `9e68172`. Line numbers are for orientation; the symbol or string quoted beside each is
the thing to find.

Read `AGENTS.md` first. Its rules that bind this stage: `cargo fmt --all` and
`cargo clippy --all-targets -- -D warnings` must pass; no `unwrap`/`expect`/`panic!` on runtime
paths; every failure mode gets a test; server failures are logged with `project_id`, `thread_id`,
and the error; documentation is updated in the same change as the code it describes; Markdown is
wrapped at 100 columns.

## Outcome

Three registry methods, `set_thread_archived`, `set_thread_name`, and `delete_thread`, stop
taking the thread's harness declaration and native id from their caller. Each resolves both from
the thread's loaded binding when the thread is live, and from the thread file when it is cold. The
routes stop passing fields they read from `thread.json`, the start-failure cleanup stops carrying
a declaration name through, and an unreachable branch of thread creation is removed.

Nothing observable changes: no route, request, or response is altered, no persisted format moves,
and the browser is untouched. This is a refactor of a pre-existing convention that Stage 2
extended when it added `harness` beside `harness_thread_id` on the same three methods.

## Why

Before Stage 2 the three methods already took `harness_thread_id` by value from the caller and
used it only when the thread was cold, building a `ThreadHandle::detached` from it. Stage 2 added
`harness` in the same shape. The result is an API where every caller must read the thread file
first and can pass a stale or wrong pair, and where the registry silently prefers the loaded
binding over what it was given. The registry already holds both sources of truth: the coordinator's
`LoadedThreadBinding` (Stage 2 gave it `harness`) and the store. Resolving inside the registry
removes the duplicated reads and the trust-the-caller surface.

The one scenario that appeared to need the caller's values, cleanup after a failed thread
creation, does not: `HarnessRegistry::open_thread` installs the coordinator before it returns, so
when `thread_metadata.create` or the first `start_turn` fails, the binding is loaded and carries
both the declaration and the handle. The only path with neither source is the route's "harness
opened the wrong thread id" branch, and that branch is unreachable: `open_thread` in the registry
already refuses a mismatched handle before the route sees a binding.

## Scope

Three work packages, in this order. Each leaves the tree green and is one commit.

1. The registry: a private target resolver and the three methods on top of it.
2. The routes: drop the passed-through fields, remove the unreachable branch, keep the 404s.
3. Documentation: the design doc's status and staging list.

## Non-goals

No change to the `AgentHarness` trait or any adapter. No change to `ensure_thread_writable`,
which stays the durable-ownership check it is today; the delete cascade relies on it *not* being
applied to sub-agent children (see package 1). No change to which operations spawn an instance: a
cold thread's archive, rename, or delete still creates its declaration's instance on demand, as it
does now. No change to `docs/api-endpoints.md`, the spec, or the README, because no endpoint's
behaviour moves; the design doc is the only document this stage touches.

## Package 1: the registry

File: `crates/giskard-server/src/registry.rs`.

### The resolver

Add, near `loaded_thread_binding` (line 590, `pub async fn loaded_thread_binding`), a private
type and method:

```rust
/// What a native thread operation needs to reach its harness: the declaration whose instance
/// holds the thread, and the handle to name it by. A live thread's binding is authoritative and
/// carries the handle the harness itself returned; a cold thread has only its file, which names
/// its declaration and native id and nothing else, hence the detached handle.
struct ThreadTarget {
    harness: String,
    handle: ThreadHandle,
}

impl HarnessRegistry {
    async fn resolve_thread_target(
        &self,
        project_id: ProjectId,
        thread_id: ThreadId,
    ) -> Result<ThreadTarget, HarnessError> {
        if let Some(binding) = self.loaded_thread_binding(thread_id).await {
            if binding.project_id != project_id {
                warn!(%project_id, %thread_id, bound_project_id = %binding.project_id,
                    action = "resolve_thread_target",
                    "thread is loaded under another project; refusing the operation");
                return Err(HarnessError::ThreadNotFound(thread_id));
            }
            return Ok(ThreadTarget {
                harness: binding.harness,
                handle: binding.handle,
            });
        }
        let file = self
            .shared
            .services
            .store
            .load_thread(project_id, thread_id)
            .await
            .map_err(|error| HarnessError::Protocol(error.to_string()))?
            .ok_or(HarnessError::ThreadNotFound(thread_id))?;
        Ok(ThreadTarget {
            harness: file.harness,
            handle: ThreadHandle::detached(thread_id, file.harness_thread_id),
        })
    }
}
```

Rules the resolver must keep:

- **Binding first, file second.** The order is what makes the creation-failure cleanup work
  without a thread file (see *Why*). Never require the file when a binding exists.
- **The binding is not re-checked against the file.** Both are stamped from the same source at
  creation, and reading the file for a live thread would add a disk read to every live operation
  for no information.
- **The project check is a guard, not a lookup.** `loaded_thread_binding` finds a thread by id
  alone. The routes already scope every call to the project in the path, so a mismatch here means a
  caller bug or a foreign thread id; log it as such and answer `ThreadNotFound`.
- **`ThreadHandle::detached`** is `crates/giskard-harness/src/lib.rs:494`, unchanged.

### The three methods

Replace the bodies of `set_thread_archived` (line 1440), `set_thread_name` (line 1460), and
`delete_thread` (line 1490). New signatures:

```rust
pub async fn set_thread_archived(
    &self,
    config: &ProjectConfig,
    thread_id: ThreadId,
    archived: bool,
) -> Result<(), HarnessError>

pub async fn set_thread_name(
    &self,
    config: &ProjectConfig,
    thread_id: ThreadId,
    name: String,
) -> Result<(), HarnessError>

pub async fn delete_thread(
    &self,
    config: &ProjectConfig,
    thread_id: ThreadId,
) -> Result<(), HarnessError>
```

Each body becomes: the existing `ensure_thread_writable(config.id, thread_id)` call where it is
today (archive and rename only; **not** delete), then
`let target = self.resolve_thread_target(config.id, thread_id).await?;`, then
`get_or_create_harness(config.id, config, &target.harness)`, then the harness call with
`&target.handle`, then whatever follows today (`retire_thread` for delete).

Keep the order `ensure_thread_writable` → resolve → `get_or_create_harness`: a read-only refusal
must come before any instance is created, as it does now. Do not add `ensure_thread_writable` to
`delete_thread`: the delete route (`routes.rs:1953`, `async fn delete_thread`) cascades through
sub-agent children, whose kind is `Subagent`, and applies the writability check to the requested
root only. Adding it inside the registry method would break the cascade. Say so in a comment on
`delete_thread`.

`ensure_thread_writable` (line 619) reads the thread file itself. For a cold archive or rename that
is two reads of the same file, one there and one in the resolver. That is the same count as today
(the route reads it, then `ensure_thread_writable` reads it) and is acceptable; do not fold the two
together in this stage.

### Tests

The registry's test module already has what these need: `discovery_registry` (line 2658),
`create_test_project` (line 2608), `save_thread_on` (line 3017), `attach_primary_on` (line 2741),
and `DiscoveryHarness` (line 2142). `DiscoveryHarness` does not record the handles passed to
`delete_thread`, `set_thread_name`, or `set_thread_archived`; add a `Mutex<Vec<ThreadHandle>>` per
operation to it (or a single `Vec<(&'static str, ThreadHandle)>`), pushed by each of those trait
methods, so the tests below can assert on what the harness received. Add these tests to the
module:

- `a_cold_thread_operation_is_addressed_by_its_file`: persist a thread on `nightly` with native
  id `native-nightly`, no instance created yet. Call `set_thread_name`, `set_thread_archived`, and
  `delete_thread` (delete last). Assert one instance was created and its declaration name is
  `nightly` (the `DiscoveryFactory` records `created` in construction order; pair it with a
  recorded name, or assert `event_driver(project, "stable")` is `None` and `"nightly"` is
  `Some`). Assert each recorded handle has `harness_thread_id == "native-nightly"` and an empty
  `workspace_root` (the detached shape).
- `a_live_thread_operation_uses_its_binding`: create the `nightly` instance, attach a primary with
  `attach_primary_on` (its handle is `ThreadHandle::opened` with `/tmp/test`), then call
  `set_thread_name`. Assert the recorded handle's `workspace_root` is `/tmp/test`, proving the
  binding's handle was used rather than a detached one built from the file.
- `an_operation_on_an_unknown_thread_is_not_found`: no file, no binding; each of the three returns
  `HarnessError::ThreadNotFound`. Assert no instance was created.
- `an_operation_on_a_thread_loaded_under_another_project_is_refused`: two projects; attach a
  thread under project A; call `set_thread_name` with project B's config. Assert
  `ThreadNotFound` and that the harness recorded no call.
- `cleanup_after_a_failed_save_deletes_through_the_binding`: this is the creation-failure
  scenario, at registry level. Create the instance, open a thread with `open_thread` (as the
  existing tests do at lines 2867 and 2935) **without** persisting a thread file, then call
  `delete_thread`. Assert the harness recorded a delete whose `harness_thread_id` is the one
  `open_thread` returned, and that `shared.coordinator(thread)` is `None` afterwards
  (`retire_thread` ran).

Existing tests that name the old signatures must be updated. On `main` no test in this crate calls
the three registry methods directly, so the compiler will find nothing; check with
`grep -rn "\.set_thread_archived(\|\.set_thread_name(\|registry.delete_thread(" crates/` after
the change and fix anything that surfaces.

## Package 2: the routes

File: `crates/giskard-server/src/routes.rs`.

### Callers

- `archive_thread` (line 1795): the call at line 1820 drops `&thread_file.harness` and
  `thread_file.harness_thread_id`. Keep the `load_thread` at the top of the handler: it is what
  turns a missing thread into `404`. `harness_api_error` (line 4502) maps `ThreadNotFound` to
  `500`, so the route must keep answering `NotFound` itself before calling in. After the change
  `thread_file` is otherwise unused in this handler; bind the load as `.ok_or(ApiError::NotFound)?;`
  with no name, or keep the name and let the linter be the judge. Do not remove the load.
- `rename_thread` (line 1842): the call at line 1871 drops the same two arguments. `thread_file`
  stays in use for the early return on an unchanged title.
- `delete_thread` (line 1953): the call at line 2076 becomes
  `.delete_thread(&project_config, *candidate)`. `thread_file` stays in use for the worktree
  cleanup above it and the log fields below it. The registry now reads each candidate's file again
  during the cascade; the route's own `thread_metadata.delete` runs *after* the registry call, so
  the file is still there. Add one sentence to the existing comment block above the worktree
  removal noting that the registry resolves the candidate from its file, which is why the local
  delete must stay after the native one.

### Start-failure cleanup

`cleanup_new_thread_after_start_failure` (line 1708) loses its `harness: &str` parameter. Keep
`harness_thread_id: String`: it is no longer passed to the registry, but the three log lines in the
function name it, and they should keep doing so. Rename nothing else. Its call at line 1029
(`"save_thread"`) and line 1061 (`"start_turn"`) drop the `&harness_name` argument. The registry
resolves the target from the binding `open_thread` installed; add a one-line comment at the top of
the function saying that the binding is what makes this cleanup possible without a thread file.

### The unreachable branch

Remove the block at lines 975–994 of `start_thread_with_message`, from `if handle.thread !=
thread_id {` through its closing brace. `HarnessRegistry::open_thread` (line 882) already returns
`HarnessError::Protocol("harness opened thread {} instead of requested thread {thread}")` at line
936 before installing anything, so the route's `Ok(binding)` arm can never see a mismatched
handle. With it gone the `handle` binding at line 974 is still needed by the `ThreadFile` literal
and the two remaining cleanup calls.

`grep -n "open_thread_mismatch" crates/` must return nothing afterwards.

### Tests

- `crates/giskard-server/tests/thread_lifecycle.rs`: extend
  `thread_lifecycle_native_failure_preserves_local_thread` (line 51) with nothing; it already
  exercises all three routes on a cold thread whose harness cannot start and expects `500` with the
  file intact, which is unchanged. Add one test beside `start_two_declaration_server` (line 131):
  `a_cold_thread_on_another_declaration_is_operated_on_by_its_own_instance`. Build the server with
  `factory::with_catalog(factory::from_fn_by_harness(...), factory::catalog(...))` where the
  closure counts creations per declaration name (an `Arc<Mutex<Vec<String>>>`), persist a thread
  on `nightly` with `fixtures::persist_primary_thread_on`, and drive `PATCH .../title`,
  `POST .../archive`, and `DELETE .../threads/{id}` over HTTP. Assert every call succeeds, the
  created list is exactly `["nightly"]`, and the file is gone after the delete. This is the
  end-to-end form of the registry test and the one that would have caught a route passing the
  project's default instead of the thread's declaration.
- No `ui.rs` or Playwright change: the browser does not change.

## Package 3: documentation

- `docs/multi-harness-design.md`: add `Stage 3 is implemented; see
  `multi-harness-design/stage-3-plan.md`.` after the Stage 2 line (line 14). In *Staging*
  (line 493), insert a `4.` entry before the Claude Code adapter (renumber it to `5.`):
  **Stage 3, thread operations resolve their own target.** `set_thread_archived`,
  `set_thread_name`, and `delete_thread` resolve the thread's declaration and native handle from
  its loaded binding or its file instead of taking them from the caller; the routes and the
  start-failure cleanup stop passing them. No observable change.
- Nothing else. `docs/api-endpoints.md`, the spec, `README.md`, and the Codex adapter README
  describe no behaviour this stage moves. Verify that with a read of the archive, rename, and
  delete paragraphs of `docs/api-endpoints.md` (around lines 224–244) before deciding they stay.

## Verification

In order, before the final push:

1. `cargo fmt --all` and `cargo clippy --all-targets -- -D warnings`.
2. `cargo test -p giskard-server registry::` for the new unit tests, then `cargo test`.
3. `grep -n "open_thread_mismatch" crates/giskard-server/src/routes.rs` must be empty, and
   `grep -n "harness_thread_id: String,$" crates/giskard-server/src/registry.rs` must match none
   of the three registry method signatures (it may still match struct fields).
4. `tests/e2e/run.sh` if a Docker daemon is available; otherwise the `playwright` CI job is the
   check. The suite must pass unchanged, since nothing the browser sees moves.

## Acceptance

- The three registry methods take `config` and `thread_id` plus their own payload and nothing
  else.
- A cold thread's archive, rename, and delete reach the instance of the thread's own declaration,
  addressed by the native id in its file, and create no other instance.
- A live thread's operations use the handle the harness returned at open.
- Cleanup after a failed `thread_metadata.create` still deletes the native thread, through the
  binding, with no thread file on disk.
- An unknown thread, or one loaded under another project, is `ThreadNotFound` at the registry and
  `404` at the routes that check first.
- No route, wire type, persisted format, or browser file changes. `docs/api-endpoints.md` and the
  spec are unchanged; the design doc records the stage.
