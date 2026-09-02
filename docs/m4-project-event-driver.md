# M4 — One event driver per project

Implementation plan for milestone M4 of [`event-pipeline-milestones.md`](event-pipeline-milestones.md).
Written against `main` at `81dd02e` (M3 merged). Every file and line reference below was checked
against that tree; re-check them if the branch has moved.

## Goal

Delete the cross-task owner lifecycle protocol. Today every thread's forwarder is its own
`tokio::spawn`, and "exactly one consumer per thread" is negotiated between installers, retirers and
the owner task through a per-thread owner mutex with weak pre-interning, a `Live → Draining →
Retired` phase machine, a cancel watch, a completed watch, a drain-and-retry loop, and a
generation bump. After M4 one task per project harness, the driver, owns every forwarder for that
project as a future in a `FuturesUnordered`. Attaching and detaching a thread are messages to that
task, so every owner transition happens in one place, in order, and nothing waits on a lock for
another task to finish.

The forwarder itself does not change. `ThreadEventForwarder::run` keeps its loop and its cancel
watch; only who polls it and who reacts to its exit changes.

## Non-goals

- No change to the forwarder's event reduction, `handle_event`, `handle_stream_error` or `finish`.
- No change to `AgentHarness`, the adapter, the transport, the hub, persistence, or routes.
- No change to the coordinator's turn machinery: `prepare_operation`, `claim_native_turn`,
  `CoordinatorToken`, generations and leases stay for M5.
- No change to sub-agent materialization or the per-parent queue (M6).
- No removal of `OwnerLock`. Its drain loop goes; the plain mutex stays around a Primary's native
  open. See "What the owner lock is still for".

## Ground truth

| Fact | Where |
| --- | --- |
| Owner installation: intern authority, `reusable_handle` if a coordinator exists, else subscribe, register a per-owner `RegistryTaskPermit`, build coordinator, `activate_owner(EventOwnerControl { cancel, completed })`, `install_coordinator_if_empty`, `launch_event_forwarder` | `registry.rs:2704-2780` |
| `launch_event_forwarder` spawns the task, runs the forwarder, and on exit calls `owner_finished`, clears the coordinator unless the exit was `PersistenceBlocked`, and sends `completed` | `registry.rs:2665-2702` |
| Installers lock the owner slot only after the previous generation drained: `lock_thread_owner_after_drain` loops on `is_retired`, `draining_control`, `completed.has_changed`, `wait_for_owner_completion`, with a `yield_now` retry when completion already fired | `registry.rs:1934-1968` |
| `lock_thread_owner` interns a weak `OwnerLock` before the authority exists; `intern_thread_authority` adopts it | `registry.rs:1895-1922`, `:329-357` |
| `forget_thread`: lock, `begin_retirement`, cancel, unlock, `wait_for_owner_completion`, relock, `clear_coordinator_if`, `finish_retirement`; `retire_thread` adds runtime forget and an overview publish | `registry.rs:1679-1719` |
| Callers of the protocol: `open_primary_thread` (`:988`, `:1037`), `admit_discovered_thread` (`:684`), `materialize_subagent_thread` (`:2411`), `ensure_subagent_thread_open` (`:2574`, `:2604`), `delete_project` (`:1820-1864`, forgets every thread after the harness is deleted), route-level `retire_thread` (`routes.rs:1658`) and `forget_thread` (`routes.rs:6002`) | grep |
| Coordinator owner phases and the methods that move them: `OwnerPhase::{Installing, Live(control), Draining(control), Retired, Failed}`, `activate_owner`, `owner_finished`, `begin_retirement`, `draining_control`, `is_retired`, `finish_retirement`; `prepare_operation` and `reusable_handle` require `Live` | `registry/thread.rs:42-54`, `:163-178`, `:179-226`, `:252-283`, `:418-475` |
| `ThreadAuthority` holds `OwnerLock`, `CoordinatorSlot`, `ThreadRuntimeSlot`, `MaterializationSlot`; `install_coordinator_if_empty` and `clear_coordinator_if` are its only slot mutators | `registry/thread.rs:557-642` |
| The harness slot per project is `ProjectHarnessState::{Active, Deleting}(Arc<dyn AgentHarness>)` behind `ProjectHarnessGuard` with `publish_active`, `begin_delete`, `rollback_delete_if_running`, `finish_delete`, `take_for_shutdown` | `registry/project.rs:108-262` |
| The harness is created and published in `get_or_create_harness`, which also spawns the per-harness discovery consumer | `registry.rs:844-891`, `:536-579` |
| Registry shutdown takes every harness out of its slot, shuts them down concurrently, then `close_and_wait`s the background task tracker (10 s) | `registry.rs:1721-1818`, timeouts at `:133-135` |
| Forwarders exit when their log closes; a harness closes every thread log when its last `Arc` drops (`EventLogs::drop`), and the discovery consumer drops its own `Arc` when the discoveries stream closes | `giskard-harness-codex/src/lib.rs:272-286`, `registry.rs:551-579` |
| `ThreadEventForwarder::new(shared, authority, coordinator, stream, cancel)` is async; `run(self) -> ForwarderExitReason` selects on the cancel watch and the stream; `finish` releases the lease | `registry/event_forwarder.rs:776-860` |
| `futures` is already a dependency of the server crate (`join_all` is imported) | `registry.rs:9` |
| Tests that build owners directly: three `activate_owner` sites in forwarder tests (`event_forwarder.rs:2732`, `:4247`, `:4313`), eight registry tests around the protocol (`registry.rs:4461-4750`), `install_test_coordinator` (`:4401`) | grep |

## Design

### The driver

New module `crates/giskard-server/src/registry/driver.rs`.

```text
ProjectEventDriver (one task per harness, holds one RegistryTaskPermit)
  rx: mpsc::Receiver<DriverCommand>
  harness: Weak<dyn AgentHarness>            // upgraded only inside Attach
  shared: Arc<RegistryShared>
  owners: FuturesUnordered<BoxFuture<OwnerExit>>
  parked: Vec<Attach>                        // attaches waiting for a detach of the same thread

DriverHandle { tx: mpsc::Sender<DriverCommand> }   // Clone; stored next to the harness

enum DriverCommand {
    Attach { binding: LoadedThreadBinding, classification: ClassificationPhase,
             reply: oneshot::Sender<Result<AttachOutcome, HarnessError>> },
    Detach { thread_id: ThreadId, reply: oneshot::Sender<()> },
}
enum AttachOutcome { Installed, Reused(ThreadHandle) }
struct OwnerExit { authority: Arc<ThreadAuthority>, coordinator: Arc<ThreadCoordinator>,
                   reason: ForwarderExitReason }
```

The loop:

```text
loop {
    select! {
        cmd = rx.recv() => match cmd {
            Some(Attach ..) => attach(..),
            Some(Detach ..) => detach(..),
            None => closed = true,
        },
        Some(exit) = owners.next(), if !owners.is_empty() => owner_exited(exit),
    }
    if closed && owners.is_empty() { return }
}
```

`attach`:

1. `intern_thread_authority(thread_id, project_id)`.
2. If `authority.coordinator()` is `Some`: if its phase is `Detaching`, push the request onto
   `parked` and return (it is retried when that thread's owner exits); otherwise answer with
   `reusable_handle(..)` exactly as `install_event_owner_locked` does today, as
   `AttachOutcome::Reused` or the same error.
3. Upgrade the harness. `None` means the harness was shut down or deleted: reply
   `Err(Protocol("harness is gone"))`.
4. `stream = harness.subscribe(&binding.handle)`; `(cancel_tx, cancel_rx) = watch::channel(false)`;
   `coordinator = ThreadCoordinator::new_live(binding, classification, cancel_tx)`;
   `authority.install_coordinator_if_empty(coordinator.clone())`. The driver is the only
   installer, so a conflict here is a protocol error, not a race to retry.
5. Push the owner future:
   `async move { let f = ThreadEventForwarder::new(shared, authority.clone(), coordinator.clone(), stream, cancel_rx).await; let reason = f.run().await; OwnerExit { authority, coordinator, reason } }`.
6. Reply `Installed`.

`detach`:

1. `authority.coordinator()`; `None` → reply now.
2. `coordinator.request_detach(reply)`: `Live(cancel)` → phase becomes `Detaching { cancel, waiters:
   [reply] }` and `cancel.send(true)`; `Detaching` → push the waiter; `Failed` → the owner future has
   already exited, so clear the slot and reply now.

`owner_exited(exit)`:

1. `let outcome = coordinator.owner_exited(reason)`, a single transition under the coordinator
   mutex:
   - `Detaching { waiters, .. }` → phase `Failed("detached")`, returns `Detached(waiters)`;
   - `Live` and `reason == PersistenceBlocked` → phase `Failed(reason)`, returns `RetainFailed`
     (the slot is kept so the failure stays visible and the thread cannot be silently reopened,
     exactly as `launch_event_forwarder` does today);
   - `Live` and any other reason → phase `Failed(reason)`, returns `ClearFailed`.
2. `Detached(waiters)` → `authority.clear_coordinator_if(&coordinator)`, send every waiter, then
   retry any `parked` attach for this thread. `ClearFailed` → clear the slot and `warn!` with the
   exit reason label, as today. `RetainFailed` → nothing else.

The driver never awaits a forwarder inline, never holds a lock across an await, and never touches
the harness except through `subscribe` inside `attach`. Its `Weak<dyn AgentHarness>` is what lets
harness shutdown close the logs: the driver must not keep the harness alive.

### Where the handle lives

`ProjectHarnessState::Active` and `Deleting` gain the `DriverHandle` next to the `Arc<dyn
AgentHarness>`. `publish_active(harness, driver)`; `active()` is unchanged; add `driver()`;
`begin_delete` returns both; `finish_delete` and `take_for_shutdown` drop the handle. Dropping the
last handle closes the command channel, and the driver exits once its remaining owners have
finished, which happens when the harness drops and its logs close.

`get_or_create_harness` spawns the driver right after `factory.create`, before
`publish_active`, with a permit from `background_tasks`. If no permit is available (shutdown), the
harness is not published, as `spawn_discovery_consumer` already refuses.

### The coordinator after M4

```rust
enum OwnerPhase {
    Live(watch::Sender<bool>),
    Detaching { cancel: watch::Sender<bool>, waiters: Vec<oneshot::Sender<()>> },
    Failed(String),
}
```

`Installing`, `Draining`, `Retired` and `EventOwnerControl` are deleted. `ThreadCoordinator::new`
becomes `new_live(binding, classification, cancel)`; `activate_owner`, `begin_retirement`,
`draining_control`, `is_retired`, `finish_retirement` and `owner_finished` are replaced by
`request_detach` and `owner_exited` above. `prepare_operation` and `reusable_handle` keep requiring
`Live`; `reusable_handle` maps `Detaching` to the existing "not reusable" error and `Failed` to
the existing failure error. "Retired" is no longer a phase: a retired coordinator is one that is
no longer in the slot. The generation counter and its bump stay untouched until M5.

### Registry call sites

| Site | Today | After |
| --- | --- | --- |
| `install_event_owner` (`:2704`) | drain-lock then `install_event_owner_locked` | look up the project's `DriverHandle`, send `Attach`, await the reply; `Ok(true)` for `Installed`, `Ok(false)` for `Reused` |
| `install_event_owner_locked` (`:2715`) | the install body | deleted; body moves into the driver's `attach` |
| `launch_event_forwarder` (`:2665`) | spawn and post-exit bookkeeping | deleted; body moves into the owner future and `owner_exited` |
| `forget_thread` (`:1679`) | lock, retire, cancel, wait, relock, clear | look up the driver; send `Detach`, await the reply. No driver (project deleted or shutting down): clear the coordinator slot directly if its phase is `Failed`, otherwise return; the owner future clears it when the closed log ends the forwarder |
| `retire_thread` (`:1706`) | unchanged | unchanged |
| `open_primary_thread` (`:988`, `:1037`) | `lock_thread_owner_after_drain`, reuse check, native open, `install_event_owner_locked` | `lock_thread_owner` (plain), reuse check via `shared.coordinator` and `reusable_handle`, native open, `install_event_owner` |
| `ensure_subagent_thread_open` (`:2574`, `:2604`) | drain-lock, reuse check, claim, `install_event_owner_locked` | drop the lock entirely: `claim_native_thread` is idempotent and `Attach` returns `Reused` for a concurrent caller |
| `admit_discovered_thread` (`:684`), `materialize_subagent_thread` (`:2411`) | `install_event_owner` | unchanged call |
| `delete_project` (`:1820`) | `begin_delete` → shutdown → `finish_delete` → `forget_thread` per thread | `begin_delete` returns the driver handle too; keep it alive across the forget loop so each `Detach` is delivered, then drop it |
| `lock_thread_owner_after_drain` (`:1934`), `wait_for_owner_completion` (`:1924`) | | deleted |
| `lock_thread_owner` (`:1895`), `ThreadIndex::unpublished_locks` | | kept |

### What the owner lock is still for

`open_primary_thread` holds the owner lock across the native `thread/start` or `thread/resume`
so that two concurrent opens of one persisted thread do not both issue a resume. That is a lock
around provider I/O, not around ownership, and it has no drain semantics: with the driver, the
second opener finds the coordinator installed and gets `Reused` from `Attach`. The lock keeps its
name in this PR (no renames in a behavior change); a follow-up may call it what it is.

### Why the harness is `Weak` in the driver

Registry shutdown relies on the harness `Arc` count reaching zero: `take_for_shutdown` removes the
slot's `Arc`, `harness.shutdown()` returns, the closure drops its `Arc`, `EventLogs::drop` closes
every thread log, each forwarder sees `Closed` and exits, the driver's `owners` empties, and the
driver releases its permit within `BACKGROUND_TASK_SHUTDOWN_TIMEOUT`. A driver holding a strong
`Arc` would keep every log open and turn every shutdown into a 10 s timeout error. The discovery
consumer already respects the same rule by exiting when the discoveries stream closes.

### Attach during detach

Today `lock_thread_owner_after_drain` makes a reopen wait for a retirement to finish. The driver
gets the same effect without a lock: an `Attach` for a thread whose coordinator is `Detaching` is
parked and retried by `owner_exited` once the detach completes. The parked list is a local of the
driver task; it is not a keyed authority map.

## Every site that changes

| File | Change |
| --- | --- |
| `registry/driver.rs` | new: `ProjectEventDriver`, `DriverHandle`, `DriverCommand`, `AttachOutcome`, `OwnerExit`, `spawn_project_event_driver`, unit tests |
| `registry/thread.rs:42-54` | delete `EventOwnerControl`, `OwnerPhase::{Installing, Draining, Retired}`; add `Detaching` |
| `registry/thread.rs:128-143`, `:163-178` | `new` → `new_live(binding, classification, cancel)`; delete `activate_owner` |
| `registry/thread.rs:252-283` | `reusable_handle` matches the three remaining phases |
| `registry/thread.rs:418-475` | delete `owner_finished`, `begin_retirement`, `draining_control`, `is_retired`, `finish_retirement`; add `request_detach`, `owner_exited` |
| `registry/project.rs:108-117`, `:182-262` | harness state carries the `DriverHandle`; `publish_active`, `begin_delete`, `driver()` |
| `registry.rs:64` | imports |
| `registry.rs:844-891` | spawn the driver in `get_or_create_harness` |
| `registry.rs:988`, `:1037` | `open_primary_thread` as above |
| `registry.rs:1679-1705` | `forget_thread` as above |
| `registry.rs:1820-1864` | `delete_project` keeps the handle across the forget loop |
| `registry.rs:1924-1968` | delete `wait_for_owner_completion`, `lock_thread_owner_after_drain` |
| `registry.rs:2558-2616` | `ensure_subagent_thread_open` without the lock |
| `registry.rs:2665-2780` | delete `launch_event_forwarder`, `install_event_owner_locked`; `install_event_owner` sends `Attach` |
| `registry/event_forwarder.rs` | production code untouched; three test sites (`:2732`, `:4247`, `:4313`) and `spawn_forwarder_handle_with_runtime` (`:4207`) build coordinators with `new_live(.., cancel_tx)` instead of `activate_owner` |

## Tests

### Driver unit tests (`driver.rs`)

Use `HarnessRegistry::new` with a fake harness whose `subscribe` returns a reader over a
test-owned `EventLog` (the pattern in `registry.rs` tests and `e2e_smoke.rs`).

- `attach_installs_one_owner_and_a_second_attach_reuses_it`: two attaches for one thread, the
  second returns `Reused` with the same handle; exactly one coordinator in the slot.
- `attach_with_an_incompatible_binding_is_rejected`: different native id or classification returns
  the `reusable_handle` error.
- `detach_cancels_the_owner_and_clears_the_slot`: attach, send `TurnStarted`, detach; the reply
  arrives after the forwarder released its lease (`runtime.has_active_turn` is false) and the slot
  is empty.
- `detach_without_an_owner_replies_immediately`.
- `attach_during_detach_is_parked_until_the_detach_completes`: attach, detach and attach again
  without awaiting; the second attach resolves `Installed` only after the detach reply, and the
  slot then holds a fresh coordinator.
- `a_persistence_blocked_exit_keeps_the_failed_coordinator`: drive a forwarder to
  `PersistenceBlocked` (the existing `stream_end_before_completion_persists_interrupted_turn`
  scaffolding in `event_forwarder.rs` shows how); the slot keeps a `Failed` coordinator; a later
  attach returns the failure error; a detach clears it.
- `any_other_owner_failure_clears_the_slot_so_the_thread_can_reopen`: close the log; the slot is
  empty afterwards and a new attach installs.
- `the_driver_exits_after_its_handle_drops_and_its_owners_finish`: drop the handle, close the
  logs; the driver task completes and the registry's task tracker count returns to zero.
- `the_driver_does_not_keep_the_harness_alive`: `Arc::strong_count` of the harness is unchanged by
  attaching.

### Registry tests to rewrite

`closing_owner_cannot_deadlock_forget_thread_behind_owner_lock`,
`installer_waits_for_draining_owner_without_holding_owner_lock`,
`installer_retires_a_draining_owner_whose_completion_sender_closed`,
`failed_forwarder_does_not_clear_a_replacement_coordinator`,
`forgetting_and_reopening_reuses_the_same_thread_authority`,
`loaded_thread_binding_is_coherent_across_coordinator_replacement` and `install_test_coordinator`
(`registry.rs:4401-4830`). The first three pin the drain protocol and are replaced by the driver
tests above; the rest are ported to attach/detach. `thread_authority_adopts_the_contended_owner_mutex`
stays: the plain lock is kept.

### Existing tests that must keep passing unchanged

The whole `event_forwarder.rs` suite apart from the three constructor edits; every server
integration test under `crates/giskard-server/tests`; the adapter suite; the six M0 tests.

## Documentation

- `AGENTS.md` conventions: replace the sentence about `RegistryShared` owner locks with: one
  `ProjectEventDriver` per harness owns every event forwarder for that project; attach and detach
  are driver messages; no code outside the driver installs or clears a coordinator or spawns an
  event forwarder.
- `docs/subagents.md`, "Long-lived native event ownership": rewrite the retirement paragraph
  (`Live` to `Draining` under the owner lock, cold opens waiting on `Draining`) to describe the
  driver: retirement is a detach message; a reopen during a detach is queued behind it.
- `specs/giskard-specification.md`: bump to 1.81 with an amendment paragraph; update §4.3's "the
  registry installs exactly one consuming event owner per loaded native thread" (`:1756-1758`) to
  say the project driver does.
- `docs/event-pipeline-milestones.md`: M4 status line.

## Order of work

1. `driver.rs` with `DriverHandle`, the loop, `attach`, `detach`, `owner_exited`, and its unit
   tests, driven directly (spawn the driver from a test with a fake harness). Nothing else changes;
   the module is unused by production.
2. Coordinator phase change and `new_live`; forwarder test constructor edits. The old protocol
   methods still compile because nothing calls the new ones yet.
3. Switch `install_event_owner` and `forget_thread` to the driver; plumb the handle through
   `ProjectHarnessState`; spawn in `get_or_create_harness`; `delete_project` ordering.
4. Delete `launch_event_forwarder`, `install_event_owner_locked`, `lock_thread_owner_after_drain`,
   `wait_for_owner_completion`, and the retired coordinator methods. Rewrite the registry tests.
5. Docs.

## Verification the implementer must perform and record

1. `cargo fmt --all --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings`;
   `cargo test --workspace --locked`; zero ignored tests.
2. `grep -rn 'launch_event_forwarder\|install_event_owner_locked\|lock_thread_owner_after_drain\|wait_for_owner_completion\|EventOwnerControl\|owner_finished\|begin_retirement\|finish_retirement\|draining_control\|is_retired\|activate_owner' crates`
   returns nothing.
3. `grep -n 'tokio::spawn' crates/giskard-server/src/registry.rs` lists only the discovery
   consumer, the thread-update forwarder, the materialization queue worker, and `start_turn`'s and
   `compact_thread`'s request tasks. The driver's own spawn lives in `driver.rs`.
4. Server shutdown with two projects open, one mid-turn, completes without the "registry
   background tasks did not drain" error and within the harness timeout. Record the log lines.
5. Manual run against a real Codex CLI: open a thread, run a turn, spawn a sub-agent, delete the
   sub-agent's parent while the child is idle, delete a project with an open thread, reopen a
   thread right after deleting it, shut the server down mid-turn. Record each outcome.
6. `git diff --stat main` non-test lines under 1000. Expected: about 300 in `driver.rs`, 100
   removed and 60 added in `registry.rs`, 120 removed and 60 added in `thread.rs`, 40 in
   `project.rs`, 40 in docs.

## Pitfalls

- Do not store a strong `Arc<dyn AgentHarness>` in the driver. See "Why the harness is `Weak`".
- Do not `await` a forwarder future inline in `detach`. The reply is sent by `owner_exited`.
- Do not clear a `PersistenceBlocked` coordinator on exit. Today's behavior keeps it so the
  thread cannot be reopened over an unpersisted turn; the driver keeps that.
- `FuturesUnordered` must not be polled when empty inside `select!` without the `if
  !owners.is_empty()` guard, or the branch resolves `None` immediately and spins.
- `ThreadEventForwarder::new` reads the store; it runs inside the owner future, not in the driver's
  command handler, so a slow disk never delays other commands.
- The `delete_project` path calls `forget_thread` after the harness is gone. Keep the handle alive
  for that loop, and let `forget_thread` tolerate a missing driver.
- Keep `RegistryTaskPermit` on the driver only. Per-owner permits go; the driver's permit covers
  every owner future it polls.

## Stop rules

Stop and re-cut if:

- `handle_event`, `handle_stream_error` or `finish` in the forwarder need to change;
- a new lock, generation, token or watch appears anywhere outside the driver's own `cancel`
  watch per owner;
- a keyed map of threads appears on the driver that is not a function-body local;
- the harness trait or the adapter needs a change;
- non-test lines exceed the budget.
