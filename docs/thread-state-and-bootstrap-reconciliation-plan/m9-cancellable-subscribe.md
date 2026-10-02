# M9 — Cancellable subscribe

Implementation plan for milestone M9 of
[`thread-state-and-bootstrap-reconciliation-plan.md`](../thread-state-and-bootstrap-reconciliation-plan.md).
Written against `main` at `15d16cf` (M8 landed, spec 1.94). Every file and line reference below
was checked against that tree; re-check them if the branch has moved. The milestone text is the
authority on scope and non-goals; this document says how to land it and how to prove it landed.

## What lands

1. The `Subscribe` bootstrap runs in a task the connection owns, keyed by thread and a
   per-connection generation, so the receive loop keeps polling and observes a close, a
   resubscribe or an `Unsubscribe` while a bootstrap is in flight.
2. Cooperative cancellation at phase boundaries and before every send. No task is aborted, no
   registry or store call is dropped mid-await.
3. One small hub read, `Hub::client_count`, so a test can prove a closed socket was disconnected
   while its bootstrap was still held.
4. Spec 1.95 and a new integration test file. No proto change, no browser change, no message
   changes.

## Facts the plan rests on

- **The connection handler.** `handle_ws` (`ws.rs:245`) creates the outbound channel
  `mpsc::channel::<ServerMessage>(256)` (`:251`), takes a client id (`:252`), registers with the
  hub (`:257`), spawns the writer task that drains `rx` into the socket (`:272-333`), sends the
  activity bootstrap (`:335`), then loops on a `select!` over server shutdown, `ws_receiver.next()`
  and the writer's completion (`:341-359`). Each text frame is parsed and passed to
  `handle_client_msg(&state, client_id, &tx, msg).await` (`:394`); an `Err(WsError)` is logged with
  its code, severity, thread, request and action fields and sent to the client (`:395-407`). After
  the loop, `hub.disconnect(client_id)` (`:420`) and the writer task is awaited or aborted
  (`:421-428`).
- **The Subscribe arm** (`ws.rs:443-603`), in order: `ensure_thread_open` with the read-only
  fallback (`:449-465`); `hub.subscribe`, guarded by the comment that registering before the
  snapshot is what makes the snapshot's `active_turn` safe (`:467-479`); the attach warning if any
  (`:481-483`); `recompute_aggregates` and the `ThreadState` send (`:485-506`); the history read,
  `load_turns_after` for a resync cursor or `load_history` for a reset (`:519-556`); the live
  snapshot (`:565-575`); the task snapshot (`:579-586`); the sends of history, snapshot and tasks
  (`:588-598`); the `RequestState` sends (`:599-603`). `Unsubscribe` is one hub call (`:605-607`).
- **`ensure_thread_open`** (`ws.rs:1326-1420`) returns early when the thread is loaded, otherwise
  finds the persisted thread and attaches through the registry (`attach_subagent_thread` or
  `open_thread`, `:1400-1420`). That is the cold attach; for Codex it can spawn an app-server.
  `recompute_aggregates` (`thread_metadata.rs:77-86`) goes through the store and publishes a
  metadata mutation. Neither is proven safe to drop mid-await: the driver's fences cover
  `start_turn` and compaction (`tests/e2e_smoke.rs:2196`, `:2264`), not `open_thread`.
- **The steering precedent.** The `SteerInput` arm spawns its harness call so the receive loop is
  not held, with the reason stated in a comment (`ws.rs:759-765`), sends errors through a cloned
  `tx`, and logs at `debug` when the writer has already ended (`:770-789`).
- **The hub.** `subscribe` refuses an unregistered client and is idempotent (`hub.rs:86-104`);
  `unsubscribe` removes one pair (`:107-115`); `disconnect` removes the client and every
  subscription (`:117-131`); `send_ordered` drops a message for a full client queue and removes a
  subscription whose queue is closed (`:135-158`). The client map is `clients: Mutex<HashMap<..>>`
  (`:51`).
- **The browser.** `openThread` (`app.js:2582`) calls `connectWs()` (`:2671`), which closes the
  previous socket and opens a new one (`:2775-2780`) and subscribes to the current thread once
  connected (`:2846-2856`). The only same-socket resubscribe is the detail-conflict resync
  (`:3791`). `app.js` never sends `unsubscribe`. Frames for another thread are discarded by
  `isCurrentThreadServerMessage` (`:3182-3187`). The spec's "one WebSocket per browser client,
  multiplexing all projects/threads" (`specs/giskard-specification.md:3784-3785`) is the
  protocol's capability; the server must keep serving several threads per socket, but the browser
  uses one per view.
- **Cancellation primitives.** `giskard-server` depends on `tokio` with `full` features
  (`Cargo.toml:41`) and not on `tokio-util`; `tokio::sync::watch` and `Notify` are already used
  (`delivery.rs:4`, `bin/common/shutdown.rs:6`). An `Arc<AtomicBool>` per bootstrap is enough for
  a cooperative check and needs no new dependency.
- **Tests.** `giskard-testenv`'s `Script::open_thread` has a default that opens through
  `core.opened` (`fake.rs:307-312`), and `FakeHarness::open_thread` records `Call::OpenThread`
  before delegating (`:421`); `Gate` offers `held`, `open`, `hold`, `release`, `pass`
  (`:571-598`); `FakeCore::wait_for_calls` (`:222`); `ws::connect(addr, cookie)`
  (`testenv/src/ws.rs:12`); `TestServer { state, addr, base, client, cookie }`
  (`server.rs:17-24`); `tests/turn_steering.rs:132-180` shows `start_server`, `create_project`
  and `create_project_and_thread` built on `fixtures::persist_primary_thread`. `ClientMessage::Ping`
  is answered with `ServerMessage::Pong` (`ws.rs:1314`), which gives a test an ordered sentinel:
  anything the server would still send on that socket arrives before the pong. Existing subscribe
  coverage that must stay green: `subscribe_thread_state_reports_a_turn_that_ended_before_the_socket…`
  and `…_the_harness_has_not_streamed…` (`e2e_smoke.rs:2325`, `:2389`),
  `subscribe_unknown_thread_returns_structured_error` (`:5778`),
  `subscribe_reopens_persisted_thread` (`:5963`), `resync_delta_over_websocket` and
  `subscribe_corrupt_history_returns_structured_error` (`history_sync.rs:122`, `:222`).
- **Spec.** Version line `:12`, newest amendment blockquote `:14`, newest changelog block at the
  1.93 → 1.94 entry; tag series `CS*` is unused. `docs/api-endpoints.md` has no subscribe
  description and no route changes here, so it is untouched.

## Decisions

- **D1. Whole arm into the task, order unchanged.** The task performs attach, hub registration,
  metadata, history, snapshot, tasks and requests in today's order. Registering with the hub inside
  the task keeps the "register before the snapshot" invariant exactly as it is; moving
  `hub.subscribe` before the attach would subscribe a client to a thread that may turn out not to
  exist.
- **D2. Cooperative cancellation.** One `Arc<AtomicBool>` per bootstrap. The task checks it after
  each phase and before each `tx.send`; when set, it logs once and returns. Nothing is aborted.
  Phase granularity is the contract, and the milestone text says so.
- **D3. Per-connection slots, per thread.** `HashMap<ThreadId, BootstrapSlot { generation: u64,
  cancelled: Arc<AtomicBool> }>` as a local of `handle_ws`, passed to `handle_client_msg` by
  `&mut`. A `Subscribe` for a thread with a slot sets that slot's flag and replaces it; `Unsubscribe`
  sets and removes; loop exit sets every flag before `hub.disconnect`. The generation is a
  per-connection `u64` that increments on every `Subscribe`. It never leaves the process.
- **D4. Errors from the task reach the client as today.** The task sends `e.into_server_message()`
  through its cloned `tx` and logs with the same fields the loop uses (`ws.rs:395-407`), unless
  cancelled. The `metadata_request_id` handling at `:387-393` does not apply to `Subscribe`.
- **D5. A finished slot is not removed eagerly.** A completed bootstrap leaves its slot in place
  with the flag unset; the next `Subscribe` or `Unsubscribe` for that thread replaces or removes
  it. Removing on completion would need the task to reach back into the loop's locals.
- **D6. `Hub::client_count`** is added as a plain `pub async fn` returning `clients.len()`, used by
  the new test to observe that a closed socket was disconnected while its bootstrap was held. It is
  the only hub change.
- **D7. Spec 1.95, tags CS1 to CS3.** No README, `api-endpoints.md` or Codex README change: nothing
  user-visible or route-visible changes.

## Changes

### A. `crates/giskard-server/src/ws.rs` (net about +90)

1. **Slot types**, near `ThreadAccess` (`:1321`):

   ```rust
   struct BootstrapSlot {
       generation: u64,
       cancelled: Arc<AtomicBool>,
   }

   #[derive(Default)]
   struct SubscriptionSlots {
       next_generation: u64,
       by_thread: HashMap<ThreadId, BootstrapSlot>,
   }
   ```

   with three methods: `begin(&mut self, thread_id) -> (u64, Arc<AtomicBool>)` (sets and replaces
   any existing slot's flag, allocates the next generation), `cancel(&mut self, thread_id)`
   (sets and removes), `cancel_all(&mut self)`.

2. **`handle_ws`**: declare `let mut slots = SubscriptionSlots::default();` beside
   `shutting_down` (`:339`); pass `&mut slots` to `handle_client_msg` (`:394`); call
   `slots.cancel_all()` immediately before `hub.disconnect(client_id).await` (`:420`).

3. **`handle_client_msg`** (`:436-442`): add `slots: &mut SubscriptionSlots`. Replace the body of
   the `Subscribe` arm (`:443-603`) with:

   ```rust
   let (generation, cancelled) = slots.begin(thread_id);
   debug!(%client_id, %thread_id, generation, action = "subscribe", "spawning subscribe bootstrap");
   let state = state.clone();
   let tx = tx.clone();
   tokio::spawn(async move {
       if let Err(e) = run_subscribe_bootstrap(&state, client_id, &tx, thread_id, since, generation, &cancelled).await {
           // same fields and level as the loop's handler error path (ws.rs:395-407)
           if !cancelled.load(Ordering::Acquire) { let _ = tx.send(e.into_server_message()).await; }
       }
   });
   ```

   and make the `Unsubscribe` arm (`:605-607`) call `slots.cancel(thread_id)` before
   `hub.unsubscribe`.

4. **`run_subscribe_bootstrap`**: a new `async fn` holding the moved arm body verbatim, with the
   generation added to its existing `debug!` lines (`subscribe_history`, `build_live_snapshot`) and
   a `bail_if_cancelled!`-style check inserted at these points: after `ensure_thread_open`, after
   `hub.subscribe`, after `recompute_aggregates`, after the history read, after the live snapshot,
   and before each `tx.send`. The check logs one `debug!` with `client_id`, `thread_id`,
   `generation`, `phase` and `elapsed_ms` and returns `Ok(())`. A cancelled task that had already
   subscribed to the hub leaves the subscription alone: on close the loop's `hub.disconnect` removes
   it; on resubscribe the newer bootstrap re-subscribes idempotently; on `Unsubscribe` the arm
   already called `hub.unsubscribe`.

5. Imports: `std::collections::HashMap` if not already imported in `ws.rs`, `std::sync::Arc`,
   `std::sync::atomic::{AtomicBool, Ordering}`. Check with `rg -n "^use std::" crates/giskard-server/src/ws.rs`.

### B. `crates/giskard-server/src/hub.rs` (+5)

```rust
/// Registered client count, for tests and future metrics.
pub async fn client_count(&self) -> usize { self.clients.lock().await.len() }
```

### C. Tests: `crates/giskard-server/tests/subscribe_cancellation.rs` (new, about 250 lines)

Model on `tests/turn_steering.rs` for the server, project, thread and socket helpers. One script:

```rust
struct HeldOpenScript { open: Gate }
impl Script for HeldOpenScript {
    async fn open_thread(&self, core: &FakeCore, opts: &OpenThreadOptions) -> Result<ThreadHandle, HarnessError> {
        self.open.pass().await;
        Ok(core.opened(opts, self.native_thread_id(opts.thread)))
    }
}
```

1. **Close during a held attach.** Persist a primary thread the fake will cold-open; connect;
   send `Subscribe`; `core.wait_for_calls(Call::OpenThread, 1)`; assert `hub.client_count() == 1`;
   drop the socket; wait until `hub.client_count() == 0` **while the gate is still held** (bounded
   loop with `yield_now`, as `turn_steering.rs` waits). Then release the gate and assert, after a
   short bounded wait, that `core.calls()` shows no further harness calls and the count stays 0.
   On `main` today this test fails at the first wait: the count stays 1 until the gate is released.
2. **Same-thread resubscribe.** Connect; send `Subscribe` twice for the same thread while the gate
   is held; release; read frames until `RunningTasks` for the thread, then send `Ping` and read
   until `Pong`; assert exactly one `ThreadState` and one `HistoryDelta` were received. (The second
   attach dedupes on the registry's cold-open lock, so both generations wake on one release.)
3. **Unsubscribe during a held attach.** Connect; `Subscribe`; wait for the open call;
   `Unsubscribe`; release; `Ping`; assert no `ThreadState`, `HistoryDelta`, `LiveTurnSnapshot` or
   `RunningTasks` arrives before `Pong`.
4. **Errors still arrive from the task.** Not new: `subscribe_unknown_thread_returns_structured_error`
   and `subscribe_corrupt_history_returns_structured_error` cover it and must pass unchanged.

### D. `specs/giskard-specification.md`

- `:12` → `**Version:** 1.95`. Insert before `:14` a blockquote
  `> **Amendment — cancellable subscribe (1.95).** …` in the convention every version since 1.88
  follows. Insert before the 1.93 → 1.94 block:
  - **CS1:** a subscribe's bootstrap runs in a connection-owned task with a server-side generation;
    the receive loop is never held by an attach or a read; message set and order unchanged.
  - **CS2:** a close, a resubscribe for the same thread on the same socket, or an unsubscribe
    cancels the in-flight bootstrap cooperatively at its next phase boundary; nothing is aborted
    mid-await and no further reads or sends happen for it.
  - **CS3:** the generation is not on the wire in this version; the browser continues to discard
    frames by thread id, and wire-level rejection by generation arrives with the bootstrap
    transaction envelope.

### Not touched

`giskard-proto`, `giskard-core`, `giskard-persist`, `giskard-harness*`, `giskard-testenv`,
`static/app.js`, `routes.rs`, the registry, `thread_runtime/`, `delivery.rs`,
`docs/api-endpoints.md`, `README.md`, `tests/e2e/`, and every existing test.

## Exit checks

| # | Check | Expected |
| --- | --- | --- |
| A | `rg -n "handle_client_msg\(" crates/giskard-server/src/ws.rs` | the definition and the one call at the loop, now with `&mut slots` |
| B | `rg -n "fn run_subscribe_bootstrap\|struct SubscriptionSlots\|struct BootstrapSlot" crates/giskard-server/src/ws.rs` | 3 |
| C | `rg -c "cancelled.load\|bail_if_cancelled" crates/giskard-server/src/ws.rs` | ≥ 9 (six phase checks, sends, the error path) |
| D | `rg -n "recompute_aggregates\|load_history\|load_turns_after\|live_snapshot\(\)" crates/giskard-server/src/ws.rs` | all inside `run_subscribe_bootstrap`, none in `handle_client_msg` |
| E | `rg -n "pub async fn client_count" crates/giskard-server/src/hub.rs` | 1 |
| F | `git diff --stat main -- crates/giskard-proto crates/giskard-core crates/giskard-persist crates/giskard-harness crates/giskard-harness-codex crates/giskard-harness-replay crates/giskard-testenv crates/giskard-server/static crates/giskard-server/src/routes.rs crates/giskard-server/src/registry crates/giskard-server/src/thread_runtime tests/e2e` | empty |
| G | `rg -n "Version:\*\* 1.95\|\*\*CS[1-3]:\*\*" specs/giskard-specification.md` | 4 lines |
| H | `rg -n "abort\(\)" crates/giskard-server/src/ws.rs` | only the pre-existing writer-task abort at the loop exit |
| I | `cargo fmt --all --check` | clean |
| J | `cargo clippy --workspace --all-targets --locked -- -D warnings` | clean |
| K | `cargo test -p giskard-server --locked --test subscribe_cancellation --test e2e_smoke --test history_sync --test turn_steering` | green |
| L | Test 1 run against `main` before the change | fails at the first wait, proving the observation |

## Signs the step has gone wrong

- `tokio::task::JoinHandle::abort` on a bootstrap task, or a `select!` racing the attach against
  the cancel flag: that is the abort the milestone forbids.
- `hub.subscribe` moved before `ensure_thread_open`: a missing thread would leave a subscription.
- Any change to what a bootstrap sends or in what order, or a new field on any `ServerMessage`.
- A slot map outside the connection handler, or one keyed by anything but this connection's own
  threads: it would be the authority map `AGENTS.md` forbids.
- The generation appearing in `giskard-proto`: that is M12.
- Test 1 passing on `main` unchanged: the assertion is not observing the parked loop.

## Size

About +100 production lines in `ws.rs` and `hub.rs` (most of the arm body moves rather than
changes), +250 test lines, +15 spec lines. Well under the plan's ceiling; if `app.js` or
`giskard-proto` appear in the diff, M12 has crept in.
