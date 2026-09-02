# Event pipeline milestones

Companion to [`event-pipeline-architecture-review.md`](event-pipeline-architecture-review.md).
That document says what is wrong. This one says how to get out in bounded steps.

## Why previous attempts rewrote half the code base

Three habits, visible in #198, #205 and #219, turn a milestone into a rewrite:

1. **Bundling.** The #199 plan's M3 bundled the transport, first-frame activation, receiver
   claims, owner reconciliation and the lag fallback into one switch because each was believed to
   require the others. Under a push model they do. Under a retained log they do not.
2. **Changing the trait surface.** Removing `subscribe`, adding capability types or changing
   `open_thread`'s return type forces every harness implementation (Codex, replay crate, the
   scripted replay binary) and every test fake to change in the same PR. #219 touches 32 files for
   that reason alone.
3. **Renaming while changing behavior.** The #206 to #217 consolidation series moved and renamed
   types. Doing that in the same PR as a behavior change makes the diff unreviewable and the
   behavior change unfalsifiable.

The plan below is cut against those habits with five rules.

## Rules for every milestone

- **One seam per milestone.** A milestone changes either the adapter side of `AgentHarness` or
  the server side, never both. The trait surface changes at most once per milestone, additively.
- **Additive, then switch, then delete.** The new path lands next to the old one; the switch is a
  one-line change; deletions are mechanical and may be split into a follow-up PR.
- **No renames or moves** in a milestone that changes behavior.
- **Budget: about 1000 changed non-test lines.** If the diff crosses it, the cut is wrong. Stop and
  split rather than push through.
- **Exit criteria are deletions and flipped-on tests**, never new mechanisms. A milestone that
  needs a new lock, token, generation, epoch or handoff to reach its exit is the wrong cut.

## The seams, as they exist today

| Seam | Where | Why it bounds a change |
| --- | --- | --- |
| `AgentEventStream` | `giskard-harness/src/lib.rs` | The only type through which events cross from any harness to the server. Its inner type leaks nowhere in production server code except the forwarder's error arm (`event_forwarder.rs:920`). |
| `AgentHarness::subscribe` | one caller, `registry.rs:2561` | Changing what it returns is a one-site change on the server. |
| `CodexTransport` | `giskard-harness-codex/src/lib.rs:599` | Isolates the stdout reader and RPC path from the mapper and the event log. |
| `ThreadEventForwarder::handle_event` | `event_forwarder.rs:1024` | The forwarder is already a state machine struct; only `run()` owns a stream and a task. |
| `HarnessFactory::create` and `spawn_thread_update_forwarder` | `registry.rs:481,677` | The existing place to attach a per-harness background consumer. |

## M0 — Scenario tests that fail today

**Goal.** Make the loss observable before touching anything, so every later milestone flips tests
on rather than writing new ones.

**Scope.** Tests only. `giskard-harness-codex` already has `FakeCodexTransport` and
`FakeCodexController` (`lib.rs:3385`); the server has the scripted harness in
`bin/giskard-server-replay.rs` and `tests/support`.

**Deliverable.** Ignored tests, tagged by the milestone that enables them, for:

- A: child `thread/started`, `turn/started`, `item/*`, `turn/completed` on stdout before the
  server's claim arrives; assert every child event reaches a subscriber. (M2)
- B: events sent after the route exists and before `subscribe`; assert delivery. (M1)
- C: 300 events with a stalled subscriber; assert none lost and no `Interrupted` turn. (M1)
- D: owner replaced mid-turn; assert the replacement sees the tail. (M1, fully M4)
- E: child events arriving while no turn is active in the adapter. (M1 with the polling gate, M5)
- Parent link before child frames, child frames before parent link, and interleaved; assert one
  persisted thread, one persisted turn, same `ThreadId`. (M2)

Salvage the deterministic scenarios from #219's tests where they fit.

**Exit.** Tests exist and are ignored with a milestone tag. Zero production changes.

**Status.** Landed in <PR>. Run `cargo test -p giskard-harness-codex --lib -- --ignored loss_scenarios`
and `cargo test -p giskard-server --lib -- --ignored replacement_forwarder` to see the current
failures.

## M1 — Retained event log in place of broadcast

**Goal.** Close windows B, C and D. Delete `Lagged` and the "persist prefix as Interrupted" fallback.

**Seam.** `AgentEventStream` plus its producers. Server changes limited to the one error arm.

**Design.** Add `giskard_harness::EventLog`: per-thread `VecDeque<AgentEvent>` with a base
sequence, a `Notify`, and cursor-based readers. `AgentEventStream` becomes a cursor over a log.
`recv()` returns the next event at the cursor or waits; it never lags. Entries below the lowest
cursor are trimmed; with no reader the log retains everything up to a byte cap, and crossing the cap
records an explicit `Gap` marker rather than dropping silently. This is a pull model: no pump task,
no channel, and `subscribe` stays synchronous because creating a cursor is synchronous.

**Scope.**

- `giskard-harness/src/lib.rs`: `EventLog`, `AgentEventStream` over it, `recv() -> Option<AgentEvent>`
  (`None` only on close), remove `into_inner`.
- `giskard-harness-codex/src/lib.rs`, `instance.rs`: `SenderMap` becomes a map of `Arc<EventLog>`;
  `broadcast_event` appends; `ensure_thread_route_sender` creates a log; deletion closes it.
- `giskard-harness-replay/src/lib.rs` and `bin/giskard-server-replay.rs`: same `EventLog`; delete
  `wait_for_receiver`.
- `registry/event_forwarder.rs`: `handle_stream_error` becomes a closed-stream handler; delete the
  `lagged` branches.
- Test fakes in `registry.rs` and `event_forwarder.rs` tests: mechanical.

**Not in scope.** Coordinator, owner lock, materialization, persistence, hub, trait methods.

**Exit.** `tokio::sync::broadcast` absent from all four harness sites and the forwarder; #200 closed;
M0 tests B, C, D pass; `docs/subagents.md` paragraph on lag truncation deleted.

**Size.** About 600 to 900 non-test lines.

## M2 — Identity minted at ingest, discoveries announced

**Goal.** Close window A. Make `ThreadKind::Orphan` reachable. Retire #219.

**Seam.** Adapter, plus one additive trait method and one additive server consumer.

**Design.** In `NativeThreadRoutes::resolve`, an unknown non-empty native id is claimed with a fresh
final `ThreadId` and its event log is created on the spot, so the frame that revealed the thread is
the first entry in its log. The adapter appends `ThreadDiscovered { thread, harness_thread_id,
parent_harness_thread_id }` to a per-harness discoveries log built on the same `EventLog` type, and
`AgentHarness::discoveries()` returns a replaying reader for it. Because the child's log retains
from its first event, the order in which the server consumes discoveries versus events is
irrelevant.

`claim_native_thread` keeps its signature. Its meaning changes in one respect: the proposed
`ThreadId` is used only if the native id is unbound; if traffic already bound it, the returned
handle carries the adapter's id. `materialize_subagent_thread` already writes `id: handle.thread`,
so it adopts that id without change. Bootstrap claims stay exact and conflicting bootstrap bindings
still fail harness publication.

**Scope.**

- `native_routes.rs`, `mapping.rs`, `instance.rs`: claim-on-unknown, discoveries log, `claim_or_adopt`
  for the live path while bootstrap keeps exact `claim`.
- `giskard-harness/src/lib.rs`: `ThreadDiscovered`, `discoveries()` with a default that returns a
  closed reader.
- `registry.rs`: `spawn_discovery_consumer` next to `spawn_thread_update_forwarder`. Under the
  existing project lifecycle lock it creates the orphan `ThreadFile` if no thread has that native
  id, then calls the existing `install_event_owner`. The parent-link path is unchanged: it finds the
  orphan and takes the existing `Orphan -> Subagent` branch (`registry.rs:2078`).
- Replay harnesses: `discoveries()` default is enough.

**Not in scope.** Receiver custody, tickets, tombstones, owner lifecycle, deletion changes.

**Exit.** `"dropping Codex notification for unknown native thread"` is unreachable for a non-empty
id; M0 tests A and the parent/child ordering set pass; #219 closed with its scenarios ported;
`docs/subagents.md` describes discovery as adapter-minted identity.

**Size.** About 500 to 800 non-test lines.

After M2 the loss is fixed. M3 onward removes the machinery that makes every later change expensive.
They can be paced, and each still follows the rules.

## M3 — Single stdout reader

**Goal.** Bound adapter memory and order RPC responses relative to notifications. This is #218's
slice, restated: it stays behind `CodexTransport` and touches nothing above it.

**Design.** One reader task decodes stdout and appends every frame to the instance's inbox;
responses are matched by request id to one-shot waiters. `request_json` never reads stdout. The
inbox is bounded by the same byte cap and `Gap` rule as `EventLog`, so saturation is explicit rather
than either silent or a stall inside Codex.

**Exit.** No code path reads stdout except the reader; `AsyncClient::request` is not called;
#217's open finding on write-failure classification is addressed at the writer.

**Size.** #218 was this. Independent of M2, may land in either order after M1.

## M4 — One driver per project

**Goal.** Delete per-thread event tasks and the owner lifecycle protocol.

**Seam.** Server registry only. No adapter change: the driver reads the same `AgentEventStream`s.

**Design.** `ProjectEventDriver` is one task per harness that owns
`HashMap<ThreadId, ThreadEventForwarder>` and a `SelectAll` over their streams plus the discoveries
reader from M2. Installing an owner becomes "send `Attach { binding }` to the driver"; retirement
becomes `Detach`. The forwarder keeps `handle_event` unchanged and loses `run()`, `cancel` and
`stream`. Per-thread state is owned by the driver, so nothing needs a lock to be exclusive.

**Scope.** `registry.rs` (`install_event_owner*`, `launch_event_forwarder`,
`lock_thread_owner_after_drain`, `retire_thread`, `forget_thread`), `registry/thread.rs`
(`OwnerPhase`, `EventOwnerControl`, `OwnerLock`, `CoordinatorSlot` shrink), the top of
`event_forwarder.rs`. The coordinator's prepared-operation and turn-claim logic stays for M5.

**Not in scope.** `start_turn`, intents, materialization, persistence.

**Exit.** No `tokio::spawn` per thread for events; `OwnerPhase`, `OwnerLock`,
`lock_thread_owner_after_drain`, cancel and completed watches deleted; M0 test D passes in full.

**Size.** The largest. Bound it by refusing to touch persistence, hub, routes or the adapter. If it
needs more than the files above, split "driver with old coordinator" from "delete owner protocol".

## M5 — Intents replace prepared operations

**Goal.** Delete `CoordinatorToken`, generations and `PreparedOperation`.

**Design.** `HarnessRegistry::start_turn` sends `Intent::StartTurn { input, overrides, reply }` to
the driver. The driver calls `harness.start_turn`, and correlates the returned `TurnId` with the
native turn it later sees, because the adapter already registers the id from the `turn/start`
response before notifications stream (`mapping.rs:229`). Compaction and interrupt follow the same
path. The polling gate in `should_poll_codex_messages` is removed: the reader always reads and the
log absorbs.

**Exit.** `ThreadCoordinator` is plain data inside the driver with no mutex; `admit_operation`,
`abort_admitted_operation`, `acknowledge_operation_turn`, `CoordinatorToken` deleted; M0 test E
passes.

## M6 — Materialization off the event path

**Goal.** Delete the per-parent FIFO and the project lifecycle lock from the event path.

**Design.** With M2, a parent link only classifies an orphan the driver already knows, or records a
link for a child whose discovery has not been consumed yet. Both are per-thread state updates in the
driver. Graph validation runs at classification time only, not on every `interacted` activity.

**Exit.** `enqueue_subagent_materialization`, `MaterializationSlot`, `coordinator_snapshot` scans
and `load_thread_graph` on the hot path deleted; `lock_project_lifecycle` is not taken between
stdout and persistence.

## M7 — Cursor-committed persistence (optional)

**Goal.** Crash-safe replay of an unpersisted turn.

**Design.** The history index record carries the log sequence it represents; the driver commits its
cursor with the turn; the adapter's log optionally spills to an append-only segment. On restart the
tail is re-projected. Item upserts already make re-application idempotent.

**Exit.** A server killed mid-turn restarts and persists the completed turn without an
`Interrupted` synthesis.

## Ordering and dependencies

```text
M0 ──► M1 ──► M2 ──► M4 ──► M5 ──► M6 ──► M7 (optional)
              │
              └──► M3 (any time after M1)
```

M1 and M2 fix the reported loss. Everything after is complexity removal and can be scheduled. Each
milestone is a PR that stands alone on `main`; none needs a long-lived branch.

## Signs that a milestone has gone wrong

- The diff adds a lock, token, generation, epoch, ticket or handoff.
- A harness implementation other than the one being changed needs more than a mechanical edit.
- The PR renames or moves a type.
- A new test enumerates a schedule instead of asserting idempotence.
- The diff passes 1000 non-test lines.

Any of these means stop, keep what is additive, and re-cut.

