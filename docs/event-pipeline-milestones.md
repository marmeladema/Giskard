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
- E: child events arriving while no turn is active in the adapter. (M3)
- Parent link before child frames, child frames before parent link, and interleaved; assert one
  persisted thread, one persisted turn, same `ThreadId`. (M2)

Salvage the deterministic scenarios from #219's tests where they fit.

**Exit.** Tests exist and are ignored with a milestone tag. Zero production changes.

**Status.** Landed in <PR>. Run `cargo test -p giskard-harness-codex --lib -- --ignored loss_scenarios`
and `cargo test -p giskard-server --lib -- --ignored replacement_forwarder` to see the current
failures.

## M1 — Retained event log in place of broadcast

**Status.** Implemented by the retained-event-log change.

**Goal.** Close windows B, C and D. Delete `Lagged` and the "persist prefix as Interrupted" fallback.

**Seam.** `AgentEventStream` plus its producers. Server changes limited to the one error arm.

**Design.** Add `giskard_harness::EventLog`: per-thread `VecDeque<AgentEvent>` with a base
sequence, a `Notify`, and cursor-based readers. `AgentEventStream` becomes a cursor over a log.
`recv()` returns the next event at the cursor or waits; it never lags. Entries below the lowest
cursor are trimmed; with no reader the log retains everything up to an entry-count cap, and crossing
the cap records an explicit `Gap` marker rather than dropping silently. `EVENT_LOG_RETAIN_LIMIT`
counts entries, not bytes; Codex transport input is separately bounded per frame by
`CODEX_MAX_FRAME_BYTES`. This is a pull model: no pump task, no channel, and `subscribe` stays
synchronous because creating a cursor is synchronous.

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

**Status.** Implemented by the M2 identity-at-ingest change, which supersedes and closes #219.

**Goal.** Close window A. Make `ThreadKind::Orphan` reachable. Retire #219.

**Seam.** Adapter, plus one additive trait method and one additive server consumer.

**Design.** When instance-level mapping reports an unknown non-empty native id, the adapter claims
it with a fresh final `ThreadId` and creates its event log on the spot, then retries mapping so the
frame that revealed the thread is the first entry in its log. The adapter appends
`ThreadDiscovered { thread, harness_thread_id, parent_harness_thread_id }` to a per-harness
discoveries log built on the same `EventLog` type before appending that event, and
`AgentHarness::discoveries()` returns a replaying reader for it. The mapper's lower-level unknown
route rejection remains unchanged.

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

**Status.** Implemented by the single-stdout-reader change.

**Goal.** Bound adapter memory and order RPC responses relative to notifications. This is #218's
slice, restated: it stays inside the Codex adapter. The transport is the primary seam; the only
instance behavior change is the one-line removal of the adapter polling gate.

**Design.** One reader task decodes stdout and appends notifications and requests to the
transport-owned inbox; responses are matched by request id to one-shot waiters. `request_json`
never reads stdout. The retained `EventLog` inbox has a 65,536-frame cap; a `Gap` is fatal, and the
reader never blocks on a full inbox. The cap counts frames, and each stdout JSONL frame is bounded
to `CODEX_MAX_FRAME_BYTES` before it enters the inbox. One bounded-queue writer owns stdin and
flushes each whole frame. The `codex-codes` stderr-drain helper is private, so production uses an
equivalent local task: it continuously reads lines, strips ANSI control sequences, and routes recognizable
`ERROR`, `WARN`, and `DEBUG` lines to the corresponding tracing level, with other lines at
`TRACE`.

**Exit.** No code path reads stdout except the reader; `AsyncClient` and
`should_poll_codex_messages` are absent; M0 test E passes; #217's open finding on write-failure
classification is addressed at the writer.

**Size.** #218 was this. Independent of M2, may land in either order after M1.

## M4 — One driver per project

**Status:** Complete.

**Goal.** Delete per-thread event tasks and the owner lifecycle protocol.

**Seam.** Server registry only. No adapter change: the driver reads the same `AgentEventStream`s.

**Design.** `ProjectEventDriver` is one task per harness that polls every
`ThreadEventForwarder::run` future in a `FuturesUnordered`. Installing an owner becomes
`Attach { binding }`; retirement becomes `Detach`. The forwarder's reduction loop is unchanged.
Owner transitions are serialized by the driver, and attaches arriving during detach are parked
until the old owner exits.

**Scope.** `registry.rs` (`install_event_owner*`, `launch_event_forwarder`,
`lock_thread_owner_after_drain`, `retire_thread`, `forget_thread`), `registry/thread.rs`
(`OwnerPhase`, `EventOwnerControl`, `OwnerLock`, `CoordinatorSlot` shrink), the top of
`event_forwarder.rs`. The coordinator's prepared-operation and turn-claim logic stays for M5.

**Not in scope.** `start_turn`, intents, materialization, persistence.

**Exit.** No `tokio::spawn` per thread for events; the drain protocol and completed watches are
deleted; one cancellation watch remains per forwarder; M0 test D passes in full.

**Size.** The largest. Bound it by refusing to touch persistence, hub, routes or the adapter. If it
needs more than the files above, split "driver with old coordinator" from "delete owner protocol".

## M5 — Intents replace prepared operations

**Status:** Complete. Implemented by the turn-intent change. See
[`m5-turn-intents.md`](m5-turn-intents.md) for the implementation plan and interleaving analysis.

**Goal.** Delete `CoordinatorToken`, generations and `PreparedOperation`.

**Design.** `HarnessRegistry::start_turn` and `compact_thread` send a `TurnIntent` through the
coordinator's live owner phase to the thread's event forwarder. That one sequential owner reserves
the runtime lease, polls the harness request alongside retained events, and attaches the first new
native turn it observes to the admitted intent. With admission and observation in one loop there is
no stale preparation to guard with a generation or token. Interrupt remains a direct harness call
because it reserves no turn.

The coordinator remains mutex-protected data for the binding, classification, and owner phase;
those fields still have registry, materialization, and driver readers and move in M6 territory.

**Exit.** `CoordinatorToken`, generations, `PreparedOperation`, and the coordinator's operation and
native-turn methods are deleted. Primary turn leases are reserved only by the forwarder.

## M6 — Materialization off the event path

**Status:** Complete. Implemented by the native-identity admission change. See
[`m6-native-identity-admission.md`](m6-native-identity-admission.md) for the implementation plan
and deletion-ordering analysis.

**Goal.** Delete the per-parent FIFO and the project lifecycle lock from the event path.

**Design.** Discoveries, sub-agent links from forwarders, and explicit link opens are native
identity admissions processed one at a time by the project's event driver. Native-to-Giskard
identity lookup uses the harness's idempotent claim operation. A claimed identity is always
recorded, while the thread graph is loaded only when the driver must decide or validate a
relationship, not for repeated activity on an already classified child.

**Exit.** `enqueue_subagent_materialization`, `MaterializationSlot`, the admission-path
`coordinator_snapshot` scans, and hot-path graph loads are deleted. `lock_project_lifecycle` is no
longer taken between stdout and persistence.

## M7 — Lifecycle fences and reader contracts

**Status.** Implemented by the lifecycle-fences change.

**Goal.** Close the remaining lifecycle races at the driver admission lane, retained-log reader
boundary, forwarder intent lane, and Codex stdout transport.

**Design.** Project deletion and registry shutdown quiesce each project driver before taking the
authoritative owner set or shutting down its harness. A quiesced driver stops polling discovery and
holds reply-less forwarder links for a possible resume. Failed reply-less admissions are retried
after the next successful admission or resume, at most three times. An `EventLog` eviction that
occurred with no reader is reported as a `Gap` to the next reader created. Forwarders process
retained events before intents; cancellation closes the intent lane and rejects anything already
queued, so sender clones cannot admit work after detach. Codex stdout frames have a 64 MiB maximum;
exceeding it fatally closes the transport.

**Exit.** Deletion and shutdown cannot race admission; failed deletion does not consume discovery;
reply-less admission has a bounded event-driven retry; no-reader eviction is visible to the next
consumer; queued native events precede intents; cancelled owners admit no new intent; oversized
frames fail loudly.

## M8 — Admission and reader fences

**Status.** Implemented by the admission-and-reader-fences change.

**Goal.** Finish the quiesce, deferred-admission, and retained-log reader fences left open by M7.

**Design.** A quiesced driver refuses event-owner attachment, making the deletion snapshot final.
Deferred admissions have no lifetime attempt cap, are deduplicated by native id, and retry one at
a time only when an attach, detach, owner exit, successful admission, or resume wakes the driver.
When the last retained-log reader is dropped behind its cursor, its unreported deficit is handed to
the next reader.

**Exit.** No owner can attach after deletion quiesces; transient admission failures remain
recoverable without polling or retry loops; dropping a lagged last reader cannot erase evidence of
retention loss.

## M9 — Cursor-committed persistence (optional)

**Goal.** Crash-safe replay of an unpersisted turn.

**Design.** The history index record carries the log sequence it represents; the driver commits its
cursor with the turn; the adapter's log optionally spills to an append-only segment. On restart the
tail is re-projected. Item upserts already make re-application idempotent.

**Exit.** A server killed mid-turn restarts and persists the completed turn without an
`Interrupted` synthesis.

## Ordering and dependencies

```text
M0 ──► M1 ──► M2 ──► M4 ──► M5 ──► M6 ──► M7 ──► M8 ──► M9 (optional)
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
