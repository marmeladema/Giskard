# Why the sub-agent event race keeps coming back

An architecture review of the harness-to-server event pipeline, written against `main` at
`b6fdf22` (2026-09-02), issue #199 and its plan comments, and PRs #196 through #219.

## 1. The short answer

The lost-early-events problem is not a bug in the sub-agent code. It is the predictable result of
three decisions that sit underneath the sub-agent code, and every fix so far has kept all three:

1. **Events are delivered through a receiver-gated, lossy transport.** The Codex adapter fans
   events out on `tokio::sync::broadcast` channels. A broadcast event exists only if a receiver
   already exists *and* keeps up. Nobody subscribed yet means the event is discarded; a slow
   subscriber means `Lagged` and the event is gone.
2. **Identity is decided late, by a different component, from a different stream.** The adapter
   refuses to route a native thread id it has not been told about ("never inferred from
   traffic") and drops the frame. The server learns about a child from the *parent's* transcript,
   then tells the adapter through a control-queue round trip, after several disk reads, a project
   lock, and an fsync. The child's own frames are on the wire before any of that starts.
3. **Consumption is one independently-lifecycled task per thread**, with "exactly one consumer" expressed as a stack of locks, generations and tokens spanning two crates.

Each decision creates a "must exist before" ordering between objects that are created by different
tasks in reaction to different inputs. A fix for one ordering adds a mechanism, and every mechanism
adds two more states whose relative order is observable. That is why each review round finds a new
interleaving, why #199's own plan needed a 70-row schedule matrix and 16 merge criteria, and why
PR #219 is a +10.7k-line change that still keeps the broadcast transport.

The sound fix is not another ordering mechanism. It is to **remove the orderings**: make ingest
total (every frame is recorded under the key the producer already knows, the native thread id,
whether or not anyone knows what that thread is yet), make Giskard identity a derived and
idempotent label on that record, and make consumers cursors over the record instead of owners of a
live channel. Under that shape most of the machinery below deletes itself. Section 6 gives the
concrete design and an incremental path.

## 2. How a child's events travel today

### 2.1 The pipeline on `main`

```text
codex app-server stdout (one ordered JSON-RPC stream per project)
  │
  ▼
CodexInstance task                         crates/giskard-harness-codex/src/instance.rs
  select! { biased; shutdown, stdout msg, command queue, control queue }
  │  mapper.map_notification(notif, fallback)
  │    native threadId ──► NativeThreadRoutes.resolve()      native_routes.rs:145
  │      unknown, non-empty id ──► Err(UnknownNativeThread) ──► DROPPED (warn)   mapping.rs:391
  ▼
broadcast_event(SenderMap, ThreadId, event)                  lib.rs:2329
  per-ThreadId tokio::sync::broadcast(256)                    lib.rs:55, :272
    no receiver ──► DROPPED silently
    slow receiver ──► Lagged, events DROPPED
  │
  ▼  harness.subscribe(handle)  (sync; no sender yet ⇒ dead receiver)   lib.rs:1374
ThreadEventForwarder task (one per thread, "long-lived owner")   registry/event_forwarder.rs
  ├─ ThreadCoordinator: claim native turn, tokens, generations     registry/thread.rs
  ├─ ThreadRuntimeRegistry: turn lease, live buffer, running tasks thread_runtime.rs
  ├─ persist turn on TurnCompleted (payload then index)
  └─ Hub ──► per-client bounded mpsc ──► WebSocket
```

Sub-agent discovery runs *beside* that pipeline, not in it:

```text
parent forwarder sees ItemStarted/ItemCompleted with a SubagentLink   event_forwarder.rs:1409
  └─► enqueue_subagent_materialization (per-parent FIFO, spawned worker)   registry.rs:2252
        └─► materialize_subagent_thread                                    registry.rs:1971
              lock_project_lifecycle
              load project file, load parent thread file
              coordinator_snapshot (locks every live coordinator)
              load_thread_graph (reads every thread file in the project)
              harness.claim_native_thread  ──► control queue ──► CodexInstance   lib.rs:1332
              thread_metadata.create (atomic write + fsync)
              install_event_owner: owner lock (after drain), intern authority,
                                   subscribe(), spawn forwarder                registry.rs:2535
```

### 2.2 The loss windows

| Window | From | To | Where the event dies |
| --- | --- | --- | --- |
| A | Codex emits the child's first frame | `claim_native_thread` runs inside `CodexInstance` | `mapping.rs:406` "dropping Codex notification for unknown native thread" |
| B | the route and sender exist | the forwarder calls `subscribe()` | `broadcast::Sender::send` with zero receivers |
| C | any time | receiver falls 256 events behind | `Lagged`; the forwarder persists the prefix as `Interrupted` and ignores the rest of that native turn (`event_forwarder.rs:920`) |
| D | owner retirement/replacement | the replacement subscribes | the draining receiver is dropped; the new one starts at "now" |
| E | Codex stdout while no turn is active | the next reason to poll | `should_poll_codex_messages` gates reading on adapter-level state (`instance.rs:106`), so frames sit unread in the pipe |

Window A is not a narrow race. It is structurally ordered to lose:

1. The `CodexInstance` select is `biased` with stdout ahead of the control queue. The claim is a
   control command, so it is served only when no stdout frame is ready. Under load, every child
   frame already in the pipe is processed, and dropped, before the claim is seen.
2. The claim is requested only after the parent's link event has traversed the same pipeline, a
   queue, a project lock, several file reads, and the round trip. Codex submits the child's first
   turn before the tool call that names it returns (#196 description), so the child's frames are
   on the wire before step one begins.

Windows B, C and D exist for every thread, not only sub-agents. Issue #200 documents C on primary
threads. The "long-lived owner" landed in #203 removed the per-turn version of D but not the
replacement version.

### 2.3 What already exists but is unreachable

`ThreadKind::Orphan`, its revision-checked `Orphan -> Subagent` classification and the
`TurnModel::Unknown` / `TurnMode::Unknown` states are on `main` (#204), but no production path
creates an orphan: the adapter drops unknown ids, so the server never sees an event it cannot
attribute. The orphan machinery is waiting for PR #219 to make it reachable.

## 3. What has been tried

| Change | Kept the three premises? | What it added | Outcome |
| --- | --- | --- | --- |
| #119 sub-agent support | yes | passive monitors, ten-minute idle bound, terminal-evidence fallback turns, handoffs | the "defect generator" (#196) |
| #196 rekey adapter by native id | yes | cheap reconciliation of two identities | closed: "stop the second identity from existing at all" |
| #197 `bind_known_threads` | yes | pre-register persisted threads at harness creation | merged; author notes "this fixes routing, not delivery" |
| #198 retention | yes | provisional binding on `thread/status/changed`, retain-until-subscribe, TTL sweep | closed after four review rounds, each a real race; the `terminal(N) → active(N+1) → TurnCompleted(N) → TurnStarted(N+1)` loss |
| #203 long-lived owner (M1) | yes | one forwarder per binding generation, `ThreadCoordinator`, two-phase `Live → Draining` retirement | merged; lag still truncates; failed-owner self-removal window admitted |
| #204 durable identity (M2) | yes | `HarnessBootstrap`, route epochs, `Orphan`, `Unknown` model/mode | merged |
| #205 bounded ownership (M3) | partly | sole stdout reader, bounded per-route mpsc, activation ack before first-frame delivery | closed (12.6k lines); M2/M3 split found "internally inconsistent" |
| #206–#217 consolidation | yes | authorities, slots, typed identifiers | merged |
| #218 sole-reader transport | partly | one reader, one bounded writer, inbox still unbounded | closed |
| #219 traffic-driven discovery | yes (still broadcast) | route authority with slots, activation keys, claim keys, tombstones; receiver custody as linear capabilities (`DiscoveryTicket → ThreadAttachment → ThreadEventOwner`); gated owner install; Primary typestate with rollback | open, unreviewed, +10.7k |

The trend is the point: each attempt is larger than the last, and each is correct about the defect
it names. #199's own signal-versus-inference table says it best: "a new edge appears whenever two
of those diverge in timing. Enumerating the timings has not converged."

## 4. Why every fix becomes a huge change

### 4.1 Two authorities for one fact, joined by a lossy channel and an RPC

"Which Giskard thread do these native frames belong to, and who is consuming them" is decided in
two places: the adapter's `NativeThreadRoutes` + `SenderMap`, owned by the Codex task, and the
server's `ThreadAuthority` + `ThreadCoordinator` + owner, owned by server tasks. The two are joined
by a fire-and-forget broadcast in one direction and a control queue in the other. Any fix must
therefore move state across that boundary while frames keep arriving, which is exactly where
epochs, tokens, drains and custody transfer come from.

### 4.2 The "must exist before" table

Every row is an ordering between objects created by different tasks for different reasons. Every
row has produced at least one reported race.

| X must exist before Y | X is created by | Y is created by |
| --- | --- | --- |
| route (native → ThreadId) | server, after parent link + locks + disk | Codex, when the child's first frame arrives |
| broadcast receiver | server, when the owner installs | adapter, at every `send` |
| owner generation N retired | forwarder task, after drain | installer of generation N+1 |
| prepared operation | HTTP handler | forwarder, at the next `TurnStarted` |
| `TurnStarted` observed | forwarder | any turn-scoped item (Codex does not guarantee this order; the forwarder has a "before seeing turn start" path) |
| orphan thread file | discovery consumer | parent-link classification |
| adapter polling enabled | adapter state (active turns, running commands) | Codex emitting for a thread the adapter has not registered |

A design that has to satisfy this table is a design where *ordering is the invariant*. Tests for
such a design enumerate schedules, which is why the test halves of these files are as large as the
code halves:

| File | Lines | First test line |
| --- | --- | --- |
| `giskard-server/src/registry.rs` | 4159 | 2609 |
| `giskard-server/src/registry/event_forwarder.rs` | 4997 | 1753 |
| `giskard-server/src/thread_runtime.rs` | 3378 | 2025 |
| `giskard-harness-codex/src/lib.rs` | 6297 | 3246 |
| `giskard-harness-codex/src/mapping.rs` | 7101 | 3359 |

Schedule tests can only cover the schedules someone thought of. They cannot prove the absence of the
next one.

### 4.3 The concurrency primitives that express "one consumer per thread" on `main`

Adapter: `SenderMap` (`std::Mutex<HashMap<ThreadId, broadcast::Sender>>`), one broadcast channel
per thread, command queue, control queue, worker-queue watchdog, shutdown watch, per-open
`ThreadUpdateSink` mpsc(1).

Server: global registry mutex over project and thread indexes, per-project `LifecycleLock` with
weak unpublished pre-locks, per-thread `OwnerLock` with weak pre-locks and a drain-and-retry loop,
`CoordinatorSlot`, `ThreadCoordinator` async mutex + `Notify` + `CoordinatorToken{generation,
sequence}`, `OwnerPhase` (5 states), `NativeActivity` (4), `NativeTurnOrigin`, per-thread
`ThreadRuntimeSlot` with `ThreadTurnLease` / `RestorePermit` / output-version permits, per-parent
`MaterializationSlot` FIFO with worker permits, `RegistryTaskTracker` permits, cancel and completed
watches per owner.

That is roughly a dozen locks and a half dozen small state machines cooperating to represent a single
fact. #219 adds a route table with three key spaces (`SlotId`, `ActivationKey`, `ClaimKey`),
`SlotState`, `Custody`, `DiscoveryState`, a discovery mpsc with defer/pending handling, a gated
owner installation, and a ten-state Primary typestate.

### 4.4 The plan's own admissions

The #199 plan (comment of 2026-08-28) already names the real culprits: "lossy broadcast, the
unbounded `AsyncClient` buffer, best-effort binding preload" and "Discovery starts from
`thread/status/changed`". Its remedy, however, keeps the push model and adds a handshake: the sole
stdout reader must *retain a frame and pause* while the server persists an orphan and installs an
owner, then acknowledge, then resume. The plan itself lists the consequences: no registry, project,
graph or coordinator lock may be held while any other path awaits a Codex RPC ("otherwise a
preceding notification and the response behind it can deadlock"); "one full thread route pauses
every route on the shared Codex stdout reader"; and pausing does not bound memory end to end because
"Codex Core's session event channel is currently unbounded". The 2026-08-30 comment then concedes
that the activation guarantee cannot even be enforced until the transport is replaced.

A design whose correctness argument requires a stdout reader to block on persistence, and whose
backpressure story ends in another process's unbounded queue, is telling you that push is the wrong
direction.

## 5. PR #219 specifically

#219 is a careful implementation of the wrong premise. It fixes window A by claiming a final
`ThreadId` on first sight of an unknown native id and pre-creating the broadcast receiver so that
the first `send` has somewhere to go. Because a broadcast receiver is the *only* place those events
exist, the receiver becomes a physical object that must be carried, without loss, from the adapter's
route slot through `DiscoveryTicket`, `ThreadAttachment` and `ThreadEventOwner` to the forwarder
task, returned on every failure path (drop hooks, exact `ClaimKey` matching, `PersistenceBlocked`
retention of the owner object), and never duplicated. `AGENTS.md` on that branch spells the
consequence out: "Native event ownership is linear … Do not recreate `AgentHarness::subscribe`,
clone these capabilities, or replace their physical custody with cross-crate reconciliation state."

Roughly 60% of `native_routes.rs`, all three capability types, and the retention branches of
`owner.rs` and `thread.rs` exist to move one receiver around safely. `discovery.rs`, the defer and
pending states, and the bounded discovery channel exist because the announcement travels on a third
channel that may be full while stdout must not block. `owner.rs`'s prepare/commit split exists to
close the gap between consuming the attachment and starting the task. None of this is needed if the
events are retained by a log instead of by a receiver, because a log can be read by anyone, from any
position, at any time, and there is nothing to transfer.

Recommendation: do not merge #219. Salvage its deterministic race scenarios as the acceptance suite
for the redesign below, and its documentation of Codex discovery signals. Its runtime mechanisms
are the cost of the premise, not the cure.

## 6. A sound shape: journal first

### 6.1 Principle

> Ingest is total and unconditional. Identity and ownership are derived, idempotent and lazy.

Nothing about routing, identity, persistence, browser delivery or locking may stand between a frame
arriving on stdout and that frame being recorded. Everything else reads the record.

### 6.2 Components

**Project event journal.** One per Codex process, owned by the `CodexInstance` task, which is
already the single stdout reader. Every decoded frame is appended with a monotonically increasing
sequence number and the native thread id it names (or a process-level key for frames without one,
closing #201). RPC responses go through the same reader and the same journal, so a `thread/start`
response is ordered relative to the notifications around it for free. The journal is retained until
every cursor has passed an entry; it is bounded by bytes with spill to an append-only segment file,
never by "someone is listening". Optionally the segment is always written, which makes mid-turn
crashes recoverable; that is a bonus, not a requirement for the race.

**Identity table.** `native_thread_id → ThreadId` plus kind and parent, durable. Rows are inserted
from bootstrap, from `thread/start` responses, from sub-agent link items, and from *unknown native
ids seen in the journal*, which are inserted as `Orphan` with a fresh, final `ThreadId`. Insertion
is idempotent and keyed by native id, so whichever source runs first wins and every later source is a
no-op or a classification. This is precisely #199's decision "assign one final Giskard ID once";
the difference is that nothing waits on it. The parent-link-versus-child-frame race disappears as a
concept, not as a handled case.

**Projector.** One task per project that reads the journal from a cursor and applies each event to
per-thread state (runtime, live buffer, persistence, hub). The native-to-Giskard lookup happens at
projection time; an unknown id is inserted as an orphan right there, before the event is applied.
Per-thread state is plain data owned by the projector task, so there are no per-thread locks,
generations or drains. Exactly-once persistence comes from committing the cursor with the turn: the
history index record carries the journal sequence it represents. A restart re-reads from the last
committed cursor; re-application is idempotent because item ids are native-keyed and the forwarder
already upserts.

**Turn boundaries.** Derived from the journal: a native turn is active between its `TurnStarted`
(or first turn-scoped event) and `TurnCompleted`. A user-initiated turn is an intent record written
by the HTTP handler; its correlation with the native turn is the `turn/start` response, which is in
the journal in order. The "prepared operation waiting for the next `TurnStarted`" pattern and its
tokens go away.

**Browser delivery.** Unchanged in shape. The projector feeds the hub. The reconciliation plan's M9
journal and watermark for the browser is the same primitive one layer up; the two can share code.

### 6.3 Why this is sound rather than merely different

Each #199 correctness invariant holds by construction instead of by protocol:

- *Exactly one event owner per live thread*: there is one projector per project and it is the only
  reader that applies. Ownership is a property of the task, not a negotiated lifecycle.
- *Every turn routes through the owner*: everything is in the journal; there is no second path.
- *A native turn is persisted at most once*: cursor commit is atomic with the index write.
- *Terminal parent evidence never stops a later native turn*: parent items only ever insert or
  classify identity rows; they never touch child turn state.
- *Unexpected owner exit does not leave a thread broadcasting to nobody*: there is no broadcast.
  A crashed projector restarts at its cursor.
- *Replacement, unload, deletion, shutdown terminate the old generation explicitly*: there is one
  projector per process; deletion is a tombstone row in the identity table that the projector
  checks before applying.

And the residual loss modes #199 explicitly deferred ("when the bounded broadcast receiver reports
`Lagged`, when the owner crashes, or while an owner is being recreated") are the ones the journal
removes outright.

### 6.4 Costs, honestly

- **Memory and disk.** The journal holds events until consumed. The projector is in-process and
  the slow step is persistence I/O, so the backlog is bounded by disk write speed, which is the same
  bound the current design has once a turn completes. Spill to disk is the cap; if the disk fails,
  the project goes persistence-blocked, as today. This is a better bound than pausing stdout, which
  the plan admits only relocates the backlog into Codex.
- **Serialization per project.** One projector serializes all threads of a project. At the spec's
  target scale (§1.4, about ten threads) and given that one stdout reader already serializes them,
  this is not a regression. If it ever matters, the projector can shard by native thread id under
  the same journal, because cursors are per reader.
- **Replay on restart.** If the segment is durable, a restart replays the uncommitted tail. That is
  new code, but it replaces the interrupted-turn synthesis that exists today.
- **`codex-codes::AsyncClient`.** Its `request` reads stdout and buffers notifications without
  bound. The journal design needs the single-reader transport just as #205 and #218 did. That work
  is independent and small compared with either.

### 6.5 Incremental path

Each step deletes something and is shippable alone.

1. **Retained log behind the existing API (adapter only).** Replace `SenderMap` + broadcast with a
   per-native-thread retained log inside `CodexInstance`, and make `subscribe` return a replaying
   reader (mpsc fed from a position). Closes windows B, C and D without touching the server, and
   removes the `Lagged` arm and the interrupted-prefix truncation. Small.
2. **Route at ingest by minting a final id.** When the mapper meets an unknown non-empty native id,
   mint a `ThreadId`, record it as an orphan in the adapter's table, and emit a
   `ThreadDiscovered { native, thread, parent_native }` event *in the same stream and in order*
   with the child's own events. The server creates the orphan thread file idempotently when it
   projects that event; parent links become classify-or-create. Closes window A. This is what #219
   does, minus receiver custody: the events are held by the log, so there is nothing to hand over,
   and no ticket, attachment, claim key or tombstone custody is needed.
3. **One projector per project.** Replace per-thread forwarder tasks with a single task that reads
   the project stream and dispatches to per-thread state it owns. Delete `ThreadCoordinator`
   tokens and generations, `OwnerLock` and the drain loop, `install_event_owner`, the per-parent
   materialization queue, and the project lifecycle lock on the event path. Route handlers send
   intents into the projector instead of preparing operations under a coordinator lock.
4. **Cursor-committed persistence.** Record the journal sequence in the history index and commit
   the cursor with the turn. Remove the polling gate in `should_poll_codex_messages` (the reader
   reads always; the journal absorbs). Optionally make the segment durable for crash recovery.

### 6.6 Acceptance, in the spirit of #199

The refactor succeeds only if these disappear:

- `tokio::sync::broadcast` in `giskard-harness-codex` and `giskard-server/src/registry`;
- `SenderMap` and the synchronous `AgentHarness::subscribe`;
- `CoordinatorToken`, owner generations, `OwnerPhase::Draining`, `lock_thread_owner_after_drain`;
- [x] `enqueue_subagent_materialization` and its per-parent queue;
- [x] `lock_project_lifecycle` anywhere on the path from stdout to persistence;
- `claim_native_thread` as a control-queue round trip triggered by an event;
- the `"dropping Codex notification for unknown native thread"` log line for non-empty ids;
- the `Lagged` handling and "persist prefix as Interrupted".

And the test suite should shrink: with ordering no longer an invariant, the tests are about
idempotence (shuffle the relative order of parent link and child frames, inject persistence delays,
assert every child event lands in exactly one persisted turn) rather than about schedules.

## 7. Is the whole Giskard architecture wrong?

No. Most of Giskard is fine and unaffected: the bounded index plus per-turn payload storage, the
metadata service with revisions, the thread graph validation, the hub and per-client delivery
lanes, the read-only sub-agent policy, `HarnessBootstrap`, `Orphan`, `TurnModel::Unknown`, the
`AgentHarness` trait as an idea. The layer that is wrong is one layer: the event transport and
ownership model between the adapter and the server. It is load-bearing, and the fixes so far have
kept its premises (push, receiver-gated, identity-before-storage, task-per-thread ownership) and
added machinery on top. Replace the premises and most of the machinery deletes itself.

Three rules in `AGENTS.md` currently entrench the premises and should be rewritten alongside the
change: "`SenderMap` remains shared only because synchronous `AgentHarness::subscribe` must read
it"; "every production Codex thread route must be established through the `CodexInstance` route
methods; do not claim … and publish its event sender as separate operations"; and "never inferred
from traffic". The goal behind the last one, never two Giskard ids for one native thread, is right.
The consequence chosen, dropping the frame, is what makes the system lossy. The right consequence
is to record under the native key and label later.
