# Thread State, Runtime, and Bootstrap Reconciliation Plan

## Status

Two landings are complete on `main`.

**Persisted metadata** (`ThreadMetadataService`, durable thread revisions, typed projections,
explicit recency, revision-aware catalog reconciliation, non-blocking replacement delivery) is
implemented as described below. Metadata publication neither waits for socket capacity nor consumes
ordered-event queue capacity, and a no-op action still receives an authoritative direct result.

**Storage layout** (`6907fd0`) split thread history into a bounded `history.jsonl` index and
per-turn payload files written atomically, with versioned headers on both. That landing was not part
of the original plan; it was inserted because the bootstrap and retention work below kept running
into the same root cause — a turn's record was both its index entry and its unbounded payload. See
*What the storage layout change unlocks* for the parts of this plan it simplifies or retires.

**Everything else in this document is not implemented.** The runtime registry, event journal,
transactional bootstrap, class-aware outbox, request claim/commit, and turn-less context restoration
remain to be built, in the milestones defined under *Implementation milestones*.

This plan began with Codex restoring a context window outside a turn. The codebase audit showed
that the context window is not a special state class. It exposed missing general primitives for
persisted thread metadata, in-flight runtime state, reconnect snapshots, request resolution, and
bounded client delivery.

The target is not another field-specific fix. The target is a small set of authorities with explicit
clocks and ownership rules.

### How to use this document

Each milestone below states its scope, its explicit non-goals, and its exit criteria. **The
milestone boundaries are constraints on the implementation, not suggestions.** Work that belongs to
a later milestone does not become in-scope because it is convenient to write at the same time, and
this document is not the place to authorize it — if a milestone appears to require something listed
as a non-goal, stop and raise it as a question rather than editing the scope.

This matters because it has already gone wrong once: an earlier attempt at the runtime work
absorbed the retention, pagination and amendment milestones as well, grew to +13,000 lines, and
edited the plan in the same commit to accommodate them. A reviewer cannot evaluate a change that
redefines its own scope.

The milestones below are deliberately small for the same reason. Fewer, larger landings are not a
saving — they are how the last attempt became unreviewable.

## Decisions

1. Persisted browser-visible thread metadata is published as one typed, revisioned snapshot.
2. A server-layer metadata service owns metadata mutation, projection, and publication. Persistence
   remains unaware of WebSockets.
3. One `ThreadRuntimeRegistry` owns per-thread runtime entries for the active-turn gate, reconnect
   state, tasks, requests, and recent event journal. One agent event is applied once.
4. Subscribe returns one explicit `ThreadBootstrap` transaction. The browser does not infer a
   bootstrap from the order of several unrelated messages.
5. Ordered events use a shared, bounded per-thread journal and a server-side snapshot watermark.
   There is no unbounded FIFO per subscriber.
6. Connection delivery is class-aware: ordered streams, coalescible replacement state, barriers,
   direct responses, and ephemeral signals do not share accidental drop semantics.
7. Running-task snapshots own the Tasks menu and controls only. History, live snapshots, and events
   exclusively own transcript rows.
8. Cross-thread runtime state is an authoritative replacement snapshot that can represent several
   simultaneous requests. Notifications are side effects of state transitions, not state storage.
9. Project thread lists remain authoritative over HTTP. Their rows carry the same persisted thread
   revision as metadata. Changes send one coalescible catalog-dirty signal; clients serialize and
   repeat stale or raced per-project refetches.
10. Request resolution is a claim/commit operation and the browser keeps the authoritative status
    by request ID. Message arrival order cannot resurrect an answered request.

## What the storage layout change unlocks

`6907fd0` changed the facts several parts of this plan were written against. The simplifications
below are available now and should be taken rather than reimplemented around.

### A bounded read is now possible, so the bootstrap can be bounded by construction

A `TurnRecord` in `history.jsonl` carries `usage`, `model`, `mode`, `status.kind`, timestamps,
`item_count`, a capped `prompt_preview` and attachment descriptors — everything a turn *summary*
needs, with no payload read. Payloads live in individually addressable per-turn files.

Consequences for Primitive 3:

- The bootstrap can send bounded turn records for the history page and fetch payloads separately,
  instead of embedding whole turns and then discovering the result does not fit a frame.
- **Byte chunking with base64 was accepted only because a single turn record could be arbitrarily
  large.** That is no longer structurally true for the index. Re-evaluate the encoding once the
  bootstrap uses bounded records: chunking the two array sections by element removes the base64
  inflation, the reassembly buffer, and the decode step. Do not treat the current byte-chunking
  decision as settled.
- An oversized single turn is now one file, retrievable over the HTTP pagination lane, where no
  frame limit applies.

The store does not expose these reads yet. Adding `load_turn_records`, `load_history_snapshot` and
`load_history_from` is in scope for the milestone whose bootstrap consumes them — the storage plan
deferred them explicitly until a consumer existed, and that consumer is M5.

### One of the two full-history reads per bootstrap is already gone

`recompute_aggregates` is index-only: the ledger folds `usage`, `model` and `status.kind` from turn
records without opening a payload. The remaining full read is the live-turn expansion path, which
needs only a turn's *position* and should use a bounded index scan rather than `load_all_turns`.

### The truncation primitive already exists

`giskard-persist::preview::bounded_preview(text, max_bytes) -> (String, bool)` is UTF-8-safe and
already has two callers (`prompt_preview`, `status.message`). The retention work must reuse it
rather than growing a second implementation. What retention adds is *policies* — distinct named
limits for durable command output, reconnect preview, task tail, and per-frame admission — not a new
truncation routine.

### Amendments have a home in the format, and no home in the code yet

Payload records already carry an explicit `index`, payload files are tagged and versioned per file,
and the fold rules for collections and singletons are stated in `parse_turn_payload`. A late command
completion can therefore be appended to its turn's payload file without a format bump.

Nothing implements this. It is M7, and it is a durable-format behaviour change that
deserves its own review: an amendment needs a durable clock the browser can compare against, a
persistence-recovery path when the amendment write fails, and a reconnect rule. It must not be
absorbed into an earlier milestone.

### Per-turn containment changes what "unreadable" means

`assemble_turns` drops a damaged turn and keeps the rest of the thread. The transcript consequence
is a silent gap: the operator gets an `error!`, the user gets nothing. Making the omission visible
needs a placeholder turn, which is a `giskard-core` change and is listed under adjacent findings.

## Design rationale and remaining gaps

### Context restoration belongs to the metadata authority

Title, mode, model, and permission changes already persist a `ThreadFile` and publish a refreshed
thread snapshot. Context restoration needed a harness-to-server update channel, but it did not need
a field-specific WebSocket message or browser watermark.

Keep the harness-neutral `ThreadUpdateSink`, per-model context-window persistence, and the lifecycle
generation guard — all of which exist only on the abandoned branch and must be ported, not found in
`main`. `ThreadContextWindowUpdated` was never merged, so M3 adds nothing in its place: restoration
publishes through the general metadata path.
Also keep the Codex mapper's distinction between active-turn usage and turn-less resume metadata,
the bounded resume replay lifetime, and centralized harness-open update forwarding. Those solve the
real provenance/lifecycle gap and are independent of browser delivery ordering.

Use one authoritative invalidation policy for delayed restoration. The registry generation/commit
guard is required because it covers adapter replay, the server retry window, external/passive turn
starts, compaction, and deletion. The adapter's separate cancellation on accepted turn start or
manual compaction is only an early-exit optimization: a late update is still rejected by the server
guard. Remove those duplicate cancellation calls and keep the adapter TTL as a resource bound, so
two lifecycle policies cannot drift.

### A subscription FIFO is not an ordering primitive

*Historical rationale. The per-subscriber FIFO described here was built on an abandoned branch and
never merged: `subscribe_buffered`, `finish_subscribe` and `broadcast_reliably` do not exist on
`main`. It is recorded because it is why the journal and watermark exist, not as a defect to find in
the code.*

The event forwarder appends ordinary in-flight events to `LiveBufferStore` before broadcasting
them. An event emitted during bootstrap can therefore be represented in both the live snapshot and
the subscription FIFO. The FIFO is flushed after the snapshot, so a non-idempotent `ItemDelta`
appends its text or command output twice.

The old live-before-snapshot behavior let `reconcileInFlightTurn` remove and rebuild early rows.
Reordering those same events after the snapshot removes that protection. A future transactional
bootstrap therefore needs a snapshot watermark and bounded journal, not a per-subscriber FIFO.

Transport pressure must not block persistence or harness event consumption. Losing an ordered
suffix must cause a thread resync, not silent divergence or a socket-wide bootstrap failure.

### Runtime bootstrap still infers a transaction from message order

Bootstrap behavior is spread across `ThreadState`, `HistoryPage` or `HistoryDelta`, an optional
`LiveTurnSnapshot`, and `RunningTasks`. The browser coordinates them with
`awaitingInitialThreadState`, `awaitingThreadResync`, `awaitingIncrementalResync`,
`pendingLiveSnapshotReconcile`, and message-type-specific completion rules.

This is an implicit protocol state machine. One aggregate bootstrap message makes the boundary and
its fallback mode explicit.

### Runtime projections still overlap

`RunningTasks` is described by the specification as a Tasks-menu snapshot, but its browser handler
also creates transcript rows and merges command output. A task revision orders task snapshots only;
it cannot deduplicate output also present in an event or live snapshot.

`ThreadActivity` stores one discriminated record per thread in the browser. A second approval or
server request overwrites the first, and resolving the represented request can clear the waiting
indicator while another request is still pending.

These are ownership problems, not missing watermarks.

## Authorities and clocks

No clock in this table orders a different row.

| State | Authority | Clock | Client projection |
| --- | --- | --- | --- |
| Persisted metadata | metadata service | thread revision | `ThreadMetadata` |
| Thread catalog | persisted files | thread revision | invalidate/refetch |
| Completed transcript | history JSONL | ordered `TurnId` | bootstrap/page |
| Active transcript | runtime registry | event sequence | snapshot plus suffix |
| Active-turn ownership | runtime registry | transition order | runtime snapshot |
| Running tasks | runtime registry | task revision | `RunningTasks` |
| Requests | runtime registry | request/runtime state | runtime snapshot |
| Runtime overview | runtime registry | overview revision | replacement overview |
| Direct action result | action handler | domain identity | response/error |
| Background warning | notice authority | notice identity | live/bootstrap notice |

The internal event sequence may reset when the server restarts. It is a cut within one process, not
a durable client cursor. Completed `TurnId`s remain the reconnect cursor across restarts.

## Primitive 1: `ThreadMetadataService`

Create a server-layer service from `PersistStore` and the publication interface. Store it in
`RegistryShared` and expose narrow registry methods to routes. Do not make the service depend on
`HarnessRegistry`; that would create an ownership cycle.

The service owns two core operations:

- mutate an existing thread under the store's per-thread lock;
- derive and publish browser projections from the committed result.

Creation/deletion orchestration may ask it to publish a committed projection or catalog
invalidation, but the service does not own those cross-system sagas.

### Persistence mutation outcome

Change `PersistStore::update_thread` to return a named outcome:

```text
Missing
Unchanged { current }
Changed { before, after }
```

Durable equality excludes `revision`. The store applies the closure, applies the explicit recency
policy, compares the candidate with the original, and only then allocates the next revision and
writes. A no-op returns the authoritative current record without an fsync or revision bump.

Use domain equality, not serialized JSON equality. The store owns the revision and uses checked
increment capped at JavaScript's maximum safe integer, because the paired client receives JSON
numbers; a closure cannot supply or preserve an arbitrary revision.

Add the persisted revision with a backward-compatible default for existing thread files. The first
actual mutation of a legacy record allocates its next revision under the same per-thread lock. A
revision is scoped to one thread and orders only its metadata snapshots.

`recompute_aggregates` then remains a crash-repair operation. It is a no-op write when both the
ledger and durable activity recency already match. Matching tokens alone may still require one
write to restore recency from the latest persisted turn; repair never substitutes its current time.

### Typed browser projection

Replace opaque `serde_json::Value` publication of the whole `ThreadFile` with a wire type containing
only audited browser fields:

```text
ThreadMetadata {
    thread_id,
    revision,
    title,
    mode,
    current_model,
    context_window,
    permission_preset,
    tokens,
}
```

Do not expose native harness IDs, model-window caches for unselected models, effort caches,
ownership internals, or Git-worktree records merely because they share a persistence file.

The service derives two revision-excluded projections from `before` and `after`:

- detail projection: fields in `ThreadMetadata`;
- catalog projection: fields returned by `GET /api/projects/{id}/threads`.

If detail changed, enqueue a revisioned `ThreadState`. If catalog projection changed, enqueue a
coalescible `ThreadCatalogChanged`. An internal-only mutation publishes neither.
This automatically handles a selected context-window change while keeping a non-selected model
cache write silent.

The browser stores the last applied metadata revision for the current subscription and ignores a
lower live `ThreadState`. A committed bootstrap resets that baseline from its included
metadata, so a server restart or thread switch cannot inherit an unrelated client watermark.

Live `ThreadState` never carries turn runtime state. That state has a different authority and
appears in bootstrap/runtime projections only.

### Catalog invalidation instead of another replicated cache

Do not add thread-summary upserts and deletion tombstones unless measurement proves the HTTP refetch
too expensive. One global catalog-dirty signal is bounded and has fewer ordering rules:

- include the persisted thread `revision` in every `ThreadSummary` row;
- coalesce all pending thread-catalog changes into one signal;
- after an invalidation, refetch every known project's authoritative thread list;
- permit only one refetch per project at a time;
- if another invalidation arrives during the request, run it again after the request completes;
- never let a row with a lower revision overwrite fields already supplied by newer metadata;
- when a response row is below a revision already applied for that thread, preserve the newer
  shared fields and mark the project dirty for another serialized refetch;
- refetch known project lists on WebSocket reconnect to repair changes made while disconnected.

The browser tracks detail and catalog application separately so an equal-revision catalog row can
still fill catalog-only fields and an equal-revision detail snapshot can fill detail-only fields.
The shared revision is the cross-transport staleness test, not a claim that the two projections have
the same fields.

Use it for committed thread-catalog changes such as title, archive state, intentional recency,
background child materialization, creation, and deletion. Existing lifecycle orchestration emits
the invalidation only after the corresponding durable step; this plan does not change the
atomicity or rollback semantics of multi-system create/archive/delete operations. Project-list
invalidation may be added separately when project lifecycle work needs it.

### Explicit recency policy

`updated_at` controls sidebar ordering and must not be a side effect of whatever closure happened to
run. Preserve recency is the default. Service operations, not arbitrary call sites, assign one of
these intents:

- preserve recency;
- touch recency only when another durable field changed;
- record user-visible activity even when no other detail field changed.

The fixed rule is:

- a successful turn completion records user-visible activity even when no other detail changes;
- an explicit user mutation touches recency only when it changes a durable user-visible field;
- ordinary background, cache, import, and normalization mutations preserve recency;
- crash aggregate repair may advance recency only to the latest persisted turn timestamp and never
  to the repair time.

Therefore native harness-ID repair, runtime context restoration, model-cache repair, background
normalization, and sub-agent title refresh all preserve the existing order. Future operations use
the default unless they fit one of the two named exceptions above.

Consolidate route and registry sub-agent title refresh into one conditional metadata operation. The
inner locked recheck decides whether the title changed, so a lost race cannot update `updated_at` or
bump revision as a no-op.

## Primitive 2: `ThreadRuntimeRegistry`

Merge the state which is currently split across `ThreadTurnGate`, `LiveBufferStore`,
`RunningTaskStore`, approval/server-request routing maps, and several event-forwarder locals that
exist only to update those stores. The registry owns the thread-entry map, global overview revision,
and cleanup; callers do not coordinate several public stores.

Use a per-thread state object behind a short-lived lock. It owns:

- reserved/acknowledged active-turn state and the turn lease;
- the normalized in-flight turn used for reconnect;
- the highest event sequence represented by that snapshot;
- running tasks and their revision;
- outstanding approval and server-request records;
- resolved request records needed by reconnect;
- the bounded recent client-visible event journal;
- the thread's cross-thread runtime summary.

The forwarder still owns persistence assembly for the completed `Turn`. Native persistence data and
browser runtime projection do not need to be the same type.

### Apply an event once

Replace the current sequence of independent store calls with one operation that returns effects:

```text
AppliedRuntimeEvent {
    stream_event,
    running_tasks_if_changed,
    overview_if_changed,
    internal_side_effects,
}
```

The operation:

1. allocates the per-thread event sequence;
2. updates the reconnect projection and represented-through watermark;
3. updates task state;
4. updates outstanding requests and runtime summary;
5. appends only a browser-visible event to the recent journal;
6. returns immutable publication effects after releasing the runtime lock.

`ContextWindowUpdated` is consumed by metadata persistence and is not a transcript event.
`ThreadOpened` and `DiffUpdated` currently reach the wire without a browser handler; keep them
internal unless an audited UI requirement is added. They must not consume journal or queue capacity.

Synthetic prompt events, fallback transcript events, turnless errors/notices, late command events,
and ordinary forwarder events all use this publication boundary. No bypass may broadcast an event
without receiving a sequence and delivery classification.

### Turn completion handoff

Turn completion is a state transition, not `persist; clear whatever is in memory`.

1. Keep the complete pre-terminal runtime projection and turn lease while append is pending.
2. Append the completed `Turn` to authoritative history.
3. If append succeeds, atomically allocate the completion sequence, commit terminal runtime state,
   append the completion journal entry with coverage `Turn(turn_id)`, and release the turn lease.
4. If append fails, atomically retain a recoverable terminal state and publish an actionable error,
   but do not release the lease or clear the only complete representation. Before retrying, reload
   history by `TurnId`: an error after a successful append must be settled as success, not appended
   twice.
5. Retry a confirmed-missing turn with named attempt, elapsed-time, and backoff bounds. When the
   budget is exhausted, enter `PersistenceBlocked { turn_id, attempts, error }`. **Scope note:** the
   browser-facing half of this step — a blocked composer with explicit recovery actions and the two
   client messages behind them — is a user-facing feature inside an otherwise internal milestone.
   Decide explicitly whether M2 ships it, or stops at "hold the lease and surface the error" with
   the recovery actions as their own small landing. The browser keeps
   the composer blocked and offers explicit `Retry persistence` and destructive
   `Discard unpersisted turn` actions. Retry preserves the representation and lease; confirmed
   discard logs the loss, clears runtime state, and releases the lease. A disk or permission fault
   cannot wedge the thread without a recovery action.
6. Metadata-cache failure after a successful history append does not make the turn disappear;
   aggregate repair remains possible.

The client does not observe `TurnCompleted` before the history append succeeds. Late terminal task
events have coverage `None`, because the current history does not contain that later state. They
are never suppressed merely because their original turn is present in a returned history view. A
history snapshot may suppress an event only when its explicit coverage token is present in the
exact returned view.

Durable reconciliation of late terminal task events is an adjacent existing defect, not part of
this implementation. Fixing it requires a durable amendment cursor or revision plus reconnect and
browser merge semantics; an append-only sidecar by itself would leave incremental reconnects
stale. Record that work separately rather than silently changing the completed-history contract in
this branch.

No client delivery is awaited while holding the turn lease, lifecycle commit lock, runtime lock, or
store lock.

### Request state semantics

Pending requests are state, not just events. Store the request payload and routing identity even
when it arrives before normal turn ownership; the current live-buffer-derived attention bootstrap
cannot reconstruct such a request.

Each runtime entry owns one authoritative projection keyed by request ID:

```text
RequestState {
    request_id,
    kind,
    payload,
    status: Pending | Responding | Resolved { decision? },
}
```

Resolution uses an atomic claim token:

```text
Pending -> Responding -> Resolved
                    \-> Pending on harness failure
```

Do not remove an approval before the harness accepts it. This also prevents two tabs from routing
the same decision concurrently. Server requests use the same state machine.

The browser marks a request as responding after send and waits for the authoritative resolution.
Its one request-state map retains a `Resolved` record even if the transcript card has not rendered;
a later chronology event binds to that state and cannot make the card actionable again. Do not add
a second client tombstone collection. A generalized `RequestResolved` message covers approval
decisions and server-request answers; the latter must not depend on a harness-generated resolved
event which may be late or absent.

Every server-side status transition also emits an ordered request-state event: a successful claim
publishes `Responding`, harness rejection republishes `Pending` and returns a direct error to the
claimant, and commit publishes `RequestResolved`. This keeps other tabs aligned without making an
optimistic local state authoritative.

`ThreadBootstrap.final_runtime.requests` contains every pending, responding, and resolved record
needed to reconstruct the active or recoverable runtime turn. Ordered request events in the suffix
provide transcript chronology; after replay, the final request projection alone controls whether a
card is actionable. `RequestResolved` is an ordered journal event, so losing it triggers the same
thread resync as losing any other ordered event. Resolved records remain until their runtime turn
is durably settled. For a request outside normal turn ownership, retain it until its runtime epoch
ends or the runtime entry becomes idle. In either case, no bootstrap pin may still reference the
record when it is removed.

The cross-thread overview derives only pending/responding request IDs from this projection. Add
`thread_id` to approval and server-request client responses so routing can validate and claim the
request directly in the registry's thread entry instead of consulting separate global
request-to-thread maps.

## Primitive 3: shared event journal and explicit bootstrap

### Why a shared journal

A shared per-thread journal retains one recent ordered suffix regardless of subscriber count. A
per-client bootstrap FIFO clones the same burst once per tab and still needs an overflow state
machine. The journal also provides the suffix needed for both initial subscribe and slow-client
resync.

Bound it by serialized bytes and entry count. Store pre-serialized or accurately sized immutable
wire events so the byte bound is real. Eviction advances the oldest available sequence. The active
turn snapshot remains the authoritative compact representation of events it covers.

The normalized live snapshot should coalesce text/reasoning deltas by item and compact command
output while advancing `represented_through`. This bounds event count by meaningful runtime state,
not by chunk count. Apply a per-turn and per-item byte ceiling to reconnect-only accumulated text,
keeping bounded head and tail content with an explicit omission marker. The live delivery path and
the eventual completed item remain authoritative and untruncated. If the event stream fails before
the harness supplies a completed item, persist or surface the same marked recovery representation
rather than silently claiming complete output. This gives runtime memory a real bound without a
second temporary persistence format.

### Wire transaction

The server assigns an internal generation to each accepted subscribe:

```text
Subscribe { thread_id, since? }

ThreadBootstrap {
    thread_id,
    subscription_generation,
    metadata: ThreadMetadata,
    history: FullPage { turns, has_more }
           | Delta { after, turns }
           | CursorReset { turns, has_more },
    live_turn?,
    ordered_suffix,
    final_runtime: {
        turn_state: Idle | Active | PersistenceBlocked { turn_id, attempts, error },
        running_tasks: { revision, tasks },
        requests: [RequestState],
    },
    notices,
}
```

The connection allocates this generation monotonically for each thread. `BootstrapStart`, chunks,
commit, live events, and resync controls carry it. A newer subscribe cancels the prior generation;
the browser discards any frame which does not match the latest started generation. WebSocket FIFO
plus this server-owned generation is sufficient—there is no client-generated subscription token.

This is one logical transaction, not one WebSocket frame. Encode it physically as
`BootstrapStart`, one or more bounded `BootstrapChunk` messages, and `BootstrapCommit`. Chunks carry
transaction and section identities and have a hard encoded-byte limit. The browser stages a
transaction and changes authoritative UI state only after validating its commit. It then applies
metadata/history/live state, replays `ordered_suffix`, and applies `final_runtime` last. Older
suffix events therefore cannot regress the final active/request/task state. Only the committed
transaction resets bootstrap state or releases an optimistic first-turn lock from an explicit
`turn_state = Idle`. Live `ThreadState` changes metadata only.

Always use start/chunk/commit, including when the payload fits in one chunk. There is one browser
staging and apply path, not a small-frame fast path. The transaction generation and commit
provide atomicity; chunk encoding only provides the size bound. The target protocol does not make chunking conditional.

The per-client bootstrap task may await capacity while emitting chunks because it is not a store,
harness, or event-forwarder producer. It reserves a transaction/barrier slot but does not place the
whole encoded transaction in the connection outbox. The initial history page has a byte as well as
turn-count budget. Individual history records larger than a frame are split across chunks. **Re-evaluate this against
the storage split before implementing:** a bootstrap history record can now be a bounded
`TurnRecord` with its payload fetched separately, which is the constraint that forced byte chunking
in the first place. See *What the storage layout change unlocks*. The
pinned ordered suffix has separate entry and byte limits and counts against both runtime-journal
and bootstrap memory until commit or cancellation.

Live request payloads, task-output projections, notices, metadata strings, and reconnect-only
accumulations each need documented size limits or bounded compact forms. An individual live event
which cannot fit the maximum ordered-event size does not enter the outbox: mark that subscription
`NeedsResync`. The next bootstrap obtains the content from chunked history or a bounded runtime
projection. Never truncate silently; an omission marker or lazy full-content retrieval must make
the boundary visible.

**Superseded.** This plan previously kept `HistoryPage` on the WebSocket for pagination. M1 moves
older-history pagination to authenticated HTTP and removes `LoadHistory` and `HistoryPage` from the
protocol: pagination is a request/response with no ordering relationship to live state, so it does
not belong in the ordered lane where it competes for outbox capacity and needs a generation. What
survives from the original reasoning is the constraint that a page must never carry a second
bootstrap meaning inferred from `pendingOlder`.

### Bootstrap algorithm

For one subscription generation:

1. Register a new `Bootstrapping` subscription generation before snapshot work. It does not clone
   ordered events; the shared journal covers those. It retains only bounded, coalesced replacement
   state and invalidations which arrive before the bootstrap barrier.
2. Under the runtime lock, capture the immutable live snapshot/watermark pair and install a journal
   pin immediately after that watermark. Reserve the pin's maximum byte/entry budget at this cut;
   eviction cannot cross it. If reservation is unavailable or later events exhaust it, fail this
   bootstrap generation rather than retrying the same race window.
3. Read persisted history after the live capture.
4. If the captured turn is now persisted and included in the returned history, omit the live copy.
5. If it became persisted outside the configured initial page, expand the bootstrap coverage to
   include that turn and every later turn, or keep its complete live representation with correctly
   ordered suffix events. Never place an older live turn after newer persisted turns.
6. Load the latest revisioned metadata. A newer metadata publication which races this read is held
   as one coalesced post-barrier replacement by the bootstrapping subscription.
7. Under the per-thread runtime lock and then the subscription lock in one fixed lock order:
   atomically capture final turn/task/request state and final journal sequence, materialize
   the already-pinned suffix, install that sequence as the generation's represented-through
   watermark, and move the generation to its transaction-ready state. No turn reservation,
   runtime transition, or delayed publisher can fall through this handoff.
8. If the pin exhausted its reservation, leave the generation unsubscribed, release the pin, and
   send a retryable thread-scoped control. Otherwise reserve the transaction barrier and stream
   bounded history/runtime chunks through the dedicated bootstrap task rather than queueing the
   whole transaction at once.
9. Send the transaction commit marker. Events through the installed watermark are
    ignored when delayed publication effects arrive; later events queue behind the commit marker.
    Only after commit does the generation become live.

Drop a suffix event only when its sequence is covered by the immutable live snapshot/watermark pair,
or its `Turn(turn_id)` coverage token is present in the exact history view returned by this
bootstrap. A turn ID alone must not suppress a late command event, because that entry has no durable
coverage token. Preserve all other event order.

Reading live first and history last converts the ordinary completion race into an overlap that can
be identified by `TurnId`. The journal covers events after the live cut. This closes both the
missing-turn and duplicate-delta windows without holding a persistence lock during history I/O.

The initial live projection and its watermark remain one immutable pair. A new subscribe attempt
recaptures and re-pins both; it never combines a newer projection with an older watermark.
Bootstrap runs in a cancellable task keyed by subscription generation. A newer subscribe,
unsubscribe, or disconnect cancels it. A bootstrap failure removes that subscription generation,
releases its pin, and sends only the structured error; it must not leave live deltas flowing
without a baseline.

During bootstrap, derive aggregate repair from the same loaded history view when possible.
`recompute_aggregates` remains crash repair, but no-op mutation detection prevents a write or
revision change when metadata already matches. If repair changes visible metadata, publish it
through `ThreadMetadataService`; a bootstrapping generation coalesces a newer revision behind its
transaction barrier.

A committed bootstrap is an unconditional task baseline for its subscription and socket. The
browser resets its prior task revision before applying that baseline. Per-thread task revisions are
process-local and may restart from a lower value after server restart.

## Primitive 4: class-aware connection outbox

Replace ad hoc broadcast and warning-specific buffering with one connection-owned
delivery pump. Persistence, harness, and event-forwarder producers never await socket capacity; only
the dedicated per-client bootstrap encoder may wait for its chunk capacity.

| Delivery class | Admission and failure behavior |
| --- | --- |
| Ordered events | FIFO per subscription; overflow becomes `NeedsResync` |
| Revisioned replacement | Keep newest by key; evict obsolete entries first |
| Catalog invalidation | Keep one dirty key per catalog |
| Bootstrap transaction | Flow-controlled start, chunks, and commit |
| Barrier/control | Use reserved capacity and preserve prerequisites |
| Direct action response | Use control reserve through delivery/failure |
| Ephemeral signal | Evict first; never authoritative |

The outbox owns separate finite data capacity and reserved control capacity, each bounded by bytes
and entries. Replacement messages and invalidations coalesce in place. Admission first removes
obsolete replacement entries and ephemeral signals. It never evicts a direct response, required
barrier, or bootstrap chunk which has begun transmission merely to admit ordinary data.

Ordered-stream overflow clears that subscription's queued suffix, records one `NeedsResync`
transition, and schedules `ResyncRequired { thread_id, subscription_generation }` in control
capacity. It logs once with the lost sequence range and byte counts. While `NeedsResync`, reject
further incremental events for that subscription until a new subscription generation establishes
a baseline. The shared runtime journal continues independently and may satisfy that resubscribe.

Bootstrap chunks are admitted by awaiting ordinary outbox capacity in their dedicated per-client
task. The transaction start, chunks, and commit retain FIFO order; control priority must never let a
commit or later incremental event overtake required chunks. Cancellation discards all unsent chunks
for that transaction and releases its pinned suffix.

Direct action errors and resync controls consume the reserve when ordinary data is full. If only
non-evictable control/direct entries remain and the reserve is exhausted, reject the enqueue to its
caller, emit a structured fatal-backpressure log, attempt a bounded drain, and close the unhealthy
connection. No enqueue failure may be treated as successful delivery.

Direct responses return only to the originating connection and carry the operation's existing
domain identity: for example `thread_id`, `request_id`, or `process_id`, plus `action` where needed
for rollback. Do not add a generic `action_id` protocol to every command. Request decisions add
`thread_id` to their existing `request_id`, which is enough to claim them in the runtime registry.

The client resubscribes on the same socket. Other thread/global state and direct controls remain
usable. If the peer does not drain even prioritized control within a bounded timeout, closing that
unhealthy connection is appropriate; a normal bootstrap burst is not such a condition.

Writer completion cancels the receive side and unregisters the client. When the server intentionally
sends a final protocol error, allow a bounded drain before closing instead of immediately aborting
the writer.

## Observability contract

Use one field vocabulary across metadata, runtime, bootstrap, and delivery boundaries:

- identity: `project_id`, `thread_id`, `turn_id`, `request_id`, `client_id`;
- ordering: `subscription_generation`, `metadata_revision`, `task_revision`,
  `overview_revision`, `event_seq_start`, `event_seq_end`;
- pressure: `message_kind`, `entries`, `bytes`, `capacity_entries`, `capacity_bytes`,
  `pinned_entries`, `pinned_bytes`;
- operation: `action`, `outcome`, `attempt`, `elapsed_ms`, and the underlying `error`.

Successful high-frequency event application stays at trace/debug. Log bootstrap start/cut/commit,
cancellation, and duration; metadata commit/publication failures; journal pin exhaustion; the
single transition to `NeedsResync`; control-reserve exhaustion; writer termination; and every
`PersistenceBlocked`, retry, and discard transition. Warnings/errors include the ordering and
capacity fields needed to distinguish slow-client pressure from a server invariant violation. Do
not log transcript, request, or command payloads merely to provide context.

Focused tests capture the failure-boundary logs where practical, including log-once overflow,
bootstrap cancellation, and lost-turn discard. Module-specific field spellings are not allowed.

## Background notices

Move retained background warnings out of `Hub`; transport should not own domain lifetime. Keep a
small per-thread notice store keyed by stable notice kind and revision. A background failure inserts
or replaces its notice, live subscribers receive a replacement update, and `ThreadBootstrap`
includes current notices. A later successful recovery, thread deletion, or an explicit lifecycle
rule clears it. Enqueueing to one tab does not globally erase the warning before another tab can
observe it. The browser deduplicates a notice identity within a page session while a fresh reload
can surface an unresolved warning again.

Direct errors caused by one browser action do not enter this store. They remain direct responses to
the requesting connection and drive its optimistic-state rollback.

## Cross-thread runtime overview

Replace additive `ThreadActivity` and `ThreadActivityBootstrap` state with an always-sent, even when
empty, revisioned replacement snapshot:

```text
ThreadRuntimeOverview {
    revision,
    threads: [{
        thread_id,
        turn_state: Active | PersistenceBlocked,
        outstanding_requests: [{ kind, request_id }],
    }],
}
```

The `ThreadRuntimeRegistry` also owns cached per-thread summaries and the global overview revision.
A runtime transition gives the registry a new per-thread summary; under one short registry lock it
updates or removes that summary, increments the revision, and returns the complete immutable
replacement snapshot for publication. Per-thread entries never allocate global revisions.

Membership is exact: include a thread while it has an active/reserved or persistence-blocked turn,
or at least one pending/responding request, and remove it otherwise. Resolved requests do not keep
a thread in the overview. An empty snapshot clears stale badges. Transient progress and error hints
may remain ephemeral notifications, but they are not retained in this authoritative projection.
The connection outbox coalesces the overview, so a dropped intermediate signal cannot leave a
thread permanently running or waiting.

The browser derives sidebar rank and hidden-sub-agent hoisting from this state. It supports several
simultaneous requests rather than one union value. Browser notifications are emitted only for newly
observed request identities and retain the existing per-page-session dedup rule. They do not mutate
or replace authoritative runtime state.

The server sends the current overview immediately on every connection. A newer live overview may
arrive before the initial one; its revision wins. Reset the browser's overview revision for a new
WebSocket connection, so a server restart with a lower in-process counter is valid.

Inactive threads still participate in this global overview. An unsubscribed thread which starts,
progresses, or acquires a request updates the overview without sending its transcript stream to the
connection. If persisted metadata changes a catalog field, catalog invalidation repairs the list.
Thread token totals are not part of `ThreadSummary` today; an unsubscribed client receives their
authoritative value on its next thread bootstrap rather than through an accidental full-event
subscription.

## Running-task and transcript ownership

`RunningTasks { thread_id, revision, tasks }` updates only the Tasks menu, elapsed timers, and stop
controls. Event/history rendering alone creates and updates transcript rows. Split the browser data
structures; one mutable `runningCommands` map must not serve both projections.

The server publishes a task snapshot after every task-state mutation. Reverse delivery cannot
regress the menu because the task revision is allocated atomically with the mutation.

A task from a turn outside the loaded history may be present in the menu without a transcript row.
Selecting it should load/navigate to the owning turn or explain that the row is not loaded; the task
snapshot must not fabricate a second transcript representation.

## Protocol simplification

After the new primitives are active, remove these server-message variants:

- `ThreadContextWindowUpdated`;
- `TokenUpdate` and `TokenScope`;
- top-level `ApprovalRequest` (it has no production producer);
- `ApprovalResolved` after generalized `RequestResolved` exists;
- top-level bootstrap-only `HistoryDelta` and `LiveTurnSnapshot`;
- additive `ThreadActivityBootstrap` and authoritative use of `ThreadActivity`;
- turn runtime state from live `ThreadState`.

Keep wire approval/request payload types used inside events and bootstrap state.

The WebSocket protocol is an internal same-release contract between the browser assets embedded in
`giskard-server` and that server. Change both ends atomically. Do not add version negotiation,
legacy decoding, parked legacy sockets, or parallel bootstrap implementations. A page loaded from
a different server release is unsupported and must be reloaded. Invalid or unknown messages are
logged and close that connection; compatibility behavior is not part of the protocol.

Consequently:

- remove obsolete wire variants and browser handlers in the same change which adds their
  replacements;
- make new wire fields required unless absence has current semantic meaning; do not add
  `serde(default)`, aliases, feature detection, or zero-value fallbacks for an older browser;
- keep only one bootstrap builder and one browser apply path—no migration adapter between old and
  new message sequences;
- test the current paired shape and malformed input, not cross-version combinations.

This does not waive persisted-file migration. Existing thread/history files survive server
upgrades, so persistence defaults such as the initial metadata revision remain intentional and are
separate from client protocol compatibility.

## Lifecycle and memory cleanup

The new authorities need explicit retirement rules rather than process-lifetime maps:

- remove a runtime entry after it has no turn lease, live projection, running task,
  pending/responding request, subscriber, or journal pin, and its idle grace period expires;
- evict unpinned journal entries continuously by byte/count bounds, and discard the journal with an
  idle runtime entry or deleted thread;
- retain resolved request records until the owning turn is durable or a turnless runtime epoch
  ends, and until no bootstrap pin can reference them;
- clear completed task entries in the same atomic runtime transition that publishes the new task
  baseline; discard task state with the runtime entry;
- clear notices on successful recovery, explicit acknowledgement policy, thread deletion, or
  project deletion;
- remove subscription generations and pinned suffixes on commit, superseding subscribe,
  unsubscribe, writer failure, or disconnect;
- remove overview membership before retiring a runtime entry, publishing the resulting replacement
  overview even when it is empty;
- remove all thread-scoped metadata publication state, revisions cached in memory, notices,
  runtime state, and subscriptions after durable thread deletion; project deletion applies the
  same cleanup to each known thread;
- treat a server restart as loss of every process-local event/task/overview revision. Persisted
  metadata revisions and completed turn IDs survive; a new connection/bootstrap establishes all
  process-local baselines.

Cleanup runs through named registry/service methods and is idempotent. It must not synthesize a new
state entry for a thread already forgotten while a delayed restore or bootstrap task is finishing.

## Adjacent findings and follow-up work

The audit found correctness issues which should not be hidden inside this already broad protocol
change:

- A command may finish after its interrupted turn was appended. The late event has no durable
  coverage, so reconnect after disconnection can show the command as running. **This is now
  M7**: the per-turn payload format admits the amendment without a format bump, and what
  remains is the durable clock, the recovery path, and the reconnect rule.
- A turn whose payload is unreadable is dropped from the returned history with an `error!` log and
  no user-visible marker. Making the omission visible needs a placeholder turn, which is a
  `giskard-core` change. Track separately.
- Thread/project creation, archive, and cascade deletion span persistence, native harness state,
  worktrees, and client catalogs. The catalog invalidation lane can publish committed outcomes, but
  it does not make those operations transactional. Separately audit collision, partial deletion,
  and native-success/metadata-failure recovery as lifecycle sagas.
- Project catalog replacement/invalidation can reuse the connection outbox's revisioned
  replacement class when project lifecycle behavior is revisited; it is not required to order
  thread metadata snapshots.

These findings receive regression issues/tests in their own changes. They are not exit criteria for
the metadata/runtime/bootstrap implementation.

## Refactor shape

Target server modules:

- `thread_metadata.rs`: typed projections, mutation outcomes, recency, catalog invalidation;
- `thread_runtime.rs`: registry, turn lease, live projection, tasks, requests, journal, overview;
- `thread_bootstrap.rs`: history/live cut and aggregate bootstrap builder;
- `delivery.rs`: connection hub, subscription generations, bounded class-aware outbox.

`registry.rs` remains harness/process orchestration. It should no longer contain field-specific
metadata publication, raw client-delivery policy, or three independent applications of every agent
event. `AppState` should not expose raw live-buffer/task stores once routes can use narrow registry
and bootstrap interfaces.

Use structural impact analysis before moving the turn gate, live buffer, running tasks, or request
maps. The current `LiveBufferStore::item_events` also serves trusted sub-agent-link resolution and
must remain available through the runtime registry's native/internal view.

## Implementation milestones

Each milestone is one landing, reviewable on its own. Scope and non-goals are binding: see
*How to use this document*.

**Dependencies.** M1 depends on nothing. M3 and M4 need M2's runtime registry — M3 for the
lifecycle state that replaces its own guard, M4 for the apply boundary it bounds. M6 needs M5's
bootstrap as its resync target. M7 needs only M5's journal coverage token. Anything not listed here
is ordering preference, not a constraint.

**New behaviour lands after the primitive it depends on, never beside it.** M3 is the worked example:
it is the bug that started this plan, and it still waits for the runtime registry, because building
its staleness guard against the forwarder M2 deletes would mean building it twice. Nothing here is
urgent enough to be worth implementing against a primitive that is about to be replaced.

Milestones are deliberately small. A milestone that looks like it will exceed roughly two thousand
lines of production change is a milestone that has absorbed its neighbour — stop and check the
non-goals.

---

### M1 — History pagination over HTTP

**The smallest independent landing, and purely subtractive.** Depends on nothing else here.

**Scope.** Serve older-history pages from an authenticated HTTP endpoint. Point the browser's
"load older" path at it. Remove `LoadHistory` and `HistoryPage` from the protocol. Cap the requested
page count; correlate or abort in-flight fetches when the active thread changes.

Pagination is a request/response with no ordering relationship to live state, so it does not belong
in the ordered lane, where it competes for outbox capacity and would need a subscription generation.
Moving it out also shrinks what M5 has to reason about.

**Non-goals.** The bootstrap transaction. Bounded reads — `load_history` already serves whole turns
and is sufficient here; the bounded reads land with the bootstrap that needs them.

**Exit criteria.** Two protocol variants are gone. Pagination cannot be confused with bootstrap
history. Switching threads mid-fetch cannot apply the previous thread's page.

**Transitional handoff to M5.** M1 moves only older-page pagination to HTTP. Until M5 replaces the
implicit bootstrap state machine, fresh subscriptions and stale-cursor recovery continue to carry
their bounded initial history as a bootstrap-only reset `HistoryDelta`; this is not a pagination
response. The server still obtains that history and the live snapshot through sequential reads, so
they do not form a transactional cut. M1 deliberately preserves that pre-existing limitation rather
than introducing a second independent HTTP/WebSocket race. M5 owns closing it with the runtime
snapshot, journal watermark, ordered suffix, and aggregate bootstrap commit described above.

---

### M2 — Runtime registry

**Scope.** `ThreadRuntimeRegistry` as the sole process-local authority for the active-turn gate,
the in-flight turn projection, running tasks, outstanding requests, and the cross-thread overview.
Every client-visible agent event goes through one apply boundary. Delete `LiveBufferStore` and
`RunningTaskStore`. Make task snapshots menu-only. Add request claim/commit with one authoritative
browser request map. Replace additive activity state with the replacement overview.

**Non-goals.** The event journal and bootstrap transaction (M5). Retention limits (M4). Durable
amendments and amendment-write recovery (M7). Changes to `giskard-persist` — **this milestone must
not touch that crate**; if it appears to need to, that is the signal to stop.

Recovery from a *failed turn append* is in scope here, because it is part of the turn-completion
handoff: hold the lease, keep the only complete representation, surface an actionable error. Whether
this milestone also ships the user-facing `Retry persistence` / `Discard unpersisted turn` actions
and their two client messages is an open decision — see the scope note in *Turn completion handoff*.

**Exit criteria.** One input event produces one sequence, one projection update, and at most one
snapshot per changed replacement projection. Both legacy stores are gone. Two simultaneous requests
remain represented when either resolves. An empty overview clears stale badges. `giskard-persist`
is untouched.

---

### M3 — Turn-less context restoration

**The original bug.** Deliberately placed after the runtime registry rather than first.

Most of this milestone is harness-side and independent: `ThreadUpdateSink`, the Codex resume
mapping, the bounded replay lifetime, and the mapper's active-turn gate that keeps replayed usage
out of turn ledgers. Those live in crates M2 and M5 never touch.

The exception is the staleness guard. On the abandoned branch it was a bespoke generation/commit
counter in `registry.rs`, hooked into `start_turn`, `compact_thread`, `forget_thread`,
`delete_project`, and the `TurnStarted` arm of the event forwarder — code M2 rewrites wholesale.
Built before M2 it would be written against the old forwarder and then immediately invalidated;
built after, the question it answers ("has a newer turn lifecycle superseded this restore?") is one
the runtime registry already owns.

**Scope.** Port the harness-side pieces above. Persist the restored window through
`ThreadMetadataService`. **Ask the runtime registry whether a newer lifecycle transition superseded
the restore — do not add a second generation counter.** Keep the adapter's replay TTL as a resource
bound only, not as a second invalidation policy.

**Non-goals.** Any new WebSocket message. Any browser handler. Any bespoke lifecycle counter outside
the runtime registry. The bootstrap and delivery layers.

**Exit criteria.** Restoring a resumed thread's context window updates the gauge through the
existing metadata path; no field-specific protocol surface was added; a delayed restore arriving
after a new turn, a compaction, or a thread deletion is rejected by the runtime registry's own
lifecycle state, with no counter of its own.

---

### M4 — Retention policies

**Small, and it must come before the journal** so the journal's byte bounds are meaningful from the
first commit rather than retrofitted.

**Scope.** Named, distinct policies built on the existing
`giskard-persist::preview::bounded_preview`: durable command output, reconnect preview, task tail,
and per-frame admission. Apply them at the runtime apply boundary M2 created. Normalize a completed
command once, before both runtime publication and persistence consume it.

**Non-goals.** A second truncation routine — `bounded_preview` already exists with two callers.
The journal itself.

**Exit criteria.** Each policy has a name, a limit, and an explicit omission marker. Nothing
truncates silently. One normalization pass, not one per consumer.

---

### M5 — Journal and transactional bootstrap

**The largest remaining milestone. Watch it.**

**Scope.** The shared bounded per-thread event journal with a snapshot watermark, pinned at the live
cut. The staged `ThreadBootstrap` transaction with subscription generations, replacing the four
browser phase flags and the split snapshot messages. Genuinely cancellable bootstrap — `handle_ws`
currently awaits the whole subscribe operation, so cancellation needs a generation-owned task, not
just a generation number.

Bounded reads land here as their consumer: add `load_turn_records` and the snapshot/range reads to
`PersistStore`, and send bounded turn records with payloads fetched separately rather than embedding
whole turns.

**Decide explicitly:** with bounded records, the byte-chunked base64 encoding may no longer be
necessary. Record the decision either way.

**Non-goals.** Class-aware outbox and resync (M6). Amendments (M7).

**Exit criteria.** An `ItemDelta` before or after the live cut appears exactly once. Completion
before, during, and after the history read produces neither a missing nor a duplicate turn. A
repeated subscribe cannot deliver an old generation. The four browser phase flags are gone.

---

### M6 — Class-aware outbox and same-socket resync

**Scope.** The connection-owned delivery pump with per-class admission, coalescing replacement
state, control reserve, and ordered-stream loss detection. Ordered overflow marks only that
subscription `NeedsResync` and resyncs on the same socket, satisfied from M5's journal.

**Non-goals.** Anything in the bootstrap transaction beyond the resync entry point.

**Exit criteria.** Replacement state coalesces to the newest revision under pressure. Ordered
overflow marks one subscription and logs once. Resync happens on the same socket while other traffic
continues. No producer awaits socket capacity.

---

### M7 — Late command completion (durable amendments)

**Its own landing, its own review.** This is a durable-format behaviour change.

**Scope.** Append a settled item to its turn's payload file; supersede the bounded turn record so a
reconnecting client can detect it; a durable clock the browser compares against; the
persistence-recovery path when an amendment write fails; the reconnect rule that turns an unseen
amendment into a `CursorReset`.

**Non-goals.** Any change to the runtime registry, journal, bootstrap, or delivery beyond the
coverage token an amendment event needs.

**Exit criteria.** A command completing after its turn was persisted survives journal eviction and a
server restart; a client that already rendered the turn learns of the change; an amendment write
failure is recoverable rather than silently lost; the persisted turn is accurate at every point in
time, not merely after the amendment.

---

### M8 — Cleanup and budget

**Scope.** Remove obsolete stores, protocol variants, browser flags, tests, and documentation left
by M2–M7. Measure the complexity budget and report it. Run unit, integration, browser E2E,
formatting, lint, and the full workspace suite.

**Exit criteria.** The measured protocol/browser counts meet the budget below.

## Required tests

### Metadata

- A no-op mutation performs no write and does not advance revision.
- An internal native-ID or non-selected model-cache change publishes no detail or catalog update.
- A selected context-window change, token fold, rename, mode/model/permission change, and title
  refresh publish the correct projections.
- Two metadata snapshots delivered in reverse revision order cannot regress any field.
- Turn-completion metadata publication cannot change active-turn state.
- Catalog invalidation during an in-flight refetch causes one serialized follow-up refetch.
- An HTTP catalog response older than an applied WebSocket metadata revision cannot regress the
  sidebar and triggers a catch-up refetch.
- Equal-revision catalog and detail projections can each populate their non-overlapping fields.

### Runtime and requests

- One input event produces one sequence, one live projection update, and at most one snapshot per
  changed replacement projection.
- Browser-irrelevant events consume no journal capacity.
- A request received before turn ownership survives reconnect and remains routable.
- An inactive/unsubscribed thread still publishes active/request overview state without publishing
  its transcript or depending on `TokenUpdate`.
- Two simultaneous approvals/requests remain represented when either one resolves.
- Concurrent decisions claim once; harness failure returns the request to pending.
- Approval and server-request resolution before card rendering produces a resolved later card.
- Final bootstrap request state overrides older suffix chronology for actionability.
- A lost ordered `RequestResolved` forces resync and cannot leave another tab actionable forever.
- An empty runtime overview clears stale waiting/running state.
- Repeated history-append failure reaches `PersistenceBlocked` after the named retry budget and is
  visible in bootstrap and the runtime overview.
- Retrying an append whose first result was ambiguous checks `TurnId` and cannot duplicate history.
- Explicitly confirmed discard releases the lease and records a structured lost-turn diagnostic.

### Bootstrap cut

- `ItemDelta` before the live cut appears exactly once.
- `ItemDelta` after the cut appears exactly once and in order.
- Completion before, during, and after history read produces neither a missing nor duplicate turn.
- A captured turn which falls outside the normal initial page is still ordered and complete.
- Several turn transitions during a slow bootstrap remain complete.
- Command-output and text compaction advance the represented-through watermark.
- History append failure retains a recoverable terminal runtime snapshot.
- Repeated subscribe/unsubscribe cannot deliver an old bootstrap generation.
- Bootstrap failure leaves no live subscription without a baseline.
- One-chunk and multi-chunk bootstraps use the same start/stage/commit path.
- Bootstrap history larger than one frame is staged and becomes visible only on commit.
- Oversized suffix admission fails with a thread-scoped retryable result and releases its pin.

### Tasks and history

- An overlapping task snapshot and output event append transcript output once.
- Reversed task snapshots cannot resurrect a completed task.
- A task snapshot never creates a transcript row.
- A bootstrap after server restart accepts its task baseline even when its revision is lower than
  the previous socket's last revision.

### Delivery

- Replacement state coalesces to the newest revision under byte and entry pressure.
- Ordered overflow marks only that subscription `NeedsResync` and logs once.
- Resync occurs on the same socket and other global/direct traffic continues.
- A dropped/coalesced overview is repaired by the latest full overview without reconnect.
- A slow client cannot block event forwarding, persistence, turn cleanup, or another client.
- Writer exit cancels the receive side; intentional final errors receive a bounded drain.
- Control reserve exhaustion is reported and closes only the unhealthy connection after a bounded
  drain.
- A bootstrap commit cannot overtake any required chunk or ordered suffix.

### Browser and protocol

- Full and incremental aggregate bootstraps remove the old phase flags and render exactly once.
- Pagination responses cannot be interpreted as bootstrap history.
- Two pending requests render one waiting state with both identities.
- The embedded browser and server use the one current protocol shape without compatibility paths.
- Invalid or unknown client messages close the connection and produce a diagnostic log.
- Browser E2E asserts exactly-once text and command-output DOM, not only server message counts.

## Documentation required with implementation

Update together:

- `specs/giskard-specification.md`: first move the authorities/clocks table into §13 and require
  every new client-visible state to name its authority, clock, and overflow class; then document
  aggregate bootstrap, request state, replacement overview, and backpressure;
- `docs/api-endpoints.md`: changed WebSocket shapes, resync, and ordering;
- `README.md`: user-visible reconnect/context-window behavior and any storage-layout change;
- `crates/giskard-harness-codex/README.md`: only if adapter lifecycle or routing semantics change.

The specification's current instruction to coalesce deltas by keeping the latest is invalid for
append fragments such as text and command output. Replace it with snapshot coverage plus ordered
suffix or observable resync.

### Complexity baseline and budget

Measured on `main` at `6907fd0`:

- `ServerMessage` has **13** variants.
- The browser has four bootstrap phase flags — `awaitingInitialThreadState`,
  `awaitingThreadResync`, `awaitingIncrementalResync`, `pendingLiveSnapshotReconcile` — appearing in
  **36** places.
- `giskard-proto/src/lib.rs` and `static/app.js` together contain **10,664** physical lines.

The target after M5 is zero of those four flags, one staged bootstrap transaction and one browser
apply path, no more than 13 `ServerMessage` variants, and no positive net line growth across those
two files. If a count cannot meet the target, stop and review the design rather than replacing the
criterion with a qualitative claim.

This is an end-state criterion. Intermediate milestones neither satisfy nor fail it; M5 reports it.

## Exit criteria

The work is complete only when:

- context-window restoration uses the same metadata primitive as every other visible metadata
  mutation;
- bootstrap contains one explicit transaction and no arbitrary message FIFO;
- every client-visible state class has a named authority, clock, and overflow behavior;
- no slow client can block a turn forwarder;
- no queue-full branch silently creates permanent client divergence;
- one agent event is not independently reconciled by several overlapping client projections;
- the measured protocol/browser counts meet the stated complexity budget.
