# Thread State, Runtime, and Bootstrap Reconciliation Plan

## Status

Two foundational landings preceded the milestones below.

**Persisted metadata** (`ThreadMetadataService`, durable thread revisions, typed projections,
explicit recency, revision-aware catalog reconciliation, non-blocking replacement delivery) is
implemented as described below. Metadata publication neither waits for socket capacity nor consumes
ordered-event queue capacity, and a no-op action still receives an authoritative direct result.

**Storage layout** (`6907fd0`) split thread history into a bounded `history.jsonl` index and
per-turn payload files written atomically, with versioned headers on both. That landing was not part
of the original plan; it was inserted because the bootstrap and retention work below kept running
into the same root cause — a turn's record was both its index entry and its unbounded payload. See
*What the storage layout change unlocks* for the parts of this plan it simplifies or retires.

**M1 through M7 are complete**: history pagination over HTTP, the runtime registry, turn-less
context restoration, lazy agent-produced diffs, lazy completed-command output, lazy completed
tool output, and strict Tasks-menu ownership for `RunningTasks`. Each milestone
below carries its own status line; this paragraph is a summary, not the record. A
runtime-owned turn projection, an item endpoint, a cancellable subscribe, a consistent bootstrap
cut, the classified content inventory, the journal and its apply
path, the class-aware outbox, and durable amendments remain to be built, as defined under
*Implementation milestones*.

This plan began with Codex restoring a context window outside a turn. The codebase audit showed
that the context window is not a special state class. It exposed missing general primitives for
persisted thread metadata, in-flight runtime state, reconnect snapshots, request resolution, and
bounded client delivery.

The target is not another field-specific fix. The target is a small set of authorities with explicit
clocks and ownership rules.

### Re-verified against `68bcb65`

Every code reference below was re-checked after the event-pipeline landings. That workstream has
its own documents — [`event-pipeline-architecture-review.md`](event-pipeline-architecture-review.md)
and [`event-pipeline-milestones.md`](event-pipeline-milestones.md) — and its own M0–M8 numbering,
which is **unrelated to the M1–M16 numbering here**. It works the seam between the Codex adapter and
the server; this plan works the seam between the server and the browser. Its review says so
explicitly: the metadata service with revisions, the hub, and the per-client delivery lanes are
named as the parts it does not touch.

Three of its results change this document rather than merely surrounding it, and are folded in
below: `registry.rs` is now a module directory, so every line reference here was re-resolved;
`AgentEvent::ContextWindowUpdated` was replaced by `AgentEvent::TurnUsageUpdated`, which unlike its
predecessor *is* a transcript wire event; and `AGENTS.md` gained entity-authority rules that
constrain how M8 may be implemented. A fourth is vocabulary, not substance: `giskard-harness` now
has an `EventLog`, and both documents call their record a *journal*. The glossary now separates
them.

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

## Glossary

This document invents vocabulary, and several terms below mean something narrower here than they do
in general use. Definitions are alphabetical; the sections that follow assume them.

**Addressable.** Agent-produced content that is complete and never truncated, but carried by
reference rather than inline: the wire sends a *descriptor* and the body is fetched on demand. See
*Bounded, addressable, truncated* for the full four-way distinction.

**Amendment.** A durable correction appended to a turn's payload file after that turn was already
persisted — the mechanism a late command or tool completion needs so history stops claiming it is
still running. M15.

**Authority.** The one component allowed to decide a given piece of state. Every state class has
exactly one, plus one clock; no other component may write it, and no clock orders a different
class's rows. See *Authorities and clocks*.

**Bootstrap.** Everything the server sends a browser when it subscribes to a thread, so the browser
can render that thread from nothing: metadata, some history, the in-flight turn, running tasks, and
outstanding requests. Distinct from *pagination*, which fetches older history later and over HTTP.

**Catalog.** The project's list of threads, as opposed to any single thread's contents. Changes are
published as one coalescible "this list is dirty" signal rather than as a new copy of the list.

**Coalesce.** Replace a queued-but-unsent message with a newer one for the same key instead of
sending both. Only safe for *replacement* classes, where the newer message is complete on its own.

**Control reserve.** Outbox capacity held back from ordinary data so that a message which must be
delivered — a direct action's error, a resync notice — can still be sent when the data lane is full.
Without it, the message announcing a delivery failure could itself be dropped.

**Coverage token.** A marker on a journal entry recording which turn, if any, that entry is
durably covered by — so a reconnecting client can tell whether history already contains it.

**Cut.** A single instant at which several things are read together, so their answers describe the
same moment. The **live cut** is the boundary between "already in the history baseline" and
"arriving as ordered events"; a **consistent cut** is what M11 introduces for the bootstrap reads.

**Descriptor.** A small, fixed-size stand-in for a large payload — sizes, counts, a content
identity, an availability flag — sent in place of the body. `CommandOutputDescriptor`,
`CapturedDiffDescriptor` and `WireToolOutput` are the three that exist.

**Ephemeral signal.** A message with no authoritative value, safe to discard first under pressure.

**Event log (`giskard_harness::EventLog`).** Not this document's journal, and the reason the entry
below says which side it is on. It is the retained log between a harness adapter and the server:
per-thread, cursor-read, bounded to `EVENT_LOG_RETAIN_LIMIT` **entries** (16,384), reporting
eviction to the next reader as an explicit `Gap` rather than dropping silently. It belongs to the
event-pipeline workstream, and this plan neither defines nor changes it.

**Journal.** A bounded, per-thread ring of recent events kept in memory on the **browser-facing**
side, so a client that reconnects can be brought up to date from the journal instead of re-reading
history from disk. Same idea as the harness event log above, on the other seam and with a different
consumer; where confusion is possible, say *harness event log* or *browser journal* rather than
*journal* alone.

**Lease.** Exclusive ownership of a thread's active turn, held for the turn's lifetime. Holding it
is what makes "this thread is busy" a fact rather than a guess; releasing it is what allows the
next turn to start.

**Ordered lane / ordered events.** The delivery class carrying agent events in strict per-thread
sequence — `ItemStarted`, `ItemDelta`, `ItemCompleted`, `TurnCompleted`. Order matters and a drop is
unrecoverable, because a delta carries no expected-previous and appends to a row already rendered.

**Outbox.** The per-connection queue of messages waiting to be written to that browser's WebSocket.
It is finite on purpose: producers — persistence, the harness, the event forwarder — must never
block waiting for a slow browser, so when it fills, something must give. **Class-aware** means each
kind of message gets its own admission and overflow rule rather than one policy for all of them.

**Projection.** A read-only view derived from an authority for one consumer, never itself a source
of truth. The Tasks menu's `RunningTasks` and the cross-thread runtime overview are projections.

**Replacement (revisioned).** A message that carries a complete current value plus a revision, so
the newest one wins and older ones can be dropped freely. The opposite of an ordered event: losing
one costs nothing.

**Request claim / commit / rollback.** The three-step protocol for answering an approval or server
request from one of several open tabs: claim it (so peers see it as answered-in-progress), commit
on success, roll back on failure — each step publishing a new revision.

**Resync / `NeedsResync`.** The proposed state for a subscription whose ordered stream lost a
message, requiring a fresh baseline before further events mean anything. Its necessity is
deliberately unsettled — see M14.

**Subscription generation.** A server-owned counter identifying one subscribe attempt, so messages
belonging to a superseded attempt can be rejected rather than filtered by the browser afterwards.

**Suffix (ordered suffix).** The events that arrived after a bootstrap's cut and must be applied on
top of the baseline once it commits.

**Turn record vs turn payload.** A thread's history is split in two. The **record** is the bounded
index line in `history.jsonl` — ids, model, status kind, usage, timestamps, capped previews. The
**payload** is that turn's own file under `turns/`, holding everything agent-driven. Reading a
turn's summary never reads its payload.

**Watermark.** A position marker saying "everything up to here is already accounted for" — used by
the journal to know what a snapshot already covers.

## Decisions

1. Persisted browser-visible thread metadata is published as one typed, revisioned snapshot.
2. A server-layer metadata service owns metadata mutation, projection, and publication. Persistence
   remains unaware of WebSockets.
3. One `ThreadRuntimeSupport` owns per-thread runtime entries for the active-turn gate, reconnect
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
  large.** That is no longer structurally true for the index. The bootstrap therefore exchanges
  bounded semantic elements as individual transaction messages. It does not base64-encode,
  concatenate, or reconstruct a serialized whole-bootstrap blob.
- An oversized single turn is now one file, retrievable over the HTTP pagination lane, where no
  frame limit applies.

The store now exposes `load_turn_records`, and ordinary format-2 pagination selects index records
before fetching their payloads. Adding the consistent `load_history_snapshot` and
`load_history_from` reads remains in scope for the milestone whose bootstrap consumes them — the
storage plan deferred those transaction-facing reads until a consumer existed, and that consumer
is M11.

### One of the two full-history reads per bootstrap is already gone

`recompute_aggregates` is index-only: the ledger folds `usage`, `model` and `status.kind` from turn
records without opening a payload. The remaining full read is the live-turn expansion path, which
needs only a turn's *position* and should use a bounded index scan rather than `load_all_turns`.

### The truncation primitive already exists

`giskard-persist::preview::bounded_preview(text, max_bytes) -> (String, bool)` is UTF-8-safe and
already has two callers (`prompt_preview`, `status.message`). M5 extends this primitive with
retention direction for durable command head/tail and wire tail previews; M12 reuses it for the
remaining named policies. Neither milestone adds a second UTF-8 truncation algorithm.

### Amendments have a home in the format, and no home in the code yet

Payload records already carry an explicit `index`, payload files are tagged and versioned per file,
and the fold rules for collections and singletons are stated in `parse_turn_payload`. A late command
or tool completion can therefore be appended to its turn's payload file without a format bump.

Nothing implements this. It is M15, and it is a durable-format behaviour change that
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

*Implemented by M3.* The harness-neutral `ThreadUpdateSink`, per-model context-window persistence,
and the lifecycle staleness check were ported from the abandoned branch.
`ThreadContextWindowUpdated` was never merged, so M3 added nothing in its place: restoration
publishes through the general metadata path.
Also keep centralized harness-open update forwarding, and observe pending restoration without a
time-based deadline. Those solve the real provenance/lifecycle gap and are independent of browser
delivery ordering. The mapper's distinction between active-turn usage and turn-less resume metadata
was also on this list; it is gone, because `TurnUsageUpdated` replaced `ContextWindowUpdated` and
made the event turn-scoped at the source. See M3's status note.

Use one authoritative invalidation policy for delayed restoration. The registry revision check at
the metadata commit boundary covers adapter replay, external/passive turn starts, compaction, and
deletion. The adapter does not cancel on a clock or on accepted turn start, so two lifecycle
policies cannot drift.

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

*Still true. M1 moved older-page pagination to HTTP and removed `HistoryPage`, but the bootstrap
sequence itself is unchanged; M10, M11 and M13 own the rest.*

Bootstrap behavior is spread across `ThreadState`, a bootstrap-only `HistoryDelta`, an optional
`LiveTurnSnapshot`, and `RunningTasks`. The browser coordinates them with
`awaitingInitialThreadState`, `awaitingThreadResync`, `awaitingIncrementalResync`,
`pendingLiveSnapshotReconcile`, and message-type-specific completion rules.

This is an implicit protocol state machine. One logical bootstrap transaction makes the boundary
and its fallback mode explicit without requiring one aggregate message.

### Runtime projections used to overlap

*Resolved by M2 and M7. Recorded because it is why the runtime registry owns one apply boundary,
not as a defect to find in the code.*

`RunningTasks` was specified as a Tasks-menu snapshot, but its browser handler also created
transcript rows and merged command output, and a task revision orders task snapshots only — it
cannot deduplicate output also present in an event or live snapshot. M7 removed `RunningTask.output`
and split the browser maps, so task snapshots can no longer create, update or delete transcript
rows.

`ThreadActivity` stored one discriminated record per thread in the browser, so a second approval or
server request overwrote the first, and resolving the represented one could clear the waiting
indicator while another was still pending. M2 deleted it in favour of the revisioned runtime
overview.

Both were ownership problems, not missing watermarks. That is the lesson the primitives below
encode.

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

## Bounded, addressable, truncated

*Authorities and clocks* says who owns each piece of state. This says what Giskard is allowed to do
to it. The distinction matters because one word was doing several jobs, and the wrong one is a lie
to the user.

**An item is agent-produced, and therefore unbounded.** Giskard cannot impose a size limit on a
model's output. The only layer that can is the harness — Codex has already truncated a command's
output before Giskard sees a byte of it. Everything downstream either transports that content or
discards it. "Bounded item" is not an achievable property, and a design claiming it is either
truncating silently or describing something else.

The adapter boundary is the one exception, and it proves the rule rather than weakening it.
`CODEX_MAX_FRAME_BYTES` caps a Codex stdout frame at 64 MiB, and exceeding it closes the transport
fatally. That is a limit imposed where a limit can be imposed — at the harness edge — and it
*fails* rather than shortens. A cap that truncated the frame instead would be the lie this section
is about.

Four distinct things, named:

**Bounded** — a limit Giskard genuinely owns, because Giskard produced the thing being limited: a
protocol frame, a `history.jsonl` index record, the live in-memory projection, a page size.
Bounding these costs nothing, because no agent content is lost.

**Addressable** — agent content: unbounded, complete, never truncated, but not carried inline. The
wire carries a bounded descriptor and reference; a preview is optional when an honest bounded
preview exists. The body is one HTTP fetch away. `CapturedDiffDescriptor`,
`CommandOutputDescriptor`, and M6's preview-free `WireToolOutput` are this pattern.

**Truncated** — content Giskard actually discards. Permitted only for durable retention, only
under a configured limit, and only when the loss is represented explicitly in the projection.
`[retention].max_command_output_bytes` defaults to 128 MiB precisely because it is a
pathological-case backstop rather than a routine policy: the intent is that real output is never
touched.

**Accepted inline** — unbounded agent content transported inline anyway, because in practice it is
small: user text, approval and activity metadata, sub-agent prompts. This is an assumption about
model behaviour, not a property of the data. It is named here rather than left implicit so that
when one of them starts arriving large, the answer is to move it to *addressable* — not to begin
truncating it. Reasoning text was on this list until M9 made it addressable in completed turns,
which is the worked example of that rule. Agent text is still on it, and deliberately: M9 leaves it
inline because it is the content a reader came for, not because it is bounded.

### Rules that follow

**Never truncate a set whose completeness is its meaning.** A command's output has a natural tail,
so a preview of it is honest. A file-change entry list does not: a patch review naming 50 of 400
touched files is not a shortened answer, it is a wrong one — and the approval card rendering that
set exists specifically to say what will be modified. When such a list does not fit a frame, page
the fetch. Do not cap the truth.

**A projection states which loss occurred.** `CommandOutputDescriptor` carries `original_bytes`,
`durable_bytes` and `preview_bytes` as three separate numbers so the browser can distinguish "this
is everything", "retention discarded some", and "this is a preview; fetch for the rest". Every new
descriptor which truncates or previews content owes the same distinction. When content remains
complete and the descriptor contains no preview, as in M6, original and durable size are identical
and there is no preview size to report.

**Addressable content needs a degraded state.** A body can be swept, or its turn deleted, between
the descriptor and the fetch. Every lazy field inherits that case: descriptor availability is a
projection-time claim, and a later 404 uses one shared browser treatment rather than silently
showing an empty body.

### What this means for Primitive 3

The bootstrap goal is not bounded items; that is unachievable. It is **no accidental unbounded
frame: every heavyweight field is addressable or explicitly accepted inline**. An item may be
arbitrarily large provided the transaction carries a reference to its heavyweight body rather than
that body itself. The journal follows the same rule: it holds bounded records and references, never
an inline heavyweight payload unless the plan names the accepted-inline assumption.

## Primitive 1: `ThreadMetadataService`

**Status:** built. This section describes code on `main`, not work to do.

A server-layer service over `PersistStore` and the publication interface, stored in
`RegistryShared`, exposing narrow registry methods to routes. It does not depend on
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

## Primitive 2: `ThreadRuntimeSupport`

**Status:** built by M2; M8 completes the one part it left behind. This section describes code on
`main` except where noted. The type was named `ThreadRuntimeRegistry` when this section was
written and is now `ThreadRuntimeSupport` (`thread_runtime.rs:35`), with `ThreadRuntimeEntry`,
`ThreadRuntimeSlot` and the read-side `ResolvedThreadRuntime` (`:315`) unchanged in role.

M2 merged the state previously split across `ThreadTurnGate`, `LiveBufferStore`, `RunningTaskStore`
and the approval/server-request routing maps — none of those types exist any more. The registry owns
the thread-entry map, global overview revision, and cleanup; callers do not coordinate several
public stores.

Use a per-thread state object behind a short-lived lock. It owns:

- reserved/acknowledged active-turn state and the turn lease;
- the normalized in-flight turn used for reconnect;
- the highest event sequence represented by that snapshot;
- running tasks and their revision;
- outstanding approval and server-request records;
- resolved request records needed by reconnect;
- the bounded recent client-visible event journal;
- the thread's cross-thread runtime summary.

The forwarder still assembles the completed `Turn` for persistence, but M8 moves the item
projection it assembles from into the registry: the forwarder asks for the items rather than
keeping its own copy. Native persistence data and browser runtime projection do not need to be the
same type.

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

`ContextWindowUpdated` no longer exists; `AgentEvent::TurnUsageUpdated` replaced it, and it is the
one event that crosses both boundaries at once. The forwarder reads it twice
(`event_forwarder.rs:1601`): once to persist the effective context window against the turn's model
through the metadata authority — a revisioned replacement — and once to publish
`WireAgentEvent::TurnUsageUpdated` as an ordered transcript event. One harness event, two
authorities, two clocks. That is legitimate, because the two carry different facts about the same
report, but it is the first such event and the publication boundary must not let the ordered half
imply anything about the replacement half's revision.

`ThreadOpened` and `DiffUpdated` currently reach the wire without a browser handler; keep them
internal unless an audited UI requirement is added. They must not consume journal or queue capacity.

Synthetic prompt events, fallback transcript events, turnless errors/notices, late item events,
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
   budget is exhausted, enter `PersistenceBlocked { turn_id, error }`, extended with the attempt
   count at that point. **Scope note:** the
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

No client delivery is awaited while holding the turn lease, runtime lock, or store lock.

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
not by chunk count. Apply a per-item byte ceiling to reconnect-only accumulated text, keeping bounded
head and tail content with an explicit omission marker. Live incremental delivery remains
authoritative; completed command output is authoritative up to its separately configured durable
limit, while its WebSocket completion carries only the bounded item representation. If the event
stream fails before the harness supplies a completed item, persist or surface the same marked
recovery representation rather than silently claiming complete output. The semantic bootstrap has
its own aggregate staged-byte and item budgets; it does not redistribute one turn-wide text budget
among items.

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
        turn_state: Idle | Active | PersistenceBlocked { turn_id, error },
        running_tasks: { revision, tasks },
        requests: [RequestState],
    },
    notices,
}
```

The connection allocates this generation monotonically for each thread. `BootstrapStart`, semantic
elements, commit, live events, and resync controls carry it. A newer subscribe cancels the prior
generation; the browser discards any frame which does not match the latest started generation. WebSocket FIFO
plus this server-owned generation is sufficient—there is no client-generated subscription token.

This is one logical transaction, not one WebSocket frame. Encode it physically as
`BootstrapStart`, independently parseable typed semantic element messages, and `BootstrapCommit`.
Each element carries transaction and section identity, ordinal information, and a hard encoded-byte
limit. The browser stages a transaction and changes authoritative UI state only after validating
its commit. It then applies metadata/history/live state, replays `ordered_suffix`, and applies
`final_runtime` last. Older suffix events therefore cannot regress the final active/request/task
state. Only the committed transaction resets bootstrap state or releases an optimistic first-turn
lock from an explicit `turn_state = Idle`. Live `ThreadState` changes metadata only.

Always use start/elements/commit, including when the transaction has only one element. There is one
browser staging and apply path, not a small-transaction fast path. Generation and commit provide
atomicity; semantic element boundaries provide the per-message size bound.

The per-client bootstrap task may await capacity while emitting elements because it is not a store,
harness, or event-forwarder producer. It reserves a transaction/barrier slot but does not place the
whole encoded transaction in the connection outbox. The initial history page has a byte as well as
turn-count budget. A bootstrap history element is a bounded `TurnRecord`; its payload is fetched
separately after commit. An element large enough to be worth deferring uses a bounded descriptor
and lazy retrieval rather than byte slicing. See *What the storage layout change unlocks*. The
pinned ordered suffix has separate entry and byte limits and counts against both runtime-journal and
bootstrap memory until commit or cancellation.

Live request payloads, notices, metadata strings, and reconnect-only accumulations each need a
documented classification: bounded, addressable, truncated with an explicit marker, or accepted
inline with the measurement behind it. M12 records those and enforces them with a test.

**Superseded.** This plan previously specified a maximum ordered-event size, and a live event
exceeding it marking its subscription `NeedsResync`. There is no such size: the socket already
carries `MAX_WS_MESSAGE_BYTES`, and inventing a second threshold would manufacture an overflow case
that then needs a policy. With every element classified, an event large enough to be worth
deferring is already addressable. Never truncate silently; an omission marker or lazy full-content
retrieval must make the boundary visible.

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
   bounded history/runtime elements through the dedicated bootstrap task rather than queueing the
   whole transaction at once.
9. Send the transaction commit marker. Events through the installed watermark are
    ignored when delayed publication effects arrive; later events queue behind the commit marker.
    Only after commit does the generation become live.

Drop a suffix event only when its sequence is covered by the immutable live snapshot/watermark pair,
or its `Turn(turn_id)` coverage token is present in the exact history view returned by this
bootstrap. A turn ID alone must not suppress a late item event, because that entry has no durable
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
the dedicated per-client bootstrap encoder may wait for its element capacity.

| Delivery class | Admission and failure behavior |
| --- | --- |
| Ordered events | FIFO per subscription; overflow becomes `NeedsResync` |
| Revisioned replacement | Keep newest by key; evict obsolete entries first |
| Catalog invalidation | Keep one dirty key per catalog |
| Bootstrap transaction | Flow-controlled start, semantic elements, and commit |
| Barrier/control | Use reserved capacity and preserve prerequisites |
| Direct action response | Use control reserve through delivery/failure |
| Ephemeral signal | Evict first; never authoritative |

The outbox owns separate finite data capacity and reserved control capacity, each bounded by bytes
and entries. Replacement messages and invalidations coalesce in place. Admission first removes
obsolete replacement entries and ephemeral signals. It never evicts a direct response, required
barrier, or bootstrap element which has begun transmission merely to admit ordinary data.

Ordered-stream overflow clears that subscription's queued suffix, records one `NeedsResync`
transition, and schedules `ResyncRequired { thread_id, subscription_generation }` in control
capacity. It logs once with the lost sequence range and byte counts. While `NeedsResync`, reject
further incremental events for that subscription until a new subscription generation establishes
a baseline. The shared runtime journal continues independently and may satisfy that resubscribe.

Bootstrap elements are admitted by awaiting ordinary outbox capacity in their dedicated per-client
task. The transaction start, elements, and commit retain FIFO order; control priority must never let
a commit or later incremental event overtake required elements. Cancellation discards all unsent
elements for that transaction and releases its pinned suffix.

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

The `ThreadRuntimeSupport` also owns cached per-thread summaries and the global overview revision.
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

**Status:** implemented by M7.

`RunningTasks { thread_id, revision, tasks }` updates only the Tasks menu, elapsed timers, and stop
controls. Event/history rendering alone creates and updates transcript rows. The browser data
structures are split: `runningTasks` serves the menu and `runningCommands` the transcript, and one
map does not serve both projections.

The server publishes a task snapshot after every task-state mutation. Reverse delivery cannot
regress the menu because the task revision is allocated atomically with the mutation.

A task from a turn outside the loaded history may be present in the menu without a transcript row.
Selecting it should load/navigate to the owning turn or explain that the row is not loaded; the task
snapshot must not fabricate a second transcript representation.

## Protocol simplification

Most of this list is done. Verified against `giskard-proto/src/lib.rs`:

- ~~`ThreadContextWindowUpdated`~~ — never merged, so there was nothing to remove;
- ~~`TokenUpdate` and `TokenScope`~~ — gone;
- ~~top-level `ApprovalRequest`~~ — never a `ServerMessage` variant;
- ~~`ApprovalResolved`~~ — removed in M2; `RequestState` is the sole resolution authority;
- ~~additive `ThreadActivityBootstrap` and authoritative use of `ThreadActivity`~~ — removed in M2
  in favour of the revisioned runtime overview;
- top-level bootstrap-only `HistoryDelta` and `LiveTurnSnapshot` — **still present**, and M13
  replaces them with the bootstrap transaction;
- turn runtime state from live `ThreadState` — **still present**.

One item moved the other way. `WireAgentEvent::TurnUsageUpdated` is a **new** transcript event,
added when live token usage started being tracked during a turn. `ServerMessage` did not grow — it
is still 11 variants, and the event rides the existing `Event` envelope — so the budget below is
unaffected. It is recorded here because this list exists to keep the protocol shrinking, and a
list that only records removals will read as progress while the wire grows underneath it.

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

- A command or tool may finish after its interrupted turn was appended. The late event has no
  durable coverage, so reconnect after disconnection can show it as running. **This is now M15**:
  the per-turn payload format admits the amendment without a format bump, and what remains is the
  durable clock, recovery path, and reconnect rule.
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
- `thread_bootstrap.rs`: history/live cut and semantic bootstrap transaction builder;
- `delivery.rs`: connection hub, subscription generations, bounded class-aware outbox.

`registry.rs` remains harness/process orchestration, and the event-pipeline work has already taken
most of this: it is now a module directory — `registry/{admission,driver,event_forwarder,project,
thread}.rs` — with the file itself down to about 3,800 lines. What remains for this plan is the
last of the three independent applications of every agent event: the forwarder's item fold, which
M8 publishes to the runtime authority. `AppState` should not expose raw live-buffer/task stores
once routes can use narrow registry and bootstrap interfaces.

Use structural impact analysis before publishing the remaining forwarder state. `LiveTurnState::
item_events`, reached through `ThreadRuntimeSupport::live_item_events` (`thread_runtime.rs:712`),
also serves trusted sub-agent-link resolution and must remain available through the registry's
native/internal view — M8 must not break that consumer while publishing the item projection.

## Implementation milestones

Each milestone is one landing, reviewable on its own. Scope and non-goals are binding: see
*How to use this document*.

**Every milestone below states the problem it fixes, with a concrete example, before it states
what to build.** A milestone whose problem cannot be made concrete is a milestone without evidence
behind it; say so in the problem section rather than proceeding as though the case were settled.

**Dependencies.** M1 depends on nothing. M3 and M4 need M2's runtime registry — M3 for the
lifecycle state that replaces its own guard, M4 for the active diff authority. M5 reuses M4's lazy
content boundary and M2's apply boundary. M6 applies the same addressable-content pattern to
completed tool output. M7 removes output from the task projection. M8 finishes M2's claim on the
in-flight turn projection, and M9 needs M8's by-identity item read. M10 and M11 are independent of
each other and of M12. M12's inventory is smaller if M9 has landed, because a previewable field no
longer has to be accepted inline. M13 needs M11's cut and M12's classified inventory before journal
byte accounting is meaningful, and needs M10's generations if it builds the transaction. M14 needs
M12's outbox measurements to choose its policy. M15 reuses M5/M6 normalization and needs M13's
journal coverage token. Anything not listed here is ordering preference, not a constraint.

**New behaviour lands after the primitive it depends on, never beside it.** M3 is the worked example:
it is the bug that started this plan, and it still waits for the runtime registry, because building
its staleness guard against the forwarder M2 deletes would mean building it twice. Nothing here is
urgent enough to be worth implementing against a primitive that is about to be replaced.

Milestones are deliberately small. A milestone that looks like it will exceed roughly two thousand
lines of production change is a milestone that has absorbed its neighbour — stop and check the
non-goals.

---

### M1 — History pagination over HTTP

**Status:** complete. Older-history pages are served over authenticated HTTP; `LoadHistory` and
`HistoryPage` are gone from the protocol.

**Problem.** Older-history pagination travelled the ordered WebSocket lane as `LoadHistory` /
`HistoryPage`, competing for outbox capacity with live events and indistinguishable from bootstrap
history. Click "load older" while a turn is streaming and a page of completed turns queued behind
the deltas; switch threads mid-fetch and the previous thread's page could still be applied. None of
that is inherent — a page has no ordering relationship to live state.

**Proposed change.** Serve older-history pages from an authenticated HTTP endpoint. Point the
browser's "load older" path at it. Remove `LoadHistory` and `HistoryPage` from the protocol. Cap the
requested page count; correlate or abort in-flight fetches when the active thread changes.

Pagination is a request/response with no ordering relationship to live state, so it does not belong
in the ordered lane, where it competes for outbox capacity and would need a subscription generation.
Moving it out also shrinks what M13 has to reason about.

**Non-goals.** The bootstrap transaction. Bounded reads — `load_history` already serves whole turns
and is sufficient here; the bounded reads land with the bootstrap that needs them.

**Expected outcome.** Two protocol variants are gone. Pagination cannot be confused with bootstrap
history. Switching threads mid-fetch cannot apply the previous thread's page.

**Transitional handoff to M11 and M13.** M1 moves only older-page pagination to HTTP. Until M13
replaces the
implicit bootstrap state machine, fresh subscriptions and stale-cursor recovery continue to carry
their bounded initial history as a bootstrap-only reset `HistoryDelta`; this is not a pagination
response. The server still obtains that history and the live snapshot through sequential reads, so
they do not form a transactional cut. M1 deliberately preserves that pre-existing limitation rather
than introducing a second independent HTTP/WebSocket race. M11 makes those reads a consistent cut;
M13 owns closing the rest with the journal watermark, ordered suffix, and exactly-once apply.

---

### M2 — Runtime registry

**Status:** complete. Runtime state and immutable publication effects now share one per-thread
transition boundary; the legacy stores and additive activity protocol are removed; request
transitions are revisioned claim/commit operations; and failed turn persistence retains the lease
and complete runtime representation according to the decision below.

`ApprovalResolved` went with them: a resolution announced twice, once revisioned and once not, is
two authorities for one request, and the unrevisioned one is the half a client cannot gate. The
attempt counter in `PersistenceBlocked` did not ship either — with no retry loop it could only ever
be a constant, so it lands with the recovery step that gives it a value.

**Problem.** In-flight thread state lived in several stores that each observed the same agent
event independently — a live buffer, a running-task store, ad hoc request maps, and additive
activity messages — with no shared transition boundary. One event produced several uncoordinated
updates, so two browser tabs answering the same approval could both believe they owned it, and an
`ItemDelta` could update one projection and not another.

**Proposed change.** `ThreadRuntimeSupport` as the sole process-local authority for the active-turn
gate, the in-flight turn projection, running tasks, outstanding requests, and the cross-thread
overview. Every client-visible agent event goes through one apply boundary. Delete `LiveBufferStore`
and
`RunningTaskStore`. Make task snapshots menu-only. Add request claim/commit with one authoritative
browser request map. Replace additive activity state with the replacement overview.

**Non-goals.** Lazy diff delivery (M4). Lazy completed-command output (M5). Lazy completed tool
output (M6). Task-projection ownership (M7). The content inventory (M12). The event journal and
bootstrap transaction (M13). Durable amendments and amendment-write recovery (M15). Changes to
`giskard-persist` — **this milestone must
not touch that crate**; if it appears to need to, that is the signal to stop.

Recovery from a *failed turn append* is in scope here, because it is part of the turn-completion
handoff: hold the lease, keep the only complete representation, surface an actionable error. Whether
this milestone also ships the user-facing `Retry persistence` / `Discard unpersisted turn` actions
and their two client messages was left open in the scope note in *Turn completion handoff*.

**Decision.** M2 stops at retaining the lease and complete runtime representation, blocking another
turn, and surfacing the append failure through the existing structured error path. It does **not**
add retry/discard client messages, controls, or destructive recovery behavior. Those recovery
actions require a separate landing and review; until then an operator repairs the persistence fault
and restarts the server. This decision does not permit releasing the lease or discarding the only
complete representation on append failure.

**Expected outcome.** One input event produces one sequence, one projection update, and at most one
snapshot per changed replacement projection. Both legacy stores are gone. Two simultaneous requests
remain represented when either resolves. An empty overview clears stale badges. `giskard-persist`
is untouched.

---

### M3 — Turn-less context restoration

**Status:** complete.

**The original bug.** Deliberately placed after the runtime registry rather than first.

Most of this milestone is harness-side and independent: `ThreadUpdateSink`, the Codex resume
mapping, pending replay observation without a time-based deadline, and the mapper's active-turn
gate that keeps replayed usage out of turn ledgers. Those live in crates M2 and M13 never touch.

The exception is the staleness guard. On the abandoned branch it was a bespoke generation/commit
counter in `registry.rs`, hooked into `start_turn`, `compact_thread`, `forget_thread`,
`delete_project`, and the `TurnStarted` arm of the event forwarder — code M2 rewrites wholesale.
Built before M2 it would be written against the old forwarder and then immediately invalidated;
built after, the question it answers ("has a newer turn lifecycle superseded this restore?") is one
the runtime registry already owns.

**Problem.** Codex reports a resumed thread's context window outside any turn, and Giskard had
nowhere to put it: the gauge only updated from active-turn usage. Reopen a resumed thread and it
showed the conservative fallback window rather than the model's real capacity, until the next turn
happened to report one. This is the bug that started the whole plan.

**Proposed change.** Port the harness-side pieces above. Persist the restored window through
`ThreadMetadataService`. **Ask the runtime registry whether a newer lifecycle transition superseded
the restore at the metadata commit boundary — do not add a second generation counter or a
time-based invalidation policy.**

**Non-goals.** Any new WebSocket message. Any browser handler. Any bespoke lifecycle counter outside
the runtime registry. The bootstrap and delivery layers.

**Expected outcome.** Restoring a resumed thread's context window updates the gauge through the
existing metadata path; no field-specific protocol surface was added; a delayed restore arriving
after a new turn, a compaction, or a thread deletion is rejected by the runtime registry's own
lifecycle state, with no counter of its own.

*Superseded in part, and worth recording.* The event this milestone was built around,
`AgentEvent::ContextWindowUpdated`, no longer exists. The event-pipeline work replaced it with
`AgentEvent::TurnUsageUpdated`, which is turn-scoped by construction, carries `context_window` and
`model` as optional fields, and persists the per-model window only when the harness acknowledged a
model for that exact turn. The turn-less case this milestone existed to handle is now designed away
at the source rather than absorbed downstream, and the mapper's active-turn/resume distinction that
this milestone asked to preserve went with it. What survived is the part that mattered: the window
is persisted through the metadata authority, the staleness guard is the runtime's own lifecycle
state, and no field-specific wire message was ever added. The new event does reach the wire, but as
a transcript event about token usage, which is a different fact — see *Apply an event once*.

---

### M4 — Lazy agent-produced diffs

**Status:** complete. Agent-produced diff bodies are lazy across live and persisted turns; active
and durable lookup share content identities; the workspace Git diff path remains unchanged.

**Why it comes first.** Diff bodies are agent-driven and can dwarf every bounded item descriptor.
Moving them behind an explicit fetch boundary keeps the later retention policies honest and lets
the bootstrap exchange semantic items without inventing a generic fragmentation format.

This milestone concerns diffs captured from agent events: inline diffs on file-change items or
request metadata and the structured `DiffUpdated` collection stored with a turn. The existing authenticated
`GET /api/projects/{id}/git/diff` workspace endpoint and Git panel remain unchanged and receive
regression coverage; they answer a different, current-worktree question.

`DiffUpdated` events and persisted `turn.diffs` already reach the wire without being rendered by
the browser. Making those turn-level diffs discoverable in the UI is an existing product gap, not
a regression introduced by M4's lazy-diff representation.

**Problem.** Agent-produced diff bodies were delivered eagerly everywhere — inline on file-change
items, in `DiffUpdated`, in every `WireTurn`, and in each history page — while the browser did not
even render turn-level diffs. A turn touching a dozen files shipped every patch body on the socket
and again on every reload, for content nobody had asked to see.

**Proposed change.** Replace eagerly delivered agent-produced diff bodies with bounded descriptors
carrying the path, change kind, display metadata or statistics, availability, and a stable content
identity. Add an authenticated
`GET /api/projects/{project_id}/threads/{thread_id}/turns/{turn_id}/diffs/{diff_id}` read for the full
captured diff. `DiffId` is an opaque, independent content identity; it does not encode an `ItemId`.
An item which owns a diff carries both its ordinary item identity and a diff descriptor, while a
turn-level `DiffUpdated` descriptor needs no invented item identity. Replacing active diff content
creates a new `DiffId`. Resolve it from runtime state for an active turn and from the immutable
payload for a completed turn, so a response for older active content cannot masquerade as the
current value.

On the wire, `WireFileChangeEntry.diff`, `WireFileDiff` bodies, and equivalent request metadata
become bounded captured-diff descriptors; `WireAgentEvent::DiffUpdated` carries that descriptor.
`WireTurn` and the existing HTTP history response use the same descriptor-only forms. The new read
returns the tagged full representation identified by `diff_id` (unified text or structured diff).

Use one logical diff-content side table per active turn. The runtime entry maps content-hash
`DiffId`s to immutable content and projections carry only descriptors. At commit, reconstruct the
existing inline payload representation and write the same atomic per-turn payload format; do not
create a separately committed blob directory. Item association remains on the projected descriptor
when one exists, but lookup is keyed by `(project_id, thread_id, turn_id, diff_id)`. The persisted
endpoint scans the selected turn payload for matching content, while history projection returns
only descriptors.

Keep the existing inline payload representation and payload format version. Derive a stable,
domain-separated hash of the complete diff identity — content kind, path, change kind, and body —
while projecting a turn, and serve the matching inline content through the same endpoint without
rewriting the turn. Repeated identical updates for one path share one runtime map entry, while the
same patch text on different paths remains independently replaceable.

Persist the full diff before releasing its runtime authority. A fetch racing turn completion may
resolve through either authority but must return the same identified content. If an active update
has replaced the requested `diff_id`, return a conflict carrying the current descriptor; do not
retain an unbounded version cache. The browser retries only while the same thread/turn remains
selected and the current descriptor still advertises that identity. A per-request selection token
rejects late responses; M10 later adds subscription-generation gating. The endpoint reads captured
agent output and must not recompute a workspace Git diff whose answer may already have changed.

**Non-goals.** Retention policy for command, tool, text, or reasoning content. The journal,
bootstrap transaction, and general payload-blob store. Any behavior change to workspace Git status
or Git diff.

**Expected outcome.** Opening, reconnecting to, and hydrating history for a thread transfers no full
agent-produced diff body eagerly. Every currently advertised diff remains retrievable while active
and after persistence, including across the completion race; a superseded active identity produces
the explicit conflict above. Workspace Git diff continues to use its existing HTTP path.

---

### M5 — Lazy completed-command output

**Status:** complete. Completed command output is carried as an 8 KiB tail descriptor and fetched
in full from its own endpoint; the durable limit is configurable, and the late-completion exception
remains as documented until M15.

**Problem.** Completed command output was streamed incrementally and then sent again in full —
embedded in `ItemCompleted`, in reconnect history, in ordinary history pages, and in every
`WireTurn`. A build producing megabytes of output crossed the socket once as deltas and then once
more per completion, per reconnect, and per history page containing that turn.

**One heavyweight field, end to end**, without changing running-command behaviour or generalizing
prematurely to heterogeneous tool JSON.

**Proposed change.** Configure `[retention].max_command_output_bytes`, defaulting to 134217728 bytes
(128 MiB) with a 32768-byte minimum; reject a smaller value at startup. Pass the resolved policy
through `AppState` into registry/runtime construction, including the replay server. Normalize every
completed `CommandExecution` item once before `CurrentTurnItems`, runtime, wire, and persistence
consume it—even when its command status remains `in_progress`. Output within the durable limit is
unchanged. Oversized output retains equal head and tail budgets (favoring the tail for an odd byte)
around `\n[… N bytes omitted from durable command output …]\n`; the UTF-8-safe marker counts toward
the configured limit.

Normalization takes the original string once and returns one result containing the durable core
output, truncation metadata, and wire descriptor. It computes original statistics and the preview
before replacing the core string; downstream consumers do not independently truncate. For durable
head/tail allocation, reserve the exact marker first, split the remaining byte budget approximately
equally, retreat to UTF-8 boundaries, and give any usable remainder to the tail before the head.
Recompute the marker/omitted count until its encoded length is stable.

Keep `ItemPayload::CommandExecution.output: String` as the durable representation and add these
compatible fields to the same variant:

```text
output_truncated: bool                 # serde default false; omit when false
output_original_bytes: Option<u64>    # present exactly when output_truncated
output_original_lines: Option<u64>    # present exactly when output_truncated
```

When not truncated, derive original counts from `output`. When truncated, both counts are required
and at least the retained counts. Empty output has zero lines; otherwise count newline-separated
logical lines without treating a final newline as an extra empty line. A malformed persisted
combination remains usable: warn with path/turn/item context, preserve the marked output, and derive
missing or impossible counts conservatively from the retained string. These additive/defaulted
fields remain payload format 1: older readers ignore them and still see valid marked output; newer
readers accept old records; no migration or rewrite occurs.

Replace the wire output string with `WireCommandOutput`: `preview`, `preview_truncated`,
`durable_truncated`, original/durable/preview byte and line counts, and
`output_available`. It is an 8192-byte tail-oriented preview: the omission marker
`[… N bytes omitted from command output preview …]\n` counts toward the limit and the remaining
budget retains final raw output. `preview_truncated` means the original exceeded that preview;
`durable_truncated` means it exceeded durable retention. Use this projection in `ItemCompleted`,
`LiveTurnSnapshot`, `WireTurn`, and HTTP history. Never repeat full completed-item output over
WebSocket. `output_available` is true exactly when the endpoint can return the complete durably
retained representation: for terminal commands retrievable from the runtime map or persisted
history, including `PersistenceBlocked` and legacy turns. It is false for a command whose status
remains running and for the post-persistence late-completion exception below.

The 32 KiB minimum guarantees that the durable head/tail representation retains enough final raw
output to reconstruct the same tail-oriented preview before and after persistence. Descriptor
construction from the live normalized item and from a reloaded payload must be byte-identical.

Add authenticated
`GET /api/projects/{project_id}/threads/{thread_id}/turns/{turn_id}/items/{item_id}/command-output`,
returning `text/plain; charset=utf-8`. Set `X-Giskard-Output-Truncated` to `true` or `false`,
`X-Giskard-Output-Original-Bytes` to the original UTF-8 byte count, and
`X-Giskard-Output-Original-Lines` to the original logical line count; these describe provider output
before durable truncation, while the body is all output Giskard retained. Validate that the thread
belongs to the path's project before lookup, matching the captured-diff endpoint. Resolve a terminal
active turn from a dedicated runtime command-output map keyed by `(turn_id, item_id)`, then use a
targeted persist-store lookup of that item in immutable history. Never scan all turns. Intentionally
return the same 404 for an unknown, wrong-kind, unavailable, or still-running item. Populate the
runtime map before publication and remove a normally completed entry only after successful
persistence, so no fetch gap exists; retain it while `PersistenceBlocked`. Thread deletion or
retirement clears it with the rest of runtime state.

The browser continues appending deltas and live-updating an open overlay while a command runs. A
nonterminal `ItemCompleted` does not release that accumulation. On terminal completion it releases
the accumulated string, renders the descriptor preview, and fetches the complete durably retained
representation only when the user opens the existing overlay. Loading, retry, `AbortController`,
thread/turn/item and overlay selection gating, linkification, copy, and download remain supported.
Closing the overlay releases fetched content.

A terminal completion received after its turn was already persisted is still normalized before
wire publication, but M5 does not amend the payload or advertise lazy availability. A browser which
observed the running stream may keep its local accumulation for that session; reconnect has only the
bounded preview. Log the deferred durable update explicitly. M15 makes this case persisted and lazy.

**Non-goals.** Tool input/output/metadata. A generic item-content abstraction. Any change to agent,
reasoning, user, approval, activity, or sub-agent content. Running-output retrieval. The journal,
transactional bootstrap, outbox, payload format bump, migration, or command amendment behavior.

**Expected outcome.** Every newly normalized command output stays within its configured durable
limit and truncation is explicit; legacy output remains unmigrated but its wire projection is
bounded. Running commands behave exactly as before. Every completed command projection carries only
the 8 KiB tail-oriented descriptor. Normal completion has byte-identical runtime and persisted
endpoint reads; the documented post-persistence late-completion exception advertises no lazy body
until M15. Old format-1 turns work without migration, and the browser retains completed output only
while its overlay is open or while preserving the late-completion exception's already-observed
stream.

---

### M6 — Lazy completed tool output

**Status:** complete. Completed tool JSON is represented by a preview-free descriptor across live
and persisted projections and fetched on demand from its authenticated item endpoint.

**Problem.** Completed `ToolCall.output` is arbitrary JSON, repeated in `ItemCompleted`, reconnect
state, ordinary history pages, and every `WireTurn`. Observed outputs regularly reach roughly
100 KiB and accumulate across a turn, so a turn with a handful of MCP calls re-sent hundreds of
kilobytes on every reload. Unlike command output it has no natural tail: truncated JSON is not
JSON, and a recursive projection would change types and semantics.

**One opaque heavyweight field, without a JSON preview language.** This milestone makes only
completed tool output addressable. It does not change the tool-call domain model or generalize
item content.

**Proposed change.** Keep `ItemPayload::ToolCall.output: Option<serde_json::Value>` as the complete
durable representation. Keep `input`, `metadata`, `error`, `name`, `server`, `status`, and
`subagent` inline. Replace only the wire `output` value with an optional bounded `WireToolOutput`
descriptor carrying
`serialized_bytes: u64` and a strong domain-separated content `version`. The version is the quoted
SHA-256 identity of the exact compact JSON response bytes and is also returned as the HTTP `ETag`.
Descriptor presence means that output exists and is retrievable at projection time; an absent
output remains absent. Do not include a lossy JSON preview: a truncated serialization is not valid
JSON, while a recursive projection would change types and semantics. Use the descriptor in
`ItemCompleted`, `LiveTurnSnapshot`, `WireTurn`, and HTTP history so no completed tool output is
repeated eagerly.

Define the response bytes once as compact `serde_json::to_vec` serialization of the output value;
the descriptor count, content version, runtime response, and reloaded response all derive from
those bytes. Preserve an explicit `Some(Value::Null)` through item serialization as a present
output: it produces a descriptor and the four-byte HTTP body `null`. Missing `output` remains
`None`. The browser uses a separate loaded-state flag and never uses JavaScript `null` to mean both
"not loaded" and a loaded JSON null.

Add authenticated
`GET /api/projects/{project_id}/threads/{thread_id}/turns/{turn_id}/items/{item_id}/tool-output`.
Return the complete retained `output` value itself, not the enclosing tool item or a stringified
JSON wrapper, with exactly `application/json` as the content type. Validate project/thread/turn/item
containment before lookup. Return the same 404 for an unknown item, a non-tool item, a tool with no
output, a still-running tool, unavailable output, or a cross-container identity.

Resolve completed active output from a dedicated runtime map keyed by `(turn_id, item_id)`, then
use a targeted persisted lookup of that item in the selected immutable turn; never scan all turns.
Populate runtime authority before publishing the descriptor and remove it only after successful
persistence, so a fetch racing completion has no gap. Retain it while `PersistenceBlocked`, and
clear it with the rest of runtime state on thread retirement or deletion. Runtime and persisted
reads return byte-identical compact JSON for the same value.

Persisted history projection must serialize and hash each completed tool output selected for a
page because the descriptor carries the strong version of the exact JSON response bytes. Active
completion performs that work off the async reactor and reuses the prepared descriptor; history
projection retains this per-page CPU cost until descriptors themselves become durable metadata.

An `ItemCompleted` tool is output-addressable when it has an output and its normalized status is
not `pending`, `in_progress`, `inprogress`, or `running`; hyphens and case are normalized as for
commands. Missing or unknown status is terminal because the harness emitted `ItemCompleted`.
A later nonterminal replacement removes any earlier descriptor and runtime authority; a later
terminal replacement atomically replaces both and changes `version`. The browser accepts a fetched
body only when its `ETag` still matches the selected item's current descriptor, otherwise it retries
only while the same thread, turn, item, and overlay remain selected.

MCP `ItemDelta::Text` values remain opaque live progress messages and continue to render while a
tool is running; they are not output chunks and cannot reconstruct the result. A running tool does
not advertise output availability. When a terminal `ItemCompleted` supplies authoritative output,
the browser releases any temporary progress representation, renders the bounded descriptor, and
fetches JSON only when the existing tool overlay is opened. Reuse the command overlay's loading,
retry, `AbortController`, selection gating, close-time release, copy, and download behavior, but
render the fetched value as JSON and never parse a preview. The completed inline row keeps its input
preview and replaces the current output snippet with the output byte count and Open affordance. The
overlay combines the already-inline input with the pretty-printed fetched output; copy and download
continue to cover the combined visible tool data. Progress text is discarded when authoritative
completion arrives and is never substituted for missing output.

A terminal tool completion received after its turn was already persisted remains ignored in M6,
as it is today: no descriptor or endpoint availability is advertised, and the drop is logged with
thread, turn, and item identity. M15 extends durable late-item amendments to this case.

Keep persisted JSON complete in M6. There is no durable tool-output limit, truncation marker,
payload format bump, or migration in this milestone. Durable JSON retention is a distinct policy
decision: byte-slicing would corrupt JSON, and replacing subtrees would invent a semantic projection
format. M12 may record the output as already addressable when auditing remaining content bounds;
it must not silently truncate it.

**Deferred global thread-identity invariant.** Runtime entries and all of their primitives are keyed
by `ThreadId`, while persisted thread paths are nested under a project. The intended invariant is
that a `ThreadId` is globally unique across every project, but the storage/bootstrap boundary does
not yet reject an externally constructed duplicate. Consequently, duplicate thread IDs can alias
runtime state across projects; examples include active command or tool output and the persisted
command-output ETag cache introduced in M5. M6 deliberately does not add `ProjectId` defenses to
individual runtime primitives: doing so would distribute protection for a missing identity
invariant throughout the registry. A later storage/bootstrap reconciliation change must reject
duplicate thread IDs as corruption and bind each runtime entry to its owning project. Endpoint
project/thread containment checks remain required authorization boundaries even after that
invariant is enforced, but they do not by themselves disambiguate aliased process-local state.

**Non-goals.** Lazy tool input, metadata, or error; a JSON preview or projection format; changing
MCP progress; a generic item-content endpoint; splitting MCP, dynamic, and subagent calls into new
domain variants; durable output truncation; payload migration; the journal or bootstrap transaction.

**Expected outcome.** Opening, reconnecting to, or hydrating a completed tool call transfers its
output descriptor but not its JSON value. Every advertised output remains retrievable across the
active to persisted race and `PersistenceBlocked`; absent and running output is not advertised. The
overlay fetches and releases valid JSON on demand, while live progress behavior and every non-output
tool field remain unchanged. The documented post-persistence late-completion exception advertises
nothing until M15.

---

### M7 — `RunningTasks` authority revamp

**Status:** complete. `RunningTasks` is a revisioned replacement projection for the Tasks menu and
its controls only. It no longer carries or accumulates command or tool output.

**Problem.** `RunningTasks` was two things at once: a menu projection and a second copy of
transcript output. Every command-output delta and tool-progress `Text` delta mutated task state and
advanced its revision, so a command emitting steady output republished the whole task snapshot
continuously — carrying an 8 KB tail the transcript already had. The Tasks menu and the transcript
were two authorities for the same bytes.

**Proposed change.** Remove `RunningTask.output` and stop treating command-output or tool-progress
deltas as task-state changes. Keep task identity, status, timing, process identity, after-turn
state, termination state, and stop routing. In the browser, hold task snapshots in a map distinct
from the transcript's running-command state. An empty task snapshot clears the menu without deleting
transcript rows.

Task cards remain shortcuts into the transcript authority. Clicking a card selects, expands, and
scrolls to the real command or tool row. A command card also has an `Open` action which launches
the existing live-output overlay without navigating first. If a current-turn task snapshot wins
the race with the corresponding transcript event, defer navigation or opening until the
authoritative row is registered. If an `after_turn` command belongs to a turn outside the loaded
history, progressively load ordinary authenticated history pages until that turn is rendered, then
fulfil the action. Scope the fetch to the active view generation and cancel it when the thread
changes, the task disappears, or a newer task action supersedes it. Exhausted or unreadable history
produces a visible warning; task state never fabricates a transcript row.

Live ordered events continue to supply all command output observed by an uninterrupted browser.
`LiveTurnSnapshot` remains the active-turn reconnect authority and retains its existing bounded
command-output projection. If a command outlives its persisted turn, output emitted after that turn
was appended but before a browser reconnects is not recoverable: the row resumes from persisted
content plus newly observed deltas and does not imply that the interval is complete. M15 closes
this late-completion durability gap. Completed command output remains lazily retrievable through
M5's endpoint. M7 changes neither durable content nor either output policy.

**Non-goals.** Running-output retention or addressability, changing the live reconnect snapshot,
completed command/tool output, persistence, the event journal, or transactional bootstrap.

**Expected outcome.** Output-only deltas do not advance the task revision. Task snapshots cannot
create, update, or delete transcript rows or output. Navigation and direct command opening resolve
through scoped turn/item identity to the real transcript state, continue following live deltas,
tolerate the task-before-transcript race, and load an older owning turn when necessary. Missing or
damaged history fails visibly instead of leaving an action pending forever. Command and tool stop
controls retain their existing routing and lifecycle behavior.

---

### M8 — Runtime-owned in-flight turn projection

**Problem.** M2 claimed "the in-flight turn projection" as runtime-owned and did not finish the job.
The active turn's content lives in two places today, maintained independently from the same event
stream:

- `LiveTurnState.events: Vec<AgentEvent>` inside `ThreadRuntimeEntry` — the raw log, narrowed to
  `LiveTurnSnapshot.accumulated` for reconnect. The browser folds it into rows client-side.
- `CurrentTurnItems { items: Vec<Item>, indexes: HashMap<ItemId, usize> }`
  (`registry/event_forwarder.rs:407`) — the folded item list, held as `ForwardedTurnState.items`
  (`:718`), a private field of the forwarder. Nothing outside the forwarder task can read it.

*Refactor shape* already names this: the registry "should no longer contain … three independent
applications of every agent event."

The concrete cost is that the forwarder is the only component that knows which items a turn
contains, so anything needing an item has nowhere to ask. That is why each lazy field has required
its own bespoke runtime side-map — `command_outputs` and `tool_outputs`, both keyed
`(TurnId, ItemId)`, both populated in `apply_event_locked`, both existing only because the item
itself is unreachable. A third lazy field would mean a third map.

*What the event-pipeline work already fixed, and what it did not.* When this milestone was first
written, `CurrentTurnItems` was a local variable inside `forward_events`. It is now a field of
`ForwardedTurnState`, alongside `diffs`, `item_ids_by_harness`, `live_usage`, `live_context_window`
and `persisted_context_window` — the per-turn state that was scattered is now one struct with one
`reset` (`:748`). That is the consolidation half of this milestone, done, and better than the
version proposed here. The publication half is untouched: `ForwardedTurnState` is private to a
forwarder that the project's event driver polls inside a `FuturesUnordered`, so an HTTP handler
still cannot ask it anything. The reachable read surface is still `ResolvedThreadRuntime`'s
`live_snapshot()` and `tasks_snapshot()` (`thread_runtime.rs:337`, `:347`) — whole-turn
projections, neither of which answers "give me this item."

**Proposed change.** Publish the item projection into the runtime authority as it is built. The
forwarder keeps `ForwardedTurnState` and keeps folding into it; each fold also writes through to
`ThreadRuntimeEntry`, beside `live`, where the existing runtime reads already live.

*Publish, do not share.* `AGENTS.md` now says entity-local state belongs on its authority or an
authority-owned component, forbids adding a peer owning map keyed by project or thread identity,
and requires an `ENTITY-AUTHORITY-OWNER` or `ENTITY-AUTHORITY-EXCEPTION` annotation on the ones
that exist. So the shape is not "hand the forwarder's struct to the runtime" and not "add a map of
in-flight items next to the runtime": it is the same write-through the forwarder already performs
for `LiveTurnState`, with the item projection landing on `ThreadRuntimeEntry`, which is
authority-owned. Convenience is explicitly not a reason for an exception, and this milestone does
not need one.

*Record an item when it starts, update it when it completes.* Today the only writer is after the
fact: `self.turn.items.upsert(item)` on `AgentEvent::ItemCompleted` (`event_forwarder.rs:1713`).
The `ItemStarted` arm (`:1661`) exists but records nothing — it only forwards sub-agent links to
the driver. That is why a running item is unreachable, and `ItemStarted` is where the
agent-composed fields arrive: `CommandExecutionStart.command` carries the full command line
including any heredoc, and `ToolCallStart` carries `input` and `metadata` whole. Record the item in
that arm. Neither arm consumes deltas; output is not part of this projection.

*Turn start.* `self.turn.items.rebuild_indexes()` on `TurnStarted` (`:1650`) becomes a runtime
turn-start reset as well as a local one.

*Persistence still takes only completed items.* `items: self.turn.items.take()` (`:1895`) stays
where it is, in `complete_forwarded_turn`, so lease and persist ordering are unchanged — and it
still filters to items that reached `ItemCompleted`. A turn ending while a command still runs must
persist exactly what it persists today, which is that item absent; that is the durability gap M15
owns. Recording an item earlier must not change what reaches disk.

*The front-insert exception is gone, and stays gone.* An earlier version of this milestone asked to
preserve `upsert_first`, which existed so a synthesized sub-agent prompt could lead its turn.
That method no longer exists. The prompt is not an item any more: it rides `TurnContext.user_input`
into `Turn.user_input` at completion (`:1894`), which is where a turn's prompt belongs. Do not
reintroduce a front-insert path.

*Add one read.* `live_item(thread_id, turn_id, item_id) -> Option<Item>`, served from the index the
projection already maintains. This is what M9 consumes.

*Keep heavy work off the lock.* The fold is a clone and an index update; that belongs under the
entry mutex. Serialization and hashing do not — `prepare_item_output` runs in `spawn_blocking`
before the lock precisely so M5 and M6 do not hash on the reactor, and that must not regress.

**Non-goals.** Changing `LiveTurnState.events` or `LiveTurnSnapshot.accumulated`: the snapshot
replays narrowed events and the browser folds them, and that stays exactly as it is. Moving
`ForwardedTurnState` itself, or any other field on it — `diffs`, `live_usage` and the context-window
fields stay task-owned, and only the item projection is published. Deleting `command_outputs` or
`tool_outputs` — M9 may retire them once items are reachable, but not here. Any HTTP endpoint, any
wire change, any change to `giskard-persist`. Any new owning map keyed by thread or turn identity.

**Expected outcome.** A running item is readable by identity from the runtime authority from the
moment its `ItemStarted` is applied, and the forwarder is no longer the only component that can
answer what a turn contains. For the same event stream, persisted turns are byte-identical to
before, including a turn that ends while an item is still running, and a turn's prompt still
arrives as `Turn.user_input` rather than as an item. `live_item` answers by identity without
scanning. No new owning map keyed by thread or turn identity is introduced, and no
`ENTITY-AUTHORITY-EXCEPTION` annotation is added. Nothing new runs under the entry mutex beyond the
upsert, and no hashing or serialization moves onto the reactor.
---

### M9 — Item endpoint

**Problem.** Every wire message must carry an item's fields whole, because there is nowhere else a
client can get one. Three lazy routes already hang off an item, and a fourth off its turn:

```text
…/turns/{turn_id}/items/{item_id}/command-output
…/turns/{turn_id}/items/{item_id}/command-output-links
…/turns/{turn_id}/items/{item_id}/tool-output
…/turns/{turn_id}/diffs/{diff_id}
```

`…/items/{item_id}` is the literal common prefix of the first three and is not served. The count is
the argument: M5 alone added two of them, so the growth is not hypothetical — every future lazily
delivered field needs a fifth route, then a sixth. Worse, because a wire preview would be the
client's *only* copy, M6 had to refuse a JSON preview language outright: a truncated serialization
is not JSON and a recursive projection changes types. That constraint exists only because there is
nothing to expand to.

One field is already paying for it. `AgentMessage.text` and `Reasoning.text` both cross the wire
whole in every history page and every bootstrap baseline, and no milestone bounds either — M4 and
M5 listed reasoning text as a non-goal, and M12 classifies both *accepted inline* on the strength
of being small in practice, which a reasoning trace breaks by construction.

**Proposed change.** Serve
`GET /api/projects/{project_id}/threads/{thread_id}/turns/{turn_id}/items/{item_id}` as
`application/json`, returning the item's own fields **in full** — no previews, no truncation. HTTP
has no frame limit and does not compete for outbox capacity, so returning references here would
turn one round trip into several for content the caller already asked for.

*What is excluded, and why it is not about size.* Command output has its own representation
(`text/plain; charset=utf-8` with the `X-Giskard-Output-*` headers), tool output is exactly
`application/json`, and a captured diff has its own identity and 409-superseded protocol. Nesting
any of them here would stringify them or lose the content type. The response therefore carries
their **descriptors** — `diff_id`, the command and tool output descriptors — and a client wanting a
body follows to the route that already serves it.

*Resolution mirrors M5 and M6 exactly.* Validate project/thread/turn/item containment before
lookup. Resolve an active turn from the runtime through M8's `live_item`, then fall back to a
targeted read of that item in the selected immutable turn payload; never scan turns. Return the
same 404 for an unknown item, a cross-container identity, or an item not yet available.

*Serve an item from the moment it starts, not from when it completes.* Restricting this to
completed items would defeat the milestone: `CommandExecutionStart.command` and `ToolCallStart`'s
`input` and `metadata` all arrive at `ItemStarted`, so previewing any of them would leave the client
unable to reach the full value for as long as the item runs — which for a long build or a slow MCP
call is exactly when someone is reading it. M8 records items at `ItemStarted` for this reason.
Never synthesize an item that has no `ItemStarted`.

*No content identity is required, and serving running items does not change that.* The large fields
are immutable from `ItemStarted`: `command`, `input` and `metadata` never change once set. What does
change while an item runs — `status`, `exit_code`, `duration_ms` — is small and already pushed live
on the wire. So nothing pairs a fetch with a specific prior state: returning current state is
correct, and newer-than-the-preview is the desired outcome rather than a race. This is the real
difference from the diff endpoint, whose body is replaceable and therefore needs `DiffId` and a
409.

*Preview reasoning in completed turns.* This is the one previewing case the milestone builds,
because it is the field with no other bound. `WireItemPayload::Reasoning` passes its `text` through
whole, so every history page and every bootstrap baseline re-sends the entire trace for content the
reader may never open. A trace grows with the thinking budget and with nothing a person chose,
which is exactly the assumption *accepted inline* cannot carry.

Give it a bounded descriptor carrying a head prefix, the prefix byte count, and the total byte and
line counts. The budget is **1 KiB**, named separately from the command-output and tool-output
budgets so it can be tuned without a protocol change. `bounded_preview` is already head-oriented,
which is what prose wants — unlike command output, whose tail is the interesting end.

*Why 1 KiB rather than zero.* Collapsed reasoning rows landed separately in `6c1fd2f`, and the
collapsed row is not an empty box: it shows a summary line that `reasoningSummaryText`
(`app.js`) derives **client-side from the head of the note's own text** — the first non-blank line,
Markdown markers stripped, capped at 140 characters, falling back to the literal string
`"Thinking"` when there is no text. A zero-byte reasoning payload would therefore turn every
collapsed row in history into an unlabelled `"Thinking"`, which is a visible regression against a
UI that already works. The preview exists to feed that summary line, so its floor is a
requirement, not a tuning choice: **the prefix always contains the first non-blank line whole**,
even when that line alone exceeds 1 KiB.

**Agent text is deliberately not previewed.** It is unbounded in the same formal sense, but the
cases separate on what the reader does with each. An agent message is the transcript's primary
content: it is rendered expanded, it is what someone scrolled back to read, and a preview would
put an expand button on the thing they came for while saving bytes they were about to fetch
anyway. A reasoning note is collapsed by default and mostly never opened, so its bytes are paid on
every page and read on almost none. Agent text stays *accepted inline*, and M12's measurements —
not this milestone — decide whether that holds.

Four constraints the implementation must respect:

- *Completed turns only.* Live `ItemDelta::Text` continues to stream unchanged and the browser keeps
  accumulating it. Previewing applies where nothing streamed: history pages and the bootstrap
  baseline.
- *A completed turn must not visibly shrink.* A browser that watched the turn live holds the full
  text; when the same turn arrives again from history carrying a truncated descriptor, keep the
  longer of the two, exactly as `mergeRunningOutput` already does for command output. Otherwise the
  row collapses under the reader at turn completion.
- *The row copy button must not silently copy a prefix.* `renderItemBody` sets
  `msg.dataset.copyText = p.text` for `reasoning` rows, and the copy button copies that string
  verbatim. Today it yields the whole note even while the row is collapsed, which is the behaviour
  `6c1fd2f` documents. With a preview in `text` it would quietly hand the reader a truncated note
  and no indication that it did. Copy must fetch the item, or wait on the same fetch expanding
  performs.
- *Truncated Markdown renders.* Reasoning text is Markdown, rendered to sanitized HTML server-side,
  so a byte-cut prefix can leave an unclosed fence or a half-written link. Cut on a line boundary at
  or below the budget — except for the first non-blank line, which the floor rule above keeps whole
  whatever its length. The renderer already degrades to plain text, but a preview that is one cheap
  fetch from the whole value does not need to round-trip and should not look broken.

*What the landed UI leaves to do.* `6c1fd2f` gives the row its toggle, its summary line, its
default-collapsed behaviour and its remembered per-row choice — the presentation half is done and
this milestone must not redo it. What it does not do is fetch: collapsing hides the body while
leaving the full text in the DOM, so expanding costs no round trip today. The transport half is
therefore the whole of M9's work here — expanding a row whose body is a prefix fetches the item
from this endpoint, renders the full note, and thereafter behaves exactly like a row that never
was truncated, including its copy button. A row that already holds the full text must not fetch.

*What else it unlocks, deliberately not built here.* The same shape makes previewing
`ToolCall.input`, `.metadata` and `CommandExecution.command` viable, and lets M12's inventory carry
fewer accepted-inline assumptions. Those wait for M12's measurements to say whether they are worth
it.

**Non-goals.** A generic field-addressing framework — this is one resource with one URL, and M6's
non-goal against a generic item-content abstraction still stands. Previewing any field other than
reasoning text: `AgentMessage.text`, `ToolCall.input`, `.metadata` and `CommandExecution.command`
all wait for M12's measurements. Re-doing the collapsed reasoning row: its toggle, summary line,
default state and remembered choice landed in `6c1fd2f` and are not in scope. Any change to live
delta streaming. Caching, `ETag`, or conditional requests. Running command output or tool progress
— those stay on their own routes and the live stream. Durable truncation: the persisted text is
untouched, and only its wire projection is bounded.

**Expected outcome.** One route serves any item that has started, running or complete, and adding a
lazily previewed field needs no new route. The response never duplicates bytes already reachable at
the output or diff URLs, and carries the descriptors needed to reach them. Containment and 404
behaviour match the two existing item routes exactly. An item with no `ItemStarted` is never
synthesized.

A completed turn carries a 1 KiB reasoning prefix that always includes the note's first non-blank
line, plus byte and line counts; no collapsed row in history reads `"Thinking"` that did not read
it before. Expanding such a row fetches the item and renders the full note, and the row's copy
button never yields a prefix without saying so. Agent text still crosses whole. A browser that
watched the turn live never sees its text shrink when the same turn returns from history. Persisted
text is unchanged, and live delta streaming is unchanged.

---

### M10 — Cancellable, generation-owned subscribe

**Problem.** `handle_client_msg` awaits the whole subscribe inline. A bootstrap in flight cannot be
stopped, and the server has no identity for "which subscribe attempt is this", so a late message
cannot be rejected at the boundary — only filtered by the browser after it arrives.

Concretely: open a thread with several turns of cold history, then immediately click another
thread. The first thread's `recompute_aggregates`, history read and live-snapshot read all still
run, and its `ThreadState`, `HistoryDelta`, `LiveTurnSnapshot` and `RunningTasks` are all still
sent. The browser discards them by thread id afterwards.

**Evidence.** The mechanism is verifiable in `routes.rs` — the reads are awaited inline and nothing
cancels them — but no user-visible failure has been reported against it. What is certain is the
waste: reads that cannot be used, and messages the browser exists to throw away. What is not
established is that any user has seen a wrong transcript because of it. Treat this milestone as
removing known-dead work, not as fixing a reported bug, and size it accordingly.

**Proposed change.** Give each subscribe a server-owned, monotonic subscription generation. Run the
bootstrap in a task the connection owns and keys by that generation, instead of inline in the
receive loop. A superseding subscribe for the same thread, an unsubscribe, or a connection close
cancels the in-flight task and drops its unsent output. Stamp bootstrap-phase messages with their
generation so a stale one is rejected where it is received. The message set and its ordering are
unchanged.

**Non-goals.** The transaction envelope and the journal (M13). The consistent cut (M11). Changing
which messages a bootstrap sends. Removing the browser phase flags.

**Expected outcome.** Switching threads mid-bootstrap produces no further messages for the
abandoned thread, and a superseded subscribe performs no further disk reads. A message carrying a
stale generation is rejected with a log rather than applied. `handle_client_msg` no longer awaits
history and live-snapshot reads.

---

### M11 — Consistent bootstrap cut

**Problem.** Subscribe reads three times at three different instants: `recompute_aggregates` for
metadata, `load_turns_after` or `load_history` for history, and `live_snapshot` for the in-flight
turn. Nothing holds them together, so the three answers need not describe the same moment.

Concretely: a turn completes between the history read and the live-snapshot read. The subscription
is registered before the reads — deliberately, so nothing is lost — which means the client also
receives the live `TurnCompleted`. The same item can therefore arrive twice: once inside
`HistoryDelta`, once as an ordered event. The browser absorbs that today with `renderedItemIds`,
`renderedHarnessItemIds` and four phase flags spread across 34 sites. The duplication is the
symptom; the missing cut is the cause.

There is now a second, milder version of the same gap, and it is worth naming because it is *not* a
defect. `LiveTurnState::append` drops every earlier `TurnUsageUpdated` when a new one arrives
(`runtime_live.rs`), so a browser watching a turn live receives every usage report in order, while
one that reconnects mid-turn receives exactly the last. That is correct — usage is a replacement
value that happens to travel on the ordered lane, and replaying twenty stale token counts would be
worse than useless. But it means the snapshot is deliberately not a replay of the stream, which is
the second compaction in the live buffer after `compact_command_output_deltas` and the first that
drops entries outright. Whatever this milestone records as the cut must therefore describe *which
events the snapshot stands for*, not *which events were appended* — a watermark that assumes the
two are the same will be wrong for usage first and for the next coalesced replacement after it.

**Proposed change.** Add the consistent `load_history_snapshot` and `load_history_from` reads to
`PersistStore` — already specified by the storage plan and deferred until a consumer existed, and
this is that consumer. Take the history baseline and the live-turn boundary at one point so the
pair is a cut rather than two reads. Record the cut's watermark so a later milestone can express
"everything after this" exactly. The messages sent are the same; only their mutual consistency
changes.

**Non-goals.** The transaction envelope, exactly-once delivery, and removing the browser phase
flags (all M13). The browser journal. The outbox. Changing which events `LiveTurnState` coalesces,
or adding a new coalescing rule — the cut must accommodate the existing ones, not extend them.

**Expected outcome.** The history baseline and live snapshot from one subscribe describe the same
instant. A turn completing during bootstrap lands in the history baseline or as an ordered event
after the cut, never in an indeterminate mixture of both. The watermark states what the snapshot
represents, and stays correct for events the live buffer coalesces rather than replays. The store
exposes the consistent reads M13 consumes. Browser dedupe is unchanged and still present — M13 is
what removes it.

---

### M12 — Content inventory and outbox instrumentation

**Problem.** This milestone previously proposed defining "maximum encoded-event admission" — a
ceiling on how large a single event may be. That was the wrong instrument, for three reasons.

A ceiling already exists — two of them now. `MAX_WS_MESSAGE_BYTES` (`routes.rs:61`) is 64 MiB,
applied to the socket through `max_message_size` and `max_frame_size`; it was sized for inbound
attachment uploads — it sits directly beneath `MAX_ATTACHMENT_BYTES` and
`MAX_ATTACHMENT_HTTP_BODY_BYTES` — and has never been revisited. `CODEX_MAX_FRAME_BYTES`
(`transport.rs:28`) is also 64 MiB, bounding each Codex stdout frame at ingest. Neither number was
derived; the second is 64 MiB because the first is. That is what happens to a number chosen by
feel, twice.

The ingest cap is worth separating from the egress one, because it is the better instrument. It
does not truncate: exceeding it closes the transport fatally, which is a fail-loud guard on a frame
that could not be valid anyway, not a silent shortening of agent content. That is the only kind of
limit this plan endorses — and note where it sits. It is the harness's own boundary, the one place
described in *Bounded, addressable, truncated* where a limit can be imposed at all.

The event-pipeline work also settled the units question by example: `EVENT_LOG_RETAIN_LIMIT` bounds
the harness event log by **entries**, not bytes, with a `Gap` marker when eviction happens. A
budget that counts things you can enumerate needs no size model.

A ceiling is a symptom guard. The invariant actually wanted is *no message carries agent-unbounded
content inline*, and that is delivered by making content addressable, which M4 through M7 already
did for diffs, command output, tool output and task output. If the inventory is complete, no
element can be large, so the ceiling catches nothing it was not already impossible to produce.

And an invented threshold manufactures a failure case that then needs a policy. That is how
`NeedsResync` came to be specified: a limit created an "overflow", and the overflow needed a state
machine. The browser journal does not change this — a bounded ring needs a total budget and an
eviction rule, not a per-entry maximum.

What genuinely remains is smaller than it looks, and mostly needs proving rather than moving.
Still inline and agent-driven today: `ToolCall.input`, `ToolCall.metadata`, `ToolCall.error`, and
`CommandExecution.command`. Of those, the first, second and fourth are rendered on the row or in
the overlay, so moving them behind a fetch would break the thing they exist to show — and M6
already recorded that observed tool inputs are small in practice. They are *accepted inline*; what
is missing is the measurement that says so, and anything enforcing it.

`ToolCall.error` is the exception. It is provider text with a natural tail, so unlike JSON it can
carry an explicit omission marker, and an MCP server is free to return an arbitrarily long one.

**Proposed change.** Three concrete pieces of work, no thresholds to invent.

*One retention policy.* Bound `ToolCall.error` with the existing
`giskard-persist::preview::bounded_preview` primitive and an explicit omission marker, in the same
UTF-8-safe form M5 established. This is the last actual truncation in the plan; no second algorithm
is introduced.

*The inventory, enforced by a test.* Enumerate every field of every wire element and classify it
bounded, addressable, truncated, or accepted inline per *Bounded, addressable, truncated*. Record
the measurement behind each accepted-inline entry against the current corpus — the byte
distribution actually observed, not an assertion. Reasoning text is no longer a candidate for that
list: M9 previews it in completed turns, so it is *addressable*. Agent text still is one, and it is
the entry whose measurement matters most — M9 left it inline on a reader-behaviour argument, and
this is where that argument is either confirmed by the corpus or overturned. The same question
stands for `ToolCall.input`, `.metadata` and `CommandExecution.command`. Land the table with a test that fails when a wire
field is added without a classification, so drift is caught where it is introduced rather than by a
constant nobody maintains.

*Outbox instrumentation.* Give the outbox a total byte and entry budget, and log the sequence range
and byte counts whenever it *would* have overflowed. Instrumentation only: no eviction, rejection
or resync behaviour changes here. This is queue capacity, which is a different question from
message size, and it is what lets M14 choose a policy from observation instead of assumption.

**Non-goals.** Any per-message size ceiling — the socket already has one and this milestone
deliberately does not add a second. Command and tool output, owned by M5 and M6. Making
`ToolCall.input`, `ToolCall.metadata` or `CommandExecution.command` addressable: they render
inline, and the inventory records why. Capping `FileChange.changes`: a file list whose completeness
is its meaning is never truncated, and with no frame ceiling there is no "does not fit" case to
answer. A second UTF-8 truncation algorithm. Any eviction, resync or connection policy — M14
decides those from this milestone's data.

**Expected outcome.** Every wire field is classified, each accepted-inline entry cites the
measurement that justifies it, and a new field cannot reach the wire unclassified without failing a
test. `ToolCall.error` truncates explicitly or not at all. The outbox has a budget and reports a
real would-have-overflowed count from real use. No new size constant exists.

---

### M13 — Journal and exactly-once bootstrap transaction

**The largest remaining milestone. Watch it — and check it is still needed.**

**Problem.** The browser applies each bootstrap message the moment it arrives. There is no envelope,
so there is no instant at which a bootstrap is complete, nothing to roll back, and no way to state
that an item appears exactly once.

Concretely: a bootstrap that fails after `HistoryDelta` leaves metadata and history applied with no
live turn, which the browser cannot distinguish from a thread that genuinely has no active turn.
And because delivery is at-least-once by construction, four phase flags across 34 sites plus two
rendered-id sets exist only to absorb the resulting duplication.

**Re-scope before starting.** The original case for this milestone was byte-chunking: a single turn
record could be arbitrarily large, so the bootstrap base64-chunked it. M4, M5, M6 and M7 removed
that entirely — diffs, command output and tool output are addressable, task output is gone, and M1
moved pagination off the socket. M10 and M11 remove two of the three remaining justifications.

So the first deliverable is a measurement, not a formality: with a cancellable generation-owned
subscribe and a consistent cut in place, determine whether the phase flags can be deleted without a
transaction protocol. If they can, this milestone shrinks to the journal alone and the envelope is
dropped. Do not begin the protocol work before answering that.

**Proposed change.** The shared bounded per-thread event journal with a snapshot watermark pinned
at the live cut. The journal holds bounded records and references to addressable payloads, never an
inline agent-sized body — see *Bounded, addressable, truncated*; settle this on its first commit,
because retrofitting it means rewriting the journal.

If the measurement above justifies it: a staged bootstrap transaction of `BootstrapStart`,
independently parseable typed semantic elements, and `BootstrapCommit`, replacing the four browser
phase flags and the split snapshot messages. Elements carry their thread, subscription generation
and section ordinal; the browser stages parsed objects without touching authoritative state,
validates section identity, ordinals, counts, generation and commit completeness, then applies in
one pass. Do not concatenate, base64-encode, or reconstruct a serialized whole-bootstrap blob. An
element large enough to be worth deferring uses a bounded descriptor and lazy retrieval; there is
no generic byte-fragment fallback. Bound both individual encoded messages and total staged
bytes/items, and fail the transaction explicitly rather than partially applying or hiding items.

**Non-goals.** The class-aware outbox (M14). Amendments (M15). Generic base64 or byte-sliced
application messages. Any protocol work not justified by the measurement above.

**Expected outcome.** An `ItemDelta` before or after the live cut appears exactly once. A cancelled
or failed bootstrap applies nothing. The four browser phase flags are gone and one apply path
remains. If the envelope was not built, the same flag count is reached without it and this outcome
is met by M10, M11 and the journal alone.

---

### M14 — Class-aware outbox

**Problem.** Producers and socket capacity are not separated by an explicit policy. Persistence,
harness and forwarder producers must never await a slow client, and the outbox must be finite, but
what happens at the boundary is currently undefined rather than chosen.

Different message classes want different answers, which is why one queue cannot serve them all.
Revisioned replacements — thread metadata, the runtime overview, running tasks — can coalesce to
newest-by-key at no cost, because the next one is complete and authoritative. Ordered events cannot:
an `ItemDelta` carries no expected-previous, so a dropped one leaves a permanently wrong row that
nothing downstream can detect.

**Proposed change.** The connection-owned delivery pump with per-class admission, coalescing
replacement state, and a control reserve. Producers never await socket capacity.

Decide the ordered-lane overflow policy from M12's measurements, not in advance. If the
would-have-overflowed log never fired in real use, the answer is to close the connection and let
the existing reconnect path rebuild — one line of policy, and for one user rebuilding one
connection's view costs almost nothing. Only if it fired, and the cause was a genuine stall rather
than a sustained rate mismatch, build per-subscription `NeedsResync`: a resync is larger than the
deltas it replaces, so it cannot help a client that is simply too slow, and shipping it on that
premise would add a state machine for a failure mode nobody observed.

**Non-goals.** Anything inside the bootstrap transaction beyond the resync entry point, if one is
built at all. The content inventory and its one retention policy (M12).

**Expected outcome.** Replacement state coalesces to the newest revision under pressure and no
producer awaits socket capacity. The ordered-lane overflow policy is documented with the evidence
that chose it. If `NeedsResync` was built, overflow marks one subscription, logs once, and resyncs
on the same socket while other traffic continues.

---

### M15 — Late item completion (durable amendments)

**Problem.** A command or tool can finish after its interrupted turn was already appended. The late
event has no durable coverage, so the persisted turn is wrong from the moment it is written until
the amendment that never comes.

Concretely: a command outlives its turn, the turn is appended, the browser disconnects, and on
reconnect the row shows the command still running with output frozen at append time — output
emitted in between is not recoverable from history, from the live snapshot (cleared at turn
completion), or from the task projection (M7 removed it). M5 and M6 both carved out this same case
as an explicit exception.

**Its own landing, its own review.** This is a durable-format behaviour change.

**Proposed change.** Append a settled command or tool item to its turn's payload file — the
per-turn format admits this without a format bump. Supersede the bounded turn record so a
reconnecting client can detect the change. Add a durable clock the browser compares against, the
persistence-recovery path for a failed amendment write, and the reconnect rule that turns an unseen
amendment into a `CursorReset`. Apply the existing command normalization and M6 tool-output
projection before publishing or amending, so their lazy bodies follow the same availability rules
as ordinary completion.

**Non-goals.** Any change to the runtime registry, journal, bootstrap, or delivery beyond the
coverage token an amendment event needs.

**Expected outcome.** A command or tool completing after its turn was persisted survives journal
eviction and a server restart. A client that already rendered the turn learns of the change. An
amendment write failure is recoverable rather than silently lost. The persisted turn is accurate at
every point in time, not merely after the amendment.

---

### M16 — Cleanup and budget

**Problem.** A sequence of replacements leaves residue: superseded stores, protocol variants,
browser flags, tests and documentation that no longer describe the system. Left in place they are
indistinguishable from live code to the next reader, and the complexity budget this plan set for
itself cannot be honestly measured while they remain.

**Proposed change.** Remove the obsolete stores, protocol variants, browser flags, tests and
documentation left by M2–M15. Re-measure and report the complexity budget after cleanup. Run unit,
integration, browser E2E, formatting, lint, and the full workspace suite.

**Expected outcome.** The measured protocol and browser counts meet the budget below.

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

### Lazy heavyweight content and retention

- Opening or reconnecting to a live or persisted turn transfers diff descriptors but no full
  agent-produced diff bodies.
- Full inline and structured diffs remain fetchable before and after turn completion; a fetch racing
  an update or completion cannot return content under the wrong identity.
- Item-owned and turn-level diffs use independent `DiffId`s without inventing or encoding an item
  identity; identical content can share one persisted content record without merging descriptors.
- Legacy inline-diff payloads project stable descriptors and remain fetchable without an eager
  migration; new payloads keep descriptor references and diff-content records in one atomic file.
- Switching threads or subscription generations during a diff fetch cannot open the stale result.
- Workspace Git status and `GET /api/projects/{id}/git/diff` retain their existing behavior.
- Durable completed-command output preserves UTF-8 boundaries, retains head and tail within the
  configured limit, accounts for its omission marker, and persists original byte/line counts.
- The retention configuration defaults to 128 MiB, accepts an override at or above 32 KiB, rejects
  a smaller value, and `config.example.toml` parses with the documented key and default.
- Old command items without truncation metadata remain format-1 compatible; unknown additive fields
  are ignored by older readers and no migration runs.
- Every terminal command status is normalized, including a terminal command carried by an
  `ItemCompleted` after earlier nonterminal completion. Running commands continue to append deltas,
  and a nonterminal `ItemCompleted` does not release their accumulation.
- Terminal `ItemCompleted`, reconnect, `WireTurn`, and history projections carry only the 8 KiB
  tail descriptor. Serialized WebSocket and HTTP-history sentinel tests prove that the complete
  retained representation does not leak into those projections.
- At the 32 KiB minimum, live and reloaded descriptors are byte-identical, including multibyte UTF-8
  boundaries, omission markers, and byte/line counts.
- Runtime, `PersistenceBlocked`, legacy format-1, and persisted command-output reads return the
  complete durably retained representation with exactly `text/plain; charset=utf-8` and the three
  specified truncation/count headers. Project/thread/turn/item containment is enforced; running,
  unknown, wrong-kind, unavailable, and cross-container identities return the same 404.
- Opening completed output fetches lazily; abort on selection change or close, retry, reopen,
  close-time release, linkification, copy, download, and fetch failures preserve the existing
  overlay behavior.
- Completed tool projections keep input, metadata, error, identity, status, server, and subagent
  fields inline but carry only an output descriptor. Serialized WebSocket and HTTP-history sentinel
  tests prove that the JSON output value does not leak into those projections.
- Active, persisted, legacy, and `PersistenceBlocked` tool-output reads return the same complete
  JSON value with exactly `application/json`. Project/thread/turn/item containment is enforced;
  unknown, non-tool, absent-output, unavailable, running, and cross-container identities return the
  same 404.
- Object, array, string, number, boolean, and explicit-null outputs survive persistence and lazy
  retrieval; missing output remains distinguishable from explicit null. Descriptor
  `serialized_bytes` equals the response-body length, and live and reloaded versions, `ETag`s, and
  compact bytes are identical.
- Pending/running aliases, absent and unknown status, nonterminal-to-terminal replacement, terminal
  replacement, and regression to nonterminal status update runtime authority and descriptors by the
  documented predicate. A body fetched across replacement is rejected when its `ETag` no longer
  matches the current descriptor.
- MCP progress `ItemDelta::Text` continues to render while running, does not advertise output
  availability, and is never treated as a fragment of the completed JSON result.
- A post-persistence late tool completion is logged and advertises nothing in M6; M13 coverage
  proves that both late command and late tool completion eventually amend and republish durably.
- Opening a completed tool fetches output lazily; abort on selection change or close, retry, reopen,
  close-time release, structured rendering, combined input/output copy and download, wrong content
  type, and malformed/failing responses preserve the existing overlay behavior without retaining
  fetched JSON after close. Running or absent output issues no fetch.
- M7's task projection carries no transcript output, and output-only deltas do not advance its
  revision. A current-turn task action survives the task-before-transcript race; a task whose
  `after_turn` falls outside the loaded history pages loads its owning turn before navigating or
  opening, and missing or damaged history fails visibly.
- M8's registry answers a by-identity item read for the in-flight turn, the forwarder holds no
  second copy of it, and the turn handed to persistence at completion is byte-identical to the one
  the local fold produced before the refactor. An item is readable from `ItemStarted`, a synthesized
  sub-agent prompt is still first in its turn, and a `TurnStarted` mid-flight leaves no item of the
  previous turn readable.
- M9's item route resolves running and completed items alike, matches the two existing item routes'
  containment and 404 behavior, never scans turns, and returns descriptors — not bodies — for
  command output, tool output, and diffs. An item with no `ItemStarted` is never synthesized.
- A completed reasoning note crosses as a 1 KiB head prefix that always contains its first non-blank
  line, with total byte and line counts, so no collapsed row's summary degrades to `"Thinking"`.
  Expanding fetches the item once and renders the full note; a row already holding the full text
  fetches nothing; and the row copy button never yields a prefix silently. A browser that watched
  the turn live keeps the longer text when the turn returns from history. Live `ItemDelta::Text`,
  `AgentMessage.text`, and the persisted reasoning text are all unchanged.
- M12's inventory classifies every wire field and a new one cannot reach the wire unclassified.
  Accepted-inline fields cite the measurement behind them, and remain named assumptions rather than
  falsely asserted limits.

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
- One-element and many-element bootstraps use the same start/stage/commit path.
- Bootstrap history larger than one message is staged and becomes visible only on commit.
- Missing, duplicate, unknown-section, over-budget, and wrong-generation elements leave the old UI
  authoritative and discard the staged transaction.
- A thread switch during staged or cooperative rendering cancels without leaking transcript rows,
  request/task state, notifications, or cursor changes.
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
- A bootstrap commit cannot overtake any required element or ordered suffix.

### Browser and protocol

- Full-page, delta, and cursor-reset semantic transactions remove the old phase flags and render
  exactly once.
- Pagination responses cannot be interpreted as bootstrap history.
- Two pending requests render one waiting state with both identities.
- The embedded browser and server use the one current protocol shape without compatibility paths.
- Invalid or unknown client messages close the connection and produce a diagnostic log.
- Browser E2E asserts exactly-once text and command-output DOM, not only server message counts.

## Documentation required with implementation

Update together:

- `specs/giskard-specification.md`: first move the authorities/clocks table into §13 and require
  every new client-visible state to name its authority, clock, and overflow class; then document
  semantic transactional bootstrap, request state, replacement overview, and backpressure;
- `docs/api-endpoints.md`: lazy captured-diff, command-output, and tool-output reads,
  descriptor-only history response fields, and changed WebSocket shapes, resync, and ordering;
- `README.md`, `config.example.toml`, and the specification's configuration appendix: the retention
  key/default, user-visible reconnect/context-window behavior, and any storage-layout change;
- `crates/giskard-harness-codex/README.md`: only if adapter lifecycle or routing semantics change.

Because M5 and M6 change visible command/tool-output overlay behavior under `static/`, regenerate
and commit the README desktop and mobile screenshots with each implementation.

The specification's current instruction to coalesce deltas by keeping the latest is invalid for
append fragments such as text and command output. Replace it with snapshot coverage plus ordered
suffix or observable resync.

### Complexity baseline and budget

Three counts, with the baseline they were set against and where they stand now:

| Count | Baseline (`6907fd0`) | After M7 (`62cf3d5`) | Now (`68bcb65`) | Target |
| --- | --- | --- | --- | --- |
| `ServerMessage` variants | 13 | 11 | **11** | no more than 11 |
| Bootstrap phase-flag sites | 36 | 34 | **34** | 0 |
| `proto/src/lib.rs` + `static/app.js` lines | 10,664 | 11,288 | **11,539** | no growth on baseline |

The four flags are `awaitingInitialThreadState`, `awaitingThreadResync`,
`awaitingIncrementalResync` and `pendingLiveSnapshotReconcile`. The end state is zero of them, one
staged bootstrap transaction and one browser apply path.

The variant target was "no more than 13" and is already met with two to spare, so it has been
tightened to the measured 11: a budget you have beaten constrains nothing.

**Record all three counts in every milestone that lands, not only at the end.** The line count is
currently **+875** against its baseline, which is expected — M4 through M9 add endpoints,
descriptors and browser fetch logic, and every corresponding removal is parked in M13 and M16. The
last 251 of those lines are not this plan's at all: 163 for the collapsed reasoning row
(`6c1fd2f`) and 88 across the event-pipeline landings. They are counted anyway, because the budget
measures two files rather than this plan's own commits, and a budget that excused other people's
growth would measure nothing. Variants held at 11 through those landings even though a new
transcript event was added, because it rides the existing `Event` envelope. The instruction below
only bites if someone is looking, and a milestone that reports its numbers makes the trajectory
visible while it is still cheap to change course.

If a count moves away from its target and the milestone that lands next does not bring it back,
stop and review the design rather than replacing the criterion with a qualitative claim. M13
measures against the target when the unified bootstrap path lands; M16 re-measures the final state
after cleanup.

## Exit criteria

The work is complete only when:

- context-window restoration uses the same metadata primitive as every other visible metadata
  mutation;
- bootstrap contains one explicit transaction and no arbitrary message FIFO;
- every client-visible state class has a named authority, clock, and overflow behavior;
- no slow client can block a turn forwarder;
- the ordered-lane overflow policy is chosen from M12's measurements and documented with the
  evidence that chose it, so no queue-full branch silently creates permanent client divergence;
- one agent event is not independently reconciled by several overlapping client projections — met
  by M2 and M7 for tasks and activity, and completed by M8 for the item projection;
- the measured protocol/browser counts meet the stated complexity budget.
