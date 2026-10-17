# Live turn: bounded loading and crash-surviving persistence

## Status

Plan, not yet implemented. Written against `main` at `7eb0150`. Every file reference below is to
that revision.

Two problems were raised together:

1. **A long live turn is slow to open.** A turn with thousands of items that has been running for
   days takes a long time to load when a browser subscribes or reloads.
2. **A crash mid-turn loses the turn.** A `kill -9`, OOM, panic, or power loss during a turn leaves
   no trace of it on disk, not even the prompt. The wish is best-effort persistence of the live
   turn, in the same format as a finished turn, so that finishing a turn becomes a file move.

This document establishes what happens today, diagnoses both problems, and proposes four
milestones with their order and dependencies. The short answer to "is one a prerequisite of the
other": **neither strictly requires the other, but both stand on the same first step** — turning
the live buffer from a flat event log into a list of items. That step (L1) is done once; pagination
(L2) pages those items and persistence (P1) writes them. The one place persistence genuinely
becomes a prerequisite is the optional end state where the server stops holding completed live
items in memory at all (L4), which needs both.

## 1. How the live turn works today

**In memory, per thread.** `ThreadRuntimeEntry.live` (`crates/giskard-server/src/thread_runtime/live.rs`)
holds one `LiveTurn { events: Vec<AgentEvent>, … }` for the in-flight turn. Every event the
forwarder applies while a turn is active is pushed onto `events`
(`thread_runtime.rs:842`, `apply_prepared_event_to_entry`). Three compactions exist:

- a completed **command** drops its `CommandOutput` deltas and earlier completions of the same item,
  and the completion's output is replaced by a bounded preview descriptor;
- a completed **tool** drops earlier completions and its output value;
- `TurnUsageUpdated` keeps only the latest, and command output deltas are head/tail-compacted past
  16 KiB (`compact_command_output_deltas`).

Nothing compacts **text deltas**. Every `ItemDelta::Text` for agent messages and reasoning notes
stays in `events` forever, alongside the `ItemCompleted` that carries the same text in full. Each
of those compactions is a `retain` over the whole vector, so per-event cost grows with the turn.

**Alongside it, in the forwarder.** `ForwardedTurnState.items: CurrentTurnItems`
(`registry/event_forwarder.rs:304`) keeps the completed items in display order with an id index.
This, not the live buffer, is what becomes `Turn.items` at commit.

**On subscribe.** `ws.rs:1380–1500` sends `ThreadState`, then a bounded `HistoryDelta` (the last
`[history] initial` completed turns), then `LiveTurnSnapshot` built by `LiveTurnState::snapshot`,
which clones *every* buffered event, converts it to its wire form, and serializes the lot into one
WebSocket text frame. The browser (`app.js:4673`, `renderLiveTurnSnapshot`) replays every event
through `handleEvent`.

**On completion.** The forwarder builds a `Turn` from `CurrentTurnItems` and calls `persist_turn`
(`event_forwarder.rs:2135`), which writes the whole payload file atomically and appends one index
record (`store.rs:1300`, `append_turn_unlocked`). Only then is the live buffer cleared
(`settle_completed_turn`). Until then **nothing about the turn is on disk**: not the prompt, not a
single item, not its model or start time.

**On graceful shutdown.** Registry shutdown stops each harness (15 s budget), the event stream ends,
and the forwarder's `handle_stream_error` (`event_forwarder.rs:1209`) synthesizes an
`Interrupted` completion and persists the turn with the items it has. So `SIGTERM`/Ctrl-C already
preserves the turn, within the 10 s background drain. What is lost is the unclean exit.

## 2. Why opening a long live turn is slow

The cost is multiplied at three stages, and the biggest factor is the first:

1. **The snapshot is the whole event log, deltas included.** For a turn of N items with D text
   deltas each, the frame carries roughly N × (D + 2) events and the text twice (deltas plus the
   final item). A few thousand items with a few dozen chunks each is hundreds of thousands of
   events and tens of megabytes of JSON, built by cloning the vector under the runtime entry's
   mutex. Nothing caps it on the way out: `MAX_WS_MESSAGE_BYTES` (64 MiB, `ws.rs:41`) configures
   tungstenite's `max_message_size`/`max_frame_size`, which bound **incoming** messages only, so
   an oversized snapshot is sent whole and the browser pays for parsing it. The heavyweight item
   bodies are already kept out of it: completed command output rides as an 8 KiB tail preview,
   tool output as a byte-count descriptor, and diffs as descriptors, each fetched on demand through
   the existing item routes, which resolve live items from the runtime before the payload. What
   stays inline is agent and reasoning text and request metadata — the "accepted inline" set.
2. **The browser replays every event synchronously.** `appendStream` (`app.js:7763`) does
   `body.textContent += text` and then sets `transcript.scrollTop`, which forces a layout pass
   **per delta**. For a reasoning row it also re-derives the summary line per delta. Then
   `finalizeStreamedItem` renders Markdown for the completed item it just streamed into.
3. **Nothing is bounded by item count.** Even without deltas, a snapshot of thousands of completed
   items renders thousands of rows before the page is usable, and the same turn, once completed,
   arrives in `HistoryDelta`/`GET …/history` as one `WireTurn` with every item inline, so the
   problem does not go away when the turn ends.

Stage 1 is a server-only fix with no wire change and removes most of the volume. Stage 3 is a wire
and UI change. Stage 2 mostly disappears once 1 and 3 land; the remaining per-delta reflow is a
small browser fix that belongs with L1.

## 3. Why a crash loses the turn

By design (spec §5.4, H5), persisted state advances only on `TurnCompleted`. The live buffer and
`CurrentTurnItems` are process memory. A clean stop is already covered (§1). An unclean one loses:

- the user's prompt (held in the forwarder's `TurnContext` only);
- every completed item, and the model, mode and start time the index record would need;
- the live token usage (`live_usage`), so the ledger never sees what the turn consumed.

After restart the thread looks as if the prompt was never sent.

## 4. Design principles carried over

These are the existing rules the milestones must keep, from `AGENTS.md` and spec §5.4:

- A payload file is written whole and atomically at commit and afterwards only **extended**;
  reads are best-effort, one bad record costs that record (LA5).
- A turn commits **payload first, index last**; an unreferenced payload is invisible and harmless.
- `history.jsonl` records are strictly bounded; agent-driven content lives in the payload.
- Every on-disk file states its own format in its header.
- The live snapshot and ordered events are the only live-output authorities (TK3); task snapshots
  never create rows.
- Heavyweight content is **addressable**, never silently truncated
  (`docs/thread-state-and-bootstrap-reconciliation-plan.md`, "Bounded, addressable, truncated").
  Items of a turn are such content: the bounded thing is how many ride inline, not the item.

## 5. Milestones

### L1 — An item-centric live buffer (server only, no wire change)

**Status: implemented in [PR #306](https://github.com/marmeladema/Giskard/pull/306)** (spec 1.100, LB1–LB3); landed once it merges.

Detailed plan: [`live-turn-pagination-and-persistence-plan/l1-item-centric-live-buffer.md`](live-turn-pagination-and-persistence-plan/l1-item-centric-live-buffer.md).

Replace `LiveTurn.events: Vec<AgentEvent>` with a structure keyed the way everything downstream
consumes it:

```rust
struct LiveTurn {
    turn_id: TurnId,
    user_input: Option<UserInput>,
    /// Arrival order: an item's position is where it was first seen (start, first delta, or
    /// completion, whichever came first); a request sits where it arrived between items.
    entries: Vec<LiveEntry>,
    slots: HashMap<ItemId, usize>,
    latest_usage: Option<AgentEvent>,
    /// Latest DiffUpdated per path, in first-seen order.
    diffs: IndexMap<PathBuf, AgentEvent>,
    /// TurnStarted, Error, Notice: bounded in practice, kept in order.
    other: Vec<AgentEvent>,
    // unchanged:
    command_output_descriptors, tool_output_descriptors, resolved_approvals, resolved_server_requests,
}

enum LiveEntry {
    Open {
        start: Option<ItemStart>,
        kind: Option<ItemKind>,
        text: String,          // coalesced ItemDelta::Text
        output: String,        // coalesced CommandOutput, head/tail-compacted past 16 KiB as today
    },
    Completed {
        start: Option<ItemStart>, // kept for commands/tools so replay still emits ItemStarted
        item: Item,               // output already replaced by preview / stripped, as today
    },
    /// ApprovalRequested, ServerRequestReceived or ServerRequestResolved, kept in place so the
    /// replay draws the card where the live transcript drew it.
    Request(AgentEvent),
}
```

Rules:

- `ItemStarted` creates an `Open` entry at the tail (or fills `start` on an existing one). A delta
  for an unknown id creates an `Open` entry with no start, as the browser already tolerates.
- `ItemCompleted` replaces the entry **in its slot** with `Completed`, discarding the coalesced
  deltas: the final text is in the item. This generalizes the command-output rule to text. A
  repeat completion for the same id replaces in place (spec §4.4 upsert rule).
- Deltas append to one `String` per open item instead of one event per chunk.
- Snapshot expansion, in `entries` order: `Completed` → `[ItemStarted?] ItemCompleted`;
  `Open` → `[ItemStarted?] [ItemDelta::Text] [ItemDelta::CommandOutput]`; `Request` → the event
  itself. Then `latest_usage`, `diffs`, `other`. The browser's `appendStream` concatenates deltas,
  so one coalesced delta renders identically to the chunk sequence, and a request card is drawn at
  the same position as before.
- `live_item` and `item_events` (used by the item endpoint and sub-agent link open) become O(1)
  through `slots`; `item_events` reconstructs the raw `ItemStarted`/`ItemCompleted` events from the
  stored start and item.

L1 is behavior-preserving: the snapshot a client receives renders the same transcript as today, only
with each completed item's chunks collapsed into the item. The request-card ordering caveat belongs
to L2, where the tail bound makes it unavoidable for some cards.

Browser companion fix, same milestone: while `renderLiveTurnSnapshot` replays, suppress the
per-delta `scrollTop` write and the per-delta reasoning summary update behind a replay flag, and do
each once after the loop. This is independent of the server change and removes the forced layout
per event.

Expected effect: the snapshot drops from "every chunk ever streamed" to "one or two events per
item plus the in-progress items' current text". Per-event server cost drops from O(buffer) to O(1).

Tests: port `live.rs` unit tests to the new structure; add equivalence tests (snapshot of a
chunked stream equals snapshot of the same text as one delta; completion drops the deltas;
completion keeps the slot; command preview/tool stripping unchanged; outstanding approvals and
server requests derived identically to `outstandingApprovals`/`outstandingServerRequests` in
`app.js`). Existing WS and e2e reconnect specs (`reasoning.spec.ts`, `request-state.spec.ts`,
`approvals.spec.ts`, `server-requests.spec.ts`) must pass unchanged.

### L2 — Bounded live snapshot and live-item paging (wire + browser)

**Wire.** `LiveTurnSnapshot` gains:

```rust
/// Completed items of this turn that precede `accumulated` and were not sent. 0 is omitted.
omitted_items: u32,
/// Cursor for `GET …/items?before=`: the oldest item id that *is* in `accumulated`.
oldest_item_id: Option<ItemId>,
```

`accumulated` carries the last `[history] turn_items` entries in order, then usage/diffs/other as in
L1. Two kinds of entry ride along even when older than the tail, appended after it in their own
arrival order: every **open** item, because it is what is still changing and a reconnecting client
must see it to keep receiving its deltas; and every **outstanding** request, because the user still
has to act on it. An answered or resolved request outside the tail is not sent. This is the one
place a card can render somewhere other than where the live transcript drew it: an approval raised
before the tail replays after the tail's items. Within the tail, order is preserved.

Spec §13.6.1 requires every new wire read to name its authority, clock and delivery class: this is
the **Active transcript** authority read through a bootstrap/page lane, like completed history is.

**HTTP.** New authenticated route, mirroring the history page:

```
GET /api/projects/{id}/threads/{thread_id}/turns/{turn_id}/items?before=<ItemId>&limit=N
→ { turn_id, items: [WireItem], has_more }   // oldest-first, limit clamped 1–100
```

Resolution follows the item endpoint (IE2): the live runtime first (page `entries` by slot, completed
entries only), then the persisted payload (page `items` by index), then the runtime again across the
commit race; unknown turn or cursor → 404. Pages use the history projection
(`WireItem::from_persisted_item`: reasoning as 1 KiB preview, outputs as descriptors), because a
paged item is scrollback, the same thing a history page delivers.

**Reasoning preview in the snapshot.** The tail's *completed* reasoning items use the same 1 KiB
head preview (RP1, `WireItem::from_persisted_item`) as history pages and paged items, so a completed
note costs the same inline whether it is read from the tail, a page, or history. An *open* reasoning
note keeps its full coalesced text: it is still streaming and the browser appends to it. This
amends RP1, which today says live events and reconnect snapshots retain full text: after L2, live
`ItemCompleted` events and the item endpoint keep full text, reconnect snapshots carry the preview.
A row's copy button then yields the preview for a note that arrived by snapshot, exactly as it does
for one that arrived by history (RN1); a later milestone can make the full note fetchable through
the item endpoint if that matters. Agent message text stays inline in full: it has no preview or
fetch route anywhere, and introducing one is a separate decision the plan does not take.

Because items are appended at the tail and `before` walks backward from a cursor, a page is stable
against the turn continuing. An in-place upsert of an already-paged item reaches the browser as a
live `ItemCompleted` and lands on the existing row through the existing upsert path.

**Browser.** `renderLiveTurnSnapshot`: when `omitted_items > 0`, render the user bubble, then a
marker row ("N earlier items in this turn", a new row kind — add it to
`mobile-overflow.spec.ts`), then the tail. The scroll-up path (`onTranscriptScroll`) checks a new
`state.partialTurn = { turnId, oldestItemId, hasMore }` **before** paging older turns: while the
topmost turn is partial, it pages its items; when `has_more` is false it clears the marker and the
next scroll-up resumes `loadHistoryPage`. Paged rows render into a detached container, as history
pages do, and are inserted after the turn's user bubble and before its first item row; reasoning
rows prepended this way are collapsed (they are superseded by construction). `maybeAutoFillHistory`
treats a partial turn the same way so a short tail still fills two screens.

`[history] turn_items` (default 100) is the one new config key; name it for reuse by L3. Document
it in `README.md`, `config.example.toml`, and the route in `docs/api-endpoints.md`.

Tests: store/runtime tests for the items route (live page, persisted page, the commit race,
cursor at the oldest item, unknown cursor); WS test that a turn with more than `turn_items`
completed items yields `omitted_items` and a tail that still includes every open item; e2e: a
scripted replay prompt that emits more items than the tail and then holds the turn open (the replay
bin already has a "hold this turn open" scenario at `giskard-server-replay.rs:111`), reload, assert
the marker and tail, scroll up, assert every item present once and in order, then let the turn
complete and assert the persisted rendering matches.

### P1 — The live payload file (durable format change, own review)

**Layout.** A live turn is a file in a new per-thread directory, written in the **payload format**:

```
threads/<thread_id>/
├── history.jsonl
├── turns/<turn_id>.jsonl          # committed payloads, exactly as today
└── live/<turn_id>.jsonl           # the one in-flight turn's payload, being extended
```

A separate directory rather than a suffix in `turns/` keeps the invariant "everything in `turns/`
is a committed payload" true by construction, leaves `sweep-orphan-payloads` and
`history_validation_errors` untouched, and makes the recovery check a single `stat` of a directory
that is normally absent. A rename within the thread directory is atomic and same-filesystem.

**Birth.** On the forwarder's first owned event for a turn, write `live/<turn_id>.jsonl` with
temp + fsync + rename, containing the `turn_header` and the `user_input` record. From that moment
a crash can no longer lose the prompt. The header gains optional bounded fields the index record
will need and the payload does not carry: `model`, `mode`, `started_at`. `PayloadTurnHeader` has no
`deny_unknown_fields`, so older builds read such a header unchanged; in the committed file these
fields are redundant with the index and ignored. (Alternative: a new `kind: "live"` record. Older
builds would warn once per file; decide at review, the header route is quieter.)

**Extension.** One appended `item` record per `ItemCompleted`, with its display `index`, exactly
the record `payload_file_bytes` writes and `amend_turn_item` appends — one `write_all`, no fsync
(LA5). A repeat completion appends another record without `index`, which the reader folds last-wins
into the first slot. One appended `usage` record (new bounded kind, last-wins singleton) per
`TurnUsageUpdated`, so recovery can credit the ledger. Deltas are never written: the spec makes
them transient, and `ItemCompleted` carries the canonical form. Diffs are written at commit as
today (fold by path makes per-update appends possible later if wanted).

**Commit.** Append the `status` record and the `diff` records, `fsync`, rename
`live/<id>.jsonl` → `turns/<id>.jsonl`, then take the existing index path.
`append_turn_unlocked` already has the branch "payload exists → skip the payload write, append the
index record" for the retry case; the rename is its second producer, and that comment must say so.
Payload-first, index-last is preserved. For a large turn this is cheaper than today's whole-file
serialization — this is the "finishing a turn moves the file" the request asked for.

`parse_turn_payload` folds collections by identity and singletons last-wins, so a file built
incrementally parses to the same `TurnPayload` as the file a whole write would have produced. A
test asserts that equality for a representative turn, including a repeat completion.

**Failure policy.** The live file is best effort and never makes commit less reliable than today.
A failed birth or append is logged at `warn` once per turn (not per item), the writer is disabled
for the rest of that turn, and commit falls back to the whole-file write because the payload is
absent. A thread on the flat layout has no per-turn files and gets no live file, reported like LA4.

**Writer ownership.** The forwarder is already the single owner of `CurrentTurnItems` and the
caller of `persist_turn`; it holds a `LiveTurnWriter` obtained from the store through
`ThreadMetadataService` (birth, `append_item`, `append_usage`, `commit`). Writes go through
`tokio::fs` (the blocking pool); item completions are far too infrequent for a queue to matter.
The store registers open writers by `(ThreadId, TurnId)` under the per-thread lock so recovery can
tell a crashed file from one being written in this process.

**Recovery.** A `live/<id>.jsonl` with no registered writer is a crashed turn. Recovery runs once
per process per thread, under the thread lock, from the same gate every read and write path already
passes (`ensure_migrated`, `store.rs:699`), so it always precedes the first turn this process runs
on that thread and the recovered record lands in the index before any newer turn's. It parses the
file best-effort, builds a `Turn` with status `Interrupted` and a bounded message ("Giskard stopped
before this turn completed; N items recovered"), `usage` from the last `usage` record,
`completed_at` = the last recovered item's `created_at`, else `started_at` — never the recovery
wall clock, so a thread that crashed days ago does not jump to the top of the list (spec §5.4
repair recency rule) — and commits it through the normal path (rename + index + aggregates). A live
file that lost its prologue (no `user_input`) is quarantined to `.corrupt-<ts>` like any
unreadable payload. `giskard-admin validate` reports an unrecovered live file per thread;
`recover-live-turns` performs recovery with the data-directory lock, for operators who want it done
before starting the server.

**Documentation that must move in the same change.** Spec §5.2 (layout), §5.4 (a third appended
file; commit as rename; recovery rule), §5.5 (admin), a 1.100 changelog; `README.md` storage
layout and admin commands; `AGENTS.md` conventions ("Two files are appended to… Nothing else
appends" becomes three, and the commit rule gains the rename).

Tests: store tests for birth atomicity, append, repeat completion, usage last-wins, commit rename
plus index, fold equality with the whole-file write, torn tail, recovery (items, usage,
`completed_at` rule, recency), lost prologue, write-failure fallback, flat layout, sweep and
validate behaviour, `.deleting` rename of a thread with a live file; forwarder tests with a failing
store (`#[traced_test]` asserting the once-per-turn warning); an integration test that kills the
replay server mid-turn (or drops the forwarder without the stream ending) and asserts the thread
reopens with the recovered turn.

### L3 — Bounded persisted turns (optional, extends L2)

The same turn, once completed, still arrives whole. Give `WireTurn` the same shape as the snapshot:
`items` capped at `[history] turn_items` from the tail, `omitted_items`, `oldest_item_id`, with
the L2 route serving the rest from the payload. `renderPersistedTurn` renders the same marker row
and shares `state.partialTurn` handling. Independent of P1.

### L4 — Bounded residency (optional end state, needs L2 and P1)

With completed items on disk during the turn and a page route that already reads payloads, the
runtime can stop retaining `Completed` entries beyond the tail and the forwarder can stop
retaining `CurrentTurnItems`, reading pages and the commit's `Turn.items` from the live file. This
is the one step where persistence is a true prerequisite of pagination. Not needed for either
problem as raised; listed so the earlier milestones leave the door open (L2 pages by slot, P1
writes indices).

## 6. Order and dependencies

```
L1 (buffer) ──► L2 (bounded snapshot + paging) ──► L3 (bounded history)
     │
     └───────► P1 (live file + recovery) ───────────► L4 (bounded residency, needs L2 too)
```

Recommended order: **L1, L2, P1**, then L3 if wanted. L1 first because it is the smallest change
with the largest effect on the reported slowness and both later milestones consume its item model.
L2 before P1 because it addresses the user-visible pain; P1 is independent of L2 and can be
developed in parallel once L1 is in. P1 is a durable-format change and gets its own review, as the
reconciliation plan asked for amendments.

## 7. Risks and open decisions

- **Replay order of request cards (L2 only).** An outstanding request older than the snapshot tail
  replays after the tail instead of at its position. L1 keeps today's order exactly. Acceptable in
  my judgment, since on a reconnect the end of the transcript is where the user looks for things to
  act on; confirm.
- **Completed reasoning uses the 1 KiB preview everywhere except live events (L2).** Tail, pages
  and history all carry the preview; only the live `ItemCompleted` and the item endpoint keep full
  text. RP1 must be amended to say so. A reconnect therefore shows less of a completed note than the
  session that watched it stream, which is already true today for any note older than the live turn.
- **Default tail size.** 100 items renders quickly and exceeds most turns; tune after measuring.
- **Header fields vs. a `live` record (P1).** Decide at review; both are backward compatible.
- **fsync at commit (P1).** One fsync of the live file before the rename is required to keep the
  payload durable before its index record exists; it replaces today's fsync of the temp file.
- **Disk churn.** One small write per completed item. Thousands of items over days is nothing; a
  pathological burst is bounded by the blocking pool, not the forwarder.
- **Writer and recovery coexistence.** A crashed live file and a new turn's live file for the same
  thread can coexist for the moment between process start and the first store access; the
  once-per-process gate and the writer registry handle it, and a test must cover it.
- **The days-long open tab** accumulates DOM regardless of these changes; out of scope.
