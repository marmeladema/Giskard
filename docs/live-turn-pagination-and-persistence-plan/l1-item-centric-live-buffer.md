# L1 — Item-centric live buffer

Implementation plan for milestone L1 of
[`live-turn-pagination-and-persistence-plan.md`](../live-turn-pagination-and-persistence-plan.md).
Written against `main` at `7eb0150` (spec 1.99); every file and line reference below was checked
against that tree. Re-check them before trusting one. The milestone text in the overall plan is the
authority on scope; this document says how to land it and how to prove it landed.

L1 changes no wire type, no HTTP route, no on-disk format, and no browser-visible transcript. It
replaces the live buffer's flat event log with a per-item structure so that a reconnect snapshot
carries one or two events per item instead of every text chunk ever streamed, and so that the
per-event cost of maintaining the buffer stops growing with the turn.

## What lands

1. `LiveTurn` in `crates/giskard-server/src/thread_runtime/live.rs` keeps an ordered list of
   **entries** — one per item, plus one per non-item event — instead of `events: Vec<AgentEvent>`.
   An item's text deltas are coalesced into one string and **discarded when the item completes**;
   command output deltas keep their existing compaction rule. Item lookups become O(1).
2. `LiveTurnState::snapshot` expands the entries back into `WireAgentEvent`s. A client that replays
   the result renders the same transcript as today; the only intermediate states that disappear are
   the ones a completion overwrote anyway.
3. The browser's snapshot replay stops forcing a layout pass per replayed event: the per-delta and
   per-finalize `scrollTop` writes are suppressed during replay and done once at its end.
4. A replay-server scenario that streams an item and holds the turn open, so an end-to-end test can
   reload mid-stream and assert the coalesced replay.
5. Spec §13.6 prose and a changelog entry describing the buffer as item-centric; unit, integration
   and end-to-end tests for ordering, coalescing, and the lookups other code builds on.

## Facts the plan rests on

- **The buffer is a flat event log.** `LiveTurn.events: Vec<AgentEvent>` (`live.rs:22-40`).
  `append_with_outputs` (`live.rs:142-215`) pushes every event. Its compactions: a completed
  **command** drops that item's `CommandOutput` deltas and earlier completions (`:155-158`,
  `remove_command_output_deltas` / `remove_completed_command_events` at `:495-501`) and either
  replaces the output with the prepared preview (`:159-166`) or, when the descriptor was rejected,
  clears it and records an unavailable descriptor (`:167-188`); a completed **tool** drops earlier
  completions and the output value (`:189-198`, `remove_completed_tool_events` at `:402-404`);
  `TurnUsageUpdated` keeps only the latest (`:202-205`); command output deltas past 16 KiB are
  head/tail-compacted into one delta (`:207-209`, `compact_command_output_deltas` at `:503-544`).
  Nothing touches `ItemDelta::Text`: the whole non-test file (`:1-580`) has no match for it, so
  every agent-message and reasoning chunk stays in `events` for the life of the turn, next to the
  `ItemCompleted` that carries the same text in full.
- **Every compaction is a `retain` over the whole vector**, so one completion costs O(buffer), and a
  turn of N items pays O(N²) over its life.
- **Two append paths.** `apply_prepared_event_to_entry` (`thread_runtime.rs:841-847`) appends with
  descriptors, substituting `PreparedItemOutput.live_event` when present; `apply_event_locked`
  (`:905-907`) appends the raw event when its caller passes `append_live = true`, which
  `settle_completed_turn` does (`:938`) and the `apply_event_for_test` helper can. The prepared
  path already substitutes the command preview and strips tool output *before* the buffer sees the
  event (`thread_runtime/outputs.rs:255-269`, `:305-320`); the buffer's own replacement at
  `live.rs:153-194` is the fallback for an unprepared event. Both must keep working.
- **`TurnCompleted` reaches the buffer.** `settle_completed_turn` appends it and then clears the
  turn on success (`thread_runtime.rs:938-943`); when persistence failed it calls
  `block_on_persistence` instead (`:962-966`, `gate.rs:113-120`) and the buffer is kept, so the
  snapshot a `PersistenceBlocked` thread sends ends with a `TurnCompleted`. The browser's
  `turn_completed` handler (`app.js:4589-4607`) then unlocks the composer. This must survive.
- **The buffer can start before `TurnStarted`.** `admit_to_live_buffer`
  (`registry/event_forwarder.rs:1929-1960`) calls `ensure_turn_with_user_input` on the first
  turn-scoped event; a later `TurnStarted` is pushed *after* that item and re-stamps `turn_id`
  (`live.rs:152-154`). The browser's `turn_started` handler clears `streamElsByItemId` and
  `itemKindsByItemId` (`app.js:4535-4552`), so a replay that emits `TurnStarted` after an item's
  events forgets that item's row and kind; later live deltas for it open a second row.
- **Snapshot construction** (`live.rs:294-361`) clones every event, converts through
  `WireAgentEvent::from_agent_event` (`giskard-proto/src/wire.rs:347`) and, for `ItemCompleted`,
  `WireItem::from_item_with_outputs` with the two descriptor maps; `answered_approvals` and
  `answered_server_requests` are derived from the events plus the two resolved sets. `ws.rs:1470-
  1499` sends it as one frame and logs `accumulated_events`.
- **Consumers other than the snapshot.** `live_item` (`live.rs:269-292`) returns the *latest*
  lifecycle event for an item as `LiveItem::Started | Completed` and feeds the item endpoint
  (`routes.rs:5009-5021`, then `wire_turn_item` at `:5032`). `item_events` (`live.rs:254-267`)
  returns the raw `ItemStarted`/`ItemCompleted` events for one item; `resolve_subagent_link_info`
  (`registry.rs:1891-1909`) walks them in reverse, preferring a completion. `is_active`,
  `resolve_approval`, `resolve_server_request`, `clear_turn`, `ensure_turn_with_user_input` and
  `replace_turn_with_user_input` are the rest of the surface; `LiveTurnState` is used only from
  `thread_runtime.rs:47,78`.
- **How the browser replays.** `renderLiveTurnSnapshot` (`app.js:4673-4716`) seeds the answered
  sets, runs every event through `handleEvent`, then re-asserts outstanding requests. Per text
  delta, `appendStream` (`:7763-7809`) does `body.textContent += text`, re-derives a reasoning
  row's summary, and writes `scrollTop` (`:7808`). Tool progress goes through `appendToolProgress`
  (`:7722-7762`), which joins chunks with `"\n"` — `payload.output = current ? current + "\n" +
  chunk : chunk` (`:7746-7747`) and `progress.textContent += (progress.textContent ? "\n" : "") +
  chunk` (`:7759`) — and writes `scrollTop` (`:7751`, `:7761`). Command output goes through
  `appendRunningCommandOutput` (`:6314-6326`), which concatenates and re-renders the command body
  per chunk. The kind that routes a delta comes only from `itemKindsByItemId`, set at
  `item_started` (`:4555`); a text delta for an item already in `renderedItemIds` is ignored by
  `appendStream` (`:7765`).
- **Completion re-renders the row from the item.** `finalizeStreamedItem` (`:7810-7849`) reuses
  the streamed body and calls `renderItemBodyForItem`, `registerRenderedItemBody`,
  `finishRunningCommand` (commands) and `markRenderedItem`; when no streamed body exists,
  `addItem` (`:7851-7992`) creates the row and ends in the same four calls. The row class and role
  agree between the two paths for every kind (`classForStream`/`roleForStream` at `:8244-8259`
  against `classForPayload`/`roleForPayload` at `:8226-8243`). A reasoning row's toggle is created
  on first render and collapsed when the next row is appended (`appendBubble` → 
  `collapseSupersededReasoningRows`, `:5568-5590`, `:6084-6093`). So a completed item replayed
  **without** its deltas ends in the same DOM as one replayed with them.
- **Card position is arrival position.** `approval_requested` and `server_request_received`
  create their card with `bubble(...)` when handled (`renderApprovalRequest` `:4807`,
  `renderServerRequest` `:5049`), `error` and `notice` likewise (`:4641-4660`). Their place in
  `accumulated` is their place in the transcript. `turn_usage_updated` only moves the gauge
  (`:4582-4588`) and `diff_updated` has no handler, so their positions are invisible.
- **Tests that depend on the buffer's shape.** In `live.rs`: `live_snapshot_keeps_only_the_latest_
  usage_event_at_the_end` (`:874-920`) asserts the order `[TurnStarted, ItemStarted,
  TurnUsageUpdated]`; `completed_command_output_uses_bounded_wire_descriptor_in_live_snapshot`
  (`:921-1003`) and `rejected_command_output_replaces_stale_descriptor_with_unavailable_snapshot`
  (`:1005-1090`) read `store.turn.as_ref().events` directly. Integration tests filter
  `accumulated` by kind, never by position: `tests/approval_reconnect.rs:197-231`,
  `tests/server_requests.rs:512-516` and `:935-948`, `tests/turn_steering.rs:682-692`,
  `tests/e2e_smoke.rs:1692-1700` and `:4118-4125`. `tests/ui.rs:721-725` pins source strings in
  `renderLiveTurnSnapshot`; `:1492-1500` pins `appendStream`'s signature and its first body lines.
- **Replay-server scenarios.** The reasoning trigger streams start, word deltas, completion
  (`bin/giskard-server-replay.rs:820-858`); the steering trigger emits only `TurnStarted` and holds
  (`:581-596`). No scenario holds a turn open with an unfinished streaming item.
- **Spec text to keep true.** §13.6 describes the buffer as "the ordered `AgentEvent`s accumulated
  since `TurnStarted`" (`specs/giskard-specification.md:4197-4202`) and as "bounded (coalesced/
  truncated for very long command output, keeping head+tail)" (`:4231-4233`). The E9 invariant
  (`:4154-4160`) requires the client to replay `accumulated` through the same handler as live
  events — unchanged by this milestone.

## Decisions

- **D1. One ordered list of entries; an item is one entry.**

  ```rust
  struct LiveTurn {
      turn_id: TurnId,
      user_input: Option<UserInput>,
      /// Expanded first: see D3.
      turn_started: Option<AgentEvent>,
      /// Arrival order. An item sits where it was first seen (start, first delta, or
      /// completion, whichever came first); every other event sits where it arrived.
      entries: Vec<LiveEntry>,
      slots: HashMap<ItemId, usize>,
      /// Expanded last: see D4.
      latest_usage: Option<AgentEvent>,
      // unchanged:
      command_output_descriptors: HashMap<ItemId, CommandOutputDescriptor>,
      tool_output_descriptors: HashMap<ItemId, WireToolOutput>,
      resolved_approvals: HashMap<ApprovalId, ApprovalDecision>,
      resolved_server_requests: HashSet<ServerRequestId>,
  }

  enum LiveEntry {
      Item(LiveItemEntry),
      /// ApprovalRequested, ServerRequestReceived, ServerRequestResolved, Error, Notice,
      /// DiffUpdated, TurnCompleted, ThreadOpened — anything that is not an item lifecycle
      /// event, TurnStarted or TurnUsageUpdated.
      Event(AgentEvent),
  }

  struct LiveItemEntry {
      id: ItemId,
      start: Option<ItemStart>,
      /// The latest completion, with command output already reduced to its preview and tool
      /// output stripped, exactly as the event is stored today.
      completed: Option<Item>,
      /// Text deltas received since the latest completion (or since the start, if none).
      text: String,
      /// CommandOutput deltas received since the latest completion, head/tail-compacted past
      /// MAX_LIVE_COMMAND_OUTPUT with the existing marker.
      output: String,
  }
  ```

  Expansion of one item entry, in this order: `ItemStarted(start)` if present, `ItemCompleted(
  completed)` if present (through `from_item_with_outputs` with the two descriptor maps, as today),
  `ItemDelta::Text { text }` if non-empty, `ItemDelta::CommandOutput { chunk: output }` if
  non-empty. Deltas received *before* a completion are gone; deltas received *after* it replay
  after it, which is their arrival order.

- **D2. Text coalescing mirrors the browser's join rule, keyed by `start.kind` only.** For an
  entry whose `start.kind` is `ToolCall`, a chunk joins as `acc = if acc.is_empty() { chunk } else
  { acc + "\n" + chunk }`, the rule `appendToolProgress` applies at `app.js:7746-7747` and `:7759`.
  Every other entry concatenates, as `appendStream` and `appendRunningCommandOutput` do. The kind
  comes from `start` alone, never from `completed`, because the browser routes a delta by
  `itemKindsByItemId`, which only `item_started` sets. An entry with no start concatenates.

- **D3. `TurnStarted` is held apart and expanded first.** Today it sits wherever it arrived, which
  is first except when an item preceded it, and that case makes the browser's `turn_started`
  handler wipe the stream maps mid-replay. Expanding it first is identical in the normal case and
  strictly better in the delayed case. It still re-stamps `turn_id` on arrival as today.

- **D4. `TurnUsageUpdated` is held apart and expanded last.** Today the latest usage sits at the
  position of its arrival, which was the end at that moment. The browser's handler is
  position-independent, and `latest_usage` makes the retain-and-push an O(1) replace. The
  existing order test still holds (its usage event is the last to arrive).

- **D5. Everything else stays an in-order `Event` entry.** Approval and server-request events,
  `Error` and `Notice` keep their positions so their cards replay where the live transcript drew
  them. `DiffUpdated` has no browser handler and `TurnCompleted` reaches the buffer only in the
  `PersistenceBlocked` case; both are kept as they are today. Dropping `DiffUpdated` from the
  snapshot is a follow-up, not this milestone.

- **D6. Completion replaces in the item's slot.** A completion for an entry sets `completed`,
  clears `output`, and clears `text` — the deltas it replaces are the ones the completion
  re-renders over (`finalizeStreamedItem`). A completion for an unseen id appends a new entry with
  only `completed`. The existing command rules carry over unchanged: the prepared preview wins
  when present; a rejected descriptor clears the output and inserts the unavailable descriptor; a
  tool completion drops its output value and records the tool descriptor. The replay therefore
  places a completed item's `ItemCompleted` at the item's slot rather than at the end of the
  buffer; the browser upserts the existing row in place either way (`addItem` at `:7866-7906`), so
  the DOM is the same, and the order is in fact closer to what a live client saw.

  One check during implementation: for a **tool** completion whose status is still running
  (`tool_status_is_running`), confirm by reading `renderItemBody`'s tool branch that the row drops
  progress text already shown. If it keeps it, keep `text` across a non-terminal tool completion
  instead of clearing it. Command completions with a running status already clear the streamed
  output today (`completed_command_item_id` does not look at status), so they clear here too.

- **D7. A late `ItemStarted` for an already-completed entry replaces `start`, keeps `completed`,
  and logs at `debug`.** Today `live_item` returns whichever lifecycle event came last; with D7 it
  reports `Completed`. Adapters guarantee one lifecycle per `ItemId` (spec §4.4), so the case is a
  protocol violation that is logged, not modelled. Nothing in the tree produces it.

- **D8. `live_item` and `item_events` read the entry.** `live_item` returns `Completed` when
  `completed` is set, else `Started` when `start` is set, else `None` (a delta-only entry has no
  lifecycle event today either). `item_events` reconstructs `[ItemStarted, ItemCompleted]` from the
  entry, in that order, so the sub-agent lookup's reverse walk still meets the completion first.

- **D9. Browser replay stops measuring per event.** `renderLiveTurnSnapshot` sets
  `state.replayingLiveSnapshot = true` around the replay loop in a `try`/`finally`. While set,
  `appendStream`, `appendToolProgress` and `finalizeStreamedItem` skip their `scrollTop` writes,
  and `appendBubble` treats `transcriptShouldStickToBottom()` as true without measuring and skips
  `keepTranscriptAtBottom`. After the loop the transcript is scrolled to the bottom once, which is
  the state every per-delta write left it in. The pinned strings in `tests/ui.rs:721-725` and
  `:1492-1500` are not touched: the guard wraps the `scrollTop` line, not the function's head.

- **D10. No wire change.** `LiveTurnSnapshot`, `WireAgentEvent` and the subscribe sequence are
  untouched. The spec changes are prose about the buffer plus a changelog entry.

## Changes

### A. `crates/giskard-server/src/thread_runtime/live.rs` (non-test about −260/+300; tests −120/+280)

- Replace `LiveTurn` with the D1 shape; `LiveItem` and `LiveTurnState` keep their public names.
- `replace_turn_with_user_input` and `ensure_turn_with_user_input`: construct the new shape;
  unchanged semantics.
- `append_with_outputs(thread_id, event, command_output, tool_output)`: same signature. Dispatch:
  - `TurnStarted` → re-stamp `turn_id`, store in `turn_started`.
  - `TurnUsageUpdated` → store in `latest_usage`.
  - `ItemStarted` → `entry_for(item.id)` (create at tail if absent); set `start`; D7 if
    `completed` is set.
  - `ItemDelta::Text` → `entry_for(item_id)`; join per D2.
  - `ItemDelta::CommandOutput` → `entry_for(item_id)`; push the chunk; if `output.len() >
    MAX_LIVE_COMMAND_OUTPUT`, replace it with `compact_command_output(&output)` (the existing
    function and marker; the Vec-walking `compact_command_output_deltas` is deleted).
  - `ItemCompleted` → `entry_for(item.id)`; apply the existing command/tool output rules to the
    item and the descriptor maps (the code at `:155-198` moves here almost verbatim, minus the
    `retain` calls); set `completed`; clear `text` and `output` (D6).
  - everything else → `entries.push(LiveEntry::Event(event))`.
- `resolve_approval`, `resolve_server_request`, `clear_turn`, `is_active`: unchanged.
- `item_events`, `live_item`: per D8, through `slots`.
- `snapshot`: expand `turn_started`, then each entry in order (D1), then `latest_usage`. Derive
  `answered_approvals` and `answered_server_requests` by walking the expanded wire events or the
  `Event` entries — same result as today. The three descriptor/resolution maps are read exactly as
  now.
- Delete `remove_command_output_deltas`, `remove_completed_command_events`,
  `remove_completed_tool_events`, `compact_command_output_deltas`; keep `compact_command_output`,
  `floor_char_boundary`, `ceil_char_boundary`, `completed_command_item_id`,
  `completed_tool_item_id`, `command_output_item_id` as needed.
- `#[cfg(test)]` helpers `pending_approvals` / `pending_server_requests` / `pending_attention`:
  iterate `Event` entries instead of `&[AgentEvent]`.
- Tests: rewrite the three that read `events` (facts above) against the new fields or, better, the
  snapshot; keep every other existing test as is — they use the public API. Add the tests in
  **Tests** below.

### B. `crates/giskard-server/src/thread_runtime.rs` (0 to +5)

No signature changes: the call sites at `:841-847`, `:905-907`, `:570-605`, `:662` and `:943`
compile unchanged. Optionally extend the lifetime-class doc comment on `ThreadRuntimeEntry`
(`:59-72`) with one clause saying `live` is item-keyed and cleared with the turn. Nothing else.

### C. `crates/giskard-server/src/ws.rs` (+2)

Keep the `build_live_snapshot` debug log (`:1470-1482`); add `open_items` and `completed_items`
counts if `LiveTurnState` exposes a cheap `stats()` — optional; `accumulated_events` alone is
enough to see the reduction.

### D. `crates/giskard-server/static/app.js` (+25)

- `renderLiveTurnSnapshot` (`:4673`): wrap the `for (const ev of (snap.accumulated||[]))
  handleEvent(ev);` loop and the two re-assertion loops in `try { state.replayingLiveSnapshot =
  true; … } finally { state.replayingLiveSnapshot = false; }`, then `scrollTranscriptToBottom()`.
- `appendStream` (`:7808`), `appendToolProgress` (`:7751`, `:7761`), `finalizeStreamedItem`
  (`:7848`): `if (!state.replayingLiveSnapshot) $("transcript").scrollTop = …`.
- `appendBubble` (`:5568-5590`): `const followBottom = state.replayingLiveSnapshot || transcript
  ShouldStickToBottom();` and skip `keepTranscriptAtBottom` while replaying.
- `resetRenderState` (`:7998`): nothing — the flag is scoped by the `finally`.

No visible change, so no screenshot regeneration (AGENTS.md: "No regeneration needed for changes
with no visible effect").

### E. `crates/giskard-server/src/bin/giskard-server-replay.rs` (+60) and `tests/e2e/tests/helpers.ts` (+4)

A new trigger, `SCRIPTED_STREAMING_HOLD_TRIGGER = "Stream a note, then hold this turn open."`,
mirrored in `helpers.ts`: `TurnStarted`; a completed agent message (`ItemStarted`, word deltas,
`ItemCompleted` with `SCRIPTED_REPLY`); then `ItemStarted` for a reasoning item followed by the
word deltas of `SCRIPTED_REASONING_DETAIL` **and no completion and no `TurnCompleted`**. The turn
stays open like the steering hold does (`:581-596`). Register it the way `wait_for_steering` is,
so the harness does not complete it.

### F. Documentation

- `specs/giskard-specification.md`: bump to 1.100; add an amendment note under the 1.99 one; add
  **Changelog (1.99 → 1.100), item-centric live buffer** before the 1.99 changelog (`:250`):
  - **LB1:** the live buffer is keyed by item: one entry per item holding its start, its latest
    completion, and the deltas received since that completion, coalesced into one text and one
    output string; non-item events keep their arrival position. Text deltas are discarded when the
    item completes because `ItemCompleted` carries the canonical text (§7.3).
  - **LB2:** `LiveTurnSnapshot.accumulated` is expanded from the entries: `TurnStarted` first,
    then each entry in arrival order (start, completion, remaining text, remaining output), then
    the latest `TurnUsageUpdated`. A replayed transcript is unchanged; the wire types are unchanged.
  - **LB3:** the browser suppresses per-event scroll writes while replaying a snapshot and scrolls
    once at the end.
  - Rewrite `:4197-4202` ("the ordered `AgentEvent`s accumulated since `TurnStarted`") to describe
    the item-keyed buffer, and `:4231-4233` to say text deltas are coalesced per open item and
    dropped on completion, alongside the existing command-output rule.
- The overall plan's L1 section links to this document and is marked landed when it is.
- `README.md`, `docs/api-endpoints.md`, `config.example.toml`, adapter READMEs: nothing changes.
- `AGENTS.md`: nothing changes.

### Not touched

- `giskard-proto` (no wire change), `giskard-persist`, `giskard-core`, both adapters.
- `registry/event_forwarder.rs`: `CurrentTurnItems` and the commit path are unchanged; L1 does not
  make the forwarder read the buffer.
- `thread_runtime/outputs.rs`, `tasks.rs`, `requests.rs`, `diffs.rs`, `gate.rs`.
- The `[history]` config, the history HTTP route, and `renderPersistedTurn`.

## Tests

**Unit, `live.rs`** (every one builds a `LiveTurnState`, appends events, and asserts on
`snapshot()` or the lookups — never on private fields):

1. *Coalescing equivalence.* An agent message streamed as 50 chunks then completed yields exactly
   `[TurnStarted, ItemStarted, ItemCompleted]` with the full text on the completion, and the same
   snapshot as the same message sent as one chunk.
2. *Open item.* An item started and streamed but not completed yields `[ItemStarted,
   ItemDelta::Text]` with the concatenation of every chunk, in order.
3. *Tool progress join rule.* A `ToolCall` start followed by chunks `["a", "", "b"]` yields one
   `ItemDelta::Text { text: "a\nb" }`; the same chunks on an entry with no start yield `"ab"`.
4. *Slot stability.* Items A (started), B (started), A (completed), B (completed) expand as
   `A.start, A.completed, B.start, B.completed`; a delta for A after its completion expands after
   `A.completed` and before B.
5. *Interleaved requests keep their place.* `[ItemStarted A, ApprovalRequested, ItemCompleted A,
   ServerRequestReceived]` expands with the approval between A's events and the server request
   after them; `outstanding_approvals` / `outstanding_server_requests` (the existing test mirrors)
   return the same ids as before.
6. *Delayed TurnStarted.* `[ItemStarted A, TurnStarted]` expands as `[TurnStarted, A.start]` and
   `turn_id` is re-stamped.
7. *Usage.* Port `live_snapshot_keeps_only_the_latest_usage_event_at_the_end`: still three events
   with the latest usage last; add a case where usage arrives before a later item and assert the
   usage is still last.
8. *Command output compaction.* Port the two command tests (`:921-1090`) to read the snapshot:
   the preview descriptor wins; the rejected descriptor yields an empty, unavailable output; a
   streamed output past 16 KiB on an open command yields one delta with the marker, head and tail.
9. *Completion clears output.* Streamed command output then a completion yields no
   `CommandOutput` delta; output streamed after a running-status completion yields one delta after
   the completion.
10. *Tool completion strips output and keeps the descriptor.* Port
    `live_tool_item_carries_descriptor_and_no_inline_output` (`:718`) unchanged — it uses the API.
11. *Lookups.* `live_item` is `Started` after a start, `Completed` after a completion (with both
    descriptors), `None` for a delta-only entry and for the wrong turn; `item_events` returns
    `[ItemStarted, ItemCompleted]` for a completed item and `[ItemStarted]` for an open one.
12. *D7.* A start after a completion keeps `live_item` at `Completed`; the `debug` line is
    asserted with `#[traced_test]` and `logs_contain`.
13. *PersistenceBlocked.* Appending `TurnCompleted` without `clear_turn` yields a snapshot that
    ends with `TurnCompleted`.
14. The existing approval/server-request tests (`:1307-1471`) run unchanged.

**Integration, `crates/giskard-server/tests/live_snapshot_shape.rs` (new, about 150 lines):**
seed a runtime with `replace_live_turn_for_test` and `apply_event_for_test`
(`thread_runtime.rs:323-333`): 20 completed agent messages of 30 chunks each, one open reasoning
item of 30 chunks, one approval; subscribe over the test WebSocket (`ws::expect_live_snapshot` as
in `tests/approval_reconnect.rs`) and assert `accumulated.len() == 1 + 20*2 + 2 + 1`, that no
`ItemDelta` names a completed item, that the open item's single delta is the concatenation, and
that the approval is outstanding. This is the regression test for the slowness itself.

**End-to-end, `tests/e2e/tests/live-snapshot.spec.ts` (new, about 60 lines):** send
`SCRIPTED_STREAMING_HOLD_TRIGGER`; wait for the agent bubble with `SCRIPTED_REPLY` and the
reasoning row to show the streamed text; `page.reload()`; assert exactly one agent bubble with
`SCRIPTED_REPLY`, exactly one reasoning row, expanded (it is the newest row), whose body contains
`SCRIPTED_REASONING_DETAIL`'s streamed prefix, and that the composer is in its active-turn state.
Existing reload specs (`approvals.spec.ts:32-80`, `server-requests.spec.ts:27`,
`reasoning.spec.ts:104-119`, `lazy-diffs.spec.ts:49-61`, `subagent-approvals.spec.ts:265-283`)
must pass unchanged. Add nothing to `mobile-overflow.spec.ts`: no new row kind.

**Static, `tests/ui.rs` (+10):** pin that `renderLiveTurnSnapshot` contains
`state.replayingLiveSnapshot = true` inside a `try` with a `finally` that clears it, and that
`appendStream`'s `scrollTop` write is guarded by `!state.replayingLiveSnapshot`.

## Exit checks

- `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` green.
- `tests/e2e/run.sh` green, including the new spec.
- `grep -n 'ItemDelta::Text' crates/giskard-server/src/thread_runtime/live.rs` now matches: the
  coalescing branch exists.
- No `retain(` over the entries in `live.rs`'s non-test code.
- Manual: with `giskard-server-replay`, send the streaming-hold trigger, reload, and read the
  `build_live_snapshot` debug line: `accumulated_events` is 5 (TurnStarted, message start,
  message completion, reasoning start, one reasoning delta), where today it is 5 plus every word.
- `wc -l` of `live.rs` non-test code did not grow past about 620 lines; if it did, the structure
  is carrying more than D1 asked for.

## Signs the step has gone wrong

- A reload during a turn shows an item twice, or an open item's later live deltas open a second
  row: the replay emitted `TurnStarted` after an item, or `itemKindsByItemId` was not seeded
  because an `ItemStarted` was dropped from the expansion.
- A tool call's progress lines run together after a reload: D2's join rule was not applied, or
  was applied to a non-tool entry.
- An approval card moves to the end of the transcript on reload: a request event went into a
  side list instead of an `Event` entry.
- The item endpoint reports `started` for an item the transcript shows completed: `live_item`
  read `start` before `completed`.
- A `PersistenceBlocked` thread's composer stays locked after a reload: `TurnCompleted` was
  filtered out of the entries.
- A test asserts on `turn.entries` directly: it is coupling itself to the shape this milestone
  exists to be free to change again in L2.
- Any change to `giskard-proto`, `app.js`'s event handlers, or the subscribe sequence in `ws.rs`:
  out of scope for L1.

## Follow-ups (not this milestone)

- Drop `DiffUpdated` from the snapshot (the browser has no handler for it) — needs a spec note.
- L2 reads `entries` by slot for the bounded tail and the items route; keep `slots` and the
  entry order stable for it.
- P1 writes `completed` items as payload `item` records in slot order.

## Size

About 600 lines changed in `live.rs` including tests, 25 in `app.js`, 60 in the replay binary, 4
in `helpers.ts`, 150 in a new integration test, 60 in a new e2e spec, 10 in `tests/ui.rs`, 30 in
the spec. One PR; the browser change (D9) can be reviewed as its own commit inside it.
