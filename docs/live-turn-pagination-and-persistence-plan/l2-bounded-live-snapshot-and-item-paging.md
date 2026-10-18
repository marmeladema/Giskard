# L2 — Bounded live snapshot and live-item paging

Implementation plan for milestone L2 of
[`live-turn-pagination-and-persistence-plan.md`](../live-turn-pagination-and-persistence-plan.md).
Written against `main` at `5e3c7c0` (L1 merged, spec 1.100); every file and line reference below
was checked against that tree. Re-check them before trusting one. The milestone text in the overall
plan is the authority on scope; where this document refines it (the order of the entries that ride
along, which items may never be omitted, the reasoning preview), this document wins and the overall
plan's L2 section has been updated to match. Nothing here touches the on-disk format.

L1 made the snapshot cost one or two events per item. L2 bounds the number of items a snapshot
carries, so opening a thread whose live turn has thousands of items sends and renders a tail, and
pages the rest on demand through one new HTTP route that serves a live turn from the runtime and a
persisted one from its payload.

## What lands

1. `LiveTurnSnapshot` carries at most `[history] turn_items` items (default 100): the newest ones.
   Everything the user still has to act on or that is still changing rides along regardless of
   age. One new optional field says how many settled items were left out and where paging starts.
   Completed reasoning notes in the snapshot carry the 1 KiB head preview that history already
   uses.
2. `GET /api/projects/{id}/threads/{thread_id}/turns/{turn_id}/items?before=<ItemId>&limit=N`
   pages a turn's items oldest-first, from the live runtime while the turn is live and from the
   payload once it is persisted, with the same runtime–payload–runtime resolution the item
   endpoint uses.
3. The browser renders a marker row under the live turn's prompt when items were omitted, pages
   them in when the reader scrolls to the top or presses the marker, and only then resumes paging
   older turns. Rows page in exactly where they belong.
4. One new config key, the route in `docs/api-endpoints.md`, spec 1.101, a replay-server scenario
   that produces a long turn, and unit, integration, and end-to-end tests.

## Facts the plan rests on

- **The buffer after L1.** `LiveTurn` (`crates/giskard-server/src/thread_runtime/live.rs:29-68`)
  holds `turn_started`, `entries: Vec<LiveEntry>`, `slots: HashMap<ItemId, usize>`,
  `pending_starts: IndexMap<ItemId, (ThreadId, TurnId, ItemStart)>`, `latest_usage`, the two
  descriptor maps and the two resolved sets. `LiveEntry` is `Item(LiveItemEntry) | Event(AgentEvent)`
  (`:72-78`); `LiveItemEntry` has `id, thread, turn, start, completed: Option<Item>, text, output`
  (`:80-93`). Entries are only pushed, never removed or moved (doc comment at `:46-49`), so an
  entry's index is a stable position for the life of the turn. `expand` (`:291-320`) emits start,
  completion through `WireItem::from_item_with_outputs`, then remaining text and output.
  `snapshot(thread_id)` (`:539-614`) takes no limit and expands `turn_started`, every entry, the
  pending starts (`:578`), then `latest_usage` (`:587`); it builds the struct literal at `:606`.
  `live_item` (`:513`) and `item_events` (`:492`) read one entry through `slots`.
- **A completion can be non-terminal.** `complete_item` (`:185-237`) stores whatever completion
  arrives. Commands complete with a running status for background processes
  (`command_status_is_running`, `giskard-core/src/item.rs:68`), tools likewise
  (`tool_status_is_running`, `:267`), and file changes carry `status: Option<String>` (`:191-199`)
  that the replay scenario sets to `in_progress` and then `completed`
  (`bin/giskard-server-replay.rs:634-640`). The running-task set is built from the
  command predicate (`thread_runtime/tasks.rs:45,65,126`). The browser upserts a repeated
  `item_completed` onto the existing row (`app.js:7880`, `addItem`, the `isRenderedItem` branch)
  and creates a new row at the bottom when the item is not rendered.
- **The wire struct.** `LiveTurnSnapshot` (`giskard-proto/src/lib.rs:226-255`) has `thread_id,
  turn_id, user_input, accumulated, answered_approvals, answered_server_requests`, the last two with
  `serde(default, skip_serializing_if)`. Struct literals: `lib.rs:1377`, `:1406`, `live.rs:606`,
  `tests/turn_steering.rs:276`. The spec sketches it at `specs/giskard-specification.md:4112-4126`.
- **Subscribe.** `ws.rs:1472` calls `runtime.live_snapshot()`; the history page size is read with
  `history_limit_or_default(state, thread_id, "subscribe_history", |config| config.history.initial,
  5)` (`routes.rs:4596-4617`, used from `ws.rs` and `routes.rs:5062`). The debug line at
  `ws.rs:1474-1482` logs `accumulated_events`.
- **The routes to mirror.** `thread_history` (`routes.rs:5049-5089`): thread existence → 404,
  `HistoryQuery { before, limit }` (`:4619`), `HistoryResponse` is a route-local
  `serde::Serialize` struct (`:4624`), `limit.clamp(1, MAX_HISTORY_PAGE_TURNS)` with the constant
  100 at `:57`, and an unknown `before` is treated as "newest page" by `load_history`
  (`store.rs:1892-1896`, `unwrap_or(records.len())`). `turn_item` (`:4991-5030`): thread existence,
  runtime `live_item`, then `store.load_turn_item`, then runtime again; `wire_turn_item` (`:5032`)
  projects a live completion with `from_item_with_outputs` and a persisted one with `WireItem::from`.
  Routes are registered at `:118-140`. `ApiError` (`:5241`) has `NotFound`, `BadRequest`, …
- **Persisted items.** `load_turn_item` (`store.rs:1694-1725`) checks the index for the turn, reads
  the payload with `history::read_turn_payload` (or `parse_turn_history` on the flat layout) and
  finds the item in `payload.items`. `parse_turn_payload` sorts items by display index
  (`history.rs`, `items.sort_by_key(|(index, _)| *index)`), so `payload.items` is display order.
- **Config.** `HistoryConfig { initial, page }` (`giskard-persist/src/config.rs:388-407`), struct
  `#[serde(default)]`, defaults 5/5 in `impl Default`; no validation of these values exists, the
  route clamps instead. Documented at `README.md:283` and `config.example.toml:71-77`.
- **Reasoning preview.** `WireItem::from_persisted_item` (`giskard-proto/src/wire.rs:495-513`)
  converts with `.into()` then replaces `Reasoning.text` with the 1 KiB head prefix and sets
  `preview: WireTextPreview` when cut; `From<Item> for WireItem` (`:483`) applies no preview, so
  the item endpoint returns full text for live and persisted items alike. The browser marks a
  previewed row `reasoningTruncated` (`app.js:8160-8168`), and both expanding it
  (`setReasoningExpanded`, `:6097`) and copying it (`:5566`) call `fetchReasoningNote` (`:9792`),
  which reads the item endpoint and swaps in the full note. So the overall plan's remark that a
  copy "yields the preview" was wrong: RP2 already fetches the full note.
- **How the browser renders the snapshot and pages history.** `renderLiveTurnSnapshot`
  (`app.js:4680-4745`) measures `stick`, reconciles the stale live block, sets
  `currentRenderTurnId`, draws the prompt with `renderLiveTurnUserInput` (`:4801-4812`, the row
  gets `dataset.liveUserInput`), seeds the answered sets, replays, re-asserts outstanding
  requests, settles reasoning rows, and scrolls once. `renderHistoryPage` (`:4215-4263`) renders
  a page into a detached container with `state.renderTarget` set and `state.activeTaskGroup`
  cleared, inserts the rows before the transcript's first child, calls
  `collapseSupersededReasoningRows`, and keeps the viewport by adding the height delta to
  `scrollTop`. `maybeAutoFillHistory` (`:4452-4463`) and `onTranscriptScroll` (`:4531-4538`) both
  gate on `hasMoreHistory`, `loadingHistory` and `oldestTurnId`. `loadHistoryPage` (`:4465-4490`)
  uses `captureActiveViewIdentity` / `activeViewIdentityIsCurrent`, which `tests/ui.rs:2309-2320`
  pins. `transcriptShouldStickToBottom` returns false while `state.renderTarget` is set (`:5512-5516`).
  Every top-level function of `app.js` is a global, which is how the e2e specs call
  `handleEvent` and `connectWs` from `page.evaluate`.
- **The prompt row and the echoed user message.** `addItem` for a `user_message` looks for the
  turn's provisional prompt row with `provisionalUserBodyForTurn` (`:4814`), which searches
  `renderTarget()`, not the transcript, and then `placeRowFirstInTurn` (`:4826`). A page rendered
  into a detached container therefore cannot find the prompt row that is already in the transcript.
- **Rows are removed by turn id.** `reconcileInFlightTurn` (`:4326`) calls `removeTurnRows(liveId)`
  (`:4437`), which removes every `.msg` whose `dataset.turn` matches. `appendBubble` stamps
  `dataset.turn` from `currentRenderTurnId` (`:5539`).
- **Task navigation pages history, not items.** `loadTaskHistoryUntilVisible` (`:7600-7665`) pages
  the history route until an `after_turn` command's turn is loaded. An `after_turn` command belongs
  to a persisted turn, never the live one, so L2 does not change it. A running command in the live
  turn is in the running-task set only while its status is running.
- **Tests and harness.** `TestServer::builder(...).config("[history]\n...")` (`giskard-testenv/
  src/server.rs:140`), `server.client` / `server.cookie` / `server.base` for authenticated GETs
  (`tests/history_sync.rs:22-50`), `ws::expect_live_snapshot` (`giskard-testenv/src/ws.rs:100`),
  `replace_live_turn_for_test` / `apply_event_for_test` (`thread_runtime.rs:323-333`),
  `fixtures::completed_turn`, and the pattern in `tests/live_snapshot_shape.rs`. The replay server
  writes its own `config.toml` (`bin/giskard-server-replay.rs:1144-1168`) with no `[history]`
  table; the streaming-hold scenario (`SCRIPTED_STREAMING_HOLD_TRIGGER`, `:117`, `:900-925`)
  is the model for a scenario that holds a turn open. `mobile-overflow.spec.ts` injects events
  through `window.handleEvent` (`:31-35`) and checks every `.msg` for horizontal overflow.
- **Spec anchors.** Version at `:12`, amendment notes from `:14`, the 1.100 changelog at `:257`,
  RP1–RP3 at `:326-333`, the authority table at `:3957-3968` with the rule "Before adding a wire
  message, its specification must name its authority, clock, and delivery class", the REST history
  bullet at `:4007-4012`, the snapshot sketch at `:4112-4126`, the subscribe sequence at
  `:4235-4248`, and the "live buffer is bounded per item" paragraph that follows it.

## Decisions

- **D1. The cut.** Let `N = max(1, [history] turn_items)`. Walk `entries` from the end counting
  `LiveEntry::Item` entries; the cut is the index of the N-th item from the end, or 0 when the turn
  has at most N items, in which case the snapshot is exactly L1's. Entries at or after the cut
  expand as L1 does. Entries before the cut are classified:
  - a **settled** item (see D2) is **omitted** and counted;
  - an **unsettled** item rides along;
  - an `Event` rides along if it is an `ApprovalRequested` not in `resolved_approvals`, a
    `ServerRequestReceived` with no later `ServerRequestResolved` and not in
    `resolved_server_requests`, an `Error`, a `Notice`, or a `TurnCompleted`; it is omitted if it
    is an answered approval, a resolved server request (both its received and resolved events), or
    a `DiffUpdated`.

  Riders expand **before** the tail, in their arrival order, right after `turn_started`. They are
  older than the tail, so above it is where a reader expects them, and the browser's
  "outstanding requests get the last word" re-assertion already runs after the replay. Pending
  starts and `latest_usage` keep their L1 positions. This refines the overall plan, which said
  "after the tail".

- **D2. Settled.** An item entry is settled when `completed` is `Some` and its payload is not
  still running: a `CommandExecution` whose `status` fails `command_status_is_running`, a
  `ToolCall` whose `status` fails `tool_status_is_running`, a `FileChange` whose `status` fails a
  new `file_change_status_is_running` (same normalisation as `normalized_command_status`,
  running when `in_progress`/`inprogress`/`running`; check the vocabulary the Codex mapper emits
  in `giskard-harness-codex/src/mapping.rs` before writing it), and every other kind. An
  unsettled item is one a later completion will upsert, and the browser can only upsert a row it
  has, so it must always be sent. This is also what keeps the Tasks menu's `Open` and navigation
  working for a background command in the live turn: a running command is unsettled.

- **D3. Wire.** `LiveTurnSnapshot` gains one optional field. The UI and the server are paired
  (a tab whose `ui_version` differs from the server's stops reconnecting and asks for a reload,
  `docs/api-endpoints.md:54-57`), so nothing here is shaped for an older client; the option is
  there because the common case has nothing to say, and the struct exists so the invariant
  "a count comes with a cursor" lives in the type rather than in prose.

  ```rust
  /// Present exactly when settled items older than the tail were left out of `accumulated`.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub omitted: Option<OmittedItems>,

  #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
  pub struct OmittedItems {
      /// How many settled items precede the tail and were not sent.
      pub count: u32,
      /// Where paging starts (`GET …/items?before=`): the first item at or after the cut, open or
      /// completed. A position, not a content reference, which is why an open item serves as well.
      pub before: ItemId,
  }
  ```

  The four struct literals named above gain the field (`omitted: None` in the three tests that do
  not exercise it).

- **D4. The route.**

  ```
  GET /api/projects/{id}/threads/{thread_id}/turns/{turn_id}/items?before=<ItemId>&limit=N
  → 200 { "turn_id": …, "items": [WireItem…], "has_more": bool }   // oldest-first
  ```

  `limit` defaults to `[history] turn_items` and is clamped to 1–100 (a new
  `MAX_ITEM_PAGE_ITEMS: usize = 100` beside `MAX_HISTORY_PAGE_TURNS`). Resolution mirrors
  `turn_item`: thread missing → 404; runtime first (`runtime.live_items_page(turn_id, before,
  limit)`, `Some` only when `turn_id` is the live turn); then the payload
  (`store.load_turn_items(project, thread, turn)`, factored out of `load_turn_item`, `None` when the
  turn is not indexed); then runtime again for the commit race; nothing → 404. A `before` that
  names no item of the resolved list → 404 (unlike the history route, which treats an unknown turn
  cursor as "newest page": an item cursor comes from this turn's own snapshot or page, so an
  unknown one is a client error, and answering it with the newest page would re-send rows the
  client already has). Without `before`, the page ends at the newest item.

  **Live page content.** Settled completed items (D2) whose entry index is strictly below the
  cursor's, the last `limit` of them, oldest-first; `has_more` is whether any settled completed
  item precedes the page. Projection: `WireItem::from_item_with_outputs(item, command_descriptor,
  tool_descriptor).with_reasoning_preview()` (D5). Open and unsettled items are never in a page;
  they are in the snapshot.

  **Persisted page content.** `payload.items` is display order; the page is the `limit` items
  before the cursor's position, projected with `WireItem::from_persisted_item`, which already
  applies the preview and reduces outputs to descriptors (it is what `WireTurn::from` uses).

  Pages are stable while the turn continues: entries never move, so an item's position never
  changes, and new items only land after the cursor. An item that was unsettled at snapshot time
  and settled afterwards is already rendered, so a page that happens to include it upserts the row
  in place. The count on the marker can therefore read one or two higher than the rows a full
  walk adds; that is accepted.

  **Authority note for §13.6.1.** This is the first HTTP read of the **Active transcript**
  authority. Its clock is entry position (the slot order L1 made stable), its delivery class is
  bootstrap/page, like the Completed transcript's history route. The table gains no row; a sentence
  under it says so.

- **D5. Reasoning preview in the snapshot.** `expand` projects a completed reasoning item with the
  same 1 KiB head preview history uses, through a new `WireItem::with_reasoning_preview(self) ->
  Self` factored out of `from_persisted_item`. An open note keeps its full coalesced text. The
  browser already handles a previewed row: it marks it truncated and fetches the full note from the
  item endpoint on expand or copy (RP2), and the item endpoint keeps returning full text for live
  items (`wire_turn_item` uses `from_item_with_outputs`, no preview). RP1's sentence "Live events,
  reconnect snapshots, item reads, and persistence retain full text" becomes "Live events, item
  reads, and persistence retain full text; a reconnect snapshot carries the preview for a completed
  note and the full text for one still streaming."

- **D6. Browser.**
  - New state `state.partialTurn: null | { turnId, oldestItemId, remaining, hasMore }` and
    `state.loadingTurnItems: false`. Set in `renderLiveTurnSnapshot` when `snap.omitted` is present
    (`remaining = snap.omitted.count`, `oldestItemId = snap.omitted.before`),
    after `renderLiveTurnUserInput` and before the replay. Cleared wherever `oldestTurnId` is
    cleared (`app.js:2742`, `:2828`, `:4096` in `resetTranscriptForAuthoritativeSnapshot`) and in
    `reconcileInFlightTurn`, whose `removeTurnRows` deletes the marker along with the live rows.
    It is **not** cleared by `turn_completed`: the omitted items are still unloaded, the marker
    stays, and the route serves the rest from the payload once the turn commits.
  - **Marker row.** A new row kind `.msg.turn-gap` drawn with `bubble("turn-gap", "")` right after
    the prompt row, so `appendBubble` stamps it with the turn id. Body: the text "N earlier items
    in this turn are not shown" and a button "Load earlier items". Styled beside `.msg.notice`
    (`app.css:858`) and the `.approval-actions button` rule (`:446-447`), with `max-width:100%`
    and `min-width:0` on the flex children so it never widens the page.
  - **Paging.** `loadTurnItemsPage()` guards with `captureActiveViewIdentity` /
    `activeViewIdentityIsCurrent` exactly as `loadHistoryPage` does, GETs
    `…/turns/${turnId}/items?before=${oldestItemId}`, and on success renders into a detached
    container with `state.renderTarget` set, `state.currentRenderTurnId = turnId`, and
    `state.activeTaskGroup = null`, calling `addItem(item, turnId, true)` per item, as
    `renderPersistedTurn` does. One pre-step per page: a `user_message` item whose text matches
    the turn's existing prompt row (`.msg.user[data-turn=turnId]` in the transcript) is registered
    onto that row with `registerRenderedItemBody` and `markRenderedItem` instead of being rendered,
    because `provisionalUserBodyForTurn` cannot see the transcript from a detached container and
    would otherwise draw a second prompt. The container's rows are inserted immediately **after
    the marker row** (pages walk backward, so each page is older than the last and belongs
    directly under the marker), then `collapseSupersededReasoningRows(transcript)`, then the
    viewport is kept with the history page's height-delta rule. The marker's count becomes
    `remaining - items.length`, `partialTurn.oldestItemId` becomes `items[0].id`, and when
    `has_more` is false the marker is removed and `partialTurn` set to null.
  - **Precedence.** `onTranscriptScroll` and `maybeAutoFillHistory` page the partial turn first:
    while `state.partialTurn?.hasMore` and not `loadingTurnItems`, they call `loadTurnItemsPage`
    and return; only when the turn is complete do they fall through to `loadHistoryPage`. The
    history gates (`hasMoreHistory`, `loadingHistory`, `oldestTurnId`) are untouched, and
    `taskHistoryLoading` keeps its veto on autofill.
  - **Known limitation, accepted.** A live `item_completed` for a settled item that was omitted
    and never paged in creates a row at the bottom instead of upserting one above. The producers
    of repeat completions are running commands, tools and file changes, which D2 keeps out of the
    omitted set. If a real producer of late re-completions of settled items appears, the fix is an
    id list inside `OmittedItems`, not this milestone.

- **D7. Config.** `HistoryConfig.turn_items: usize`, default 100, added to `impl Default` (the
  struct is `#[serde(default)]`, so an absent key reads the default). No validation: the use sites
  take `max(1, turn_items)` like the route clamps `limit`. Documented in `README.md`'s table row
  and `config.example.toml`'s `[history]` block.

- **D8. Observability.** The `build_live_snapshot` debug line gains `omitted_items` and
  `tail_items`. The route logs one debug line per page with `project_id`, `thread_id`, `turn_id`,
  `source` (`live`/`persisted`), `before`, `returned`, `has_more`, and one debug line naming the
  reason for each 404 (`unknown_turn`, `unknown_cursor`).

## Changes

### A. `crates/giskard-core/src/item.rs` (+12)

`pub fn file_change_status_is_running(status: &str) -> bool`, mirroring `command_status_is_running`
through `normalized_command_status`, with a unit test. Confirm the status strings the Codex mapper
writes into `FileChange.status` first and include them.

### B. `crates/giskard-proto` (`lib.rs` +25 incl. tests, `wire.rs` +20/−10)

D3's `omitted: Option<OmittedItems>` on `LiveTurnSnapshot` and the `OmittedItems` struct. Extend
the two round-trip tests at `lib.rs:1377` and `:1406` so `None` serializes to nothing and a
`Some` round-trips with both members.
`WireItem::with_reasoning_preview(self) -> Self` holding the body of `from_persisted_item`, which
becomes `Self::from(item).with_reasoning_preview()`.

### C. `crates/giskard-persist` (`config.rs` +10, `store.rs` +30/−12)

`turn_items` per D7. `pub async fn load_turn_items(&self, project, thread, turn) ->
Result<Option<Vec<Item>>, PersistError>` containing the current body of `load_turn_item` up to the
item search; `load_turn_item` becomes a one-line find over it. A store test that `load_turn_items`
returns display order for a payload written with a non-monotonic amendment (the existing amendment
fixtures at `store.rs:4096-4121` show the shape).

### D. `crates/giskard-server/src/thread_runtime/live.rs` (+150 non-test, tests +260)

- `fn is_settled(&self) -> bool` on `LiveItemEntry` (D2).
- `pub fn snapshot(&self, thread_id, tail_items: usize)`: compute the cut; expand riders, then the
  tail, then pending starts, then usage; fill `omitted` when the cut left anything out. Keep the
  `answered_*` derivations over all `Event` entries as today (a card not sent but listed as
  answered is harmless, and the client ignores unknown ids).
- `pub fn items_page(&self, thread_id, turn_id, before: Option<ItemId>, limit) ->
  Option<LiveItemsPage>` where `LiveItemsPage { items: Vec<WireItem>, has_more: bool }`; `None`
  when the turn id does not match; `Some` with an `Err(UnknownCursor)`-style signal when `before`
  names no entry (an enum `LiveItemsLookup { Page(LiveItemsPage), UnknownCursor, NotLive }` keeps
  the route's three outcomes explicit).
- `expand`: completion through `.with_reasoning_preview()` (D5).
- Doc comments on `LiveTurn` and `snapshot` updated for the cut.

### E. `crates/giskard-server/src/thread_runtime.rs` (+45)

`ThreadRuntime::live_snapshot()` stays and delegates to a new `live_snapshot_bounded(tail_items)`
with `usize::MAX`, so `tests/e2e_smoke.rs:1692,4118` and `live_snapshot_shape.rs` compile unchanged;
`ws.rs` switches to the bounded one. `ThreadRuntime::live_items_page(turn_id, before, limit)` and
the matching `ThreadRuntimeSupport` method, modelled on `live_item` (`:596-606`).

### F. `crates/giskard-server/src/ws.rs` (+12)

Read `turn_items` through `history_limit_or_default(state, thread_id, "subscribe_live_tail",
|config| config.history.turn_items, 100)`, take `max(1, …)`, call `live_snapshot_bounded`, and add
`omitted_items` and `tail_items` to the debug line at `:1474-1482`.

### G. `crates/giskard-server/src/routes.rs` (+110)

Route registration after the `items/{item_id}` route (`:138-140`); `MAX_ITEM_PAGE_ITEMS`;
`TurnItemsQuery { before: Option<ItemId>, limit: Option<usize> }`; route-local
`TurnItemsResponse { turn_id, items: Vec<WireItem>, has_more }`; handler `turn_items` per D4 with
the D8 logs. Reuse `history_limit_or_default` for the default limit.

### H. `crates/giskard-server/static/app.js` (+190) and `app.css` (+15)

Per D6: state fields, the marker row, `loadTurnItemsPage`, `renderTurnItemsPage`, the precedence
change in `onTranscriptScroll` and `maybeAutoFillHistory`, the resets. Place the new functions
after `renderPersistedTurn` and before `onTranscriptScroll`, outside the slices `tests/ui.rs`
pins (`:2066` ends at `function maybeAutoFillHistory`; `:2309` ends at the comment `// Render one
persisted turn from history`; the L1 pin at `:1498` ends at `function transcriptShouldStickToBottom`).
The marker is a new row kind: add it to `mobile-overflow.spec.ts` (Tests below). A visible change
means regenerating `docs/screenshots/ide-{desktop,mobile}.png` only if the screenshot scenario shows
a marker; it does not (the seeded thread is short), so no regeneration.

### I. Replay server and e2e helpers (+70)

- `SCRIPTED_LONG_TURN_TRIGGER = "Stream twenty replies, then hold this turn open."`: twenty
  completed agent messages, each `ItemStarted` + `ItemCompleted` with text `Scripted reply K of
  20` (no deltas needed), then the streaming-hold tail (a reasoning note streamed and left open),
  registered in `active_steering_turns` like the other holds so Stop ends it. Mirrored in
  `helpers.ts`.
- `write_config` (`:1144-1168`) gains `[history]\nturn_items = 8` with a comment that the e2e long
  turn relies on it. No existing scenario reaches 8 items in one turn (`mobile-overflow` injects
  seven), so no other spec changes.

### J. Documentation

- `specs/giskard-specification.md`: version 1.101; an amendment note above the 1.100 one;
  **Changelog (1.100 → 1.101), bounded live snapshot**: LP1 the cut and what rides along (D1,
  D2), LP2 the `omitted` field (D3), LP3 the items route and its resolution and cursor rules (D4), LP4
  the browser marker, paging and precedence (D6), LP5 the reasoning preview in snapshots (D5) and
  the RP1 amendment, LP6 `turn_items`. Update the snapshot sketch (`:4112`), step 2 of the
  subscribe sequence (`:4237`), the "bounded per item" paragraph after `:4248`, the REST bullet
  list (`:4007`) with the items route, RP1 (`:326`), and the authority-table note (`:3968`).
- `docs/api-endpoints.md`: the inventory at `:1-27` and a paragraph after the item-endpoint one
  (`:206-213`) giving the shape, defaults, clamp, resolution order, the 404 cases, and the preview.
- `README.md:283` table row and `:157` ("scrolling the transcript to the top lazy-loads older
  history, and the earlier items of a long turn"); `config.example.toml:71-77`.
- The overall plan: L2 marked implemented when the PR exists, landed when it merges.

### Not touched

- `WireTurn`, `HistoryDelta`, `renderPersistedTurn`, `renderHistoryPage`: a persisted turn still
  arrives whole. That is L3.
- `giskard-persist` formats, `history.jsonl`, payloads, `giskard-admin`.
- The item, command-output, tool-output and diff routes.
- `tasks.rs`, `requests.rs`, `outputs.rs`, `diffs.rs`, `gate.rs`, both adapters.

## Tests

**Unit, `live.rs`** (public API only, through `snapshot(thread, n)` and `items_page`):

1. *No cut under the limit.* 5 items, `n = 100` → identical to `snapshot(thread, usize::MAX)`,
   `omitted == None`.
2. *The cut.* 30 completed messages, `n = 10` → `turn_started`, then exactly items 21–30 as
   start+completion pairs, `omitted == Some { count: 20, before: items[20] }`.
3. *Riders.* Before the cut: an open reasoning item, a command completed with status `running`,
   an outstanding approval, an answered approval, a received+resolved server request, an
   outstanding server request, an `Error`, a `Notice`, a `DiffUpdated`. The snapshot expands, in
   arrival order and before the tail: the open item (start + delta), the running command
   (start + completion), the outstanding approval, the outstanding server request's received
   event, the error, the notice; and omits the answered approval, both server-request events of
   the resolved one, and the diff. `outstanding_approvals` / `outstanding_server_requests` (the
   existing test mirrors) return exactly the two outstanding ids.
4. *Settled.* `is_settled` through observable behaviour: a file change with `status:
   "in_progress"` rides along, one with `"completed"` is omitted; same for tool `running` vs
   `completed`.
5. *Cursor position.* `omitted.before` is an open item when the first entry at the cut is open.
6. *Pages from the live buffer.* With the state of test 2: `items_page(before = items[20], 10)` →
   items 11–20 oldest-first, `has_more == true`; `before = items[10]` → 1–10, `has_more == false`;
   `before = None` → the newest 10; unknown id → `UnknownCursor`; another turn id → `NotLive`;
   limit 3 → the 3 nearest the cursor. A page never contains an open or running item.
7. *Preview.* A completed 3 KiB reasoning note expands with `text.len() <= 1024` and `preview`
   set; a 100-byte one has no `preview`; an open note keeps its full delta. The same page item
   carries the preview.
8. *Pending starts and usage still last.* Port the L1 ordering tests to the new signature with
   `usize::MAX`; they pass unchanged.
9. *Persistence blocked.* A `TurnCompleted` before the cut rides along (D1).

**Store, `store.rs`:** `load_turn_items` display order with an amended payload; `None` for an
unindexed turn; flat layout returns the turn's items.

**Integration, `tests/live_turn_paging.rs` (new, about 220 lines):** `TestServer::builder(...)
.config("[history]\nturn_items = 10\n")`; seed as `live_snapshot_shape.rs` does: 30 completed
messages, one open note, one approval before the cut. Subscribe and assert `omitted.count == 20`,
`omitted.before`, the approval present in `accumulated` before the tail, the tail being items
21–30. Then over HTTP with the cookie: `items?before=<oldest>` → 11–20 oldest-first,
`has_more: true`; walk to the start; `limit=3`; unknown cursor → 404; the turn under another
thread → 404; a persisted turn seeded with `fixtures::completed_turn` carrying 25 items →
`items` without `before` → the newest 10, then two more pages to exhaustion, reasoning items
previewed. Unauthenticated → the same status the history route gives.

**End-to-end, `tests/e2e/tests/live-turn-paging.spec.ts` (new, about 90 lines):** send the long
trigger, wait for twenty agent rows and the open note, reload; expect the prompt row, then a
`.msg.turn-gap` reading "12 earlier items", then agent rows 9–20 in order and the open note
expanded; press the marker → rows 1–8 appear between the marker's place and row 9, in order, the
marker is gone, and `.msg.agent` count is 20 with no duplicate text; Stop the turn. A second test
scrolls to the top instead of pressing and asserts the same end state. A third checks that after
loading all items, scrolling to the top still pages older *turns* (create one completed turn
first). `mobile-overflow.spec.ts`: call `renderLiveTurnSnapshot({ thread_id, turn_id, accumulated:
[], omitted: { count: 3, before: "x" } })` from `page.evaluate` after `openThread` and include
the resulting `.msg.turn-gap` in the overflow check with the long token in its text.

**Static, `tests/ui.rs` (+15):** pin that `onTranscriptScroll` and `maybeAutoFillHistory` test
`state.partialTurn` before `hasMoreHistory`, and that `loadTurnItemsPage` captures and re-checks
the active view identity like `loadHistoryPage`.

## Exit checks

- `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` green.
- `tests/e2e/run.sh` green including the new spec and the extended overflow spec.
- With the replay server and `turn_items = 8`: reload during the long turn and read the
  `build_live_snapshot` line: `omitted_items=12`, `accumulated_events` is 1 + 8×2 + 2 (the open
  note's start and delta).
- `grep -n 'turn_items' README.md config.example.toml docs/api-endpoints.md specs/giskard-specification.md` each hit.
- `git diff --stat` shows no change under `crates/giskard-persist/src/history.rs` or
  `crates/giskard-persist/src/bin/`.

## Signs the step has gone wrong

- A reload during a long turn shows an approval card below the tail: riders were expanded after
  the tail, or a resolved request's `resolved` event was sent without its `received` one.
- Paging adds a second prompt bubble: the `user_message` pre-step in `renderTurnItemsPage` is
  missing or searched the container instead of the transcript.
- Rows paged in appear above the marker or below the tail: the insertion anchor is wrong; it must
  be the marker's next sibling.
- The Tasks menu's `Open` on a background command in the live turn reports the row unavailable:
  D2 treated a running completion as settled.
- Scrolling to the top of a partially loaded turn loads older *turns*: the precedence in
  `onTranscriptScroll` or `maybeAutoFillHistory` is inverted.
- `tests/e2e_smoke.rs` stops compiling: `live_snapshot()` was removed instead of kept as the
  unbounded delegate.
- A test constructs `LiveTurnSnapshot { … }` without the new field and fails to compile: the four
  literals are listed above.
- `history.jsonl` or a payload format changed: out of scope.

## Follow-ups (not this milestone)

- **L3:** bound `WireTurn.items` the same way and reuse the route and the marker for persisted
  turns; `load_turn_items` and `with_reasoning_preview` are built for it.
- An id list inside `OmittedItems` if late re-completions of settled items turn out to exist.
- Dropping `DiffUpdated` from the snapshot entirely (it already falls out before the cut).

## Size

About 410 lines in `live.rs` including tests, 45 in `thread_runtime.rs`, 110 in `routes.rs`, 12 in
`ws.rs`, 55 in `giskard-proto`, 40 in `giskard-persist`, 12 in `giskard-core`, 205 in
`app.js`/`app.css`, 70 in the replay server and helpers, 220 in the new integration test, 90 in the
new e2e spec plus 15 in the overflow spec, 15 in `tests/ui.rs`, 60 in the spec and docs. One PR;
the browser commit (H) and the server commits (B–G) can be reviewed separately within it.
