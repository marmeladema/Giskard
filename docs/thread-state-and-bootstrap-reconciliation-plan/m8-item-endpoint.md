# M8 — Item endpoint

Implementation plan for milestone M8 of
[`thread-state-and-bootstrap-reconciliation-plan.md`](../thread-state-and-bootstrap-reconciliation-plan.md).
Written against `main` at `807ba2b` (S11 landed, spec 1.93). Every file and line reference below
was checked against that tree; re-check them if the branch has moved. The milestone text is the
authority on scope and non-goals; this document says how to land it and how to prove it landed.

## What lands

1. A derived in-flight item read, `live_item`, returning a `LiveItem` (`Started` or `Completed`
   with its output descriptors) from the live buffer's own lifecycle events. Nothing stored.
2. A shared persisted lookup, `Store::load_turn_item`, extracted from the preambles that
   `load_command_output` and `load_tool_output` currently duplicate.
3. `GET /api/projects/{id}/threads/{thread_id}/turns/{turn_id}/items/{item_id}` returning a
   `state`-tagged JSON body, running items first from the runtime, completed ones from the runtime
   or the persisted turn payload, with the same 404 rules as the two existing item routes.
4. A bounded reasoning preview on completed turns delivered by history and bootstrap, with the
   first non-blank line always whole, applied only where completed turns become `WireTurn`s.
5. Browser: expanding a previewed reasoning row fetches the item; the row copy button never yields
   a prefix; a turn the browser watched live never shrinks when it returns from history.
6. Spec 1.94, `docs/api-endpoints.md`, README, and tests for every path above.

## Facts the plan rests on

- The live buffer holds every owned lifecycle event of the in-flight turn. `apply_owned` applies
  through `apply_to_runtime` (`registry/event_forwarder.rs:1607`), the entry appends at
  `thread_runtime.rs:805`, and `LiveTurnState::item_events` (`thread_runtime/live.rs:245`) already
  filters one item's `ItemStarted`/`ItemCompleted` events. The registry uses it at
  `registry.rs:1766-1780`. Late events are applied with `append_live: false`
  (`event_forwarder.rs:1413`), so the buffer never sees a settled turn's events.
- What the buffer keeps of a completed item: a command item's `output` is replaced by the
  descriptor preview and the descriptor kept in `LiveTurn.command_output_descriptors`
  (`live.rs:147-155`); a tool item's `output` is set to `None` and its descriptor kept in
  `LiveTurn.tool_output_descriptors` (`live.rs:178-188`). The reconnect snapshot rebuilds the wire
  item from those three things through `WireItem::from_item_with_outputs` (`live.rs:293-303`,
  `wire.rs:494`).
- `AgentEvent::ItemStarted` carries `ItemStart` (`core/src/event.rs:43-47`; `core/src/item.rs:46-56`),
  and the wire already has its browser-safe mirror `WireItemStart` with `From<ItemStart>`
  (`proto/src/wire.rs:138-146`, `:544-566`), which drops the harness-native sub-agent id. The
  endpoint must return `WireItemStart`, not core `ItemStart`.
- Persisted lookups: `load_command_output` (`persist/src/store.rs:1540-1612`) and
  `load_tool_output` (`:1613-1660`) share a 26-line preamble: `ensure_migrated`, the turn-record
  existence check, the `ThreadLayout::Flat` versus per-turn payload branch, and the item search.
- The two item routes and their handlers: registration at `routes.rs:123-134`; `tool_output`
  (`:4663-4700`) checks `load_thread(project, thread)`, reads the runtime through
  `state.registry.thread_runtime(thread_id)`, reads the persisted copy, then re-checks the runtime
  before answering from the persisted copy (the persistence race); `load_command_output`
  (`:4446-4547`) uses `verified_thread_runtime`. `ApiError` is at `:4908`.
- Completed turns become `WireTurn` in exactly three places, all completed-turn deliveries:
  HTTP history (`routes.rs:4755`) and the two `HistoryDelta` frames (`ws.rs:532`, `:552`), each
  through `From<Turn> for WireTurn` (`wire.rs:806-820`), which maps items through
  `From<Item> for WireItem` (`:467`) and `From<ItemPayload>` (`:582`, `Reasoning` arm `:587`).
  Live `ItemCompleted` events take a different path (`from_agent_event`, `:357`;
  `from_item_with_outputs`, `:494`), so a preview applied in `From<Turn>` touches no live event.
- `giskard-proto` depends only on `giskard-core`, `serde`, `serde_json`, `chrono`
  (`proto/Cargo.toml:8-12`); `bounded_preview` lives in `giskard-persist` (`preview.rs:27`), which
  proto cannot use. Core already hosts `command_output_tail_preview` and
  `command_output_logical_lines` (`core/src/item.rs:330`, `:322`); the head preview goes beside them.
- Browser: `reasoningSummaryText` (`app.js:5754`) takes the first non-blank line of `p.text`;
  `applyReasoningRow` (`:5832`) builds the toggle and its `onclick` calls `setReasoningExpanded`
  (`:5790`); `renderItemBody` (`:7805`) sets `msg.dataset.copyText = p.text` (`:7813`) for
  reasoning rows and renders through `renderMarkdown(body, p.text ...)`, which posts to
  `/api/projects/{id}/threads/{thread_id}/render` (`:7961`); `attachRowCopy` (`:5264`) copies
  `dataset.copyText`; `addItem` (`:7531`) upserts a repeated item id by re-rendering the row;
  rows carry `dataset.turn` and `dataset.item` (`reasoningRowKey`, `:5775-5781`);
  `loadToolOutputOverlay` (`:9415-9475`) is the lazy-fetch pattern with an `AbortController`,
  a generation guard and a bounded retry; URL builders sit at `:9354-9359`.
- Tests: `tests/ui.rs` is one substring test (`index_page_is_served_and_public`) with the lazy
  tool-output block at `:2837-2856` and the Markdown assertion at `:1458-1461`;
  `tests/history_sync.rs` is the HTTP-plus-fixtures pattern (`setup`, `fixtures`, `TestServer`);
  `giskard-testenv::fixtures::{persist_primary_thread, completed_turn}` (`fixtures.rs:35`, `:120`)
  and `PersistStore::append_turn` (`store.rs:1217`) seed persisted turns; `tests/turn_steering.rs`
  shows a `FakeHarness` script with `Gate`s. The scripted replay reasoning note
  (`bin/giskard-server-replay.rs:108-109`, `:823`) is under 100 bytes, so
  `tests/e2e/tests/reasoning.spec.ts` never sees a preview.
- Spec: version line `specs/giskard-specification.md:12`, newest amendment blockquote at `:14`,
  newest changelog block at `:191`, RN1 at `:282-286` (says expanding "costs no re-render or
  fetch" and copy "still yields the whole note"). Tags `IE*` and `RP*` are unused.
  `docs/api-endpoints.md` lists the item routes at `:12-15` and describes them at `:100-129`.
  `README.md:136-142` describes reasoning rows and completed command rows.

## Decisions

- **D1. `LiveItem` carries the descriptors.** `Completed { item, command_output, tool_output }`
  rather than `Completed(Item)`, so the endpoint's completed branch is one call to
  `from_item_with_outputs` with the same inputs the snapshot uses, and no descriptor is ever
  re-derived from a stripped item.
- **D2. `Started` is served as `WireItemStart`.** The core `ItemStart` carries the harness-native
  sub-agent id; the wire mirror redacts it. The endpoint returns what `ItemStarted` already put on
  the wire, byte for byte in shape.
- **D3. One persisted lookup.** `Store::load_turn_item(project, thread, turn, item_id) ->
  Result<Option<Item>, PersistError>` owns the shared preamble; both output loaders call it. This
  is the only refactor in the milestone and it is mechanical.
- **D4. Reasoning preview shape.** `WireItemPayload::Reasoning` gains
  `preview: Option<WireTextPreview>`, present only when `text` is a prefix. `text` stays the field
  the browser reads, so the summary line, Markdown rendering and copy paths change only where the
  milestone says they change. `WireTextPreview { prefix_bytes, total_bytes, total_lines }`.
- **D5. The cut is a core function.** `giskard_core::item::reasoning_head_preview(text, max_bytes)
  -> (String, bool)` and `REASONING_PREVIEW_MAX_BYTES: usize = 1024` live in `giskard-core`,
  beside `command_output_tail_preview`. The rule: if `text.len() <= max_bytes`, whole; otherwise
  cut at the last newline at or below `max_bytes`, but never before the end of the first
  non-blank line, which is kept whole whatever its length. A note with no newline is therefore
  returned whole, untruncated. UTF-8 safety follows from cutting only at newlines.
- **D6. Applied in `From<Turn>` only**, through a new `WireItem::from_persisted_item(item)` that
  the turn conversion maps items with. `From<Item> for WireItem` and `From<ItemPayload>` are
  unchanged, so every live event, the reconnect snapshot, and the item endpoint itself keep full
  text.
- **D7. Endpoint semantics.** `Content-Type: application/json`, no `ETag`, no caching headers.
  Order: `load_thread` containment, runtime `live_item(turn, item)`, persisted `load_turn_item`,
  runtime again, persisted result. 404 for a missing thread, unknown turn, unknown item, an item
  whose turn id does not match, or a live turn id that is not the entry's live turn. No other
  status is introduced.
- **D8. Browser rule for shrinking.** In `renderItemBody`'s reasoning branch, if the incoming
  payload carries `preview` and the row's existing `dataset.copyText` is longer than the incoming
  `text`, keep the existing text and do not mark the row truncated. That one place covers the
  history upsert and the finalize path alike, mirroring `mergeRunningOutput` (`app.js:5603`).
- **D9. The sub-agent link fold in `registry.rs:1766-1780` is not touched.** It stops at the
  newest event that carries sub-agent information, which differs from "last lifecycle event
  wins"; it is not a `live_item` consumer.

## Changes

Order is the order that keeps the tree compiling.

### A. `crates/giskard-core/src/item.rs` (+40, tests +40)

After `command_output_tail_preview` (`:330`):

```rust
/// Bytes of reasoning text a completed turn carries eagerly on the wire (M8, spec RP1).
pub const REASONING_PREVIEW_MAX_BYTES: usize = 1024;

/// Head prefix of a reasoning note. Whole when it fits; otherwise cut at the last newline at or
/// below `max_bytes`, never before the end of the first non-blank line, which is always kept
/// whole. Returns the prefix and whether anything was dropped.
pub fn reasoning_head_preview(text: &str, max_bytes: usize) -> (String, bool)
```

Unit tests, in the existing `mod tests` of that file: a note under the budget is whole and
`false`; a multi-line note over the budget is cut at a newline at or below the budget and `true`;
a note whose first non-blank line alone exceeds the budget keeps that line whole and `true`; a
single-line note over the budget is returned whole and `false`; leading blank lines do not count
as the first non-blank line; the cut never splits a character (use a multi-byte string across the
boundary).

### B. `crates/giskard-proto/src/wire.rs` and `lib.rs` (+60, tests +40)

- `wire.rs:189-191`: `Reasoning { text: String, #[serde(default, skip_serializing_if =
  "Option::is_none")] preview: Option<WireTextPreview> }`. Add
  `pub struct WireTextPreview { pub prefix_bytes: u64, pub total_bytes: u64, pub total_lines: u64 }`
  with the usual derives. Update the one construction at `:587` to `preview: None`; run
  `rg -n "Reasoning \{" crates/giskard-proto crates/giskard-server` for any test construction.
- Add `impl WireItem { pub fn from_persisted_item(item: Item) -> Self }`: convert through
  `From<Item>`, then if the payload is `Reasoning`, call
  `reasoning_head_preview(&text, REASONING_PREVIEW_MAX_BYTES)`; when truncated, replace `text`
  with the prefix and set `preview` with `prefix.len()`, `text.len()`, and
  `command_output_logical_lines(&text)` (all from the full text, computed before replacement).
- `wire.rs:811`: `items: t.items.into_iter().map(WireItem::from_persisted_item).collect()`.
- Add the response type, exported from `lib.rs:13-17`:

  ```rust
  #[derive(Debug, Clone, Serialize, Deserialize)]
  #[serde(tag = "state", rename_all = "snake_case")]
  pub enum WireTurnItem {
      Started { item: WireItemStart },
      Completed { item: WireItem },
  }
  ```

  Also export `WireTextPreview` and `WireItemStart` from `lib.rs:13-17`; neither is exported
  today (`WireItemStart` is only reachable through `WireAgentEvent`), and `routes.rs` needs both.
- Tests: `from_persisted_item` truncates a long reasoning note and leaves an agent message and a
  short note untouched; `From<Turn>` applies it while `from_agent_event(ItemCompleted)` does not;
  `WireTurnItem` round-trips with `"state":"started"` and `"state":"completed"`; a `Reasoning`
  payload without `preview` serializes without the key.

### C. `crates/giskard-persist/src/store.rs` (net −30, tests +40)

- Add `pub async fn load_turn_item(&self, project, thread, turn, item_id) ->
  Result<Option<Item>, PersistError>` holding the preamble currently at `:1547-1572` (and
  identically at `:1620-1645`): `ensure_migrated`, `load_turn_records_unlocked` existence check,
  the `ThreadLayout::Flat` branch reading `paths.history()` through `parse_turn_history`, the
  per-turn branch reading `paths.turn_payload(turn)` through `history::read_turn_payload`, then
  `items.into_iter().find(|item| item.id == item_id)`.
- `load_command_output` and `load_tool_output` become: `let Some(item) =
  self.load_turn_item(..).await? else { return Ok(None) };` followed by their existing payload
  matching and status checks. Behaviour is unchanged; the same `None` cases fall out in the same
  order.
- Tests in `mod tests` (`:2306`): per-turn layout returns the item and `None` for an unknown item
  or a turn id from another thread; the flat-layout case reuses the fixture the test at `:4823`
  uses. The two output loaders keep their existing tests green.

### D. Runtime read (+70, tests +60)

`crates/giskard-server/src/thread_runtime/live.rs`:

- Add, `pub(crate)`:

  ```rust
  pub(crate) enum LiveItem {
      Started(ItemStart),
      Completed {
          item: Item,
          command_output: Option<CommandOutputDescriptor>,
          tool_output: Option<WireToolOutput>,
      },
  }
  ```

  `CommandOutputDescriptor` (`:10`) and `WireToolOutput` (`:16`) are already imported in this
  file; add `Item` and `ItemStart` to the `giskard_core::item` import at `:10`.
- Add `pub fn live_item(&self, thread_id, turn_id, item_id) -> Option<LiveItem>` next to
  `item_events` (`:245`): return `None` unless `self.thread_id == Some(thread_id)` and the live
  turn's `turn_id` matches; walk `turn.events` in reverse and stop at the first
  `ItemCompleted`/`ItemStarted` whose `item.id == item_id`; clone that one event's item; for
  `Completed`, attach `command_output_descriptors.get(&item_id).cloned()` and
  `tool_output_descriptors.get(&item_id).cloned()`. Do not reuse `item_events`, which walks forward
  and clones every match.
- Tests beside the existing live tests: started only; started then completed (completion wins and
  carries the command descriptor it was appended with, via `append_with_command_output`); a tool
  completion carries its tool descriptor and `output: None`; a different turn id gives `None`; an
  unknown item gives `None`; a cleared turn gives `None`.

`crates/giskard-server/src/thread_runtime.rs`:

- `ThreadRuntimeSupport::live_item(&self, authority, turn_id, item_id) -> Option<LiveItem>` next
  to `live_item_events` (`:558`), same shape: `existing_entry`, `lock_unpoison`, delegate.
- `ResolvedThreadRuntime::live_item(&self, turn_id, item_id) -> Option<LiveItem>` next to
  `tool_output` (`:287`).
- Re-export `LiveItem` where `live::LiveTurnState` is imported (`:46`), so `routes.rs` can name it
  as `crate::thread_runtime::LiveItem`.

### E. `crates/giskard-server/src/routes.rs` (+90)

- Register after the tool-output route (`:131-134`):

  ```rust
  .route(
      "/api/projects/{id}/threads/{thread_id}/turns/{turn_id}/items/{item_id}",
      get(turn_item),
  )
  ```

- `async fn turn_item(State(state), AxumPath((project_id, thread_id, turn_id, item_id))) ->
  Result<Json<WireTurnItem>, ApiError>`, placed after `tool_output_response` (`:4700`):
  1. `load_thread(project_id, thread_id)` → `ApiError::NotFound` when absent (as `:4671-4680`).
  2. `let runtime = state.registry.thread_runtime(thread_id).await;`
     `let live = || runtime.as_ref().and_then(|r| r.live_item(turn_id, item_id));`
  3. `if let Some(item) = live() { return Ok(Json(wire_turn_item(item))); }`
  4. `let persisted = state.store.load_turn_item(project_id, thread_id, turn_id, item_id).await
     .map_err(|e| ApiError::Internal(e.to_string()))?;`
  5. `if let Some(item) = live() { return Ok(Json(wire_turn_item(item))); }` (the persistence race,
     exactly as `tool_output` does at `:4698-4700`).
  6. `let item = persisted.ok_or(ApiError::NotFound)?;`
     `Ok(Json(WireTurnItem::Completed { item: WireItem::from(item) }))`.
- `fn wire_turn_item(item: LiveItem) -> WireTurnItem`: `Started(start)` →
  `WireTurnItem::Started { item: start.into() }`; `Completed { item, command_output, tool_output }`
  → `WireTurnItem::Completed { item: WireItem::from_item_with_outputs(item, command_output,
  tool_output) }`.
- No ETag, no hashing, nothing on `spawn_blocking`: the body is item fields only.

### F. `crates/giskard-server/static/app.js` (+90) and `tests/ui.rs` (+20)

- `turnItemUrl(projectId, threadId, turnId, itemId)` next to `toolOutputUrl` (`:9357`), same
  encoding, path ending in `/items/${encodeURIComponent(itemId)}`.
- `renderItemBody` reasoning branch (`:7813` area): compute
  `const prev = msg.dataset.copyText || ""; const incoming = p.text || "";`
  `const text = (p.preview && prev.length > incoming.length) ? prev : incoming;` set
  `msg.dataset.copyText = text`; set `msg.dataset.reasoningTruncated = "1"` when `p.preview` is
  present and `text === incoming`, else delete it; render with `text`. Keep the
  `renderMarkdown(body, ...)` call and update the ui.rs literal at `:1459` to the new call text.
- `fetchReasoningNote(msg)`: dedupe per row on `msg._reasoningFetch`; resolve identity from
  `msg.dataset.turn` and `identityTokens(msg.dataset.item)[0]` (as `reasoningRowKey` does);
  capture `state.projectId`, `state.threadId`, `state.activeViewGeneration`; `fetch(turnItemUrl(..))`;
  require `response.ok` and `application/json`; require `body.state === "completed"` and
  `body.item.payload.kind === "reasoning"`; guard that the row is still connected and the
  captured identity still matches before applying; on success `renderMarkdown(body, full)`, set
  `dataset.copyText = full`, delete `dataset.reasoningTruncated`, refresh the summary label (it
  cannot change, since the first non-blank line is whole in the prefix, but re-deriving keeps one
  code path); on failure clear `_reasoningFetch` so the next expand retries and leave the prefix
  rendered. Follow the `loadToolOutputOverlay` guards (`:9448-9451`, `:9469-9471`); an
  `AbortController` is unnecessary since the row keeps the prefix either way.
- `setReasoningExpanded` (`:5790`): when `expanded` and `msg.dataset.reasoningTruncated`, call
  `fetchReasoningNote(msg)`.
- `attachRowCopy` (`:5264`): before reading `dataset.copyText`, if `el.dataset.reasoningTruncated`
  is set, `await fetchReasoningNote(el)`; if it fails, copy nothing and surface the same failure
  message path the row fetch uses rather than the prefix.
- `tests/ui.rs`: in the existing test, add substring assertions for `function turnItemUrl`,
  `/items/${encodeURIComponent(itemId)}\``, `function fetchReasoningNote`,
  `dataset.reasoningTruncated`, `body.state === "completed"`, `await fetchReasoningNote(el)` in the
  copy handler, and the longer-wins expression `prev.length > incoming.length`. Update `:1459`.
- No screenshot regeneration: nothing changes at the sizes the screenshot flow renders, and no row
  in the scripted replay exceeds the budget.

### G. Server tests: `crates/giskard-server/tests/item_endpoint.rs` (new, about 350 lines)

Model on `tests/history_sync.rs` (`setup`, `TestServer`, `fixtures`, `auth::login`, reqwest) and
`tests/turn_steering.rs` (a `Script` with `Gate`s). Cover:

1. **Persisted completed item.** Seed a thread with `fixtures::persist_primary_thread` and
   `store.append_turn` of `fixtures::completed_turn`; `GET` the agent-message item: 200,
   `state == "completed"`, full text, `Content-Type: application/json`.
2. **Reasoning preview and full fetch.** Append a turn whose reasoning item is a 3 KiB multi-line
   note. `GET .../history`: the item's `text` is a prefix ending at a newline at or below 1 KiB,
   starts with the note's first line, and `preview.total_bytes` equals the note length. `GET` the
   item: full text and no `preview` key. Then a note whose first line alone is 2 KiB: history
   carries that whole line as `text`.
3. **Running item, three states.** A script whose `start_turn` appends `TurnStarted`, an
   `ItemStarted` with a `CommandExecutionStart`, then waits on a `Gate`. `GET` during the wait:
   `state == "started"`, `item.command.command` is the scripted command line, and neither
   `output` nor `exit_code` is present in the JSON. Release the gate so the script appends the
   `ItemCompleted` and holds again: `state == "completed"`, `item.payload.output` is a descriptor
   object. Release to `TurnCompleted`, wait for it on the WebSocket: `GET` still returns the
   completed item, now from the persisted payload.
4. **404s.** Unknown item id; an item id from another turn of the same thread; the live item
   queried with a stale turn id; a thread id under another project; a thread that does not exist.
5. **Persistence race**, optional. No existing fixture holds the store's append open (search
   `PersistenceBlocked` under `crates/giskard-server/tests`), and building one is outside this
   milestone. The re-check in step 5 of the handler is kept because `tool_output` keeps it; if a
   held-persistence fixture appears later, add the test then.

### H. Documentation

- `specs/giskard-specification.md`: `:12` → `**Version:** 1.94`. Insert before `:14` a blockquote
  `> **Amendment — item endpoint and reasoning previews (1.94).** …` (three or four lines, the
  convention every version since 1.88 follows). Insert before `:191` the block
  `**Changelog (1.93 → 1.94), item endpoint and reasoning previews:**` with:
  - **IE1:** the route, the `state`-tagged body, `started` carrying `WireItemStart` exactly as the
    `item_started` event did, `completed` carrying the item with descriptors and never an output
    body or diff body.
  - **IE2:** resolution order (runtime `live_item`, then the immutable turn payload, runtime
    re-checked across the persistence race) and the single 404 for unknown, cross-container, or
    not-yet-started items; an item without `ItemStarted` is never synthesized.
  - **IE3:** `live_item` is derived from the live buffer's lifecycle events; no stored projection,
    no new `ThreadRuntimeEntry` component; persisted turns unchanged.
  - **RP1:** completed turns in history pages and bootstrap deltas carry `Reasoning.text` as a head
    prefix bounded by 1 KiB plus `preview { prefix_bytes, total_bytes, total_lines }` when cut;
    the cut is on a line boundary and the first non-blank line is always whole; live events,
    reconnect snapshots and the item endpoint carry full text; persisted text is unchanged.
  - **RP2:** expanding a previewed row fetches the item; the row copy button copies the whole note
    or nothing; a browser that watched the turn live keeps the longer text when the turn returns
    from history.
  - **RP3:** agent text is not previewed.
  - `:282` → `- **RN1 (amended by 1.94/RP2):**`, text kept.
- `docs/api-endpoints.md`: add the route to the list at `:12-15` and a paragraph after `:129`
  describing both body states, the descriptor-only rule, the resolution order, and the 404 cases.
  Add one sentence to the history description that reasoning text is previewed at 1 KiB with the
  first line whole and the item route serves the full note.
- `README.md:139-142`: extend the reasoning-notes sentence: a long note arrives as its first
  kilobyte; opening it fetches the rest, and copying always yields the whole note.
- `crates/giskard-harness-codex/README.md`: no change; nothing in the adapter moves.
- Plan document: set M8's status line to landed when merged, per its convention.

### Not touched

`giskard-harness`, `giskard-harness-codex`, `giskard-harness-replay`, `giskard-testenv`
(beyond any test helper the new test file needs, which stays in the test file),
`registry/event_forwarder.rs`, `thread_runtime/outputs.rs`, `thread_runtime/diffs.rs`,
`LiveTurnState::append_with_outputs` and `snapshot`, `ForwardedTurnState`, `ThreadRuntimeEntry`,
the persisted formats in `giskard-persist/src/history.rs`, the live `ItemCompleted` wire path,
`ItemDelta` handling, and `tests/e2e/`.

## Exit checks

Run on the finished tree; baselines are from `main` at `807ba2b`.

| # | Check | Expected |
| --- | --- | --- |
| A | `rg -n '/turns/\{turn_id\}/items/\{item_id\}"' crates/giskard-server/src/routes.rs` | 1 line, the new route |
| B | `rg -n "fn live_item\b" crates/giskard-server/src` | 3: `live.rs`, `ThreadRuntimeSupport`, `ResolvedThreadRuntime` |
| C | `rg -n "load_turn_item\(" crates/giskard-persist/src/store.rs` | the definition and 2 production callers, plus tests |
| D | `rg -n "reasoning_head_preview\|REASONING_PREVIEW_MAX_BYTES" crates --type rust` | core definitions, one proto use each, tests |
| E | `rg -n "from_persisted_item" crates/giskard-proto/src/wire.rs` | the definition and the `From<Turn>` use only |
| F | `rg -n "WireTurnItem\|WireTextPreview" crates/giskard-proto/src/lib.rs` | both exported |
| G | `rg -c "turnItemUrl\|fetchReasoningNote\|reasoningTruncated" crates/giskard-server/static/app.js` | ≥ 8 |
| H | `rg -n "Version:\*\* 1.94\|\*\*IE[1-3]:\*\*\|\*\*RP[1-3]:\*\*\|RN1 \(amended" specs/giskard-specification.md` | 8 lines |
| I | `rg -n "items/\{item_id\}\`" docs/api-endpoints.md` | the list entry plus the description |
| J | `git diff --stat main -- crates/giskard-harness crates/giskard-harness-codex crates/giskard-harness-replay crates/giskard-server/src/registry crates/giskard-server/src/thread_runtime/outputs.rs crates/giskard-server/src/thread_runtime/diffs.rs crates/giskard-persist/src/history.rs tests/e2e` | empty |
| K | `rg -n "struct ThreadRuntimeEntry" -A9 crates/giskard-server/src/thread_runtime.rs` | the eight fields unchanged |
| L | `cargo fmt --all --check` | clean |
| M | `cargo clippy --workspace --all-targets --locked -- -D warnings` | clean |
| N | `cargo test --workspace --locked` | green, including `item_endpoint`, `history_sync`, `ui`, `turn_steering` |
| O | `tests/e2e/run.sh` (optional locally; CI runs it) | `reasoning.spec.ts` unchanged and green |

## Signs the step has gone wrong

- A new field on `ThreadRuntimeEntry`, a new map keyed by turn or item, or an
  `ENTITY-AUTHORITY-EXCEPTION` comment: the read is derived, and the milestone's non-goals forbid
  the stored fold.
- `From<Item>` or `From<ItemPayload>` gained the preview: live events and the item endpoint would
  then truncate too. Only `from_persisted_item` cuts.
- `Item` fields became `Option`, or an `ItemStart` is turned into an `Item` anywhere: the tagged
  body exists so that neither is needed.
- The endpoint returns command output bytes, tool output JSON, or a diff body: those have their
  own routes and representations.
- `renderMarkdown(body, p.text` still asserted in `ui.rs` while the code changed, or a reasoning
  row copies `dataset.copyText` while `reasoningTruncated` is set.
- The persistence-race re-check was dropped from `turn_item`: a request landing between
  `settle_completed_turn` and the payload write would 404 for an item that exists.

## Size

About +450 production lines (core 40, proto 60, persist net −30 plus 40 for the lookup, runtime
70, routes 90, app.js 90, docs) and about +550 test lines. Under the plan's two-thousand-line
ceiling with room to spare; if it is not, something from M11 or M14 has crept in.
