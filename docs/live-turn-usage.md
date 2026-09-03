# Live turn usage and unknown-model context windows

Implementation plan for the `ignoring Codex model context window without a registered turn model`
warning and for live context-usage tracking during a turn. Written against `main` at `339244d`
(M7 merged, spec 1.84). Every file and line reference below was checked against that tree; re-check
them if the branch has moved.

## Problem

The Codex adapter emits one runtime-metadata event per turn, `AgentEvent::ContextWindowUpdated`,
and only when it knows the turn's model. The model is registered exactly once, when Giskard itself
issues `turn/start` with a model override (`giskard-harness-codex/src/lib.rs:1994-1998`). Every turn
that Codex starts on its own — a sub-agent thread's turns, an orphan's turns, a turn started from
the Codex CLI on a thread Giskard also displays, the compaction turn behind `thread/compact` — is
registered from the `turn/started` notification alone (`mapping.rs:427-435`), so `turn_models` has
no entry and every `thread/tokenUsage/updated` for that turn is discarded with the warning at
`mapping.rs:515-524`. The Codex `Turn` payload carries no model field (`codex-codes-0.151.2
protocol_generated/types.rs:8840-8863`), so nothing in the protocol can fill the gap
authoritatively.

Independently of the model question, usage is only ever surfaced to the browser at the end of a
turn: the mapper caches `tokenUsage.last` per turn (`mapping.rs:484-485`) and attaches it to
`TurnCompleted` (`mapping.rs:450`); the browser updates the gauge from that final usage
(`static/app.js:4284`). During a long turn the gauge is stale, a synthesized `Interrupted`
completion persists zero usage (`event_forwarder.rs:1237,1243`), and a reload mid-turn shows the
previous turn's footprint.

## Goals

1. A valid context window reported for the active turn is never discarded and never warned about
   because the model is unknown. The adapter does not guess or inherit a model, and no new
   thread-indexed authority map is added anywhere.
2. Context usage is tracked throughout the active turn and shown live: turn-scoped usage updates
   flow adapter → forwarder → hub → browser in event order, the gauge moves before completion, the
   final usage on `TurnCompleted` is retained exactly as today, and a delayed or replayed usage
   notification can never update a different turn at any of the three fences (adapter, forwarder,
   browser).
3. Per-model persistence (`record_model_context_window`) happens only when the event carries the
   model acknowledged by `turn/start`. Unknown-model turns never write `model_context_windows` and
   never change the thread's persisted `context_window`.

## Non-goals

- No change to `AgentHarness`, `ThreadUpdateSink`, or the resume-time restore path
  (`ThreadUpdate::ContextWindowRestored`, `giskard-harness/src/lib.rs:306-311`,
  `giskard-harness-codex/src/lib.rs:1805`, `registry.rs:473`). Restoration after `thread/resume`
  keeps its authoritative model from the resume response and is unrelated to active-turn usage.
- No change to `ThreadMetadata`, `ThreadState`, the metadata lane, or either persisted JSON format.
  The thread file's `context_window` cache and `model_context_windows` keep their meaning
  (`giskard-persist/src/store.rs:208-218`).
- No new `ServerMessage` variant. Live usage rides the existing transcript event lane as one more
  `WireAgentEvent` kind, which is the "Active transcript / ordered journal" row of spec §13.6.1.
- No change to how `TurnCompleted` usage is produced or folded into ledgers (spec §10.1, §10.2).
- No timers, no revision counters beyond the ones that exist, no per-turn persistence of the
  context window on the `Turn` record (listed under Pitfalls as a possible later extension).

## Ground truth

| Fact | Where |
| --- | --- |
| Mapper state: `turn_usage`, `turn_context_windows`, `turn_models`, `missing_context_model_turns`, `invalid_context_window_turns`, `active_turns`, each with an `ENTITY-AUTHORITY-EXCEPTION` comment | `giskard-harness-codex/src/mapping.rs:94-145` |
| `clear_active_turn` retains away every per-thread entry of those maps | `mapping.rs:208-220` |
| `register_active_turn` (no model) and `register_active_turn_with_model` (inserts `turn_models`, clears the warning dedup) | `mapping.rs:227-253` |
| `turn/start` registers the model only when `overrides.model` is `Some` | `giskard-harness-codex/src/lib.rs:1994-1998` |
| Server passes `model: Some(..)` for every turn it starts and every compaction it requests; the `model: None` sites are tests | `giskard-server/src/routes.rs:963-967, 4937-4941`; `registry/driver.rs:1101-1105` and `event_forwarder.rs:2110, 5036` (tests) |
| `TurnStarted` notification inserts `active_turns` and never touches `turn_models` | `mapping.rs:427-435` |
| `TurnCompleted` removes the active turn, takes `turn_usage` (default zero), clears the four other maps, attaches usage | `mapping.rs:437-465` |
| `ThreadTokenUsageUpdated`: empty `turnId` → `None`; not the active native turn → debug "routing Codex usage outside the active turn ledger" and `None`; cache `breakdown_to_usage(last)`; no window → `None`; invalid/zero window → warn once, `None`; unchanged window → `None`; no model → **warn once, `None`**; else insert `turn_context_windows` and emit `ContextWindowUpdated` | `mapping.rs:467-532` |
| `breakdown_to_usage` maps `input_tokens`, `output_tokens`, `total_tokens` | `mapping.rs:2504-2510` |
| Codex payload: `ThreadTokenUsage { last, model_context_window: Option<i64>, total }`; notification `{ thread_id, token_usage, turn_id }`; `Turn` has no model field | `codex-codes-0.151.2 protocol_generated/types.rs:8575-8586, 8590-8596, 8840-8863` |
| `AgentEvent::ContextWindowUpdated { thread, turn, model: ModelRef, context_window: u32 }` documented as runtime metadata persisted against the turn model; `thread_id()` arm; serde test | `giskard-core/src/event.rs:27-37, 108, 154-177` |
| `TokenUsage { input, output, total }`, `context_ratio` | `giskard-core/src/token.rs:5-38` |
| `Turn.usage: TokenUsage`; no context window on the turn record | `giskard-core/src/turn.rs:159-179` |
| `WireAgentEvent` has no variant for the event; `from_agent_event` returns `None` for it; test asserts that | `giskard-proto/src/wire.rs:37-100, 340, 1067-1078` |
| `LiveTurnSnapshot.accumulated: Vec<WireAgentEvent>` built through `from_agent_event`; `ItemCompleted` special-cased | `giskard-proto/src/lib.rs:219-231`; `giskard-server/src/runtime_live.rs:278-303` |
| Live buffer append: `turn.events.push(event)` then command-output delta compaction | `runtime_live.rs:119, 192-196, 445-505` |
| Runtime event sequence skips `ThreadOpened`, `DiffUpdated`, `ContextWindowUpdated` | `giskard-server/src/thread_runtime.rs:988-996` |
| `Hub::broadcast_event` keeps `ThreadOpened`, `DiffUpdated`, `ContextWindowUpdated` off the browser stream, then narrows through `from_agent_event`; per-thread FIFO `broadcast` with `try_send`; `event_kind` label | `giskard-server/src/hub.rs:232-259, 125-135, 265-270` |
| Forwarder `event_turn_id` includes the variant; `event_kind` label; `log_metadata_only_event_rejection` | `giskard-server/src/registry/event_forwarder.rs:20-30, 104, 182` |
| Forwarder owned-turn fence: an event for a turn that is neither owned nor persisted is dropped; a persisted turn goes to the late path | `event_forwarder.rs:1309-1329` |
| No owned turn + unseen turn: reserve an external turn from `admitted` or `external_turn_defaults` (model inherited from the thread file's `current_model` or the binding's `native_model`) | `event_forwarder.rs:1329-1381`, `external_turn_defaults` `:673-684` |
| Late path for persisted turns: only terminal command completions and completed tool calls do anything; every other event returns `Continue` without broadcast or runtime apply | `event_forwarder.rs:1422-1517` |
| Turnless events (`owned_turn` none, no turn id) apply to runtime and broadcast only `Error`/`Notice`/`ServerRequestReceived` | `event_forwarder.rs:1519-1575` |
| `ContextWindowUpdated` handling: model mismatch → error log and drop; unknown context model → adopt the event model; `persist_model_context_window`; `return Continue` (never broadcast, never live-buffered) | `event_forwarder.rs:1582-1616` |
| `persist_model_context_window` → `ThreadMetadataService::mutate` → `record_model_context_window`; logs per outcome | `event_forwarder.rs:483-540` |
| `ForwardedTurnState { context, lease, observed_turn, owned_turn, started_at, items, diffs, seen_notices, item_ids_by_harness, saw_context_compaction_marker }`; `new`, `reset` | `event_forwarder.rs:712-752` |
| Completion path: `complete_forwarded_turn(turn, usage, status)` builds the `Turn` with the event usage, persists, inserts `seen_turn_ids`, then `hub.broadcast_event` | `event_forwarder.rs:1706-1830, 1838-1910` |
| Every other admitted event: `ensure_live_turn` / live-buffer admission, `apply_prepared_event`, then `broadcast_event_with_context` | `event_forwarder.rs:1718-1786, 1833`; `registry.rs:1855-1901` |
| Synthesized `Interrupted` completion on stream end or `Gap` uses `TokenUsage::default()` | `event_forwarder.rs:1202-1259` (`:1237`, `:1243`) |
| `finish` does not synthesize a completion | `event_forwarder.rs:1098-1176` |
| `TurnContext { user_input, model: TurnModel, mode, kind }` | `giskard-server/src/registry.rs:84-89` |
| Browser: `updateGauge(used, window)` keeps the previous window when passed `0`; `updateGaugeFromUsage(usage)` uses `usage.input`, falls back to `total`; `updateGaugeFromTurns` | `static/app.js:10085-10110` |
| Browser: `turn_started` sets `state.currentRenderTurnId = ev.turn`; `turn_completed` clears it and calls `updateGaugeFromUsage(ev.usage)`; the `switch` has no `default` arm, unknown kinds are ignored | `app.js:4229-4290, 4340-4342` |
| Browser: live snapshot sets `currentRenderTurnId = snap.turn_id` before replaying `accumulated` through `handleEvent` | `app.js:4360-4387` |
| Browser: `ThreadState` applies `updateGauge(state.contextUsed, effective.context_window \|\| 0)`; gauge state reset on thread switch | `app.js:3694, 2502-2504, 2583-2585` |
| UI source test asserts the metadata gauge line and the absence of `case "context_window_updated":` | `giskard-server/tests/ui.rs:3300-3331` (`browser_applies_revisioned_thread_metadata_to_the_gauge`) |
| Other exhaustive matches on the variant | `giskard-harness-codex/src/lib.rs:1729, 2283, 2388`; `giskard-harness-replay/src/lib.rs:433, 665` |
| Existing tests: mapper `:3789, 3863, 3907, 3936, 4067`; forwarder `:2956, 3085, 3195` and the log-label test `:2650-2710`; proto `wire.rs:1067`; core `event.rs:154` | see files |
| Spec: enum §4.4 `:1802-1807`; C8/C9 changelog `:553-560`; §10.3 gauge text `:3250-3275`; CR2 `:202-204`; version `:12`; amendment format `:14-24` | `specs/giskard-specification.md` |
| Adapter README "Runtime context window"; root README gauge paragraph | `giskard-harness-codex/README.md:538-565`; `README.md:147-151` |
| Rules: no peer map keyed by thread identity; exception comments; spec is authoritative and docs stay in sync | `AGENTS.md:10-29, 136-141` |

## Design

### D1. One turn-scoped event replaces the metadata-only one

Replace `AgentEvent::ContextWindowUpdated` with:

```rust
/// Live token usage for an in-flight turn, emitted whenever the harness reports a change.
///
/// `usage` is the turn's latest reported usage (the same value `TurnCompleted` will carry at the
/// end). `context_window` is the effective window the harness applies to this turn when it
/// reports one; it is turn-scoped runtime data, not a property of a model. `model` is present only
/// when the harness acknowledged a model for this exact turn at start; the server persists the
/// window per `(provider, model)` only then, and never derives a model from thread state.
TurnUsageUpdated {
    thread: ThreadId,
    turn: TurnId,
    usage: TokenUsage,
    context_window: Option<u32>,
    model: Option<ModelRef>,
},
```

Renaming rather than widening `ContextWindowUpdated` is deliberate: the old name says "metadata"
and the old wire rule says "never on the transcript stream"; both are reversed here, and a variant
whose meaning flips should not keep its name. Every site that changes is an exhaustive match or a
test, so the rename costs nothing beyond the sites already listed below. (`TurnCompleted` is not
touched; retaining the final usage there is goal 2 and already works.)

Wire mirror, `giskard-proto/src/wire.rs`:

```rust
TurnUsageUpdated {
    thread: ThreadId,
    turn: TurnId,
    usage: TokenUsage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_window: Option<u32>,
},
```

The model is not on the wire. The browser's model authority is `ThreadMetadata.current_model`
(spec §13.6.1), and a per-event model would invite the client to second-guess it. `from_agent_event`
maps the variant (dropping `model`) instead of returning `None`.

### D2. The adapter reports every change and attaches the model it was given

`ThreadTokenUsageUpdated` handling (`mapping.rs:467-532`) becomes:

1. Empty `turnId` → `None` (unchanged).
2. Not the active native turn for this thread → debug log, `None` (unchanged). This is the adapter
   fence: replayed history after `thread/resume` and any notification arriving after
   `turn/completed` removed the turn from `active_turns` (`mapping.rs:438-446`) are rejected here,
   before any cache is touched.
3. `usage = breakdown_to_usage(&last)`; insert into `turn_usage` (unchanged; `TurnCompleted` keeps
   consuming it).
4. Validate `model_context_window` exactly as today: absent → `None` window; not `u32` or zero →
   warn once through `invalid_context_window_turns`, `None` window. Do **not** return early: an
   invalid window only suppresses the window, never the usage.
5. Compare `(usage, window)` with the last emitted pair for the key. If unchanged → `Ok(None)`.
   Otherwise record it and emit `TurnUsageUpdated { usage, context_window: window, model:
   self.turn_models.get(&key).cloned() }`.

State changes in `CodexMapper`:

- `turn_context_windows: HashMap<NativeTurnKey, u32>` becomes
  `emitted_usage: HashMap<NativeTurnKey, (TokenUsage, Option<u32>)>`, "last reported pair for
  dedup". Keep the `ENTITY-AUTHORITY-EXCEPTION` comment, updating Role/Invalidation.
- `missing_context_model_turns` is deleted, together with its comment, its `clear_active_turn` and
  `TurnCompleted` cleanup lines, and the `remove` in `register_active_turn_with_model`. The warning
  is gone.
- `turn_usage`, `turn_models`, `invalid_context_window_turns`, `active_turns` stay as they are.
  `turn_models` remains the only source of the event's `model`; it is populated only by
  `register_active_turn_with_model`, i.e. by the `turn/start` response for a Giskard-issued start.

No thread-level or binding-level model is consulted. A turn that Codex started without Giskard
therefore reports `model: None` for its whole life.

### D3. The forwarder forwards usage for the owned turn and persists only with a model

All three existing fences already apply to the new variant because `event_turn_id` covers it:

| Arrival | Path today (`event_forwarder.rs`) | Effect for `TurnUsageUpdated` |
| --- | --- | --- |
| Turn is the owned turn | falls through to the admitted-event path | handled below |
| Owned turn exists, event names another turn that was never persisted | `:1316` cross-turn drop | dropped, logged |
| Event names a turn already in `seen_turn_ids` (persisted) | `:1422-1517` late path | must return `Continue` before runtime apply, live buffer, or broadcast. Add an explicit arm at the top of that block: `if matches!(event, AgentEvent::TurnUsageUpdated { .. }) { debug!(..., "ignoring usage update for an already-persisted turn"); return Continue; }` so the behaviour is stated rather than a side effect of "not a terminal command completion". |
| No owned turn, unseen turn | `:1329-1381` reserves an external turn | unchanged; a usage update can be the first event a freshly attached forwarder sees for a native turn, exactly like an `ItemStarted` today |

The dedicated block at `:1582-1616` is replaced by the following, placed at the same position (after
`capture_event_diffs`, before the `match`):

```rust
if let AgentEvent::TurnUsageUpdated { turn, usage, context_window, model, .. } = &event {
    self.turn.live_usage = Some(*usage);
    if let Some(window) = context_window {
        self.turn.live_context_window = Some(*window);
    }
    match (model, context_window) {
        (Some(model), Some(window)) => {
            if self.turn.context.model.as_known().is_some_and(|expected| {
                model.provider != expected.provider || model.model != expected.model
            }) {
                error!(..., "skipping model context-window persistence for the wrong turn model");
            } else {
                if self.turn.context.model.as_known().is_none() {
                    self.turn.context.model = TurnModel::Known(model.clone());
                }
                if self.turn.persisted_context_window != Some(*window) {
                    persist_model_context_window(..., *turn, model, *window).await;
                    self.turn.persisted_context_window = Some(*window);
                }
            }
        }
        _ => {} // no model: live only; the per-model cache and the thread window stay untouched
    }
    // fall through: live buffer, runtime apply, broadcast, like any admitted turn event
}
```

Points that matter:

- **Model adoption only from an event model.** The `as_known().is_none()` adoption exists today
  (`:1603-1605`) and stays, but it can only fire with `model: Some`. The forwarder never copies
  `binding.native_model` or the thread file's `current_model` into the event, and the persisted
  `Turn.model` for a native-started turn keeps whatever `external_turn_defaults` gave it, exactly
  as today.
- **Mismatch no longer drops the event.** The gauge is about the turn that is running; the model
  disagreement only affects which cache row would be written, so only persistence is skipped.
- **Persist once per distinct window.** `persisted_context_window` dedups per turn so that usage
  ticks with an unchanged window do not call the metadata service on every notification. The
  mapper's dedup is on the pair, so a usage-only change would otherwise reach
  `persist_model_context_window` and log `Unchanged` each time.
- **The event continues into the normal path.** It is appended to the live buffer (D4), applied to
  the runtime (it gets an event sequence; no task or request state changes), and broadcast through
  `broadcast_event_with_context` (`:1833`) on the per-thread FIFO lane, in log order with the
  transcript events around it.

`ForwardedTurnState` gains three fields, all reset in `reset`:

```rust
live_usage: Option<TokenUsage>,
live_context_window: Option<u32>,
persisted_context_window: Option<u32>,
```

These are turn-local values on the thread's event owner, not a keyed map; no new authority.

Synthesized completions: both `TokenUsage::default()` sites in `handle_stream_error`
(`:1237`, `:1243`) use `self.turn.live_usage.unwrap_or_default()`. The real `TurnCompleted` keeps
its own usage (the mapper attaches the same cached value, so there is nothing to reconcile); do not
add a "fall back to live usage when the completion says zero" rule.

### D4. Live buffer and hub

- `hub.rs:232-244`: remove the variant from the internal-only list. `broadcast_event` then narrows it
  through `from_agent_event` like every transcript event. `event_kind` label → `"turn_usage_updated"`.
- `thread_runtime.rs:988-996`: remove the variant from the no-sequence list. It is an ordered
  transcript event now.
- `runtime_live.rs:192-196` (`append`): before `turn.events.push(event)`, if the event is
  `TurnUsageUpdated`, `turn.events.retain(|e| !matches!(e, AgentEvent::TurnUsageUpdated { .. }))`.
  The buffer keeps one usage event per turn, the latest, at the end. A reconnect snapshot therefore
  replays the current footprint after the items, and the buffer cannot grow by one entry per model
  call. `snapshot` (`:278-303`) needs no change because `from_agent_event` now maps the variant.

### D5. Browser

`static/app.js`, `handleEvent` switch: add

```js
case "turn_usage_updated":
  // Fence: only the turn being rendered may move the gauge. A late or replayed update for another
  // turn is ignored; `turn_completed` already cleared `currentRenderTurnId` for finished turns.
  if (ev.turn && ev.turn === state.currentRenderTurnId) {
    updateGaugeFromUsage(ev.usage, ev.context_window);
  }
  break;
```

and let `updateGaugeFromUsage(usage, window)` pass `window || state.contextWindow` to
`updateGauge` (`:10104-10110`), keeping the `usage.input` numerator and its comment. `updateGauge`
already treats `0`/`undefined` as "keep the current window" (`:10086`), so a `ThreadState` for an
unknown-model thread (persisted `context_window` from config or the fallback) and a live event never
fight: the live window wins while the turn runs; the persisted one applies again after reload. The
`turn_completed` arm (`:4279-4284`) is unchanged.

Reconnect: `renderLiveTurnSnapshot` sets `currentRenderTurnId = snap.turn_id` (`:4369`) before
replaying `accumulated` (`:4387`), so the coalesced usage event passes the fence and the gauge shows
the in-flight footprint immediately after reload.

### D6. Order and replay guarantees, end to end

| Scenario | Where it is stopped |
| --- | --- |
| Historical usage replayed by Codex after `thread/resume` | adapter: not the active native turn (`mapping.rs:474-482`) |
| Usage notification delivered after `turn/completed` for that turn | adapter: `TurnCompleted` removed the active turn (`mapping.rs:438-446`); same check |
| Two Codex threads reuse native turn ids | adapter: `NativeTurnKey` is `(thread, native turn)`; existing test `:3936` |
| Retained-log replay to a newly attached forwarder (M1/M4) | log order is preserved; the forwarder sees `TurnStarted`, usage, `TurnCompleted` in that order; a usage event for a turn already in `seen_turn_ids` takes the late path and is ignored (D3) |
| Usage event for a turn the forwarder does not own and never persisted | forwarder cross-turn drop (`:1316`) |
| Usage event arriving while the browser renders a different turn | browser fence on `currentRenderTurnId` (D5) |
| Live event and metadata `ThreadState` carrying the same window | both idempotent on the gauge; different lanes, different clocks, same value |
| Live event after the turn completed in the browser | `turn_completed` cleared `currentRenderTurnId`; ignored |

No clock is compared across lanes: the transcript lane orders usage against items, the metadata
lane orders the persisted window against other metadata, and the browser gauge takes whichever
arrives, both being values for the same turn.

### D7. What an unknown-model turn looks like after this change

During the turn: gauge shows live `input / modelContextWindow`. On completion: `Turn.usage` is the
final usage; `Turn.model` is whatever the forwarder's context had (unchanged); no per-model cache
row is written; the thread's `context_window` is unchanged. After a reload of that thread: the gauge
denominator comes from `ThreadMetadata.context_window` (config, provider metadata, restore, or the
fallback), i.e. the live window is not remembered across reloads for such turns. That is the stated
trade-off of goal 3; see Pitfalls for the additive extension if it is ever wanted.

## Every site that changes

| File | Change |
| --- | --- |
| `crates/giskard-core/src/event.rs:27-37` | Replace the variant and its doc comment (D1); `:108` arm; rewrite test `:154-177` for the new shape (`kind == "turn_usage_updated"`, `context_window` omitted when `None`, `model` omitted when `None`) |
| `crates/giskard-proto/src/wire.rs:37-100, 326-345` | Add the wire variant; map it in `from_agent_event`; replace test `:1067-1078` with one that asserts the wire event exists, carries `usage` and `context_window`, and has no `model` key |
| `crates/giskard-harness-codex/src/mapping.rs:94-145, 181-186, 208-220, 241-253, 437-465, 467-532` | D2: rename `turn_context_windows` → `emitted_usage` with the new pair type; delete `missing_context_model_turns`; new handler; `TurnCompleted` and `clear_active_turn` cleanup lines follow |
| `crates/giskard-harness-codex/src/lib.rs:1729, 2283, 2388` | Mechanical: variant name, label `"turn_usage_updated"` |
| `crates/giskard-harness-replay/src/lib.rs:433, 665` | Mechanical |
| `crates/giskard-server/src/hub.rs:232-244, 269` | D4 |
| `crates/giskard-server/src/thread_runtime.rs:988-996` | D4 |
| `crates/giskard-server/src/runtime_live.rs:192-196` | D4 coalescing |
| `crates/giskard-server/src/registry/event_forwarder.rs:23, 104, 712-752, 1237, 1243, 1422-1430, 1582-1616` | D3 |
| `crates/giskard-server/static/app.js:4229-4342, 10104-10110` | D5 |
| `crates/giskard-server/tests/ui.rs:3300-3331` | Keep the two existing assertions; add `case "turn_usage_updated":` and `ev.turn === state.currentRenderTurnId` presence checks |
| `specs/giskard-specification.md` | See Documentation |
| `crates/giskard-harness-codex/README.md:538-565`, `README.md:147-151` | See Documentation |

`docs/api-endpoints.md` enumerates no event kinds and needs no change.
`docs/thread-state-and-bootstrap-reconciliation-plan.md` is a historical plan; leave it.

## Tests

Mapper (`mapping.rs`), all with a `TurnStarted` notification and no `register_active_turn_with_model`
unless stated:

1. `usage_updates_emit_for_a_native_turn_without_a_registered_model`: usage with
   `modelContextWindow: 258400` → `TurnUsageUpdated { usage: last, context_window: Some(258_400),
   model: None }`. Then `TurnCompleted` carries the same usage.
2. `usage_updates_carry_the_turn_start_model`: after `register_active_turn_with_model`, the event has
   `model: Some(model)`. Update test `:3789` (`token_usage_attached_on_turn_completed`) to this shape;
   the second notification with identical `last` and window still returns `None` (`:3843` stays true).
3. `changed_usage_re_emits_with_the_unchanged_window`: second notification with different
   `last.inputTokens` → a second event, same `context_window`.
4. `invalid_context_window_emits_usage_without_a_window`: rewrite `:3863` — negative window → event
   with `context_window: None` and the usage, then `TurnCompleted` still carries the usage; a later
   valid window on the same turn → event with `Some`.
5. `usage_after_turn_completed_is_ignored`: `TurnCompleted`, then a usage notification for the same
   native turn id → `None`, and a subsequent `TurnStarted` for a new turn is unaffected.
6. `:3907` (`historical_usage_replay_is_not_attributed_to_a_turn`) unchanged.
   `:3936` (`token_usage_is_scoped_by_thread_when_native_turn_ids_repeat`): the two `is_none()`
   assertions at `:3997-4005` become "each returns an event for its own thread with its own usage";
   the completion assertions stay. `:4067` unchanged (unknown native thread still `None`).

Forwarder (`event_forwarder.rs`, using the existing in-memory harness/log fixtures of the tests at
`:2956-3330`):

7. `forwarder_broadcasts_live_usage_for_an_unknown_model_turn_and_persists_nothing`:
   `TurnStarted`, `TurnUsageUpdated { model: None, context_window: Some(258_400) }`; assert a
   `ServerMessage::Event` with kind `turn_usage_updated` reaches the subscriber, the live snapshot's
   `accumulated` contains it, the thread file's `model_context_windows` and `context_window` are
   unchanged, and no `ThreadState` with the new window is published. Then `TurnCompleted` with usage
   → the persisted `Turn.usage` equals it.
8. Rewrite `:3195` (`..._for_matching_turn_model`): same assertions as today for persistence and the
   metadata `ThreadState`, plus the transcript event is received.
9. Rewrite `:3085` (`..._mismatched_turn_model`): persistence does not happen, but the transcript
   event is still received.
10. `forwarder_ignores_usage_for_an_already_persisted_turn`: complete turn A (persisted), then
    append `TurnUsageUpdated` for A while idle → no `Event` broadcast, no persistence, no live turn
    created. Repeat while turn B is owned → same, and B's gauge event is unaffected.
11. `synthesized_interrupted_completion_carries_live_usage`: `TurnStarted`, usage event, then end the
    stream (or a `Gap`) → the persisted `Turn` has the live usage and the broadcast `TurnCompleted`
    carries it.
12. `persistence_happens_once_per_distinct_window`: three usage events with the same window and
    a known model → one metadata revision bump.
13. The log-label test at `:2650-2710`: update the label to `"turn_usage_updated"` and drop the
    rejection expectation that no longer applies (the variant is no longer metadata-only; keep the
    test for a variant that still is, e.g. `DiffUpdated`).

Runtime and hub:

14. `runtime_live.rs`: append two usage events and one item → `accumulated` has the item followed by
    exactly one usage event, the second.
15. `hub.rs`: `broadcast_event` with `TurnUsageUpdated` delivers a `ServerMessage::Event`;
    `DiffUpdated` remains internal-only.

Proto and core: serde round-trips (site table above).

UI (`tests/ui.rs`): source assertions listed in the site table.

No e2e test is required; the forwarder fixtures exercise the whole server path. If one is added,
model it on `:6871-7004` and drive the log directly rather than a Codex binary.

## Documentation

`specs/giskard-specification.md` (bump `:12` to 1.85 and add an amendment block after `:24` in the
same form as the 1.84 one):

- §4.4 `:1802-1807`: replace the enum entry with `TurnUsageUpdated` and its doc.
- §10.3 `:3269-3271`: replace "The Codex adapter emits `ContextWindowUpdated` whenever the value
  changes within a turn, tagged with that turn's exact model, and the server persists it by
  `(provider, model)`" with: the adapter emits `TurnUsageUpdated` for the active turn whenever the
  reported usage or window changes; the browser updates the gauge live from it for the turn it is
  rendering; the server persists the window per `(provider, model)` only when the event carries the
  model acknowledged at `turn/start`, and never derives one from thread metadata.
- §13.6.1: one sentence after the table noting that live turn usage is an "Active transcript" event
  and that the persisted window remains on the metadata row; do not add a table row.
- Amendment text (1.85): unknown-model turns keep their live usage and window; no warning; no
  per-model persistence without an event model; live buffer keeps the latest usage event per turn.
- Leave the 1.54 (C8/C9) and 1.70 (CR2) changelog entries as history; they describe versions that
  existed.

`crates/giskard-harness-codex/README.md:538-565`: rewrite the two paragraphs about
`ContextWindowUpdated` to describe `TurnUsageUpdated`, the model-only-from-`turn/start` rule, the
removal of the warning, and that an invalid window suppresses only the window.

`README.md:147-151`: say the header value updates during the turn from the latest reported input
tokens, not only at turn end.

`docs/event-pipeline-milestones.md`: no change; this is not a pipeline milestone.

## Order of work

One PR, one seam (the event's meaning), in this order so the tree compiles at each step:

1. `giskard-core` variant + test; `giskard-proto` wire variant + `from_agent_event` + test.
2. Mechanical arms: `giskard-harness-codex/src/lib.rs`, `giskard-harness-replay`, `hub.rs`,
   `thread_runtime.rs`, forwarder `:23, :104`.
3. Mapper D2 with tests 1-6.
4. Forwarder D3 with tests 7-13; `runtime_live` D4 with test 14; hub test 15.
5. Browser D5 and `ui.rs` assertions.
6. Docs.

Expected size: roughly 300-500 non-test lines, most of them deletions and renames.

## Verification the implementer must perform and record

- `cargo test -p giskard-core -p giskard-proto -p giskard-harness-codex -p giskard-harness-replay
  -p giskard-server`, `cargo clippy --all-targets`, `cargo fmt --check`, and the UI source tests.
- `grep -rn ContextWindowUpdated crates/ specs/ README.md docs/` returns only the historical
  reconciliation plan.
- `grep -rn "without a registered turn model" crates/` returns nothing.
- A manual run against Codex with a sub-agent spawn: no warning in the log, the child thread's gauge
  moves during its turn, the parent's gauge is unaffected by the child's events, and the child's
  thread file has no `model_context_windows` row added by that turn.
- A manual reload mid-turn: the gauge shows the in-flight footprint from the live snapshot.

## Pitfalls

- Do not "fix" the unknown model by reading `binding.native_model` or the thread file's
  `current_model` in the adapter or the forwarder. Both are thread-level and neither is guaranteed to
  be the model Codex ran the turn with. The forwarder's `TurnContext.model` for native turns already
  inherits from them for the persisted `Turn.model`; that pre-existing behaviour is out of scope
  and must not be extended to per-model persistence.
- Do not gate the event on the window being present. Usage without a window must still reach the
  browser; that is most of goal 2.
- Do not broadcast the event from the dedicated block and also fall through. It must take the
  normal admitted path once, so it is live-buffered, sequenced, and broadcast in order.
- Do not keep every usage event in the live buffer. Long turns produce one per model call.
- Do not let the browser apply a usage event without the turn fence, even from the live snapshot.
- `updateGauge(used, 0)` keeps the previous window; keep passing `window || state.contextWindow`
  rather than `0` for events without a window.
- Removing the variant from the `thread_runtime.rs` no-sequence list is required; otherwise the
  event applies with `sequence: None` and the debug logs mislead.
- If a later change wants the live window to survive reloads for unknown-model threads, the
  additive path is `Turn.context_window: Option<u32>` (serde default) written from
  `live_context_window` in `complete_forwarded_turn`, and `updateGaugeFromTurns` reading it. It is
  not part of this plan.

## Stop rules

Stop and re-cut if the diff:

- adds a map keyed by thread or turn identity outside `CodexMapper` and `ForwardedTurnState`;
- infers a model from anything other than the `turn/start` response;
- adds a `ServerMessage` variant, a timer, or a revision counter;
- changes `ThreadMetadata`, `ThreadState`, `Turn` serialization, or either JSON file format;
- changes how `TurnCompleted` usage is computed or folded into ledgers;
- touches `ThreadUpdateSink` or the resume-time restore path.
