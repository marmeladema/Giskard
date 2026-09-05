# S2 — Group per-turn and per-item state by lifetime

Implementation plan for step 2 of [`design-straightening-review.md`](design-straightening-review.md)
(findings B1 and B2). Written against `main` at `b07ea17` (S1 merged); every file and line
reference below was checked against that tree. Re-check them if the branch has moved.

## Goal

Replace maps that are always read, written, and cleared together for the same key with one map of
one struct, so that a turn's or an item's state has one owner, one cleanup site, and one
`ENTITY-AUTHORITY-EXCEPTION` comment. No behaviour changes.

## Two corrections to the review

Verifying the access sites narrowed both groupings relative to the review's sketch. Lifetime,
not key type, decides what may share a struct.

1. **`turn_ids` stays separate.** The review listed it inside `NativeTurnState`. It is keyed by
   `NativeTurnKey` like the others, but it is never removed: not on `TurnCompleted`
   (`mapping.rs:436-441`), not in `clear_active_turn` (`:197-207`). Only `resolve_turn`
   touches it, through `entry().or_default()` (`:356-367`). That is deliberate: a command can
   outlive its persisted turn, and its late terminal completion must resolve to the *same*
   `TurnId` so the forwarder's late path (`event_forwarder.rs`, `seen_turn_ids`) recognises it.
   Folding it into a struct that is removed at completion would mint a fresh id for those events.
2. **`persisted_command_output_versions` stays separate.** The review listed it with the two
   output maps. The two live output maps are pruned for the completed turn in
   `settle_completed_turn` (`thread_runtime.rs:1080-1087`); the version cache is not pruned
   there, only dropped with the whole entry in `forget_threads` (`:1254-1262`), because the HTTP
   durable-output route reads it for persisted turns (`routes.rs:4151-4202`). Different lifetime,
   different struct.

So S2 folds four mapper maps into `NativeTurnState` and two runtime maps into `ItemOutputs`.

## Non-goals

- No change to `NativeTurnKey`, `NativeItemKey`, `NativeProcessKey`
  (`giskard-harness-codex/src/native_ids.rs:81-125`) or to `turn_ids`, `item_ids`,
  `active_turns`, `running_commands`, `running_command_turns`, `file_change_previews`,
  `native_parents`, `pending_*`.
- No change to `persisted_command_output_versions`, `captured_diffs`, `requests`, `live`, `tasks`.
- No change to any `AgentEvent` emitted, any log line, or any public method signature on
  `CodexMapper` or `ThreadRuntimeSupport`.
- No change to the tests' assertions. Two tests read a mapper field directly and keep doing so
  (`turn_ids` at `mapping.rs:4172`, `file_change_previews` at `:4874-4910`); neither field moves.

## Ground truth

### Mapper (`crates/giskard-harness-codex/src/mapping.rs`)

| Fact | Where |
| --- | --- |
| `CodexMapper` has 15 fields, 11 with `ENTITY-AUTHORITY-EXCEPTION` comments (five-line template: Role / Source of truth / Structural reason / Synchronization / Invalidation) | `:68-162` |
| Four maps keyed by `NativeTurnKey` share one lifetime, "turn/start or turn/started → turn/completed or thread cleared": `turn_usage: HashMap<_, TokenUsage>` `:103`, `emitted_usage: HashMap<_, (TokenUsage, Option<u32>)>` `:111`, `turn_models: HashMap<_, ModelRef>` `:120`, `invalid_context_window_turns: HashSet<_>` `:129` | `:96-129` |
| Constructed empty | `:172-175` |
| Cleared per thread by four `retain` calls in `clear_active_turn` | `:200-204` |
| Cleared per turn by four `remove` calls on `TurnCompleted`, after `turn_usage.remove(..).unwrap_or_default()` supplies the completion's usage | `:435-439` |
| `turn_models` written once by `register_active_turn_with_model` | `:230-238` |
| Usage handler reads and writes all four on the same key: `turn_usage.insert` `:470`, `invalid_context_window_turns.insert/remove` `:476, 486, 490`, `emitted_usage.get/insert` `:502-506`, `turn_models.get` `:512` | `:452-515` |
| No other file names these four fields | verified: `grep -rlE "\b(turn_usage\|emitted_usage\|turn_models\|invalid_context_window_turns)\b" crates/giskard-harness-codex/src` lists only `mapping.rs`; 24 matching lines there |
| `turn_ids` is keyed by `NativeTurnKey` but has harness lifetime (see correction 1) | `:86, 170, 361`; test `:4172` |
| `file_change_previews` and `running_commands` are keyed by `NativeItemKey { thread_id, turn_id: TurnId, native_item_id }`, a Giskard turn id, not a native one; `file_change_previews` is cleared per turn/thread (`:205, 441`), `running_commands` is not cleared at completion (`:841, 881, 895`) | `native_ids.rs:96-110`; `mapping.rs:150, 159` |

### Runtime (`crates/giskard-server/src/thread_runtime.rs`)

| Fact | Where |
| --- | --- |
| `ThreadRuntimeEntry` is `#[derive(Default)]` with 11 fields; `command_outputs: HashMap<(TurnId, ItemId), RuntimeCommandOutput>` `:51`, `tool_outputs: HashMap<(TurnId, ItemId), RuntimeToolOutput>` `:52`, `persisted_command_output_versions: HashMap<(TurnId, ItemId), String>` `:53` | `:40-54` |
| `RuntimeCommandOutput { output, output_truncated, original_bytes, original_lines, version }`, `RuntimeToolOutput { bytes, descriptor }` | `:103-115` |
| Reads: `command_output` `:545-562`, `tool_output` `:565-577` (each returns a `*Lookup` enum) | |
| Removals: `remove_tool_output` `:581-592`, `remove_command_output` `:595-606` | |
| Writers: `update_command_output_authority` `:1467-1509` (remove on non-command, running, or inconsistent metadata; insert otherwise), `update_prepared_item_output_authority` `:1602-1630` (insert-or-remove both on one key), `update_tool_output_authority` `:1632-1677` (remove on non-tool, running, absent, or unserializable; insert otherwise) | |
| Pruning: `settle_completed_turn` retains entries whose turn is not the completed one, for both maps and not for the version cache | `:1080-1087` |
| Version cache read and written only through `PersistedCommandOutputVersionPermit` | `:138-165`; caller `routes.rs:4151-4202` |
| No other file names these fields; no test reads them directly | verified: `grep -rlE "\b(command_outputs\|tool_outputs)\b" crates/giskard-server/src` lists only `thread_runtime.rs`; 21 matching lines there, none past `mod tests` |

## Design

### D1. `NativeTurnState` in the mapper

```rust
/// Everything the adapter tracks for one native turn between its start and its completion.
#[derive(Default)]
struct NativeTurnState {
    /// Latest `thread/tokenUsage/updated.last` for the turn; attached to `TurnCompleted`.
    usage: TokenUsage,
    /// Last `(usage, context_window)` pair emitted as `TurnUsageUpdated`, for dedup.
    emitted_usage: Option<(TokenUsage, Option<u32>)>,
    /// Model acknowledged by `turn/start`, when Giskard supplied one.
    model: Option<ModelRef>,
    /// Whether an invalid context window was already logged for this turn.
    invalid_window_warned: bool,
}
```

with one field replacing four:

```rust
// ENTITY-AUTHORITY-EXCEPTION:
// Role: Correlate out-of-band Codex usage, usage dedup, the acknowledged model, and the
//       invalid-window warning with their owning native turn.
// Source of truth: turn/start (model) and thread/tokenUsage/updated (the rest).
// Structural reason: Codex delivers these separately from turn completion.
// Synchronization: The single Codex background task owns and mutates the mapper.
// Invalidation/removal: Turn completion or thread cleanup removes the entry; shutdown drops the rest.
turns: HashMap<NativeTurnKey, NativeTurnState>,
```

Semantics per site, each preserving today's value exactly:

- `clear_active_turn`: the four `retain` calls become `self.turns.retain(|key, _| key.thread_id != thread)`.
- `register_active_turn_with_model`: `self.turns.entry(key).or_default().model = Some(model)`.
- `TurnCompleted`: `let usage = self.turns.remove(&key).map(|t| t.usage).unwrap_or_default()`;
  the three other `remove` calls disappear. `unwrap_or_default` keeps the "zero if Codex never
  sent usage" rule.
- Usage handler: `let turn = self.turns.entry(key.clone()).or_default();` then
  `turn.usage = usage;` the window validation sets `turn.invalid_window_warned` where the set
  used `insert` (warn only when it flips from `false`) and clears it where the set used `remove`;
  the dedup compares `turn.emitted_usage == Some((usage, context_window))` and stores the pair;
  the event's `model` is `turn.model.clone()`. Note `HashSet::insert` returned `true` on first
  insertion, which is what gated the warning; `!warned` then `warned = true` is the same gate.
- The doc comments on the four old fields move onto the struct fields.
- A doc comment on `CodexMapper` itself records the lifetime classes so the next field is placed
  by the same rule. Text to use, above `pub struct CodexMapper`:

  ```rust
  /// Adapter state is grouped by lifetime, not by key type:
  /// - harness lifetime, never removed while the process runs: `routes`, `native_parents`,
  ///   `turn_ids`, `item_ids` (identity must stay stable for late events);
  /// - one native turn, removed at `turn/completed` or `clear_active_turn`: `turns`,
  ///   `file_change_previews`;
  /// - one running command, removed when the command ends, which may be after its turn:
  ///   `running_commands`, `running_command_turns`;
  /// - one pending request, removed when answered: `pending_approval_responses`,
  ///   `pending_server_requests`.
  /// A new field joins the struct whose cleanup site matches its lifetime; a field that needs
  /// its own cleanup site gets its own map and its own exception comment.
  ```

`invalid_context_window_turns` was the only one of the four that could exist for a turn without
`turn_usage` existing (the invalid-window branches run after `turn_usage.insert`, so in practice
never); `or_default()` makes the combined entry exist from the first notification, which matches.

### D2. `ItemOutputs` in the runtime

```rust
/// The live, authoritative outputs of one completed item while its turn is in flight.
#[derive(Default)]
struct ItemOutputs {
    command: Option<RuntimeCommandOutput>,
    tool: Option<RuntimeToolOutput>,
}

impl ItemOutputs {
    fn is_empty(&self) -> bool { self.command.is_none() && self.tool.is_none() }
}
```

with `item_outputs: HashMap<(TurnId, ItemId), ItemOutputs>` replacing `command_outputs` and
`tool_outputs`. Two private helpers on `ThreadRuntimeEntry` keep every site one line:

```rust
fn set_command_output(&mut self, key: (TurnId, ItemId), output: Option<RuntimeCommandOutput>);
fn set_tool_output(&mut self, key: (TurnId, ItemId), output: Option<RuntimeToolOutput>);
```

Each writes the field and removes the map entry when `is_empty()` afterwards, so a key never
lingers with two `None`s. That is the only new rule and it is invisible: today an absent key and
a present-but-irrelevant key read the same through `command_output` / `tool_output`.

Semantics per site:

- `command_output` / `tool_output`: `get(&key).and_then(|o| o.command.clone())` and the tool
  counterpart; `Missing` when either level is `None`.
- `remove_command_output` / `remove_tool_output`: `set_*_output(key, None)`.
- `update_command_output_authority`: every `remove` becomes `set_command_output(key, None)`, the
  `insert` becomes `set_command_output(key, Some(..))`. Same for `update_tool_output_authority`.
- `update_prepared_item_output_authority`: `set_command_output(key, prepared.command_runtime)`
  and `set_tool_output(key, prepared.tool_runtime)`; the error log for a missing descriptor stays.
- `settle_completed_turn`: one `retain(|(turn_id, _), _| *turn_id != completed_turn)` instead of two.
- `persisted_command_output_versions` and the permit are untouched.
- A doc comment on `ThreadRuntimeEntry` records the same rule for the runtime. Text to use,
  above `pub(crate) struct ThreadRuntimeEntry`:

  ```rust
  /// Per-thread runtime state, grouped by lifetime:
  /// - the in-flight turn, cleared for the completed turn in `settle_completed_turn`:
  ///   `captured_diffs` (per turn, slot model: turn-level and per-item paths evolve
  ///   independently), `item_outputs` (per item, one command and one tool output at most),
  ///   `live`, and the resolved `requests` of that turn;
  /// - the thread's owner and clocks, cleared only with the entry: `active_turn`,
  ///   `lifecycle_revision`, `event_sequence`, `task_revision`;
  /// - caches for persisted turns, cleared only with the entry:
  ///   `persisted_command_output_versions`;
  /// - `tasks`, which outlive turns because a command may finish after its turn persisted.
  /// A new per-item value that ends with the turn goes in `ItemOutputs`; a new per-turn
  /// value that has slot or sharing semantics goes next to `captured_diffs`; anything that
  /// survives completion gets its own field and its own cleanup site.
  ```

## Every site that changes

| File | Change |
| --- | --- |
| `crates/giskard-harness-codex/src/mapping.rs:96-129` | Four fields and their four exception comments → `turns` with one comment; add `NativeTurnState` above the struct |
| `mapping.rs:172-175` | Constructor: one `HashMap::new()` |
| `mapping.rs:200-204` | One `retain` |
| `mapping.rs:230-238` | `entry().or_default().model = Some(model)` |
| `mapping.rs:435-439` | One `remove`, usage taken from the removed state |
| `mapping.rs:468-512` | Usage handler on one `entry().or_default()` |
| `crates/giskard-server/src/thread_runtime.rs:51-52` | Two fields → `item_outputs`; add `ItemOutputs` and the two setters |
| `thread_runtime.rs:545-606` | Four accessors read or set through `item_outputs` |
| `thread_runtime.rs:1080-1087` | One `retain` |
| `thread_runtime.rs:1467-1509, 1602-1677` | Writers use the setters |
| `docs/design-straightening-review.md` | Mark B1/B2 (step 2) landed and record the two corrections above |
| `AGENTS.md:136-137` | After the entity-local-state bullet, add: "Keyed state is grouped by lifetime: a new map joins the struct whose cleanup site matches when it is removed, or gets its own field, comment, and cleanup site. Both `CodexMapper` and `ThreadRuntimeEntry` list their lifetime classes in a doc comment." |

## Tests

Existing tests cover every site and must pass unchanged; they are the specification for "no
behaviour change":

- Mapper: `token_usage_attached_on_turn_completed`, `usage_updates_emit_for_a_native_turn_without_a_registered_model`,
  `changed_usage_re_emits_with_the_unchanged_window`, `usage_after_turn_completed_is_ignored`,
  `invalid_context_window_does_not_hide_token_usage`, `historical_usage_replay_is_not_attributed_to_a_turn`,
  `token_usage_is_scoped_by_thread_when_native_turn_ids_repeat`, `unknown_native_thread_usage_is_not_cached_after_registration`,
  `clear_active_turn_removes_registered_native_turn`, and the file-change preview tests at `:4874-4910`.
- Runtime and forwarder: every test that reads command or tool output through
  `command_output` / `tool_output` or the HTTP output routes (`tests/running_tasks.rs`,
  `tests/e2e_smoke.rs` durable output cases, `event_forwarder.rs` late-completion tests).

Add two small tests, one per struct, that pin the new invariant rather than behaviour:

1. `mapping.rs`: `turn_state_is_created_on_first_usage_and_removed_on_completion`. After
   `TurnStarted` there is no `turns` entry; after one usage notification there is exactly one;
   after `TurnCompleted` there is none, and `turn_ids` still holds the turn (correction 1
   pinned). Reads `mapper.turns` and `mapper.turn_ids` directly like `:4172` does.
2. `thread_runtime.rs`: `item_outputs_entry_is_dropped_when_both_outputs_are_cleared`. Apply a
   completed command item (entry present), then `remove_command_output` → `item_outputs` has no
   key for it. Then a completed tool item and `remove_tool_output` → same. Reads
   `entry.item_outputs` through the existing test access to the runtime entry.

## Order of work

1. D1 with test 1; `cargo test -p giskard-harness-codex`.
2. D2 with test 2; `cargo test -p giskard-server`.
3. `cargo clippy --workspace --all-targets -- -D warnings`; `cargo fmt --check`.
4. Exit checks. The mapper patterns target the four *map fields on `CodexMapper`* (their
   declarations, constructor initialisers, and `self.` accesses), not the word `emitted_usage`,
   which legitimately survives as a field of `NativeTurnState`. Validated on the base tree: the
   three mapper commands match 4, 4, and 16 lines today; the runtime command matches 21. All
   four must match nothing afterwards.

```sh
grep -nE "^\s*(turn_usage|emitted_usage|turn_models|invalid_context_window_turns): Hash(Map|Set)<NativeTurnKey" crates/giskard-harness-codex/src/mapping.rs
grep -nE "^\s*(turn_usage|emitted_usage|turn_models|invalid_context_window_turns): Hash(Map|Set)::new\(\)" crates/giskard-harness-codex/src/mapping.rs
grep -nE "self\.(turn_usage|emitted_usage|turn_models|invalid_context_window_turns)\b" crates/giskard-harness-codex/src/mapping.rs
grep -nE "\b(command_outputs|tool_outputs)\b" crates/giskard-server/src/thread_runtime.rs
```

   and `grep -c "ENTITY-AUTHORITY-EXCEPTION" crates/giskard-harness-codex/src/mapping.rs` is 8
   (was 11).
5. `grep -c "grouped by lifetime" crates/giskard-harness-codex/src/mapping.rs crates/giskard-server/src/thread_runtime.rs AGENTS.md`
   reports 1 for each file. The phrase appears in the two mandated doc comments ("Adapter state
   is grouped by lifetime", "Per-thread runtime state, grouped by lifetime") and in the AGENTS.md
   bullet ("Keyed state is grouped by lifetime"). It matches nothing on the base tree.

Expected size: about 90 lines added, about 130 deleted. One PR.

## Pitfalls

- Do not put `turn_ids` in `NativeTurnState` or remove it at completion (correction 1).
- Do not put `persisted_command_output_versions` in `ItemOutputs` or prune it at completion
  (correction 2).
- Keep `unwrap_or_default()` on the completion's usage; a turn with no usage notification must
  still complete with zero usage.
- The invalid-window warning is gated on the *first* transition to warned within a turn and a
  later valid window resets the gate; keep both halves when replacing the set.
- Do not make `NativeTurnState` or `ItemOutputs` `pub`; both are private to their module. The two
  new tests live in those modules and reach the fields the way existing tests do.
- `file_change_previews` and `running_commands` look similar but are keyed by Giskard turn id and
  have different lifetimes; leave them out.

## Stop rules

Stop and re-cut if the diff:

- changes any `AgentEvent` field value, log line, or `*Lookup` result in an existing test;
- touches `native_ids.rs`, `resolve_turn`, `turn_ids`, or the version permit;
- adds a method to `ThreadRuntimeSupport`'s public surface;
- edits an existing test's assertion.
