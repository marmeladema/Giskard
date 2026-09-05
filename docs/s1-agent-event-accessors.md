# S1 — `AgentEvent` identity accessors

Implementation plan for step 1 of [`design-straightening-review.md`](design-straightening-review.md)
(finding C1). Named S1 because M1 is the retained event log that already landed. Written against
`main` at `37fb77e` plus PR #239; every file and line reference below was checked against that
tree. Re-check them if the branch has moved.

## Goal

Give `AgentEvent` its own identity accessors in `giskard-core` and delete the ten hand-rolled
per-variant tables that four crates keep for the same questions. After this step, adding an
`AgentEvent` variant touches `event.rs` and the places that genuinely handle the new variant,
never a label table in another crate.

## Non-goals

- No change to the `AgentEvent` variants, their serde shape, or the wire types.
- No change to helpers that answer a *forwarder-specific* question rather than an identity
  question: `event_item_identity`, `event_item_delta_kind`, `completed_item_diagnostics`,
  `is_terminal_command_completion`, `track_item_identity` (all in `event_forwarder.rs`), the
  runtime's `completed_*_item_id` / `command_output_item_id` (`runtime_live.rs`), and
  `compaction_event_name` (`giskard-harness-codex/src/lib.rs:1715`). They stay where they are.
- No logging or behaviour change. Every replaced call returns the same value it does today.

## Ground truth

| Fact | Where |
| --- | --- |
| `AgentEvent` has 13 variants; every one carries `thread: ThreadId`. `turn` is `TurnId` on `TurnStarted`, `TurnUsageUpdated`, `ItemStarted`, `ItemDelta`, `ItemCompleted`, `DiffUpdated`, `ApprovalRequested`, `TurnCompleted`; `Option<TurnId>` on `ServerRequestReceived`, `ServerRequestResolved`, `Error`, `Notice`; absent on `ThreadOpened` | `crates/giskard-core/src/event.rs:16-102` |
| The enum is `#[serde(tag = "kind", rename_all = "snake_case")]`, so the serialized `kind` string is the snake-case variant name | `event.rs:17` |
| `AgentEvent::thread_id(&self) -> ThreadId` already exists as the only accessor | `event.rs:104-121` |
| `event.rs` imports `ItemId`, `TurnId`, `ThreadId` already | `event.rs:6` |
| Existing core tests: `agent_event_serde_roundtrip`, `turn_usage_update_serde_roundtrip`, `server_request_events_serde_roundtrip` | `event.rs:133, 159, 188` |
| **Copy 1** `event_turn_id(&AgentEvent) -> Option<TurnId>`, `pub(super)`; `Some` for the eight `TurnId` variants, the field value for the four `Option` variants, `None` for `ThreadOpened` | `crates/giskard-server/src/registry/event_forwarder.rs:20-42` |
| **Copy 2** `agent_event_turn`, identical semantics to copy 1 (verified variant by variant) | `crates/giskard-harness-codex/src/lib.rs:1726-1742` |
| **Copy 3** `event_kind(&AgentEvent) -> &'static str`, `pub(super)`, snake-case labels equal to the serde tag | `event_forwarder.rs:100-116` |
| **Copy 4** `event_kind`, private, identical labels | `crates/giskard-server/src/hub.rs:263-279` |
| **Copy 5** `agent_event_kind`, identical labels | `giskard-harness-codex/src/lib.rs:2279-2295` |
| **Copy 6** `event_item_id(&AgentEvent) -> Option<ItemId>`, `pub(super)`: `ItemStarted`/`ItemCompleted` → `item.id`, `ItemDelta` → `item_id`, else `None` | `event_forwarder.rs:118-125` |
| **Copy 7** `event_thread(&AgentEvent) -> ThreadId`, identical to `AgentEvent::thread_id` | `giskard-harness-codex/src/lib.rs:2384-2400` |
| **Copy 8** `event_thread` inside the replay crate's test module, identical | `crates/giskard-harness-replay/src/lib.rs:661-677` |
| **Copy 9** `remap_event_thread(&mut AgentEvent, ThreadId)`: sets `thread` on every variant | `giskard-harness-replay/src/lib.rs:429-445` |
| **Copy 10** `item_id_of(&AgentEvent) -> Option<&str>` in the codex test module: `harness_item_id` of `ItemCompleted` only. Different question (native id, one variant); not an identity accessor | `giskard-harness-codex/src/lib.rs:6420-6425` |
| Callers of copy 1 | `registry.rs:1899`; `event_forwarder.rs:175, 1296, 1318, 2625` |
| Callers of copies 3/4 | `registry.rs:1898`; `hub.rs:239`; `event_forwarder.rs:174, 212, 1309, 1489, 1546, 1777, 1810` |
| Callers of copy 6 | `registry.rs:1900`; `event_forwarder.rs:176, 213, 2644` |
| Callers of copy 2 | `giskard-harness-codex/src/lib.rs:1556, 1692, 2414`; `instance.rs:486` |
| Caller of copy 5 | `giskard-harness-codex/src/lib.rs:2272` |
| Callers of copy 7/8 | `giskard-harness-codex/src/lib.rs:2404`; `instance.rs:465, 560`; `giskard-harness-replay/src/lib.rs:632` (test) |
| Caller of copy 9 | `giskard-harness-replay/src/lib.rs:282` |
| `registry.rs` reaches copies 1, 3, 6 through `super::event_forwarder::{event_turn_id, event_kind, event_item_id}` re-exports | `registry.rs` imports near the top of the file (grep `event_forwarder::`) |
| The `log_metadata_only_event_rejection` and `log_cross_turn_event_drop` helpers take the label as `&'static str`, so a method returning `&'static str` slots in unchanged | `event_forwarder.rs:182, 200-215` |

## Design

Add to `impl AgentEvent` in `giskard-core/src/event.rs`, next to `thread_id`:

```rust
/// The serialized `kind` tag of this variant, for logs and diagnostics.
///
/// Kept equal to the `#[serde(tag = "kind", rename_all = "snake_case")]` name so a log line
/// and a wire frame name the same event the same way; the test below pins that.
pub fn kind(&self) -> &'static str { /* the 13-arm table, once */ }

/// The turn this event belongs to, when it names one.
///
/// Turn-scoped events always carry a turn; `ServerRequest*`, `Error` and `Notice` may be
/// thread-scoped; `ThreadOpened` never has one.
pub fn turn(&self) -> Option<TurnId> { /* copy 1's table */ }

/// The Giskard item this event is about, for the three item events.
pub fn item_id(&self) -> Option<ItemId> { /* copy 6's table */ }

/// Re-address the event to another thread. Used by fixtures and replays that rebind a
/// recorded stream to a fresh thread id.
pub fn set_thread(&mut self, thread: ThreadId) { /* copy 9's table */ }
```

Semantics are exactly those of the copies; copies 1 and 2 were compared arm by arm and agree.
`thread_id` stays as is. No other method is added: `harness_item_id` (copy 10) is a Codex test
convenience about native ids, and the forwarder's item-identity and delta-kind helpers encode
forwarder policy (empty native ids are "no identity"), not event identity.

## Every site that changes

| File | Change |
| --- | --- |
| `crates/giskard-core/src/event.rs:104-121` | Add `kind`, `turn`, `item_id`, `set_thread` to the existing `impl` |
| `crates/giskard-server/src/registry/event_forwarder.rs:20-42, 100-125` | Delete `event_turn_id`, `event_kind`, `event_item_id` |
| `event_forwarder.rs:174-176, 212-213, 1296, 1309, 1318, 1489, 1546, 1777, 1810, 2625, 2644` | `event_turn_id(&e)` → `e.turn()`, `event_kind(&e)` → `e.kind()`, `event_item_id(&e)` → `e.item_id()` |
| `crates/giskard-server/src/registry.rs` (import line; `:1898-1900`) | Drop the three names from the `event_forwarder` import; use the methods |
| `crates/giskard-server/src/hub.rs:239, 263-279` | Delete the private `event_kind`; use `event.kind()` |
| `crates/giskard-harness-codex/src/lib.rs:1726-1742, 2279-2295, 2384-2400` | Delete `agent_event_turn`, `agent_event_kind`, `event_thread` |
| `giskard-harness-codex/src/lib.rs:1556, 1692, 2272, 2404, 2414`; `instance.rs:465, 486, 560` | Use `turn()`, `kind()`, `thread_id()` |
| `crates/giskard-harness-replay/src/lib.rs:282, 429-445` | Delete `remap_event_thread`; call `event.set_thread(thread_id)` |
| `giskard-harness-replay/src/lib.rs:632, 661-677` | Delete the test-module `event_thread`; use `thread_id()` |
| `docs/design-straightening-review.md` | Mark C1 / step 1 as landed |

Nothing else compiles against the deleted names. The exit check targets the *symbols*, not
every textual occurrence: `event_turn_id` also names a structured log field
(`event_turn_id = display_opt(..)` at `event_forwarder.rs:175, 193, 1296`) and the log-format
tests at `:2705, 2729` assert on that field name; both stay exactly as they are. The patterns use
`grep -E` with a trailing `\b` so that `event_item_identity`, which is retained, is not matched
as a prefix of `event_item_id`. Validated on the base tree: the first command lists exactly the
nine definitions above and the second exactly their 28 call sites, with no hit on
`event_item_identity`.

```sh
grep -rnE "fn (event_turn_id|event_kind|event_item_id|event_thread|agent_event_turn|agent_event_kind|remap_event_thread)\b" crates/ --include=*.rs
grep -rnE "\b(event_turn_id|event_kind|event_item_id|event_thread|agent_event_turn|agent_event_kind|remap_event_thread)\(" crates/ --include=*.rs
```

After the change both must return nothing. `server_message_kind(` in `hub.rs` and
`event_item_identity(` in `event_forwarder.rs` are not matched by either pattern.

## Tests

In `giskard-core/src/event.rs`, next to the existing serde tests:

1. `kind_matches_the_serde_tag_for_every_variant`. Build one value of each of the 13 variants
   (a small `fn every_variant() -> Vec<AgentEvent>` fixture; the existing round-trip tests show
   how to construct each), serialize with `serde_json::to_value`, and assert
   `json["kind"] == event.kind()` for each. Also assert the fixture has 13 entries so a new
   variant fails this test until it is added to the fixture.

   The `Error` entry must use a unit or struct variant of `HarnessError`, for example
   `HarnessError::Overloaded` or `HarnessError::ThreadBusy { thread }`. `HarnessError` is
   `#[serde(tag = "kind")]`, and serde refuses to serialize an internally tagged *newtype*
   variant whose payload is a string or id (`Spawn`, `Transport`, `Protocol`, `Unsupported`,
   `Timeout`, `ThreadNotFound`); the unit and struct variants serialize normally. Do not change
   `HarnessError`'s serde shape in this step; see the note below.
2. `turn_is_present_exactly_where_the_variant_carries_one`. Over the same fixture: the eight
   turn-scoped variants return `Some`, `ThreadOpened` returns `None`, and the four optional
   variants return their field (build each once with `Some` and once with `None`).
3. `item_id_names_the_item_for_the_three_item_events`. `ItemStarted` and `ItemCompleted` return
   `item.id`, `ItemDelta` returns `item_id`, every other variant `None`.
4. `set_thread_readdresses_every_variant`. For each fixture entry, `set_thread(new)` then
   `thread_id() == new`.

No other test changes are needed: the replay crate's `:632` assertion and the codex tests keep
their meaning through the method calls. The forwarder's log-format tests (`:2619-2700`) compare
strings that the method returns unchanged.

## Found while planning: `HarnessError` newtype variants are not JSON-serializable

`HarnessError` (`crates/giskard-core/src/error.rs:8-20`) is internally tagged, and six of its
eleven variants are newtypes over `String` or `ThreadId`. serde rejects that combination at
serialization time ("cannot serialize tagged newtype variant"), so any `AgentEvent::Error` that
carries one of them fails `serde_json::to_value`. Nothing on the wire is affected: the browser
sees `WireHarnessError`, which is converted. It does affect anything that writes `AgentEvent`
itself as JSON, such as the replay fixture format in `giskard-harness-replay`. This is out of
S1's scope; open an issue for it. The likely fix is `#[serde(tag = "kind", content = "detail")]`
or struct variants, which is a format change and needs its own plan.

## Order of work

1. Add the four methods and tests 1–4 to `giskard-core`; `cargo test -p giskard-core`.
2. Replace and delete in `giskard-server` (`event_forwarder.rs`, `registry.rs`, `hub.rs`).
3. Replace and delete in `giskard-harness-codex` (`lib.rs`, `instance.rs`).
4. Replace and delete in `giskard-harness-replay`.
5. The two greps above; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo fmt`;
   `cargo test --workspace` minus Playwright.

Expected size: about 60 added lines in core, about 150 deleted elsewhere. One PR.

## Pitfalls

- `kind()` must return the serde tag, not a prettier label. The three existing label tables
  already match the tag; test 1 keeps it that way.
- Keep `thread_id` as the name of the thread accessor; do not rename it to `thread` for symmetry
  with `turn`. It has 20+ callers and the rename would swamp the diff.
- `event_turn_id` is `pub(super)` and imported by `registry.rs`; remove the import in the same
  commit as the function or the build breaks between steps.
- Do not rename the `event_turn_id` structured log field or the test strings that match it.
  Only the function goes; the field is part of the log contract those tests pin.
- Do not fold `event_item_identity` into `item_id`. The former deliberately returns `None` for
  an empty native id, which is forwarder policy.
- Do not add a `Display` impl or a `From<&AgentEvent> for &'static str`; a named method is what
  the call sites read as.

## Stop rules

Stop and re-cut if the diff:

- changes any variant, field, or serde attribute of `AgentEvent`;
- adds a method that is not a pure projection of one variant's fields;
- touches `WireAgentEvent::from_agent_event` or any hub lane logic (that is S5);
- edits a test assertion's expected value.
