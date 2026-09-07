# S9 — `AgentHarness` by scope, and what a second harness needs from it

Implementation plan for step 9 of [`design-straightening-review.md`](design-straightening-review.md)
(finding C4). Written against `main` at `f830df8` (S8 merged); every file and line reference
below was checked against that tree. Re-check them if the branch has moved.

Revision 2, written after implementing: four corrections, all in the checks and the shorthand
rather than in the design. (1) Exit check E asked for an `AgentHarness` import in `routes.rs`
that the code does not need and the compiler rejects — see the pitfall. (2) Exit check D missed
the multi-line call chains, the way S5's first cut did; it needs `-U`. (3) Exit check F counts 3,
not 4: D3's own trait text says "unique across", so the phrase "unique within" lands in three
places, not four. (4) The D2 table's shorthand for the two best-effort functions' error arms says
`(None, Vec::new())`, but they return a `ModelListingWarning`; the arms are moved unchanged, as
the sentence under the table already required. Every correction is applied below.

This plan is written for discussion first. The mechanical half (D1, D2) is settled and verified;
the contract half (D3, D4) states what the trait must promise so that an adapter with a different
process model can implement it; and **Decisions to settle** lists the four choices that need an
owner's answer before an agent starts.

## Goal

The review's C4 has two parts. Part one is plumbing: seven `HarnessRegistry` methods are pure
pass-throughs to the project's harness and exist only so `routes.rs` never touches the trait.
Part two is the trait: its 21 methods fall into three scopes, and the review proposed three
traits.

The owner's concern is different from the review's: the trait must not be modelled on Codex's
process model (one `app-server` process per project hosting every thread), because a Claude Code
adapter runs one process per primary thread. This plan therefore answers, per method, whether
the trait assumes a process model, and turns the answer into contract text on the trait.

After S9:

- `HarnessRegistry::harness(&ProjectConfig)` is the one way routes reach a project's harness;
  the seven pass-throughs are gone and routes call the trait.
- The trait's documentation states its scopes and the three contracts a multi-process adapter
  needs: one instance per working context regardless of process count, request ids unique within
  the instance, and per-thread stream end.
- Whether the trait is split into three is a settled decision, not an open one.

No behaviour change.

## Is the trait shaped by Codex's process model?

Each of the 21 methods (`crates/giskard-harness/src/lib.rs:476-628`), what it assumes, and what
a one-process-per-primary-thread adapter would do.

| Method | Scope | Process assumption in the signature | One-process-per-thread adapter |
| --- | --- | --- | --- |
| `capabilities` `:477` | instance | none; sync, static | static table |
| `client_version` `:496` | instance | none | `claude --version` at factory time |
| `list_models` `:480` | instance | none | static or CLI |
| `list_providers` `:485` | instance | none | `Unsupported` (default) |
| `list_mcp_servers` `:501`, `reload_mcp_servers` `:508`, `start_mcp_oauth_login` `:515` | instance | none | CLI or `Unsupported` (defaults) |
| `discoveries` `:554` | instance | none; default closed | closed, or synthesized in-process sub-agents |
| `shutdown` `:627` | instance | none; "cleanly shut down the harness" | stop every thread process |
| `open_thread` `:522` | thread | none | spawn or resume a process; the handle's `harness_thread_id` is the session id |
| `claim_native_thread` `:530` | thread | none; default `Unsupported` | default |
| `subscribe` `:551` | thread | **sync**: must return a stream for any handle the instance issued, before traffic | a retained `EventLog` per thread, filled by the process's reader; Codex does the same (`SenderMap`, codex README "Runtime ownership") |
| `set_thread_name` `:595`, `set_thread_archived` `:603`, `delete_thread` `:616` | thread | none; defaults | archive/delete may stop the process |
| `compact_thread` `:576`, `interrupt` `:573` | thread | none | write to that thread's process |
| `start_turn` `:543` | turn | none | write a user message to that thread's process |
| `terminate_command` `:584` | turn | none | `Unsupported` (default) |
| `respond_approval` `:559`, `respond_server_request` `:566` | turn | **no thread in the signature**: the adapter routes by id | native `request_id`s are per process; the adapter must namespace them |

Conclusion: no signature names a process. `HarnessFactory::create` (`registry.rs:75-84`) builds
one instance per project, and spec §4.7's first bullet already states the generalisation ("one
working context = one harness instance"). Two methods carry a contract the docs do not state
(`subscribe` before traffic; request ids unique across processes), and one server behaviour the
spec describes at project scope is actually per thread (a stream ending is handled by that
thread's forwarder, `event_forwarder.rs` `handle_stream_error` `:1224-1325`; the spec's §4.7
"crash handling" bullet talks of marking the project's threads disconnected). Those three are
what D3 writes down.

Two things the server relies on that are conventions, not trait shape, and would bind a second
adapter (recorded, not changed here; see decision C):

- The compaction marker is an `Activity` item whose title equals `"Context compacted"`
  (`event_forwarder.rs:3-8`), produced by the Codex mapper (`mapping.rs:755`), checked by the
  Codex adapter itself (`codex/src/lib.rs:1746`), emitted by the replay harness (`replay/src/lib.rs:379`)
  and asserted by two end-to-end tests (`e2e_smoke.rs:606`, `:2561`, `:2605`). A second adapter
  must emit that exact string for manual compaction to be tracked.
- Running-status vocabulary: `command_status_is_running` accepts `in_progress`, `inprogress`,
  `running`; `tool_status_is_running` adds `pending` (`giskard-core/src/item.rs:63-73`, `:267-272`).
  Documented in core; an adapter emits those strings.

## Several harnesses in one project

The owner also wants a project to be able to run, say, one thread on Codex and another on Claude
Code. The code does not support that today, and S9 neither adds it nor makes it harder; it is
recorded here so the trait text and the route change are written with it in view.

Where the one-harness-per-project assumption lives (all verified):

| Assumption | Where | What multi-harness changes |
| --- | --- | --- |
| One harness kind per project | `ProjectConfig.harness: String` (`giskard-persist/src/store.rs:57`); the production factory rejects anything but `"codex"` (`bin/giskard-server.rs:25-28`) | the thread-start request chooses the harness, as it chooses model and mode |
| A thread's harness is implied by its project | `ThreadFile` carries `harness_thread_id` but no harness kind (`store.rs:77-100`) | an additive, versioned field on the thread file |
| One instance per project | `ProjectHarnessSlot` holds one `Active(harness, driver)` (`registry/project.rs:110-118`); `get_or_create_harness` and `active_harness` are keyed by project | keyed by (project, kind); turn-facing paths resolve a thread to its harness through the recorded kind |
| One driver and one bootstrap table per project | `spawn_project_event_driver` in `get_or_create_harness` (`registry.rs:715-722`); `known_thread_bindings(project)` hands the whole thread table to the one instance (`:733-772`) | one driver per instance; the table filtered by kind |
| Instance-scoped answers are per project | the seven route functions in correction 4; the per-project model catalog (`project_model_catalog`, `registry.rs:628-660`); capability-driven UI (spec §13.5) | answers per (project, kind); the UI adapts per thread |
| One `app-server` per project | spec §4.7, §6.4 | one instance per (project, kind) |

S9's part in this: after D1 and D2 the routes reach a harness through one method,
`registry.harness(config)`, so a harness kind is added to one signature rather than to seven
facades. The trait needs nothing: it is per instance, and several instances per project are
several values behind the same trait. D3's doc text is worded for that ("one value per working
context; a project may hold several").

## Corrections to the review

1. **Option 2's trigger is gone.** The review offered the split "if C6 wants it", because 21
   fakes then implemented the whole trait. After S4b there are nine `impl AgentHarness for`
   blocks, and trait defaults already let each implement what it uses: `ShutdownHarness` /
   `BindingOrderHarness` 9 methods, `TestIntentHarness` 10, `DiscoveryHarness` / `TestHarness`
   11, `ScriptedHarness` 12, `FakeHarness` 14, `ReplayHarness` 16, `CodexHarness` 21. A split would not reduce what any of them, or a Claude Code adapter, must write.
2. **A split has a cost the review did not count.** Calling a supertrait method on
   `dyn AgentHarness` requires the supertrait in scope, so every file that calls the trait
   (`registry.rs`, `registry/driver.rs`, `registry/event_forwarder.rs`, `routes.rs` after D2,
   `giskard-testenv/src/fake.rs`, `tests/e2e_smoke.rs`) gains imports, and every adapter and
   fake becomes three `impl` blocks. Trait upcasting (`Arc<dyn AgentHarness>` to
   `Arc<dyn HarnessInstance>`) is available at MSRV 1.89 (stable since 1.86), so narrow views
   would coerce, but nothing in the server needs a narrow view today.
3. **"Process-scoped" is the wrong word.** The nine instance methods are scoped to the harness
   instance, which is one per project working context. Calling that group "process" is exactly
   the Codex assumption the owner wants kept out of the trait. This plan says "instance".
4. **Seven facades, thirteen call sites, no tests.** The pass-throughs are
   `registry.rs:1322-1376`; their callers are all in `routes.rs` (13 calls in 7 functions:
   `refresh_project_model_catalog` `:3447-3543`, `harness_provider_table` `:3551-3592`,
   `overlay_harness_metadata` `:3598-3650`, `list_mcp_servers` `:3652-3699`,
   `reload_mcp_servers` `:3701-3736`, `start_mcp_oauth_login` `:3738-3783`,
   `harness_knows_provider` `:5830-5867`). No test in the workspace calls any of the seven
   (`rg 'registry\s*\.\s*(list_models|…)\('` matches only `routes.rs`), so "and their tests"
   removes nothing.
5. **The registry has 25 `pub async fn`, not 30.** Seven are the pass-throughs.

## Decisions to settle

Settled by the owner (revision 1): **A no split, B no thread-close hook for now, C yes as S9b,
D yes.** The plan below assumes exactly those answers; the alternatives stay recorded for the
reasoning, not as open options. S9 introduces no crate and no trait: `giskard-harness` already
holds the trait, and S9 is the seven pass-throughs, the route migration, and the contract text.

**A. Split the trait into `HarnessInstance` / `HarnessThreads` / `HarnessTurns`?**
Recommended: **no**. Corrections 1 and 2 say why: no adapter or fake writes less, every caller
imports more. The scope grouping is still made explicit, as documentation (D3) and as the order
of the methods in the trait. If **yes**: D5 gives the shape (three `#[async_trait]` traits, a
blanket `AgentHarness`, neutral names, defaults kept on their traits); add the supertrait imports
to the six files in correction 2 and turn each of the nine `impl` blocks into three; exit check
counts change as D5 states.

**B. Add a thread-close hook for adapters that own a process per thread?** Recommended: **no,
not in S9**. The adapter already hears the two durable thread ends the server initiates,
`delete_thread` (`registry.rs:1378-1393`) and `set_thread_archived` (`:1288-1303`), and the
instance end, `shutdown`. What it does not hear is the server dropping a thread from memory
(`retire_thread` `:1415-1422`, `forget_thread` `:1402-1413`; called from `routes.rs:1656`,
`:1918`, `:6003` and `registry.rs:1391`, `:1603`) — for Codex that is meaningless, and for a
per-thread process it is an idle question, which spec §4.7 already assigns to the adapter as an
optional idle shutdown. If Claude Code's adapter needs an explicit signal later, it is one
default-`Ok(())` method plus one call in `retire_thread`, a behaviour change that deserves its
own step with the adapter in hand.

**C. Replace the `"Context compacted"` title convention with a typed marker?** Recommended:
**yes, as S9b**, not here: it touches `giskard-core` (`ItemPayload::Activity` or a new event),
the Codex mapper and its tests, the replay harness, the forwarder, and two end-to-end tests, and
it changes a persisted item shape. S9 records it; a plan for S9b follows the same discipline.

**D. State the request-id contract?** Recommended: **yes, in S9** (D3 and D4). It is
documentation of a rule Codex already satisfies (JSON-RPC ids are per connection, and Codex has
one connection per instance) and that a multi-process adapter must satisfy deliberately.

## Non-goals

- No change to any trait method signature, default body, or error string.
- No change to `HarnessFactory`, `HarnessBootstrap`, `ThreadHandle`, `OpenThreadOptions`,
  `HarnessCapabilities`, or any `giskard-core` type.
- No change to the Codex adapter, the replay harness, the test fake, or the five test-local
  fakes.
- No route, path, request, or response change; `docs/api-endpoints.md` is untouched.
- No change to which capability gates the routes apply or to the warnings they log.
- No change to `get_or_create_harness`'s locking, the fast path, or the bootstrap read.

## Ground truth

| Fact | Where |
| --- | --- |
| The trait and its 21 methods (`:476-628`); the trait-level doc is four lines (`:471-474`) | `giskard-harness/src/lib.rs` |
| Required methods (no default): `capabilities`, `list_models`, `open_thread`, `start_turn`, `subscribe`, `respond_approval`, `respond_server_request`, `interrupt`, `shutdown` (9); the other 12 have defaults | read |
| Nine `impl AgentHarness for` blocks: `registry.rs` ×3, `registry/driver.rs`, `registry/event_forwarder.rs`, `bin/giskard-server-replay.rs`, `giskard-harness-codex/src/lib.rs`, `giskard-testenv/src/fake.rs`, `giskard-harness-replay/src/lib.rs` | `rg 'impl(<[^>]*>)? AgentHarness for'` |
| `HarnessFactory::create(config, bootstrap) -> Arc<dyn AgentHarness>`; one instance per project | `registry.rs:75-84` |
| `get_or_create_harness(&self, project: ProjectId, config: &ProjectConfig)` is private, `:679-731`; 12 production callers pass `(config.id, config)`; 15 test callers pass `(project, &config)` | grep |
| The seven pass-throughs: `list_mcp_servers` `:1322-1329`, `list_models` `:1330-1338`, `list_providers` `:1340-1347`, `client_version` `:1348-1354` (returns `Option`, swallowing a creation error with `.ok()?`), `capabilities` `:1356-1362`, `reload_mcp_servers` `:1364-1367`, `start_mcp_oauth_login` `:1369-1376` | read |
| Their 13 callers, by route function (correction 4): `:3494` `client_version`; `:3555` `capabilities`, `:3574` `list_providers`; `:3605` `capabilities`, `:3624` `list_models`; `:3663` `capabilities`, `:3676` `list_mcp_servers`; `:3712` `capabilities`, `:3727` `reload_mcp_servers`; `:3756` `capabilities`, `:3772` `start_mcp_oauth_login`; `:5838` `capabilities`, `:5853` `list_providers` | `routes.rs` |
| `routes.rs` imports `giskard_harness::HarnessProvider` (`:33`) and `AgentHarness` only inside `mod tests` (`:2106`); `harness_api_error` `:3785` maps `HarnessError::Unsupported` to `BadRequest` `:3787` | grep |
| The server calls `harness.<method>` in production at: `registry.rs` (`capabilities`, `client_version`, `list_*` ×3, `reload_mcp_servers`, `start_mcp_oauth_login`, `delete_thread`, `interrupt`, `set_thread_archived`, `set_thread_name`, `terminate_command`, `shutdown` ×2), `registry/driver.rs` (`subscribe`), `registry/event_forwarder.rs` (`compact_thread`; `start_turn` via a boxed future); `admission.rs` none | grep |
| The spec's stated generalisation: "one working context = one harness instance" | `specs/giskard-specification.md:2258-2261` |
| The spec's `ApprovalId` sketch says "harness-native request id (opaque; short-lived, not persisted)" and nothing about uniqueness scope | `:1933` |
| Stream end is handled per thread by that thread's forwarder | `event_forwarder.rs:1224-1325` |
| `forget_thread` / `retire_thread` never call the harness | `registry.rs:1402-1422` |
| Compaction marker string sites | `event_forwarder.rs:6`, `codex/src/mapping.rs:755`, `codex/src/lib.rs:1746`, `replay/src/lib.rs:379`, `tests/e2e_smoke.rs:606, :2561, :2605` |
| MSRV 1.89 (`Cargo.toml:22`); trait upcasting stable since 1.86 | read |
| Review anchors: C4 paragraph `:220-237`, C5 heads `:239`; sequencing row 9 `:305` | grep |

## Design

### D1. `HarnessRegistry::harness`

```rust
/// The project's harness instance, created on first use. This is the one way code outside the
/// registry reaches a harness; the trait is the API from here on.
pub async fn harness(
    &self,
    config: &ProjectConfig,
) -> Result<Arc<dyn AgentHarness>, HarnessError> {
    self.get_or_create_harness(config.id, config).await
}
```

Placed where the seven pass-throughs were (`:1322`). `get_or_create_harness` stays private and
unchanged, so its 15 test callers and the 5 production callers that remain are untouched. Delete `:1322-1376`.

### D2. Routes call the trait

Each of the seven functions resolves the harness once, then calls the trait. `capabilities()` is
synchronous and infallible on a harness, so the `Result` handling that guarded creation moves to
the one `harness()` call. Per function:

| Function | Today | After |
| --- | --- | --- |
| `refresh_project_model_catalog` `:3494` | `state.registry.client_version(project_config).await` (an `Option`, `None` on creation failure) | `state.registry.harness(project_config).await.ok().and_then(\|harness\| harness.client_version())` |
| `harness_provider_table` `:3555-3592` | `match registry.capabilities(..)` (`Ok` without `provider_listing` → `(None, [])`; `Err` → warn + `(None, vec![ModelListingWarning { .. }])`), then `match registry.list_providers(..)` | `let harness = match registry.harness(..) { Ok(h) => h, Err(e) => { the same warn and the same `(None, vec![ModelListingWarning { .. }])` } }; if !harness.capabilities().provider_listing { return (None, Vec::new()); } match harness.list_providers().await { .. }` |
| `overlay_harness_metadata` `:3605-3624` | same shape with `model_listing` and `list_models`; its error arm returns `(base, Some(ModelListingWarning { .. }))` | same transformation, that arm likewise moved unchanged |
| `list_mcp_servers` `:3663-3676` | `registry.capabilities(..).map_err(harness_api_error)?`, gate on `mcp_status`, `registry.list_mcp_servers(..)` | `let harness = registry.harness(&project_config).await.map_err(harness_api_error)?; let capabilities = harness.capabilities();` … `harness.list_mcp_servers().await.map_err(harness_api_error)?` |
| `reload_mcp_servers` `:3712-3727` | same with `mcp_reload` | same |
| `start_mcp_oauth_login` `:3756-3772` | same with `mcp_oauth_login` and `name` | same |
| `harness_knows_provider` `:5838-5853` | `match registry.capabilities(..)` (`Err` → warn, `true`; no `provider_listing` → `true`), then `match registry.list_providers(..)` | `let harness = match registry.harness(..) { Ok(h) => h, Err(error) => { same warn; return true } }; if !harness.capabilities().provider_listing { return true; } match harness.list_providers().await { .. }` |

Every warn keeps its text, its fields, and the value it returns; the two best-effort functions'
error arms move onto the `harness()` error arm as they stand, warning vector included.
`routes.rs:33` stays `use giskard_harness::HarnessProvider;` and the test-module import at `:2106`
stays: revision 1 asked for an `AgentHarness` import here, but a method call on `dyn AgentHarness`
resolves through the trait object's principal trait without one, so the import is unused and
`-D warnings` rejects it. See the pitfall.

One observable difference, stated so it is not mistaken for a bug: today each function creates
or fetches the harness twice (`capabilities` then `list_*`), after S9 once. A harness that is
torn down between the two calls today gets recreated by the second; after S9 the second call
runs on the instance already held. That window is a project deletion racing a settings read, and
the held instance answers or errors the way the trait says.

### D3. The trait documents its scopes and contracts

Replace the trait-level doc (`lib.rs:471-474`) with:

```rust
/// The neutral harness contract (spec §4.3).
///
/// One value implements this trait per working context: `HarnessFactory::create` builds it,
/// `shutdown` ends it, and it owns the threads it opened. Today a project has one working
/// context; a project may later hold several (one per harness kind, each with its own threads),
/// and nothing in this trait may assume otherwise. How many operating-system processes stand
/// behind an instance is the adapter's business, and nothing here may depend on the answer
/// either. Codex runs one `app-server` per instance and hosts every thread in it; an adapter for
/// a CLI that runs one process per primary thread spawns in `open_thread` and stops in
/// `delete_thread`, `set_thread_archived`, `shutdown`, or on its own idle policy. Both satisfy
/// this trait unchanged.
///
/// Methods are grouped by scope, in this order:
/// - instance: `capabilities`, `client_version`, `list_models`, `list_providers`,
///   `list_mcp_servers`, `reload_mcp_servers`, `start_mcp_oauth_login`, `discoveries`,
///   `shutdown`;
/// - thread, taking a `ThreadHandle`: `open_thread`, `claim_native_thread`, `subscribe`,
///   `set_thread_name`, `set_thread_archived`, `delete_thread`, `compact_thread`, `interrupt`;
/// - turn: `start_turn`, `respond_approval`, `respond_server_request`, `terminate_command`.
///
/// Three contracts follow from "one instance, any number of processes":
/// - `subscribe` is synchronous and must return a stream for every handle this instance
///   issued, before the native session has produced anything; a retained log the session's
///   reader fills later satisfies it.
/// - `ApprovalId` and `ServerRequestId` name a pending request within the instance, and the
///   responses carry no thread. An adapter fronting several processes must make its native
///   request ids unique across them before publishing them.
/// - A thread's event stream ends when that thread's native session ends, and the server
///   handles every stream end per thread. An adapter must not close other threads' streams
///   because one session ended, and must close the ended thread's stream rather than leave it
///   open.
///
/// Every method is dyn-compatible: `&self` receivers, no generic method params, no `Self`-by-value.
/// The whole application holds harnesses as `Arc<dyn AgentHarness>`.
```

Reorder the methods inside the trait to match the three groups (today `discoveries` sits between
`subscribe` and `respond_approval`, `:554`; `interrupt` and `compact_thread` sit between the
turn methods, `:573-582`). Reordering trait items changes no behaviour and no impl. Add a
one-line `// instance scope` / `// thread scope` / `// turn scope` comment above each group.

Add to `respond_approval`'s doc (`:558`) and `respond_server_request`'s (`:565`): "The id is
unique within this instance (see the trait doc)." Add to `subscribe`'s (`:550`): "Must succeed
for any handle this instance issued, before the session has produced events."

### D4. Spec

One line at `specs/giskard-specification.md:1933`: the `ApprovalId` sketch comment becomes
`// harness-native request id (opaque; short-lived, not persisted; unique within one harness
instance)`. Nothing else: §4.7's first bullet already carries the instance rule, and the crash
bullet describes the Codex adapter's situation, which D3 does not contradict (every thread of a
one-process instance ends together).

### D5. Only if decision A is "split"

```rust
#[async_trait] pub trait HarnessInstance: Send + Sync { /* the 9 instance methods */ }
#[async_trait] pub trait HarnessThreads: Send + Sync { /* the 8 thread methods */ }
#[async_trait] pub trait HarnessTurns: Send + Sync { /* the 4 turn methods */ }
pub trait AgentHarness: HarnessInstance + HarnessThreads + HarnessTurns {}
impl<T: HarnessInstance + HarnessThreads + HarnessTurns + ?Sized> AgentHarness for T {}
```

Defaults stay on the trait that owns the method. The nine `impl AgentHarness for X` blocks each
become three `impl` blocks with the same bodies. The six files in correction 2 add
`use giskard_harness::{HarnessInstance, HarnessThreads, HarnessTurns};` as needed. The D3 doc
moves to `AgentHarness` with the scope list becoming the three trait names. No other change.

## Every site that changes

| File | Change |
| --- | --- |
| `crates/giskard-server/src/registry.rs` | `:1322-1376` replaced by `harness` (D1) |
| `crates/giskard-server/src/routes.rs` | `:33` import; the seven functions in D2 |
| `crates/giskard-harness/src/lib.rs` | `:471-474` doc; method order; three method docs (D3) |
| `specs/giskard-specification.md` | `:1933` (D4) |
| `docs/design-straightening-review.md` | a `**Status: landed in S9**` paragraph after the C4 paragraph (`:220-237`, before C5 at `:239`) naming corrections 1–5 and the decisions taken; row 9 (`:305`) gains ` — **landed in S9**` |

With decision A "split": also the nine `impl` sites and the six import sites (D5).

## Tests

No test changes. The seven pass-throughs have no tests (correction 4); the route behaviour is
unchanged and the routes' existing tests cover the capability gates. If a test for D1 is wanted,
one `#[tokio::test]` in `registry.rs`'s test module: `harness()` twice for one config returns
`Arc::ptr_eq` instances, and the factory was called once. It fits the existing fixtures
(`UnusedHarnessFactory` at `:1962` shows the shape) and is the only permitted addition.

## Order of work

0. Record the baselines in **Exit checks**.
1. D1: add `harness`; leave the seven in place. Build.
2. D2: migrate the seven route functions one at a time, each followed by
   `cargo test -p giskard-server routes`. Then delete `registry.rs:1322-1376`. Build; the
   compiler finds any caller the grep missed.
3. D3, D4: docs and method order. `cargo doc -p giskard-harness --no-deps` builds without
   warnings.
4. `cargo fmt --all`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
   `cargo test --workspace`; the review-doc markers.

## Exit checks

Run from the repository root. "Before" is `main` at `f830df8`. Check K compares against `main`,
so run it against the remote's `main` rather than a stale local ref.

```sh
R=crates/giskard-server/src/registry.rs
S=crates/giskard-server/src/routes.rs
H=crates/giskard-harness/src/lib.rs
A: rg -c 'pub async fn (list_mcp_servers|list_models|list_providers|client_version|capabilities|reload_mcp_servers|start_mcp_oauth_login)\(' $R
B: rg -c 'pub async fn harness\(' $R
C: rg -U -c 'registry\s*\.\s*(list_mcp_servers|list_models|list_providers|client_version|capabilities|reload_mcp_servers|start_mcp_oauth_login)\(' $S
D: rg -U -c 'registry\s*\n?\s*\.\s*harness\(' $S
E: rg -c '^use giskard_harness::HarnessProvider;' $S
F: rg -c 'unique within' $H specs/giskard-specification.md
G: rg -c '^    // (instance|thread|turn) scope' $H
I: rg -c 'impl(<[^>]*>)? AgentHarness for' crates | awk -F: '{s+=$2} END{print s}'
J: rg -c 'pub async fn ' $R
K: git diff --stat main -- crates/giskard-harness-codex crates/giskard-harness-replay crates/giskard-testenv crates/giskard-core crates/giskard-proto crates/giskard-persist docs/api-endpoints.md
L: cargo doc -p giskard-harness --no-deps 2>&1 | rg -c warning
```

| Check | Before | After |
| --- | --- | --- |
| A, the seven pass-throughs | 7 | 0 |
| B | 0 | 1 |
| C, route calls to the seven | 13 | 0 |
| D, route calls to `harness` | 0 | 7 (one per function in D2) |
| E, the import line unchanged | 1 | 1 |
| F, the request-id contract | 0 | 3 (`respond_approval`, `respond_server_request`, spec; the trait doc states the same rule as "unique across them") |
| G, scope comments | 0 | 3 |
| I, adapter and fake impls | 9 | 9 (27 with decision A "split") |
| J | 25 | 19 |
| K | — | empty |
| L | 0 | 0 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings`; `cargo test --workspace` | clean, green | clean, green |

## Pitfalls

- **`client_version` swallowed creation errors on purpose** (`registry.rs:1349-1354`, comment
  at `:1350-1351`). The D2 replacement keeps that with `.ok().and_then(..)`; do not turn it into
  `?`.
- **`capabilities()` is not `async` and not `Result` on the trait.** Dropping `.await` and the
  `map_err` at the four `capabilities` sites is correct; forgetting to drop them is a compile
  error, not a silent change.
- **Keep every warn.** The two `match` shapes in `harness_provider_table` and
  `harness_knows_provider` each carry a warn with `action = "provider_is_known"` or the
  provider-table wording; move them onto the `harness()` error arm unchanged.
- **Do not import `AgentHarness` into `routes.rs`.** Revision 1 said the trait must be in scope
  to call its methods on `Arc<dyn AgentHarness>`. It does not: for a receiver of type
  `dyn Trait` the principal trait's methods are inherent candidates, so the calls resolve with
  no import, and adding one leaves it unused, which `-D warnings` rejects. D2's code never names
  the type. Exit check E is now that `:33` is unchanged.
- **Reordering trait methods is safe; renaming or re-signing is not.** D3 moves items and adds
  comments only.
- **`cargo doc` must stay warning-free**: the new doc uses backticked names that exist; do not
  add intra-doc links to items outside the crate.
- **Do not touch the spec beyond `:1933`.** §4.7 is Codex's process section by title and stays.

## Stop rules

Stop and report instead of improvising if:

- a route needs a registry method that is not `harness` to do what it does today (the design
  missed a caller);
- keeping a warn's text and fields needs a value the trait call does not give;
- `cargo doc` warns on the new text and the fix would change a name;
- decision A is answered "split" and the blanket impl or upcasting fails to compile at MSRV
  1.89 (then report the compiler's message; do not add `#[allow]` or raise the MSRV).
