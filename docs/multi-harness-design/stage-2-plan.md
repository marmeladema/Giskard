# Stage 2 implementation plan: one harness per thread

Implements Stage 2 of [`../multi-harness-design.md`](../multi-harness-design.md), on top of the
merged Stages 0 and 1. This plan is written for an implementing agent. Every file, symbol, and
string below was verified against `main` at `4cfb4e8`. Line numbers are for orientation; the
symbol or string quoted beside each is the thing to find.

Read `AGENTS.md` first. Its rules that bind this stage: `cargo fmt --all` and
`cargo clippy --all-targets -- -D warnings` must pass; `README.md`, `docs/api-endpoints.md`,
`docs/subagents.md`, and the spec are updated in the same change as the code they describe; keyed
state joins the struct whose cleanup site matches its lifetime and carries the lifetime-class
comment; no peer owning map keyed by project or thread identity; `history.jsonl` records gain no
agent-driven field; no `unwrap`/`expect`/`panic!` on runtime paths; every failure mode gets a test;
Markdown is wrapped at 100 columns.

## Outcome

After this stage the draft composer's model picker lists the models of every declared harness,
grouped by harness when more than one is declared. Picking a model picks its harness, and the
thread is created on that instance. Each thread of a project may run on a different declaration.
An existing thread's picker, MCP menu, and compaction control address the thread's own instance.
A config with one declaration, and every existing data directory, behaves exactly as today.

## Scope

Seven work packages, in this order. Each leaves the tree green and is one commit.

1. `ThreadFile.harness`, stamped everywhere a thread file is created.
2. The registry: one instance per project and declaration, bootstrap filtered per instance, and
   every operation resolving the instance by thread.
3. `HarnessFactory::create` receives the declaration name.
4. Model catalogs per instance and the grouped models response.
5. Thread wire fields and the harness-scoped MCP routes.
6. The browser: grouped picker, harness-aware keys, per-instance capabilities and MCP.
7. Documentation.

## Non-goals

No change to `ModelRef`, turn records, the token ledger, or `[providers]`. No `ProjectSummary`
harness field: the project list is built from `projects.json`, which has no harness column, and
the draft learns the project's default from the models response instead. No thread badge in the
sidebar beyond a `title` tooltip. No per-declaration provider overlay; the same-id
different-endpoint warning from the design is deferred with it. No screenshot regeneration: the
replay server declares one harness, so the picker, the MCP menu, and the sidebar render exactly as
before.

## Decisions that differ from the design document

Amended in work package 7.

- **The models response is one flat list with a per-harness index, not nested groups.** The
  design says the response "returns one group per declared harness". `state.models` in `app.js`
  is a flat list keyed by provider and model in about two dozen places, and the existing
  integration tests read `body["models"]`. The response therefore keeps `models` and `warnings`
  at the top level, adds `harness` to each entry and each warning, and adds a `harnesses` index
  carrying each group's kind, default flag, capabilities, and whether it answered. A single
  declaration produces the exact response served today plus the new fields.
- **An existing thread's catalog request is scoped by query.** The design says "once a thread
  exists, its picker requests one group". This is `GET /api/projects/{id}/models?harness=<name>`,
  which composes only that instance. Without the query, every declared instance is composed and
  created, which is what a draft needs and what the design accepts. Opening a thread on one
  declaration must not spawn the others.
- **`HarnessFactory::create` gains a `harness: &str` parameter.** The design has it take a
  declaration; the factory already owns the catalog since Stage 1, so the name is enough and the
  registry never holds a declaration. The `giskard_testenv::factory::from_fn` closures keep their
  two-argument shape through the adapter, so the twenty-odd test factories are untouched.

## Work package 1: the thread field

### `crates/giskard-persist/src/store.rs`

`ThreadFile` (line 71) gains, after `harness_thread_id`:

```rust
/// The `[harnesses.<name>]` declaration this thread runs on. Fixed at native creation, for
/// the same reason `current_model`'s provider is: the native id exists in exactly one harness
/// home. Files written before the field carry no value and belong to the reserved `codex`,
/// which is what they always meant; the constant default keeps the project file non-load-bearing
/// at read time. Skipped on write when it is that constant so a thread on the default harness
/// stays readable by an older binary under `deny_unknown_fields`.
#[serde(default = "default_thread_harness", skip_serializing_if = "is_default_thread_harness")]
pub harness: String,
```

with `default_thread_harness()` returning `HarnessCatalog::SYNTHESIZED_NAME.to_string()` and
`is_default_thread_harness(value: &str) -> bool`. No version bump: `revision` and `kind` set this
precedent, and the design fixes it.

### Every construction site

`grep -rn "ThreadFile {" crates --include=*.rs` finds 61 literal constructions and none use
struct-update syntax, so every one must add the field. The production sites and what they stamp:

- `crates/giskard-server/src/routes.rs:962`, in `start_thread_with_message`: the harness the
  draft chose (work package 5), else the project's `harness`.
- `crates/giskard-server/src/registry/admission.rs:72`, `orphan_file`: the admitting instance's
  declaration name, passed in by `admit` (work package 2). The sub-agent classification reuses the
  same file, so a linked child inherits it.
- `crates/giskard-testenv/src/fixtures.rs:46`, `persist_primary_thread`: add a sibling
  `persist_primary_thread_on(store, project, thread, native, model, harness: &str)` and make the
  existing helper call it with `codex`, so multi-harness tests can place threads on a named
  declaration without touching the many single-harness callers.

The remaining 58 are tests and `store.rs` fixtures; each adds `harness: "codex".into()` or the
fixture's constant. Add to `store.rs` tests:

- `a_thread_file_without_harness_reads_as_codex`: the JSON of a file written before the field
  deserializes with `harness == "codex"`.
- `a_default_harness_is_not_written_and_another_is`: round-trip a file on `codex` and check the
  serialized JSON has no `harness` key; round-trip one on `nightly` and check it does.

## Work package 2: the registry

### Slots per declaration, `crates/giskard-server/src/registry/project.rs`

`ProjectAuthority` (line 53) replaces its two slots:

```rust
/// One installed instance per `[harnesses.<name>]` declaration this project has used.
///
/// Lifetime class: entries are created on first use by `get_or_create_harness`, removed by
/// `delete_project` and registry `shutdown` through the same transition guard, and never
/// otherwise. Keyed by declaration name, which is configuration, not entity identity: this is
/// the authority's own state, not a peer owning map.
harnesses: Mutex<IndexMap<String, ProjectHarnessState>>,
/// The composed catalog per declaration, keyed the same way and cleared by
/// `clear_model_catalog` (project delete) or replaced whole by a refresh.
model_catalogs: RwLock<HashMap<String, Vec<ModelDescriptor>>>,
```

`HarnessTransitionGuard::project(&authority)` becomes `project(&authority, harness: &str)` and
`ProjectHarnessGuard` holds the whole map guard plus the name; its methods (`active`,
`active_or_creatable`, `publish_active`, `driver`, `begin_delete`, `rollback_delete_if_running`,
`finish_delete`) operate on that name's entry and keep their bodies. Add
`HarnessTransitionGuard::project_all(&authority) -> Vec<(String, ProjectHarnessGuard)>`-style
iteration for the two whole-project paths, `delete_project` and `shutdown`, or simpler: a
`take_all_for_shutdown` and a `begin_delete_all` on a `ProjectHarnessesGuard` that holds the map
guard. `model_catalog`, `replace_model_catalog`, and `clear_model_catalog` take a name; `clear`
without a name clears all and is what project deletion calls.

### `RegistryShared`, `crates/giskard-server/src/registry.rs`

- `active_harness(project_id)` (line 343) and `event_driver(project_id)` (line 349) gain
  `harness: &str`.
- `get_or_create_harness(project, config)` (line 734) gains `harness: &str`, locks the named
  slot, and passes the name to `known_thread_bindings` and to `factory.create` (work package 3).
  The driver it spawns receives the name too (below).
- `known_thread_bindings(project)` (line 794) gains `harness: &str` and keeps only the threads
  whose `harness` equals it. The two uniqueness checks run over that filtered set: two instances
  may legitimately hold the same native id string, since each home numbers its own threads.
- `harness(config)` (line 1438) becomes `harness(config, harness: &str)`.
- `attach_subagent_thread` (836), `set_thread_archived` (1402), `set_thread_name` (1419), and
  `delete_thread` (1445) already receive the `ThreadFile` or its ids; each resolves the instance
  from `thread.harness`. `delete_thread` gains a `harness: String` parameter beside
  `harness_thread_id`, since its caller has the file.
- `open_thread` (858) gains `harness: &str`; the route and `ws.rs` callers pass the thread's
  field or the draft's choice.
- `LoadedThreadBinding` (line 207) gains `harness: String`, set at its three production sites
  (`open_thread` at 911, `ensure_subagent_thread_open` at 1946, `admission::admitted` at 40) and
  its 19 test literals. This is what lets `forget_thread` (1469) and `install_event_owner` (1957)
  find the right driver: both take the name from the binding, `forget_thread` through
  `authority.coordinator()` and `coordinator.binding()`. A thread with no coordinator has no
  owner to detach, which is already the branch that path takes.
- `open_subagent_link` (1312) resolves the parent's file and uses its `harness` for the driver:
  a child is admitted by the instance that ran its parent.
- `steer_turn` (1185), `interrupt`, `compact_thread`, `respond_approval`, and
  `respond_server_request` reach `active_harness(project_id)` at lines 1009, 1071, 1193, 1246,
  and 1368; each already holds a resolved binding or authority, so they pass `binding.harness`.
- `delete_project` (1608) iterates every slot of the project: `begin_delete` all, quiesce every
  driver, collect the project's threads once, shut every harness down, `finish_delete` each, and
  roll back all on the first failure the way the single path does today. `shutdown` (1497)
  drains every slot of every project into its map keyed by `(ProjectId, String)`; the log lines
  gain a `harness` field.

### The driver, `crates/giskard-server/src/registry/driver.rs`

`ProjectEventDriver` (line 308) and `spawn_project_event_driver` (424) gain `harness: String`,
logged on every existing `project_id` log line in the file, and passed to `admission::admit` at
lines 780 and 805. `admit` (`admission.rs:96`) gains `harness: &str` and hands it to
`orphan_file`.

### Tests

Unit, in `registry.rs` tests, using the existing `DiscoveryFactory` and `BindingOrderFactory`
patterns:

- `two_declarations_get_two_instances_and_two_drivers`: a project with threads stamped `stable`
  and `nightly` creates two harnesses, each bootstrapped with only its own bindings (assert
  through the recording factory's bootstrap capture at `registry.rs:2028`).
- `the_same_native_id_on_two_harnesses_is_not_a_conflict`: two thread files with equal
  `harness_thread_id` and different `harness` both bootstrap.
- `delete_project_shuts_down_every_instance` and `shutdown_drains_every_instance_of_a_project`:
  extend the existing deletion and shutdown tests with a second declaration.
- `a_child_is_admitted_by_its_parents_instance`: a discovery on the `nightly` driver persists a
  file stamped `nightly`.

## Work package 3: the factory

`HarnessFactory::create` (`registry.rs:74`) becomes
`create(&self, config: &ProjectConfig, harness: &str, bootstrap)`. `HarnessKindFactory::create`
(`harness_kinds.rs`) resolves `harness` instead of `config.harness`, and its undeclared-name
error and once-per-name warning key on that string. The five direct implementors in
`registry.rs` tests (lines 2251, 2329, 2423, 2447, 2472) and `FailingFactory` in `routes.rs:2154`
add the parameter. `giskard_testenv::factory::FnFactory` (`factory.rs:11`) adds it and keeps
`from_fn`'s two-argument closure; add `from_fn_by_harness` taking
`Fn(&ProjectConfig, &str, HarnessBootstrap)` for tests that construct a different fake per name.
The replay binary's `ScriptedKind` is untouched.

Tests: the Stage 1 `harness_kinds.rs` tests pass the name explicitly; add one asserting a project
stamped `stable` whose thread names `nightly` reaches the `nightly` declaration, which is the
whole point of the parameter.

## Work package 4: catalogs per instance and the models response

### Server

`refresh_project_model_catalog` (`routes.rs:4018`), `harness_provider_table` (4131),
`overlay_harness_metadata` (4181), `project_model_catalog` (3961), `provider_is_known` (4933),
and `harness_knows_provider` gain `harness: &str` and use it for `registry.harness` and the
catalog slot. Every warning `source` stays `harness:<name>` or `provider:<id>`; the name is now
the declaration rather than the project field, which reads the same for a single declaration.
`validate_provider_ids` in `models.rs:512` renames its third parameter to `harness_name`.

Callers pass the thread's field where a thread exists: `routes.rs:685` (open) and `:756`
(read-only context), `ws.rs:556` (send_input), `:756` (select_model), `:1551` (reopen), and
`:1785` (read-only context). At `ws.rs:556` and `:756` the thread file is loaded inside the
mutation; load it once before, which the reopen path at `:1551` already does. The draft path at
`routes.rs:869` passes the harness the request chose or the project's field.

`project_list_models` (3898) takes an optional `harness` query parameter. With it, only that
declaration is composed; it must be declared or the route is `404`. Without it, every
declaration in `state.registry.harness_catalog()` is composed in declaration order; each
instance is created if needed, and a declaration whose instance cannot start contributes a
`harness:<name>` warning and an index entry with `capabilities` absent, never a failure of the
whole response.

### Wire, `crates/giskard-proto/src/lib.rs`

`ListModelsResponse` (line 719) becomes:

```rust
pub struct ListModelsResponse {
    /// Every offered model, each naming the declaration whose instance offers it. A single
    /// declaration yields exactly the list served before this field existed.
    pub models: Vec<HarnessModelEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<ModelListingWarning>,
    /// One entry per composed declaration, in declaration order.
    pub harnesses: Vec<HarnessModelGroup>,
    /// The project's default declaration, which the draft preselects.
    pub project_harness: String,
}

pub struct HarnessModelEntry {
    pub harness: String,
    #[serde(flatten)]
    pub model: ModelDescriptor,
}

pub struct HarnessModelGroup {
    pub name: String,
    pub kind: String,
    pub default: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<HarnessCapabilitiesInfo>,
}
```

`ModelListingWarning` (712) gains `pub harness: String`. The top-level `capabilities` field from
Stage 0 is removed; the browser reads the group's. `HarnessCapabilitiesInfo` is unchanged.

### Tests

`crates/giskard-server/tests/project_models.rs`:

- The existing tests keep passing with `models[i]["harness"] == "codex"` added where they index
  entries, and `project_models_carry_harness_capabilities` reads
  `body["harnesses"][0]["capabilities"]`.
- `project_models_group_two_declarations`: a two-declaration catalog through
  `HarnessKindFactory::with_catalog` over `from_fn_by_harness` fakes whose `list_models` differ;
  the response lists both, each entry stamped with its harness, `harnesses` in declaration order
  with the default marked, and `project_harness` equal to the project's field.
- `a_scoped_models_request_composes_one_instance`: `?harness=nightly` returns only that group,
  and the other fake's constructor was never called (count through the closure).
- `an_undeclared_scope_is_not_found`: `?harness=nope` is `404`.
- `an_instance_that_cannot_start_degrades_its_group`: the second fake returns
  `HarnessError::Spawn`; the response still carries the first group's models, a `harness:nightly`
  warning, and a `harnesses` entry for `nightly` without `capabilities`.

## Work package 5: thread wire fields and MCP routes

### Wire

- `StartThreadRequest` (`lib.rs:634`) gains `#[serde(default)] pub harness: Option<String>`;
  `None` means the project's field. An undeclared name is `400`, using the same message shape
  as `create_project`.
- `StartThreadResponse` (672), `OpenThreadResponse` (624), and `ThreadSummary` (582) gain
  `pub harness: String`. `thread_summary` (`routes.rs:1715`) copies it; the open route's three
  response sites (704, 737, 800) copy `thread_file.harness`; the start route copies the stamped
  value.

### MCP

The three routes at `routes.rs:169` to `:173` move under
`/api/projects/{id}/harnesses/{name}/mcp`, `/mcp/reload`, and `/mcp/oauth-login`. The handlers
take `AxumPath((project_id, harness))`, resolve `state.registry.harness(&project_config,
&harness)`, and answer `404` for an undeclared name. The old paths are removed rather than kept
as aliases: nothing but this browser calls them, and `docs/api-endpoints.md` is the contract.
`crates/giskard-server/tests/e2e_smoke.rs:3241` to `:3326` update their six paths with
`/harnesses/codex/`.

### Tests

- `crates/giskard-server/tests/thread_lifecycle.rs`: a two-declaration server starts one thread
  with `harness: "nightly"` and one without; the responses and the thread list carry `nightly`
  and the project default respectively, and the two threads' files are stamped the same way.
- `read_only_thread.rs`: extend `a_project_naming_an_undeclared_harness_names_the_config_key`
  with a thread stamped `nightly` on a server that declares only `stable`; the thread opens
  read-only naming `nightly`, while a sibling thread stamped `stable` in the same project opens
  normally. This is the "per-thread degraded state, not project-wide failure" rule.
- `security.rs`: `start_thread_rejects_an_undeclared_harness` is `400` and creates no file.
- `e2e_smoke.rs`: the moved MCP paths, plus one assertion that the old path is `404`.

## Work package 6: the browser

All in `crates/giskard-server/static/app.js` unless noted.

- **State.** `state.harnessCapabilities` becomes a map from declaration name to capability
  object, filled from `res.harnesses` in `loadProjectModels` (line 348); `state.projectHarness`
  holds `res.project_harness`; `state.threadHarness` is set from `res.harness` in `openThread`
  (2627) and from the start response in `startDraftThread` (10035), and cleared with the rest of
  the per-thread state at lines 2116, 2532, and 2580. `harnessCan(flag)` (352) looks up the
  active harness: the draft's selected group on a draft, `state.threadHarness` otherwise.
- **Scoped loads.** `loadProjectModels(pid, opts)` gains `opts.harness`; `openThread` passes the
  opened thread's harness, and the draft passes nothing. `state.modelsProject` becomes a key of
  project plus scope so a scoped list is not mistaken for the full one when a draft opens next.
- **Keys.** `modelKey(m)` (10532) and `findModelDescriptor` (10535) take the harness into
  account: the option `value` stays a string for the `<select>`, built from the three parts with a
  separator that cannot appear in a declaration name, and the option carries `dataset.harness`
  beside `dataset.provider` and `dataset.model`; `selectedModelFromControl` (10624) returns the
  triple. `state.currentModel` stays a `ModelRef`; the harness rides on `state.threadHarness`
  or the draft's selection, never inside it.
- **Rendering.** `renderModelSelect` (10488) emits one `<optgroup label="<name>">` per group
  in `res.harnesses` order, only when `state.harnesses.length > 1`; the option label stays
  `modelOptionLabel(m)`. On an existing thread the list is already scoped by the server.
- **Draft.** `openDraftThread` (2553) records `state.draftThread.harness = null`;
  `settleDraftModel` (2398) prefers the default model of the `project_harness` group, else its
  first entry, else the first entry overall, and records the chosen group on the draft.
  `sendSelectedModel` (10629) on a draft records the option's harness; `startDraftThread` sends
  `harness: state.draftThread.harness` only when set. `modelProviderLocked` (10554) gains a
  sibling check that an existing thread cannot select an option from another harness, with the
  notice `Create a new thread to use models from harness <name>.`.
- **MCP.** `loadMcpServers`, `reloadMcpServers`, and `startMcpOauthLogin` (8812 to 8858) use
  `/api/projects/${pid}/harnesses/${encodeURIComponent(name)}/mcp…` with the active harness, and
  skip with the menu hidden when there is none yet.
- **Sidebar.** `threadRow` (1420) sets `el.title` to the harness name when more than one
  declaration is known; nothing else changes visually.

### Tests

`crates/giskard-server/tests/ui.rs` pins that must change: line 1865
(`o.value = modelKey(state.currentModel);` stays valid only if `modelKey` keeps that call
shape; adjust the pin to the new call), 3706 (`state.harnessCapabilities = res.capabilities ||
null;` becomes the map assignment), and 1645/1649 (`/mcp/reload`, `/mcp/oauth-login` still
present as substrings). Add `browser_groups_the_picker_by_harness_only_with_a_choice` pinning
the `<optgroup>` emission under `state.harnesses.length > 1`, the `?harness=` scoped load in
`openThread`, and `harness: state.draftThread.harness` in the start request.

Playwright: `tests/e2e/tests/draft-composer.spec.ts` and `thread.spec.ts` keep passing with one
declaration; add to `draft-composer.spec.ts` an assertion that `#modelSel` has no `optgroup`
and that the start request's payload has no `harness` key, mirroring the new-project test.

## Work package 7: documentation

- **`docs/api-endpoints.md`.** The models paragraph (line 51): the `harness` query, the
  per-entry `harness`, the `harnesses` index with capabilities, `project_harness`, and the
  removed top-level `capabilities`. The threads paragraph (line 70): `harness` on the start
  request, response, thread summaries, and open response; the `400` for an undeclared name; the
  per-thread read-only rule. The MCP paths (line 24) under `/harnesses/{name}/`.
- **`README.md`.** Line 47 "chosen per project" becomes "chosen per thread from the model
  picker"; the storage-layout comment on `thread.json` (line 434) adds `harness declaration`; the
  picker paragraph (line 347) says the picker groups models by harness when several are declared
  and that a thread's harness is fixed at creation like its provider.
- **`docs/subagents.md`.** The ownership list (line 47) adds the harness declaration, inherited
  from the parent's instance.
- **`specs/giskard-specification.md`.** Bump to 1.98 with an amendment above the 1.97 one:
  per-thread harness binding, the constant default, per-instance bootstrap, the grouped picker,
  and the scoped MCP routes. §5.3 (line 2512): add `"harness"` to the `thread.json` sample with
  the skip-when-`codex` note. §7.1 (2799): the draft chooses a harness through the picker; the
  create step opens on that instance; sub-agents inherit. §8.3 (3122): catalogs are per instance;
  the models route composes all declarations for a draft and one for a thread. §13.6 (3841): the
  start request and response carry `harness`.
- **`docs/multi-harness-design.md`.** In *Status*, add `Stage 2 is implemented; see
  \`multi-harness-design/stage-2-plan.md\`.` Under *Model discovery and the picker*, replace
  "returns one group per declared harness" with the flat-list-plus-index shape and add the
  `?harness=` scoping. Under *Registry*, replace the `HarnessFactory::create` bullet with the
  `harness: &str` parameter. Under *Wire protocol and UI*, mark the thread fields and MCP routes
  done and leave `ProjectSummary` deferred.

## Verification

CI runs `rustfmt`, `clippy`, `test`, and `playwright`; green CI is the verification. Run locally
only what CI cannot show: start `giskard-server` against a `config.toml` with two Codex
declarations, open a draft, confirm two `<optgroup>`s and that picking a model under the second
group creates a thread whose `thread.json` carries `"harness": "<second>"`, and that opening that
thread does not spawn the first declaration's app-server.

Acceptance:

- A data directory from before this stage opens every thread on `codex` and writes no `harness`
  key back for them.
- With two declarations, a draft lists both groups, a thread created under the second is stamped
  with it, and opening that thread composes and spawns only its instance.
- Two threads of one project with the same native id on different declarations both bootstrap.
- A thread stamped with an undeclared name opens read-only naming it while its siblings open
  normally; starting a thread on an undeclared name is `400`.
- Project deletion and server shutdown stop every instance of a project.
- With one declaration the picker has no `optgroup`, the start payload has no `harness`, and no
  file under `docs/screenshots/` changes.
