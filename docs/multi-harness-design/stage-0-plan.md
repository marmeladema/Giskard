# Stage 0 implementation plan: no behaviour change

Implements Stage 0 of [`../multi-harness-design.md`](../multi-harness-design.md). This plan is
written for an implementing agent. Every file, symbol, line, and string below was verified against
the tree at commit `0b03b4e` (`main` after PR #272). Line numbers are for orientation and may
drift; the symbol or string quoted beside each is the thing to find.

Read `AGENTS.md` first. Its rules that bind this stage: `cargo fmt --all` and
`cargo clippy --all-targets -- -D warnings` must pass; `README.md`, `config.example.toml`,
`docs/api-endpoints.md`, and the spec must be updated in the same change as the code they
describe; no `unwrap`/`expect`/`panic!` on runtime paths; failures need tests; Markdown is wrapped
at 100 columns.

## Scope

Five work packages, in this order. Each leaves the tree green and is one commit.

1. Neutral wording in the server and the browser.
2. Harness capabilities on the wire and capability-gated browser controls.
3. A kind-dispatching harness factory.
4. Removal of the dead `[harness]` config section.
5. Documentation rewritten around harness instances.

## Non-goals

Nothing here may change persisted formats, the `[harnesses]` declarations, per-thread harness
binding, `ModelRef`, `LoadedThreadBinding`, or the `turn_steering` field on thread open and start
responses. Idle shutdown is not implemented; its config key is removed. No screenshot regeneration
is needed: every UI edit below is copy-only or leaves the replay server's rendering unchanged, and
`tests/e2e/screenshots/ide-theme.ts` opens neither the usage menu nor the pickers.

## Decisions that differ from the design document

Two points are decided here rather than as the design document phrased them, and the document is
amended in work package 5 to match.

- **Capabilities ride on the project models response, not the thread-open response.** The
  design says "serialized in full on the thread-open response". The controls that need gating are
  the draft composer's mode, permission, model, and effort pickers, and a draft has no thread to
  open. `GET /api/projects/{id}/models` is already loaded on every project open, draft, and
  thread open (`loadProjectModels` at `app.js:2547` and `:2636`) and already reaches the project's
  harness, so it is the one place that serves every case. With one harness per project the two
  are equivalent; Stage 2 groups this same response per harness, which is where per-harness
  capabilities belong. `turn_steering` on open and start responses is a per-attach fact and is
  left exactly as it is.
- **An unsupported Compact button is disabled, not hidden.** The replay harness reports
  `context_compaction: false` and `tests/e2e/tests/subagents.spec.ts:63` asserts
  `#compactBtn` is disabled on a sub-agent thread, which requires the element to exist. Spec
  §13.5 allows "hide or disable"; disabling with an explanatory title keeps the element.

## Work package 1: neutral wording

No behaviour change. User-visible text that names Codex in the neutral layers is reworded to name
the harness. Comments that document a Codex quirk the code accommodates are left alone.

### `crates/giskard-server/src/ws.rs`

`WsError::from_harness` (line 134). Replace the message strings only; codes are unchanged.

| Variant | Current message | New message |
|---|---|---|
| `Spawn` | `Codex CLI could not start.` | `The harness could not start.` |
| `NotInitialized` | `Codex is not ready for this request.` | `The harness is not ready.` |
| `Unauthenticated` | `Codex is not authenticated.` | `The harness is not authenticated.` |
| `Transport` | `Codex transport failed.` | `The harness transport failed.` |
| `Protocol` | `Codex protocol error.` | `Harness protocol error.` |
| `Overloaded` | `Codex is overloaded.` | `The harness is overloaded.` |
| `Timeout` | `Codex operation timed out.` | `Harness operation timed out.` |

`Unsupported`, `ThreadNotFound`, `ThreadBusy`, and `ThreadReadOnly` already read neutrally.

Timeout strings, each present once as a log message and once as the `Timeout` payload:

- line 947 and 957: `approval decision timed out waiting for Codex`
- line 996 and 1007: `server request response timed out waiting for Codex`
- line 1024 and 1028: `interrupt request timed out waiting for Codex`
- line 1090 and 1094: `context compaction request timed out waiting for Codex`
- line 1137 and 1140: `terminate command request timed out waiting for Codex`

Replace `waiting for Codex` with `waiting for the harness` in all ten.

Provider binding:

- line 1976 log: `rejecting provider change on provider-bound Codex thread` becomes
  `rejecting provider change on provider-bound thread`.
- line 2025 log: `rejecting persisted provider mismatch on provider-bound Codex thread` becomes
  `rejecting persisted provider mismatch on provider-bound thread`.
- line 2044, inside `provider_locked_error`: the first sentence
  `This Codex thread is bound to a different provider.` becomes
  `This thread is bound to a different provider.`; the second sentence is unchanged.

Leave the doc comment at line 1827 that names `codex_resume_failed`: that is the adapter's real
warning code.

### `crates/giskard-server/static/app.js`

User-visible strings:

| Line | Current | New |
|---|---|---|
| 1920 | `, and all corresponding Codex threads` | `, and all corresponding harness threads` |
| 1921 | ` and its corresponding Codex thread` | ` and its corresponding harness thread` |
| 4690 | `File list was not provided by Codex.` | `File list was not provided by the harness.` |
| 4918 | `Codex server request` | `Harness server request` |
| 5937 | `Ask Codex to stop this running command` | `Ask the harness to stop this running command` |
| 7139 | `Ask Codex to stop this running command` | `Ask the harness to stop this running command` |
| 8815 | `No MCP servers reported by Codex.` | `No MCP servers reported by the harness.` |
| 10710 | `Compact this thread's Codex context` | `Compact this thread's context` |

Comments at lines 328, 1658, 2547, 2636, 3142, 7172, 8684, and 10670 describe Codex behaviour
the browser accommodates. Leave them.

### `crates/giskard-server/static/index.html`

Line 347, the delete-thread comment: `the corresponding Codex threads managed by the harness`
becomes `the corresponding native threads managed by the harness`.

### Tests to update

- `crates/giskard-server/tests/ui.rs:259`: the pinned source string becomes
  `if (!kind.path) return "File list was not provided by the harness.";`. Reword the assertion
  message at line 260.
- `crates/giskard-server/tests/ui.rs:535`: `all corresponding Codex threads` becomes
  `all corresponding harness threads`.
- `tests/e2e/tests/approvals.spec.ts:20`: expected text becomes
  `File list was not provided by the harness.`.
- `tests/e2e/tests/subagents.spec.ts:113`: expected text becomes
  `all corresponding harness threads`.

The assertion messages at `ui.rs:400`, `:1662`, and `:1666` mention Codex only in the test's
own description. Leave them.

### Verification

```bash
grep -n "Codex" crates/giskard-server/src/ws.rs          # only the doc comment near line 1827
grep -n "Codex" crates/giskard-server/static/app.js       # only the comment lines listed above
grep -n "Codex" crates/giskard-server/static/index.html   # nothing
```

## Work package 2: capabilities on the wire, gated controls

### Wire type

`giskard-proto` depends only on `giskard-core`, and `HarnessCapabilities` lives in
`giskard-harness`. Add a wire mirror rather than a dependency.

In `crates/giskard-proto/src/lib.rs`, beside `McpCapabilitiesResponse` (line 709):

```rust
/// The capability flags of a project's harness (spec §4.2), mirrored for the browser so it can
/// gate its controls (§13.5). A mirror rather than the harness type itself: this crate holds wire
/// shapes and depends on `giskard-core` alone.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessCapabilitiesInfo {
    pub live_approvals: bool,
    pub plan_build_modes: bool,
    pub per_turn_model: bool,
    pub reasoning_effort: bool,
    pub structured_diffs: bool,
    pub resumable_threads: bool,
    pub model_listing: bool,
    pub provider_listing: bool,
    pub token_usage: bool,
    pub mcp_status: bool,
    pub mcp_reload: bool,
    pub mcp_oauth_login: bool,
    pub context_compaction: bool,
    pub turn_steering: bool,
}
```

The fourteen fields are exactly those of `giskard_harness::HarnessCapabilities`
(`crates/giskard-harness/src/lib.rs:27`).

Extend `ListModelsResponse` (line 702):

```rust
/// The project harness's capabilities, when it answered. Absent when the harness could not be
/// reached; `warnings` then carries a `harness:<kind>` entry saying why.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub capabilities: Option<HarnessCapabilitiesInfo>,
```

### Conversion

In `crates/giskard-server/src/routes.rs`, near `project_list_models` (line 3859), add:

```rust
fn capabilities_info(caps: HarnessCapabilities) -> HarnessCapabilitiesInfo {
    // Destructured exhaustively so a flag added to the harness type cannot be forgotten here.
    let HarnessCapabilities {
        live_approvals, plan_build_modes, per_turn_model, reasoning_effort, structured_diffs,
        resumable_threads, model_listing, provider_listing, token_usage, mcp_status, mcp_reload,
        mcp_oauth_login, context_compaction, turn_steering,
    } = caps;
    HarnessCapabilitiesInfo { /* same names */ }
}
```

In `project_list_models`, after `refresh_project_model_catalog`:

```rust
let capabilities = match state.registry.harness(&project_config).await {
    Ok(harness) => Some(capabilities_info(harness.capabilities())),
    Err(error) => {
        // The catalog refresh has already reported this harness in `warnings`; this is the
        // same get-or-create and fails for the same reason.
        debug!(project_id = %project_id, harness = %project_config.harness,
               action = "project_list_models", %error,
               "harness capabilities unavailable for the model list");
        None
    }
};
Ok(Json(ListModelsResponse { models, warnings, capabilities }))
```

`HarnessRegistry::harness` is get-or-create (`registry.rs:1427`); the refresh above has already
created the instance on the success path, so this is a lookup.

No other construction site of `ListModelsResponse` exists outside `routes.rs` and the proto crate.

### Browser

State (`app.js:177`, the `state` literal): add `harnessCapabilities:null`.

`prepareProjectModelCatalog` (`app.js:340`): add `state.harnessCapabilities = null;` beside
`state.models = [];`.

`loadProjectModels` (`app.js:363`), inside the `pid === state.projectId` branch that stores
`res.models`: add `state.harnessCapabilities = res.capabilities || null;`.

Add one helper next to `prepareProjectModelCatalog`:

```js
// A flag is gated only when the harness answered. Unknown capabilities (no harness yet, or one
// that could not be reached) leave every control as it is: the read-only paths already cover a
// thread that cannot attach, and a draft must not lose its pickers to a transient failure.
function harnessCan(flag) {
  const caps = state.harnessCapabilities;
  return !caps || caps[flag] !== false;
}
```

Gating, all inside `updateComposerControls` (`app.js:2965`) unless noted:

- **`plan_build_modes`.** Give the mode field a stable hook: in `index.html:253` the
  `<div class="mp-field">` wrapping `modeSel` gets `id="modeField"`. In
  `updateComposerControls`: `$("modeField").hidden = !harnessCan("plan_build_modes");`. On a draft
  with the flag false, force `setMode("build")` so the start request sends `build` (S7). Do not
  change the mode of an existing thread from the browser; the server already resolves it.
- **`reasoning_effort`.** In `syncEffortControl` (`app.js:10528`), after computing `efforts`:
  `if (!efforts.length || !harnessCan("reasoning_effort")) { control.hidden = true; return; }`.
- **`per_turn_model`.** Extend the three `disabled` expressions for `modelSel`,
  `modelPickerBtn`, and `effortSel` (`app.js:3013` to `:3015`) with
  `|| (!draft && !harnessCan("per_turn_model"))`. Set
  `$("modelPickerBtn").title` to `This harness fixes the model when a thread is created.` in that
  case, and back to `Model & reasoning effort for this thread` otherwise.
- **`context_compaction`.** In the `compactBtn` block (`app.js:3016`): add
  `|| !harnessCan("context_compaction")` to `disabled`, and set `compactBtn.title` to
  `This harness does not support context compaction.` when gated, else
  `Compact this thread's context`. Keep `textContent` logic unchanged; `ui.rs:471` pins it.
- **`live_approvals`.** The `ask_first` option of `permissionPresetSel` depends on live approval
  routing. Add `id="presetAskFirst"` to that `<option>` in `index.html:263` and, in
  `updateComposerControls`, set `$("presetAskFirst").disabled = !harnessCan("live_approvals")`
  with `title` `This harness cannot route approvals to the browser.` when disabled. Do not
  coerce a persisted preset; the server owns that.
- **`structured_diffs`.** Nothing to gate: there is no Diffs tab in this UI. Diff rows are
  rendered from items the harness emits, and a harness without structured diffs emits none.
- **`mcp_*`.** Already gated from the MCP endpoint's own `capabilities`. No change.
- **`turn_steering`.** Already gated from the thread open and start responses. No change.

`updateComposerControls` runs after every model load (`app.js:389`), so the gates apply as soon
as capabilities arrive.

### Tests

Integration, in `crates/giskard-server/tests/project_models.rs`, using the existing
`spawn_project` fixture and `giskard_testenv::fake` scripts:

- `project_models_carry_harness_capabilities`: a `Script` whose `capabilities()` returns
  `caps::RESUMABLE` (`crates/giskard-testenv/src/fake.rs:658`, every gated flag false). Assert
  `body["capabilities"]["plan_build_modes"] == false`,
  `["per_turn_model"] == false`, `["reasoning_effort"] == false`,
  `["context_compaction"] == false`, `["live_approvals"] == false`,
  `["resumable_threads"] == true`, and that all fourteen keys are present.
- `project_models_omit_capabilities_when_the_harness_cannot_start`: build the server with
  `giskard_testenv::factory::failing(HarnessError::Spawn("boom".into()))`. Assert
  `body.get("capabilities").is_none()` and that `warnings` contains an entry whose `source`
  starts with `harness:`.

Source pins, in `crates/giskard-server/tests/ui.rs`, one new test
`browser_gates_controls_on_harness_capabilities` asserting `app_js()` contains
`state.harnessCapabilities = res.capabilities || null;`, `function harnessCan(flag)`,
`harnessCan("plan_build_modes")`, `harnessCan("reasoning_effort")`,
`harnessCan("per_turn_model")`, `harnessCan("context_compaction")`, and
`harnessCan("live_approvals")`, and that the index markup, read with
`include_str!("../static/index.html")` as `ui.rs:3047` already does, contains `id="modeField"`
and `id="presetAskFirst"`.

Playwright, in `tests/e2e/tests/thread.spec.ts`: on the Demo project's primary thread, after
the first reply, `#compactBtn` is disabled and its `title` contains
`does not support context compaction`. The replay harness reports `context_compaction: false`
(`giskard-server-replay.rs:177`), and before this change the button was enabled and the click
failed with `harness_unsupported`; this pins the new state. Everything else the replay harness
reports as `true`, so no other e2e assertion changes.

### Documentation in this package

`docs/api-endpoints.md`, the `GET /api/projects/{id}/models` paragraph (line 38): add that the
response carries `capabilities`, the harness's capability flags, present only when the harness
answered, and that the browser gates its mode, permission, model, effort, and compaction controls
on it (spec §13.5).

## Work package 3: kind-dispatching factory

### New module `crates/giskard-server/src/harness_kinds.rs`

```rust
//! Harness kinds a binary can construct, keyed by the kind string a project names.

#[async_trait]
pub trait HarnessKind: Send + Sync {
    /// The kind string a `project.json` names in its `harness` field.
    fn name(&self) -> &str;
    async fn create(&self, config: &ProjectConfig, bootstrap: HarnessBootstrap)
        -> Result<Arc<dyn AgentHarness>, HarnessError>;
}

/// A `HarnessFactory` that dispatches on `ProjectConfig::harness`.
pub struct HarnessKindFactory {
    // Insertion order is the order kinds are listed in errors; `indexmap` is already a dependency.
    kinds: IndexMap<String, Arc<dyn HarnessKind>>,
}

#[derive(Debug, thiserror::Error)]
#[error("harness kind {0:?} is registered twice")]
pub struct DuplicateHarnessKind(pub String);

impl HarnessKindFactory {
    pub fn new() -> Self;
    pub fn register(self, kind: Arc<dyn HarnessKind>) -> Result<Self, DuplicateHarnessKind>;
    pub fn kinds(&self) -> impl Iterator<Item = &str>;
}
```

`impl HarnessFactory for HarnessKindFactory`: look up `config.harness`; on a hit, delegate; on a
miss, `warn!` with `project_id`, `harness`, `action = "create_harness"`, and return
`HarnessError::Unsupported(format!("unsupported harness kind {:?} for project {}; this server \
supports: {}", config.harness, config.id, kinds joined by ", "))`. Routes map `Unsupported` to
`400` with that message (`routes.rs:4274`), so the user sees which kind the project names and
which ones exist.

Export from `crates/giskard-server/src/lib.rs`: `pub mod harness_kinds;` and
`pub use harness_kinds::{DuplicateHarnessKind, HarnessKind, HarnessKindFactory};`.

### Binaries

`crates/giskard-server/src/bin/giskard-server.rs`: replace `CodexFactory` (line 15) with
`struct CodexKind;` implementing `HarnessKind` with `name() == "codex"` and the same body as
today's `create` minus the kind check. In `run`:

```rust
let factory = Arc::new(
    HarnessKindFactory::new()
        .register(Arc::new(CodexKind))
        .map_err(|error| error.to_string())?,
);
```

`crates/giskard-server/src/bin/giskard-server-replay.rs`: replace `ScriptedFactory`
(line 1043) with `struct ScriptedKind;` implementing `HarnessKind`. It registers under the name
`"codex"`, with this comment: `create_project` stamps every project with the kind `codex`
(`crates/giskard-persist/src/store.rs:959`), so the replay server's seeded projects name that
kind; Stage 1 replaces this with a declaration named `codex` of kind `replay`.

### Tests

Unit tests in `harness_kinds.rs`, with a stub `HarnessKind` whose `create` returns
`Err(HarnessError::Protocol("stub reached".into()))` so dispatch is observable without a harness:

- a project naming a registered kind reaches that kind's `create`;
- a project naming an unregistered kind gets `HarnessError::Unsupported` whose message contains
  the requested kind, the project id, and every registered name;
- registering the same name twice returns `DuplicateHarnessKind`.

The existing integration tests implement `HarnessFactory` directly and are unaffected.

## Work package 4: remove the `[harness]` config section

The section's `kind` is never read at runtime and `idle_shutdown_secs` is unimplemented
(`grep -rn idle_shutdown crates --include=*.rs` finds only `config.rs`). The top-level `Config`
carries `#[serde(default)]` and no `deny_unknown_fields`, so a `config.toml` still holding the
table keeps parsing after the type is gone.

### `crates/giskard-persist/src/config.rs`

- Remove `pub harness: HarnessConfig` from `Config` (line 24), `struct HarnessConfig`
  (line 263), and its `Default` impl (line 268).
- Tests: delete the `assert_eq!(config.harness.kind, "codex")` lines in `parse_full_config`
  (line 366), `default_config` (455), `empty_config_uses_defaults` (462), and
  `shipped_example_config_parses` (533). Remove the `[harness]` block from the `parse_full_config`
  TOML literal (line 348).
- Add a test `a_removed_harness_table_is_ignored`: parsing
  `"[harness]\nkind = \"codex\"\nidle_shutdown_secs = 0\n"` succeeds and yields defaults.

### Other code and config

- `crates/giskard-persist/src/lib.rs:15`: drop `HarnessConfig` from the re-export list.
- `crates/giskard-server/src/bin/giskard-server-replay.rs`, `write_config` (line 1081): delete
  the `[harness]` / `kind = "replay"` lines.
- `config.example.toml` lines 79 to 84: delete the `[harness]` block. Keep the
  `# ---- Providers ----` section that follows.
- `README.md` lines 263 and 264: delete the two `[harness]` rows of the configuration table.
- `specs/giskard-specification.md` Appendix C, lines 4390 to 4392: delete the `[harness]` block.

`crates/giskard-persist/src/store.rs:2688` asserts `config.harness == "codex"` on a
`ProjectConfig`, not on the app config. Leave it.

## Work package 5: documentation around instances

Prose only. Every edit below states what the code does after packages 1 to 4.

### `specs/giskard-specification.md`

Bump `**Version:** 1.95` to `1.96` and add, above the 1.95 amendment:

> **Amendment — harness instances (1.96).** A harness *instance* is one `AgentHarness` value per
> working context, created lazily and shut down as a unit; how many operating-system processes
> stand behind it is the adapter's business. Codex runs one app-server per instance hosting every
> thread; a per-thread-process adapter spawns in `open_thread` and stops per thread. The browser
> receives the instance's capability flags with the project model list and gates its controls on
> them (§13.5). The `[harness]` config section is removed. Neutral layers name "the harness", not
> Codex.

Replace §4.7 (line 2363) heading and first bullet:

`### 4.7 Process lifecycle (Codex)` becomes `### 4.7 Harness instances and processes`.

The first bullet becomes:

> - **One harness instance per working context.** An instance is one `AgentHarness` value:
>   created lazily on first use, bootstrapped with the thread bindings that belong to it, owning
>   one project event driver, and shut down as a unit. How many operating-system processes stand
>   behind it is the adapter's business. **Codex:** one `codex app-server` process per instance,
>   hosting every thread of that instance (Codex threads are durable containers within a
>   connection); a process exit ends every thread stream of the instance. **A per-thread-process
>   adapter** (the shape Claude Code takes): one process per primary thread, spawned in
>   `open_thread` and stopped on delete, archive, shutdown, or an idle policy; a process exit
>   ends that thread's stream only. Today a project has one instance. See §4.5 for the
>   object-safety constraint and `docs/multi-harness-design.md` for several per project.

The idle-shutdown bullet becomes:

> - **Idle shutdown:** not implemented. `docs/multi-harness-design.md` lists it as an open
>   question, as an instance policy an adapter may apply per process.

The crash-handling bullet becomes:

> - **Crash handling:** when a native process exits unexpectedly, the server handles the ended
>   stream of every thread that process hosted: it marks those threads "disconnected", surfaces
>   an `Error` event to the UI, and offers a "reconnect" action that respawns and resumes. For
>   Codex that is every thread of the instance; for a per-thread process it is that thread.

The remaining bullets (transport, lazy spawn, server shutdown, native identifier mapping,
resume-failure fallback, version check) are Codex-specific and stay; prefix the transport bullet
with `**Codex** transport:`.

§6.4 (line 2739): heading becomes `### 6.4 Harness instance management (per project)`, and the
first bullet becomes:

> - One harness instance per project, created lazily (§4.7) and reused across the project's
>   threads. For Codex that instance is one `codex app-server`, resumed after a crash.

§6.5's first bullet `their harness processes run concurrently` becomes `their harness instances
run concurrently`.

### `README.md`

- *Supported harnesses* (line 38): after the Codex bullet, add one sentence: `Giskard manages a
  harness *instance* per project; whether that is one process or one per thread is the adapter's
  concern.`
- Line 60: `Giskard spawns one \`codex app-server\` process per project;` becomes
  `Giskard runs one harness instance per project, which for Codex is one \`codex app-server\`
  process;`.
- Line 414, the storage layout comment on `project.json`: `workspace root, harness kind` stays
  accurate; no change.
- Configuration table: the two rows removed in package 4.

### `crates/giskard-harness-codex/README.md`

- Line 14: `Each project app-server process has exactly one` becomes `Each harness instance runs
  one app-server process, which has exactly one`.
- Line 134: `when its Codex app-server process is respawned` is accurate; no change.
- Add one sentence to *Runtime ownership*: `An instance is one \`CodexHarness\`; the server
  creates one per project today and treats the process count behind it as this adapter's
  concern.`

### `docs/multi-harness-design.md`

- In *Wire protocol and UI*, the bullet beginning `\`HarnessCapabilities\` is serialized in full
  on the thread-open response` becomes: `\`HarnessCapabilities\` is serialized in full on the
  project models response, which the draft and every thread open already load, and the UI gates
  Plan/Build, approvals, effort, model, and compaction on it as spec §13.5 describes. Per-harness
  groups in Stage 2 carry it per group.`
- In *Status*, add: `Stage 0 is implemented; see \`multi-harness-design/stage-0-plan.md\`.`

### `docs/api-endpoints.md`

Updated in package 2. No further change.

## Verification

Run after every package and before the PR:

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
cargo build -p giskard-server --bin giskard-server-replay
GISKARD_E2E_PREBUILT_BIN=target/debug/giskard-server-replay tests/e2e/run.sh
awk 'length > 100 {print FILENAME": "FNR}' README.md docs/*.md docs/multi-harness-design/*.md \
    crates/giskard-harness-codex/README.md
```

Acceptance:

- The three `grep` checks under package 1 return only the listed comment lines.
- `GET /api/projects/{id}/models` returns `capabilities` for a project whose harness answers and
  omits it when the harness cannot start; the two integration tests pass.
- With the replay server, a primary thread's Compact button is disabled with the unsupported
  title; every other e2e test passes unchanged apart from the two reworded expectations.
- A `config.toml` containing `[harness]` still starts the server.
- `giskard-server` refuses a project whose `project.json` names an unknown kind with a `400`
  naming the kind and listing `codex`.
- No file under `docs/screenshots/` changes.
