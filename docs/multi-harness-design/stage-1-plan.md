# Stage 1 implementation plan: named declarations, one harness per project

Implements Stage 1 of [`../multi-harness-design.md`](../multi-harness-design.md), on top of the
merged Stage 0 (`stage-0-plan.md`). This plan is written for an implementing agent. Every file,
symbol, and string below was verified against `main` at `eab8847`. Line numbers are for
orientation; the symbol or string quoted beside each is the thing to find.

Read `AGENTS.md` first. Its rules that bind this stage: `cargo fmt --all` and
`cargo clippy --all-targets -- -D warnings` must pass; `README.md`, `config.example.toml`,
`docs/api-endpoints.md`, and the spec are updated in the same change as the code they describe; all
Codex-specific types stay in `giskard-harness-codex`; no `unwrap`/`expect`/`panic!` on runtime
paths; every failure mode gets a test; Markdown is wrapped at 100 columns.

## Outcome

After this stage a user can write

```toml
[harnesses.codex-stable]
kind = "codex"
default = true

[harnesses.codex-nightly]
kind = "codex"
command = "/opt/codex-nightly/bin/codex"
profile = "nightly"
[harnesses.codex-nightly.env]
CODEX_HOME = "/home/you/.codex-nightly"
```

and create a project on `codex-nightly` from the new-project modal. Every thread of that project
runs on that instance. A config with no `[harnesses]` table behaves exactly as today. A project
that names a declaration the config no longer has opens its threads read-only with the config key
in the error, and refuses to start new ones with a `400` naming it.

## Scope

Six work packages, in this order. Each leaves the tree green and is one commit.

1. Declarations in `giskard-persist`: parsing, the `default` rule, the synthesized `codex`, and
   validation that needs no adapter.
2. The environment overlay in `giskard-harness` and its use by discovery.
3. Codex adapter launch options: `command`, `args`, `env`, `profile`.
4. The factory resolves a project's declaration, validates every declaration at boot, and exposes
   the catalog.
5. Choosing a harness at project creation: request field, `GET /api/harnesses`, the modal select.
6. Documentation.

## Non-goals

No per-thread harness field, no per-name slots on the project authority, no grouped model
composition, no harness-scoped MCP routes, no `ProjectSummary.harness` (the project list is built
from `projects.json`, which has no harness column; the detail endpoint already returns the whole
`project.json`), no `idle_shutdown_secs`, no per-declaration provider overlay, and no change to
`HarnessCapabilities`. No screenshot regeneration: the only visible UI change is a select that is
hidden when one declaration exists, and the replay server declares exactly one.

## Decisions that differ from the design document

Amended in work package 6 so the two documents agree.

- **`ProjectConfig.harness` stays a required `String`, stamped at creation.** The design says the
  field "becomes optional, with a missing value meaning the declaration marked `default`". Every
  existing `project.json` already carries `codex`, and project creation is the one moment the
  default matters, so stamping the resolved name at creation gives the same behaviour with no
  `Option` plumbing through the registry and factory. It also matches how Stage 2 stamps the
  thread field. The field remains a creation-time input: nothing reads it as a default after
  creation.
- **`HarnessFactory::create` keeps its signature.** The design has it take a declaration. The
  registry does not know the catalog, and Stage 2's per-thread resolution can change this call
  when it needs to. Instead `HarnessKindFactory` owns the catalog, resolves `config.harness` to a
  declaration, and hands `HarnessKind::create` a `HarnessInstanceSpec` carrying it. Nothing outside
  the factory reads a declaration.
- **`idle_shutdown_secs` is not added.** Stage 0 removed it; the design lists idle shutdown as an
  open question. The example config in the design still shows the key and is corrected.
- **Declarations are read once at startup.** `PersistStore::load_config` caches `config.toml` for
  the process lifetime (`store.rs:585`), so this is already the rule for every section; the
  catalog is built from that one load and validated before the server listens.

## Work package 1: declarations in `giskard-persist`

### Types, in `crates/giskard-persist/src/config.rs`

Add to `Config` (line 9), after `providers`:

```rust
/// Declared harnesses, keyed by name (design: *Configuration*). An `IndexMap` because
/// declaration order decides the default when none is marked. Empty means "not declared", which
/// `HarnessCatalog::resolve` turns into the synthesized `codex` entry.
pub harnesses: IndexMap<String, HarnessDeclaration>,
```

New types in the same file:

```rust
/// One `[harnesses.<name>]` entry. The neutral keys are parsed here; everything else is kept as
/// an opaque table for the kind's adapter to type-check at boot, because Codex-specific types
/// stay in the adapter crate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarnessDeclaration {
    pub kind: String,
    #[serde(default)]
    pub default: bool,
    /// Program to spawn. `None` leaves it to the adapter (Codex: `codex` on `PATH`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Extra arguments appended after the adapter's own.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Environment overlay for every process this instance spawns.
    #[serde(default, skip_serializing_if = "HarnessEnv::is_empty")]
    pub env: HarnessEnv,
    /// Kind-specific keys, validated by the adapter.
    #[serde(default, flatten)]
    pub options: toml::Table,
}
```

`deny_unknown_fields` cannot be combined with `flatten`, which is why unknown-key rejection for
the neutral part is done in `validate` below and for the kind part by the adapter.

`HarnessEnv` is a newtype over `IndexMap<String, String>` with `is_empty`, `iter`, `get`, and a
custom `Debug` that prints names only, on the `ProviderHttpHeaders` pattern
(`crates/giskard-harness/src/lib.rs:106`). Its `Deserialize` is the map's.

```rust
/// The declarations after the rules are applied: every name is declared, exactly one is the
/// default, and an empty table has become the synthesized `codex`.
#[derive(Debug, Clone)]
pub struct HarnessCatalog {
    declarations: IndexMap<String, HarnessDeclaration>,
    default: String,
}

impl HarnessCatalog {
    pub const SYNTHESIZED_NAME: &str = "codex";
    pub fn resolve(config: &Config) -> Result<Self, HarnessConfigError>;
    /// The catalog an empty table resolves to: one `codex` of kind `codex`.
    pub fn synthesized() -> Self;
    pub fn get(&self, name: &str) -> Option<&HarnessDeclaration>;
    pub fn default_name(&self) -> &str;
    pub fn names(&self) -> impl Iterator<Item = &str>;
    pub fn iter(&self) -> impl Iterator<Item = (&str, &HarnessDeclaration)>;
}
```

`resolve` rules, each a `HarnessConfigError` variant whose `Display` names the
`[harnesses.<name>]` key:

- Empty table: synthesize `codex` of kind `codex`, no command, no args, empty env, empty options,
  `default = true`.
- More than one `default = true`: `MultipleDefaults(Vec<String>)` naming every marked entry.
- None marked: the first declared entry is the default.
- `kind` blank: `BlankKind(name)`.
- `command` present but blank: `BlankCommand(name)`.
- Every `env` name non-empty and containing neither `=` nor NUL; every value free of NUL:
  `InvalidEnvName { declaration, name }` and `InvalidEnvValue { declaration, name }`. Never
  include the value in the message.
- `options` containing a key named `default`, `command`, `args`, `env`, or `kind` cannot happen
  (serde takes those first); no rule needed.

Export from `crates/giskard-persist/src/lib.rs:14`: `HarnessCatalog`, `HarnessConfigError`,
`HarnessDeclaration`, `HarnessEnv`.

### Tests, in the `config.rs` test module

- `no_harnesses_table_synthesizes_codex`: `toml::from_str::<Config>("")`, resolve, one entry
  named `codex` of kind `codex`, default `codex`.
- `declared_table_is_exactly_what_it_declares`: two entries, neither marked; the first is the
  default; no `codex` entry exists.
- `a_marked_default_wins_over_order`: second entry marked; `default_name()` is the second.
- `two_defaults_are_an_error`: both marked; the error names both keys.
- `kind_specific_keys_are_kept_opaque`: `[harnesses.x]\nkind = "codex"\nprofile = "p"\n` parses
  and `options["profile"] == "p"`.
- `env_names_are_validated`: a name with `=` and a name that is empty both fail, and the error
  names the declaration and the variable, never the value.
- `env_debug_redacts_values`: `format!("{:?}", env)` contains the name and not the value.
- `shipped_example_config_parses` (existing, line ~520): extend with
  `HarnessCatalog::resolve(&config)` succeeding and naming the example's default.

## Work package 2: the environment overlay in discovery

Discovery is Giskard's own HTTP request, and today it resolves env-backed keys, env-backed
headers, and auth commands against Giskard's process environment
(`crates/giskard-harness/src/lib.rs:205` and `:248`; `crates/giskard-server/src/models.rs:1082`).
With a per-declaration overlay that is wrong: a key present only in the overlay is not found, and
a key from Giskard's shell is sent for a home that uses a different one.

### `crates/giskard-harness/src/lib.rs`

Add:

```rust
/// A harness instance's environment overlay: the declaration's variables over Giskard's own.
/// Cheap to clone; every `HarnessProvider` an instance reports carries one so discovery resolves
/// names the way the instance's processes see them.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct EnvOverlay(Arc<Vec<(String, String)>>);

impl EnvOverlay {
    pub fn new(entries: impl IntoIterator<Item = (String, String)>) -> Self;
    /// The overlay's value first, then the process environment.
    pub fn var(&self, name: &str) -> Option<String>;
    pub fn entries(&self) -> &[(String, String)];
}
```

`Debug` prints names only. `HarnessProvider` gains `pub env: EnvOverlay`, defaulting to the
empty overlay; every construction site updates (`giskard-harness-codex/src/lib.rs:2356` and
`:2370`, `giskard-harness-replay`'s `with_providers` callers, and the test constructors in
`giskard-harness/src/lib.rs` and `giskard-server/tests`).

- `resolve_api_key` (line 201): `ProviderAuth::Env(var)` reads `self.env.var(var)` instead of
  `std::env::var(var)`.
- `run_auth_command` (line 248): takes `&EnvOverlay` and applies `command.envs(overlay.entries())`
  before spawning. The overlay is applied, not the whole environment replaced, so an auth helper
  still inherits `PATH` and `HOME`.

### `crates/giskard-server/src/models.rs`

In the header builder (line 1058 to 1090), replace `std::env::var(variable)` with
`provider.env.var(variable)`. The function already receives the provider.

### Tests

- `giskard-harness` unit tests: an overlay value wins over an unset variable; an overlay value
  wins over a set one (use `GISKARD_TEST_DISCOVERY_KEY`, supplied by `.cargo/config.toml:21`,
  with an overlay of the same name and a different value); the auth command sees the overlay
  (`sh -c 'printf %s "$GISKARD_TEST_OVERLAY_TOKEN"'` with the overlay providing it).
- `crates/giskard-server/tests/model_refresh.rs`: one test on the `DiffHarnessConfig` pattern
  (line 19) where the provider's `auth` is `ProviderAuth::Env("GISKARD_TEST_OVERLAY_KEY")`, a
  name absent from `.cargo/config.toml`, and the provider's `env` overlay supplies it; the mock
  `/models` route asserts the `Authorization: Bearer` header carries the overlay's value.

## Work package 3: Codex launch options

### `crates/giskard-harness-codex/src/lib.rs`

```rust
/// How to launch this instance's app-server (design: *Configuration*).
#[derive(Debug, Clone, Default)]
pub struct CodexLaunchOptions {
    /// Binary path or name. `None` is `codex` on `PATH`, the SDK's default.
    pub command: Option<PathBuf>,
    /// Appended after `app-server --listen stdio://` (`AppServerBuilder::extra_args`).
    pub args: Vec<String>,
    /// Applied on the child over the inherited environment (`AppServerBuilder::envs`).
    pub env: EnvOverlay,
    /// `-c profile=<name>` (`AppServerBuilder::config_override`).
    pub profile: Option<String>,
}

/// The kind-specific keys of a `[harnesses.<name>]` declaration of kind `codex`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexDeclarationOptions {
    #[serde(default)]
    pub profile: Option<String>,
}
```

`CodexDeclarationOptions` is what the factory deserializes the opaque `options` table into; the
`deny_unknown_fields` is what turns a typo into a boot error. A blank `profile` is an error too:
`CodexDeclarationOptions::validate(&self) -> Result<(), String>`.

Add `CodexHarness::launch(workspace_root, options: CodexLaunchOptions, bootstrap)`; the existing
`start_with_bootstrap` becomes `launch(root, CodexLaunchOptions::default(), bootstrap)` and
`start_with` is removed: nothing calls it. The builder is assembled by one function so it can be
unit-tested:

```rust
fn app_server_builder(options: &CodexLaunchOptions) -> codex_codes::AppServerBuilder
```

applying, in this order: `command` when set, `envs(options.env.entries())`,
`config_override("profile", p)` when set, `extra_args(options.args)`. The SDK (`codex-codes`
0.155.1, `cli.rs:20`) places `-c` overrides before the `app-server` subcommand and extra args
after `--listen stdio://`, and applies `envs` on top of the inherited environment, so the
precedence stated in the design holds: command, then environment, then the profile override and
args. The instance keeps its `EnvOverlay` and sets it on every `HarnessProvider` it reports from
`handle_list_providers` (line 2340).

Log at `info` on spawn, `action = "start_codex_client"`: the command, the profile, the count of
extra args, and the overlay's variable names. Never the values.

### Tests

- `app_server_builder_maps_every_option`: `AppServerBuilder` is `Debug` and its fields are
  private, so assert through `build_command_sync()` on a `CodexLaunchOptions` with
  `command = "/bin/true"`: the `std::process::Command`'s `get_program()` is `/bin/true`,
  `get_args()` is `-c profile=nightly app-server --listen stdio:// --foo`, and `get_envs()`
  contains `("CODEX_HOME", "/tmp/x")`. `/bin/true` exists on the CI runners and is absolute, so
  `resolve_command` does not consult `PATH`.
- `blank_profile_is_rejected` and `unknown_codex_option_is_rejected` on
  `CodexDeclarationOptions`.
- `listed_providers_carry_the_instance_overlay`: through the existing fake transport
  (`spawn_fake_harness_with_bootstrap`, line ~3259), a harness launched with an overlay reports
  providers whose `env` is that overlay.

### `crates/giskard-harness-codex/README.md`

New section `## Launch options` after *Runtime ownership*: the four options, the SDK calls they
map to, the precedence, that `model/list` describes the instance's startup provider only, and
that a profile's `model_provider` therefore changes what that catalog contains.

## Work package 4: the factory resolves declarations

### `crates/giskard-server/src/harness_kinds.rs`

```rust
/// What a kind needs to construct one instance.
pub struct HarnessInstanceSpec<'a> {
    pub project_id: ProjectId,
    pub workspace_root: PathBuf,
    pub name: &'a str,
    pub declaration: &'a HarnessDeclaration,
}

#[async_trait]
pub trait HarnessKind: Send + Sync {
    fn name(&self) -> &str;
    /// Type-check a declaration's kind-specific `options` at boot. The error is shown to the
    /// operator with the `[harnesses.<name>]` key prepended by the caller.
    fn validate(&self, declaration: &HarnessDeclaration) -> Result<(), String>;
    async fn create(&self, spec: HarnessInstanceSpec<'_>, bootstrap: HarnessBootstrap)
        -> Result<Arc<dyn AgentHarness>, HarnessError>;
}
```

`HarnessKindFactory` gains `catalog: HarnessCatalog`, set by `with_catalog(catalog)`, and:

- `validate(&self) -> Result<(), HarnessValidationError>`: for every declaration, the kind must
  be registered (`UnknownKind { declaration, kind, supported }`) and `kind.validate` must pass
  (`InvalidOptions { declaration, message }`). `Display` starts with `[harnesses.<name>]`.
- `catalog(&self) -> &HarnessCatalog`.
- `HarnessFactory::create`: look `config.harness` up in the catalog. Missing:
  `HarnessError::Unsupported(format!("project {} names harness {:?}, which config.toml does not \
  declare under [harnesses]; declared: {}", ...))`, logged at `warn` once per name per process
  (a `Mutex<HashSet<String>>` of names already reported; Stage 0's review noted the repeated
  warn). Found: look the kind up, build the spec with `workspace_root =
  config.workspace_root.as_deref().unwrap_or(&config.dir)`, and delegate. A declared kind that is
  not registered cannot happen after `validate`, but is handled with the same `Unsupported` path
  rather than a panic.

`HarnessFactory` (registry.rs:74) gains a defaulted method so the many test factories are
untouched:

```rust
/// The declarations this factory can construct. The default is the synthesized single `codex`,
/// which is what every test factory constructs.
fn catalog(&self) -> HarnessCatalog { HarnessCatalog::synthesized() }
```

`HarnessRegistry` gains `pub fn harness_catalog(&self) -> HarnessCatalog` delegating to its
factory (the `factory` field is at registry.rs:298).

### Binaries

`giskard-server.rs`: `CodexKind::validate` deserializes `declaration.options.clone()` into
`CodexDeclarationOptions` via `toml::Value::Table(...).try_into()` and calls its `validate`.
`CodexKind::create` builds `CodexLaunchOptions` from the spec: `command` from
`declaration.command`, `args`, `env: EnvOverlay::new(declaration.env.iter())`, `profile` from
the typed options, then `CodexHarness::launch`. In `run` (line 287): resolve the catalog with
`HarnessCatalog::resolve(&startup.config)`, build the factory with it, and call
`factory.validate()`; either error is returned as the startup `String` so `main` prints it and
exits `1`, the same path `load_required_config` uses. Validation runs before the listener binds.

`giskard-server-replay.rs`: `ScriptedKind::name` becomes `"replay"` and `validate` accepts an
empty options table only. `write_config` (line 1077) adds

```toml
[harnesses.codex]
kind = "replay"
```

so the seeded `Demo` project, stamped `codex` by `create_project`, resolves to the scripted kind
under the reserved name, exactly as the design states. The Stage 0 comment on `ScriptedKind`
saying it registers as `codex` is replaced by one saying the declaration does.

### Tests

Unit tests in `harness_kinds.rs`, extending the existing stub-kind tests:

- a project whose `harness` is a declared name reaches that declaration's kind with the
  declaration in the spec;
- a project naming an undeclared name gets `Unsupported` whose message contains the name, the
  project id, `[harnesses]`, and every declared name;
- `validate` fails for a declared kind no binary registered, naming the key;
- `validate` fails when a kind rejects its options, with the key prepended;
- the undeclared-name warning is logged once for two creates of the same name (use
  `CapturedLogWriter` from `crates/giskard-server/src/test_logs.rs` as a tracing writer).

Integration, `crates/giskard-server/tests/read_only_thread.rs`: a server whose factory catalog
declares only `other`, and a persisted project stamped `codex`. Opening its thread returns the
read-only warning whose `detail` contains `[harnesses]` and `codex`; starting a thread returns
`400` with the same message. The registry's attach failure already takes the degraded path at
`routes.rs:706`, and `harness_api_error` maps `Unsupported` to `400` at `routes.rs:4343`; this
test pins that both name the config key.

## Work package 5: choosing a harness at project creation

### Wire

`crates/giskard-proto/src/lib.rs:553`: `CreateProjectRequest` gains
`#[serde(default)] pub harness: Option<String>`. New response type beside `ProjectSummary`:

```rust
#[derive(Debug, Clone, Serialize)]
pub struct HarnessDeclarationSummary {
    pub name: String,
    pub kind: String,
    pub default: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListHarnessesResponse {
    pub harnesses: Vec<HarnessDeclarationSummary>,
}
```

### Routes, `crates/giskard-server/src/routes.rs`

- `GET /api/harnesses` (protected, registered beside `/api/projects` at line 81):
  `state.registry.harness_catalog()` in declaration order, `default` on the one default.
- `create_project` (line 420): resolve the name as `req.harness` or the catalog's default; an
  unknown name is `ApiError::BadRequest(format!("unknown harness {:?}; declared: {}", ...))`.
  `PersistStore::create_project` (store.rs:935) gains a `harness: &str` parameter and stamps it
  instead of the literal `codex`. The replay binary's seeding call (line 1188) and
  `create_project_via_api` in `giskard-testenv/src/server.rs:91` pass the catalog default, which
  is `codex` for both.

`ProjectConfig.harness` (store.rs:57) gets the doc comment the design asks for: the declaration
name, stamped at creation, a creation-time input that nothing reads as a default afterwards.

### Browser

`index.html:311`, after the project-name field:

```html
<div class="field" id="pmHarnessField" hidden>
  <label for="pmHarness">Harness</label>
  <select id="pmHarness"></select>
  <div class="hint">Which declared harness this project's threads run on.</div>
</div>
```

`app.js`, `openProjectModal` (line 2165): fetch `/api/harnesses`, fill `#pmHarness` with one
option per declaration labelled `name (kind)`, select the default, and unhide `#pmHarnessField`
only when more than one declaration exists. A fetch failure leaves the field hidden and sends no
`harness`, so creation falls back to the server default rather than blocking the modal; report it
through `pmErr` as `Harness list unavailable: …` without disabling Create. `pmCreate.onclick`
(line 2280) sends `harness` only when the field is visible.

### Tests

- `crates/giskard-server/tests/security.rs`, beside `create_project_is_confined_to_browse_roots`
  (line 265): `create_project_stamps_the_requested_or_default_harness` (a factory with a two-entry
  catalog; omit the field and read back `codex-stable`; send the second name and read it back via
  `GET /api/projects/{id}`) and `create_project_rejects_an_undeclared_harness` (`400`, message
  names the declared list). The catalog comes from `HarnessKindFactory::with_catalog` over stub
  kinds, so build the factory directly rather than through `giskard_testenv::factory`.
- `GET /api/harnesses` returns the declarations in order with the default marked; unauthenticated
  is `401` like the other protected routes (add the path to the loop at `security.rs:45`).
- `crates/giskard-server/tests/ui.rs`: the source-pin test at line 1712 must keep passing;
  extend it to assert `create_project` contains `harness` only under the visibility condition
  (`pmHarnessField`), and that `index.html` contains `id="pmHarness"`.
- `tests/e2e/tests/new-project.spec.ts:42`: beside the `#pmModel` count assertion, assert
  `#pmHarnessField` is hidden and the POST payload has no `harness` key. The replay server
  declares one harness.

### Documentation in this package

`docs/api-endpoints.md`: add `GET /api/harnesses` to the highlights list (line 4) and a paragraph
after the `POST /api/projects` one (line 34): the optional `harness` field, the default when
omitted, the `400` for an unknown name, and what `GET /api/harnesses` returns.

## Work package 6: documentation

- **`config.example.toml`**: a `# ---- Harnesses ----` section before `# ---- Providers ----`
  with the two-Codex example from *Outcome*, commented out apart from a note that the whole
  section is optional and that omitting it means one harness named `codex` of kind `codex`. State
  the rules: the name is durable and persisted on projects; `default = true` on at most one; the
  first declared is the default otherwise; declaring the table is the off switch for the
  synthesized `codex`, and pre-declaration projects still need an entry named `codex` to open;
  `env` is literal and never logged; the Codex-only key is `profile`.
- **`README.md`**: configuration table (line 250) gains rows for `[harnesses.<name>]` with
  `kind`, `default`, `command`, `args`, `env`, and `profile` (Codex only); *Supported harnesses*
  (line 38) gains one sentence that several Codex declarations can run side by side, chosen per
  project; the storage-layout comment at line 413 becomes `workspace root, harness declaration
  name`; a short paragraph under *Configuration* states the durable-name rule and the `codex`
  reservation.
- **`specs/giskard-specification.md`**: bump to 1.97 with an amendment paragraph above the 1.96
  one summarising declarations, the default rule, the synthesized `codex`, the environment
  overlay reaching discovery, and the project-creation choice. Appendix C (line 4385, before
  `[providers.openai]`) gains the `[harnesses.<name>]` block with a comment on each rule. §6.1
  (line 2721): the flow "names it, picks a directory, optionally sets workspace root" gains "picks
  a declared harness when more than one is declared, the default otherwise". §8.2 gains one
  sentence: env-backed keys, headers, and auth commands resolve through the instance's declared
  environment overlay before Giskard's own.
- **`docs/multi-harness-design.md`**: in *Status*, add `Stage 1 is implemented; see
  \`multi-harness-design/stage-1-plan.md\`.` Under *Configuration*, drop `idle_shutdown_secs = 0`
  from the example and remove it from the neutral-keys bullet. Under *Persistence*, replace "It
  becomes optional, with a missing value meaning the declaration marked `default`; existing files
  carry `codex`, which resolves unchanged." with "It stays a required string, stamped at creation
  with the chosen or default declaration name; existing files carry `codex`, which resolves
  unchanged." Under *Registry*, replace the `HarnessFactory::create` bullet with the
  `HarnessInstanceSpec` on `HarnessKind::create` as built here. Under *Wire protocol and UI*,
  mark the `CreateProjectRequest` sentence as done and leave `ProjectSummary` for Stage 2.
- **`crates/giskard-harness-codex/README.md`**: written in work package 3.

## Verification

CI runs `rustfmt`, `clippy`, `test`, and `playwright`; green CI is the verification. Run locally
only what CI does not show: a manual start of `giskard-server` against a `config.toml` with a
deliberate `[harnesses.x] kind = "nope"` to see the boot error name the key, and with
`profile = ""` to see the adapter's message.

Acceptance:

- A `config.toml` without `[harnesses]` starts, lists one harness named `codex`, and creates
  projects stamped `codex`, exactly as before.
- A declaration with an unknown kind, two defaults, a bad env name, or an unknown Codex key
  refuses startup with a message naming `[harnesses.<name>]`.
- A project stamped with an undeclared name opens read-only with `[harnesses]` and the name in
  the warning detail, and `POST …/threads/start` on it is `400`.
- `GET /api/harnesses` lists declarations in order with one default; the modal shows the select
  only when more than one exists; the e2e suite passes with the replay server's single
  declaration.
- Discovery for a provider whose key is named only in a declaration's `env` succeeds, and the key
  is absent from every log line and `Debug` output.
- No file under `docs/screenshots/` changes.
