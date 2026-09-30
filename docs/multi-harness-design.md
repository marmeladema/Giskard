# Multiple harnesses: design

## Status

Design, not yet implemented. This document records the audit of where Giskard currently assumes a
single Codex harness, and the design for declaring several harnesses in `config.toml`, running more
than one per project, and binding each thread to one of them. It is written to be executed in the
stages listed at the end; each stage is independently shippable.

Stage 0 is implemented; see `multi-harness-design/stage-0-plan.md`.

Stage 1 is implemented; see `multi-harness-design/stage-1-plan.md`.

Stage 2 is implemented; see `multi-harness-design/stage-2-plan.md`.

The spec (`specs/giskard-specification.md`) remains authoritative for the harness contract. Where
this document proposes changes to the spec, they are called out explicitly under *Spec and
documentation changes*.

## Goals

- **Several harness kinds.** Codex today; Claude Code next. Adding a kind must not touch
  persistence, the UI, or the core domain model beyond what the spec's hard constraints already
  allow (§1.2).
- **Several harnesses of one kind.** Two Codex declarations side by side, for example a stable CLI
  and a nightly one, with separate binaries and separate Codex homes.
- **Per-thread harness choice.** Different threads of one project may run on different harnesses.
  The project is not the unit of harness binding; the thread is.
- **Instance, not process.** A harness *instance* is the unit Giskard manages. How many operating
  system processes stand behind it is the adapter's business: one app-server hosting every thread
  for Codex, one process per primary thread for Claude Code.

## Vocabulary

- **Harness kind.** An adapter implementation: `codex`, `claude-code`, `replay`. The set of kinds is
  fixed by the binary's factory.
- **Harness declaration.** A named, user-authored entry in `config.toml` with a kind and
  kind-specific options. The name is a durable identifier, like a provider id.
- **Harness instance.** One `Arc<dyn AgentHarness>`, created lazily per project and declaration,
  bootstrapped with the thread bindings that belong to it, owning one project event driver, and
  shut down as a unit.
- **Native process.** Whatever the adapter spawns. Never named outside the adapter.

## Audit: where the code assumes one harness

The trait in `crates/giskard-harness/src/lib.rs` is already neutral. Its doc comment states that
one value implements it per working context, that a project may later hold several, and that the
process count behind an instance is the adapter's business. The three instance contracts it lists
(`subscribe` works before any native session exists, request ids are unique within the instance,
a stream ends per thread) were written for adapters with several processes. Nothing in the trait
needs to change for this design.

The single-harness assumption lives in the layers above it.

### Config and factory

- `[harness] kind` and `idle_shutdown_secs` in `crates/giskard-persist/src/config.rs` are parsed and
  asserted on in tests but never read at runtime. `idle_shutdown_secs` is unimplemented.
- `PersistStore::create_project` in `crates/giskard-persist/src/store.rs` hard-codes the project's
  harness string to `codex`. Neither the HTTP request nor the UI offers a choice.
- `HarnessFactory::create` in `crates/giskard-server/src/registry.rs` takes a `&ProjectConfig`, and
  the production `CodexFactory` in `crates/giskard-server/src/bin/giskard-server.rs` rejects any
  value but `codex`. The replay binary writes `kind = "replay"` into its config, which shows that
  the kind string is meaningful only to the binary that owns the factory.
- `CodexHarness::start_with_bootstrap` takes only a workspace root and the bootstrap. The binary,
  Codex home, and extra arguments cannot be configured; `start_with` accepts a binary path but is
  not reachable from production.

### Registry

- `ProjectAuthority` in `crates/giskard-server/src/registry/project.rs` holds exactly one
  `ProjectHarnessSlot` and one `ProjectModelCatalogSlot`.
- `HarnessRegistry::harness(config)` is get-or-create per project. Every route and WebSocket action
  reaches the harness through it.
- `known_thread_bindings` feeds every thread of the project into the one bootstrap and treats a
  repeated native id as corruption. Two harnesses have disjoint native id spaces; each must receive
  only its own threads, and uniqueness must be checked per instance.
- One `ProjectEventDriver` is spawned per harness, which already matches "one driver per instance"
  and needs no change beyond being keyed by declaration.

### Thread identity

- `ThreadFile` carries `harness_thread_id` but nothing that says which harness that id belongs to.
  A native id is meaningful only relative to an instance.
- Discovery admission (`crates/giskard-server/src/registry/admission.rs`) and sub-agent linking bind
  native ids per project driver. That is correct once a driver is per instance: a discovered or
  linked child inherits the instance that produced it.

### Project-scoped surfaces that are really instance-scoped

- `GET /api/projects/{id}/models` composes one catalog from config, discovery, and "the project's
  harness". `validate_provider_ids` takes a harness kind string for its warning text.
- `GET /api/projects/{id}/mcp`, `POST /api/projects/{id}/mcp/reload`, and MCP OAuth login all
  resolve the project's single harness.
- The `[providers]` table is global and is validated against one provider table. Two Codex homes
  can declare different providers.

### Provider binding policy

The thread-open path in `crates/giskard-server/src/ws.rs` encodes the Codex rule that a provider is
bound to a native thread at creation and cannot change afterwards. The harness binding has the same
shape and the same reason: the native id exists in exactly one harness home.

### Codex wording outside the adapter

None of these are structural, but they are what a user sees:

- `WsError::from_harness` in `ws.rs` renders neutral `HarnessError` variants as "Codex CLI could not
  start", "Codex is not authenticated", "Codex operation timed out", and so on.
- `app.js` and `index.html` name Codex in the delete confirmation, the stop-command tooltip, the
  MCP empty state, the compact-context button, the server-request label, and the file-list
  fallback.
- README, spec §4.7, spec §6.4, and `crates/giskard-harness-codex/README.md` all say "one
  app-server per project".
- `crates/giskard-core` and `crates/giskard-proto` mention Codex only in doc comments, which is
  fine.

### Capabilities never reach the browser

Only `turn_steering` and the three MCP flags cross the wire. Plan/Build, approvals, reasoning
effort, diffs, and compaction are always shown, contrary to spec §13.5. Two Codex declarations do
not care; a second kind will.

## Design

### The instance model

An instance is created lazily, per project and declaration, the first time anything needs it:
opening a draft's model picker, opening a thread, an MCP request. Creating an instance is not a
promise to spawn a process.

- **`create`.** Codex spawns the app-server and runs the `initialize` handshake. Claude Code
  builds an in-memory instance and spawns nothing.
- **`open_thread`.** Codex sends `thread/start` or `thread/resume` on the shared process. Claude
  Code spawns that thread's process.
- **`subscribe`.** Codex has a retained log per route, filled by the shared reader. Claude Code
  creates the retained log at open and fills it from that process's reader.
- **Stream end.** A Codex process exit ends every thread stream of the instance. A Claude Code
  process exit ends that thread's stream only.
- **`discoveries`.** Codex reports native threads first seen in traffic. Claude Code's stream is
  always empty; a per-thread process produces no foreign traffic.
- **Idle policy.** Codex applies it at instance level and terminates the app-server. Claude Code
  applies it per process, inside the adapter.
- **`shutdown`.** Codex closes the transport and kills the process. Claude Code stops every live
  process.

The bootstrap contract is unchanged: `HarnessFactory::create` receives every `(native id,
ThreadId)` binding that belongs to this instance, validated and installed before ordinary event
dispatch begins.

### Configuration

Harnesses are declared in a table keyed by name, for the reasons `config.rs` already gives for
providers: a repeated name is a TOML parse error rather than a silent first-wins duplicate, and
declaration order is preserved for the picker.

```toml
[harnesses.codex-stable]
kind = "codex"
default = true                    # new threads start here unless a project or draft says otherwise
command = "codex"                 # optional; PATH lookup by default
# args = ["--foo"]                # extra arguments appended after Giskard's own

[harnesses.codex-nightly]
kind = "codex"
command = "/opt/codex-nightly/bin/codex"
# A Codex profile selects that home's `[profiles.<name>]` at spawn (`-c profile=nightly`): a
# different default model, effort, sandbox, or default provider for this instance only.
profile = "nightly"
# Environment for every process this instance spawns, applied over Giskard's own environment.
# Values are literal and may be credentials; they are never logged.
[harnesses.codex-nightly.env]
CODEX_HOME = "/home/you/.codex-nightly"

[harnesses.claude]
kind = "claude-code"
# command = "claude"
[harnesses.claude.env]
ANTHROPIC_API_KEY = "sk-ant-..."
```

Rules:

- **Neutral keys are `kind`, `command`, `args`, and `env`** (plus `default`, below). Spawning a
  process is common to every kind, so these are parsed by `giskard-persist` and apply to every
  process an instance spawns: Codex's one app-server and Claude Code's per-thread processes alike.
- **`env` is an overlay on Giskard's inherited environment.** Each entry is set on top of what
  Giskard itself was started with; nothing is removed or replaced wholesale. Values are literal:
  no tilde expansion and no `${VAR}` interpolation, so a path is written absolute. Expanding `~`
  would be right for `CODEX_HOME` and wrong for almost any other variable, and Codex already
  offers `env_key` indirection where a secret should stay out of a file. Names must be non-empty
  and contain neither `=` nor NUL, values no NUL; that is checked at boot with the rest. Values
  may be credentials, so they follow the `ProviderHttpHeaders` convention: a `Debug` that prints
  names only, and log lines that name variables and never their values. This is the neutral knob
  that replaces any kind-specific path key: `CODEX_HOME` for Codex, and for Claude Code whatever
  that CLI reads from its environment for its API key, config directory, or routing.
- **Kind-specific keys stay out of `giskard-persist`.** AGENTS.md confines Codex types to the
  adapter, and persist cannot depend on it. Persist parses the neutral keys above and an opaque
  TOML table of everything else. The binary's factory deserializes that table into the adapter's
  typed options with `deny_unknown_fields`, so a typo is an error rather than a silently ignored
  key. For Codex the only such key is `profile`.
- **Validate at boot.** `HarnessKind` has a `validate(&HarnessDeclaration)` step, and the
  binary's `HarnessKindFactory::validate` runs it over every declaration before serving. A
  declaration with an unknown kind or bad options fails startup with a message naming the table
  key. Instance creation must not be the first place a config mistake surfaces.
- **The name is a durable identifier.** It is persisted on projects and threads. Renaming a
  declaration is a migration, exactly as renaming a provider id would be. The README must say so.
- **The default harness is marked on its entry.** `default = true` on at most one declaration;
  two or more is a boot error naming both keys. None marked means the first declared entry, which
  is the rule the model catalog already uses for its default model, and declaration order is
  already what the `IndexMap` preserves for the picker. There is no separate `[harness]` section:
  one table describes the harnesses and which one is the default.
- **`codex` is the reserved default name, and only when nothing is declared.** With no
  `[harnesses]` table at all, Giskard synthesizes one declaration named `codex` of kind `codex`
  with default options, and it is the default because it is the only entry. Every existing
  `project.json` already says `harness = "codex"`, which reinterprets as that name unchanged, and
  a thread file without a `harness` field belongs to it (see *Persistence*).
- **Declaring the table is the off switch.** As soon as any `[harnesses.<name>]` entry exists,
  the config is exactly what it declares and nothing is synthesized. A Claude Code only setup
  declares `[harnesses.claude]` and never has a Codex harness. Pre-declaration projects and
  threads still resolve to the name `codex`, so a table without that entry leaves them in the
  degraded state below, with the config key named; adding the entry back reopens them. The README
  says so.
- **Unknown name is a visible degraded state.** A project or thread naming a harness that is not
  declared opens read-only with a structured error naming the config key. It never falls back to
  another harness silently.
- **The `[harness]` section is removed.** Its `kind` was never read at runtime, and nothing
  replaces it at the top level: the default lives on the entry. An old config still carrying the
  section is ignored, as unknown top-level tables are today, since no config is known to have
  used the key. `idle_shutdown_secs` was removed with it in Stage 0 rather than kept parsed but
  unused; idle shutdown as an instance policy remains an open question.
- **Providers are not declarations.** A provider is something an instance reports, not something
  the user declares as a harness. See *Provider scoping* for what is global and what is not.

### Persistence

No on-disk format version changes and nothing is migrated. Both files gain or reinterpret one
field with a constant default, following the precedent `ThreadFile` already set for `revision` and
`kind`: existing files predate the field, and the default is what they always meant.

**Project.** `ProjectConfig.harness` keeps its name and its existing values, and changes meaning:
it is the project's *default harness for new threads*. It stays a required string, stamped at
creation with the chosen or default declaration name; existing files carry `codex`, which resolves
unchanged. It is a creation-time input only. No runtime path may read it once a thread exists; the
thread's own field is authoritative. Document this on the field the way `_default_model` is documented in
`store.rs`.

It is kept rather than dropped because "this project runs on nightly" is naturally a project
setting that the global default cannot express, and because the existing value needs no rewrite.

**Thread.** `ThreadFile` gains `harness: String` naming the declaration, with a serde default of
the constant `codex`, and it is skipped on serialization when it equals that constant, as `kind`
is skipped when primary. A thread on the default harness therefore stays readable by an older
binary, which `deny_unknown_fields` would otherwise break on the first rewrite; a thread on any
other declaration writes the field explicitly. The default is a constant rather than "the project's
harness" on purpose: a serde default cannot see another file, and a constant keeps the project
field non-load-bearing at read time.

The field is fixed at native creation and never changes, for the same reason
`current_model.provider` is fixed on a Codex thread: the native id exists in exactly one harness
home. Sub-agent and discovered threads take the name of the instance whose driver admitted them.
The provider is untouched by any of this: it stays inside `current_model` and in each turn record,
and on open it is passed to the instance as `initial_model` exactly as today, where that instance's
own provider table interprets it.

The bootstrap scan gets a clean rule from this: an instance receives the threads whose `harness`
names it, a missing field counts as `codex`, and a thread naming an undeclared harness is a
per-thread degraded state rather than a project-wide failure.

**Turns.** No change. A turn belongs to a thread, and the thread's harness is fixed, so a turn
record needs no harness field. Token ledger keys stay `provider/model`.

### Registry

- `ProjectAuthority` holds one slot per declaration name instead of one slot: a map from harness
  name to `(Arc<dyn AgentHarness>, DriverHandle)` plus a model catalog slot per name. The map lives
  on the authority, keyed by declaration name rather than by project or thread identity, which is
  entity-local state on its authority and not a peer owning map. It needs the lifetime-class
  comment the convention asks for: entries are created on first use and removed on project delete
  and registry shutdown.
- `HarnessRegistry::harness(config)` becomes `harness(project, declaration_name)`. Callers that
  hold a `ThreadFile` pass its `harness`; callers acting on a draft pass the resolved default or
  the user's choice.
- `known_thread_bindings(project)` becomes `known_thread_bindings(project, harness)` and filters
  the thread graph by the thread field. Native id uniqueness is checked within that filtered set.
- `HarnessFactory::create(config, harness: &str, bootstrap)` takes the declaration name beside the
  `&ProjectConfig` (Stage 2). The factory already owns the catalog, so the name is enough and the
  registry never holds a declaration: the binary's `HarnessKindFactory` resolves the name — a
  thread's `harness`, or the project's default for a draft — to a declaration, and hands
  `HarnessKind::create` a `HarnessInstanceSpec { project_id, workspace_root, name, declaration }`;
  nothing outside that factory reads a declaration.
- Project deletion and registry shutdown quiesce every driver of the project before taking owner
  sets or shutting harnesses down, in the same order as today, iterated over the map.
- The harness transition gate stays root-wide and non-keyed.

### Model discovery and the picker

There is no separate harness picker. The draft's model picker shows the models of every declared
harness, and the harness is derived from the selection, the same way the provider already is.

**Composition.** `GET /api/projects/{id}/models` composes every declared harness, each from that
instance's catalog (config, discovery, harness catalog). The response is one flat `models` list
whose entries each carry `harness` beside the descriptor fields, `warnings` stamped with `harness`
the same way, a `harnesses` index giving each composed declaration's name, kind, default flag, and
capabilities (absent when the instance could not start), and `project_harness`, the project's
default declaration that the draft preselects. A flat list rather than nested groups keeps the
browser's model lookups and the existing response readers unchanged, and a single declaration
yields exactly the list served before, plus the new fields. Composing every group means every
instance for the project is created; for Codex that is one process per declaration, which is
accepted. `?harness=<name>` composes that declaration alone (an undeclared name is `404`). The
picker renders group headers only when more than one harness is declared, so a single-harness setup
looks exactly as it does today.

**Selection.** A picker entry is a triple `(harness, provider, model)`. `ModelRef` is unchanged
because it is a persistence and ledger key and `provider/model` is the right cost identity; the
harness rides alongside it on the wire entry and on the thread-open message. The server validates
the model against that instance's catalog, which it already does per project. Two Codex
declarations sharing a home will both offer `openai/gpt-5.5`; the group header is what tells them
apart, so the harness picker is folded into the model picker rather than removed.

**Rendering.** Only two of the three are drawn. `renderModelSelect` in `app.js` today builds a flat
`<select>` whose options read `name [provider]`. Each harness becomes an `<optgroup>` around its
instance's entries, emitted only when more than one harness is declared, and the option label stays
`name [provider]`. A single-harness setup therefore renders exactly as it does now. The option
carries harness, provider, and model as separate data attributes rather than a joined key: model
ids already contain slashes, so the current `provider/model` string is unambiguous only by luck.

**Scoping.** Once a thread exists, its picker requests one group, its own harness's, with
`?harness=<name>`, so opening a thread on one declaration never spawns the others. Mid-thread
model switching stays inside that harness for the same reason provider switching is rejected on a
bound Codex thread.

**Default.** The preselected entry is the default harness's default model, where the default
harness is the project's field, else the declaration marked `default`, else the first declared.

**Claude Code models.** Giskard's discovery mechanism is already the right one and is
harness-neutral: the harness reports providers with an endpoint and a key location, and Giskard
makes the model-list request itself (spec §8.2, §8.3). Anthropic serves `GET /v1/models` with no
beta header. Each entry carries `id`, `display_name`, `max_input_tokens` (the context window),
`max_tokens` (the output cap), and a `capabilities` tree that includes the supported effort levels.
That is the `ModelDescriptor` shape Giskard composes today, and it is richer than Codex's
`model/list`, which omits the window. The Claude Code adapter therefore reports `provider_listing`
with one provider named `anthropic` and leaves `list_models` unsupported.

Three extensions to discovery in `crates/giskard-server/src/models.rs`:

- A third body shape beside the plain OpenAI list and the Codex catalog: a `data` array with the
  fields above, paginated through `has_more` and an `after_id` query parameter.
- An auth placement on the provider. Discovery currently always sends `Authorization: Bearer`. An
  Anthropic API key goes in an `x-api-key` header, so `HarnessProvider` or `ProviderAuth` must say
  which. An OAuth token does use `Authorization: Bearer` but then also needs
  `anthropic-beta: oauth-2025-04-20`.
- The required `anthropic-version` header, which the harness delivers through the literal
  `http_headers` it already reports.

And one change that the declaration `env` forces on discovery for every kind. Today
`HarnessProvider::resolve_api_key` reads `ProviderAuth::Env` with `std::env::var`,
environment-backed headers resolve the same way, and `ProviderAuth::Command` runs in Giskard's own
environment. With a per-declaration overlay that is wrong twice over: a key that exists only in
the declaration's environment is not found, and a key from Giskard's own shell could be sent for a
home that uses a different one. So env-backed resolution and the auth command consult the
instance's overlay over the process environment. `HarnessProvider` carries an environment handle
supplied by the instance that reported it, `resolve_api_key` and header resolution read through
that handle, and `ProviderAuthCommand` gains the same `env` beside its existing `cwd`. Spec §8.2's
rule that only key locations cross into Giskard is untouched; the overlay is the user's own
configuration.

The credential is the part to verify against a real install before committing. An
`ANTHROPIC_API_KEY` user maps onto `ProviderAuth::Env` with the header placement above. A user
signed in through a subscription has an OAuth token in Claude Code's own credential store, and
§8.2 deliberately reads key locations rather than secrets; a `ProviderAuth::Command` that prints
the token keeps that line if a suitable command exists. Whether that token is accepted by the
Models API, and where Claude Code keeps it, are to be tested, not assumed.

Two fallbacks exist regardless. `[providers.anthropic.models]` already lets a user declare models
by hand with a context window and works on day one with no adapter work. A compiled-in default list
in the adapter is possible but goes stale, and the spec has avoided built-in model metadata on
purpose; discovery plus config is the design, and the compiled-in list is a last resort only.

### Provider scoping

**The harness owns the provider table.** This is true today and the design keeps it. A provider id
is a routing id the harness interprets: Codex's `[model_providers.<id>]` plus its built-ins, and
`anthropic` for Claude Code. `list_providers` is per instance, so two declarations have two
provider tables, and `openai` in `codex-stable` and `openai` in `codex-nightly` are two entries
that may or may not point at the same endpoint. Validation, discovery, and the composed catalog
are all per instance, so in substance a provider is harness-scoped.

**A provider does not carry its harness; the thread does.** `ModelRef` stays `provider/model`.
Once the thread field exists, every persisted `ModelRef` sits next to a `harness`, so a stored
provider id is always resolved relative to a known instance. The picker entries carry the harness
for the same reason, and the UI never uses provider alone as a key.

**What stays keyed by id alone.** Three surfaces, deliberately:

- The `[providers.<id>]` overlay. It applies to every instance that reports that id. For two Codex
  homes sharing provider names that is the intended behaviour; for a second kind the namespaces do
  not collide, since Codex has no `anthropic` and Claude Code has no `openai`.
- `[tokens.rates."provider/model"]`.
- The ledger's `by_model` aggregation.

The one bad case is two instances of one project reporting the same id with different endpoints:
the overlay's context windows would be right for one and wrong for the other, and cost would be
attributed to one line. Composition detects it and emits a warning naming both harnesses. A
per-declaration overlay, `[harnesses.<name>.providers.<id>]`, is the fix if that case is ever
observed in practice; it is not built ahead of it.

**Alternatives considered and rejected.** Two shapes were weighed that would remove providers as a
Giskard concept and make each provider its own harness instance of the same kind, either by
spawning Codex with `-c model_provider=<id>` per declaration, or by pointing each declaration at a
Codex profile whose `model_provider` selects it. Both were rejected:

- Codex hosts threads on different providers inside one process. `thread/start` takes a
  `modelProvider`, and the adapter already uses it. One app-server per provider per project
  multiplies memory and startup for something the protocol does for free.
- Both forfeit provider discovery. Today a provider declared to Codex appears in the picker with no
  Giskard config at all, read back through `config/read`. Under either shape every provider needs a
  Giskard declaration, and adding one to Codex means editing two files.
- Profiles are a Codex-only concept. Claude Code has settings files and a `--model` flag, and one
  provider unless routed through Bedrock or Vertex by environment. The neutral model has to be "an
  instance reports N providers", with N equal to one for Claude Code.

What survives from the profile idea is the spawn knob: the Codex declaration carries `profile`
beside the neutral `command`, `args`, and `env`, passed as `-c profile=<name>`. `codex app-server`
has no `--profile` flag, but `-c` overrides any config key, and `profile` is one. A profile can
change the instance's default model, effort, sandbox, and default provider; that default provider
is the one Codex's own `model/list` describes, since its catalog is per process and per startup
provider. The process sees `command`, then the environment overlay, then the profile override and
`args`; inside Codex a `-c` override beats the environment, as on the command line today.

**Codex's models cache, checked against `openai/codex` `193632d` (2026-09-29).** The concern that
two processes on one Codex home with different providers would fight over the model list cache is
real but benign, and moot for Giskard:

- The cache is one file, `$CODEX_HOME/models_cache.json`, holding a single entry that every store
  overwrites with a plain write. The TTL is a fixed five minutes.
- Each entry carries a SHA-256 identity over provider name, base URL, catalog URL, query
  parameters, auth mode, account, and auth headers (`model-provider/src/models_identity.rs`). A
  load whose identity does not match is a miss. Two processes on different providers or accounts
  therefore overwrite each other's entry and refetch on the next refresh; they never read a wrong
  catalog. Two processes on the same provider and account share the entry.
- The cache rarely applies to custom providers at all. Codex fetches `/models` only for the ChatGPT
  backend, command-auth providers, and API-key providers that opt into API-key model discovery. A
  plain `[model_providers.x]` with an `env_key` falls back to the bundled catalog, which is why
  Giskard does its own `/v1/models` discovery per provider (§8.3) and never depends on this cache.
- The catalog is per process and per startup provider: `ThreadManagerState` holds one
  `SharedModelsManager` built from `config.model_provider`, `model/list` takes no provider
  parameter, and a provider change in config answers "Restart Codex to apply".

### Wire protocol and UI

- The project detail carries the project's default harness name. `ProjectSummary` does not: the
  project list is built from `projects.json`, which has no harness column, and the draft learns the
  project's default from the models response instead. Deferred.
- Thread summaries and the thread-open response carry the thread's harness name, so the UI can
  scope the picker and name it in the sidebar row's tooltip (done in Stage 2; no badge).
- The draft's start request carries `harness` beside `model_ref`, sent only when the picker
  offered a choice (done in Stage 2). `CreateProjectRequest` accepts an optional default harness
  name (done in Stage 1, with `GET /api/harnesses` listing the declarations for the new-project
  modal).
- `HarnessCapabilities` is serialized in full on the project models response, which the draft and
  every thread open already load, and the UI gates Plan/Build, approvals, effort, model, and
  compaction on it as spec §13.5 describes. Per-harness groups in Stage 2 carry it per group.
- The models route returns groups as described above. MCP routes gain a harness segment:
  `/api/projects/{id}/harnesses/{name}/mcp`, `/mcp/reload`, and `/mcp/oauth-login`, and the
  project-wide paths are removed (done in Stage 2). `docs/api-endpoints.md` is updated in the same
  change.
- Neutral wording replaces Codex wording in `WsError::from_harness`, `app.js`, and `index.html`.
  Where a message must name the harness, it names the declaration, which is what the user wrote in
  their config.

### Spec and documentation changes

- §4.7 and §6.4 are rewritten around the instance definition above, with the process model as an
  adapter-specific paragraph each: Codex, one app-server per instance hosting every thread; Claude
  Code, one process per primary thread spawned in `open_thread` and stopped on delete, archive,
  shutdown, or idle. The crash-handling paragraph becomes "marks the threads whose stream ended",
  which is what the server already does per thread.
- §1.2 and §13.5 stand; §13.5 becomes true rather than aspirational.
- Appendix C replaces the `[harness]` section with `[harnesses.<name>]`, including the `default`
  flag.
- §8.2 gains the auth-placement note and the Anthropic body shape.
- README: the *Supported harnesses* section, the config table, the storage layout comment on
  `project.json`, a note that a harness name is a durable identifier, and the rule that a
  declaration named `codex` must remain while pre-declaration projects or threads exist.
  `config.example.toml` gains the declarations block.
- `crates/giskard-harness-codex/README.md`: "one app-server per project" becomes "per instance", and
  the declaration options (`command`, `args`, `env`, `profile`) are documented next to lifecycle
  behavior,
  with the note that `model/list` describes the instance's startup provider only.
- `docs/subagents.md`: a child inherits its parent's harness.

## Staging

Each stage is shippable on its own and leaves the previous behaviour intact for a config that does
not opt in.

1. **Stage 0, no behaviour change.**
   Replace Codex wording in the neutral layers. Serialize `HarnessCapabilities` to the browser and
   gate the UI on it. Turn `CodexFactory` into a kind-dispatching factory that also owns validation.
   Remove the dead `[harness]` section and either implement or remove `idle_shutdown_secs`. Rewrite
   §4.7, §6.4, and the adapter README around instances.
2. **Stage 1, named declarations, one harness per project.**
   `[harnesses.<name>]` with its `default` flag, the synthesized `codex` default, boot-time
   validation, the project field reinterpreted as a name, `HarnessFactory::create` taking a
   declaration, and the Codex adapter accepting `command`, `args`, `env`, and `profile`. The
   replay binary declares its scripted harness under the name `codex` so its seeded projects
   resolve unchanged. Stable versus nightly already works at project granularity here, and this
   is the cheap win.
3. **Stage 2, per-thread harness.**
   The defaulted thread field, per-name slots on the project authority, per-harness
   bootstrap filtering, grouped model composition, harness-scoped MCP routes, the wire fields, and
   the grouped picker.
4. **Claude Code adapter.** A separate track that depends on Stage 0's capability gating and on the
   discovery extensions above, and on nothing in Stages 1 or 2. The registry needs no change for a
   per-thread-process adapter.

## Open questions

- **Idle shutdown.** Implement as an instance policy on the declaration, or drop the key. If
  implemented, Codex terminates the app-server and resumes threads on next use; Claude Code applies
  it per process.
- **Claude Code credentials for discovery.** See *Claude Code models*. Resolve against a real
  install before designing the `ProviderAuth` extension in detail.
- **Per-declaration provider overlay.** Deferred until the same-id, different-endpoint warning in
  *Provider scoping* is seen in practice.
- **Bedrock and Vertex through Claude Code.** Claude Code can route through either; whether the
  adapter reports them as providers, and what discovery looks like there, is out of scope for the
  first adapter.
