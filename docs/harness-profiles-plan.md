# Harness profiles: multiple harnesses in one project

Implementation plan, written against `main` at `2525288` on 2026-09-07. This is a handoff
document, not an amendment to the authoritative specification. The implementation must update
`specs/giskard-specification.md` as described below.

## Goal

A project is a workspace, not a harness lifetime. One project may contain threads backed by
different harness kinds and by different configurations of the same kind. The first shipped use
is multiple Codex profile files, including one Codex app-server per selected profile, without
sharing Codex's provider-agnostic `models_cache.json` between those processes.

A **harness kind** selects an adapter implementation, such as `codex`, `claude-code`, or `pi`. A
**harness profile** is a named, independently runnable configuration discovered by that adapter.
A thread is durably bound to exactly one `(kind, profile)` pair when it is created. It never
switches profile afterward.

For Codex, profiles are:

- `default`, representing ordinary Codex configuration from `$CODEX_HOME/config.toml`; and
- each `$CODEX_HOME/<name>.config.toml`, represented by profile id `<name>` and launched with
  Codex's native `--profile <name>` option.

Codex `[model_providers]` entries are not profiles by themselves. A user who wants an additional
provider as a separately selectable harness creates a Codex profile file that selects it. Giskard
enumerates profile filenames but does not parse their contents; Codex remains the authority for
configuration precedence and validation.

## Settled product decisions

- A project can mix profiles and harness kinds. Harness selection belongs to a new-thread draft,
  not project creation.
- Harness kinds are explicitly enabled in Giskard configuration. A locally installed CLI is not
  probed, started, or displayed when its kind is disabled.
- Profile discovery is adapter-owned. Giskard configuration overlays discovered profiles; it is
  not normally the source of their native configuration.
- `default_profile` belongs to each kind. `default_kind` is global.
- Existing threads migrate to their project's old harness kind and that kind's `default` profile.
- A missing or disabled bound profile leaves history readable and harness-backed actions
  unavailable, matching the current missing-provider read-only behavior. Giskard never silently
  rebinds it.
- A persisted thread cannot change harness profile. Starting a new thread is the way to use a
  different profile.
- Sub-agents inherit the exact profile of their parent.
- MCP and other harness-instance settings exist only in an opened thread context. The server
  derives the profile from that thread; there is no fallback to a default profile.
- Codex multi-profile support is available only when its cache can be isolated. Linux and updated
  WSL2 are the initial target. WSL1, native Windows, macOS, and failed capability probes reject
  additional Codex profiles while leaving the default usable.

## Current constraints verified in the repository

- `ProjectConfig.harness: String` makes one harness kind a project property
  (`crates/giskard-persist/src/store.rs`).
- `ThreadFile` persists `harness_thread_id` and `current_model` but no harness identity.
- `ProjectHarnessSlot` holds one `ProjectHarnessState` and one event driver
  (`crates/giskard-server/src/registry/project.rs`).
- `HarnessRegistry::get_or_create_harness` builds one bootstrap and driver per project
  (`crates/giskard-server/src/registry.rs`).
- `HarnessFactory::create` receives only `ProjectConfig` and a whole-project `HarnessBootstrap`.
- `docs/s9-harness-scopes.md` already establishes that `AgentHarness` is an instance contract,
  not an operating-system-process contract, and inventories the one-harness assumptions.
- The current model catalog is cached per project, and the UI stores one `state.models` list per
  project.
- MCP HTTP routes are project-scoped today even though their UI is shown only with an open thread.
- Codex's `ModelsManager` uses `$CODEX_HOME/models_cache.json`. The released implementation does
  not make a fresh cache entry provider/profile-specific.
- `CodexHarness` currently owns one `CodexInstance`; its task-owned mapper, routes, active turns,
  compactions, context restores, and transport must remain owned by that instance.

## Configuration contract

Replace the singular `[harness]` configuration with this shape:

```toml
[harnesses]
default_kind = "codex"

[harnesses.codex]
enabled = true
default_profile = "default"
idle_shutdown_secs = 0

# Optional overlay for a discovered Codex profile.
[harnesses.codex.profiles.cloudflare]
enabled = true
display_name = "Codex — Cloudflare"
```

The core keys under a kind are `enabled`, `default_profile`, `idle_shutdown_secs`, and
`profiles`. A profile overlay has only `enabled` and `display_name` in the first version. Native
Codex configuration continues to live in `CODEX_HOME`; do not duplicate commands, provider URLs,
credentials, or native profile contents into Giskard configuration.

Configuration behavior:

1. Omitted future harness kinds are disabled. For backward compatibility, an entirely absent
   `[harnesses]` section behaves like today's default Codex configuration.
2. `enabled = false` prevents discovery and process creation for the kind.
3. Discovery produces the implicit `default` profile first, followed by native profile names in
   bytewise filename order. An overlay may rename or disable one without changing its identity.
4. A profile overlay that does not match discovery is retained as an unavailable configuration
   warning; it does not fabricate a runnable profile.
5. `default_kind` must name an enabled kind. Its `default_profile` must resolve to an enabled,
   available profile before a new draft can send. Discovery failures are visible rather than
   replaced with the first profile.
6. Reject duplicate profile ids after platform-appropriate filename normalization. Do not choose
   one nondeterministically on case-insensitive filesystems.
7. Continue accepting legacy `[harness]` only as a compatibility input. Translate it in memory to
   `harnesses.<kind>` plus `default_kind = <kind>` and `default_profile = "default"`. Reject a file
   that contains both legacy and new sections. Update `README.md` and `config.example.toml` in the
   same implementation change.

Define neutral configuration types in `giskard-persist`; adapter-native discovery results belong
in `giskard-harness`, not persistence:

```rust
pub struct HarnessProfileRef {
    pub kind: String,
    pub profile: String,
}

pub struct DiscoveredHarnessProfile {
    pub id: String,
    pub display_name: String,
    pub availability: HarnessProfileAvailability,
}

pub enum HarnessProfileAvailability {
    Available,
    Unavailable { reason: String },
}
```

`Unavailable.reason` must be non-empty at construction. Enabled/disabled is configuration policy,
not discovery availability, and remains outside this enum: an available discovered profile may be
disabled by its overlay, but it cannot simultaneously be available and unavailable.

Use structured fields everywhere. Do not build a persisted `"kind:profile"` string: externally
owned identifiers may contain punctuation, and separator escaping would become part of the disk
format.

## Persistence and migration

Add `harness_profile: HarnessProfileRef` to `ThreadFile` and bump
`THREAD_METADATA_VERSION`. Keep `ProjectConfig.harness` readable only for migration; new project
files no longer use it as runtime authority. Preserve `deny_unknown_fields` and explicitly absorb
the legacy field, following the existing `_default_model` compatibility pattern.

Migration rules are deterministic:

- A version-old thread without `harness_profile` receives
  `{ kind: project.legacy_harness_or_default_kind, profile: "default" }`.
- Loading does not rewrite files. The field is written on the next ordinary atomic thread metadata
  commit, with the new version.
- New threads persist the selected profile in their first `ThreadFile` write.
- Imported/discovered native child threads inherit the admitting parent's profile. A provider-owned
  discovery with no attributable parent is admitted by the profile driver that observed it, so it
  receives that driver's profile.
- `harness_thread_id` remains opaque only within its harness profile. Bootstrap and route identity
  must always include the profile before comparing native ids.
- Removing a profile never edits affected thread files. History, diffs, attachments, worktree
  inspection, archive metadata, and deletion remain available where they do not require native
  harness I/O. Native operations return a structured unavailable-profile error.

Update persisted fixtures and add migration tests for old project/thread files, missing profiles,
disabled kinds, malformed new configuration, and round trips of the new version.

## Harness discovery and factory interfaces

Split adapter availability from instance creation. The production server should own a registry of
kind factories rather than one `HarnessFactory`:

```rust
#[async_trait]
pub trait HarnessKindFactory: Send + Sync {
    fn kind(&self) -> &'static str;

    async fn discover_profiles(
        &self,
        project: &ProjectConfig,
        config: &HarnessKindConfig,
    ) -> Result<Vec<DiscoveredHarnessProfile>, HarnessError>;

    async fn create(
        &self,
        project: &ProjectConfig,
        profile: &HarnessProfileRef,
        bootstrap: HarnessBootstrap,
    ) -> Result<Arc<dyn AgentHarness>, HarnessError>;
}
```

Keep `AgentHarness` as the contract for one created instance. Do not split it by scope: the S9
analysis established that the existing trait is process-model-neutral and splitting it does not
reduce adapter work.

The profile-discovery cache is per `(project, kind)` and must distinguish success, failure, and an
in-flight refresh. Concurrent requests share one refresh. Configuration reload invalidates it.
Discovery must not start disabled kinds. Profile creation revalidates that the discovered profile
is still available before publishing a harness.

For tests, replace the single injected factory with a small kind-factory registry and provide a
one-kind convenience constructor so existing test setup remains concise. Extend `giskard-testenv`
with deterministic profile discovery, per-profile harness lookup, and per-profile failure fixtures.

## Registry authority and lifecycle

`RegistryShared::projects` and `RegistryShared::threads` remain the only strong root entity-owner
maps. Do not introduce another project- or thread-keyed peer map.

Within `ProjectAuthority`, replace the singular harness slot with profile components grouped by
their identical cleanup lifetime. Each entry owns:

- the active `Arc<dyn AgentHarness>`;
- its `ProjectEventDriver` handle;
- lazy-creation or deletion transition state; and
- its profile-scoped composed model catalog.

Use `HarnessProfileRef` as the key and document the cleanup site. This is component state owned by
the existing project authority, not a competing project authority.

Lifecycle requirements:

1. Resolve or create a harness using `(project authority, profile)`.
2. Build `HarnessBootstrap` only from threads whose persisted profile exactly matches.
3. Create one event driver and one discovery consumer per active profile. All forwarders installed
   by that driver belong to the same profile.
4. Store the profile on `ThreadHandle`/route capabilities so subscription and later calls cannot be
   accidentally sent through another instance. Do not add a convenience map keyed by `ThreadId`.
5. Turn admission continues to go only through the thread's event forwarder. The forwarder resolves
   its weak harness from the profile component selected during attachment.
6. A crash or stream closure affects only threads attached to that profile. Other profiles remain
   live.
7. Idle shutdown is evaluated independently per profile using the kind's configured timeout.
8. Project deletion and registry shutdown first quiesce every profile driver, take the complete
   owner set only after those fences, then shut harnesses down concurrently.
9. Project deletion removes every profile-specific cache artifact after its harness has stopped.
10. Request ids published without a thread (`ApprovalId`, `ServerRequestId`) must remain unique
    within each `AgentHarness`. Server routing must retain the originating profile; never search
    active harnesses for a matching id.

Add race tests for same-profile concurrent creation, different-profile parallel creation,
creation versus project deletion, shutdown with several profiles, one-profile crash isolation, and
profile disappearance between discovery and creation.

## HTTP, WebSocket, and model contracts

Add authenticated profile discovery for a project. Prefer a collection response rather than
encoding externally owned ids in path segments:

```text
GET /api/projects/{project_id}/harness-profiles
```

Each entry carries the structured reference, display name, tagged availability, capability summary,
enabled state, and whether it is the configured initial selection. Failures for one enabled kind
are returned as structured warnings while successful kinds remain usable. The wire availability
uses the same mutually exclusive `available` or `unavailable { reason }` representation rather than
a boolean plus optional reason.

Change model listing to require a profile through query parameters or a structured request. Keep
models scoped to that profile:

```text
GET /api/projects/{project_id}/models?harness_kind=codex&harness_profile=cloudflare
```

Do not merge all profiles into a global model list and do not add harness identity to `ModelRef`.
Within a selected profile, the existing `(provider, model)` identity remains valid. Cache composed
catalogs per `(project, profile)` and invalidate only the affected entry.

Add `harness_profile` to `StartThreadRequest`, `StartThreadResponse`, `ThreadSummary`, thread state,
and reconnect snapshots wherever the browser must retain or render the binding. The server validates
the requested profile immediately before native creation and persists exactly the effective profile.

For existing-thread actions, never trust a client-supplied profile. Load the thread, read its durable
binding, and resolve the harness from that. Cross-profile requests fail before harness I/O.

MCP status, reload, and OAuth are visible only for an opened thread. Amend those routes to identify
the thread, then derive `(kind, profile)` server-side. There is no default-profile fallback and no
MCP UI when no thread is open. Apply the same rule to any other instance-scoped control that is only
present in thread context.

Update `docs/api-endpoints.md` in the same change as route modifications. Update protocol JSON
round-trip tests and reject legacy start requests that omit a profile once the browser and replay
fixtures have migrated; persistence compatibility does not require indefinite HTTP compatibility.

## Browser behavior

Add a harness selector to the new-thread draft before the model selector. Its value is a structured
profile reference held in draft state.

- Opening a project loads profile discovery, selects `harnesses.default_kind` and that kind's
  `default_profile`, then loads only that profile's models.
- Changing the draft profile clears the prior model, loads the new catalog, and selects that
  catalog's default model. A late response for a previously selected profile is ignored.
- Sending is disabled until both the profile and model are resolved. Never fall back to another
  profile after an error.
- Persisted threads display their profile and keep the selector locked. Model changes remain within
  that profile.
- A missing profile shows the existing read-only presentation with a profile-specific explanation.
  It does not unlock profile switching.
- Sub-agent navigation displays the inherited profile but offers no selector.
- Capability-gated controls are recomputed from the active thread's profile. With no active thread,
  thread-only controls including MCP are absent.

Update replay seeded state and helpers, UI unit assertions, and Playwright coverage. Because the
composer controls visibly change, regenerate `docs/screenshots/ide-desktop.png` and
`docs/screenshots/ide-mobile.png` with `tests/e2e/screenshots.sh`.

## Codex profile discovery

Resolve `CODEX_HOME` with the same environment/default rules as the launched Codex. Treat the
implicit `default` profile as present even when `config.toml` does not exist. Enumerate direct child
files matching `<name>.config.toml`; exclude `config.toml`, directories, invalid/empty names, and
ambiguous normalized duplicates. Symlink handling must be explicit and tested; follow a symlink only
when its resolved target is a regular file.

Do not read provider ids from the files. Start the selected profile and use Codex `config/read` and
`model/list` for its effective configuration. A malformed native profile is discovered but becomes
unavailable when validation/startup fails, with the profile name and underlying Codex error logged.

Each project/profile gets its own `CodexHarness` and therefore its own task-owned `CodexInstance`,
mapper, transport, queues, routes, and retained logs. Do not make these structures shared through
new mutexes. The post-`2525288` module split places queue, RPC, upload, transport, and instance logic
in their own files; keep process selection/spawn mechanics near transport construction rather than
moving protocol state out of `CodexInstance`.

Launch native profiles with Codex's `--profile <name>` startup option; omit it for `default`.
Initialization, version checking, provider listing, model listing, and MCP all occur independently
on that profile's app-server connection.

## Codex model-cache isolation

Before implementing the namespace path, add an integration spike that runs two real or fixture
Codex-compatible processes against one synthetic `CODEX_HOME` and proves that every file except
`models_cache.json` is shared while cache contents differ.

A synthetic `CODEX_HOME` symlink farm was considered as a namespace-free alternative: give each
profile its own directory and symlink every entry except `models_cache.json` back to the real home.
Reject that design. Codex creates files and directories lazily, so Giskard cannot know all required
links when it constructs the synthetic home. Pre-creating links for entries that do not exist yet
could also change or confuse Codex's existence-based behavior. The complete set of home entries is
not a documented compatibility contract and may change between Codex versions; maintaining a
hard-coded allowlist would therefore be incomplete and error-prone. The mount namespace instead
preserves the real `CODEX_HOME` wholesale and replaces only the known conflicting cache file.

Initial Linux/WSL2 implementation:

1. Add pnut as a Linux-target dependency.
2. Spawn the current Giskard server executable in a hidden helper mode with inherited piped stdio.
   The helper builds the sandbox and uses pnut's `execve` mode, so Tokio continues to supervise the
   process that becomes `codex app-server`. Do not call pnut's blocking `Sandbox::run()` on an async
   worker.
3. Enable only user and mount namespaces. Preserve the host network namespace; Codex requires API
   access.
4. Rebind the host filesystem and overlay a pre-created project/profile cache file at
   `$CODEX_HOME/models_cache.json`. Everything else under `CODEX_HOME`, including configuration,
   authentication, sessions, skills, logs, and SQLite state, remains the real shared path.
5. Store cache files below the project's Giskard directory in a deterministic profile directory.
   Use normal atomic Giskard directory/file creation and remove them on project deletion.
6. Probe Linux >= 5.11, unprivileged user namespaces, mount namespace creation, the required mount
   operations, and cache write/readback. Detect capabilities, not WSL branding.
7. The current Codex cache uses truncate/write semantics. A bind-mounted file cannot safely support
   every future rename-over replacement. Detect a cache write failure, log project/profile/path and
   source error, surface it to the browser, and never retry against the shared cache.
8. When isolation is unavailable, permit `codex/default` only. Additional profiles remain visible
   as unavailable with an actionable explanation. WSL1, native Windows, and macOS follow this path.

Security review must verify that the namespace adds no unintended filesystem or network restriction
to agent commands: its purpose is cache identity, not command sandboxing. Test WSL-style paths and
workspaces under `/mnt/c`, but recommend the WSL native filesystem for performance.

## Implementation milestones

### M1 — Neutral profile types and configuration

- Add structured profile references, new config parsing, legacy translation, validation, and docs.
- Add adapter discovery types and the kind-factory registry without changing runtime selection.
- Exit: existing Codex-only behavior and all tests remain unchanged through the compatibility path.

### M2 — Durable thread ownership

- Add and migrate `ThreadFile.harness_profile`; propagate it through core/proto summaries and
  bootstrap bindings.
- Persist inheritance for sub-agents and native discoveries.
- Exit: old stores load without writes; new/updated files round-trip at the new version.

### M3 — Multi-profile registry

- Replace the project singleton slot with profile components, partition bootstraps, and install one
  driver per profile.
- Route all thread/turn/request lifecycle calls using durable or route-carried profile identity.
- Exit: two fake profiles run concurrent turns in one project; deletion and shutdown drain both.

### M4 — Profile-scoped APIs and catalogs

- Add discovery endpoint, scope model catalogs, change thread start, and make MCP thread-derived.
- Update endpoint documentation and protocol tests.
- Exit: failures and refreshes in one profile do not affect another profile's models or settings.

### M5 — Browser and replay

- Add draft selection, locked persisted display, unavailable behavior, and capability recomputation.
- Update replay fixtures and E2E coverage.
- Exit: browser tests cover mixed profiles, races between catalog loads, reload, and missing profiles.

### M6 — Codex native discovery and independent processes

- Discover default/profile files, launch the matching native profile, and exercise independent
  app-server lifecycle.
- Exit: two Codex profiles in one project produce correctly attributed catalogs and turns.

### M7 — pnut isolation and platform gating

- Add helper mode, mount override, probes, cleanup, WSL2 tests, and unsupported-platform errors.
- Exit: simultaneous profiles cannot observe or overwrite one another's model cache.

### M8 — Specification, documentation, and final cleanup

- Replace every authoritative “one harness per project” statement with the profile model.
- Synchronize README, example config, Codex harness README, endpoint inventory, storage layout, and
  screenshots.
- Remove legacy runtime assumptions only after migration tests prove compatibility.

## Test matrix

At minimum, add focused coverage for:

- absent, legacy, valid, conflicting, disabled, and malformed harness configuration;
- deterministic Codex profile enumeration, symlinks, invalid names, duplicates, and missing home;
- explicit defaults, unavailable defaults, and no enabled kind;
- old thread migration and new metadata round trips;
- default-bound legacy threads, per-profile bootstrap partitioning, and native-id reuse in different
  profiles;
- concurrent turns on two profiles and two kinds using fakes;
- profile creation races, crash isolation, restart, idle shutdown, registry shutdown, project
  deletion, and rollback after partial creation;
- sub-agent inheritance and orphan discovery attribution;
- approval/server-request routing with identical native ids from two profiles;
- model catalog isolation, concurrent refresh, stale response rejection, and partial warnings;
- MCP derivation from an opened thread and rejection of mismatched/missing thread context;
- draft selection, persisted locking, unavailable read-only UI, reconnect, and multi-tab updates;
- two isolated Codex cache files with shared configuration and sessions;
- capability-probe and cache-write failures with structured browser errors and contextual logs.

Run after every milestone at the narrowest affected crate, then finish with:

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
cargo build
cargo build -p giskard-server --bin giskard-server-replay
GISKARD_E2E_PREBUILT_BIN=target/debug/giskard-server-replay tests/e2e/run.sh
tests/e2e/screenshots.sh
git diff --check
```

Tests that open listeners must continue to bind port `0` through `giskard-testenv::TestServer`.

## Documentation changes required during implementation

- `specs/giskard-specification.md`: project/thread definitions, harness contract, process lifecycle,
  persistence examples, model selection, MCP scope, configuration appendix, and every occurrence of
  one harness/driver/catalog per project.
- `README.md` and `config.example.toml`: configuration, profile discovery, selection, unavailable
  behavior, platform restrictions, storage, and run examples.
- `crates/giskard-harness-codex/README.md`: native profile mapping, process/mapper/route scope,
  bootstrap partitioning, cache namespace, restart semantics, and native-id ownership.
- `docs/api-endpoints.md`: profile discovery, profile-scoped models, thread-start shape, thread-derived
  MCP requests, and structured failures.
- `docs/s9-harness-scopes.md`: mark its future multi-harness inventory as implemented or superseded;
  retain its process-neutral trait reasoning.

## Stop rules

Stop and report rather than improvising if:

- a Codex release no longer maps profile files to `--profile` as documented;
- pnut cannot rebind the host root plus one writable file while preserving inherited stdio;
- Codex changes cache persistence to an operation incompatible with a file mount;
- an existing thread cannot be attributed to a project legacy harness during migration;
- implementing profile routing appears to require a second strong project/thread owner map;
- project deletion cannot quiesce all profile drivers before taking their harness owner set; or
- a browser action would need to trust a profile different from the thread's durable binding.
