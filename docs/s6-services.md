# S6 — `Services`: the forwarder takes what it uses, not the registry

Implementation plan for step 6 of [`design-straightening-review.md`](design-straightening-review.md)
(finding B3). Written against `main` at `b2ae422` (S5 merged); every file and line reference below
was checked against that tree. Re-check them if the branch has moved.

## Goal

`RegistryShared` is two things: the registry's own state (project and thread indexes, the harness
transition gate, the background-task tracker, the driver event sink) and five process-wide service
handles (`hub`, `runtime`, `store`, `thread_metadata`, `ledger`) that other components reach
through it. The thread event forwarder is the largest such component and, in production, touches
only the five services and none of the registry state. S6 moves the five handles into a
`Services` struct, owned by `RegistryShared` and handed to the forwarder by `Arc`, so the
forwarder no longer holds a `RegistryShared` at all. No behaviour changes; every call moves one
field deeper or one type sideways.

## Non-goals

- No change to what any service does, to `ThreadRuntimeSupport`, `ThreadMetadataService`,
  `Hub`, `PersistStore`, or `LedgerHandle`, or to any log line.
- No change to the registry's public API (`HarnessRegistry`) or to `AppState`.
- The driver and the admission code keep their `Arc<RegistryShared>`: the driver needs
  `thread_authority` and `driver_events`, admission needs `coordinator` (ground truth). They reach
  services through `shared.services`.
- The thread-update forwarder (`registry.rs:469-517`) keeps `Arc<RegistryShared>`: it needs
  `background_tasks`. The one forwarder-module test that exercises it keeps its `RegistryShared`.
- No `Deref` from `RegistryShared` to `Services`, and no accessor methods that hide the extra
  field: the point is that a reader can see which components need the registry and which need
  only its services.

## Ground truth

| Fact | Where |
| --- | --- |
| `RegistryShared` has ten fields: `projects`, `harness_transitions`, `threads`, `background_tasks`, `driver_events`, and the five services `hub: Arc<Hub>`, `runtime: Arc<ThreadRuntimeSupport>`, `store: Arc<PersistStore>`, `thread_metadata: Arc<ThreadMetadataService>`, `ledger: LedgerHandle` | `crates/giskard-server/src/registry.rs:281-292` |
| Constructors: `new(hub, store, ledger)` (`:398-407`, `cfg(test)`), `new_with_driver_events(..)` (`:409-423`, `cfg(test)`), `new_with_max_command_output_bytes(hub, max, store, ledger, driver_events)` (`:425-447`), which builds `thread_metadata = ThreadMetadataService::new(store.clone(), hub.clone())` (`:432`) and `runtime = ThreadRuntimeSupport::with_max_command_output_bytes(max)` | `registry.rs` |
| `HarnessRegistry` constructors wrap them: `new` (`:579-589`, `cfg(test)`), `new_with_driver_events` (`:592-608`, `cfg(test)`), `new_with_max_command_output_bytes` (`:611-629`); `thread_metadata_service` returns `self.shared.thread_metadata.clone()` (`:631-633`) | `registry.rs` |
| The forwarder holds `shared: Arc<RegistryShared>` (`event_forwarder.rs:677`), takes it in `new` (`:699`), and in production reads only services: `shared.runtime` ×10, `shared.hub` ×8, `shared.thread_metadata` ×2, `shared.store` ×1, `shared.ledger` ×1, plus `publish_runtime_overview(&self.shared)` at `:855`. It never calls a `RegistryShared` method. The identifier `shared` occurs 35 times before `mod tests` (`:1959`); `RegistryShared` twice | awk over the code before `mod tests` |
| `publish_runtime_overview(shared: &RegistryShared)` (`registry.rs:1668-1673`) reads `shared.hub` and `shared.runtime` only; four callers: `registry.rs:1091`, `:1420`, `:1604`, `event_forwarder.rs:855` | grep |
| Registry-side production reads of the five fields: 21 in `registry.rs` (`shared.runtime` ×15 at `:465, :496, :524, :548, :555, :860, :978, :1005, :1040, :1084, :1398, :1418, :1603, :1671, :1710`; `shared.store` ×3 at `:750, :1754, :1847`; `shared.hub` `:1100`; `shared.thread_metadata` `:632`; `shared.ledger` `:1512`) and 4 in `registry/admission.rs` (`shared.store` at `:122, :138, :178, :226`). `driver.rs` reads none | grep |
| The driver spawns the forwarder with `let shared = self.shared.clone()` (`driver.rs:614`) and `ThreadEventForwarder::new(shared, ..)` (`:622-631`) | read |
| Free functions in `registry.rs` that take a `RegistryShared` and also use registry state (`thread_authority`, `coordinator`, `active_harness`, `event_driver`, `background_tasks`, `intern_thread_authority`): `prepare_thread_updates` `:451`, `spawn_thread_update_forwarder` `:469`, `resolve_subagent_link_info` `:1693`, `resolve_reverse_subagent_target` `:1748`, `ensure_subagent_thread_open` `:1834`, `install_event_owner` `:1893`. They keep their parameter | read |
| Forwarder tests build a `RegistryShared` five times (`event_forwarder.rs:2123`, `:2944`, `:3709`, `:5472`, `:5538`) and pass it to `ThreadEventForwarder::new` four times (`:2145`, `:3729`, `:5510`, `:5565`); two fixture helpers return `Arc<RegistryShared>` in their tuple (`:2113`, `:2177`). Test-side reads: `shared.runtime` ×7, `shared.store` ×5, `shared.clone()` ×4, `shared.thread_authority` ×1. The `thread_authority` read (`:2983`) and the `RegistryShared` at `:2944` belong to the thread-update forwarder test `resumed_context_window_uses_resumed_model_and_metadata_service` (`:2900-3010`), which calls `prepare_thread_updates` and `spawn_thread_update_forwarder` | grep + read |
| Other `RegistryShared::new*` callers in tests: `registry.rs` ×3 (`:3195`, `:3728`, `:3763`), `driver.rs` ×2 (`:1292`, `:1727`). Their signatures do not change | grep |
| `ThreadRuntimeSupport::with_max_command_output_bytes(usize)` `thread_runtime.rs:691`; `ThreadMetadataService::new(store, hub)` `thread_metadata.rs:42`; `LedgerHandle` is `Clone` (`ledger.rs:37-38`); `ThreadRuntimeSupport` and `ThreadMetadataService` are `pub(crate)` | read |
| `lib.rs` module list `:1-22`; no `services` module or `Services` type exists anywhere in `src` | grep |
| Unit tests: `event_forwarder.rs` 44, `registry.rs` 29, `driver.rs` 49, `hub.rs` 11; integration 226 | grep |

## Design

### D1. `crate::services::Services`

New file `crates/giskard-server/src/services.rs`, declared `mod services;` in `lib.rs`:

```rust
/// The process-wide services a thread's event owner needs, independent of the registry that
/// spawned it: where outbound messages go, where runtime state lives, where turns and metadata
/// persist, where token usage is tallied.
pub(crate) struct Services {
    pub(crate) hub: Arc<Hub>,
    pub(crate) runtime: Arc<ThreadRuntimeSupport>,
    pub(crate) store: Arc<PersistStore>,
    pub(crate) thread_metadata: Arc<ThreadMetadataService>,
    pub(crate) ledger: LedgerHandle,
}

impl Services {
    pub(crate) fn new(hub: Arc<Hub>, store: Arc<PersistStore>, ledger: LedgerHandle, max_command_output_bytes: usize) -> Self;
        // thread_metadata = ThreadMetadataService::new(store.clone(), hub.clone());
        // runtime = ThreadRuntimeSupport::with_max_command_output_bytes(max_command_output_bytes)
    #[cfg(test)]
    pub(crate) fn for_test(hub: Arc<Hub>, store: Arc<PersistStore>, ledger: LedgerHandle) -> Self;
        // new(.., RetentionConfig::DEFAULT_MAX_COMMAND_OUTPUT_BYTES)
    /// Publish the current cross-thread overview. Moved from `registry::publish_runtime_overview`.
    pub(crate) async fn publish_runtime_overview(&self);
}
```

The construction order and expressions are those of `RegistryShared::new_with_max_command_output_bytes`
today, moved verbatim.

### D2. `RegistryShared` owns one `Arc<Services>`

```rust
struct RegistryShared {
    projects: Arc<Mutex<ProjectIndex>>,
    harness_transitions: Arc<HarnessTransitions>,
    threads: Arc<Mutex<ThreadIndex>>,
    background_tasks: Arc<RegistryTaskTracker>,
    driver_events: Arc<dyn DriverEventSink>,
    services: Arc<Services>,
}
```

The three `RegistryShared` constructors keep their signatures; `new_with_max_command_output_bytes`
becomes `services: Arc::new(Services::new(hub, store, ledger, max_command_output_bytes))` plus the
five registry fields. Every `shared.<service>` read in `registry.rs` and `admission.rs` becomes
`shared.services.<service>`; `publish_runtime_overview(&self.shared)` becomes
`self.shared.services.publish_runtime_overview()`; the free function at `:1668-1673` is deleted.

### D3. The forwarder holds `Arc<Services>`

```rust
pub(super) struct ThreadEventForwarder {
    services: Arc<Services>,
    // the other fifteen fields unchanged
}

impl ThreadEventForwarder {
    pub(super) async fn new(services: Arc<Services>, authority, coordinator, harness, stream, cancel, intents, driver) -> Self
}
```

Inside the forwarder every `shared` becomes `services`: the field, the parameter, the four reads
in `new` (`:711`, `:724`, `:728`, `:742`), the 24 `self.shared.<service>` reads, and
`publish_runtime_overview(&self.shared)` (`:855`) → `self.services.publish_runtime_overview()`.
The driver passes `self.shared.services.clone()` (`driver.rs:614`, `:622`).

Nothing else in the forwarder changes. The `use super::*;` at `event_forwarder.rs:1` still
brings in the registry types it does need (`ThreadAuthority`, `ThreadCoordinator`, `DriverHandle`,
`TurnIntent`, ...); `Services` reaches it through `registry.rs`'s `use crate::services::Services;`.

### D4. Tests

Forwarder tests that only drive a forwarder build `Services::for_test(hub, store, ledger)` where
they build a `RegistryShared` today (`:2123`, `:3709`, `:5472`, `:5538`), pass `services.clone()`
to `ThreadEventForwarder::new` (`:2145`, `:3729`, `:5510`, `:5565`), and read
`services.runtime`/`services.store` where they read `shared.runtime`/`shared.store`. The two
fixture helpers whose tuples carry `Arc<RegistryShared>` (`:2113`, `:2177`) carry `Arc<Services>`.

The thread-update forwarder test (`:2900-3010`) is a registry test that happens to live in this
module; it keeps `RegistryShared::new` (`:2944`), `prepare_thread_updates`,
`spawn_thread_update_forwarder`, and `shared.thread_authority` (`:2983`), and reads
`shared.services.runtime` at `:3000`.

`registry.rs` and `driver.rs` tests change only where they read a service through `shared`:
`shared.runtime` ×1 in `registry.rs` tests and ×3 in `driver.rs` tests become `shared.services.runtime`.

## Every site that changes

| File | Change |
| --- | --- |
| `crates/giskard-server/src/services.rs` | new: D1 |
| `crates/giskard-server/src/lib.rs` | `mod services;` |
| `crates/giskard-server/src/registry.rs` | struct `:281-292`; constructor `:425-447`; `publish_runtime_overview` `:1668-1673` deleted; 21 production reads and 3 `publish_runtime_overview(&self.shared)` callers (D2); `thread_metadata_service` `:632`; `use crate::services::Services;`; 1 test read |
| `crates/giskard-server/src/registry/admission.rs` | 4 reads (`:122`, `:138`, `:178`, `:226`) |
| `crates/giskard-server/src/registry/driver.rs` | `:614-631` passes `services`; 3 test reads |
| `crates/giskard-server/src/registry/event_forwarder.rs` | D3 and D4 |
| `docs/design-straightening-review.md` | mark B3 (step 6) landed |

## Tests

Existing tests are the specification; the counts above stay (44, 29, 49, 11, 226) and no
assertion changes. One test is added in `services.rs`:

- `services_share_one_store_and_hub`: build `Services::for_test`; the metadata service and the
  runtime it created are the ones reachable through the struct, and `publish_runtime_overview`
  delivers the runtime's current overview to a registered client's replacement lane (the same
  observation the hub tests make).

## Order of work

1. `services.rs` with D1 and its test; `mod services;`. `cargo check -p giskard-server`.
2. D2: the struct and constructor; fix the 21 + 4 + 1 reads and the three
   `publish_runtime_overview` callers; delete the free function. `cargo test -p giskard-server --lib registry`.
3. D3: the forwarder and the driver's spawn. `cargo test -p giskard-server --lib registry::event_forwarder`.
4. D4: the tests. `cargo test -p giskard-server`.
5. `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`.

Expected size: about 60 lines added (`services.rs`), about 15 deleted, and roughly 80 one-token
edits.

## Exit checks

Validated on the base tree; baselines given. `prod` prints a file up to its `mod tests` line.

```sh
S=crates/giskard-server/src
prod() { awk '/^mod tests/{exit} {print}' "$1"; }
# 0 → 1 and 0 → 1
grep -c "^mod services;" $S/lib.rs
grep -c "^pub(crate) struct Services" $S/services.rs
# 10 → 6: RegistryShared's fields
sed -n '/^struct RegistryShared {/,/^}/p' $S/registry.rs | grep -c ":"
# 2 → 0 and 35 → 0: the forwarder holds no registry and names no `shared`
prod $S/registry/event_forwarder.rs | grep -c RegistryShared
prod $S/registry/event_forwarder.rs | grep -cw shared
# 9 → 1: only the thread-update forwarder test still builds a RegistryShared in this file
grep -c RegistryShared $S/registry/event_forwarder.rs
# 21 → 0 and 4 → 0: every service read goes through `services`
prod $S/registry.rs | grep -oE "shared\.(hub|runtime|store|thread_metadata|ledger)\b" | wc -l
grep -oE "shared\.(hub|runtime|store|thread_metadata|ledger)\b" $S/registry/admission.rs | wc -l
# 1 → 0 and 4 → 0: the free helper and its callers are gone
grep -c "^async fn publish_runtime_overview(shared: &RegistryShared)" $S/registry.rs
grep -c "publish_runtime_overview(&self.shared)" $S/registry.rs $S/registry/event_forwarder.rs | awk -F: '{s+=$2} END{print s}'
# 44 → 44, 29 → 29, 49 → 49, 11 → 11, 226 → 226; services.rs 0 → 1
grep -cE "^\s*#\[(tokio::)?test" $S/registry/event_forwarder.rs
grep -cE "^\s*#\[(tokio::)?test" $S/registry.rs
grep -cE "^\s*#\[(tokio::)?test" $S/registry/driver.rs
grep -cE "^\s*#\[(tokio::)?test" $S/hub.rs
grep -cE "^\s*#\[(tokio::)?test" crates/giskard-server/tests/*.rs | awk -F: '{s+=$2} END{print s}'
grep -cE "^\s*#\[(tokio::)?test" $S/services.rs
# 0 → 0: nothing outside the server crate
git diff --stat origin/main -- crates/giskard-core crates/giskard-harness crates/giskard-persist crates/giskard-proto crates/giskard-testenv | wc -l
```

## Pitfalls

- `Services::new` must build `thread_metadata` from the same `store` and `hub` it stores; today
  the constructor clones both before moving them into the struct, and a `Services` whose metadata
  service wrote to a different store would persist into the wrong directory.
- Do not add `impl Deref for RegistryShared` or `fn runtime(&self)` accessors to shorten
  `shared.services.runtime`. The extra segment is the information this step exists to expose.
- The forwarder's `use super::*;` means a `Services` import in `registry.rs` is enough; do not add
  a `use crate::services` to the forwarder and then leave the registry's unused.
- Keep the six functions in the ground truth that take `&RegistryShared` or `&Arc<RegistryShared>`
  as they are; each uses registry state, and only `publish_runtime_overview` was services-only.
- `for_test` is `cfg(test)`; the integration tests build servers through `AppState`, never a
  `Services`, so nothing in `giskard-testenv` changes.

## Stop rules

Stop and re-cut if the diff:

- changes any service type, any log line, or any assertion;
- leaves `RegistryShared` or the identifier `shared` in the forwarder's production code;
- adds a `Deref` impl, an accessor that hides `services`, or a second way to construct the
  five handles;
- touches any crate other than `giskard-server`, or `giskard-testenv`.
