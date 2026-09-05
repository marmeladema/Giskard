# S3b — `DriverEventSink` as an injected dependency

Follow-up to [`s3-driver-events.md`](s3-driver-events.md). Written against `main` at `36c42e8`
(S3 merged); every file and line reference below was checked against that tree. Re-check them if
the branch has moved.

## Goal

Make the driver's observation seam an ordinary injected dependency instead of a `cfg(test)`
field. `RegistryShared` holds an `Arc<dyn DriverEventSink>`; the default implementation writes
the log lines S3 moved into `DriverEvent::log`; anything that builds an `AppState` or a registry
may pass another. No cargo feature, no `cfg(test)` on the seam, and no behaviour change in
production: the default sink logs exactly what `observe` logs today.

This is what lets `giskard-testenv` (S4) hand the integration tests a probe, and it is a
prerequisite of S4: S4's `TestServerBuilder` carries the sink through `AppState::new_with_config`.

## Non-goals

- No change to which decisions are reported, to any `DriverEvent` variant or field, or to any
  log line. Emission sites do not move.
- No change to any test's assertions. The driver and registry unit tests change only in how they
  obtain a probe.
- No integration test changes. Those come with S4b, through the testenv.
- No setter. The sink is fixed at construction; there is no `set_driver_event_sink`, because a
  setter needs interior mutability on `RegistryShared` and leaves a window in which decisions
  reach the previous sink.

## Ground truth

| Fact | Where |
| --- | --- |
| `DriverEvent` and its companions `DeferReason`, `RefusedSubject`, `OwnerExitDisposition` are `pub(super)` | `crates/giskard-server/src/registry/driver.rs:37, 77, 87, 104` |
| `DriverEvent::log(&self, project_id: ProjectId)` is private to the impl | `driver.rs:118` |
| `OwnerExited` carries `reason: ForwarderExitReason`, which is `pub(super)` in `event_forwarder.rs` and imported into `registry.rs` at `:58-60`; making `DriverEvent` public requires it public too (E0446 otherwise) | `registry/event_forwarder.rs:637-638`; `registry.rs:58-60` |
| `ProjectEventDriver::observe` logs, then under `#[cfg(test)]` calls `self.shared.driver_events.emit(event)` | `driver.rs:452-456` |
| The test-only `probe` module holds `DriverEventSink(Mutex<Option<UnboundedSender<DriverEvent>>>)` with `observe() -> DriverProbe` and `emit`, and `DriverProbe` with `expect` (2 s) and `drain` | `driver.rs:1018-1085` |
| `RegistryShared.driver_events: driver::probe::DriverEventSink` under `#[cfg(test)]` at `:283-284`, initialised at `:418-419` | `registry.rs` |
| `RegistryShared::new(hub, store, ledger)` is `#[cfg(test)]` (`:396-397`) and delegates to `new_with_max_command_output_bytes(hub, max, store, ledger)` (`:406`) | `registry.rs` |
| `HarnessRegistry::new(factory, hub, store, ledger)` is `#[cfg(test)]` (`:559-560`); `new_with_max_command_output_bytes(factory, hub, max, store, ledger)` is `pub(crate)` (`:573`) and is the only production path, called from `AppState::new_with_config` | `registry.rs`; `app.rs:91` |
| `AppState::new(store, factory, session_key)` (`app.rs:63`) delegates to `new_with_config(store, factory, session_key, viz, retention)` (`:74`); `new_with_config` has two callers, both binaries | `src/bin/giskard-server.rs:304-310`; `src/bin/giskard-server-replay.rs:1108-1114` |
| `AppState::new` has 35 call sites in the integration tests and one in a `routes.rs` unit test (`:2196`); none passes a sink | grep |
| Unit tests that use the probe: driver tests through `setup()` (`driver.rs:1264-1290`, 47 callers of `setup()`) and the helper `observe(&shared)` (`:1292-1294`, 23 callers); registry tests through `discovery_registry` (`registry.rs:2476-2489`, 9 callers) and `registry.shared.driver_events.observe()` at `:2974` and `:3011`; `wait_for_discovery_records(probe, ..)` at `:2524` takes a `DriverProbe` | grep |
| Other `RegistryShared::new` callers in tests: `registry.rs:3199, 3732, 3767`; other `HarnessRegistry::new` test sites: 18 in `registry.rs`. None uses the probe | grep |
| `#[cfg(test)]` above `mod tests`: 5 in `driver.rs` (two `DriverHandle` constructors, `mod tests`, `mod probe`, the `emit` line), 6 in `registry.rs` (`:283`, `:396`, `:418`, `:431`, `:559`, and the `mod tests` attribute) | awk |
| Unit tests: 48 in `driver.rs`, 29 in `registry.rs` | grep |
| `lib.rs` re-exports `app::{AppShutdown, AppState, build_app}` and `registry::{HarnessFactory, HarnessRegistry}` | `lib.rs:27-28` |
| `rust-version = "1.89"` | `Cargo.toml:21` |

## Design

### D1. The trait and the default

In `driver.rs`, replacing nothing yet:

```rust
/// Receives every decision the project event driver reports. One sink serves every project
/// driver in a registry, so `project_id` says which one decided.
pub trait DriverEventSink: Send + Sync + 'static {
    fn observe(&self, project_id: ProjectId, event: &DriverEvent);
}

/// The default sink: each decision becomes its log line (`DriverEvent::log`).
#[derive(Debug, Default, Clone, Copy)]
pub struct LogDriverEventSink;

impl DriverEventSink for LogDriverEventSink {
    fn observe(&self, project_id: ProjectId, event: &DriverEvent) {
        event.log(project_id);
    }
}
```

`DriverEvent`, `DeferReason`, `RefusedSubject`, `OwnerExitDisposition` become `pub`;
`DriverEvent::log` becomes `pub` so a custom sink can keep the log lines and add its own
delivery. `ForwarderExitReason` becomes `pub` (its derive list and variants are unchanged).

Re-exports: `registry.rs` gains
`pub use driver::{DeferReason, DriverEvent, DriverEventSink, LogDriverEventSink, OwnerExitDisposition, RefusedSubject};`
and `pub use event_forwarder::ForwarderExitReason;`; `lib.rs:28` becomes
`pub use registry::{DeferReason, DriverEvent, DriverEventSink, ForwarderExitReason, HarnessFactory, HarnessRegistry, LogDriverEventSink, OwnerExitDisposition, RefusedSubject};`.

The `DriverEvent` doc comment's last sentence (`driver.rs:34-35`, "In production the event becomes
the log line … under `cfg(test)` it is also delivered to the test probe") becomes "It is delivered
to the registry's `DriverEventSink`; the default sink turns it into the log line for that decision
(`DriverEvent::log`)."

### D2. The field and the call

`RegistryShared` (`registry.rs:283-284`): drop the `#[cfg(test)]`; the field becomes
`driver_events: Arc<dyn DriverEventSink>`.

`observe` (`driver.rs:452-456`) becomes:

```rust
fn observe(&self, event: DriverEvent) {
    self.shared.driver_events.observe(self.project_id, &event);
}
```

The `event.log(self.project_id)` call moves into `LogDriverEventSink`; the `#[cfg(test)]` emit
line goes.

### D3. Construction

One new parameter, threaded along the only production path and the test constructors that need
it:

| Constructor | Change |
| --- | --- |
| `RegistryShared::new_with_max_command_output_bytes(hub, max, store, ledger)` `:406` | gains `driver_events: Arc<dyn DriverEventSink>`; initialiser `:418-419` becomes `driver_events` |
| `RegistryShared::new(hub, store, ledger)` `:396-397` (`cfg(test)`) | unchanged signature; passes `Arc::new(LogDriverEventSink)`. Its four test callers do not change |
| `RegistryShared::new_with_driver_events(hub, store, ledger, driver_events)` | **new**, `cfg(test)`; used by the driver tests' `setup()` |
| `HarnessRegistry::new_with_max_command_output_bytes(factory, hub, max, store, ledger)` `:573` | gains `driver_events`, passes it through |
| `HarnessRegistry::new(factory, hub, store, ledger)` `:559-560` (`cfg(test)`) | unchanged signature; passes `Arc::new(LogDriverEventSink)`. Its 18 test callers do not change |
| `HarnessRegistry::new_with_driver_events(factory, hub, store, ledger, driver_events)` | **new**, `cfg(test)`; used by `discovery_registry` |
| `AppState::new_with_config(store, factory, session_key, viz, retention)` `app.rs:74` | gains `driver_events: Arc<dyn DriverEventSink>` as its last parameter, passed to the registry at `:91` |
| `AppState::new(store, factory, session_key)` `app.rs:63` | unchanged signature; passes `None, None, Arc::new(LogDriverEventSink)` |
| `src/bin/giskard-server.rs:304-310`, `src/bin/giskard-server-replay.rs:1108-1114` | append `Arc::new(LogDriverEventSink)` |

`AppState::new` keeping its three parameters is what keeps the 35 integration-test call sites and
`routes.rs:2196` untouched.

### D4. The unit-test probe

The `probe` module (`driver.rs:1018-1085`) stays `#[cfg(test)]` and keeps `DriverProbe` with
`expect` and `drain` unchanged. Its sink type is renamed and becomes an implementation of the
trait:

```rust
#[derive(Default)]
pub(in crate::registry) struct ProbeSink(std::sync::Mutex<Option<mpsc::UnboundedSender<DriverEvent>>>);

impl ProbeSink {
    /// Start observing. Replaces any earlier probe; events emitted before this call are not
    /// replayed, so call it before triggering the behaviour under test.
    pub(in crate::registry) fn probe(&self) -> DriverProbe { /* body of today's observe() */ }
}

impl DriverEventSink for ProbeSink {
    fn observe(&self, project_id: ProjectId, event: &DriverEvent) {
        event.log(project_id);                       // tests keep their log output
        if let Some(sender) = self.0.lock().unwrap().as_ref() {
            let _ = sender.send(event.clone());
        }
    }
}
```

The inherent method is `probe`, not `observe`, so it cannot be confused with the trait method.

Driver tests: `setup()` (`:1264-1290`) builds `let sink = Arc::new(ProbeSink::default());`,
passes `sink.clone()` to `RegistryShared::new_with_driver_events`, and returns it as a sixth
tuple element `Arc<ProbeSink>`. The 47 `let (shared, harness, driver, project_id, store) =
setup();` lines gain `, sink` (`_sink` where unused); the helper `observe(&shared)` (`:1292-1294`)
is deleted and its 23 callers become `sink.probe()`.

Registry tests: `discovery_registry` (`:2476-2489`) builds a `ProbeSink`, passes it to
`HarnessRegistry::new_with_driver_events`, and returns it as a third tuple element; its 9
callers bind it (`_sink` where unused); `:2974` and `:3011` become `sink.probe()`.

## Every site that changes

| File | Change |
| --- | --- |
| `crates/giskard-server/src/registry/driver.rs` | trait + default sink (D1); four enums and `log` to `pub`; doc sentence; `observe` (D2); `probe` module (D4); `setup()` and 70 test lines (D4) |
| `crates/giskard-server/src/registry/event_forwarder.rs:638` | `pub(super) enum ForwarderExitReason` → `pub enum` |
| `crates/giskard-server/src/registry.rs` | field `:283-284`; constructors `:396-419`, `:559-590` (D3); two `pub use` lines; `discovery_registry` and 11 test lines (D4) |
| `crates/giskard-server/src/app.rs:63-91` | D3 |
| `crates/giskard-server/src/bin/giskard-server.rs:304-310`, `src/bin/giskard-server-replay.rs:1108-1114` | one argument each |
| `crates/giskard-server/src/lib.rs:28` | re-export line |
| `docs/design-straightening-review.md` | note under A that the seam is injected |

## Tests

Existing 77 unit tests in the two files are the specification and keep passing with their
assertions untouched. One addition in `driver.rs`'s test module:

- `the_default_sink_is_the_log`: build `RegistryShared::new` (which installs
  `LogDriverEventSink`), spawn a driver, quiesce, attach; the attach reply is the "project is
  being deleted" error and the test completes. This pins that a registry built without a probe
  runs every emission site against the default sink without panicking or blocking; it is the
  path every production registry takes.

## Order of work

1. D1 and the visibility changes; `cargo check -p giskard-server`.
2. D3 with the field ungated and `observe` rewritten (D2); the `probe` module still exists but
   is unused. `cargo check -p giskard-server --all-targets` fails only in the tests that call
   `driver_events.observe()`.
3. D4; `cargo test -p giskard-server --lib registry`.
4. `cargo test -p giskard-server`, `cargo clippy --workspace --all-targets -- -D warnings`,
   `cargo fmt --check`.

Expected size: about 60 lines added in production code, about 90 mechanical test-line edits.

## Exit checks

Validated on the base tree with the baseline for each.

```sh
S=crates/giskard-server/src
# 0 → 1 and 0 → 1
grep -c "^pub trait DriverEventSink" $S/registry/driver.rs
grep -c "^impl DriverEventSink for LogDriverEventSink" $S/registry/driver.rs
# 0 → 4 (pub) and 4 → 0 (pub(super))
grep -cE "^pub enum (DriverEvent|DeferReason|RefusedSubject|OwnerExitDisposition)" $S/registry/driver.rs
grep -cE "^pub\(super\) enum (DriverEvent|DeferReason|RefusedSubject|OwnerExitDisposition)" $S/registry/driver.rs
# 0 → 1
grep -c "^pub enum ForwarderExitReason" $S/registry/event_forwarder.rs
# 2 → 2: the field and initialiser gates are gone; the two required test constructors remain
grep -A1 "#\[cfg(test)\]" $S/registry.rs | grep -c "driver_events"
# 1 → 0: no cfg(test) emit path
grep -c "driver_events.emit" $S/registry/driver.rs
# 0 → 6: both binaries and AppState import and install the default sink
grep -c "LogDriverEventSink" $S/app.rs $S/bin/giskard-server.rs $S/bin/giskard-server-replay.rs | awk -F: '{s+=$2} END{print s}'
# 0 → 2: the trait name is also a substring of LogDriverEventSink after rustfmt splits the re-export
grep -c "DriverEventSink" $S/lib.rs
# 0 → 25: 23 driver-test probes plus the two registry tests
grep -c "\.probe()" $S/registry/driver.rs $S/registry.rs | awk -F: '{s+=$2} END{print s}'
# 5 → 4 in driver.rs (the emit line's gate is gone); 6 → 6 in registry.rs (field and init gates gone, two cfg(test) constructors added)
awk '/^mod tests/{exit} /#\[cfg\(test\)\]/{c++} END{print c}' $S/registry/driver.rs
awk '/^mod tests/{exit} /#\[cfg\(test\)\]/{c++} END{print c}' $S/registry.rs
# 48 → 49 and 29 → 29
grep -cE "^\s*#\[(tokio::)?test" $S/registry/driver.rs
grep -cE "^\s*#\[(tokio::)?test" $S/registry.rs
```

## Pitfalls

- `event.log(project_id)` must move into `LogDriverEventSink::observe`, not be duplicated in
  `ProjectEventDriver::observe`; otherwise a custom sink that also logs prints every line twice.
- `ProbeSink::observe` logs before sending, so the unit tests keep the log output they have today.
- `DriverEvent` is public now, so `ForwarderExitReason` must be public before `cargo check` will
  pass; do the visibility changes together.
- Do not add `set_driver_event_sink`, `Mutex<Arc<dyn …>>`, or `ArcSwap`. Construction is the
  only injection point.
- The 18 `HarnessRegistry::new` and four `RegistryShared::new` test sites must not change; only
  the fixtures that need a probe use the `_with_driver_events` constructors.

## Stop rules

Stop and re-cut if the diff:

- changes a log line, a `DriverEvent` variant, or an emission site;
- adds a cargo feature or any `cfg` around the sink;
- changes `AppState::new`'s signature or any integration test;
- adds a setter for the sink or interior mutability around it;
- touches `giskard-testenv` (it does not exist yet) or `tests/`.
