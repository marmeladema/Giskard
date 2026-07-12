# AGENTS.md — Giskard

## Project
Giskard: a local-first, single-user web UI for agentic coding CLIs (Codex CLI first).
Built in Rust (Dioxus fullstack + Axum). No npm/Node/JS toolchain.

## Specification
`specs/giskard-specification.md` (v1.39) is the authoritative spec. Read it before making changes.

## Documentation
`README.md` is the practical setup/usage guide and MUST be kept in sync with the code. Update it
(and `config.example.toml`) in the same change whenever you touch config keys/defaults, the
`giskard-admin` commands, HTTP/WS endpoints, the run/quick-start steps, the storage layout, or the
crate list. The spec stays the authoritative design source; the README must never contradict it or
the code.

## Build & Test

```bash
# Build all native crates (Phase 0 — no WASM needed yet)
cargo build

# Run all tests
cargo test

# Lint
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

## Architecture
Cargo workspace with 8 crates under `crates/`:
- `giskard-core` — pure domain types (no I/O)
- `giskard-harness` — `AgentHarness` trait + capabilities
- `giskard-harness-codex` — Codex CLI adapter (Phase 1)
- `giskard-harness-replay` — deterministic replay for tests (Phase 1)
- `giskard-persist` — flat-file storage + `giskard-admin` binary
- `giskard-proto` — shared client↔server wire types
- `giskard-server` — Axum backend (Phase 2)
- `giskard-ui` — Dioxus WASM frontend (Phase 2)

## Conventions
- Edition 2024, MSRV 1.85.
- All Codex-specific types confined to `giskard-harness-codex`.
- Atomic writes for all persistence (temp file + fsync + rename).
- IDs are ULIDs.
- Comments are welcome when they explain intent, invariants, protocol contracts, or non-obvious
  failure handling. Avoid comments that only restate the code.
- Do not use `unwrap`, `expect`, `panic!`, `todo!`, or `unreachable!` in runtime paths unless the
  condition is proven infallible in local context. Prefer returning typed errors, logging, or
  surfacing a structured browser error. Test-only assertions may use panics normally.
- Errors and failures must be visible at the right boundary:
  - browser-action failures should produce a user-visible message over HTTP or WebSocket;
  - server/operator failures should be logged with enough context to diagnose the action, thread,
    project, and underlying error when available;
  - degraded-but-usable flows should surface warnings rather than fail silently.
- Error paths need tests too. When adding or changing a failure mode, add focused coverage for the
  structured error, warning, log-adjacent behavior, or persisted recovery path as appropriate.
- New async, WebSocket, harness, persistence, approval, command/tool, or cross-thread lifecycle
  paths need useful observability at their boundaries. Prefer structured logs with stable fields
  such as `project_id`, `thread_id`, `turn_id`, `action`, `method`, `command_id`, `tool_call_id`,
  and the underlying error source when available.
- Do not silently drop, coalesce, synthesize, or recover from protocol events without logging enough
  context to diagnose why. Expected user/client failures should generally be `debug` or `warn`;
  server invariants, data corruption, lost events, foreign-thread events, and unexpected harness
  failures should be `warn` or `error`.
- When adding a recovery path, timeout, idempotent close, deduplication rule, fallback completion,
  or lifecycle cleanup, add focused tests for the failure path and make sure logs or browser-visible
  errors explain what happened.
