# AGENTS.md — Giskard

## Project
Giskard: a local-first, single-user web UI for agentic coding CLIs (Codex CLI first).
Built in Rust (Dioxus fullstack + Axum). No npm/Node/JS toolchain.

## Specification
`specs/giskard-specification.md` (v1.4) is the authoritative spec. Read it before making changes.

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
- No comments in code unless explicitly requested.
