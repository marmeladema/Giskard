# AGENTS.md — Giskard

## Project
Giskard: a local-first, single-user web UI for agentic coding CLIs (Codex CLI first).
Built in Rust (Axum backend + a hand-authored vanilla HTML/CSS/JS UI embedded in the
`giskard-server` binary). No npm/Node/JS toolchain. The vanilla static UI is the supported frontend
for the foreseeable future; an earlier Dioxus/WASM plan was dropped.

## Specification
`specs/giskard-specification.md` is the authoritative spec. Read it before making changes.

## Documentation
`README.md` is the practical setup/usage guide and MUST be kept in sync with the code. Update it
(and `config.example.toml`) in the same change whenever you touch config keys/defaults, the
`giskard-admin` commands, HTTP/WS endpoints, the run/quick-start steps, the storage layout, or the
crate list. The spec stays the authoritative design source; the README must never contradict it or
the code.

When modifying `giskard-harness-codex`, read
`crates/giskard-harness-codex/README.md` first and keep it synchronized with changes to native
identifier mappings, lifecycle behavior, protocol routing, process control, and restart semantics.

The HTTP/WS endpoint inventory and behavior notes live in `docs/api-endpoints.md`, linked from the
README. Update that file in the same change whenever you add, remove, or change an HTTP or WebSocket
route in `crates/giskard-server/src/routes.rs` (path, method, request/response shape, or documented
behavior) so it never drifts from the code.

Per-thread Git worktrees are documented in `docs/git-worktrees.md`, linked from the README and
referenced from spec §7.1. Keep it synchronized with `crates/giskard-server/src/worktree.rs` —
especially the branch/path naming, the boundary between what a worktree isolates and what it shares
with the project's repository, and what archive, delete and project delete do to a worktree and its
branch. The documentation is the mitigation for this feature's
sharpest edge (work that exists only inside a worktree), so a behavior change that is not reflected
there is not finished.

## Build & Test

```bash
# Build all crates
cargo build

# Run all tests
cargo test

# Lint
cargo fmt --all
cargo clippy --all-targets -- -D warnings

# Browser end-to-end tests (Playwright, in Docker — no host Node/npm needed)
tests/e2e/run.sh
```

For local E2E and screenshot runs, prefer refreshing and supplying a compatible host replay binary
when supported; this avoids compiling Rust in the Docker build:

```bash
cargo build -p giskard-server --bin giskard-server-replay
GISKARD_E2E_PREBUILT_BIN=target/debug/giskard-server-replay tests/e2e/run.sh
```

The script builds the binary in Docker when that path is absent. See `tests/e2e/README.md` for the
host-binary compatibility requirements and screenshot equivalent.

Playwright tests in `tests/e2e/` drive the real UI against `giskard-server-replay` (a bin in
`giskard-server`): a deterministic, Codex-free server with a scripted in-process harness. When you
change the login/project/thread/settings UI or that binary's seeded state, keep those tests and the
`SCRIPTED_REPLY` constant (mirrored in `tests/e2e/tests/helpers.ts`) in sync. See
`tests/e2e/README.md`.

The README's UI screenshots (`docs/screenshots/ide-{desktop,mobile}.png`) are generated from the
same server. Whenever you change the frontend in a way that affects how it looks — anything under
`crates/giskard-server/static/` (`index.html`, `app.css`, `app.js`), the appearance themes, or the
layout — regenerate them with `tests/e2e/screenshots.sh` and commit the updated PNGs in the same
change, so the README never shows a stale UI. (No regeneration needed for changes with no visible
effect, e.g. backend-only or copy-only edits.)

## Architecture
Cargo workspace with 8 crates under `crates/`:
- `giskard-core` — pure domain types (no I/O)
- `giskard-git-parser` — parsers for `git` command output (pure, no I/O)
- `giskard-harness` — `AgentHarness` trait + capabilities
- `giskard-harness-codex` — Codex CLI adapter
- `giskard-harness-replay` — deterministic replay harness for tests
- `giskard-persist` — flat-file storage + `giskard-admin` binary
- `giskard-proto` — shared client↔server wire types
- `giskard-server` — Axum backend + the embedded vanilla static web UI (`static/`)

## Conventions
- Edition 2024, MSRV 1.89 (`std::fs::File::try_lock`). CI runs `@stable` with no
  `rust-toolchain.toml`, so it will not catch a newer API sneaking past this line — raising it is
  always a deliberate act.
- All Codex-specific types confined to `giskard-harness-codex`.
- One `CodexInstance` is the single-task state authority for each Codex app-server process. Its
  transport, mapper, active turns, pending compactions, and pending context restores must remain
  task-owned. Do not share them through `Arc<Mutex<_>>`, `Arc<RwLock<_>>`, or independent
  state-mutating workers; helper futures may borrow this state only through `&mut self`.
- Every production Codex thread route must be established through the `CodexInstance` route
  methods; do not claim or replace mapper identity and publish its event log as separate
  operations. Resume-fallback replacement must require the exact prior native/Giskard binding.
- Atomic writes for all persistence (temp file + fsync + rename).
- The store's per-thread locks are in-process `Mutex`es and order nothing between binaries. Anything
  that rewrites or deletes store files from outside `giskard-server` must hold the advisory
  data-directory lock (`giskard_persist::DataDirLock`, `<data_dir>/.giskard.lock`) and fail rather
  than proceed alongside a running server. Read-only paths and `--dry-run` take no lock and warn
  instead. Never reintroduce a wall-clock heuristic as a stand-in for exclusion.
- Thread storage is split by how a field grows. `threads/<id>/history.jsonl` is a **bounded** index
  — a header line, then one record per turn carrying only strictly bounded or human-scaled fields
  (ids, model, status kind, usage, timestamps, a capped prompt preview, a capped status message,
  attachment descriptors). Anything **agent-driven** (prompt text, provider error text, items,
  diffs, command output) belongs in that turn's payload file,
  `threads/<id>/turns/<turn_id>.jsonl`, which is written atomically. Never add an agent-driven field
  to a turn record: the index staying small no matter what the agent did is the property the split
  exists to create. A turn commits payload first, index last.
- Every on-disk format states its own version in the file it governs (the `history.jsonl` header for
  the layout, each payload header for that turn). Unknown `kind` values are skipped with a warning;
  a newer payload format fails that turn only; a newer history format fails the thread.
- IDs are ULIDs.
- Comments are welcome when they explain intent, invariants, protocol contracts, or non-obvious
  failure handling. Avoid comments that only restate the code.
- `RegistryShared` is the designated process-local entity authority root.
  `RegistryShared::projects` and `RegistryShared::threads` are the only strong process-local owner
  maps for project and thread identity.
- Entity-local state belongs on its authority or an authority-owned component. Do not add a peer
  owning map keyed directly or indirectly by project or thread identity.
- Each root authority owner map must have an adjacent `ENTITY-AUTHORITY-OWNER` comment naming the
  entity whose process-local identity it owns. Every long-lived struct field that is a permanent
  keyed exception must have an adjacent `ENTITY-AUTHORITY-EXCEPTION` comment. Function-body locals
  do not carry these annotations.
- Convenience, reduced plumbing, speculative performance, and compatibility are not valid reasons
  for a keyed authority exception.
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
