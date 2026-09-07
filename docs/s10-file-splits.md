# S10 — Two mechanical file splits: `ws.rs` and the Codex adapter's modules

Implementation plan for step 10 of [`design-straightening-review.md`](design-straightening-review.md)
(finding D, first two bullets). Written against `main` at `5d7d234` (S9 merged); every file and
line reference below was checked against that tree. Re-check them if the branch has moved.

The review's third bullet, splitting `app.js`, is deliberately not part of S10 (see **What S10
does not do**). The typed compaction marker previously pencilled in as S9b is now **S11**, where
the broader question of how strongly typed notices and activities should be gets its own
discussion and plan.

## Goal

Two files hold more than one unit each, and both splits are pure moves:

- `crates/giskard-server/src/routes.rs` (6,343 lines) holds the HTTP handlers, a 650-line test
  module in the middle of the file (`:2100-2750`), and the WebSocket session: ticket and upgrade
  handlers with their own `WsError` mapping, `handle_ws`, and the 830-line `handle_client_msg`
  that dispatches 13 client message kinds. The WebSocket session and the helpers only it calls
  move to a sibling module `src/ws.rs`, about 1,820 lines, next to `hub.rs` and `delivery.rs`,
  the delivery half of the same protocol. Helpers that HTTP handlers also call stay in
  `routes.rs` and become `pub(crate)`: eleven items and two struct fields, listed in D1, which
  is the exact HTTP-to-WebSocket coupling made visible.
- `crates/giskard-harness-codex/src/lib.rs` (6,792 lines; 3,150 production, 3,640 test) holds the
  public handle, the `AgentHarness` impl, the turn lifecycle, the per-command handlers, and three
  self-contained groups the review names: the worker-queue watchdog, the JSON-RPC helpers, and
  upload preparation. Those three move to `queue.rs`, `rpc.rs`, `uploads.rs`, about 510 lines,
  with the one pure test that belongs to the watchdog.

No behaviour change: no route, path, wire message, log line, error string, or test assertion
changes. One unit test moves file; none is edited.

## Corrections to the review

1. **`ui.rs` is `tests/ui.rs`.** The review calls the UI substring tests "the `ui.rs` source
   tests"; they are the integration test `crates/giskard-server/tests/ui.rs` (3,511 lines, 29
   tests, about 1,037 `contains` assertions on the served script). S10 does not touch them.
2. **`app.js` is deferred.** See **What S10 does not do**: the split is not a move, and the
   review's stated gain does not materialise without a JavaScript test toolchain.
3. **Sizes.** The Codex `lib.rs` has 3,150 production lines (review: 3,202); the watchdog group is
   227 lines (review: 208). `routes.rs` has 85 log macros and its test module sits mid-file with
   3,600 production lines after it, which is why a split is also a tidy-up of test placement.
4. **Layout.** The review says `ws.rs`, and that is what S10 does: `src/ws.rs`, declared by
   `mod ws;` in the server's `lib.rs`. The alternative, a child module `routes/ws.rs`, would
   let the WebSocket code reach `routes.rs`'s private helpers through `super::` with no
   visibility change, but it would leave `routes/` holding one file, and it would hide which
   helpers the two protocols share. The sibling layout names them: eleven items become
   `pub(crate)` (D1). The Codex crate keeps its flat layout (`mod instance; mod transport; …`
   in `lib.rs`), so its three new modules are flat siblings too.

## Ground truth

### `routes.rs`

| Fact | Where |
| --- | --- |
| 6,343 lines; `#[cfg(test)] mod tests` `:2100-2750`, 23 tests, none referencing a WebSocket item; production resumes at `:2755` | read |
| The router registers the WebSocket endpoints: `.route("/api/ws-ticket", get(ws_ticket))` `:182`, `.route("/api/ws", get(ws_handler))` `:183`; these are the only references to `ws_ticket` / `ws_handler` outside their definitions | grep |
| WebSocket region 1: `ws_ticket` `:3832-3841`, `WsQuery` `:3842-3846`, `ws_handler` `:3847-3876`, `WsError` + `impl WsError` + `impl fmt::Display` `:3877-3983` | read |
| `warning_info` `:3984-4002` is shared: HTTP `:765`, `:996`; WebSocket `:5624`, `:6054` | grep |
| `history_limit_or_default` `:4003-4024` is shared: HTTP `:4413`, WebSocket `:4764` | grep |
| `harness_error_means_command_unmanaged` `:4442-4456` sits among HTTP handlers but is referenced only at `:5422` (WebSocket) | grep |
| WebSocket region 2: `send_activity_bootstrap` `:4457-4473` (ref `:4562`), `handle_ws` `:4474-4662`, `handle_client_msg` `:4663-5492` (16 `ClientMessage::` sites), `ThreadAccess` `:5493-5497`, `ensure_thread_open` `:5498-5653` (refs `:4676`, `:5698`), `find_persisted_thread` `:5654-5692` (refs `:5511`, `:5731`), `project_for` `:5693-5706` (refs `:4847`, `:5324`), `project_for_readonly` `:5707-5751` (7 refs, all `:4680-5468`) | grep |
| Shared block inside region 2: `ReadOnlyProviderContext` `:5752-5758` (HTTP `open_thread` builds one at `:722-731`), `provider_is_known` `:5798-5838` (HTTP `:724`), `harness_knows_provider` `:5839-5885` (only `:5814`, inside `provider_is_known`), `read_only_info` `:5886-5918` (HTTP `:738`) | grep |
| WebSocket-only in region 2: `read_only_provider_context` `:5759-5797` (only `:5925`; it constructs `ReadOnlyProviderContext`, whose fields are private to `routes.rs` and therefore visible to `routes::ws`), `read_only_warning` `:5919-5943` (only `:4689`), `switch_provider_cold` `:5944-6063` (only `:5085`), `ensure_provider_change_allowed` `:6064-6099`, `ensure_send_harness_provider_current` `:6100-6148`, `provider_locked_error` `:6149-6167` (refs `:6092`, `:6141`), `broadcast_running_commands` `:6187-6199` (refs `:5398`, `:5430`, `:5459`), `save_plan` `:6200-6252` (refs `:5468-5482`) | grep |
| `load_thread` `:6168-6186` is shared: 22 references, 8 in the WebSocket ranges | grep |
| `ApiError` `:6253-6343` is `pub` and shared (2 WebSocket references) | grep |
| Constants: `HARNESS_CONTROL_TIMEOUT` `:53` referenced only at `:5207-5409` (inside `handle_client_msg`); `MAX_WS_MESSAGE_BYTES` `:62` referenced only at `:3872-3873` | grep |
| Names the WebSocket ranges use from the rest of the file: `UI_VERSION` `:207` (at `:3838`), `validate_user_attachments` `:1066` (`:4858`), `thread_workspace` `:2832` (`:6209`), `project_model_catalog` `:3399` (`:4887`, `:5032`, `:5537`), `normalize_persisted_thread_model` `:3415` (`:5538`), plus the shared items above | cross-reference script over the ranges |
| The names the WebSocket ranges use from `routes.rs`, complete: `UI_VERSION` `:207`, `validate_user_attachments` `:1066`, `thread_workspace` `:2832`, `project_model_catalog` `:3399`, `normalize_persisted_thread_model` `:3415`, `warning_info` `:3984`, `history_limit_or_default` `:4003`, `ReadOnlyProviderContext` `:5752` (fields `provider`, `configured`), `provider_is_known` `:5798`, `read_only_info` `:5886`, `load_thread` `:6168`, and the already-`pub` `ApiError` `:6254`. The other two hits of the cross-reference script (`open_thread`, `index`) are a `registry.open_thread(..)` method call and a local variable | cross-reference script, then read |
| `lib.rs` declares modules alphabetically (`pub mod hub; pub mod ledger; … mod services; …`, `lib.rs:1-26`) | read |
| Nothing outside the crate names a `routes` item except `app.rs:12` (`http_request_context_middleware`, `protected_routes`, `public_routes`) | grep over `src`, `tests`, `giskard-testenv` |
| The WebSocket behaviour is covered by the integration tests in `crates/giskard-server/tests/` (`approval_reconnect`, `e2e_smoke`, `history_sync`, `interrupt`, `provider_switch`, `read_only_thread`, `running_tasks`, `server_requests`, `turn_controls`, …), which reach it only through `/api/ws` | ls |
| `AGENTS.md:25` names `routes.rs` as where a route's path and method live; that stays true | read |

### Codex `lib.rs`

| Fact | Where |
| --- | --- |
| 6,792 lines; `mod tests` `:3151`; 79 tests; 39 log macros; modules declared `:10-15` (`instance`, `log_fields`, `mapping`, `native_ids`, `native_routes`, `transport`); `instance.rs:1` is `use super::*;`, `transport.rs:1-4` is `use super::{CodexStreamError, CodexTransport, HarnessError, NON_JSON_STDOUT_PREVIEW_BYTES, bounded_utf8_preview};` | read |
| Watchdog group `:291-517`: `WorkerQueueKind`, `WorkerQueueToken`, `WorkerQueueEntrySnapshot`, `WorkerQueueSnapshot`, `WorkerQueueState`, `WorkerQueueWatchdog` + `impl`, `snapshot_queue_token`, `run_worker_queue_watchdog`. Referenced from the rest of `lib.rs`: `WorkerQueueKind` (2), `WorkerQueueToken` (2, in `QueuedHarnessCommand` / `QueuedControlCommand` `:164-175`), `WorkerQueueWatchdog` (2), `run_worker_queue_watchdog` (1); from `instance.rs`: `WorkerQueueWatchdog` and its `mark_started` / `mark_finished` / `close`. Uses `WORKER_QUEUE_WARN_AFTER` (`:66-69`, a `cfg(test)` / `cfg(not(test))` pair, referenced only at `:451`, `:464`, `:468`) | cross-reference script |
| The one pure watchdog test: `worker_queue_snapshot_preserves_operation_identity` `:3187-3205` (uses `WorkerQueueWatchdog::new`, `enqueue`, `mark_started`, `snapshot().active` fields) | read |
| RPC group `:645-738`: `CodexStreamError`, `NON_JSON_STDOUT_PREVIEW_BYTES` `:656`, `bounded_utf8_preview`, `codex_request`, `codex_respond_json`, `codex_respond_error_json`. Referenced from `lib.rs`: `CodexStreamError` (1), `codex_request` (17), `codex_respond_json` (2), `codex_respond_error_json` (3); from `uploads`: `codex_request` (3); from `transport.rs`: the four names in its `use super::{..}`; from `instance.rs`: `CodexStreamError`. Uses `CODEX_JSON_RPC_TIMEOUT` (`:58-61`, also used at `:608`, `:1644`: stays in `lib.rs`), `CodexOperationContext` `:518-614`, `CodexTransport` `:615-644` | cross-reference script |
| Uploads group `:1998-2182`: `PreparedUserInput`, `prepare_user_input_for_codex_uploads`, `cleanup_active_turn_upload`, `cleanup_all_active_turn_uploads`, `cleanup_codex_upload_dir`, `codex_upload_dir`, `codex_upload_path`, `safe_upload_file_name`. Referenced from `lib.rs`: `prepare_user_input_for_codex_uploads` (`:1956`, then `prepared.input` / `prepared.upload_dir` field reads), `cleanup_codex_upload_dir` (3); from `instance.rs`: `cleanup_all_active_turn_uploads` (5), `cleanup_active_turn_upload` (2); from the `lib.rs` tests: `cleanup_active_turn_upload` `:3824`, `cleanup_all_active_turn_uploads` `:3869` (both drive the fake transport, so they stay in `lib.rs`). Uses `CODEX_UPLOAD_DIR_NAME` (`:71`, only `:2147`), `CodexOperationContext`, `CodexTransport`, `ActiveTurns` `:1562`, `codex_request` | cross-reference script |
| A parent's private `use` items resolve from child modules through both `use super::*` and `use super::{..}` under `-D warnings` | scratch crate, checked |
| README code map: `crates/giskard-harness-codex/README.md:631-642` lists `mapping.rs`, `instance.rs`, `lib.rs` | read |
| Review anchors: D `:287-300`, E heads `:302`; row 10 `:327` | grep |

## Design

### D1. `src/ws.rs`

The server's `lib.rs` gains `mod ws;` in alphabetical position (after `pub mod worktree;`, `lib.rs:21`).
`crates/giskard-server/src/ws.rs` holds, verbatim and in today's order:

1. the two constants `HARNESS_CONTROL_TIMEOUT` (`:53`) and `MAX_WS_MESSAGE_BYTES` (`:62`);
2. region 1, `:3832-3983`, with `ws_ticket` and `ws_handler` made `pub(crate)`;
3. `harness_error_means_command_unmanaged` `:4442-4456`;
4. region 2 minus the shared block: `:4457-5751`, then `:5759-5797`, then `:5919-6167`, then
   `:6187-6252`.

`routes.rs` keeps everything else. Exactly these become `pub(crate)` there, and nothing else:
`UI_VERSION`, `validate_user_attachments`, `thread_workspace`, `project_model_catalog`,
`normalize_persisted_thread_model`, `warning_info`, `history_limit_or_default`,
`ReadOnlyProviderContext` and its two fields, `provider_is_known`, `read_only_info`,
`load_thread`. `harness_knows_provider` stays private (only `provider_is_known` calls it).
`ApiError` is already `pub`. The router lines `:182-183` become `get(crate::ws::ws_ticket)` /
`get(crate::ws::ws_handler)`.

`ws.rs` imports explicitly: `use crate::routes::{ApiError, ReadOnlyProviderContext,
history_limit_or_default, load_thread, normalize_persisted_thread_model, project_model_catalog,
provider_is_known, read_only_info, thread_workspace, validate_user_attachments, warning_info,
UI_VERSION};` plus the crate and external imports the moved code uses (`AppState`, `Outbound`,
`giskard_proto::*`, `axum::extract::ws::{..}`, `futures::{SinkExt, StreamExt}`, `tokio::sync`,
`tracing`, …); the compiler's errors under `-D warnings` are the arbiter for that list and for
which `use` lines in `routes.rs` (`:10`, `:23`, `:40` are the candidates) only the moved code
needed.

### D2. `queue.rs`, `rpc.rs`, `uploads.rs`

`lib.rs` declares `mod queue; mod rpc; mod uploads;` in the block at `:10-15`, alphabetically.
Each new module starts with `use super::*;` (as `instance.rs` does) plus what it needs from the
others, and holds its group verbatim:

| Module | Moves | Made `pub(crate)` | Also |
| --- | --- | --- | --- |
| `queue.rs` | `:291-517` and the `WORKER_QUEUE_WARN_AFTER` pair `:66-69` | `WorkerQueueKind`, `WorkerQueueToken`, `WorkerQueueWatchdog` and the methods `lib.rs` / `instance.rs` call (`new`, `enqueue`, `cancel`, `mark_started`, `mark_finished`, `is_closed`, `close`), `run_worker_queue_watchdog` | `#[cfg(test)] mod tests` holding `worker_queue_snapshot_preserves_operation_identity` (`:3187-3205`), moved verbatim |
| `rpc.rs` | `:645-738` | `CodexStreamError`, `NON_JSON_STDOUT_PREVIEW_BYTES`, `bounded_utf8_preview`, `codex_request`, `codex_respond_json`, `codex_respond_error_json` | `transport.rs:1-4` becomes `use super::{CodexTransport, HarnessError};` + `use crate::rpc::{CodexStreamError, NON_JSON_STDOUT_PREVIEW_BYTES, bounded_utf8_preview};` |
| `uploads.rs` | `:1998-2182` and `CODEX_UPLOAD_DIR_NAME` `:71` | `PreparedUserInput` and its fields, `prepare_user_input_for_codex_uploads`, `cleanup_active_turn_upload`, `cleanup_all_active_turn_uploads`, `cleanup_codex_upload_dir` | `instance.rs` adds `use crate::uploads::{cleanup_active_turn_upload, cleanup_all_active_turn_uploads};`; the `lib.rs` test module adds the same `use` for `:3824` and `:3869` |

`lib.rs` imports what its own production code uses:
`use queue::{WorkerQueueKind, WorkerQueueToken, WorkerQueueWatchdog, run_worker_queue_watchdog};`,
`use rpc::{CodexStreamError, codex_request, codex_respond_error_json, codex_respond_json};`,
`use uploads::{cleanup_codex_upload_dir, prepare_user_input_for_codex_uploads};`. Names `lib.rs`
does not use itself (`NON_JSON_STDOUT_PREVIEW_BYTES`, `bounded_utf8_preview`,
`cleanup_active_turn_upload`, `cleanup_all_active_turn_uploads`) are not imported into `lib.rs`
at all, which is why `transport.rs` and `instance.rs` take them from `crate::` directly: an
import used only by a descendant is a warning waiting to happen. `CodexOperationContext`,
`CodexTransport`, `ActiveTurns`, `CODEX_JSON_RPC_TIMEOUT` stay in `lib.rs` with their current
visibility; the new modules see them through `super::`.

`README.md` `:631-642` gains three lines naming `queue.rs` (worker-queue watchdog), `rpc.rs`
(JSON-RPC request and response helpers, stream error), `uploads.rs` (attachment upload
preparation and cleanup), and `lib.rs`'s line drops "low-level JSON-RPC helpers".

### D3. Review doc

Row 10 (`:327`) gains ` — **landed in S10** (`ws.rs`, codex modules; `app.js` deferred)`. A
`**Status: landed in S10**` paragraph after finding D's bullets (`:287-300`, before E at `:302`) names corrections 1–4 and
says `app.js` is deferred with the reason in one sentence. A row 11 is added to the sequencing
table: `| 11 | Typed notices and activities, the compaction marker first | core + adapters | ±150 |`,
marked "plan pending", so the renumbering is recorded where the sequence lives.

## Every site that changes

| File | Change |
| --- | --- |
| `crates/giskard-server/src/lib.rs` | `mod ws;` |
| `crates/giskard-server/src/routes.rs` | `:53`, `:62`, `:3832-3983`, `:4442-4456`, `:4457-5751`, `:5759-5797`, `:5919-6167`, `:6187-6252` removed; `:182-183` qualified; eleven items and two fields `pub(crate)`; imports only the moved code used, removed |
| `crates/giskard-server/src/ws.rs` | new, the removed lines in order, explicit imports at the top, `pub(crate)` on `ws_ticket` and `ws_handler` |
| `crates/giskard-harness-codex/src/lib.rs` | three `mod` lines; `:66-69`, `:71`, `:291-517`, `:645-738`, `:1998-2182`, `:3187-3205` removed; three `use` lines; one `use` in the test module |
| `crates/giskard-harness-codex/src/{queue,rpc,uploads}.rs` | new, the removed lines in order |
| `crates/giskard-harness-codex/src/transport.rs` | `:1-4` import split |
| `crates/giskard-harness-codex/src/instance.rs` | one `use crate::uploads::{..};` line |
| `crates/giskard-harness-codex/README.md` | `:631-642` code map |
| `docs/design-straightening-review.md` | D3 |

Untouched: every other file, in particular `hub.rs`, `app.rs`,
`docs/api-endpoints.md` (no route changes), `AGENTS.md`, the integration tests, `giskard-testenv`,
`mapping.rs`, `native_routes.rs`.

## Tests

No test is edited. One test moves file (`worker_queue_snapshot_preserves_operation_identity`).
`routes.rs` keeps its 23; the Codex `lib.rs` goes from 79 to 78 and `queue.rs` has 1. The
WebSocket session is exercised by the integration tests listed in the ground truth, unchanged.

## Order of work

Each step compiles and passes `cargo test -p <crate>` on its own.

1. Server: create `src/ws.rs`, `mod ws;` in `lib.rs`, move region 1 and the two constants,
   qualify `:182-183`, mark `UI_VERSION` `pub(crate)`. Build, test.
2. Server: move `harness_error_means_command_unmanaged` and region 2 minus the shared block;
   mark the remaining ten items and the two fields `pub(crate)` as the compiler asks for them.
   Build, test. Delete imports the compiler reports unused.
3. Codex: `queue.rs` with its constants and its test. Build, test.
4. Codex: `rpc.rs`; `transport.rs` import split. Build, test.
5. Codex: `uploads.rs`; the `instance.rs` and test-module imports. Build, test.
6. README code map; review doc.
7. `cargo fmt --all`, `cargo clippy --workspace --all-targets --locked -- -D warnings`,
   `cargo test --workspace`.

Two PRs are fine (one per crate); one PR with two commits is also fine. Nothing in one half
depends on the other.

## Exit checks

Run from the repository root. "Before" is `main` at `5d7d234`.

```sh
R=crates/giskard-server/src/routes.rs
W=crates/giskard-server/src/ws.rs
C=crates/giskard-harness-codex/src/lib.rs
WSFN='fn (ws_ticket|ws_handler|send_activity_bootstrap|handle_ws|handle_client_msg|ensure_thread_open|find_persisted_thread|project_for|project_for_readonly|read_only_provider_context|read_only_warning|switch_provider_cold|ensure_provider_change_allowed|ensure_send_harness_provider_current|provider_locked_error|broadcast_running_commands|save_plan|harness_error_means_command_unmanaged)\('
CXNAMES='fn (run_worker_queue_watchdog|snapshot_queue_token|bounded_utf8_preview|codex_request|codex_respond_json|codex_respond_error_json|prepare_user_input_for_codex_uploads|cleanup_active_turn_upload|cleanup_all_active_turn_uploads|cleanup_codex_upload_dir|codex_upload_dir|codex_upload_path|safe_upload_file_name)|struct (WorkerQueueToken|WorkerQueueEntrySnapshot|WorkerQueueSnapshot|WorkerQueueState|WorkerQueueWatchdog|PreparedUserInput)|enum (WorkerQueueKind|CodexStreamError)'
A: rg -c "$WSFN" $R            ;  A2: rg -c "$WSFN" $W
B: rg -c 'ClientMessage::' $R  ;  B2: rg -c 'ClientMessage::' $W
C1: rg -c '^mod ws;' crates/giskard-server/src/lib.rs
C2: rg -c '^pub\(crate\) (async fn|fn|struct|const) (UI_VERSION|validate_user_attachments|thread_workspace|project_model_catalog|normalize_persisted_thread_model|warning_info|history_limit_or_default|ReadOnlyProviderContext|provider_is_known|read_only_info|load_thread)\b' $R
C3: rg -c '^pub\(crate\)' $R
D: rg -c '#\[test\]|#\[tokio::test\]' $R $W
E: rg -c '(debug|info|warn|error)!\(' $R $W | awk -F: '{s+=$2} END{print s}'
F: rg -c 'fn (warning_info|history_limit_or_default|provider_is_known|harness_knows_provider|read_only_info|load_thread)\(|struct ReadOnlyProviderContext' $R
G: rg -c "$CXNAMES" $C          ;  G2: rg -c "$CXNAMES" crates/giskard-harness-codex/src/{queue,rpc,uploads}.rs | awk -F: '{s+=$2} END{print s}'
H: rg -c '^mod (queue|rpc|uploads);' $C
I: rg -c '#\[test\]|#\[tokio::test\]' $C crates/giskard-harness-codex/src/queue.rs
J: rg -c '(debug|info|warn|error)!\(' $C crates/giskard-harness-codex/src/{queue,rpc,uploads}.rs | awk -F: '{s+=$2} END{print s}'
K: git diff main --numstat -- $R $W $C crates/giskard-harness-codex/src/{queue,rpc,uploads}.rs
L: git diff --stat main -- crates/giskard-server/src/hub.rs crates/giskard-server/src/app.rs crates/giskard-server/src/delivery.rs crates/giskard-server/tests crates/giskard-testenv crates/giskard-harness-codex/src/mapping.rs crates/giskard-harness-codex/src/native_routes.rs docs/api-endpoints.md AGENTS.md
M: git diff main | rg -c '^[-+].*#\[allow'
```

| Check | Before | After |
| --- | --- | --- |
| A / A2, the 18 moved route functions | 18 / — | 0 / 18 |
| B / B2, client-message dispatch sites | 16 / — | 0 / 16 |
| C1 | 0 | 1 |
| C2, the eleven shared items | 0 | 11 |
| C3, all `pub(crate)` items in `routes.rs` | (record it) | before + 11; the two fields are indented and not counted here |
| D, tests per file | 23 / — | 23 / 0 |
| E, log macros across the two files | 85 | 85 |
| F, the seven shared helpers still in `routes.rs` | 7 | 7 |
| G / G2, the 22 moved Codex items | 22 / — | 0 / 22 |
| H | 0 | 3 |
| I, tests | 79 / — | 78 / 1 |
| J, log macros across the four Codex files | 39 | 39 |
| K | — | for each pair, lines removed from the old file ≈ lines added to the new file, within 40 (imports, `mod` lines, visibility) |
| L | — | empty |
| M | — | 0 |
| `wc -l` | `routes.rs` 6,343; Codex `lib.rs` 6,792 | `routes.rs` ≈ 4,520, `ws.rs` ≈ 1,830; `lib.rs` ≈ 6,280, `queue.rs` ≈ 250, `rpc.rs` ≈ 100, `uploads.rs` ≈ 195 |
| `cargo clippy --workspace --all-targets --locked -- -D warnings`; `cargo test --workspace` | clean, green | clean, green |

## Pitfalls

- **The shared block sits inside the WebSocket region.** `:5752-5758` and `:5798-5918` stay in
  `routes.rs` even though everything around them moves; the ground truth lists the HTTP callers.
  Moving them would put HTTP-called code in the WebSocket file, which is the cohesion the
  step exists to fix, and would force `pub(crate)` on them for the HTTP side instead.
- **`read_only_provider_context` moves, `ReadOnlyProviderContext` stays.** The function builds
  the struct, so the struct and its two fields become `pub(crate)`; that is the one struct on
  the list.
- **Widen exactly the eleven.** If the compiler asks for a twelfth `pub(crate)` in `routes.rs`,
  the caller map missed a reference: check whether the item is HTTP-shared (then widen it and
  correct this plan) or WebSocket-only (then it should have moved). If it asks for one in
  `ws.rs` beyond `ws_ticket` and `ws_handler`, something in `routes.rs` calls moved code: move
  it back.
- **Constants with `cfg` pairs move as pairs.** `WORKER_QUEUE_WARN_AFTER` is two declarations
  (`:66-69`); both go to `queue.rs`. `CODEX_JSON_RPC_TIMEOUT` stays: `lib.rs` uses it at `:608`
  and `:1644`.
- **Imports used only by descendants are unused imports.** The scratch check shows children can
  resolve a parent's private `use`; it does not make that `use` count as used in the parent.
  `lib.rs` imports only what its own code calls; `transport.rs` and `instance.rs` import from
  `crate::rpc` / `crate::uploads` directly.
- **Two tests in `lib.rs` call moved functions** (`:3824`, `:3869`); they stay where the fake
  transport is and import the functions. Do not move the fake.
- **`items_after_test_module`.** Each new file ends with its test module or has none; `lib.rs`
  keeps its test module last; `routes.rs` keeps its mid-file module exactly where it is (moving
  it is not S10's business).
- **Keep the order.** Moving regions in today's order keeps `git diff -M` and a reviewer's
  side-by-side honest; do not reorder functions "while at it".
- **No `#[allow]`**, no renamed function, no changed signature. If `-D warnings` objects to an
  unused item after the move, the item was already dead code and the compiler is now able to see
  it; report it, do not delete it silently.

## What S10 does not do

- **No `app.js` split.** `app.js` is 10,351 lines of one classic script (`"use strict"`, no
  modules, 659 top-level declarations in one global scope), served at a content-hashed path that
  `build.rs` computes from that single file and that doubles as the UI version for the forced
  reload check. `tests/ui.rs` asserts about 1,037 substrings against the served body. A module
  split is not a move: every implicit cross-reference becomes an explicit import, the build
  script grows per-file hashes with a combined version, `index.html` and the routes change, and
  the substring assertions are re-homed. The gain the review names, "real boundaries to assert
  on", needs a JavaScript unit-test toolchain the project rules exclude; the e2e suite is the
  only behavioural net either way. Revisit when a UI feature forces a region to be touched, or
  when a JavaScript test runner is adopted.
- **No dispatch table for `handle_client_msg`.** Its 830 lines move as one function. Giving it
  the S8 treatment (classify the message, dispatch to per-kind effects) is a later step, easier
  once it lives in its own file.
- **No further Codex modules.** The per-command handlers (`handle_*`, `:2457-3150`) and the
  turn lifecycle (`:1479-1997`) are candidates for a later split; S10 takes the three groups the
  review named.

## Stop rules

Stop and report instead of improvising if:

- the compiler asks for a `pub(crate)` outside the eleven in `routes.rs` or the two in `ws.rs`
  (the caller map is wrong: report which item and who calls it);
- `-D warnings` reports an unused import that no listed rule resolves;
- a test needs an edit beyond the one `use` line in the Codex test module;
- any count in the exit-check table lands elsewhere and the reason is not a plain miscount in
  this document.

## Implementation corrections

Implementation on `c802813` verified four mechanical omissions in the plan. Server steps 1 and 2
must be applied before the first server build because the moved `ws_handler` calls `handle_ws`,
which moves only in step 2. `WsQuery` must be `pub(crate)` because it appears in the
`pub(crate) ws_handler` signature. `WorkerQueueWatchdog::cancel` is called by production code in
`lib.rs`, and `WorkerQueueWatchdog::is_closed` is called by a `lib.rs` test, so both methods must
also be `pub(crate)`. The exit-check `CXNAMES` expression also matches the longer test name
`cleanup_all_active_turn_uploads_removes_every_directory`; there are 21 listed production
definitions, all of which move.
