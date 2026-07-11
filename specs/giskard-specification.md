# Giskard — Technical Specification

> A local-first, single-user web application that provides a modern browser UI on top of
> agentic coding CLIs. The first supported agent harness is OpenAI's **Codex CLI** (via its
> `app-server` JSON-RPC protocol), but the application is designed so the harness is a
> replaceable component. Built entirely in Rust (Dioxus fullstack + Axum), with **no npm,
> Node, or JavaScript toolchain** anywhere in the build.

**Document status:** Implementation-ready specification.
**Audience:** An AI coding agent (and its human reviewer) implementing the system.
**Version:** 1.36

**Changelog (1.35 → 1.36), observability gap closure:**
- **O1:** Turn startup and forwarding emit structured operator logs at the decision points needed to
  diagnose provider/proxy failures: harness `turn/start` acceptance or rejection, turnless harness
  errors before turn ownership, stream failures before completion, and forwarder exit while the
  active-turn gate is still held. Logs include project/thread ids, harness thread id when known,
  model/provider, mode, elapsed time, buffered state, and the underlying harness/Codex error.
- **O2:** HTTP/API errors are logged at the response boundary. Internal failures are `error`,
  conflicts are `warn`, and expected client errors remain `debug` so browser-visible failures are
  not silently returned without any server-side diagnostic trail.
- **O3:** WebSocket subscribe must not silently omit history when loading persisted history fails.
  The client receives a structured error, and the server logs the failure through the WebSocket
  action-error path with the affected thread/action.
- **O4:** Server-side event drops and recovery paths are observable. Slow/closed browser outbound
  queues, same-thread/different-turn harness events, duplicate notice/item suppression, failed
  persisted-history scans used for deduplication, history page-size config fallbacks, corrupt-file
  quarantine failures, and token-ledger load fallbacks emit structured logs with stable ids where
  available.
- **O5:** If the Codex stream closes or fails before any `turn/started` event arrives, Giskard emits
  a browser-visible harness error rather than only logging server-side. If a turn did start, Giskard
  still synthesizes a terminal failed turn so history records the failed attempt.
- **O6:** Browser-only enhancement failures for Markdown rendering and path linkification degrade to
  plain text, but emit console warnings so UI diagnostics can distinguish expected fallback from a
  missing feature.

**Changelog (1.34 → 1.35), authoritative reconnect resync:**
- **RX4:** A browser `Subscribe` response is an authoritative active-thread resync, not an
  append-only delta. The client clears transient browser-rendered transcript state before replaying
  the returned recent history and any live snapshot so failed-turn fallback bubbles, optimistic user
  rows, and stale active-turn flags cannot duplicate or survive a reconnect.
- **RX5:** WebSocket `error` events update persistent connection status but do not directly create
  warning notices. Warning/error notices are reserved for actionable foreground failures such as
  authorization failures, offline state, or abnormal foreground closes; sockets recently
  foregrounded after tab/mobile lifecycle suspension reconnect without toast spam. Once a socket
  successfully opens or receives a message while foregrounded, later failures are treated as normal
  foreground failures again.

**Changelog (1.33 → 1.34), mobile-friendly WebSocket reconnect UX:**
- **RX1:** Browser WebSocket disconnects are treated as a recoverable lifecycle state. The client
  reconnects with bounded exponential backoff, resubscribes to the active thread, and uses the
  existing thread state/history/live-turn/task snapshots to resync.
- **RX2:** Expected mobile/tab-suspension disconnects must not produce repeated error toasts. The
  thread header shows persistent connection state instead, while foreground auth/network failures
  remain visible through throttled warnings/errors.
- **RX3:** User messages are blocked while the WebSocket is reconnecting rather than queued. The
  composer stays editable so users can keep drafting, but no optimistic pending bubble is created
  until the socket is open.

**Changelog (1.32 → 1.33), thread-scoped WebSocket and Codex routing isolation:**
- **WS1:** Browser clients must reject stale messages from a replaced WebSocket connection and must
  ignore any thread-scoped server message whose `thread_id` does not match the currently selected
  thread. This guard applies before rendering or mutating transcript state for `ThreadState`,
  `HistoryPage`, `LiveTurnSnapshot`, `RunningTasks`, `Event`, `ApprovalRequest`, thread-scoped
  `TokenUpdate`, and thread-scoped `Error`.
- **WS2:** Thread-scoped `TokenUpdate` messages include `thread_id` on the wire. The browser only
  renders token ledgers into the active thread usage menu when that `thread_id` matches the active
  thread; project/global token updates must not be rendered as thread totals.
- **WS3:** Event forwarders must verify that each incoming `AgentEvent.thread` matches the
  forwarder's owning `ThreadId` before attaching to a turn, updating live buffers, broadcasting, or
  persisting. Harness stream leakage across native subscriptions must therefore be ignored rather
  than written into the wrong Giskard JSONL history, and logged as an operator-visible error with
  the owner thread, event thread, event kind, and turn id when available.
- **WS4:** The Codex harness must broadcast mapped notifications and server requests to the
  `ThreadId` carried by the mapped `AgentEvent`, not merely to the thread whose stream is currently
  being drained. A `TurnCompleted` only ends the currently drained stream when the completed event
  belongs to that same thread; foreign-thread lifecycle events must not terminate or release another
  thread's forwarder.
- **WS5:** After at least one native Codex thread id is registered, non-empty unknown native
  `threadId` values are unroutable. The harness must reject/drop them with an operator-visible warning
  instead of falling back to the caller's scoped thread. Omitted `threadId` values may still use the
  scoped fallback for global/threadless notifications and requests.
- **WS6:** Reopening an already-open Giskard thread must preserve the existing per-thread harness
  sender/subscriptions. Metadata normalization while opening a thread may update persisted thread
  state, but it must not force a second native open or replace the live sender.

**Changelog (1.31 → 1.32), thread reasoning-effort selector:**
- **RE1:** The thread header shows an `Effort` selector immediately after the model picker when the
  selected model descriptor advertises `supports_reasoning_effort`. The selector offers
  `Model default`, `Minimal`, `Low`, `Medium`, `High`, and `Extra High` for Codex-compatible
  reasoning models, sends the selected value through `SelectModel.model_ref.reasoning_effort`, and
  hides for models that do not support reasoning effort.
- **RE2:** Selecting `Model default` for the current model clears the thread's explicit
  `reasoning_effort`, so the next turn omits the effort parameter. Switching away from and back to
  a reasoning model still restores that model's last explicit effort via the existing per-thread
  `model_efforts` map.

**Changelog (1.30 → 1.31), manual compaction completion hardening:**
- **CC4:** After `thread/compact/start` succeeds, the Codex harness keeps draining app-server
  notifications even when no command is running, so context-compaction notifications/items reach
  the browser and registry. A `Context compacted` marker is terminal only for marker-only
  compactions; once Codex emits `TurnStarted`, Giskard keeps draining until the matching
  `TurnCompleted` event.
- **CC5:** Manual compaction completion is robust to Codex versions that emit only a
  context-compaction item/notification and no normal `turn/completed`. If no `TurnStarted` was
  observed for the manual compaction, the first `Context compacted` activity item is treated as a
  terminal successful compaction turn, persisted as `/compact`, broadcast to the browser, and used
  to release the per-thread turn gate.

**Changelog (1.29 → 1.30), collapsible project sidebar groups:**
- **PC1:** Project groups in the left sidebar / mobile Projects drawer are collapsible so a user can
  hide a project's thread list without leaving the project behind. The collapsed/expanded state is
  a durable browser UI preference stored in local storage, not server state, and the New Thread
  action remains available from the project row.

**Changelog (1.28 → 1.29), per-thread turn gate:**
- **TG1:** Giskard enforces a server-side, per-thread turn gate around normal user turns and manual
  context compaction. The gate is reserved before calling the harness, so it covers the race before
  `TurnStarted` reaches the live buffer. Overlapping `SendInput` or `CompactContext` on the same
  thread is rejected with a structured `thread_turn_active` error; other threads and projects
  remain usable. The gate is released when the owned turn completes, or earlier startup paths fail
  or are cancelled.

**Changelog (1.27 → 1.28), manual context compaction:**
- **CC1:** The context usage menu exposes a `Compact context` action that asks the active harness to
  compact the native thread context. For Codex this maps to app-server `thread/compact/start`, not
  to sending a literal `/compact` user message.
- **CC2:** While a manual compaction request is in flight, the header control is disabled and shows
  `Compacting...`. The control is also disabled while another turn is running. Giskard still relies
  on Codex for automatic near-limit compaction; no threshold warning is required.
- **CC3:** Manual compaction starts the same event-forwarding path as a normal turn so compaction
  activity is visible live and persisted in Giskard history.

**Changelog (1.26 → 1.27), appearance-aware transcript scrollbar:**
- **SB1:** The thread transcript owns a scoped thin scrollbar whose track, thumb, and hover colors
  are part of the active Appearance theme. Other browser scroll containers keep their native
  rendering unless they get their own explicit styling later.

**Changelog (1.25 → 1.26), settings menu and two-column shell:**
- **S1:** The desktop shell no longer reserves a right column for appearance-only content. The
  application layout is a left project/thread sidebar plus the main thread workspace.
- **S2:** Durable client UI preferences, starting with Appearance, live in a `Settings` popover
  opened from an icon button pinned to the bottom of the left sidebar. On mobile the same control is
  reached inside the Projects drawer.

**Changelog (1.24 → 1.25), transcript task grouping:**
- **TG1:** Command execution and tool/MCP call transcript items render inside top-level `Tasks`
  transcript rows. Every task item participates, including singletons. Consecutive task items in
  the same turn merge into the same group; any non-task transcript item or turn boundary closes the
  active group.
- **TG2:** A task group shows a compact chronological task list when expanded. Selecting a compact
  task expands that task's existing command/tool detail row inline inside the selected task card
  itself, and selecting the same task again collapses its detail. Transcript-row task selection
  updates in place without scrolling the thread; header Tasks-menu selection may still scroll to
  the task entry. The task preserves the original item id, lifecycle state, output/input collapse
  state, Stop action, and menu select/scroll behavior.
- **TG2a:** The task-group header is an aggregate control: activating it expands all task details
  in the group, or collapses all details when every task detail is already expanded. It does not
  perform an invisible no-op.
- **TG3:** Task groups remain expanded while tasks are running unless the user manually toggles the
  group. Once every task in a group reaches a terminal state, the group collapses automatically
  unless the user explicitly expanded or collapsed it.

**Changelog (1.23 → 1.24), tasks menu:**
- **TM1:** The thread header includes a `Tasks N` control for commands and tool/MCP calls that are
  still known running. The count is the current running-task snapshot size, and the control changes
  visual state between idle, running, and stop-requested tasks.
- **TM2:** Running-task cards move from the permanent right context panel into the `Tasks` popover.
  Selecting a task still scrolls to/selects the transcript row, and the same Stop action remains
  available from the menu.

**Changelog (1.22 → 1.23), context usage menu:**
- **CU1:** The thread-header context gauge is an interactive `Context` control. Activating it opens a
  popover that shows the current context footprint and the thread's cumulative input/output/total
  token usage. Cumulative tokens remain separate from the gauge source: they are informational totals
  and must not drive the context-occupancy numerator.
- **CU2:** Thread token totals no longer occupy a permanent right-column section. The right context
  panel kept running tasks until v1.24 moved them to the header `Tasks` menu; thread-level token
  details are reached from the header context control.

**Changelog (1.21 → 1.22), tool calls as running tasks:**
- **TK1:** The running-command surface is generalized to **running tasks**. `RunningCommand` →
  `RunningTask` with a `kind` (`command` | `tool`) and a `server` field; the `RunningCommands`
  server message → `RunningTasks { thread_id, tasks }`. The server registry tracks tool/MCP calls
  the same way it tracks commands (name + server, live output, elapsed time), so they appear in the
  same running-task summary. Tool calls carry no `process_id` and do not outlive their turn: a tool
  still running when its turn completes (an interrupted turn) is dropped; commands are kept as
  `after_turn`. Stopping a tool sends `Interrupt { thread_id }` (Codex has no per-call cancel);
  commands still `TerminateCommand` by process id. Tool progress arrives as `Text` item deltas.
- **TK2:** Tool-call transcript rows render input/output like command output: running rows stay
  expanded while small and may auto-collapse once large; completed tool-call input/output is
  collapsed by default regardless of size. The transcript row itself owns the toggle handler, so
  clicking the row (or pressing Enter/Space while focused) expands or collapses tool input/output.
  Tool-call lifecycle status uses the same symbol/wording and row placement as command lifecycle
  status, including best-effort elapsed/terminal duration when the start timestamp is available.

**Changelog (1.20 → 1.21), MCP status surface:**
- **MCP1:** The thread header includes an `MCP` control with a status dot and server count.
  Activating it opens an MCP menu that lists the active project's MCP servers, auth state, tool
  count, resource count, and expandable tool/resource detail. Servers that require OAuth expose an
  authenticate action when the harness supports it. Codex `unsupported` auth state means the
  server does not expose Codex-managed auth, not that the MCP server itself is unusable, so the UI
  presents it as a usable unauthenticated server state. MCP elicitation cards with an empty
  requested schema do not show an empty JSON content editor; accepting them sends empty content.
- **MCP2:** Giskard exposes project-scoped MCP REST endpoints backed by the harness:
  `GET /api/projects/{id}/mcp`, `POST /api/projects/{id}/mcp/reload`, and
  `POST /api/projects/{id}/mcp/oauth-login`. Codex maps these to `mcpServerStatus/list`,
  `config/mcpServer/reload`, and `mcpServer/oauth/login`. Server status is visible first;
  enable/disable is not implemented until the exact Codex config contract is intentionally modeled.

**Changelog (1.19 → 1.20), thread rename lifecycle:**
- **TN1:** The thread list actions menu includes `Rename`. Activating it edits the row title next
  to the `...` menu, not the read-only thread header. Enter saves; Escape/blur cancels. A
  successful rename updates the sidebar row and the header/mobile title when that thread is open.
- **TN2:** Rename calls the harness lifecycle operation first (Codex `thread/name/set`) and updates
  local `ThreadFile.title` only after success. Empty titles are rejected, whitespace is normalized
  to a single line, and native rename failure preserves the old local title.

**Changelog (1.18 → 1.19), thread archive/delete lifecycle:**
- **TD1:** Threads continue to create/resume their native Codex thread eagerly when opened. Giskard
  does not create local-only placeholder threads; accidental threads are handled by explicit
  archive/delete actions.
- **TD2:** The thread list exposes a per-thread `...` actions menu. Active threads offer `Archive`
  and `Delete`; archived threads offer `Unarchive` and `Delete`.
- **TD3:** Archive/unarchive calls the harness first (`thread/archive` / `thread/unarchive` for
  Codex) and only then updates the local thread metadata. Delete calls the harness first
  (`thread/delete` for Codex) and only then removes local metadata/history. Giskard rejects
  archive/delete while a turn or command is active.

**Changelog (1.17 → 1.18), Codex collaboration mode alignment:**
- **CM1:** Giskard Plan/Build mode now maps to both Codex sandbox policy and Codex
  `collaborationMode` on `turn/start`: Plan sends `collaborationMode.mode = "plan"` and Build
  sends `collaborationMode.mode = "default"`. This keeps Codex-only tool availability, including
  `request_user_input` / `item/tool/requestUserInput`, aligned with the visible Giskard mode and
  resets the app-server after a plan turn.
- **CM2:** The Codex harness initializes app-server with `capabilities.experimentalApi = true`,
  matching the current app-server contract needed for experimental interaction APIs such as
  collaboration modes and `request_user_input`.

**Changelog (1.16 → 1.17), thread-scoped approval policy:**
- **AP1:** Approval policy is now a concrete thread setting stored in
  `<thread_id>.json`. Project creation no longer asks for policy, and `project.json` no longer owns
  an effective policy. New threads start with `ask`.
- **AP2:** `SetApprovalPolicy` is thread-scoped: `SetApprovalPolicy { thread_id, policy }` persists
  the selected thread's policy and broadcasts `ThreadState`. Threads in the same project can
  therefore run with different approval policies.

**Changelog (1.15 → 1.16), approval metadata:**
- **A3:** `ApprovalRequest` carries structured, card-facing `metadata` entries in addition to the
  backward-compatible `ApprovalKind` summary. Codex command approvals surface managed-network
  hosts, proposed network/exec policy amendments, and parsed command action paths; file approvals
  surface grant roots and changed paths; permissions approvals surface requested filesystem paths,
  glob/special entries, and network enablement. Path metadata is rendered as plain text unless the
  harness marks it as a validated workspace source file, in which case the browser uses the normal
  source-overlay link controls instead of burying it in an opaque detail string.

**Changelog (1.14 → 1.15), pending Codex server requests:**
- **SR1:** Codex `ServerRequest`s are no longer rejected as the normal unsupported path.
  Command, file, permissions, `execCommandApproval`, and `applyPatchApproval` requests are mapped
  to first-class approvals; all other request methods are surfaced as pending transcript cards and
  wait for an explicit browser response.
- **SR2:** The neutral harness contract includes `respond_server_request`, and the browser sends
  `ServerRequestResponse { result | error }` for non-approval server requests. The Codex adapter
  preserves the original JSON-RPC request id (integer or string) when sending the response.
- **SR3:** The browser has dedicated pending-card handling for `item/tool/call`,
  `item/tool/requestUserInput`, and `mcpServer/elicitation/request`, plus an unknown-method
  fallback that can intentionally return `{}` or a JSON-RPC error.
- **SR4:** Live-turn snapshots include unresolved server requests as well as approvals, so reloads
  and reconnects do not lose browser-side work while Codex is waiting.

**Changelog (1.13 → 1.14), tool calls, approvals, and Codex parity:**
- **T1:** `ItemStart` carries optional `ToolCallStart` metadata for MCP/dynamic tool calls when
  the harness can provide it (`name`, `input`, optional `server`, `status`, `started_at_ms`).
- **T2:** The browser renders started tool calls as visible pending transcript rows immediately,
  including their server/tool name, status, and input. Progress deltas append to that row, and the
  later `ItemCompleted` finalizes it in place. A stuck MCP call therefore remains visible instead
  of looking like an idle active turn.
- **Q1:** Plan-mode Codex turns send `readOnly { networkAccess: true }`; Build-mode Codex turns
  send `workspaceWrite { networkAccess: true }`. Network reads are available in Plan mode, while
  agents remain responsible for avoiding mutating network actions during planning.
- **Q2 (superseded by SR1/SR2 in v1.15):** Every Codex `ServerRequest` must receive a JSON-RPC
  response. The temporary v1.14 unsupported-request rejection path has been replaced by pending
  server-request cards and explicit browser responses.
- **A1:** Browser clients render live approval requests as actionable transcript cards, not
  transient notices. The card sends `ApprovalDecision` messages for command, file, and permissions
  approvals and is de-duplicated across live-turn snapshots.
- **A2:** Codex `item/permissions/requestApproval` is a supported approval request. Accept replies
  with `{ permissions, scope: "turn" }`, accept-for-session replies with
  `{ permissions, scope: "session" }`, and decline/cancel use JSON-RPC errors, matching CodexUI's
  app-server contract.

**Changelog (1.12 → 1.13), rendered agent Markdown:**
- **M1:** Agent and reasoning messages are GitHub-flavored Markdown. The server renders them to
  sanitized HTML via `POST /api/projects/{id}/render`; the browser injects the returned HTML.
  Rendering happens when the `ItemCompleted` message is finalized (not per delta); the raw text is
  shown until the render resolves, so streaming stays readable and a failed request degrades to
  plain text.
- **M2:** Rendering is a superset of the `/linkify` pass: detected workspace paths become the same
  `.path-link` controls, wrapped inline during rendering. Paths inside code spans/fenced code
  blocks are left literal. `/linkify` remains for command output (which is not Markdown).
- **M3:** The renderer is the trust boundary. It escapes all text, never passes through raw HTML in
  the source (it is escaped to inert text), and only emits `href`s with an `http`/`https`/`mailto`
  scheme; images are not fetched (alt text is shown). Output is safe to inject as trusted HTML.

**Changelog (1.11 → 1.12), live-turn interruption and running commands:**
- **I1:** The browser exposes a Stop control while a turn is live and sends
  `Interrupt { thread_id }`; the control is disabled while the interrupt request is in flight.
- **I2:** Harness adapters must be able to process interrupt/control commands while a turn is
  streaming, not only while waiting for an approval request. Normal queued user messages remain a
  separate policy decision.
- **R1:** Command execution items are surfaced as transcript rows with live output, elapsed time,
  lifecycle status, and a Stop control when the harness supplies a process id.
- **R2:** The UI includes a running-command summary. Selecting a summary row scrolls to and selects
  the matching transcript command row.
- **R3:** The server maintains a running-command registry from command start/output/completion
  events, separate from the live-turn buffer, and broadcasts running-task snapshots on subscribe
  and after registry changes. The current wire message is `RunningTasks` (generalized in TK1).
- **R4:** Harness adapters that can observe command lifecycle notifications after `TurnCompleted`
  continue draining them while commands are known running. Late terminal command completions update
  the running-command registry and may be broadcast to connected clients without mutating persisted
  completed turn history.
- **R5:** Command rows and summaries distinguish running, succeeded, failed, and
  terminated/declined/interrupted states with both a fixed symbol and subtle state color.
- **R6:** `TerminateCommand { thread_id, process_id }` is a request to the active harness. Giskard
  must not terminate local processes directly. For Codex agent `CommandExecution` items, the
  Codex harness maps stop requests to `turn/interrupt { threadId, turnId }` using the native Codex
  turn id that owns the command process; it must not use `command/exec/terminate` for these items.
- **R7:** A command marked `terminating` means "stop requested through the harness", not "process
  terminated". The browser labels this state as "stop requested" and preserves the later terminal
  command status reported by Codex. If Codex reports normal successful completion after a stop
  request, the command row shows the successful completion annotated with "stop requested" and the
  server logs a structured warning.
- **R8:** Stop-request failures are surfaced through the normal structured `Error` path and the
  command remains visible with `terminating: false`. Codex's "no active command/exec for process
  id" response is treated as stale-state cleanup only for commands already marked `after_turn`.

**Changelog (1.10 → 1.11), source target positioning:**
- **L7:** Opening a source link with a target line centers that line in the code overlay viewport
  when possible, instead of pinning it near the top, so surrounding context remains visible.

**Changelog (1.9 → 1.10), colon source line targets:**
- **L6:** Path linkification accepts `path:<line>` and compiler-style `path:<line>:<column>` in
  addition to `path#<line>`. The column is kept in the clickable span but the overlay targets the
  line.

**Changelog (1.8 → 1.9), source overlay line targets:**
- **L4:** Code overlay previews render a left-side line-number gutter for text files.
- **L5:** Path linkification recognizes line-target references such as `path#<line>`. The server
  validates `path` exactly like a normal link, returns the normalized path plus an optional target
  line, and the UI opens the overlay scrolled to that line.

**Changelog (1.7 → 1.8), code overlay implementation slice:**
- **L1:** Wired the existing Phase 4 highlight/linkify/raw-file backend into the served browser UI:
  completed agent/reasoning text and command output are linkified through the server endpoint, and
  clicked paths open a code overlay with server-side `syntect` HTML plus a download action (§11.2).
- **L2:** Hardened path detection for absolute workspace paths, `./`-prefixed relative paths, and
  sentence/markdown punctuation. The linkifier still validates every candidate by canonicalizing it
  under the workspace root before surfacing a link (§11.2).
- **L3:** Full large-file virtualization remains Phase 4 follow-up work: oversized or binary files
  currently render metadata and a download-only fallback, while the endpoint already accepts line
  ranges for future paginated viewing (§11.3).

**Changelog (1.0 → 1.1), from review:**
- Resolved thread token schema vs §10.2: thread `tokens` now carries `total` + `by_model` (§5.3).
- Config models are now **typed** `[[providers.models]]` entries with a documented metadata
  precedence + conservative fallback (§8.3, App. C), fixing the missing `context_window` /
  `supports_reasoning_effort` source.
- `AgentHarness::shutdown` is now `&self` (object-safe for `Arc<dyn AgentHarness>`); added an
  explicit object-safety note (§4.3).
- Added §4.5 **normative type sketches** for all previously-undefined referenced types (`Item`,
  `FileDiff`, `ApprovalRequest`, `HarnessError`, `OpenThreadOptions`, `ThreadHandle`,
  `TurnStatus`, `UserInput`, etc.).
- Removed `attachments` from `SendInput`; attachments are explicitly out of v1 scope (§13.6, §4.5).
- Defined "session" for `accept_for_session` = harness-process lifetime, fail-closed on respawn (§9.2.1).
- Defined reconnect/live-turn resync via a per-turn in-memory live buffer + snapshot (§13.6).
- Clarified Plan mode × approval policy: orthogonal but read-only makes policy moot; policy value
  preserved (§9.1).
- Made `tokens-global.json` a single-writer **ledger actor** (cross-project hot file) (§5.4).
- Pinned **Dioxus 0.7** and forbade the auto-Tailwind path (no-npm) (§13.1).
- Named candidate Codex context-usage fields + selection order (§10.3).
- Corrected Codex client crate references to real crates: `codex-app-server-sdk` (recommended),
  `codex-codes`, `codex-app-server-protocol` (§3.3, App. A, App. D).

**Changelog (1.1 → 1.2), from D2 investigation against installed Codex CLI 0.142.5:**
- **D2 resolved: `codex-codes` v0.143.0 is the chosen client crate** (async-client feature).
  Verified on crates.io: its `AsyncClient` API maps 1:1 to `AgentHarness`, it tracks Codex CLI
  0.143.0 (≈ installed 0.142.5), includes a schema-drift scorecard for CI (§14.4), and ships real
  JSONL test captures. Reordered §3.3 + App. A to put `codex-codes` first; updated App. D item 2
  from "open" to "resolved". Fallback (`codex-app-server-sdk` v0.5.1 or hand-rolled) only if a
  future CLI version diverges.

**Changelog (1.2 → 1.3), from review (integration pass over v1.2):**
- **B1:** Added the normative `Turn` type sketch (§4.5); `Thread.turns` persists `Vec<Turn>` (§5.3).
- **B2:** Split the Giskard-owned item id from the harness-native id: `ItemId(Ulid)` + a separate
  `harness_item_id: String` field on `Item`/`ItemStart` (§4.5). Applies the thread-id pattern to items,
  so persistence, the diff viewer, and the code overlay no longer depend on Codex item-id stability
  across resume.
- **B3:** Documented that the single `TokenUsage { input, output, total }` struct is reused for both
  per-turn usage and cumulative ledger/`by_model` sums — no parallel `TokenTotals` type (§4.5, §10.2).
- **B4:** Required `CodexHarness` to maintain an explicit `harness_thread_id ↔ ThreadId` map,
  populated at `open_thread` and used to translate inbound notifications, including the resume case
  where the native id is re-established (§4.7).
- **B5:** Renamed the `ItemStarted` **struct** to `ItemStart` to remove the collision with the
  `AgentEvent::ItemStarted` **variant** (§4.4, §4.5).
- **C1 (most important):** Resolved the core-vs-proto ownership contradiction at the WASM boundary.
  `giskard-core` stays native/authoritative; `giskard-proto` owns the wire vocabulary and defines
  `Wire*` mirror types for every payload that carries a `PathBuf` (paths become `String` via a
  server-side lossy conversion); path-free domain types are re-exported through `giskard-proto`. The
  server maps `core → wire` at the outbound boundary. `giskard-ui` depends only on `giskard-proto`
  (§3.2, §3.5, §13.6).
- **C2:** `ApprovalDecision` is path-free (its `AcceptWithExecPolicyAmendment { amendment: Vec<String> }`
  round-trips as JSON), so it is re-exported through `giskard-proto` rather than mirrored — consistent
  with the C1 decision (§3.5, §9.2).
- **C3:** The per-model token breakdown is stored as a **nested object** (`by_model[provider][model]`),
  not an interpolated `"provider/model"` string key, because provider/model ids can contain slashes
  (e.g. `@cf/z-ai/glm-4.7`) and would be ambiguous to re-split (§5.3, §10.2).
- **C4:** Thread `context_window` is a **cache**, not a source of truth: it is derived from the current
  model's descriptor and recomputed from `current_model` on load, so a corrected config value is
  honored (§5.3, §8.4, §10.3).
- **C5:** Defined the resume-failure policy: if resume-by-id fails (Codex thread store purged/rotated),
  start a fresh native thread, keep the Giskard-side history, and warn the user that agent context was
  lost (§4.7, §7.1).
- **S1:** Corrected the §4.6 mapping table — the `initialize`/`initialized` handshake happens once per
  process (per project), not per thread; `thread/start` maps to `open_thread`.
- **S2:** Removed `TurnStatusKind::Declined` (no producer; the pinned Codex `TurnStatus` is
  `Completed | Interrupted | Failed | InProgress`) (§4.5).
- **S3:** Renamed `HarnessError::Timed` → `HarnessError::Timeout` (§4.5).
- **S4:** Aligned `Effort` to the pinned Codex `ModelReasoningEffort`
  (`minimal | low | medium | high | xhigh`) instead of hardcoding three values (§4.5, §8.5).
- **S5:** Documented that project ordering defaults to ULID creation order; the `projects.json`
  `order` field is reserved for a future manual/drag reorder and is not yet surfaced by the UI (§5.3).

**Changelog (1.3 → 1.4), from review (Phase 3 contract hardening):**
- **P1:** Removed the effort double-home: `TurnOverrides.reasoning_effort` is dropped — effort
  lives only in `ModelRef.reasoning_effort` (§8.1). `TurnOverrides` is now a **resolved snapshot**
  (not a delta): the server builds it at `start_turn` from the thread's current mode, current model
  (which carries effort), and effective approval policy. `TurnOverrides.model = None` means "reuse
  the thread's current model." `TurnOverrides.approval_policy` remains in the struct but is now the
  policy snapshot (read from durable state or coerced), not a per-turn override — see P3/AP1
  (§7.5).
- **P2:** `SwitchMode` and `SelectModel` now **persist immediately** and echo state: the new
  mode/current_model is written to `<thread_id>.json` before the server returns, then a
  `ThreadState` is broadcast to all connected tabs so they stay in sync. The sandbox/model effect
  still takes hold at the next turn; only the stored intent is now durable (§7.4, §13.6).
- **P3 (superseded by v1.17 AP1/AP2):** Downgraded the "overridable per turn"
  approval-policy claim. `TurnOverrides.approval_policy` is no longer a per-turn override — it is
  the policy snapshot the server reads from durable state and includes so the harness can pass it to
  `turn/start` (§9.1, §13.6).
- **P4:** The plan-dump write path (§7.4.1) now explicitly cross-references §6.2's path-confinement:
  the resolved path is canonicalized and anything escaping the workspace root is rejected before
  writing.
- **C6:** Confirmed "current plan" = strictly the single most recent Plan-mode turn; no
  concatenation of earlier plan turns, even when they held content the user might expect (§7.4.1).
- **C7:** Per-model effort retention: switching away from a reasoning model preserves its effort
  value; switching back restores it. The effort param is never sent when the active model doesn't
  support it (§8.5 already handles the send-side) (§8.4).
- **C8:** Policy coercion for degraded harnesses: on harness attach, if the harness lacks
  `live_approvals` and the stored policy is `ask`, the effective policy is coerced to `read_only`
  for that session without overwriting the stored value, and a notice is surfaced (§9.4).
- **S6:** Approval diff preview in Phase 3 uses the **raw diff string** from the harness; structured
  `FileDiff` parsing is deferred to Phase 4 (§9.2, §15).
- **S7:** When `plan_build_modes = false`, `Mode` resolves to the Build-equivalent (workspace-write)
  single mode, so `TurnOverrides` is well-defined for every harness (§7.5, §13.5).

**Changelog (1.4 → 1.5), from usability/debugging pass:**
- **E1:** Added structured, flattened server errors with stable `code`, `severity`, `message`,
  optional `detail`, `thread_id`, and `action`, and required WebSocket parse/handler failures to be
  sent to the browser and logged without panicking (§13.6).
- **E2:** Added degraded-open warnings: `ThreadHandle.warning` / `OpenThreadResponse.warning`
  surface non-fatal resume/attach failures while keeping the persisted Giskard thread usable (§4.5,
  §13.6).
- **E3:** Defined persisted-thread reopen semantics: opening or subscribing to an existing thread
  reattaches the harness using the stored native thread id and preserves the durable Giskard
  `ThreadId`; if native resume fails, Giskard starts a fresh native session and warns (§4.5, §7.1).
- **E4:** Added a short-lived signed WebSocket ticket endpoint for browser clients that cannot rely
  on the session cookie during upgrade; `/api/ws` accepts either the session cookie or the ticket
  (§12.1, §13.6).
- **E5:** Required model refs loaded from projects/threads to be normalized against configured
  providers when a stale provider id names a model that exists under exactly one configured provider;
  unsupported reasoning effort is cleared during normalization (§8.3, §8.4).
- **E6:** Required live UI rendering to de-duplicate completed items by Giskard `ItemId` and
  harness-native `harness_item_id`, so streamed deltas finalize in place instead of duplicating the
  completed agent response (§13.6).

**Changelog (1.6 → 1.7), split thread persistence into metadata + JSONL history:**
- **Motivation:** history previously lived inside `<thread_id>.json` as a `turns[]` array, rewritten
  in full on every turn — so listing/restoring parsed whole histories and per-turn write cost was
  O(history). The `.jsonl` (formerly "disposable") is now the **authoritative** history and the
  `.json` a small metadata/aggregates file.
- **H1:** Two files. `<thread_id>.json` = metadata only (version, id, project_id, title,
  harness_thread_id, mode, current_model, context_window cache, token aggregates, timestamps — no
  `turns[]`). `<thread_id>.jsonl` = authoritative history, **one `Turn` per line**, append-only
  (§5.2, §5.3, §5.4).
- **H2:** Append path is a single `write()` of `JSON + "\n"` to an `O_APPEND` file — atomic against
  concurrent writers and process-kill on local POSIX (no app lock for append ordering); the loader
  tolerates a torn final line (skips it) for the power-loss case. NFS/network storage is out of
  scope (§1.2 local-first).
- **H3:** Append history first, then update metadata aggregates. Aggregates are a recomputable
  cache (like `context_window`, C4); `recompute_aggregates(thread)` folds the JSONL to repair after a
  crash between the two writes.
- **H4/H6:** Restore/list read only `.json` (no history parse). Opening a thread loads the last N
  turns; older pages load on demand via `LoadHistory { thread_id, before: TurnId, limit }` →
  `HistoryPage { thread_id, turns: [WireTurn], has_more }`, decoupled from the `ThreadState`
  snapshot (§13.6). Page sizes are config (`[history] initial`/`page`, §16.3). `TurnId` (ULID) is the
  pagination cursor — no index file.
- **H5:** The loader composes `[last N turns from JSONL] + [live turn from the live buffer]`; the
  in-flight turn is not in the JSONL until `TurnCompleted`.
- **H7:** `giskard-admin`: `compact_thread`/`dump_thread` operate on the `.jsonl`, plus
  `recompute_aggregates`; `validate` parses the JSONL line-by-line and reports the first bad line
  rather than quarantining whole histories (§5.5).

**Changelog (1.5 → 1.6), from typed transcript rendering pass:**
- **E7:** File-change and tool-call items are visible transcript items, not hidden/empty agent
  bubbles. `FileChange` keeps a backward-compatible summary `path`/`change` plus optional
  per-file `changes` and `status`; `ToolCall` preserves server, status, and error metadata
  (§4.5, §13.6).
- **E8:** Added a generic `Activity` item kind/payload for Codex app-server items that are not chat
  text but must still be surfaced, such as web searches, image events, sub-agent activity, context
  compaction, and model reroutes (§4.5).
- **E9:** The browser must replay `LiveTurnSnapshot` accumulated events on subscribe/reconnect and
  track `ItemStarted.kind` so streamed deltas are styled as command, file-change, tool-call,
  reasoning, or activity rows before finalization (§13.6).

---

## Table of Contents

1. [Overview & Goals](#1-overview--goals)
2. [Glossary & Concepts](#2-glossary--concepts)
3. [System Architecture](#3-system-architecture)
4. [The `AgentHarness` Abstraction](#4-the-agentharness-abstraction)
5. [Data Model & Persistence](#5-data-model--persistence)
6. [Project Management](#6-project-management)
7. [Threads & Turns](#7-threads--turns)
8. [Model Selection & Providers](#8-model-selection--providers)
9. [Approvals & Permissions](#9-approvals--permissions)
10. [Token Tracking](#10-token-tracking)
11. [Visualization: Diffs & Code Overlay](#11-visualization-diffs--code-overlay)
12. [Authentication](#12-authentication)
13. [UI / UX](#13-ui--ux)
14. [Testing Strategy](#14-testing-strategy)
15. [Implementation Phases](#15-implementation-phases)
16. [Appendices](#16-appendices)

---

## 1. Overview & Goals

### 1.1 Purpose

Giskard is a web application that lets a single user drive one or more agentic coding CLIs
from a browser, on desktop and mobile, instead of a terminal. It manages multiple projects,
each containing multiple concurrent conversation threads, streams the agent's work in real
time, visualizes file changes and referenced source files, and tracks token usage.

### 1.2 Hard Constraints

These are non-negotiable and shape every downstream decision:

- **Rust everywhere.** Backend and frontend are both Rust. The frontend is compiled to
  WebAssembly via Dioxus. There must be **zero** dependency on npm, Node.js, Yarn, a
  JavaScript bundler, or any JS package manager in the build pipeline. The only acceptable
  JS is small hand-written glue if strictly unavoidable (see §13.7), checked into the repo,
  not fetched from a registry.
- **Local-first.** The application and the agent harness processes run on the same machine.
  Remote execution is explicitly out of scope for v1 but the abstractions must not preclude
  it.
- **Single-user.** One shared password protects the whole app. No user accounts, no roles,
  no multi-tenancy. (The word "permissions" in this document refers to *agent action
  approvals*, never to user roles — see §9.)
- **Harness-agnostic.** Codex is the only harness implemented in v1, but all
  agent-facing logic goes through a trait (`AgentHarness`). Adding another harness (e.g.
  Claude Code) later must not require touching the persistence layer, the UI, or the core
  domain model.
- **Everything is tested.** Unit tests for pure logic, integration tests driven by
  **deterministic recorded replays** of agent sessions (no live LLM calls in CI), and a
  small headless-browser end-to-end suite to guard against UI regressions. Testability
  (a mockable harness transport) is a design input from day one, not an afterthought.

### 1.3 Non-Goals (v1)

- Remote / multi-machine harness execution.
- Multiple end users, role-based access control, or per-user data isolation.
- Git integration for the diff viewer (staging, committing, branch ops). The diff view is
  read-only visualization of agent-produced changes.
- Accepting/rejecting individual diffs (visualization only).
- A second harness implementation (the *abstraction* is in scope; a working Claude Code
  adapter is not).

### 1.4 Target Scale

Mono-user, roughly **up to ~10 concurrently active threads**. The design should be simple
and correct at this scale rather than optimized for high concurrency.

### 1.5 Naming

The project is **Giskard** (after R. Giskard Reventlov, the orchestrating robot of Asimov's
Robot series). The Cargo workspace uses `giskard-*` crate names throughout (see §3.2).

---

## 2. Glossary & Concepts

| Term | Definition |
|------|------------|
| **Harness** | An underlying agentic coding CLI that does the actual model interaction and tool execution. v1: Codex CLI. Abstracted behind the `AgentHarness` trait. |
| **Project** | A working context bound to exactly one filesystem **directory**. Holds metadata, configuration, and a set of threads. Backed by one harness process instance. |
| **Workspace root** | The directory the agent is allowed to read/write within (the harness sandbox boundary). Defaults to the project directory; overridable per project. |
| **Thread** | A durable conversation within a project (maps to a Codex *Thread*). Contains an ordered sequence of turns. Resumable across restarts. |
| **Turn** | One unit of agent work initiated by a single user input (maps to a Codex *Turn*). Produces a sequence of items and ends with a completion carrying token usage. |
| **Item** | The atomic unit of agent input/output within a turn: a user message, an agent message, a reasoning note, a command execution, a file change, an approval request, a diff. Has a lifecycle: `started` → optional `delta`s → `completed`. |
| **Mode** | A thread-level state: **Plan** (read-only; the agent analyzes and proposes) or **Build** (read-write; the agent implements). Switchable within a thread (§7.4). |
| **Approval** | A server-initiated request from the harness asking the user to allow or deny a command execution or file change. Handled per the thread's approval policy (§9). |
| **AgentEvent** | Giskard's internal, harness-neutral representation of everything streamed from a harness. Codex protocol messages are mapped into `AgentEvent`s. |
| **Replay** | A recorded sequence of harness transport messages, played back through a mock harness for deterministic testing (§14). |

### 2.1 Conceptual hierarchy

```
Config (global)
└── Project (1 directory, 1 harness process)
    ├── ProjectConfig (workspace root, default model, harness kind, …)
    └── Thread (durable conversation)
        ├── ThreadState (mode, current model, approval policy, token totals, context window)
        └── Turn (one user input → agent work)
            └── Item (message / reasoning / command / file-change / diff / approval)
```

---

## 3. System Architecture

### 3.1 High-level component diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                          Browser (WASM)                                │
│  Dioxus frontend  ── single multiplexed WebSocket ──┐                  │
│  (desktop + mobile responsive UI)                    │                 │
└──────────────────────────────────────────────────────┼────────────────┘
                                                        │  WS frames
                                                        │  (client↔server
                                                        │   protocol, §13.6)
┌───────────────────────────────────────────────────────▼────────────────┐
│                       giskard-server (Axum)                             │
│                                                                         │
│  ┌───────────┐   ┌──────────────┐   ┌───────────────┐  ┌─────────────┐  │
│  │ Auth /    │   │ WS hub        │   │ Domain / app  │  │ Persistence │  │
│  │ session   │   │ (fan-out)     │◄─►│ services      │◄►│ (flat files)│  │
│  └───────────┘   └──────┬───────┘   └───────┬───────┘  └─────────────┘  │
│                         │                    │                          │
│                  ┌──────▼────────────────────▼──────┐                   │
│                  │        AgentHarness trait         │                  │
│                  │  (harness-neutral event stream)   │                  │
│                  └──────┬───────────────────┬────────┘                  │
│                         │                    │                          │
│              ┌──────────▼───────┐   ┌────────▼──────────┐               │
│              │ CodexHarness     │   │ ReplayHarness     │  (tests only) │
│              │ (JSON-RPC client)│   │ (recorded fixture)│               │
│              └──────────┬───────┘   └───────────────────┘               │
└─────────────────────────┼───────────────────────────────────────────────┘
                          │  JSON-RPC 2.0 over stdio (newline-delimited)
                          │  (one app-server process per project)
                ┌─────────▼──────────┐
                │  codex app-server  │  ── OpenAI / provider API ──►  LLM
                │  (child process)   │  ── sandboxed FS / shell   ──►  project dir
                └────────────────────┘
```

### 3.2 Cargo workspace layout

A single Cargo workspace with focused crates. Names are prefixed `giskard-`.

| Crate | Responsibility |
|-------|----------------|
| `giskard-core` | Harness-neutral domain types: `Project`, `Thread`, `Turn`, `Item`, `AgentEvent`, `UserInput`, `Mode`, `ModelRef`, `TokenUsage`, IDs, error types. No I/O. Pure, fully unit-testable. |
| `giskard-harness` | The `AgentHarness` trait + `HarnessCapabilities`. Defines the neutral contract only. |
| `giskard-harness-codex` | `CodexHarness`: spawns/manages `codex app-server`, speaks JSON-RPC, maps Codex protocol ⇄ `giskard-core` types. |
| `giskard-harness-replay` | `ReplayHarness`: reads a recorded transcript, emits the same `AgentEvent` stream deterministically. Used by integration tests and for a "demo mode". |
| `giskard-persist` | Flat-file persistence: load/save projects, threads, token ledgers; atomic writes; a small maintenance/debug API (list/inspect/delete). |
| `giskard-server` | Axum app: routes, auth/session, WebSocket hub, application services orchestrating harness + persistence, syntax highlighting, filesystem browser. |
| `giskard-ui` | Dioxus frontend (compiled to WASM). Components, client-side state, WS client. |
| `giskard-proto` | Shared client↔server **wire vocabulary** (serde), used by both `giskard-server` and `giskard-ui` so the wire protocol is defined once. Owns `Wire*` mirror types for any payload that carries a `PathBuf` (§3.5) and re-exports the path-free `giskard-core` domain types. This is the **only** crate `giskard-ui` depends on. |

> Dioxus "fullstack" can colocate server and client in one crate, but splitting `giskard-ui`
> (client) from `giskard-server` (backend) with a shared `giskard-proto` crate keeps the
> harness/persistence layers free of any WASM-target constraints and makes the backend
> independently testable. The implementer may merge `giskard-ui` into a fullstack crate if
> Dioxus tooling makes the split awkward, provided `giskard-proto`, `giskard-core`,
> `giskard-harness*`, and `giskard-persist` remain separate crates.
>
> **`giskard-core` is authoritative and native-facing** (it holds `PathBuf` and `serde_json::Value`
> internally). The browser never consumes `giskard-core` directly; it consumes `giskard-proto`.
> `giskard-proto` re-exports the pure, path-free `giskard-core` types (ids, `ModelRef`, `TokenUsage`,
> `Mode`, `ApprovalPolicy`, `ApprovalDecision`, `Effort`, `TurnStatus`, `DiffHunk`/`DiffLine`,
> `HarnessError`) — these are trivial serde structs that compile to `wasm32` cleanly — and defines
> its own `Wire*` mirrors for the path-bearing streamed tree (§3.5). This keeps `giskard-core` clean
> and its persisted/internal path representation lossless, while the wire representation is UTF-8
> `String` and cross-platform-safe.

### 3.3 Runtime & key dependencies

- **Async runtime:** Tokio.
- **HTTP/WS server:** Axum (Dioxus fullstack integrates with Axum).
- **Frontend:** Dioxus (WASM target), built with the `dx` CLI. No JS toolchain.
- **Serialization:** `serde` + `serde_json`.
- **Syntax highlighting:** `syntect` (server-side; returns highlighted HTML). See §11.
- **Persistence:** flat JSON files (see §5); `tempfile`-style atomic rename for writes.
- **Password hashing:** `argon2` (session password verification, §12).
- **Session cookies:** signed cookies (e.g. `tower-cookies` + an HMAC key), or a signed
  bearer token; see §12.
- **Codex client:** prefer an existing crate over hand-rolling. Verified options on crates.io
  (versions checked against the pinned Codex CLI 0.142.5 at implementation time):
  - **`codex-codes`** (v0.143.0) — **recommended first choice.** Typed Rust SDK for the Codex
    CLI app-server JSON-RPC protocol, tested against Codex CLI 0.143.0 (≈ installed 0.142.5).
    Provides `AsyncClient` (Tokio) with `start()` (process spawn), `thread_start`, `turn_start`
    (accepting `model`, `reasoning_effort`, `sandbox_policy` — mapping onto `TurnOverrides`
    + `ModelRef.reasoning_effort`, P1),
    `next_message()` (streaming `ServerMessage::Notification/Request`), `respond()` (approval
    decisions), and `shutdown()`. Feature flags: `async-client` (Tokio), `types` (WASM-compatible
    serde models only). Includes a **schema coverage scorecard** that validates typed structs
    against `codex app-server generate-json-schema` output — directly usable for the CI
    protocol-drift check (§14.4). Ships real JSONL test captures (useful as `ReplayHarness`
    fixtures, §14.2). Raw `JsonRpcMessage`/`ServerMessage` access preserved for unknown/drifted
    messages. Apache-2.0. Sibling `claude-codes` crate exists (bonus for a future second harness).
    Repository: github.com/meawoppl/rust-code-agent-sdks.
  - **`codex-app-server-sdk`** (v0.5.1) — Tokio SDK for the app-server JSON-RPC over
    stdio/JSONL, with typed v2 request methods, raw-JSON fallback, `spawn_stdio` process
    management, and `resume_thread`. Smaller version offset from the CLI and less recent; evaluate
    as a fallback if `codex-codes` proves insufficient. Repository: github.com/thehumanworks/codex-sdk-rs.
  - **`codex-app-server-protocol`** (v0.63.0) — protocol types only (no client), stale relative to
    Codex 0.142.x. Not recommended unless only types are needed and `codex-codes`' `types` feature
    is somehow unsuitable.

  **Decision: use `codex-codes` with the `async-client` feature.** Its API maps directly onto the
  `AgentHarness` trait; Giskard wraps `next_message()` into a `broadcast::Sender<AgentEvent>` for
  multi-subscriber support and maps `codex-codes` types to `giskard-core` types at the boundary.
  If a future Codex CLI version diverges beyond what `codex-codes` tracks, fall back to a minimal
  hand-rolled JSON-RPC client inside `giskard-harness-codex`. **Either way, all Codex/app-server
  types must be confined to `giskard-harness-codex`** (nothing Codex-specific leaks upward) and the
  raw-JSON/unknown-message fallback preserved so protocol drift degrades gracefully rather than
  panicking.

### 3.4 Data-flow summary

1. Browser authenticates (§12), opens one WebSocket to the server.
2. User selects/creates a project → server ensures a `codex app-server` process exists for
   that project (spawned lazily on first use, see §6.4).
3. User opens a thread and sends input → server issues `turn/start` to the harness.
4. Harness streams JSON-RPC notifications → `CodexHarness` maps them to `AgentEvent`s →
   application service updates in-memory + persisted thread state → WS hub fans the events
   out to the subscribed browser(s) for that thread.
5. Server-initiated approval requests flow the same way in reverse: harness → `AgentEvent`
   (approval requested) → WS → UI prompt → user decision → WS → harness response (§9).
6. On `turn/completed`, token usage is recorded in the ledger (§10) and persisted.

### 3.5 Core-vs-proto ownership at the WASM boundary (decision — resolves C1/C2)

The frontend (WASM) and the backend (native) both need to speak about `AgentEvent`s, `Item`s, diffs,
and approval requests. Two of the core types are hostile to a naïve shared-crate approach:

- `PathBuf` serializes losslessly on the native side but a non-UTF-8 path (legal on Linux) round-trips
  **lossily** through JSON and back, so a shared `PathBuf` on the wire is a latent cross-platform bug.
- `serde_json::Value` is fine in `wasm32` but is an untyped escape hatch.

**Decision.** `giskard-proto` is the single wire vocabulary and the **only** crate `giskard-ui` links:

1. **Path-free domain types stay in `giskard-core` and are re-exported by `giskard-proto`.** IDs,
   `ModelRef`/`Effort`, `TokenUsage`, `Mode`, `ApprovalPolicy`, `ApprovalDecision`, `TurnStatus`,
   `DiffHunk`/`DiffLine`, `FileChangeKind`, `HarnessError`. They contain no `PathBuf`, so there is no
   lossiness and no reason to duplicate them.
2. **Path-bearing streamed types are mirrored in `giskard-proto` as `Wire*` types with `String`
   paths.** Concretely: `WireAgentEvent`, `WireItem`, `WireItemPayload`, `WireFileDiff`,
   `WireApprovalRequest`, `WireApprovalKind`. `serde_json::Value` payloads (`ToolCall`) stay `Value`
   (wasm-safe).
3. **The server maps `core → wire` at the outbound edge** (the WS fan-out and the live-turn snapshot),
   performing the lossy `PathBuf → String` conversion **once, server-side**, with
   `Path::to_string_lossy()`. Inbound client messages are already path-free (`SendInput` is text;
   `SavePlan` carries a `String` path validated server-side against the workspace root).

**C2 corollary.** `ApprovalDecision` — including `AcceptWithExecPolicyAmendment { amendment: Vec<String> }`
— is path-free and round-trips as JSON, so it is re-exported (case 1), not mirrored. It travels
client→server in `ClientMessage::ApprovalDecision` and server→client inside `WireApprovalRequest`.


---

## 4. The `AgentHarness` Abstraction

This is the keystone of the "harness-agnostic" requirement. Everything above this layer
(domain services, persistence, UI) speaks only in `giskard-core` types.

### 4.1 Design principles

- **Capabilities are negotiated, not assumed.** Different harnesses support different
  features. A harness advertises what it can do via `HarnessCapabilities`; the UI adapts
  (e.g. hides the live-approval prompt if the active harness cannot push approval requests).
- **The internal event model is a superset shaped by, but not identical to, Codex.** Codex's
  Thread/Turn/Item model is well designed and maps cleanly onto Giskard's model. A weaker
  harness (e.g. Claude Code's `stream-json`) maps onto a subset and reports reduced
  capabilities.
- **The transport is mockable.** `AgentHarness` is a trait; `ReplayHarness` implements it
  from a recorded transcript. No integration test ever spawns a real LLM call.

### 4.2 Capabilities

```rust
pub struct HarnessCapabilities {
    /// Server-initiated, per-action approval requests (accept/decline while a turn is live).
    pub live_approvals: bool,
    /// Distinct read-only (plan) vs read-write (build) sandbox modes switchable per turn.
    pub plan_build_modes: bool,
    /// Per-turn model override (change model between turns of one thread).
    pub per_turn_model: bool,
    /// Reasoning-effort control (medium/high/xhigh, model-dependent).
    pub reasoning_effort: bool,
    /// Structured, per-file diff stream (for the side-by-side viewer).
    pub structured_diffs: bool,
    /// Durable thread resume across process/app restarts.
    pub resumable_threads: bool,
    /// A queryable model list (e.g. GET /v1/models via the provider).
    pub model_listing: bool,
    /// Token usage reported on turn completion.
    pub token_usage: bool,
    /// MCP server status can be listed through the harness.
    pub mcp_status: bool,
    /// MCP server config can be reloaded through the harness.
    pub mcp_reload: bool,
    /// MCP OAuth login can be started through the harness.
    pub mcp_oauth_login: bool,
    /// Manual context compaction can be requested for a thread.
    pub context_compaction: bool,
}
```

Codex advertises all Codex-backed capabilities as `true`, except harness-owned model listing
(`model_listing`) because Giskard currently resolves provider models through its config/provider
metadata (§8.3). A future Claude Code adapter would likely set `live_approvals`,
`structured_diffs`, `mcp_status`, and possibly `plan_build_modes` to `false` or a degraded form,
and the UI reacts accordingly (§13.5).

### 4.3 The trait

```rust
#[async_trait]
pub trait AgentHarness: Send + Sync {
    fn capabilities(&self) -> HarnessCapabilities;

    /// List models available through this harness/provider, if supported.
    async fn list_models(&self) -> Result<Vec<ModelDescriptor>, HarnessError>;

    /// List configured MCP servers and their visible tools/resources.
    async fn list_mcp_servers(&self) -> Result<Vec<McpServerStatus>, HarnessError>;

    /// Reload MCP server configuration.
    async fn reload_mcp_servers(&self) -> Result<(), HarnessError>;

    /// Start an OAuth login flow for one MCP server.
    async fn start_mcp_oauth_login(&self, name: &str) -> Result<McpOauthStart, HarnessError>;

    /// Open (or resume) a thread. `resume` carries a harness-native thread id if resuming.
    async fn open_thread(
        &self,
        opts: OpenThreadOptions,
    ) -> Result<ThreadHandle, HarnessError>;

    /// Start a turn: send user input, applying per-turn overrides (model, mode).
    async fn start_turn(
        &self,
        thread: &ThreadHandle,
        input: UserInput,
        overrides: TurnOverrides,
    ) -> Result<TurnId, HarnessError>;

    /// Subscribe to the stream of neutral events for a thread.
    /// Implemented as a broadcast/mpsc receiver of `AgentEvent`.
    fn subscribe(&self, thread: &ThreadHandle) -> AgentEventStream;

    /// Respond to a pending approval request (no-op error if unsupported).
    async fn respond_approval(
        &self,
        req: ApprovalId,
        decision: ApprovalDecision,
    ) -> Result<(), HarnessError>;

    /// Respond to a pending non-approval server request.
    async fn respond_server_request(
        &self,
        req: ServerRequestId,
        response: ServerRequestResponse,
    ) -> Result<(), HarnessError>;

    /// Interrupt the active turn of a thread.
    async fn interrupt(&self, thread: &ThreadHandle) -> Result<(), HarnessError>;

    /// Ask the harness to compact the thread context.
    async fn compact_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError>;

    /// Rename a durable thread in the underlying harness.
    async fn set_thread_name(
        &self,
        thread: &ThreadHandle,
        name: &str,
    ) -> Result<(), HarnessError>;

    /// Archive or unarchive a durable thread in the underlying harness.
    async fn set_thread_archived(
        &self,
        thread: &ThreadHandle,
        archived: bool,
    ) -> Result<(), HarnessError>;

    /// Delete a durable thread in the underlying harness.
    async fn delete_thread(&self, thread: &ThreadHandle) -> Result<(), HarnessError>;

    /// Cleanly shut down the harness (terminate child process, flush).
    /// Takes `&self` (not `self: Arc<Self>`) so the trait stays object-safe and is
    /// callable through `Arc<dyn AgentHarness>`. Idempotent: implementations perform the
    /// actual teardown once (e.g. behind a `OnceCell`/atomic flag) and treat further calls
    /// as no-ops. The child process is also terminated on `Drop` as a safety net.
    async fn shutdown(&self) -> Result<(), HarnessError>;
}
```

> **Object-safety note.** Every method above is dyn-compatible: `&self` receivers, no
> generic method params, no `Self`-by-value. The whole application holds harnesses as
> `Arc<dyn AgentHarness>`, so this is a hard requirement, not a stylistic one. `#[async_trait]`
> is used to keep `async fn` in the trait object-safe.

`AgentEventStream` is an `impl Stream<Item = AgentEvent>` (or a typed wrapper around a
`tokio::sync::broadcast::Receiver`). Multiple subscribers per thread are supported (e.g. two
browser tabs).

### 4.4 The neutral event model (`AgentEvent`)

```rust
pub enum AgentEvent {
    ThreadOpened { thread: ThreadId, harness_thread_id: String },
    TurnStarted  { thread: ThreadId, turn: TurnId },

    ItemStarted   { thread: ThreadId, turn: TurnId, item: ItemStart },
    ItemDelta     { thread: ThreadId, turn: TurnId, item_id: ItemId, delta: ItemDelta },
    ItemCompleted { thread: ThreadId, turn: TurnId, item: Item },

    /// A structured file diff update (for the diff viewer).
    DiffUpdated { thread: ThreadId, turn: TurnId, diff: FileDiff },

    /// Server-initiated approval request.
    ApprovalRequested { thread: ThreadId, turn: TurnId, request: ApprovalRequest },

    /// Server-initiated non-approval request that needs a browser response.
    ServerRequestReceived { thread: ThreadId, turn: Option<TurnId>, request: ServerRequest },

    /// A pending server request was answered or otherwise resolved.
    ServerRequestResolved { thread: ThreadId, turn: Option<TurnId>, request_id: ServerRequestId },

    TurnCompleted { thread: ThreadId, turn: TurnId, usage: TokenUsage, status: TurnStatus },

    Error { thread: ThreadId, turn: Option<TurnId>, error: HarnessError },
}
```

`ItemStart`/`Item` cover: user message, agent message (with streaming text deltas), reasoning
note, command execution (with output deltas), file change, and MCP/tool calls. `ItemDelta` carries
incremental text or command output, keyed by the Giskard-owned `ItemId` (the `CodexHarness`
translates the harness-native item id to the owned `ItemId` via the map established at `ItemStarted`;
§4.7).

> **Note (B5):** the `ItemStarted` above is an `AgentEvent` **variant**; the payload struct it carries
> is named `ItemStart` (§4.5), not `ItemStarted`, to avoid the name collision.

### 4.5 Supporting types (normative sketches)

These types are referenced by the trait (§4.3) and event model (§4.4). The shapes below are
**normative sketches**: field names and variants are the contract; the implementer may add
fields but must not rename or drop the ones shown, so that persistence (§5), the wire protocol
(`giskard-proto`), and the UI agree. All live in `giskard-core`.

```rust
// ---- IDs (ULID-backed newtypes) ----
pub struct ProjectId(pub Ulid);
pub struct ThreadId(pub Ulid);
pub struct TurnId(pub Ulid);
pub struct ItemId(pub Ulid);         // Giskard-owned item id (B2); the harness-native id
                                     // lives in `harness_item_id` on Item/ItemStart
pub struct ApprovalId(pub String);   // harness-native request id (opaque; short-lived, not persisted)

// ---- Handles / options ----
pub struct ThreadHandle {
    pub thread: ThreadId,
    pub harness_thread_id: String,    // native id used for resume
    pub warning: Option<HarnessNotice>, // non-fatal attach/open warning to surface to the user
}

pub struct HarnessNotice {
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
}

pub struct OpenThreadOptions {
    pub project: ProjectId,
    pub thread: Option<ThreadId>,     // Some(existing id) ⇒ resume/attach to persisted thread
    pub workspace_root: PathBuf,      // effective sandbox root (§6.3)
    pub resume: Option<String>,       // Some(native id) ⇒ resume; None ⇒ fresh thread
    pub initial_model: ModelRef,
}

pub struct TurnStatus {              // outcome of a completed turn
    pub kind: TurnStatusKind,        // Completed | Interrupted | Failed
    pub message: Option<String>,
}
// S2: no `Declined` — the pinned Codex `TurnStatus` is Completed | Interrupted | Failed | InProgress
// (InProgress is not a terminal outcome and maps to no completed-turn kind). Re-add a variant here
// only when a real producer exists (and wire it in §7/§9).
pub enum TurnStatusKind { Completed, Interrupted, Failed }

// ---- Turn (B1) ----
/// One unit of agent work initiated by a single user input. Persisted inside the thread file
/// (§5.3) as an element of `Thread.turns`, and the unit the diff viewer / token gauge read from.
pub struct Turn {
    pub id: TurnId,
    pub user_input: UserInput,
    pub items: Vec<Item>,             // completed items, in order
    pub model: ModelRef,              // model used for this turn (may differ across turns, §8.4)
    pub mode: Mode,                   // plan | build applied to this turn (§7.4)
    pub status: TurnStatus,
    pub usage: TokenUsage,            // per-turn usage (same struct reused in ledgers, B3)
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,   // None while the turn is still live
}

// ---- Items ----
pub struct ItemStart {                // B5: renamed from `ItemStarted` (collides with the event variant)
    pub id: ItemId,                   // Giskard-owned (B2)
    pub harness_item_id: String,      // native id, used to correlate deltas/completion
    pub kind: ItemKind,               // discriminant; payload fills in on completion
    pub command: Option<CommandExecutionStart>, // present for command items when known
    pub tool: Option<ToolCallStart>,   // present for tool-call items when known
}

pub struct CommandExecutionStart {
    pub command: String,
    pub cwd: String,                  // wire-safe display path
    pub status: Option<String>,       // e.g. in_progress
    pub process_id: Option<String>,   // enables terminate when present
    pub started_at_ms: Option<i64>,   // Unix epoch ms when supplied by the harness
}

pub struct ToolCallStart {
    pub name: String,
    pub input: serde_json::Value,
    pub server: Option<String>,
    pub status: Option<String>,       // e.g. in_progress
    pub started_at_ms: Option<i64>,   // Unix epoch ms when supplied by the harness
}

pub enum TaskKind { Command, Tool }   // TK1: a running task is a shell command or a tool/MCP call

pub struct RunningTask {              // TK1: formerly `RunningCommand`; generalized over commands + tools
    pub kind: TaskKind,
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    pub item_id: ItemId,              // transcript row to select/scroll to
    pub harness_item_id: String,
    pub command: String,              // command line (command) or tool name (tool)
    pub cwd: String,                  // wire-safe display path (empty for tools)
    pub server: Option<String>,       // MCP/tool server name when this is a tool call
    pub status: String,               // in_progress / running-like while present
    pub process_id: Option<String>,   // present for commands; None for tools (stop → turn interrupt)
    pub started_at_ms: i64,           // server-observed fallback when harness omits it
    pub output: String,               // bounded output tail for the task menu
    pub after_turn: bool,             // true when the turn ended but the command is still known
    pub terminating: bool,             // true while waiting for a terminal event after terminate
}

pub enum ItemKind {
    UserMessage,
    AgentMessage,
    Reasoning,
    CommandExecution,
    FileChange,
    ToolCall,                          // MCP/other tool invocations
    Activity,                          // non-chat Codex activity surfaced in the transcript
}

/// The finalized item persisted in thread history and sent on `ItemCompleted`.
pub struct Item {
    pub id: ItemId,                   // Giskard-owned (B2): stable across resume, addressable by
                                      // the diff viewer and linked by the code overlay
    pub harness_item_id: String,      // native id (opaque; not relied on for stability)
    pub payload: ItemPayload,
    pub created_at: DateTime<Utc>,
}

pub enum ItemPayload {
    UserMessage    { text: String },
    AgentMessage   { text: String },
    Reasoning      { text: String },
    CommandExecution {
        command: String,
        cwd: PathBuf,
        output: String,               // accumulated stdout+stderr
        exit_code: Option<i32>,
        status: Option<String>,       // completed / failed / in_progress / declined
        process_id: Option<String>,   // retained for UI correlation / terminate affordance
        duration_ms: Option<i64>,     // completed command elapsed time when supplied
    },
    FileChange {
        path: PathBuf,                  // summary/back-compat path
        change: FileChangeKind,         // summary/back-compat change
        changes: Vec<FileChangeEntry>,  // optional per-file details
        status: Option<String>,
    },
    ToolCall {
        name: String,
        input: serde_json::Value,
        output: Option<serde_json::Value>,
        server: Option<String>,
        status: Option<String>,
        error: Option<String>,
    },
    Activity {
        title: String,
        detail: Option<String>,
        metadata: Option<serde_json::Value>,
    },
}
pub struct FileChangeEntry { path: PathBuf, change: FileChangeKind, diff: Option<String> }
pub enum FileChangeKind { Created, Modified, Deleted }

pub enum ItemDelta {
    Text { text: String },            // agent-message / reasoning increment
    CommandOutput { chunk: String },  // command stdout/stderr increment
}

// ---- Diffs (for the side-by-side viewer, §11.1) ----
pub struct FileDiff {
    pub path: PathBuf,
    pub change: FileChangeKind,
    pub old_text: Option<String>,     // None for created files
    pub new_text: Option<String>,     // None for deleted files
    pub hunks: Vec<DiffHunk>,         // precomputed for rendering; may be empty if full-text
    pub binary: bool,
}
pub struct DiffHunk {
    pub old_start: u32, pub old_lines: u32,
    pub new_start: u32, pub new_lines: u32,
    pub lines: Vec<DiffLine>,
}
pub enum DiffLine { Context(String), Added(String), Removed(String) }

// ---- Approvals (§9) ----
pub struct ApprovalRequest {
    pub id: ApprovalId,
    pub kind: ApprovalKind,
    pub reason: Option<String>,
    pub metadata: Vec<ApprovalMetadata>,      // structured host/path/detail rows for the card
    pub available: Vec<ApprovalDecision>,   // decisions the harness will accept
}
pub enum ApprovalKind {
    CommandExecution { command: String, cwd: PathBuf },
    FileChange       { path: PathBuf, change: FileChangeKind },
    Permission       { detail: String },    // network / extra-fs escalation
}
pub enum ApprovalMetadata {
    Text { label: String, value: String },
    Path { label: String, path: PathBuf, source_link: bool },
    Host {
        label: String,
        host: String,
        protocol: Option<String>,
        port: Option<i64>,
        target: Option<String>,
    },
}
pub enum ApprovalDecision {
    Accept,
    AcceptForSession,                        // see §9.2.1 for "session" definition
    Decline,
    Cancel,
    AcceptWithExecPolicyAmendment { amendment: Vec<String> }, // command exec only
}

// ---- Non-approval server requests (§9.2) ----
pub struct ServerRequest {
    pub id: ServerRequestId,
    pub method: String,
    pub params: serde_json::Value,            // original harness method params
    pub received_at: DateTime<Utc>,
}
pub enum ServerRequestResponse {
    Result { value: serde_json::Value },
    Error { code: i64, message: String },
}

// ---- Models & usage ----
pub struct ModelRef {
    pub provider: String,
    pub model: String,
    pub reasoning_effort: Option<Effort>,
}
// S4: mirrors the pinned Codex `ModelReasoningEffort` (verified against codex-codes 0.143.0:
// minimal | low | medium | high | xhigh). Do not hardcode a smaller set; a future harness with a
// different vocabulary should map onto (or extend) this enum at its boundary, and the effort
// selector is only shown when the chosen model advertises `supports_reasoning_effort` (§8.5).
pub enum Effort { Minimal, Low, Medium, High, XHigh }

pub struct ModelDescriptor {
    pub provider: String,
    pub model: String,
    pub context_window: u32,                 // drives the context gauge (§10.3)
    pub supports_reasoning_effort: bool,     // drives effort-selector visibility (§8.5)
    pub display_name: Option<String>,
}

// B3: this ONE struct is reused everywhere usage is expressed — the per-turn usage on `Turn`/
// `TurnCompleted`, and the cumulative sums in the thread/project/global ledgers and their
// `by_model` breakdowns (§10.2). Do not introduce a parallel `TokenTotals` type.
pub struct TokenUsage { pub input: u64, pub output: u64, pub total: u64 }

// ---- User input ----
pub enum UserInput {
    Text { text: String },
    // NOTE: image/file attachments are NOT in v1 scope. If added later, extend here.
}

// ---- Errors ----
pub enum HarnessError {
    Spawn(String),            // failed to start/locate the harness binary
    NotInitialized,           // used before handshake completed
    Unauthenticated,          // harness reports missing/invalid credentials
    Transport(String),        // I/O / framing / connection error
    Protocol(String),         // unexpected/unparseable protocol message
    Overloaded,               // JSON-RPC -32001 after retries exhausted
    Unsupported(String),      // capability not offered by this harness
    ThreadNotFound(ThreadId),
    ThreadBusy { thread: ThreadId },
    Timeout(String),          // operation timed out (S3: renamed from `Timed`)
}
```

> `AgentEventStream` is `impl Stream<Item = AgentEvent> + Send` (concretely a wrapper over a
> `tokio::sync::broadcast::Receiver<AgentEvent>`), supporting multiple subscribers per thread.

### 4.6 Codex mapping (informative)

The `CodexHarness` maps the Codex app-server JSON-RPC protocol onto the above. Key mappings
(protocol details in [Appendix A](#appendix-a-codex-app-server-mapping-reference)):

| Codex app-server | Giskard |
|------------------|---------|
| `initialize` + `initialized` handshake | **once per process** (per project), during process spawn — not per thread (S1) |
| `thread/start`, `thread/resume` | `open_thread` (S1: this is the per-thread call, distinct from the handshake) |
| `turn/start` (with model/effort/sandbox per turn) | `start_turn` + `TurnOverrides` (P1: effort lives in `ModelRef`, not `TurnOverrides`) |
| `item/started`, `item/*/delta`, `item/completed` | `ItemStarted` / `ItemDelta` / `ItemCompleted` |
| `turn/diff/updated` | `DiffUpdated` |
| `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`, `item/permissions/requestApproval` | `ApprovalRequested` |
| `turn/completed` (token usage) | `TurnCompleted` |
| `turn/interrupt` | `interrupt` |
| JSON-RPC error `-32001` "overloaded" | retry with exponential backoff + jitter, surfaced as transient `Error` only if retries exhausted |

Plan vs build maps to the Codex per-turn sandbox policy: **Plan → `readOnly` with
`networkAccess: true`**, **Build → `workspaceWrite` with `networkAccess: true`**. Approval policy
maps to Codex's approval configuration (§9).

### 4.7 Process lifecycle (Codex)

- **One `codex app-server` process per project.** The process hosts all of that project's
  threads (Codex threads are durable containers within a connection). This isolates projects
  from each other, matches Codex's model, and generalizes to future harnesses ("one working
  context = one harness instance"). See also §4.5 for the object-safety constraint this places
  on the trait.
- Transport: **stdio** (newline-delimited JSON-RPC), the stable/production transport. The
  WebSocket transport is not used in v1 (it is for remote, which is out of scope).
- **Lazy spawn:** the process starts on first interaction with the project, not at app boot.
- **Idle shutdown (optional, configurable):** a project's process may be terminated after a
  configurable idle timeout to reclaim memory; threads are resumed on next use via
  `thread/resume`. Default: keep alive while the app runs (given the ~10-thread scale).
- **Crash handling:** if the child exits unexpectedly, the server marks the project's active
  threads as "disconnected", surfaces an `Error` event to the UI, and offers a "reconnect"
  action that respawns and resumes.
- **Native-thread-id registry (B4).** `AgentEvent` is tagged by the Giskard `ThreadId` (a ULID),
  but Codex notifications arrive tagged with the **native** `threadId` string. `CodexHarness` MUST
  maintain an explicit `harness_thread_id → ThreadId` map:
  - populated at `open_thread` (both fresh `thread/start` and `thread/resume`), and
  - consulted when mapping every inbound notification/request to route it to the correct owned
    `ThreadId` (falling back to the handle in scope when a message omits the id).

  This matters especially on **resume**: the native id is re-established (possibly different from the
  previous run), and the map re-binds it to the same durable `ThreadId` so history stays continuous.
  The mapper similarly keeps `harness_item_id → ItemId` (B2) and `harness_turn_id → TurnId` maps,
  established at `ItemStarted`/`TurnStarted`, so streamed deltas and completions resolve to the
  owned ids rather than minting a fresh id per message.
- **Resume-failure fallback (C5).** `thread/resume {threadId}` can fail even though Giskard has the
  stored native id — Codex's own thread store may have been purged or rotated. On a resume-by-id
  failure the harness MUST **not** hard-error the thread: it starts a **fresh** native thread
  (`thread/start`), re-binds the new native id to the existing `ThreadId` in the B4 map, preserves
  the Giskard-side display history (already on disk), and surfaces a non-fatal `Error`/warning event
  so the UI can tell the user "agent context was lost — continuing with a new session; your history
  is intact." This is Phase-1 behavior.
- **Version check:** on spawn, record the Codex CLI version. If it differs from the version
  the protocol mapping was written/tested against, log a warning surfaced in the UI (the
  app-server protocol is versioned and can drift). The implementer should generate and vendor
  the schema via `codex app-server generate-json-schema` for the pinned version and add a CI
  check.


---

## 5. Data Model & Persistence

### 5.1 Requirements recap

- After a backend restart, the app comes back in the **same state**: the list of projects,
  their threads, thread history, mode, selected model, and token ledgers.
- Storage is **flat files** (human-readable, hand-editable, debuggable with `cat`/`jq`),
  unless a technical constraint makes it untenable — in which case SQLite is the documented
  fallback (§5.6), but SQLite is *not* v1's default.
- State corruption must be avoidable: **atomic writes** (write to temp file, `fsync`, rename)
  and a single-writer discipline per file.

### 5.2 On-disk layout

Root directory (XDG): `${XDG_DATA_HOME:-~/.local/share}/giskard/`. Overridable via
`GISKARD_DATA_DIR`.

```
giskard/
├── config.toml                 # global app config (§16.3) — human-authored + app-updated
├── projects.json               # index of projects (id, name, dir, created_at, order)
├── projects/
│   └── <project_id>/
│       ├── project.json        # ProjectConfig: workspace root, default model,
│       │                       #   provider defaults, harness kind
│       ├── threads/
│       │   ├── <thread_id>.json        # thread metadata, approval policy, token cache — no history
│       │   └── <thread_id>.jsonl       # authoritative turn history, one Turn per line (§5.4)
│       └── tokens.json         # per-project token ledger (aggregates + daily buckets)
└── tokens-global.json          # global token ledger (daily/weekly/monthly/total)
```

- **IDs** are ULIDs (sortable, timestamp-prefixed) rendered as strings. Filenames use the ID.
- **`projects.json`** is the small, frequently-read index. Individual project/thread files
  hold the bulk, so no single giant file must be parsed to render the project list.
- **`order` field (S5):** project ordering defaults to **ULID creation order** (ULIDs already sort
  by creation time). The `order` field is **reserved** for a future explicit/drag reorder and is not
  yet surfaced by the UI; until then it is written as the creation index and the list is sorted by
  id. Keep the field (cheap to persist now) rather than migrating the schema later.

### 5.3 Core persisted types (serde JSON)

All defined in `giskard-core`, serialized by `giskard-persist`. Illustrative shapes:

```jsonc
// projects.json
{
  "version": 1,
  "projects": [
    { "id": "01J…", "name": "ostinato-radio", "dir": "/home/elie/dev/ostinato-radio",
      "created_at": "2026-07-06T10:00:00Z", "order": 0 }
  ]
}
```

```jsonc
// projects/<id>/project.json
{
  "version": 1,
  "id": "01J…",
  "name": "ostinato-radio",
  "dir": "/home/elie/dev/ostinato-radio",
  "harness": "codex",
  "workspace_root": null,               // null ⇒ defaults to `dir`
  "default_model": { "provider": "openai", "model": "gpt-5.5", "reasoning_effort": "high" },
  "created_at": "…", "updated_at": "…"
}
```

```jsonc
// projects/<id>/threads/<thread_id>.json
{
  "version": 1,
  "id": "01J…",
  "project_id": "01J…",
  "title": "Fix Qobuz OAuth refresh",
  "harness_thread_id": "th_abc123",     // native id used for resume
  "mode": "build",                       // "plan" | "build"
  "current_model": { "provider": "openai", "model": "gpt-5.5", "reasoning_effort": "high" },
  "context_window": 262144,              // CACHE ONLY (C4): derived from current_model's descriptor;
                                         //   recomputed from current_model on load — not a source of
                                         //   truth. May be omitted; a corrected config value wins.
  "approval_policy": "ask",              // "ask" | "auto" | "read_only" (§9)
  "archived": false,                     // hidden from the active thread group when true
  "model_efforts": {                     // C7: per-model effort retention. Maps "provider/model"
    "openai/gpt-5.5": "high"             //   → stored Effort, so switching back to a reasoning model
  },                                     //   restores the user's last effort choice. Entries are
                                         //   created/updated on SelectModel when the outgoing model
                                         //   supports reasoning_effort.
  "tokens": {
    "total": { "input": 12000, "output": 3400, "total": 15400 },
    "by_model": {                        // nested object (C3): provider → model → usage.
      "openai": {                        //   NOT an interpolated "provider/model" string key, which
        "gpt-5.5": { "input": 12000, "output": 3400, "total": 15400 }   // is ambiguous when the
      }                                  //   model id contains slashes (e.g. "@cf/z-ai/glm-4.7").
    }
  },
  "created_at": "…", "updated_at": "…"
  // NB: no `turns[]` — history is the authoritative `<thread_id>.jsonl`, one `Turn` per line (H1).
}
```

> The thread `tokens` object carries both the aggregate (`total`) **and** the per-model
> breakdown (`by_model`), matching §10.2. A thread accumulates a distinct `by_model[provider][model]`
> entry whenever its model changes mid-thread (§8.4). `context_window` is a cache (C4): on load the
> server resolves the current model's descriptor and recomputes it, so it never goes stale relative
> to config.

```jsonc
// projects/<id>/tokens.json  and  tokens-global.json
{
  "version": 1,
  "total": { "input": 0, "output": 0, "total": 0 },
  "by_day":   { "2026-07-06": { "input": …, "output": …, "total": … } },
  "by_model": { "openai": { "gpt-5.5": { "input": …, "output": …, "total": … } } }  // nested (C3)
}
```
Weekly/monthly aggregates are **derived on read** from `by_day` (no separate storage), so
there is one source of truth to correct if needed.

### 5.4 Write strategy & durability

- **In-memory authoritative state** per running server; disk is the durable mirror.
- Every mutating operation updates memory, then persists the affected file(s) via
  **atomic replace**: write to `<file>.tmp-<rand>`, `fsync`, `rename` over the target.
- **Per-file async mutex** (or an actor owning each file) guarantees single-writer; the
  ~10-thread scale makes contention negligible.
- **`tokens-global.json` is a cross-project hot file** (every `TurnCompleted` in any project
  updates it). It is owned by a **single dedicated ledger actor** (one Tokio task holding the
  in-memory global ledger); all projects send `TokenUsage` deltas to it over an `mpsc`
  channel, and it serializes the atomic writes. This avoids multi-writer races on the one
  shared file without a global lock, and it batches rapid updates (coalesce N deltas arriving
  close together into one atomic write). The same actor owns per-project `tokens.json` writes,
  or those may be delegated to per-project sub-tasks — either is acceptable, but the global
  file must have exactly one writer.
- **Turn history (authoritative JSONL, H1/H2/H3):** a thread's history is `<thread_id>.jsonl`,
  the **source of truth** — one `Turn` per line, append-only. On `TurnCompleted` the server appends
  one line via a single `write()` (JSON + `\n`) to an `O_APPEND` file: on a local POSIX filesystem
  this is atomic against concurrent writers without an application lock, and a process kill leaves
  the line all-or-nothing. It does **not** survive power loss (page cache), so the loader tolerates a
  single unparseable **final** line (torn append) — skipping it — while a bad interior line is real
  corruption. This atomicity holds on local storage only; NFS/network `GISKARD_DATA_DIR` is out of
  scope (§1.2). After appending, the server updates `<thread_id>.json` (token aggregates,
  `updated_at`) — history-first, so a crash between the two leaves the turn recoverable and the
  aggregates rebuildable from the JSONL (`recompute_aggregates`, treating aggregates as a cache like
  `context_window`, C4). The metadata `.json` never holds `turns[]`.
- **Crash consistency:** because writes are atomic renames, a crash leaves either the old or
  the new complete file, never a partial one. On startup the server validates each JSON file;
  a corrupt file is moved aside to `<file>.corrupt-<ts>` and logged, and the app continues
  with the rest (a single bad thread never blocks the whole app).

### 5.5 Debug / maintenance surface

Because the store is plain files, the primary debugging tool is the filesystem itself
(inspect with `jq`, delete a thread by removing its file). In addition, `giskard-persist`
exposes a small maintenance API used by a `giskard-admin` binary (or hidden UI panel):

- `list_projects`, `list_threads(project)` (including active/archived status),
  `dump_thread(id)` (pretty JSON to stdout),
- `delete_thread(id)`, `delete_project(id)` (with confirmation),
- `validate_all` (parse every file, report corruption),
- `compact_thread(id)` (rewrite/prune the jsonl log).

This satisfies the "complete tool to debug and potentially correct the database" requirement
without a SQL console.

### 5.6 SQLite fallback (documented evolution, not v1)

If flat files prove painful (e.g. thread history JSON grows large enough that per-turn
rewrites cause latency, or concurrent aggregation becomes error-prone), migrate to SQLite via
`sqlx` or `rusqlite`. The domain types in `giskard-core` are storage-agnostic and
`giskard-persist` is the only crate that would change. Provide a one-shot importer that reads
the flat-file tree into the SQLite schema, and ship a debug view (e.g. a bundled
`giskard-admin db …` subcommand) so the "inspect and correct the DB" requirement is preserved.
This section exists so the migration path is pre-approved; do **not** implement it in v1.

---

## 6. Project Management

### 6.1 Project creation

Flow: user clicks "New project" → names it → picks a directory via the file browser (§6.2)
→ optionally sets workspace root and default model → confirm.

- The chosen directory may be **empty or existing, git or non-git** — all valid. No git
  requirement, no scaffolding.
- On confirm: create `projects/<id>/`, write `project.json`, add to `projects.json`. The
  harness process is **not** spawned yet (lazy, §6.4).

### 6.2 Filesystem browser / picker

- Backend-driven: the picker browses **the server machine's filesystem** (not the browser
  host). Endpoint returns directory entries (name, is_dir, size, mtime) for a given path.
- **Access scope** is governed by config key `browse.roots` (§16.3):
  - **unset / empty ⇒ full filesystem** is browsable (default).
  - if set to a list of absolute paths ⇒ navigation is **confined** to those subtrees.
- **Security hardening (mandatory even though single-user):** the server canonicalizes every
  requested path (resolve `.`/`..`/symlinks) and, when `browse.roots` is set, rejects any
  path escaping the allowed roots. Never trust a client-supplied path verbatim. Hidden files
  are listed but visually de-emphasized; the picker can filter to directories only when
  choosing a project dir.

### 6.3 Workspace root

- `workspace_root` in `project.json`: **`null` ⇒ equals the project `dir`** (the common
  case). May be set to a subdirectory (narrow the agent's write scope) or a different/wider
  path. This value becomes the harness sandbox boundary passed to Codex.
- The UI shows the effective workspace root and warns if it differs from the project dir.

### 6.4 Harness process management (per project)

- One `codex app-server` per project, spawned lazily (§4.6), reused across the project's
  threads, resumed after idle shutdown or crash.
- The server keeps a registry: `project_id → HarnessInstance` (holding the `Arc<dyn AgentHarness>`,
  child process handle, and per-thread subscriber bookkeeping).
- Deleting a project: shut down its harness, then remove `projects/<id>/` and its
  `projects.json` entry (with a confirm dialog; irreversible).

### 6.5 Multiple projects & threads in parallel

- Projects are independent; their harness processes run concurrently.
- Within a project, multiple threads can be active concurrently (the app-server supports
  concurrent turns across threads). The UI lets the user switch among open threads without
  interrupting their in-flight work; background threads keep streaming and their state keeps
  updating server-side (and is pushed over the shared WebSocket, §13.6).


---

## 7. Threads & Turns

### 7.1 Thread lifecycle

- **Create:** user starts a new thread in a project; server calls `open_thread` (Codex
  `thread/start`), stores the returned `harness_thread_id`, writes `<thread_id>.json`.
- **Open existing:** selecting a persisted thread calls the same open endpoint with
  `thread_id = Some(existing_id)`. The server reattaches the harness using the stored native
  `harness_thread_id` but preserves the durable Giskard `ThreadId`; opening a thread is
  idempotent if it is already attached and its model/provider state is still current.
- **Send input:** user submits a message; server builds a `TurnOverrides` snapshot from the
  thread's persisted state (mode, current model — which carries effort) and calls `start_turn`.
  A turn begins.
- **Stream:** `AgentEvent`s flow to the UI (§13.6) and update persisted state.
- **Complete:** on `TurnCompleted`, token usage is folded into the ledgers (§10) and the
  thread file is rewritten atomically.
- **Resume (after restart):** on startup or first access, `open_thread` with the stored
  `harness_thread_id` (Codex `thread/resume`) rehydrates the native session; Giskard already
  holds the display history from disk. If resume-by-id fails (Codex store purged/rotated), the
  harness falls back to a fresh native thread and warns that agent context was lost, keeping the
  Giskard history intact (C5, §4.7).
- **Interrupt:** user can interrupt an in-flight turn (`turn/interrupt`). The UI exposes this as a
  live-turn Stop control; sending another user message while a turn is still live is a separate
  queueing policy and is not implied by interrupt support.
- **Archive / unarchive:** the thread list exposes an actions menu (`...`) per thread. Archive calls
  the harness lifecycle operation first (Codex `thread/archive`) and marks the local thread
  metadata `archived = true` only after success. Unarchive is the reverse operation (Codex
  `thread/unarchive`, then `archived = false`). Archived threads are listed separately and do not
  restore as the active thread after reload.
- **Delete:** delete calls the harness lifecycle operation first (Codex `thread/delete`) and removes
  local `<thread_id>.json` + `<thread_id>.jsonl` only after success. Delete also drops the in-memory
  Giskard harness handle. Archive/delete are rejected while the thread has an active turn or running
  command; the browser surfaces the failure as an error notice.
- **Rename:** the thread list actions menu exposes `Rename`. It edits the row title next to the
  `...` menu. Saving calls the harness first (Codex `thread/name/set`) and then persists
  `ThreadFile.title`; the browser updates both the row title and the open-thread header/mobile
  breadcrumb after success.

### 7.2 Titles

Auto-generate an initial title from the first user message (truncated); user-editable.
(Optional enhancement: ask the harness to summarize; not required for v1.)

### 7.3 Streaming semantics

- Agent message text arrives as `ItemDelta`s; the UI appends incrementally.
- Command executions stream stdout/stderr as `ItemDelta`s under a command item.
- Command output bodies are collapsible transcript sections. Running command output starts
  expanded while small and may auto-collapse once output is large; completed command output is
  collapsed by default regardless of size. Expanding a command renders the output inline.
- Tool-call input/output bodies follow the same collapse model as command output: running rows
  start expanded while small and may auto-collapse once large; completed tool-call input/output is
  collapsed by default, and expanding the row renders input/output inline. Tool-call status is
  rendered in the same meta position and with the same lifecycle wording as command status.
- Reasoning notes (if the model/effort emits them) render in a collapsible "thinking" block.
- Each item ends with `ItemCompleted` carrying its final, canonical form (this is what gets
  persisted; deltas are transient).

### 7.4 Plan / Build modes

- **Mode is thread state**, persisted, and **switchable at any time within the thread**
  (requires `capabilities.plan_build_modes`).
- **Plan mode** ⇒ harness runs **read-only with network access**; the agent analyzes and proposes
  an implementation plan without modifying files or performing mutating network actions.
- **Build mode** ⇒ harness runs **workspace-write**; the agent implements, subject to the
  approval policy (§9).
- For the Codex harness, this same thread mode also drives Codex's app-server
  `collaborationMode`: Plan sends `plan`, Build sends `default`. This is distinct from sandboxing
  but must stay synchronized because Codex gates some interaction tools, such as
  `request_user_input` / `item/tool/requestUserInput`, on collaboration mode.
- The mode applied to a turn is the thread's mode **at the moment `start_turn` is called**
  (Codex takes sandbox per turn). Switching mode takes effect on the next turn; the UI makes
  this explicit ("Plan mode — next message will be read-only").
- **Durable switch (P2).** `SwitchMode` and `SelectModel` **persist immediately**: the new
  `mode` / `current_model` is written to `<thread_id>.json` before the server acknowledges, then
  a `ThreadState` is broadcast to all connected tabs so they stay in sync. This satisfies the §5
  "same state after restart" requirement — a switch is not lost if the app restarts before the user
  sends the next message. The sandbox/model *effect* still takes hold at the next turn (Codex
  accepts these per `turn/start`); only the stored *intent* is durable now.
- **Switching back and forth** is fully supported (Plan → Build → Plan …).

#### 7.4.1 Plan dump to markdown

- A **"Save plan to project"** button is available while in (or after) Plan mode.
- It writes the current plan as a markdown file **into the project directory**. Default path:
  `docs/plan-<thread-title-slug>-<YYYYMMDD-HHmm>.md` (configurable default in `config.toml`;
  the user may edit the path in a small dialog before saving). If `docs/` doesn't exist it is
  created.
- **What constitutes "the current plan":** the concatenation of the agent-message items of
  the **most recent Plan-mode turn** in the thread (i.e. the latest plan the agent produced),
  rendered to markdown. This is **strictly the single most recent Plan-mode turn** — no
  concatenation of earlier plan turns, even when earlier plan turns held content the user might
  expect (C6). If multiple plan turns exist, the latest wins; the dialog shows a preview so the
  user can confirm. (Rationale: simplest unambiguous rule; avoids trying to detect "the plan"
  heuristically across the whole thread.)
- Writing the plan file is a normal file write within the workspace root; it is **not** gated
  by the agent approval flow (it's a user action, not an agent action), but it respects the
  workspace-root boundary. **Path confinement (P4):** the resolved path is canonicalized and
  anything escaping the workspace root (via `..` or symlink) is rejected before writing, using
  the same hardening specified for the browse endpoint in §6.2. A user-edited path like
  `../../etc/foo.md` must hit this check on write, not just on browse.
- After saving, the UI links the new file (openable in the code overlay, §11.2).

### 7.5 `TurnOverrides`

```rust
pub struct TurnOverrides {
    pub model: Option<ModelRef>,          // None ⇒ reuse the thread's current model
    pub mode: Mode,                       // plan | build → sandbox policy
    pub approval_policy: ApprovalPolicy,  // thread policy snapshot
}
```

`TurnOverrides` is a **resolved snapshot**, not a delta. The server constructs it at
`start_turn` by reading the thread's persisted state:

- **`mode`** — from `thread.mode` (the thread's current mode, switchable via `SwitchMode`, §7.4).
- **`model`** — `None` means "reuse the thread's `current_model`." The server always resolves it
  to the effective `ModelRef` (which carries `reasoning_effort` in itself, §8.1) before passing it
  to the harness, so there is exactly one home for effort. A non-`None` value would override the
  thread's model for this turn only (not persisted); in practice the UI persists model changes via
  `SelectModel` (P2) and sends `None` here.
- **`approval_policy`** — read from `thread.approval_policy`. This is **not** a per-turn override
  (P3/AP1): the user changes the thread's durable setting, not a single message. It appears in the
  snapshot because the harness needs it to pass to `turn/start`. It is set persistently via
  `SetApprovalPolicy` (§13.6).

**Effort lives only in `ModelRef.reasoning_effort`** (P1). There is no standalone
`TurnOverrides.reasoning_effort` field — it was removed to eliminate the double-home. The
effective effort is read from `current_model.reasoning_effort` and is sent to the harness only when
the active model advertises `supports_reasoning_effort` (§8.5).

**When `plan_build_modes = false`** (S7): `Mode` resolves to `Build` (the workspace-write
single mode), so `TurnOverrides` is well-defined for every harness regardless of capability.
The Plan/Build toggle is hidden in the UI (§13.5) and `Mode::Build` is always used.

---

## 8. Model Selection & Providers

### 8.1 Model identity

A model is identified by the **pair `(provider, model_id)`** plus optional reasoning effort:

```rust
pub struct ModelRef {
    pub provider: String,          // e.g. "openai", "cloudflare-litellm"
    pub model: String,             // e.g. "gpt-5.5", "@cf/z-ai/glm-4.7"
    pub reasoning_effort: Option<Effort>,
}
```

The **same model name on two providers is two distinct entries** (explicit requirement). The
UI always shows provider + model together.

### 8.2 Provider configuration

Providers are declared in `config.toml` (§16.3). Each declares: `id`, display `name`,
`base_url`, auth reference, `wire_api` (`responses`/`chat`), and whether it exposes a model
list endpoint. Example providers relevant here: OpenAI direct (Codex's built-in), and a
LiteLLM gateway fronting Cloudflare Workers AI.

> Note: Codex itself reads its own `~/.codex/config.toml` for provider/auth (Codex is
> "already configured", §12.2). Giskard's provider config governs (a) what the UI offers in
> the model picker and (b) optional `/v1/models` discovery. The `ModelRef` Giskard sends as a
> per-turn override must correspond to a model/provider Codex is configured to reach.

### 8.3 Model list: static + dynamic

- A **static list** in config is always available (works offline, deterministic for tests).
  Each static entry is a **typed model definition**, not a bare string, so it can supply the
  `ModelDescriptor` fields the UI needs (see the `[[providers.models]]` tables in Appendix C):

  ```toml
  [[providers.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = true
  ```
- If a provider advertises `model_listing` and exposes `GET /v1/models`, Giskard can **refresh
  the list dynamically** and merge results with the static list. A manual "refresh models"
  action triggers this; results are cached in memory (and optionally written back into the
  provider's config section) with a timestamp.
- Each model entry resolves to a `ModelDescriptor { provider, model, context_window,
  supports_reasoning_effort, display_name }`. `context_window` drives the thread context gauge
  (§10.3); `supports_reasoning_effort` drives whether the effort selector is shown (§8.5).
- **Metadata source precedence** (first hit wins):
  1. the typed `[[providers.models]]` entry in config;
  2. the `/v1/models` response, **if** it includes the field (many OpenAI-compatible
     endpoints, including LiteLLM, return `context_window`/`max_input_tokens` and capability
     hints — use them when present);
  3. a built-in **defaults table** in `giskard-core` keyed by well-known model ids;
  4. a **conservative fallback** (`context_window = 128000`, `supports_reasoning_effort =
     false`) with a UI warning badge ("context size unknown — using default"), so an unknown
     model is still usable and the gauge still renders.
- **Stale-provider normalization (E5).** When a persisted/project `ModelRef` names a provider
  that is no longer configured, but its `model` id appears under exactly one configured provider,
  the server rewrites the provider to that configured provider before opening/resuming or starting
  a turn. If the matched model does not support reasoning effort, `reasoning_effort` is cleared.
  If zero or multiple configured providers match, the model ref is left unchanged and normal
  error/reporting paths apply.

### 8.4 Changing model within a thread

- Supported and expected. Selecting a different model updates the thread's `current_model`;
  it takes effect on the **next turn** (Codex accepts model per `turn/start`). This satisfies
  "change model during a thread".
- When the model changes, the thread's cached `context_window` (C4) is updated from the new
  model's descriptor and the context gauge (§10.3) recomputes. Because the value is a cache derived
  from `current_model`, it is also recomputed on load, so a corrected config `context_window` takes
  effect without a migration.
- On project/thread load, open/resume, `SendInput`, and `SelectModel`, the server applies
  stale-provider normalization (E5) before computing the context window or passing the model to the
  harness. This allows a project saved with an old provider id to recover when the configured
  provider set now contains the model under one unambiguous provider.
- **Per-model effort retention (C7).** When switching from a reasoning model (e.g.
  `effort = high`) to one with `supports_reasoning_effort = false`, the old model's effort value is
  **retained** in a per-thread `model_efforts` map keyed by `(provider, model)`. Switching back to
  the reasoning model restores the stored effort automatically. The effort parameter is never sent
  to the harness when the active model doesn't support it (§8.5 already handles the send-side). This
  means a user can toggle between a reasoning and a non-reasoning model without losing their effort
  preference on the reasoning model.

### 8.5 Reasoning effort

- Effort (`minimal | low | medium | high | xhigh`, matching the pinned Codex `ModelReasoningEffort`,
  S4) is selectable **only when the chosen model supports it** (`supports_reasoning_effort`);
  otherwise the selector is hidden and no effort param is sent (avoids sending unsupported
  parameters). If a model descriptor supplies a concrete effort list, the browser uses it;
  otherwise the Codex-compatible set above is offered for reasoning models.

---

## 9. Approvals & Permissions

> "Permissions" here = **agent action approvals**, not user roles. There is exactly one user.

### 9.1 Policy per thread

`ApprovalPolicy` is stored in each thread's `<thread_id>.json`:

- **`read_only`** — strictly no writes/exec (natural companion to Plan mode).
- **`ask`** — the agent must request approval for each command execution and file change;
  the UI prompts the user.
- **`auto`** — approvals are granted automatically (full-auto within the workspace sandbox).

Policy is a **thread-level** setting, **not** a per-project or per-turn override (P3/AP1). Project
creation does not ask for policy. New threads start with `ask`. It is settable via the
`SetApprovalPolicy` client message (§13.6), which persists immediately and echoes a `ThreadState` to
all connected tabs — the same durable-switch pattern as `SwitchMode`/`SelectModel` (P2). The `ask`
policy is only offered when the active harness advertises `live_approvals` (§9.4); otherwise it is
coerced at attach time.

**Interaction with Plan mode.** Mode (Plan/Build) and approval policy are **orthogonal
settings**, but Plan mode changes what the sandbox permits: file writes remain blocked while
network reads are allowed. Therefore in Plan mode an `ask` policy can still matter for commands or
permission escalations, and those approvals must be surfaced normally. Plan mode does **not**
overwrite the thread's `approval_policy`.

### 9.2 Live approval flow (requires `capabilities.live_approvals`)

1. Harness pushes an approval request (command exec / file change / permission escalation);
   `CodexHarness` maps it to `AgentEvent::ApprovalRequested` with the details (command, cwd,
   reason, target path, and the set of available decisions).
2. UI shows a non-blocking prompt scoped to the thread (with the command/diff preview).
   **Phase 3 (S6):** the preview uses the **raw diff string** from the harness (the text carried
   in the `ApprovalRequest`'s reason/detail). Structured `FileDiff` parsing and the side-by-side
   diff viewer are Phase 4 (§11, §15); the dependency is stated here so it is not discovered as a
   gap later.
3. User chooses a decision; server calls `respond_approval`.

Codex also has server-initiated requests that are not approval decisions:
`item/tool/call`, `item/tool/requestUserInput`, `mcpServer/elicitation/request`, auth refresh,
attestation, and future method names. These use `AgentEvent::ServerRequestReceived` rather than
`ApprovalRequested`, are rendered as transcript cards, and must remain pending until the browser
sends `respond_server_request`. Giskard may provide first-class UI for known methods, but unknown
methods must still be visible and answerable; silent best-effort rejection is not a valid normal
path.

**Decision granularity** (mirrors Codex):
- `accept` (this once), `accept_for_session` (don't ask again this session for this kind),
  `decline`, `cancel`. For command exec, an optional "accept with amended exec policy" may be
  offered if the harness provides it.

#### 9.2.1 Definition of "session" for `accept_for_session`

"Session" = **the lifetime of the current harness process for that project** (i.e. the
`codex app-server` child spawned for the project, §4.7). Rationale: the approval memory is a
property of the running agent process, which is what actually enforces it, so the boundary
must match that process.

Concretely:
- A `accept_for_session` grant persists until the project's harness process is shut down or
  restarted (idle shutdown, crash + reconnect, or app restart). After a respawn, previously
  session-granted approvals are **not** remembered and the agent will ask again. This is the
  safe default (fail-closed) and is simple to reason about.
- It is **not** tied to the browser tab/session, and **not** persisted to disk. Giskard
  mirrors the harness's own session-grant behavior rather than maintaining an independent
  grant store. The UI communicates this ("Approved for this session — resets if the agent
  restarts").
- Scope of a grant follows what the harness scopes it to (e.g. by command kind / destination,
  §9.3); Giskard does not broaden it.

### 9.3 Grouping & concurrency

- Concurrent approval prompts are grouped where the harness groups them (e.g. Codex groups
  network-access prompts by destination). The UI shows a queue if several are pending, scoped
  per thread so prompts from background threads don't hijack the foreground.

### 9.4 Degraded harness

If the active harness lacks `live_approvals`, the UI hides live prompts and the thread must
run in `auto` or `read_only` (the selector disables `ask`). This keeps the experience coherent
across harnesses.

**Policy coercion (C8).** `approval_policy` is persisted per thread, so a thread created under
Codex (which supports `live_approvals`) may store `ask`, then later be opened under a degraded
harness that lacks `live_approvals`. On harness attach, if the harness lacks `live_approvals` and
the stored policy is `ask`, the server coerces the **effective** policy to `read_only` for that
session **without overwriting the stored value**, and surfaces a notice to the UI ("Approval policy
'ask' is not supported by this harness — running as read-only for this session"). If the harness
later regains the capability (e.g. after a reconnect to a full-capability harness), the stored
`ask` policy takes effect again. This is latent under v1 (Codex only) but is specified now while
the approvals design is fresh.


---

## 10. Token Tracking

### 10.1 Sources

Token usage comes from `TurnCompleted` (Codex reports usage on `turn/completed`). Each turn
contributes `{ input, output, total }` tagged with the `(provider, model)` used for that turn.

### 10.2 Aggregation levels

Recorded and viewable at:

- **Thread** — running totals in `<thread_id>.json` (`tokens`), plus per-model breakdown. The
  browser shows these totals in the thread-header context usage popover, not as a permanent
  right-panel section.
- **Project** — `projects/<id>/tokens.json`: `total`, `by_day`, `by_model`.
- **Global** — `tokens-global.json`: `total`, `by_day`, `by_model`.

Every `total` / `by_day[…]` / `by_model[provider][model]` value is the same `TokenUsage`
struct (B3). `by_model` is a **nested** `provider → model → TokenUsage` object (C3), never a
`"provider/model"` string key, so slash-bearing model ids stay unambiguous.

**Time windows** for the global (and project) views: **day / week / month / total**. Weekly
and monthly figures are derived on read by summing `by_day` buckets (single source of truth,
§5.3). A dashboard renders these as tables and simple charts.

### 10.3 Context-window gauge (per thread)

Within a thread, show the thread's current context footprint **relative to the active
model's context window** (e.g. 15.4k / 262k, or / 1M). The denominator is
`ModelDescriptor.context_window` for the current model and **recomputes when the model
changes** (§8.4). This is a usage-vs-capacity indicator to warn before hitting context limits.
The gauge is rendered as a header button; activating it opens a compact card with the same current
context footprint plus cumulative thread token totals from §10.2 and a manual `Compact context`
action routed through `HarnessCapabilities.context_compaction`. Unsupported harnesses return a
structured browser-visible error. Manual compaction is a thread-level operation and is disabled
while that thread has an active turn; other threads and projects remain usable while compaction is
running. Giskard does not need to warn near the limit because Codex may compact automatically.

> **Codex source field.** "Tokens used in the thread" for the gauge should reflect the current
> conversation's *context occupancy*, which is not the same as cumulative billed tokens
> (cumulative usage keeps growing across turns; context occupancy reflects what's currently in
> the window after any compaction). Codex reports usage on `turn/completed`; the token-usage
> payload distinguishes cumulative totals from the last turn's usage and typically exposes an
> input-tokens / context-used figure per turn. **Candidate fields to use (in order):**
> (1) an explicit context/window-used field on the turn's usage object if present in the
> pinned schema; (2) otherwise the **last turn's input tokens** (the input side reflects the
> context sent to the model, which is the best available proxy for occupancy); (3) fall back
> to cumulative `total` only if neither is available. The implementer must inspect the pinned
> `codex app-server generate-json-schema` output for the exact field name on the
> `turn/completed` usage object, pick per the order above, and record the choice in a code
> comment + the README. Whichever is chosen, the gauge denominator is the active model's
> `context_window` (§8.3).

### 10.4 Optional cost estimation

Optional (config-gated): per-model € rates in `config.toml` produce an estimated spend
alongside raw token counts. Off by default; raw token counts are the primary metric.

---

## 11. Visualization: Diffs & Code Overlay

### 11.1 Diff viewer (side-by-side)

- **Visualization only** in v1 (no accept/reject of individual hunks, no git ops).
- Fed by `AgentEvent::DiffUpdated` (Codex `turn/diff/updated`), per file.
- Rendered **side-by-side, in a panel next to the thread** on desktop. On mobile it becomes a
  full-screen tab/drawer (unified inline diff if side-by-side doesn't fit; §13.4).
- Shows the set of files changed in the current turn, selectable; each file shows old vs new
  with additions/deletions highlighted. Large diffs are virtualized (§11.3).

### 11.2 Code overlay for referenced paths

- When an agent message (or command output) mentions a **filesystem path** within the
  workspace, Giskard makes it a clickable link. Clicking opens an **overlay/panel** showing
  that file.
- **Server-side syntax highlighting** with `syntect`: the backend reads the file from the
  project filesystem, highlights based on extension/first line, and returns highlighted HTML
  (plus raw text for download). The frontend renders the trusted server HTML; no JS
  highlighter, no npm.
- The overlay provides a **"Download file"** action (streams the raw file) and shows the
  file's path, size, language, and line numbers for text previews.
- **Path detection:** a server-side (or shared) linkifier scans agent text for path-like
  tokens and resolves them against the workspace root. Only paths that (a) resolve inside the
  allowed scope and (b) exist are linkified. Ambiguous/relative paths are resolved relative to
  the workspace root. This detection runs when an `ItemCompleted` agent message is finalized
  (not on every delta) for efficiency.
- **Line targets (L5/L6):** a path may include a `#<line>` or `:<line>` suffix, for example
  `src/main.rs#42` or `src/main.rs:42`. Compiler-style `:<line>:<column>` is also accepted, with
  the column ignored for navigation. The suffix is included in the clickable span but removed before
  filesystem validation; the response carries `path = "src/main.rs"` and `line = 42`, and the
  overlay scrolls to that line after loading. When possible, the target line is centered in the
  overlay viewport so before/after context is visible.
- **Initial UI slice (L1):** the served self-contained UI calls `POST /api/projects/{id}/linkify`
  for completed text items, renders path spans as inline controls, opens
  `GET /api/projects/{id}/highlight?path=…` in the code overlay, and downloads through
  `GET /api/projects/{id}/raw?path=…`. This is intentionally whole-file oriented until the
  virtualized line-range viewer in §11.3 is implemented.
- **Markdown rendering (M1–M3):** agent/reasoning text is Markdown, so finalized messages are sent
  to `POST /api/projects/{id}/render` instead of `/linkify`. The server parses the Markdown
  (`pulldown-cmark`) and emits **sanitized** HTML with a custom serializer: all text is escaped,
  raw HTML in the source is escaped to inert text (never passed through), link URLs are restricted
  to `http`/`https`/`mailto`, and images are not fetched. Path detection runs in the same pass over
  prose text runs (not inside code), emitting the same `.path-link` controls the overlay wires up.
  Fenced code blocks are syntax-highlighted server-side with `syntect` when their fence language is
  recognized, and every code block is rendered with a compact header showing the resolved language
  label (for example `Rust` or `JSON`; unknown fence labels are shown as provided after
  sanitization). Inline code spans are escaped/styled but not syntax-highlighted because Markdown
  does not carry a reliable language for them. The browser injects the returned HTML as trusted
  markup and attaches the path-link handlers. `/linkify` is retained for command output, which is
  plain text rather than Markdown.

### 11.3 Large files & performance

- Files above a configurable size threshold are highlighted/served in **paginated chunks**
  (line ranges) and the viewer virtualizes rendering (only visible lines in the DOM).
- Highlighting is cached per (path, mtime) in memory to avoid recomputing on repeat opens.
- Binary/non-text files are detected and shown as "binary — download only".

---

## 12. Authentication

### 12.1 App auth (single shared password)

- **One shared password** gates the whole app. No accounts, no roles, no 2FA (v1).
- The password is stored as an **Argon2 hash** in `config.toml` (or an env var
  `GISKARD_PASSWORD_HASH`), never in plaintext. A `giskard-admin set-password` command
  generates the hash.
- **Login:** a POST verifies the password against the hash and issues a **signed session
  cookie** (HMAC-signed, `HttpOnly`, `SameSite=Strict`, `Secure`). Session lifetime is
  configurable (default 30 days, sliding). A signing key is generated on first run and stored
  in the data dir (`session.key`, 0600).
- **All routes except the login page and static assets require a valid session.** The
  WebSocket upgrade validates either the session cookie or a short-lived signed ticket from
  authenticated `GET /api/ws-ticket`. The ticket is only for WebSocket upgrade compatibility
  and carries the same session signing key semantics as the cookie.
- TLS is terminated upstream (Nginx). Giskard assumes HTTPS in production; the `Secure`
  cookie flag is on by default and can be disabled via config for local HTTP dev.

### 12.2 Harness (Codex) auth

Codex is **already configured** on the machine (its own `~/.codex` credentials — ChatGPT
login or API key / custom provider). Giskard does **not** manage Codex's auth; it inherits the
environment when spawning the child process. Document the assumption clearly and fail with a
helpful message if the spawned app-server reports it is unauthenticated.


---

## 13. UI / UX

### 13.1 Stack

- **Dioxus fullstack, pinned to the 0.7 line** (`dioxus = "0.7"`, latest patch; 0.7 is the
  Axum-based Server Functions rebuild with single-line fullstack WebSocket support, which
  Giskard depends on). **Pin the exact minor in `Cargo.toml` and the `dx` CLI version in
  `rust-toolchain`/CI**, because the fullstack API differs between 0.6 and 0.7. Do not build
  against `main`/git.
- WASM frontend + Axum backend, built with the `dx` CLI. **No npm / Node / JS bundler.**
- **Styling: hand-authored CSS** (a single scoped stylesheet or Dioxus scoped-CSS), **not
  Tailwind.** Note: Dioxus 0.7 ships *automatic Tailwind* that spawns a Tailwind watcher when a
  `tailwind.css` is present — **do not use it**, since that path can pull in a JS toolchain and
  violates the no-npm constraint. Simply omit the `tailwind.css` trigger file. The Radix-based
  Dioxus primitives (unstyled, accessible) may be used for behavior (focus, ARIA, keyboard) as
  long as styling stays hand-authored CSS.
- Shared wire types live in `giskard-proto` so client and server never disagree on the
  protocol.

### 13.2 Design direction

A **minimal, intuitive, calm control surface** — this is a tool the user lives in for long
sessions, so clarity and low visual noise beat flourish. Explicitly avoid the generic
"AI app" defaults (cream + terracotta serif, or black + acid-green). Concrete direction:

- **Restraint first.** One accent color used sparingly for the primary action and active
  state; everything else neutral. Dark theme by default (long coding sessions), with a light
  theme available.
- **Typography:** a clean, slightly technical UI face for chrome; a real **monospace** for
  code, command output, diffs, and paths (these are the substance of the app). Paths and
  model names are always monospace so they read as "things you can click / act on".
- **Structure encodes state,** not decoration: mode (Plan/Build), model, and approval policy
  are always visible and legible at a glance in the thread header; a running turn has a clear
  live indicator. Running commands are shown both inline in the transcript and in the header
  `Tasks` menu; selecting a summary entry scrolls to the transcript command row.
  Command lifecycle state is shown with a non-color cue plus subtle color: `●` amber for running,
  `✓` green for succeeded, `✕` red for failed, and `■` muted gray/orange for terminated or
  declined.
- **Signature element:** the **thread transcript** treated as a first-class typed document —
  agent text, collapsible reasoning, command blocks with collapsible streamed output, and inline
  linkified paths — paired with the **context-window gauge** as a persistent, honest read on
  "how full is this conversation." That gauge + linkified transcript is what makes Giskard
  feel purpose-built rather than a generic chat wrapper.
- Copy is plain and action-named (§ frontend writing guidance): buttons say exactly what
  happens ("Save plan to project", "Switch to Build", "Interrupt"). Empty states invite
  action ("No projects yet — create one to start."). Thread setting controls use visible labels
  and action-oriented option text; for example, the approval policy selector is labeled
  "Approvals" and shows "Ask first", "Auto approve", and "Read only" rather than raw enum names.

> The design plan above is a starting brief for the implementer, not a locked visual spec.
> The implementer should produce a small token system (4–6 named colors, the 2–3 typefaces,
> spacing scale) and iterate, keeping the restraint principle and the two non-negotiables:
> monospace for code/paths, and the always-visible mode/model/gauge state.

### 13.3 Primary layout (desktop / laptop)

```
┌───────────┬───────────────────────────────────────────────┐
│ Projects  │  Thread header: mode · model · approval ·      │
│ + threads │  tasks · MCP · context usage                   │
│ (sidebar) ├───────────────────────────────────────────────┤
│           │                                               │
│  proj A   │  Transcript (streamed items, linkified paths,  │
│   ├ th 1  │  collapsible reasoning, tasks, command output, │
│   └ th 2  │  file changes)                                │
│  proj B   │                                               │
│   └ th 3  │                                               │
│           ├───────────────────────────────────────────────┤
│  Settings │  Composer: input · send/interrupt              │
└───────────┴───────────────────────────────────────────────┘
```

- **Left sidebar:** projects with their thread lists, a project-row disclosure control to collapse
  or expand each project's threads, "new project" / per-project "new thread" actions, and a
  bottom-pinned **Settings** menu for durable client UI preferences such as Appearance. Project
  collapse state is browser-local and persists across reloads.
- **Center:** thread header (mode, model, approval policy, tasks menu, MCP menu, context usage menu
  with manual compact action, plan-dump & interrupt actions) + transcript + composer.
- Source/code previews and downloads open as overlays from linkified transcript paths rather than
  occupying a permanent right column.

### 13.4 Responsive (smartphone)

- The two columns collapse into a **single-column drawer navigation**:
  - The top bar opens the **Projects** drawer, which also contains the **Settings** menu.
  - The transcript remains the primary view.
  - Side-by-side diffs fall back to **unified inline** diffs when width is insufficient.
- Composer stays pinned to the bottom on the Transcript view. Approval prompts appear as a
  bottom sheet.
- Touch targets ≥ 44px; the app is usable one-handed for the common loop (read → approve →
  send).

### 13.5 Capability-driven UI

The UI reads `HarnessCapabilities` for the active harness and adapts:

- No `live_approvals` ⇒ hide approval prompts; approval-policy picker offers only
  "Auto approve" / "Read only" (`auto` / `read_only` on the wire).
- No `plan_build_modes` ⇒ hide the Plan/Build toggle (thread is single-mode). `Mode` resolves to
  `Build` (workspace-write) so `TurnOverrides` is always well-defined (S7).
- No `per_turn_model` ⇒ model is fixed at thread creation (picker disabled mid-thread).
- No `reasoning_effort` or model doesn't support it ⇒ hide the effort selector.
- No `structured_diffs` ⇒ hide the Diffs tab (or show a plain textual change summary).
- No `mcp_status` ⇒ hide or disable the MCP menu. No `mcp_reload` ⇒ the menu can refresh the
  cached status but not ask the harness to reload MCP config. No `mcp_oauth_login` ⇒ servers in
  `not_logged_in` state show the auth state without an authenticate button.

This guarantees a coherent experience when a future, less-capable harness is plugged in.

### 13.6 Client ↔ server protocol (single multiplexed WebSocket)

- **One WebSocket per browser client**, multiplexing all projects/threads (chosen for lowest
  CPU/memory: one connection, one server-side fan-out task, no per-thread sockets).
- Messages are tagged with `project_id` / `thread_id`. Defined once in `giskard-proto`.
- **Thread open is REST-backed:** `POST /api/projects/{project_id}/threads` accepts
  `{ thread_id?: ThreadId, resume?: String }`. `thread_id = None` creates a new Giskard thread;
  `thread_id = Some(existing)` reopens/reattaches a persisted Giskard thread; `resume` is the
  optional native harness id for explicit resume/create flows.

**Client → server** (examples): `Subscribe { thread_id }`, `Unsubscribe { thread_id }`,
`SendInput { thread_id, text }`, `SwitchMode { thread_id, mode }`,
`SelectModel { thread_id, model_ref }`, `SetApprovalPolicy { thread_id, policy }`,
`Interrupt { thread_id }`, `CompactContext { thread_id }`,
`TerminateCommand { thread_id, process_id }`,
`ApprovalDecision { request_id, decision }`, `SavePlan { thread_id, path }`.

`SendInput` and `CompactContext` are serialized per thread by the server before they enter the
harness. If another normal turn or manual context compaction is already starting or running on the
same thread, the server rejects the later request with `Error { code: "thread_turn_active", ... }`
instead of starting a second forwarder. This is a correctness boundary, not only a browser disabled
state: multiple tabs or reconnect races must not be able to start overlapping native turns for one
thread.

> **Durable settings switches (P2/P3).** `SwitchMode`, `SelectModel`, and `SetApprovalPolicy`
> persist immediately to `<thread_id>.json` before the server acknowledges, then broadcast a
> `ThreadState` to all connected tabs. This guarantees the §5 "same state after restart"
> requirement: a switch is not lost if the app restarts before the user sends the next message.
> The sandbox/model/policy *effect* still takes hold at the next turn; only the stored *intent* is
> durable now.

> **`SendInput` carries text only in v1.** Image/file attachments are out of scope (matching
> `UserInput` in §4.5). If added later, extend both `UserInput` and this message together.

**Server → client** (examples): `Event { thread_id, agent_event }` (a serialized
`WireAgentEvent` — the path-mirrored wire form of `AgentEvent`, §3.5),
`ThreadState { thread_id, state }` (persisted snapshot on subscribe/resync),
`LiveTurnSnapshot { thread_id, turn_id, accumulated, pending_approval?, pending_server_requests }`
(in-flight turn reconstruction on reconnect, carrying `WireAgentEvent`s, a `WireApprovalRequest`,
and unresolved `ServerRequest`s),
`RunningTasks { thread_id, tasks: [RunningTask] }` (commands and tool/MCP calls still known to be
running, including commands that outlived an interrupted turn),
`TokenUpdate { scope, thread_id?, ledger }`, `ApprovalRequest { thread_id, request }` (a
`WireApprovalRequest`),
`Error { code, severity, message, detail?, thread_id?, action? }`, `Pong`.

For `TokenUpdate`, `thread_id` is required when `scope = "thread"` and omitted for non-thread
ledger scopes. The browser must only apply a thread-scoped token update to the thread usage menu
when the message `thread_id` matches the active thread.

`OpenThreadResponse` may also carry `warning: ErrorInfo?` with the same `code` / `severity` /
`message` shape when the requested thread was opened but degraded (for example, Codex resume
failed and Giskard started a fresh native session while keeping persisted history).

**Client rendering invariant (E6):** `ItemDelta { item_id }` and the later `ItemCompleted`
for the same `Item.id` are one lifecycle. The UI must finalize or replace the streamed body in
place when the completed item arrives, and must de-duplicate rendered items by both Giskard
`ItemId` and harness-native `harness_item_id` when replaying persisted state or receiving live
events.

**Client thread isolation invariant (WS1/WS2):** before rendering or mutating local thread state,
the browser must verify that every thread-scoped server message belongs to the active thread.
Messages for a previously selected thread, including frames delivered by a replaced WebSocket
connection, are ignored. Thread-scoped messages without a usable `thread_id` fail closed, except
for global errors that intentionally omit `thread_id`.

**Server thread isolation invariant (WS3):** a per-thread event forwarder only owns
`AgentEvent`s whose `thread` field equals the forwarder's `ThreadId`. Events for another thread
are ignored before turn ownership, live-buffer updates, running-task updates, approval/server
request registration, hub broadcast, or JSONL persistence. Each ignored foreign-thread event is
logged at error level with structured fields sufficient to diagnose the harness routing bug without
dumping the full event payload.

**Harness routing invariant (WS4/WS5/WS6):** harness adapters must route every mapped native event by
the mapped `AgentEvent.thread` before it reaches the server forwarder. If a native message carries a
non-empty unknown native thread id after native-thread registration has begun, the adapter treats it
as unroutable and logs/rejects it rather than relabeling it as the current fallback thread. Reopening
an already-open thread reuses the existing per-thread sender so live subscribers and forwarders are
not orphaned by metadata refreshes or duplicate open requests.

**Transcript visibility invariant (E7/E8/E9):** every finalized item payload with user-observable
meaning is rendered as a transcript row. `FileChange`, `ToolCall`, and `Activity` are visible rows;
they must not fall through to empty agent bubbles or be silently hidden. Started tool calls with
`ToolCallStart` metadata are also visible before completion, so long-running or stuck tool calls
do not appear as silent active turns. The client records `ItemStarted.kind` and uses it to style
streamed deltas before completion. On reconnect, the client replays `LiveTurnSnapshot.accumulated`
events through the same event handler used for live WebSocket events.

> **Wire types (C1/§3.5).** Everything the server emits that could carry a filesystem path
> (`Event`, `ApprovalRequest`, the `LiveTurnSnapshot` contents) is mapped `core → Wire*` at the
> fan-out boundary, so paths are UTF-8 `String`s on the wire. Client→server messages are path-free
> (`SendInput` is text; `SavePlan.path` is a `String` re-validated server-side).

- **Fan-out:** the server keeps `thread_id → set<client_conn>`. An `AgentEvent` is serialized
  once and sent only to subscribed clients. Background threads keep producing events; a client
  that isn't subscribed to a thread still receives lightweight `ThreadState`/badge updates so
  the sidebar shows activity, but not the full delta stream (bandwidth control).
- **Backpressure:** per-connection bounded queue; if a client falls behind, coalesce deltas
  (keep latest) rather than unbounded buffering. Heartbeat ping/pong; auto-reconnect on the
  client with resubscribe + state resync. Browser disconnects caused by mobile/tab suspension are
  not user-facing errors while recovery is in progress; foreground auth/network failures remain
  visible through persistent connection state and throttled warning/error notices.
- **Reconnect & live-turn resync.** Persisted thread state only advances on `TurnCompleted`
  (§5.4), so a client that reconnects **during** an in-flight turn cannot reconstruct the live
  turn from disk alone. To close this gap the server keeps, **per active turn**, an in-memory
  **live buffer**: the ordered `AgentEvent`s accumulated since `TurnStarted` (agent-text so
  far, command output so far, pending approval/server requests, current diffs). This buffer is
  discarded on `TurnCompleted` (the finalized items are then on disk). On `Subscribe`, the
  server sends a **snapshot** first:
  1. the persisted thread state (all completed turns) from disk, then
  2. if a turn is currently live, a `LiveTurnSnapshot` reconstructed from the live buffer
     (accumulated text/output + any pending approval/server request), then
  3. subsequent deltas as normal.

  The browser treats this subscribe/resubscribe snapshot as authoritative for the active thread.
  It must clear transient browser-rendered transcript state (including optimistic pending user
  rows, fallback failed-turn bubbles, pending approvals/server requests, running-task DOM maps, and
  stale active-turn controls) before rendering the returned recent history and then replaying the
  live snapshot. Later metadata-only `ThreadState` broadcasts are not subscribe snapshots and must
  not clear the visible transcript.

  This means a reconnected client sees the full in-progress turn, including still-pending
  approval and server-request prompts. The live buffer is bounded
  (coalesced/truncated for very long command output, keeping head+tail) to cap memory; the
  authoritative full output still lands on disk at `TurnCompleted`.
- **Running-task resync (TK1).** Commands can outlive an interrupted turn even after the live buffer
  is discarded. The server therefore keeps a separate in-memory running-task registry keyed by
  `thread_id` + `item_id`, updated from command **and tool-call** item start/output/completion
  events. Tool calls are tracked the same way (name + server, elapsed time, output tail) and shown
  in the same `Tasks` menu, but they carry no `process_id` and do not outlive their turn: a
  tool still running when its turn completes (i.e. an interrupted turn) is dropped, while commands
  are kept as `after_turn`. Stopping a tool has no per-call cancel in Codex, so the browser sends
  `Interrupt { thread_id }` (turn-level) rather than `TerminateCommand`. On subscribe, and after
  each registry change, the server sends `RunningTasks`; the browser renders these in the header
  `Tasks` menu and maps `item_id` back to the transcript row for select/scroll (tool transcript
  rows are owned by the item stream, not re-rendered from the snapshot).
  `TerminateCommand` requests are forwarded to the active harness. Giskard must not terminate
  local processes directly; Codex-owned command processes are stopped only through the Codex
  app-server protocol. Codex agent command executions are not standalone `command/exec` sessions,
  so the Codex harness maps terminate requests for those items to `turn/interrupt` with the native
  Codex turn id that owns the command process. It must not use `command/exec/terminate` for agent
  command execution items. When the harness accepts a terminate request, the matching command
  remains in the registry with `terminating: true` until a terminal command event arrives, but the
  browser labels this state as "stop requested" rather than "terminated" or "terminating". A
  successful terminate request is not itself proof that the process has stopped. If Codex later
  reports a normal successful completion, the browser preserves the successful status and annotates
  it with "stop requested"; the server logs a warning that Codex did not terminate the process.
  Codex's "no active command/exec for process id" response is treated as stale-state cleanup only
  for commands already marked `after_turn`; for live commands it is surfaced through the normal
  structured `Error` path and the command remains visible with `terminating: false`.
  Harness adapters that can observe post-turn command lifecycle messages must keep draining them
  while any command is known running. When a late terminal command completion arrives for an
  already-persisted turn, the server updates `RunningTasks` and may broadcast the terminal
  `ItemCompleted` event to connected clients, but it does not mutate the already-appended JSONL
  turn record.
  Running-task snapshots include `started_at_ms`; clients use it to render elapsed time and
  refresh that display about once per second. Completed command payloads include `duration_ms`
  when the harness supplies it; clients render terminal outcome text from the status plus duration.
- **Auth:** the WS upgrade validates the session cookie (§12).

### 13.7 JavaScript glue policy

Default: **none.** If a browser capability is unreachable from Dioxus/WASM without a tiny JS
shim (e.g. a specific clipboard or file-download nicety), the shim is **hand-written, minimal,
and committed to the repo** — never pulled from npm. Document any such shim in the README with
its justification. Prefer WASM/Rust or server-side solutions first (e.g. downloads are served
by the backend, §11.2, avoiding JS entirely).

---

## 14. Testing Strategy

Everything is tested. Three layers:

### 14.1 Unit tests

- `giskard-core`: pure domain logic (mode transitions, token aggregation math, path
  linkification, model-ref equality treating provider as significant, context-gauge
  computation, week/month derivation from daily buckets). No I/O, fast, exhaustive.
- `giskard-persist`: atomic-write behavior, corruption quarantine, load/round-trip fidelity,
  concurrent-write safety (property tests where useful).
- `giskard-server`: auth (password hash verify, cookie signing/expiry), the filesystem
  browser's path-confinement logic (must reject `..`/symlink escapes when roots are set),
  syntax-highlight caching, protocol (de)serialization.

### 14.2 Integration tests with deterministic replay (no LLM)

This is the core requirement. Mechanism:

- **`ReplayHarness`** implements `AgentHarness` by reading a **recorded transcript** — an
  ordered list of harness transport messages (the raw JSON-RPC frames exchanged with a real
  `codex app-server`, captured once) — and emitting the corresponding `AgentEvent` stream
  with deterministic timing (no real model, no network).
- **Recording:** a `giskard-admin record` mode (or a test harness wrapper) runs a real Codex
  session once and writes the transcript fixture (`tests/fixtures/<name>.jsonl`). Fixtures are
  committed. A scrubbing step removes any credentials.
- **Replaying:** integration tests wire the application services to `ReplayHarness` and assert
  end-to-end behavior: sending input produces the expected persisted thread state; token
  ledgers update correctly; approval and server requests surface and responses are routed; plan
  dump writes the expected markdown; diffs are parsed and exposed; mode/model switches take effect
  on the right turn.
- **Determinism:** replay advances on demand (test drives the clock/step), so assertions are
  stable. The same fixtures double as a "demo mode" for the app without a real harness.

### 14.3 End-to-end (headless browser)

- A small **headless-browser** suite (e.g. via a WebDriver/Chromium-headless runner invoked
  from Rust) guards critical flows against regression: login, create project (with the file
  picker), open thread, send a message (backed by `ReplayHarness`), see streamed transcript,
  receive and answer approval and server-request prompts, view a diff, open a code overlay, switch
  mode/model, observe token/context updates. Runs in CI headless.
- Kept intentionally small (smoke-level for the main loop); business logic is covered more
  cheaply by §14.2.

### 14.4 CI gates

- All layers run in CI. A CI job regenerates the Codex app-server JSON schema for the pinned
  Codex version and diffs it against the vendored schema to catch protocol drift.
- Formatting (`cargo fmt`), lints (`cargo clippy -D warnings`), and the WASM build must pass.

---

## 15. Implementation Phases

All features are in scope for v1. Phases order the work so each builds on a working base; they
are **not** a scope reduction.

**Phase 0 — Foundations.** Workspace + crates skeleton; `giskard-core` domain types;
`giskard-proto`; flat-file persistence with atomic writes + validation + `giskard-admin`;
config loading; unit tests for core + persist.

**Phase 1 — Harness spine.** `AgentHarness` trait + capabilities; `CodexHarness` (spawn
app-server, JSON-RPC client, handshake, thread/turn lifecycle, event mapping);
`ReplayHarness` + fixture format + recording tool. Integration test: open thread, one turn,
assert persisted state (replay-driven).

**Phase 2 — Server & minimal UI loop.** Axum app; auth (password + session cookie); single
multiplexed WebSocket + fan-out; Dioxus shell; project list + create + filesystem picker;
open thread; send input; streamed transcript. E2E smoke: login → project → thread → message.

**Phase 3 — Modes, models, approvals.** Plan/Build toggle + per-turn sandbox mapping; plan
dump to markdown; model picker (static list) + per-turn model change + reasoning effort;
approval policy + live approval prompts + decision routing. Approval diff preview uses the raw
diff string (S6); structured `FileDiff` parsing is deferred to Phase 4. Tests for each via replay.

**Phase 4 — Visualization.** Side-by-side diff viewer from `DiffUpdated`; path linkification;
code overlay with `syntect` highlighting + download (initial whole-file UI slice complete in L1);
large-file virtualization/pagination.

**Phase 5 — Tokens & polish.** Thread/project/global ledgers; day/week/month/total dashboard;
context-window gauge; dynamic `/v1/models` refresh; responsive/mobile passes; optional cost
estimation; accessibility (focus, reduced motion), reconnect/backpressure hardening.

**Phase 6 — Hardening & docs.** Full E2E suite; protocol-drift CI check; README (setup, Codex
prerequisite, config reference, admin tooling); corruption/crash-recovery tests.

> The multi-harness abstraction is built in Phase 1 and exercised by `ReplayHarness`
> throughout; a second real harness (Claude Code) is **not** implemented in v1 but the trait,
> capabilities, and capability-driven UI ensure it can be added without touching persistence,
> core, or most of the UI.


---

## 16. Appendices

### Appendix A — Codex app-server mapping reference

The Codex **app-server** is a bidirectional **JSON-RPC 2.0** interface (the `"jsonrpc":"2.0"`
header is omitted on the wire). v1 uses the **stdio** transport (newline-delimited JSON), the
stable/production transport. The protocol is organized around three nested primitives —
**Thread → Turn → Item** — which map directly onto Giskard's model.

**Handshake (once per connection):** send `initialize`, then the `initialized` notification,
before any other call. The server returns its user-agent, `codexHome`, and platform info.

**Lifecycle:**

```
initialize → initialized
thread/start (or thread/resume {threadId})            → { threadId }
turn/start { threadId, input:[…], model?, effort?, sandbox? }
    ⇢ item/started, item/*/delta, item/completed  (stream)
    ⇢ turn/diff/updated                            (stream)
    ⇢ item/commandExecution/requestApproval  |  item/fileChange/requestApproval
                                              |  item/permissions/requestApproval   (server→client request)
    ⇠ (client responds with a decision)
turn/completed { usage, … }
turn/interrupt { threadId }                            (to cancel)
```

**Approval decisions** (client → server response): command execution — `accept`,
`acceptForSession`, `decline`, `cancel`, or an exec-policy-amendment variant; file change —
`accept`, `acceptForSession`, `decline`, `cancel`. Requests include `threadId`/`turnId` to
scope UI state.

**Overload:** JSON-RPC error `-32001` ("Server overloaded; retry later") ⇒ retry with
exponential backoff + jitter.

**Schema generation (vendored + CI-checked):**
```
codex app-server generate-json-schema --out schemas/
codex app-server generate-ts          --out schemas/   # reference only; not used at build
```
Artifacts are version-pinned to the Codex binary that produced them; regenerate on upgrade.

> Sandbox/mode mapping: **Plan ⇒ `read-only`**, **Build ⇒ `workspace-write`**. Codex
> collaboration-mode mapping is sent on every turn too: **Plan ⇒ `plan`**, **Build ⇒ `default`**.
> The Build/default send is intentional because Codex app-server collaboration mode is sticky after
> a plan turn. Approval policy maps to Codex's approval configuration. `TurnOverrides.model` maps
> to the per-turn `turn/start` model field; reasoning effort is carried inside
> `ModelRef.reasoning_effort` (P1: no standalone effort field on `TurnOverrides`).
> `TurnOverrides.approval_policy` is the thread policy snapshot (P3/AP1: not a per-turn override).

**Client library:** use `codex-codes` (v0.143.0, tested against Codex CLI 0.143.0) with the
`async-client` feature — its `AsyncClient` API (`spawn`, `initialize`, `thread_start`, generic
`request`, `next_message`, `respond`, `shutdown`) maps onto the `AgentHarness` trait. The Codex
`turn/start` call uses the generic `request` path while `codex-codes`' typed `TurnStartParams`
lags newer fields such as `collaborationMode`. Its built-in schema coverage scorecard validates
typed structs against `codex app-server generate-json-schema` output and can be wired into the CI
drift check (§14.4). Fall back to `codex-app-server-sdk` (v0.5.1) or a hand-rolled client only if a
future Codex CLI version diverges beyond what `codex-codes` tracks. Whichever is chosen, confine all
Codex types to `giskard-harness-codex` and preserve the raw-JSON fallback for unknown/drifted
messages.

The harness initializes Codex app-server with `capabilities.experimentalApi = true` before starting
or resuming threads. This is required for the experimental app-server fields/requests Giskard
supports, including `collaborationMode` and `item/tool/requestUserInput`.

### Appendix B — Example client↔server WebSocket messages

```jsonc
// client → server
{ "type": "SendInput", "thread_id": "01J…", "text": "Refactor the auth module" }
{ "type": "SwitchMode", "thread_id": "01J…", "mode": "build" }
{ "type": "SelectModel", "thread_id": "01J…",
  "model_ref": { "provider": "cloudflare-litellm", "model": "@cf/z-ai/glm-4.7",
                 "reasoning_effort": null } }
{ "type": "SetApprovalPolicy", "thread_id": "01J…", "policy": "auto" }
{ "type": "ApprovalDecision", "request_id": "ap_7", "decision": "accept_for_session" }
{ "type": "SavePlan", "thread_id": "01J…", "path": "docs/plan-auth-20260706-1030.md" }

// server → client
{ "type": "Event", "thread_id": "01J…",
  "agent_event": { "kind": "ItemDelta", "item_id": "it_3",
                   "delta": { "text": "I'll start by reading auth.rs…" } } }
{ "type": "ApprovalRequest", "thread_id": "01J…",
  "request": { "id": "ap_7", "kind": "command_execution",
               "command": "cargo test", "cwd": "/home/elie/dev/x",
               "decisions": ["accept","accept_for_session","decline","cancel"] } }
{ "type": "Event", "thread_id": "01J…",
  "agent_event": { "kind": "TurnCompleted",
                   "usage": { "input": 1200, "output": 340, "total": 1540 },
                   "status": "completed" } }
```

### Appendix C — Configuration reference (`config.toml`)

```toml
# ${XDG_DATA_HOME:-~/.local/share}/giskard/config.toml   (path overridable via GISKARD_DATA_DIR)

[server]
bind = "127.0.0.1:8787"
secure_cookies = true          # set false only for local plain-HTTP dev

[auth]
# generate with: giskard-admin set-password
password_hash = "$argon2id$v=19$m=…"    # or via env GISKARD_PASSWORD_HASH
session_days = 30

[browse]
# empty/unset ⇒ entire filesystem browsable.
# set to confine the file picker to these subtrees:
roots = []                     # e.g. ["/home/elie/dev"]

[plan]
default_dir = "docs"           # where "Save plan to project" writes
filename_template = "plan-{slug}-{ts}.md"

[tokens]
cost_estimation = false
# [tokens.rates."openai/gpt-5.5"]  input_per_mtok_eur = …  output_per_mtok_eur = …

[[providers]]
id = "openai"
name = "OpenAI (Codex built-in)"
wire_api = "responses"
model_listing = false
  # typed model entries carry the metadata the UI needs (§8.3):
  [[providers.models]]
  id = "gpt-5.5"
  display_name = "GPT-5.5"
  context_window = 262144
  supports_reasoning_effort = true
  [[providers.models]]
  id = "gpt-5.4"
  display_name = "GPT-5.4"
  context_window = 262144
  supports_reasoning_effort = true

[[providers]]
id = "cloudflare-litellm"
name = "Cloudflare Workers AI (via LiteLLM)"
base_url = "http://127.0.0.1:4000/v1"
wire_api = "responses"          # LiteLLM bridges /responses → /chat/completions
model_listing = true            # GET /v1/models available; merged over static entries
  [[providers.models]]          # static fallback; dynamic listing may add/refine
  id = "@cf/z-ai/glm-4.7"
  display_name = "GLM-4.7 (Workers AI)"
  context_window = 131072
  supports_reasoning_effort = false

[harness]
kind = "codex"
idle_shutdown_secs = 0          # 0 ⇒ keep alive while app runs
```

### Appendix D — Open items to confirm during implementation

These are deliberately left for the implementer to resolve and document, with a recommended
default already stated in-line:

1. **Context-gauge source field** — §10.3 now names the candidate fields and a selection order
   (explicit context-used field → last turn's input tokens → cumulative total). Remaining task:
   confirm the exact field name in the pinned Codex JSON schema and record the pick in code +
   README.
2. **Codex client crate choice** — **resolved: `codex-codes` v0.143.0** with the `async-client`
   feature (§3.3, App. A). Verified on crates.io against installed Codex CLI 0.142.5; its
   `AsyncClient` API maps 1:1 to `AgentHarness`, it includes a schema-drift scorecard for CI, and
   it ships real JSONL test captures. Fallback (`codex-app-server-sdk` v0.5.1 or hand-rolled) only
   if a future CLI version diverges.
3. **Dioxus fullstack single-crate vs split `giskard-ui`/`giskard-server`** — keep split
   unless tooling friction dictates otherwise; non-WASM crates stay separate regardless (§3.2).
   **Resolved (C1/C2):** `giskard-proto` is the sole crate `giskard-ui` links; it owns `Wire*`
   mirrors for path-bearing streamed types and re-exports the path-free `giskard-core` types; the
   server maps `core → wire` at the fan-out edge (§3.5).
4. **Plan-content extraction rule** — spec defaults to "latest Plan-mode turn's agent
   messages" with a preview before saving (§7.4.1); confirmed (C6): strictly the single most
   recent Plan-mode turn, no concatenation of earlier plan turns.
5. **Headless-browser runner choice** — pick a Rust-drivable headless option for §14.3 that
   introduces no npm dependency.

---

*End of specification.*
