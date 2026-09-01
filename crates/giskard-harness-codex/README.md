# Codex harness adapter

`giskard-harness-codex` maps the Codex app-server JSON-RPC protocol onto the
harness-neutral types and lifecycle events defined by `giskard-harness` and
`giskard-core`.

The [Giskard specification](../../specs/giskard-specification.md) defines the
owned identifier semantics and invariants. This document describes how the
Codex adapter satisfies them, including the scope and lifetime of Codex-native
identifiers.

## Runtime ownership

`CodexHarness` is the cloneable public API handle. Each project app-server process has exactly one
non-cloneable `CodexInstance`, owned by exactly one Tokio task, that owns its transport, mapper,
active turns, pending compactions and context restores, workspace configuration, command/control
receivers, and worker lifecycle. It serves every native thread on that process and is unrelated to
the Primary/sub-agent hierarchy. Helper futures borrow its protocol state through `&mut self`; no
independent worker mutates that state.

`CodexTransport` remains the mockable request/read abstraction. `SenderMap` remains shared only
because synchronous `AgentHarness::subscribe` must read it; `CodexInstance` is its sole runtime
lifecycle mutator. It also owns route establishment: durable bootstrap, explicit open/resume, and
provider-owned child claims all use the same primitive. That operation claims identity before
publishing the broadcast sender, and an idempotent claim preserves the existing sender.
When a bootstrapped native resume fails because its rollout disappeared, the instance atomically
replaces that exact native/Giskard binding with the fresh session identity, advances its route
epoch, and preserves the delivery sender.

## Identifier model

Giskard-owned identifiers are durable application identities. Codex-native
identifiers are opaque protocol correlation values and process handles. Do not
substitute one category for the other.

| Identifier | Owner | Scope and lifetime | Persisted by Giskard | Purpose |
| --- | --- | --- | --- | --- |
| `ThreadId` | Giskard | Durable Giskard thread | Yes | Routes and stores a Giskard thread |
| Codex `threadId` | Codex | Codex thread store | Yes, as `harness_thread_id` | Opens or resumes the native thread and routes notifications |
| `TurnId` | Giskard | Durable Giskard turn | Yes | Identifies a turn in events and history |
| Codex `turnId` | Codex | Native Codex turn | Indirectly, through the mapped `TurnId` | Correlates native turn events |
| `ItemId` | Giskard | One logical transcript item | Yes | Correlates item start, deltas, completion, and UI state |
| Codex item `id` / tool `call_id` | Codex | One item lifecycle in its originating turn | Yes, as `harness_item_id` | Correlates native item events |
| Unified-exec `processId` | Codex | Loaded Codex thread session | Stored in command metadata, but live only in Codex memory | Controls an agent command process |
| `write_stdin.session_id` | Codex tool schema | Same lifetime as unified-exec `processId` | No additional value | Model-facing name for the unified-exec process ID |
| `command/exec.processId` | App-server client | Originating app-server connection | No | Controls a standalone `command/exec` process |
| JSON-RPC request ID | Codex and adapter | Pending request on the connection | No | Routes approval and server-request responses |
| `ApprovalId` / `ServerRequestId` | Giskard | Pending browser action | No | Routes a browser response back to the JSON-RPC request |
| Host OS PID | Operating system | Host process lifetime | No | Diagnostic only; not a supported Codex control handle |

## Mapping keys

The adapter currently maintains these identity mappings:

```text
Codex threadId
    -> Giskard ThreadId

(Giskard ThreadId, Codex turnId)
    -> Giskard TurnId

(Giskard ThreadId, Giskard TurnId, Codex itemId)
    -> Giskard ItemId

(Giskard ThreadId, Codex processId)
    -> originating Codex turnId while the command is known running
```

Retained Codex identities use adapter-local newtypes and named composite keys. String identities
convert back to strings only at harness-neutral, wire, persistence, and logging boundaries; route
epochs similarly return to their plain numeric representation at the existing harness boundary.
This keeps the opaque domains distinct without changing their protocol representation.

### Where the thread mapping comes from

`Codex threadId -> Giskard ThreadId` is populated three ways, and the order
matters because Codex can talk about a thread before Giskard opens it.

**Bootstrapped during harness construction.** `HarnessFactory::create` receives a
complete `HarnessBootstrap` containing every `(harness_thread_id, ThreadId)` pair
the project has already persisted. The registry rejects an incomplete scan,
empty IDs, and duplicate mappings. The Codex adapter installs every route and
event sender before launching its worker and ordinary event dispatch;
initialization traffic already buffered by the current client cannot be mapped
first. This is construction input, not a command sent to an already-running harness.

It exists because Codex announces a sub-agent's thread as soon as it loads one,
which for a child persisted in an earlier run happens before the parent's tool
call names it. Without the pair already in hand the adapter meets a native id it
has never seen and has to invent a `ThreadId` for a thread that already has one —
two identities for one thread, and every registry above keyed by whichever came
first. Pre-registration removes the second identity rather than reconciling it.

**On claim or open.** `claim_native_thread` binds a provider-owned child without
issuing `thread/resume`, starting work, or fabricating model metadata. `open_thread`
binds the native id it explicitly started or resumed to the authoritative
`OpenThreadOptions::thread` supplied by Giskard. Live opens never discover or mint a Giskard
identity from a native id.

**Never inferred from traffic.** A non-empty native thread id that resolves to
nothing is a routing failure, reported as such. It is only attributed to the
caller's fallback thread while the adapter knows of no threads at all — which,
for a project with persisted threads, stops being true the moment its harness is
created.

Every first binding receives a monotonically allocated route epoch for that
adapter lifetime. Repeating the same native/local pair is idempotent; binding
either side to a different identity is a protocol error and never rekeys state.
These registries belong to one `CodexInstance` and are rebuilt from durable
bootstrap when its Codex app-server process is respawned. Durable Giskard IDs
and completed transcript items remain in Giskard persistence; native
live-process state does not. Deleting a thread removes its delivery sender but preserves its native
identity claim for the lifetime of that Codex process; an identical later claim recreates the
sender, while a conflicting identity remains an error.

The turn key includes the Giskard thread because Codex does not expose a
protocol contract making turn IDs globally unique across threads. The item key
also includes the Giskard thread and turn because Codex does not expose a
protocol contract making item IDs unique across all turns and threads. These
scopes prevent copied or reused native IDs from aliasing Giskard entities.

An empty native item ID is not entered into the registry. The adapter mints a
new `ItemId` for that event because it has no native correlation key.

## Item lifecycle

Codex documents the item lifecycle as:

```text
item/started -> zero or more item-specific deltas -> item/completed
```

For one logical item, the adapter must emit the same Giskard `ItemId` for every
stage. `item/completed` is the authoritative final state and updates the item
started under the same identity.

Example:

```text
Codex item/started(thread_a, turn_1, call_7)
    -> Giskard ItemStarted(thread_A, turn_1, item_X)

Codex outputDelta(thread_a, turn_1, call_7)
    -> Giskard ItemDelta(thread_A, turn_1, item_X)

Codex item/completed(thread_a, turn_1, call_7)
    -> Giskard ItemCompleted(thread_A, turn_1, item_X)
```

Reusing `call_7` in another turn or thread produces another Giskard `ItemId`.

Some Codex notifications carry an item ID without producing a visible Giskard
item. The mapper may seed the scoped item registry from those notifications so
that later deltas and completion still resolve to the same `ItemId`.

A `functionCallOutput` item is mapped to a Giskard `ToolCall`. Codex emits it for a tool result the
client supplied on `turn/start`, so the item carries the tool name, its optional namespace, and the
output, but never the arguments — the recorded input is `null` rather than an invented object. A
text body is kept as a JSON string so the transcript shows the tool's own text instead of a wrapper.

## Sub-agent links

Codex collaboration items are mapped into harness-neutral `SubagentLink` values before they leave
the adapter. Both native spawning protocols are supported:

- legacy `multi_agent_v1` is exposed by the app server as a `collabAgentToolCall` whose tool is
  `spawnAgent`; its start event has no receiver, so the adapter links the child on completion and
  preserves the supplied prompt as `initial_prompt`. `agentsStates` is keyed by native thread id;
  the adapter reads only the linked receiver's state. Single-child `sendInput`, `wait`,
  `resumeAgent`, and `closeAgent` calls also carry lifecycle links, while a multi-child `wait`
  remains unlinked rather than attributing aggregate state to one child; and
- current collaboration v2 is exposed as a completed `subAgentActivity`; the adapter preserves its
  related thread id, agent path, and action. The referenced thread is normally a child when the
  event has `kind = started`, but reverse activity in a child's transcript can reference its
  parent; the server resolves that direction from persisted ownership. Its activity title uses the
  final non-empty path component as the task name and does not expose the native child id; the
  complete path and id remain in link metadata. This event does not contain the delegated prompt,
  so the server uses its explicit `Sub-agent turn` fallback rather than misidentifying an inherited
  parent turn as the task.

The server materializes the child from either representation and installs one long-lived native event
owner for it. Parent lifecycle evidence establishes or refreshes the relationship only; it does not
start a second monitor, synthesize a fallback turn, or stop the owner after a timeout. The browser
addresses links by Giskard parent-thread and item IDs; the server resolves native routing metadata
from its authoritative item, and native thread IDs are redacted from browser-facing sub-agent
payloads. Linked children use identity-only claims. Materializing or reattaching one does not call
`thread/resume`: the child is provider-owned, and observing it must not nudge native work. The
adapter records parentage attested by sub-agent link events and returns that evidence with the
claim, preserving mismatched-parent and reverse-link validation without a read RPC. Primary
threads retain their separate fresh-session recovery when a stored native rollout is genuinely
gone. Sub-agent threads are always read-only;
matched provider-request responses, active-work interrupt, and command termination remain supported.
See
[Sub-agent threads](../../docs/subagents.md) for the complete lifecycle and ownership contract. When
opening or resuming a primary Codex thread, the adapter also maps
`thread.agent_nickname` to
`ThreadHandle.agent_name`; Giskard uses that harness-neutral name to title imported sub-agent
threads and their Sub-agents card entries. It maps `thread.parent_thread_id` to
`ThreadHandle.parent_harness_thread_id` as a validation signal: the server accepts a proposed
Giskard parent only when it agrees with this native parent when Codex supplies one. Reverse
child-to-parent activity therefore remains transcript navigation and cannot reparent the real
parent thread.

Codex thread deletion is idempotent only for the exact JSON-RPC `-32600` response `no rollout found
for thread id <requested-id>`. That response proves the requested native rollout is already absent,
so the adapter returns success and lets Giskard remove stale local metadata. A different native ID,
JSON-RPC code, timeout, authentication failure, or any other transport error remains an error.

## Command item ID versus process ID

A Codex command execution item can contain both:

```text
id        = logical item ID / tool call_id
processId = underlying process control ID
```

These identifiers are not interchangeable:

- The item ID updates the transcript item in its originating turn.
- The process ID sends input to or terminates the underlying process.
- A host OS PID is not accepted by the Codex process-control APIs.

Giskard retains both the Giskard `ItemId` and the Codex `processId` in running
command state. Selecting a task uses the item identity; stopping it uses the
process identity.

## Commands that outlive a turn

Each loaded Codex thread owns a unified-exec process manager shared across its
turns. Codex registers a live process in that manager before the initial command
wait yields, allowing the process to survive turn interruption or completion.

When the command remains live, Codex reports a model-facing session ID:

```text
Process running with session ID 12345
```

That value is the unified-exec `processId`. A later turn can interact with it
through `write_stdin`:

```json
{
  "session_id": 12345,
  "chars": ""
}
```

The later `write_stdin` invocation has its own tool call ID. It does not replace
the original command item identity. Output and final completion for the process
remain associated with the original command call ID and originating turn.

```text
Turn A: command item call_7 starts process 12345
Turn A: turn completes or is interrupted while process 12345 remains live
Turn B: write_stdin(session_id = 12345)
Later: item/completed(call_7, processId = 12345) updates the Turn A item
```

The adapter keeps draining Codex messages while it knows any command is running
so that this late completion can clear the running-task state.

## Background terminal discovery

`thread/backgroundTerminals/list` returns live unified-exec entries for a loaded
Codex thread. Each entry contains both:

```text
itemId    = original command item ID / call_id
processId = numeric unified-exec process ID
```

The process ID is the control handle. The item ID only links the process back to
its transcript item.

The list operation is the authoritative live inventory. A process ID retained
in old transcript history does not prove that a controllable process still
exists. Giskard currently relies on streamed command lifecycle events and does
not reconcile its running-command registry from this list operation.

## Process termination

Giskard sends `TerminateCommand { thread_id, process_id }` to the adapter. The
adapter must never implement command stop by interrupting the entire turn.

### Unified-exec commands

Use:

```text
thread/backgroundTerminals/terminate(threadId, processId)
```

The `processId` is numeric. Despite the API name, Codex registers the process
before the initial command wait completes, so this operation can terminate:

- a command still executing in the current active turn;
- a command that has yielded a session ID;
- a command that outlived its originating turn.

The operation terminates only the command process. It does not interrupt the
turn.

### Standalone app-server commands

Commands started directly with `command/exec` belong to a separate process
store and use a client-supplied process ID:

```text
command/exec/terminate(processId)
```

This operation cannot terminate an agent unified-exec command. Conversely,
`thread/backgroundTerminals/terminate` cannot terminate a standalone
`command/exec` process.

The current adapter uses a numeric process ID as the unified-exec discriminator.
For numeric IDs, it tries background-terminal termination first. If Codex
returns `terminated: false` or an error, the adapter currently tries
`command/exec/terminate`; nonnumeric IDs go directly to `command/exec/terminate`.
The fallback crosses two independent Codex process stores and therefore cannot
terminate the same unified-exec process. Tracking the process backend explicitly
or reconciling against `thread/backgroundTerminals/list` would remove this
heuristic.

## User attachments

Giskard receives browser attachments as transient `UserAttachment` values on
`UserInput::Text`. The adapter maps them before `turn/start`:

- image attachments are sent as Codex `UserInput::Image` values with
  `data:<mime>;base64,<bytes>` URLs;
- other files, including PDFs, are uploaded to the Codex app-server host with
  `fs/createDirectory` and `fs/writeFile`, then the harness-host path is appended
  to the text prompt.

Each turn uses a randomized upload directory under the harness host temp
directory, not the project workspace. The adapter removes it through
`fs/remove` when the turn ends and also cleans up partial uploads, a failed
`turn/start`, stream loss, command/control channel closure, and shutdown.
Cleanup failures are logged but do not replace the turn result. Giskard omits
raw attachment bytes from persisted history and the parsed in-memory history
cache.

Harness shutdown is completion-based: every caller waits until the single worker has cleaned active
turn uploads and closed the Codex transport. Transport shutdown is bounded; on timeout the adapter
drops the transport and then reports the worker complete. Concurrent and repeated shutdown calls
share that same completion and do not start another teardown. Initiation uses a dedicated
idempotent signal rather than the bounded command queues, so cancelling the initiating caller does
not cancel worker teardown.

## Permission presets

Giskard sends Codex `turn/start` overrides for every turn. Plan/Build mode maps
only to Codex collaboration mode (`plan` or `default`); it does not select the
sandbox. The thread permission preset selects Codex's built-in permission
profile and permission preset:

| Giskard preset | Codex `permissions` | Codex `approvalPolicy` |
| --- | --- | --- |
| `ask_first` | `:read-only` | `on-request` |
| `auto_approve` | `:workspace` | `on-request` |
| `full_access` | `:danger-full-access` | `never` |

`turn/start` must not include `sandboxPolicy` when it includes `permissions`;
Codex treats those fields as mutually exclusive.

After initialization, the adapter calls `config/read` with the project's
effective working directory and caches the configured extra
`sandbox_workspace_write.writable_roots` for that app-server process. Auto
Approve turns send `runtimeWorkspaceRoots` alongside `permissions:
":workspace"` because Codex binds the symbolic `:workspace_roots` entries in
that profile to the runtime roots supplied by the client. The runtime roots are
rebuilt per turn from the thread's opened workspace root plus those configured
extras, so an isolated worktree thread remains writable in its worktree rather
than in the project's checkout, while configured Cargo, sccache, Docker, and
similar external roots remain writable. A failed or unsupported config read is
logged as a warning and only omits configured extra roots; the thread workspace
root is still sent for Auto Approve. Ask First remains read-only, and Full
Access does not need additional roots.

## Model catalog (`model/list`)

The adapter advertises the `model_listing` capability and implements
`list_models` against the app-server `model/list` RPC. Like the MCP-status
listing, the request runs as a control command on the worker queue
(`handle_list_models`), paginating with the response cursor until exhausted.

Each returned model is mapped to a Giskard `ModelDescriptor` (`map_model`):

- **Display name** — Codex's friendly `display_name` is carried through, so the
  picker can show it instead of the raw slug.
- **Reasoning efforts** — the model's `supported_reasoning_efforts` are preserved
  verbatim (Codex `ReasoningEffort` is a bare string). Codex exposes the default
  separately from the selectable alternatives. If the alternatives list is empty
  and `default_reasoning_effort` is not `none`, the adapter inserts that default as
  the sole Giskard effort, matching the Codex TUI. An empty alternatives list with
  a `none` default maps to no reasoning-effort support.
- **Hidden models** are filtered out (only picker-visible entries are returned).
- **Attributed provider** — the `model/list` catalog is provider-agnostic (a
  bare model slug, no provider), but Giskard routes by `(provider, model)`, so an
  unattributed descriptor could only ever enrich an entry some other source
  produced — leaving a stock Codex, whose built-in providers have no `base_url`
  to discover against, with an empty picker. The adapter therefore issues a
  second `config/read` (with the project `cwd`, since config is layered per
  directory) and attributes every entry to the provider Codex routes to.
  - An **absent or empty** `model_provider` means the `openai` built-in, which is
    Codex's own default.
  - A **failed** `config/read` fails the whole listing rather than guessing:
    defaulting there would attribute every model to `openai` for a user routing
    elsewhere, inventing picker entries that cannot work, and nothing downstream
    can tell a guessed attribution from a real one.
- **Conservative context window** — `model/list` omits the context window, so
  descriptors use the conservative default; the catalog is a source of names and
  reasoning-effort levels only, not gauge sizing.
- **Default model** — Codex's `is_default` is carried through, so the server can
  seed a new draft's starting model from the catalog rather than storing one per
  project.

The server overlays this metadata onto the configured/discovered model list by
model id (see `giskard-server` §8.3): config names win, and reasoning efforts
fill in for models the config did not explicitly declare.

## Provider table (`config/read`)

The adapter advertises the `provider_listing` capability and implements
`list_providers` from the same `config/read` RPC used for writable roots, run as
a control command on the worker queue (`handle_list_providers`). Codex owns
provider configuration, so Giskard reads it back instead of asking the user to
restate it in `config.toml`.

`config/read` returns the whole effective config, and the app-server `Config`
type forwards every key it does not model itself. `[model_providers]` therefore
arrives as an unmodeled key, which the adapter's own `CodexConfig` picks up — the
generated `codex-codes` types omit it, which is why it is deserialized locally.

- **Per-directory, but the table is not** — the request carries the project's
  workspace root, as every other `config/read` here does. It does not change the
  provider table: `model_provider` and `model_providers` are both on Codex's
  `PROJECT_LOCAL_CONFIG_DENYLIST`, so a project-local `.codex/config.toml` has
  them stripped (with a startup warning) before the layers are merged, trusted
  directory or not. Providers therefore always come from the user-level,
  packaged-default, or enterprise-managed layers.

  That is what makes running `auth.command` safe to do here: a checked-in config
  in a cloned repository cannot introduce a provider, so it cannot introduce a
  command for Giskard to run.
- **Built-ins are added** — `[model_providers]` holds only user-declared entries,
  so `CODEX_BUILT_IN_PROVIDER_IDS` (`openai`, `amazon-bedrock`,
  `amazon-bedrock-runtime`, `ollama`, `lmstudio`) is merged in. Without them a
  project pinned to `openai` would look like an unknown provider.
- **Key location, never the key** — a provider's `env_key` becomes
  `ProviderAuth::Env`, and `[model_providers.<id>.auth]` becomes
  `ProviderAuth::Command`, carrying the command, args, cwd and timeout so
  Giskard can run it when it needs a discovery token.
  `experimental_bearer_token` is not reported: an inline secret stays in Codex's
  config rather than being copied into another process.
- **`auth` wins over `env_key`** — Codex's own `ModelProviderInfo::validate`
  rejects a provider declaring both, so at most one is ever present. Preferring
  `auth` keeps a config Codex would refuse to load from authenticating discovery
  differently than it would authenticate a turn.
- **`refresh_interval_ms` is not read** — Giskard reruns the command whenever it
  needs a token instead of caching one, so there is no cached token for an
  interval to age out. `timeout_ms` *is* read, defaulting to Codex's own 5 s.
- **Empty is absence** — Codex defaults an omitted `name` to `""`; the adapter
  normalizes empty strings to `None`.

## Version, for `/models` discovery

`initialize` answers with a `user_agent` like `codex_cli_rs/0.58.0 (Linux …) …`. The adapter keeps
the version out of it and reports it as `client_version()`.

This is the only place the running Codex states its own version over the protocol. It is not used
verbatim: the user agent carries the full `CARGO_PKG_VERSION`, while Codex's own
`client_version_to_whole` reduces the same version to `MAJOR.MINOR.PATCH` (its doc gives
`"1.2.3-alpha.4" -> "1.2.3"`). The suffix is dropped here so Giskard sends exactly what Codex
sends. Giskard sends it on discovery so a provider serving Codex's richer catalog
(`{"models": [...]}`, with `context_window` and `supported_reasoning_levels`) answers Giskard the
way it would answer Codex. Codex asks for that catalog whenever the provider uses command auth or
Codex's own backend; the metadata never reaches Giskard through `model/list`, which carries no
context window at all — hence fetching it directly.

A user agent that does not parse yields `None`, and the parameter is omitted rather than guessed.

The same version doubles as a protocol-drift signal. `codex-codes` publishes the Codex release its
own suite was last run against as `version::tested_cli_version()`, and the adapter warns once per
spawned app-server when the running Codex is strictly newer than that. The bindings' own
`check_codex_version` is not used: it shells out to `codex --version` on `PATH`, ignoring the
configured `codex_path`, and reports through the `log` crate, which Giskard does not bridge into
`tracing`. Drift is not fatal — protocol additions the bindings do not model arrive as unknown
notifications and requests — but it is the first thing to check when Codex behavior looks
truncated.

## Resume model verification

`thread/resume` treats `model`/`modelProvider` as *overrides*: Codex's
`merge_persisted_resume_metadata` returns early once either is present, so the
thread's own persisted model stops being applied. Supplying one therefore moves
an existing conversation onto a different model rather than expressing a
preference.

`OpenThreadOptions::initial_model` is authoritative for every Primary open. When the adapter issues
a native open:

- **Reopening** a thread Giskard already tracks passes its persisted model — that override is also
  the mechanism for switching a thread's provider.
- **Starting** a fresh thread uses the required model directly. Missing-rollout recovery starts its
  replacement with that same model and Giskard identity.

`thread/resume` also reports `reasoningEffort`, and a reported effort wins over a
requested one. `thread/start` reports none, so there the request is the only source. When Codex
reports an empty model or provider, the adapter logs and returns no effective model rather than
guessing.

## Runtime context window

Codex includes the effective context capacity in
`thread/tokenUsage/updated.tokenUsage.modelContextWindow`. This is the window Codex
actually applies after reserving any model-specific headroom, so it is authoritative
for the thread gauge even when it differs from a provider's raw advertised maximum.

During an active turn, the adapter emits `AgentEvent::ContextWindowUpdated` whenever
the valid reported value changes and suppresses consecutive unchanged repeats. Each
event carries the model selected for that turn.
The server persists accepted values per `(provider, model)` so they survive reloads
and model switches.

Existing threads initialize the gauge from Giskard's latest persisted runtime value
for the selected model. If none has been observed, they use provider/config metadata
or the conservative fallback. Codex may replay historical token usage after
`thread/resume`; that replay is not a new turn and is never folded into Giskard's
token ledger. After a successful resume that reports an authoritative model, the
adapter offers the first valid matching window through the bounded thread-update
channel with that model. The runtime registry accepts it only when no newer turn,
compaction, or deletion lifecycle superseded the open. This observation has no
time-based deadline.

An invalid or out-of-range `modelContextWindow` suppresses only the context-window
update. It never suppresses the turn's token usage, which is still attached on
`turn/completed`. While unresolved, a pending restore keeps the Codex message loop
awake so an arbitrarily late update can still be observed.

## Restart and unload behavior

The stdio transport is newline-delimited. If app-server stdout contains a
syntactically non-JSON line that does not begin like a JSON-RPC object, the
adapter logs the parse diagnostic, payload byte length, and an escaped preview
of up to 4 KiB. It then discards that single consumed line and continues
reading at the next frame boundary. This keeps obvious stdout contamination
from terminating every active turn and closing the project's worker while
retaining evidence for upstream diagnosis. Malformed object-like JSON,
parseable JSON with an invalid JSON-RPC envelope, and typed payload errors
remain fatal because discarding them could lose a real lifecycle event.

Unified-exec process entries are in memory and belong to the loaded Codex thread
session. Their process IDs:

- remain valid across later turns in that loaded session;
- are not persisted as resumable process handles;
- may be reused after process removal or restart;
- become stale when the Codex thread session or app-server exits.

Codex normally terminates stored unified-exec processes during thread/session
shutdown. If a host process survives an abnormal Codex exit, a new Codex process
cannot rediscover or terminate it through the background-terminal APIs.

Standalone `command/exec` processes are scoped to the app-server connection and
are terminated when their originating connection closes.

## Request and approval correlation

Codex server requests use their JSON-RPC request ID for protocol responses. The
adapter creates a Giskard `ApprovalId` or `ServerRequestId` for browser routing
and retains the original request ID in an in-memory pending-request registry.

The browser-facing ID is not a thread, turn, item, or process ID. For ordinary
browser responses, the adapter prepares the native response without mutation
and removes the exact pending correlation only after the transport reports a
successful write. A reported write failure therefore retains correlation for a
browser retry. After an accepted interrupt, failed best-effort approval
cancellation explicitly abandons that correlation because the stopped turn may
no longer be answered. Exact native request-ID checks prevent stale completion
from removing a replacement entry with the same browser-facing ID.

This is adapter-state retry safety, not proof that retrying is wire-safe after a
timeout. The current transport cannot distinguish no write from a partial frame
or a completed frame whose flush reported failure; that requires the planned
non-cancelling writer boundary.

Current Codex file-change approval requests identify the associated item but do
not carry its changed paths. With current Codex app-server ordering, the adapter
retains the structured changes from the preceding `item/started` file-change
item and refreshes them from any subsequent `item/fileChange/patchUpdated`
notification. It then exposes those paths as approval metadata. If no item
changes were supplied, Giskard logs the degraded request and the browser states
that Codex did not provide the file list. An optional grant root remains
separately labeled permission-scope metadata and is never presented as a changed
target.

## Code and tests

- [`src/mapping.rs`](src/mapping.rs) owns native-to-Giskard identity translation and command
  lifecycle tracking.
- [`src/instance.rs`](src/instance.rs) defines the single-task app-server runtime that owns protocol
  state and reduction for all native threads on that process.
- [`src/lib.rs`](src/lib.rs) owns the public harness handle, low-level JSON-RPC helpers, timeouts,
  and process termination calls.
- Mapper tests assert same-lifecycle stability, cross-turn and cross-thread
  separation, and independent running commands when Codex reuses an item ID.
- Worker tests assert background-terminal and `command/exec` termination routing
  and verify that process termination never falls back to turn interruption.
