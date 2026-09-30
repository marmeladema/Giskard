# Milestone 1 implementation plan: crate, fixtures, output mapper

Implements milestone 1 of [`../claude-code-harness-plan.md`](../claude-code-harness-plan.md)
(§11). This plan is written for an implementing agent. Every file, symbol and behaviour below was
verified against `main` at `0404ec8`, against Claude Code **2.1.286** driven over a stdio pipe,
and against the `claude-codes` **2.1.286** crate source. Line numbers are for orientation; the
symbol quoted beside each is the thing to find.

Read `AGENTS.md` first. Its rules that bind this milestone: `cargo fmt --all` and
`cargo clippy --all-targets -- -D warnings` must pass; no `unwrap`/`expect`/`panic!` on runtime
paths; nothing is dropped, coalesced or recovered from silently — every skipped frame is logged
with enough context to diagnose it; every failure mode gets a test; the `AGENTS.md` and
`README.md` crate lists are updated in the same change; Markdown prose is wrapped at 100 columns
(table rows may run longer, as the plan's own do).

## Outcome

After this milestone the workspace has a ninth crate, `giskard-harness-claude`, that compiles, is
linted and is tested in CI, and that contains **the pure mapper** from Claude Code's stream-json
output to Giskard's `AgentEvent`s, driven by recorded protocol fixtures. Nothing spawns a process,
nothing implements `AgentHarness`, and no `[harnesses.<name>]` declaration can name the kind yet.
The commit is one unit: the crate, its fixtures, the mapper with its tests, and the documentation
that names the crate.

## Scope

One commit, built in this order so that each step compiles on its own:

1. Crate skeleton and workspace wiring.
2. The fixtures, moved from this plan's directory into the crate.
3. The frame layer: one raw JSON line to a typed `Frame`, tolerant of everything the crate cannot
   type.
4. The mapper: `Frame` in, `AgentEvent`s and control replies out, with the turn, item and task
   state one child needs.
5. Identifier and logging conventions, the crate README, and the two crate lists.

## Non-goals

No child process, no transport, no `AgentHarness` implementation, no `HarnessKind`, no server
change, no spec change. No sub-agent routes: frames carrying a `parent_tool_use_id` are routed only
if a route for that id exists, and no code in this milestone creates one (milestone 5 does). No
approval *responses*: the mapper produces `ApprovalRequested` and records what a reply needs, but
`respond_approval` is milestone 3. No `--include-partial-messages` decision: the mapper handles a
turn with and without `stream_event` frames, and milestone 2 chooses the flag.

## Step 1: crate skeleton and workspace wiring

Files, in the shape of `crates/giskard-harness-codex` and `crates/giskard-harness-replay`:

- `Cargo.toml` (root): add `"crates/giskard-harness-claude"` to `[workspace] members` (line 3–13)
  and `giskard-harness-claude = { path = "crates/giskard-harness-claude" }` to
  `[workspace.dependencies]` (beside line 28's codex entry). `Cargo.lock` changes with it and is
  committed: CI runs `cargo test --workspace --locked`.
- `crates/giskard-harness-claude/Cargo.toml`, mirroring the codex manifest
  (`crates/giskard-harness-codex/Cargo.toml`): `version.workspace`, `edition.workspace`,
  `license.workspace`, `rust-version.workspace`, `readme = "README.md"`. Dependencies:
  `giskard-core`, `giskard-harness`, `serde`, `serde_json`, `chrono`, `tracing = "0.1"`,
  `thiserror` (all `{ workspace = true }` where the workspace defines them), and
  `claude-codes = { version = "=2.1.286", features = ["async-client"] }`. Pin exactly, as
  `codex-codes` is pinned: the crate's version tracks the CLI release it models (plan §3.7).
  Dev-dependency: `tracing-subscriber = { workspace = true }` for log-capture tests. `tokio` and
  `async-trait` are not needed until milestone 2; do not add them now.
- `cargo deny check` stays green without a `deny.toml` change: `claude-codes` is Apache-2.0 (on the
  allow list at `deny.toml:21`), MSRV 1.85, edition 2021, and its `async-client` feature pulls
  `anyhow`, `tokio`, `log`, `uuid` and `which`, all permissively licensed. Verify with the command
  in *Verification* rather than by assumption.
- `crates/giskard-harness-claude/src/lib.rs` with private modules `frame`, `mapper`, `ids`,
  `log_fields`, and `pub use` of exactly what milestone 2 will need: `ClaudeMapper`,
  `MapperOutput`, `Frame`, `Route`, and the capability constant below. Keep every Claude-specific
  type inside this crate, as `AGENTS.md` requires of Codex types.
- `pub const CAPABILITIES: HarnessCapabilities` (or a `pub fn capabilities()`), the full §4 matrix
  of the plan, with `live_approvals`, `plan_build_modes`, `per_turn_model` and `reasoning_effort`
  **false** for now: they turn true in milestone 3 when the paths behind them exist (plan §11,
  milestone 2). The rest per §4: `structured_diffs: false`, `resumable_threads: true`,
  `model_listing: true`, `provider_listing: true`, `token_usage: true`, `mcp_status: true`,
  `mcp_reload: false`, `mcp_oauth_login: false`, `context_compaction: true`,
  `turn_steering: false`. `HarnessCapabilities` derives `Default` (all false) at
  `crates/giskard-harness/src/lib.rs:25-57`, so build it with struct-update syntax.
- `src/log_fields.rs`: copy `display_opt` from `crates/giskard-harness-codex/src/log_fields.rs`
  (19 lines) rather than sharing it; the two adapters are independent crates by design.

## Step 2: the fixtures

`specs/claude-code-harness-plan/fixtures/` holds thirteen scenarios recorded against 2.1.286,
sanitized, with a README that documents each one, the recorder's argv and the sanitization. Move the
whole directory with `git mv` to `crates/giskard-harness-claude/tests/fixtures/` (the README
included; its first paragraph already says this is where it ends up, so update that sentence), and
delete nothing else. Then change the two sentences in `specs/claude-code-harness-plan.md` §11
(*Milestone 1*) that say the fixtures live "in the crate's `tests/fixtures/`" and the README's
"until then it lives beside the plan" so both name the final location only.

The scenarios and the frames each contains (the README's table has the policies and stop rules):

| Fixture | Frames the mapper must handle from it |
| --- | --- |
| `initialize.out.jsonl` | two `control_response` frames: the `initialize` response with `models`, `account`, `current_permission_mode`, and the `list_models` response with the same `models` array |
| `text-turn.out.jsonl` | `system/init`, `system/status {status:"requesting"}`, the full `stream_event` sequence (`message_start`, `content_block_start` for a `thinking` then a `text` block, `content_block_delta` with `thinking_delta`, `signature_delta` and `text_delta`, `content_block_stop`, `message_delta` with `usage`, `message_stop`), two `assistant` frames for one `message.id` (one block each), `rate_limit_event`, `result` |
| `tool-allowed.out.jsonl` | `assistant` with a `tool_use` block for `Bash`, `control_request can_use_tool`, `user` with a `tool_result` whose `tool_use_result` is `{stdout, stderr, interrupted, isImage, noOutputExpected}`, `result` |
| `tool-denied.out.jsonl` | the same ask, then `user` with `tool_result {is_error: true, content: "Declined"}`, `tool_use_result: "Error: Declined"` and `tool_result_meta: [{id, non_execution_kind: "permission-rule"}]`; `result.permission_denials` lists the denied call |
| `accept-for-session.out.jsonl` | an ask whose `permission_suggestions` holds one `addRules` entry with `destination: "localSettings"`, then three completed `Bash` calls with one ask |
| `cancel.out.jsonl` | after a deny with `interrupt: true`: `user tool_result {is_error: true}` with the rejection wording and `non_execution_kind: "user-rejected"`, a `user` frame with the text block `[Request interrupted by user for tool use]`, and a `result` with `subtype: "error_during_execution"`, `is_error: true`, `stop_reason: "tool_use"` and the call in `permission_denials` |
| `delegation.out.jsonl` | a **foreground** delegation: `assistant tool_use Agent` (input has `run_in_background`), `system/task_started {task_type: "local_agent", is_backgrounded: false, tool_use_id}`, the child's `assistant`/`user` frames with `parent_tool_use_id` set, `task_progress`, `task_updated {patch: {status: "completed"}}`, `task_notification`, the parent's `tool_result`, one `result` with `subagent_stats` |
| `delegation-interrupted.out.jsonl` | a **backgrounded** delegation: `system/background_tasks_changed`, `task_started {is_backgrounded: true}`, the parent's `tool_result` "Async agent launched successfully", a first `result`, the child's `tool_use` with `parent_tool_use_id`, then after the interrupt `task_updated {status: "killed"}`, `task_notification {status: "stopped"}`, the `control_response` to the interrupt, the child's rejection `tool_result` and interruption text, and **no second `result`** |
| `compact.out.jsonl` | a text turn's `result`, then for `/compact`: `system/status {status: "compacting"}`, `system/status {status: null, compact_result: "success"}`, a re-emitted `system/init`, `system/compact_boundary {compact_metadata: {trigger: "manual", pre_tokens, post_tokens, cumulative_dropped_tokens, duration_ms}}`, and a degenerate `result` with `stop_reason: null`, `num_turns: 0`, `result: ""` |
| `plan-exit-denied.out.jsonl` | `assistant tool_use Write` to the plans directory with `tool_use_result {type: "create", filePath, …}`, a `ToolSearch` tool call, `control_request can_use_tool` for `ExitPlanMode` with `permission_suggestions: null`, the denial's `tool_result`, and a `result` whose `permission_denials[0]` carries `tool_input.plan` and `planFilePath` |
| `background-bash.out.jsonl` | `assistant tool_use Bash` with `run_in_background: true`, `system/background_tasks_changed`, `task_started {task_type: "local_bash", is_backgrounded: true}`, a `tool_result` "Command running in background with ID", the turn's `result`; then `task_updated {status: "completed"}`, `task_notification`, a re-emitted `system/init`, an `assistant` text "Background task completed." and a **second `result`** for a continuation the CLI started on its own |
| `resume-missing.out.jsonl` | one `result` with `subtype: "error_during_execution"`, `is_error: true`, `num_turns: 0`, `stop_reason: null`, no `result` text; `resume-missing.stderr.txt` holds `No conversation found with session ID: <uuid>` |
| `autocompact-state.out.jsonl` | two **top-level** frames, `{type: "active_goal", value: null}` and `{type: "autocompact_state", value: {enabled, effective_window, threshold, enforced, source}}`, which the crate cannot type |

Two shapes to know before writing a test:

- `assistant` frames arrive **one content block per frame**, each carrying the whole message
  envelope with the same `message.id`; `stop_reason` on them is `null`. So an item is keyed by
  `(message.id, block index)` for text and thinking, and by the block's own `id` for `tool_use`.
- A `thinking` block in an `assistant` frame has `thinking: ""` and a `signature` (the recordings
  ran with thinking display omitted). The mapper emits no `Reasoning` item for an empty thought.

## Step 3: the frame layer (`src/frame.rs`)

The crate's types are used **per frame, after peeking**, never by deserializing a whole line into
`claude_codes::ClaudeOutput`. The reason is measured, not theoretical (plan §3.7): `ClaudeOutput`
has no fallback variant, so an unknown top-level `type` fails the line, and two real frames in the
fixtures are exactly that (`autocompact_state`, `active_goal`); `ControlRequestPayload` types five
subtypes, so a `request_user_dialog` or `rename_session` ask fails the line; and a missing required
field (`AssistantMessageContent.id`, `ThinkingBlock.signature`, `ResultMessage.total_cost_usd`)
fails the line. A stream that dies on its first unknown frame is the failure mode `AGENTS.md`'s
"do not silently drop" rule is about, in the other direction: the adapter must neither drop
silently nor die.

```rust
/// One stdout line, classified. Typed where the crate types it, raw where it does not.
pub enum Frame {
    Init(claude_codes::InitMessage, serde_json::Value),
    Status(claude_codes::StatusMessage),
    TaskStarted(claude_codes::TaskStartedMessage),
    TaskUpdated(claude_codes::TaskUpdatedMessage),
    TaskNotification(claude_codes::TaskNotificationMessage),
    CompactBoundary(claude_codes::CompactBoundaryMessage),
    ApiRetry(claude_codes::ApiRetryMessage),
    PermissionDenied(claude_codes::PermissionDeniedMessage),
    SessionTitleChanged(claude_codes::SessionTitleChangedMessage),
    /// `system` subtypes this milestone reads but does not act on, kept for the debug log.
    SystemIgnored { subtype: String },
    Assistant(claude_codes::AssistantMessage),
    User(claude_codes::UserMessage),
    Stream(StreamEvent),
    Result(claude_codes::ResultMessage),
    RateLimit(claude_codes::RateLimitEvent),
    AutocompactState { effective_window: u64, threshold: Option<u64> },
    CanUseTool {
        request_id: String,
        request: claude_codes::ToolPermissionRequest,
        agent_id: Option<String>,
        raw: serde_json::Value,
    },
    ControlRequest { request_id: String, subtype: String, raw: serde_json::Value },
    ControlResponse { request_id: String, raw: serde_json::Value },
    Unknown { r#type: String, subtype: Option<String> },
}
```

Rules:

- `Frame::parse(line: &str) -> Result<Frame, FrameError>`: `serde_json::from_str::<Value>` first
  (a non-JSON line is `FrameError::NotJson` and the caller logs it at `warn` with the line's byte
  length, never its content); then read `type`, and for `system` and `control_request` the
  `subtype`; then `serde_json::from_value` into the named struct. A typed conversion that fails
  yields `FrameError::Untyped { r#type, subtype, error }`, which the mapper logs at `warn` and
  treats as `Unknown`. `Init` keeps the raw `Value` beside the typed struct because
  `system/init.capabilities` is read from it later (milestone 8) and because `apiKeySource` is
  what the plan's §7 signal reads.
- Type paths: `InitMessage`, `StatusMessage`, the `Task*Message`s, `CompactBoundaryMessage`,
  `ApiRetryMessage`, `PermissionDeniedMessage`, `SessionTitleChangedMessage`, `RateLimitEvent`,
  `ToolPermissionRequest`, `UsageInfo`, `SystemMessage` and `StreamEventMessage` are re-exported at
  the crate root. `AssistantMessage` and `UserMessage` (`src/io/message_types.rs`) and
  `ResultMessage` (`src/io/result.rs`) are **not**; import them through the `io` module path the
  crate exposes (check `src/lib.rs` for the public path before writing the `use`).
- Typed system subtypes come from `claude_codes::SystemMessage` and its accessors
  `as_init`, `as_status`, `as_compact_boundary`, `as_task_started`, `as_task_updated`,
  `as_task_notification`, `as_thinking_tokens`, `as_session_title_changed`
  (`claude-codes/src/io/message_types.rs:1339-1550`). Note that each accessor returns `None` on a
  parse failure without saying why; call `serde_json::from_value` on `SystemMessage.data` directly
  so the error reaches the log. `background_tasks_changed`, `task_progress`, `thinking_tokens` and
  `post_turn_summary` are `SystemIgnored`.
- `stream_event.event` is an untyped `Value` in the crate (`StreamEventMessage`), so `StreamEvent`
  is this crate's own small enum: `MessageStart { message_id }`, `ContentBlockStart { index,
  block: BlockStart }` with `BlockStart::{Text, Thinking, ToolUse { id, name }, Other(String)}`,
  `ContentBlockDelta { index, delta: Delta }` with `Delta::{Text(String), Thinking(String),
  InputJson(String), Signature, Other(String)}`, `ContentBlockStop { index }`,
  `MessageDelta { stop_reason: Option<String>, usage: Option<claude_codes::UsageInfo> }`,
  `MessageStop`, `Other(String)`. Each carries `parent_tool_use_id: Option<String>` from the
  envelope. The `text-turn` fixture holds every one of these.
- `CanUseTool` reads `agent_id` from the raw request because `ToolPermissionRequest` has no such
  field (plan §3.7.1); the typed struct provides `tool_name`, `input`, `permission_suggestions`,
  `blocked_path`, `decision_reason`, `tool_use_id`. Keep `raw` too: milestone 3 echoes a suggestion
  back verbatim with its `destination` rewritten, and the typed `PermissionSuggestion` drops keys
  such as `directories`.
- `ControlRequest` covers every other inbound subtype (`request_user_dialog`, `rename_session`,
  `hook_callback`, `mcp_message`, …) without deserializing the payload.
- `AutocompactState` is a top-level frame `{type: "autocompact_state", value: {...}}`; read
  `value.effective_window` and `value.threshold`. `active_goal` is `Unknown` and logged at `debug`.

Tests for this step (in `frame.rs`, `#[cfg(test)]`): every line of every fixture parses to a
non-`Unknown` frame except the two top-level frames of `autocompact-state.out.jsonl`
(`autocompact_state` → `AutocompactState`, `active_goal` → `Unknown`) and the `SystemIgnored`
subtypes; a non-JSON line is `NotJson`; a `system` frame with an unknown subtype is `Unknown` with
both names filled; an `assistant` frame missing `message.model` is `Untyped` naming the type.

## Step 4: the mapper (`src/mapper.rs`)

`ClaudeMapper` is a pure state machine in the shape of `CodexMapper`
(`crates/giskard-harness-codex/src/mapping.rs:94-160`): frames in, outputs out, no I/O, no clock
except `Utc::now()` for `Item.created_at`. One mapper serves one child process, which is one
primary thread plus any `task:` child routes milestone 5 adds.

### Construction and routes

```rust
pub struct ClaudeMapper { /* private */ }

/// Where a frame's items belong: the child process's own thread, or a sub-agent route.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Route { Primary, Task /* by tool_use_id, milestone 5 */ }

impl ClaudeMapper {
    pub fn new(thread: ThreadId, harness_thread_id: String, workspace_root: PathBuf) -> Self;
    /// The adapter opened a turn for a user message it just wrote. Returns the `TurnStarted`.
    pub fn begin_turn(&mut self, turn: TurnId, kind: TurnKind) -> Vec<MapperOutput>;
    /// The adapter sent `interrupt`; the next error-shaped result is `Interrupted`, not `Failed`.
    pub fn note_interrupt_sent(&mut self);
    /// The model the adapter asked for on this turn, so `TurnUsageUpdated.model` can be set.
    pub fn note_turn_model(&mut self, model: ModelRef);
    pub fn map(&mut self, frame: Frame) -> Vec<MapperOutput>;
    pub fn active_turn(&self) -> Option<TurnId>;
}

pub enum TurnKind { User, Compaction }

pub enum MapperOutput {
    Event(AgentEvent),
    /// A control_response the adapter must write, needing no user: a denied `ExitPlanMode`.
    Reply(serde_json::Value),
    /// A control_response to a request the adapter sent; it correlates the waiter itself.
    ControlResponse { request_id: String, payload: serde_json::Value },
    /// An approval the adapter must remember until `respond_approval` (milestone 3).
    PendingApproval { id: ApprovalId, request_id: String, tool_use_id: Option<String> },
    /// A server request the adapter must remember until `respond_server_request` (milestone 3).
    PendingServerRequest { id: ServerRequestId, request_id: String },
}
```

`thread` is the `ThreadId` of the primary route; every emitted event names it. Frames whose
`parent_tool_use_id` is `Some` and matches no claimed route are dropped with a `debug` log carrying
`thread_id`, `parent_tool_use_id` and the frame type, because without `--forward-subagent-text`
they should not arrive, and with it (milestone 5) they belong to a route. Do not attribute them to
the primary thread: that would render a sub-agent's tool calls as the main agent's (plan §5.3).

### State the mapper owns

Group by lifetime, with a doc comment listing the classes, as `CodexMapper` does:

- **Per turn** (cleared when the turn completes): the active `TurnId` and its `TurnKind`; the
  requested model; the last usage seen (`TokenUsage`); `open_agent_tasks: HashSet<task_id>`;
  `held_result: Option<ResultMessage>` (a `result` that arrived while agent tasks were open);
  `interrupt_sent: bool`; `denied_tool_use_ids` (tool-use ids whose ask was answered deny, so the
  completion is `declined`, not `failed`).
- **Per item, within the turn**: `item_ids: HashMap<NativeItemKey, ItemId>` where
  `NativeItemKey` is `ToolUse(String)` (the block's `id`, which `can_use_tool.tool_use_id` and
  `tool_result.tool_use_id` reference) or `Block { message_id: String, index: u32 }`; plus for each
  open tool item the `ItemStart` data needed to complete it (`name`, `input`, `command`/`cwd`).
- **Per session** (lives as long as the mapper): `session_model: Option<String>`,
  `permission_mode: Option<String>` (from `init` and `status`), `effective_window: Option<u32>`
  (from `AutocompactState`), `task_kinds: HashMap<task_id, TaskType>` (a task's type is only on
  `task_started`; `task_updated` carries the id alone).

### Turn boundaries

- `begin_turn(turn, kind)` records the active turn and returns `TurnStarted { thread, turn }`.
  Calling it while a turn is active is a caller bug: log at `error` and return the event anyway;
  the previous turn is completed as `Failed` with the message "superseded by a new turn" first.
- **A turn the CLI starts on its own.** After a backgrounded task's `task_notification` the CLI
  re-emits `system/init` and runs a continuation turn with no user message
  (`background-bash.out.jsonl`, second `result`; `delegation-interrupted` shows the same start
  without the continuation). When an `Assistant`, `Stream` or `Result` frame arrives with no active
  turn, the mapper opens one itself: mint `TurnId::new()`, emit `TurnStarted`, and log at `info`
  with `action = "external_turn"`. The server already supports this: the event forwarder claims a
  turn it did not intend as an *external turn* with `ExternalTurnDefaults`
  (`crates/giskard-server/src/registry/thread.rs:71`, `event_forwarder.rs:1663-1666`), minting the
  id at `event_forwarder.rs:2467` when the harness's `TurnStarted` arrives first.
- **`Result` completes the turn**, with two exceptions that both come from `task_started`:
  - if `open_agent_tasks` is non-empty, the result is held (`held_result`) and nothing is emitted;
    only `task_type: "local_agent"` tasks are in that set. A `local_bash` task never gates
    completion (`background-bash`: the turn completes at its first `result` while the task runs);
  - a terminal `task_updated` (`status` in `completed`, `failed`, `killed`, `stopped`) removes its
    task. If the set becomes empty and a result is held: for `completed`, keep holding, the CLI's
    continuation `result` completes the turn (`delegation-interrupted` shows the notification;
    the plan's §5.3 table shows the second `result`); for `killed`, `failed` or `stopped`, complete
    the turn now from the held result with status `Interrupted` (if `interrupt_sent`) or `Failed`,
    because no second `result` comes (`delegation-interrupted.out.jsonl` ends without one).
  - the **foreground** delegation (`delegation.out.jsonl`) never holds: its `task_updated
    completed` arrives before the single `result`, so the set is empty when the result lands.
- **Status mapping** for a completing `Result`: `is_error: false` → `Completed`; `is_error: true`
  with `interrupt_sent` or `terminal_reason` in `aborted_streaming` / `aborted_tools` →
  `Interrupted`; otherwise `Failed` with `message` from `result.result`, else `errors.join("; ")`,
  else the subtype. `TurnStatus.message` is `None` on `Completed`. The `cancel` fixture (deny with
  interrupt, `error_during_execution`, `stop_reason: "tool_use"`) is `Interrupted` only because the
  adapter marks it so through `note_interrupt_sent`: milestone 3 calls it when it sends a `Cancel`;
  the mapper test does.
- **The compaction turn** (`TurnKind::Compaction`, opened by milestone 3's `compact_thread`):
  `status compacting` emits nothing; `compact_boundary` emits an `ItemCompleted` with
  `ItemPayload::Activity { title: "Context compacted", detail: Some("<pre> → <post> tokens
  (manual)"), metadata: Some(compact_metadata as JSON), subagent: None }` and
  `harness_item_id = "compact_boundary:<uuid>"`; the degenerate `result` (`stop_reason: null`,
  `num_turns: 0`, empty `result`) completes the turn as `Completed` with **no** `AgentMessage`
  item, which is the rule plan §4 states for `context_compaction`. A re-emitted `init` inside any
  turn only refreshes the per-session fields.

### Usage and context window

- `TokenUsage::new(input, output)` where `input = usage.input_tokens +
  usage.cache_creation_input_tokens + usage.cache_read_input_tokens` and
  `output = usage.output_tokens` (plan §6). `text-turn`'s result gives `10 + 7149 + 14811 = 21970`
  in and `53` out; a test pins that arithmetic.
- `TurnUsageUpdated { thread, turn, usage, context_window, model }` is emitted from every
  `MessageDelta` that carries `usage` (live, mid-turn) and once more just before `TurnCompleted`
  from `result.usage`. `context_window` is `effective_window` when the session reported one,
  else `result.modelUsage[<session_model>].contextWindow` once a result has been seen, else
  `None`. `model` is `Some(ModelRef { provider: "anthropic", model, reasoning_effort: None })`
  only when `note_turn_model` was called for this turn. Consecutive identical `(usage, window)`
  pairs are suppressed, as `CodexMapper` does through `emitted_usage`.
- `result.modelUsage` may carry a second model (plan §6). `AgentEvent` has no per-model usage
  channel, so the turn's usage is `result.usage` and every `modelUsage` entry is logged at `debug`
  with its tokens under `model = <id>`. Record that limitation in the README.

### Items

The item events, per block kind, with `ItemStart.kind` and the completion payload:

| Source | `ItemStarted` | `ItemDelta` | `ItemCompleted` |
| --- | --- | --- | --- |
| `text` block | `ContentBlockStart{Text}` → `AgentMessage`, id keyed `Block{message_id, index}`; without stream events, the `assistant` frame starts it | `Delta::Text` → `ItemDelta::Text` | the `assistant` frame with the block → `ItemPayload::AgentMessage { text }` (empty text still completes) |
| `thinking` block | started only when the first non-empty `Delta::Thinking` or a non-empty block arrives → `Reasoning` | `Delta::Thinking` → `ItemDelta::Text` | `assistant` frame → `Reasoning { text }`; an empty thought (the recorded shape) emits nothing at all |
| `tool_use` `Bash` | the `assistant` frame → `CommandExecution` with `CommandExecutionStart { command: input.command, cwd: workspace_root, status: Some("in_progress"), process_id: None, started_at_ms }`; id keyed `ToolUse(id)` | none (Claude streams no output) | the `user` `tool_result` with that `tool_use_id` → `CommandExecution { command, cwd, output, exit_code: None, status, .. }` where `output` is `tool_use_result.stdout` then `stderr` when it is that object, else the block's text content; `status` is `"declined"` when `tool_result_meta` has `non_execution_kind` or the id is in `denied_tool_use_ids`, `"failed"` when `is_error`, else `"completed"` |
| `tool_use` `Write`/`Edit`/`NotebookEdit` | `FileChange` with `command: None, tool: None` | none | `FileChange { path: input.file_path, change, changes: vec![FileChangeEntry { path, change, diff: None, captured_diff: None }], status }` with `change` `Created` when `tool_use_result.type == "create"`, else `Modified`; `status` as above |
| `tool_use` `Agent` | `ToolCall` with `ToolCallStart { name: "Agent", input, server: None, status: Some("in_progress"), metadata: None, subagent: None, started_at_ms }` (**no `SubagentLink`** until milestone 5; plan §11) | none | `ToolCall { name, input, output: Some(content as JSON), status, .. }` |
| `tool_use` `mcp__<server>__<tool>` | `ToolCall` with `server: Some(<server>)`, `name: <tool>` | none | `ToolCall { .. }` |
| any other `tool_use` | `ToolCall` with `name`, `input` | `Delta::InputJson` is ignored (the `assistant` frame carries the final input) | `ToolCall { .. , error: Some(text) when is_error }` |
| `user` frame with a `text` block and no `tool_result` | `Activity` item started and completed together, `title` = the text (the `[Request interrupted by user for tool use]` marker) | | |

Item ids: `resolve_item(key) -> ItemId`, get-or-mint, so the id on `ItemStarted`, `ItemDelta` and
`ItemCompleted` is the same (`CodexMapper::resolve_item`, `mapping.rs:361`). `harness_item_id` is
the block's `tool_use` id, or `"<message_id>:<index>"` for a text or thinking block. A `tool_result`
whose `tool_use_id` matches no open item is logged at `warn` with the id and dropped; the fixtures
never produce one, so build that case in a test.

### Approvals and server requests

- `CanUseTool` for any tool except the two below → `ApprovalRequested { thread, turn, request }`
  and a `PendingApproval` output. `ApprovalId` is `ApprovalId::new(request_id)`: the control
  request's id is what the reply must carry, and it is a UUID the CLI mints. `ApprovalKind` per
  plan §9.3: `Bash` → `CommandExecution { command: input.command, cwd: workspace_root }`;
  `Write`/`Edit`/`NotebookEdit` → `FileChange { path: input.file_path, change: Modified }`;
  `mcp__<server>__<tool>` → `McpToolCall { server, tool_name }`; else
  `Permission { detail: description or tool_name }`. `reason` is `description`. `metadata`: a
  `Text { label: "Tool", value: display_name }`, a `Path { label: "Blocked path", path }` when
  `blocked_path` is set, and `Text { label: "Suggestion", value }` per suggestion type. `available`
  is `[Accept, AcceptForSession, Decline, Cancel]`. A `CanUseTool` with no active turn opens an
  external turn first, as above.
- `CanUseTool` for `AskUserQuestion` → `ServerRequestReceived { thread, turn: Some(turn),
  request: ServerRequest { id: ServerRequestId::new(request_id), method:
  "claude/ask_user_question", params: input, received_at } }` and a `PendingServerRequest`.
- `CanUseTool` for `ExitPlanMode` or `EnterPlanMode` → no event; a `MapperOutput::Reply` carrying
  a `control_response` of `subtype: "success"` for that `request_id` whose `response` is
  `{"behavior": "deny", "message": "Giskard chooses the mode per turn; present the plan as this
  turn's answer."}`, and a `warn` log naming the tool, because with `--disallowedTools`
  (milestone 3) the ask should never appear. `plan-exit-denied.out.jsonl` shows the CLI's reaction
  to exactly this reply.
- `ControlRequest` (any other subtype) → `ServerRequestReceived` with `method =
  "claude/<subtype>"` and `params = request`, plus `PendingServerRequest`.
- `ControlResponse` → `MapperOutput::ControlResponse { request_id, payload }`, nothing else.

### Session-level frames

- `Init`: store `model` and `permissionMode`. When `apiKeySource` is present and not `"none"`,
  emit `Notice { thread, turn: None, message: "Claude Code authenticated with <source>; usage is
  billed to that credential, not to the subscription" }` and log at `warn` (plan §7). Emit nothing
  otherwise.
- `Status` with `permissionMode`: store it; if it differs from the mode the adapter last set
  (`set_expected_mode`, a small setter milestone 3 calls), emit a `Notice` and log at `warn` with
  `action = "permission_mode_drift"` (plan §3.2, §8.2). `Status` with `status: "compacting"` or
  `"requesting"`: nothing.
- `RateLimit`: `Notice` only when `rate_limit_info.status` is not `allowed`, or any
  `unifiedWindows` utilization is at least `0.9`; otherwise a `debug` log. The fixtures' events
  are all `allowed` at `0.52`, so the test constructs the warning case.
- `ApiRetry` → `Notice { turn: active, message: "retrying after <error> (attempt n of m)" }`.
- `PermissionDenied` → `Notice` with the tool name and `decision_reason`.
- `SessionTitleChanged`, `TaskNotification`, `SystemIgnored`, `Unknown` → a `debug` log with
  `thread_id`, the frame's `type` and `subtype`; `Unknown` at `warn` the first time each
  `(type, subtype)` pair is seen in a session, `debug` after, so a new CLI frame is visible once
  without flooding.

### Logging

`tracing` fields as the Codex crate uses them: `thread_id`, `turn_id`, `harness_thread_id`,
`action`, `native_item_id` (a tool-use id or `message_id:index`), `task_id`, `request_id`,
`tool_name`, `error = %error`. Never log a frame's content, a prompt, a tool input, a tool result or
a suggestion's rule text; a test captures logs at `WARN` (the Codex crate's `capture_logs` helper,
`mapping.rs:3334-3420`) while mapping the `tool-denied` fixture and asserts the command text
`touch probe.txt` appears in no log line.

### Tests

All in `mapper.rs` under `#[cfg(test)]`, fixture-driven through one helper:

```rust
// Reads tests/fixtures/<name>.out.jsonl and drives a fresh mapper through it.
fn run_fixture(name: &str, kind: TurnKind) -> Vec<MapperOutput>
```

that constructs a mapper, calls `begin_turn(TurnId::new(), kind)` once per line of the matching
`.in.jsonl` that is a `user` message (so `compact` opens two turns, the second as `Compaction`),
calls `note_interrupt_sent` when the `.in.jsonl` holds an `interrupt` control request or a deny with
`interrupt: true`, and feeds every `.out.jsonl` line through `Frame::parse` and `map`. Test names
are sentences, as in the Codex crate. The list below is the minimum; each row is one test.

| Test | Asserts |
| --- | --- |
| `a_text_turn_streams_one_agent_message_and_completes` | `text-turn`: exactly one `ItemStarted{AgentMessage}`, `ItemDelta::Text("pong")`, `ItemCompleted{AgentMessage{"pong"}}`, no `Reasoning` item, `TurnUsageUpdated` from `message_delta` and before completion, `TurnCompleted{Completed}` with usage `in 21970 / out 53` |
| `an_allowed_bash_call_is_a_command_item_that_completes` | `tool-allowed`: `ItemStarted{CommandExecution{command:"touch probe.txt"}}`, `ApprovalRequested{CommandExecution}` with `available` of four, `PendingApproval` whose `request_id` equals the ask's, `ItemCompleted{CommandExecution{status:"completed"}}` |
| `a_denied_bash_call_completes_declined_not_failed` | `tool-denied`: `ItemCompleted{CommandExecution{status:"declined"}}`, `TurnCompleted{Completed}` |
| `a_cancelled_turn_is_interrupted_when_the_adapter_sent_the_interrupt` | `cancel`: with `note_interrupt_sent`, `TurnCompleted{Interrupted}`; without it, `Failed` |
| `an_activity_item_marks_the_interruption_text` | `cancel`: an `Activity` item titled `[Request interrupted by user for tool use]` |
| `a_foreground_delegation_is_one_turn` | `delegation`: exactly one `TurnCompleted`, emitted after the single `result`; the `Agent` item completes with `status:"completed"`; every child frame (`parent_tool_use_id` set) produces no event |
| `a_backgrounded_delegation_holds_the_turn_until_the_task_is_terminal` | `delegation-interrupted`: no `TurnCompleted` at the first `result`; one `TurnCompleted{Interrupted}` after `task_updated{killed}`; still exactly one |
| `a_background_shell_task_never_gates_the_turn` | `background-bash`: `TurnCompleted` at the first `result`; the later `assistant` and `result` open an external turn (`TurnStarted` with a new id, `info` log with `action="external_turn"`) that completes on the second `result` |
| `compaction_emits_an_activity_and_no_agent_message` | `compact`: second turn has `ItemCompleted{Activity{title:"Context compacted"}}` with `pre_tokens 22017`/`post_tokens 1262` in `detail`, no `AgentMessage`, `TurnCompleted{Completed}` |
| `a_denied_exit_plan_mode_is_answered_by_the_mapper` | `plan-exit-denied`: one `Reply` with `behavior:"deny"`, no `ApprovalRequested` for it, the `Write` item completes as `FileChange{Created}`, the `ToolSearch` item as `ToolCall` |
| `a_missing_resume_is_a_failed_turn` | `resume-missing`: `TurnCompleted{Failed}` with a non-empty message |
| `session_scope_asks_carry_the_suggestion_metadata` | `accept-for-session`: the `ApprovalRequested.metadata` names the `addRules` suggestion; the two later `Bash` items complete with no further ask |
| `the_effective_window_wins_over_model_usage` | feed `autocompact-state` then `text-turn`: the `TurnUsageUpdated` before completion carries `context_window: Some(180000)`; without it, `Some(200000)` from `modelUsage` |
| `a_foreign_api_key_source_is_a_notice` | a `system/init` line copied from `text-turn` with `apiKeySource` rewritten to `"ANTHROPIC_API_KEY"` → one `Notice` |
| `a_rate_limit_warning_is_a_notice_and_an_allowed_one_is_not` | the recorded event (allowed, 0.52) → no event; the same with `status:"allowed_warning"` → `Notice` |
| `an_orphan_tool_result_is_dropped_with_a_warning` | a `user tool_result` whose id matches no item → no event, one `warn` line naming the id |
| `unknown_frames_are_logged_once_per_kind` | two `{"type":"active_goal"}` lines → no event, one `warn` then one `debug` |
| `logs_never_carry_frame_content` | map `tool-denied` under `capture_logs`; assert `touch probe.txt` is absent |
| `a_turn_begun_while_one_is_active_fails_the_first` | `begin_turn` twice → `TurnCompleted{Failed}` for the first, `TurnStarted` for the second, one `error` log |

## Step 5: identifiers, README and crate lists

- `src/ids.rs`: `NativeItemKey` as above, and the `task:` prefix constant
  `pub const TASK_ID_PREFIX: &str = "task:"` with `is_task_native_id(&str)`, used by milestone 2's
  `open_thread` refusal and milestone 5's claims. Nothing else; a session UUID needs no newtype in
  this milestone.
- `crates/giskard-harness-claude/README.md`, mirroring the Codex README's opening and these of its
  headings, filled for what exists: *Identifier model* (a table: `harness_thread_id` = the
  client-minted session UUID, `task:<tool_use_id>` for a sub-agent, `TurnId` minted by Giskard at
  `start_turn` or by the mapper for a continuation the CLI started, `ItemId` keyed by tool-use id
  or `message:index`, `ApprovalId` and `ServerRequestId` = the control `request_id`), *Mapping
  keys* (the table of Step 4), *Item lifecycle*, *Turn completion* (the agent-task gate, the
  external turn, the compaction turn), *Runtime context window* (`TurnUsageUpdated`, the
  `effective_window` precedence, the per-model usage limitation), *Frames the crate cannot type*
  (why the frame layer peeks first), and *Code and tests* (one bullet per `src/*.rs`, plus the
  fixtures README). Sections for process control, resume and approvals responses say "milestone
  2" / "milestone 3" in one line rather than describing unbuilt behaviour.
- `AGENTS.md`: "Cargo workspace with 8 crates" becomes 9, and the list gains
  `giskard-harness-claude — Claude Code CLI adapter (milestone 1: mapper only)` after the codex
  line; add the sentence "When modifying `giskard-harness-claude`, read
  `crates/giskard-harness-claude/README.md` first" beside the codex one (line 19–21). The
  convention line "All Codex-specific types confined to `giskard-harness-codex`" gains "and all
  Claude Code-specific types to `giskard-harness-claude`".
- `README.md`: the *Architecture* table (line 536–545) gains the crate row. The *Supported
  harnesses* entry ("Claude Code — not yet supported") is **unchanged**: nothing is reachable yet.
- `specs/claude-code-harness-plan.md`: add the line "Milestone 1 is implemented; see
  `claude-code-harness-plan/milestone-1-plan.md`." directly under the intro paragraph that begins
  "**Multi-harness is already built.**", the way `docs/multi-harness-design.md` records its stages.

## Verification

In order, before the commit:

1. `cargo fmt --all --check`, then `cargo clippy --workspace --all-targets --locked -- -D warnings`.
2. `cargo test -p giskard-harness-claude`, then `cargo test --workspace --locked`.
3. `cargo deny check advisories bans licenses sources` (the `audit.yml` command).
4. `grep -rn "unwrap()\|expect(\|panic!\|todo!\|unreachable!" crates/giskard-harness-claude/src`
   must match only inside `#[cfg(test)]` modules.
5. `git status` shows the fixtures under `crates/giskard-harness-claude/tests/fixtures/` and
   nothing left under `specs/claude-code-harness-plan/fixtures/`.

## Acceptance

- The workspace builds with a ninth crate and CI's three jobs plus `cargo deny` pass unchanged.
- Every fixture line parses to a `Frame`, and the only `Unknown` frames come from the two
  top-level frames the crate cannot type, each logged with its `type`.
- The mapper closes a foreground delegation at its single `result`, holds a backgrounded one
  until the agent task is terminal, and never lets a `local_bash` task hold a turn.
- A denial completes its item as `declined`, an interrupt the adapter sent completes the turn as
  `Interrupted`, and a compaction turn persists no agent message.
- The context window reaches the server only through `TurnUsageUpdated`, and the input token
  count is the three-summand sum.
- No log line contains frame content, and every dropped frame is logged with its kind.
- No process is spawned, no trait is implemented, and the README says which milestone supplies
  what is missing.
