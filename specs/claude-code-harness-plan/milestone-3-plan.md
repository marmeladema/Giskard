# Milestone 3 implementation plan: approvals, server requests, per-turn settings

Implements milestone 3 of [`../claude-code-harness-plan.md`](../claude-code-harness-plan.md)
(§11). This plan is written for an implementing agent. Every file, symbol and behaviour below was
verified against `main` at `dee480b` (milestone 2 merged), against Claude Code **2.1.286** driven
over a stdio pipe, and against the `claude-codes` **2.1.286** crate source. Line numbers are for
orientation; the symbol quoted beside each is the thing to find.

Read `AGENTS.md` first. Its rules that bind this milestone: `cargo fmt --all` and
`cargo clippy --all-targets -- -D warnings` must pass; no `unwrap`/`expect`/`panic!` on runtime
paths; nothing is dropped, coalesced or recovered from silently; every new approval, server-request,
timeout and fallback path gets a focused test and a log line that explains what happened; the crate
README is updated in the same change; Markdown prose is wrapped at 100 columns (table rows may run
longer).

## Outcome

After this milestone a Claude Code thread is **interactive**: the browser's approval card answers
`can_use_tool` asks with the plan §9.3 mapping (`Accept`, `AcceptForSession` with the session
destination, `Decline`, `Cancel`), `AskUserQuestion` and the CLI's other inbound control requests
reach `respond_server_request`, every turn runs under the permission mode its `TurnOverrides` ask
for (Plan wins over the preset), a model or effort change reaches the child before the turn and is
read back, and `compact_thread` runs `/compact` as a compaction turn. `capabilities()` finally
reports `live_approvals`, `plan_build_modes`, `per_turn_model`, `reasoning_effort` and
`context_compaction` true. Nothing is user-reachable until milestone 4 registers the kind, except
one small browser change this milestone needs (Step 6). The commit is one unit: the code, its
tests, the crate README, the browser dispatch line, and the plan amendments in Step 9.

## Scope

One commit, built in this order so that each step compiles on its own:

1. Mapper additions: what an ask must remember, the `control_cancel_request` frame, the
   `system/init` mode check.
2. The launch mode and the handshake's `set_permission_mode`, with the root fallback.
3. The supervisor's request-and-wait helper and the new commands.
4. Per-turn settings in `start_turn`: mode, model, effort, read-back.
5. Approvals and server requests: `respond_approval`, `respond_server_request`, cancellation.
6. `AskUserQuestion` end to end, including the browser's dispatch line.
7. `compact_thread`.
8. Tests.
9. Documentation: the crate README and the plan amendments.

## Non-goals

No `HarnessKind`, no `config.example.toml`, no spec change (milestone 4; its §9.1 / §9.2.1
amendments are already listed there). No sub-agent approval routing by `tool_use_id` →
`parent_tool_use_id` (milestone 5; every ask attaches to the primary thread's turn, which is what
milestone 1 already does). No `mcp_status`. No hook route. No `AcceptWithExecPolicyAmendment`. No
new fixtures: every CLI frame a test feeds in comes from `tests/fixtures/` or is a one-line
control response the verified facts below spell out.

## Verified facts this milestone rests on

Each row was established in this session against Claude Code 2.1.286 with the fixtures' argv
(`tests/fixtures/README.md`), a scrubbed environment and a throwaway working directory. Rows marked
**[fixture]** are in the committed fixtures.

| Fact | Consequence |
| --- | --- |
| `set_permission_mode {mode}` answers `{"subtype":"success","response":{"mode":"<mode>"}}`; `manual` is accepted and echoed as **`default`**; after every *change* the CLI also emits `system/status {permissionMode}` (it arrives after the control response); an unknown mode answers `{"subtype":"error","error":"Cannot set permission mode: must be one of acceptEdits, auto, bypassPermissions, default, dontAsk, plan","error_code":"invalid_mode"}` | The control request takes the CLI's own names, so the adapter sends `default`, never `manual`, and sets the mapper's expected mode to that name before writing, so the `status` frame is not drift. An error response fails the turn start |
| `set_permission_mode bypassPermissions` on a child launched with `--permission-mode manual` answers `{"subtype":"error","error":"Cannot set permission mode to bypassPermissions because the session was not launched with --dangerously-skip-permissions","error_code":"bypass_not_launched"}`, as root and as an ordinary user | `full_access` is a **launch-time** capability. A child that must ever serve it has to be launched with `--permission-mode bypassPermissions` |
| Launched with `--permission-mode bypassPermissions` as an ordinary user: `initialize` answers `current_permission_mode: "bypassPermissions"`, then `set_permission_mode default` succeeds (`{"mode":"default"}`) and `set_permission_mode bypassPermissions` succeeds again. (Established as a non-root user up to and including those control responses; the turns that followed could not be read in this session) | A bypass-launched child can serve every preset through `set_permission_mode`. This is what makes Step 2's launch strategy work |
| Launched with `--permission-mode bypassPermissions` **as root**: exit 1 in 0.2 s, stderr `--dangerously-skip-permissions cannot be used with root/sudo privileges for security reasons`, no stdout | The bypass launch must fall back to a standard launch, and `full_access` is then refused per turn with that sentence |
| `set_model {model}` answers success with no payload; an unknown model answers `{"subtype":"error","error":"Model 'x' not found","error_code":"catalog_unknown"}` and leaves the model unchanged; the next turn's `system/init.model` and `result.modelUsage` carry the new model; `set_model` **mid-turn** also succeeds and the rest of that turn runs on the new model | Model changes are per turn and must only be sent while idle (`start_turn` already refuses a busy thread). No `system/init` re-emit follows `set_model` by itself; the next turn's `init` carries it |
| `get_settings` answers `{applied:{model:"<resolved id>", effort:<level or null>, …}, effective:{effortLevel?}, sources:[{source:"flagSettings", settings:{…}}]}`. On Sonnet, `apply_flag_settings {settings:{effortLevel:"high"}}` answers success and `applied.effort` becomes `high`; `set_model haiku` makes `applied.effort` `null` (Haiku has no effort) and `set_model sonnet` brings `high` back; `apply_flag_settings` with `max` answers success, `applied.effort` becomes `max` **and `effective` is cleared to `{}`**; with `banana` it answers success, `applied.effort` keeps `max` and `effective` stays `{}`. Every `apply_flag_settings` answers success (plan §3.3) | `applied.effort` is the only read-back: it equals the requested level when the model took it, keeps the previous level when the value was invalid, and is `null` on a model without effort. `effective.effortLevel` is unreliable (a valid `max` cleared it) and must not be compared |
| A `can_use_tool` for `AskUserQuestion` carries `display_name`, `input`, `requires_user_interaction: true`, `subtype`, `tool_name`, `tool_use_id` and **no** `permission_suggestions`, `blocked_path` or `description`. Its `input` is `{questions:[{question, header, options:[{label, description}], multiSelect}]}`. The answer is `{"behavior":"allow","updatedInput":{questions:<the input's>, answers:{<question text>: "<label>"}}}`: keyed by **question text**, the `tool_result` reads `Your questions have been answered: "…"="Cats"`; keyed by `header` the tool result reads `The user did not answer the questions`. A `multiSelect` answer is the labels joined with `", "` (`"Apple, Cherry"` was accepted and understood) | The adapter translates the browser's per-question answers into that map, and must never key it by header |
| An `interrupt` while an ask is pending: the CLI writes `{"type":"control_cancel_request","request_id":"<the ask's request_id>"}`, then the interrupt's response, then the user frames and a `result` (`error_during_execution`, `terminal_reason: "aborted_tools"`) whose `permission_denials` lists the asked tool. A control response written **after** that is ignored: no frame answers it, and the next turn runs normally | The CLI withdraws its own asks on interrupt; the adapter only has to drop the pending entry (and resolve a server request) when `control_cancel_request` names it. A late answer is harmless but must be logged, not sent as if it mattered |
| `/compact` as a user message: `status compacting`, `status` with `compact_result`, a re-emitted `init`, `compact_boundary` (`trigger: "manual"`), two `user` frames (`isCompactSummary` / synthetic bookkeeping), one degenerate `result` **[fixture: `compact`]** | The mapper already turns this into a compaction turn with one `Activity` item and no agent message (`compaction_emits_an_activity_and_no_agent_message`, `mapper.rs:2563`) |
| In plan mode without `--disallowedTools`, a request to write a file produced an `ExitPlanMode` ask (with `permission_suggestions: null`) rather than a `Write` ask; allowing it switched the mode back to the previous one **[fixture: `plan-exit-denied`, denied]** | The `--disallowedTools EnterPlanMode ExitPlanMode` flag milestone 2 already passes is what keeps a plan turn in plan mode; the mapper's existing denial is the backstop |
| A `Bash` ask's `permission_suggestions` **[fixture: `tool-allowed`]**: `[{type:"addRules", rules:[{toolName:"Bash", ruleContent:"touch probe.txt"}], behavior:"allow", destination:"localSettings"}, {type:"addDirectories", directories:["/work/project"], destination:"session"}, {type:"setMode", mode:"acceptEdits", destination:"session"}]`; the recorded `AcceptForSession` reply echoes the `addRules` entry with `destination` rewritten to `session` **[fixture: `accept-for-session.in.jsonl`]** | Echo the raw `addRules` suggestion objects, never retyped: `claude-codes`' `PermissionSuggestion` drops `directories` and its `PermissionDestination` has no `localSettings` variant (`io/control.rs:67`) |
| A deny with `interrupt: true` ends the turn with `error_during_execution`, `terminal_reason: "aborted_tools"`, the asked tool in `permission_denials` **[fixture: `cancel`]**; a plain deny blocks the tool and the turn completes normally with `tool_result_meta.non_execution_kind: "permission-rule"` **[fixture: `tool-denied`]** | The §9.3 rows hold as recorded; `Cancel` must tell the mapper an interrupt was sent so the turn persists as `Interrupted` |
| A deny whose response carries **no `message`** is processed: a `user` tool result followed and the model retried the command with a fresh ask | A `message` is not required, but every deny the adapter writes carries one, since it is what the model and the transcript see |

## Step 1: mapper additions (`src/mapper.rs`, `src/frame.rs`)

- **What an ask must remember.** `MapperOutput::PendingApproval` (`mapper.rs:81`) gains
  `tool_name: String` and `suggestions: Vec<Value>` (the raw `permission_suggestions` array of the
  ask, `[]` when absent or `null`); `on_can_use_tool` (`mapper.rs:1627`) already reads them off
  `raw`. `MapperOutput::PendingServerRequest` (`mapper.rs:93`) gains `subtype: String`
  (`"can_use_tool"` for `AskUserQuestion`, else the control request's subtype) and
  `input: Value` (the ask's `input`, so the answer can echo its `questions`; `Null` otherwise).
- **`AskUserQuestion` params.** The `ServerRequestReceived` the mapper emits for it
  (`mapper.rs:1669`) keeps `method: "claude/ask_user_question"` but its `params` become
  `{"questions": [...]}` where each question is the CLI's object plus `"id": "<index>"` (`"0"`,
  `"1"`, …). The browser's user-input card keys answers by `id` (`static/app.js:5173`
  `collectToolQuestionAnswers`), and the CLI's question objects carry none.
- **`control_cancel_request`.** `Frame::parse` (`frame.rs:108`) gains
  `Frame::ControlCancelRequest { request_id }` for the top-level
  `{"type":"control_cancel_request","request_id":…}` frame (today it is `Frame::Unknown`, logged
  at `warn` once). The mapper maps it to a new `MapperOutput::CancelRequest { request_id }` and
  logs it at `info` with `action = "control_cancel_request"`; it holds no pending state itself, so
  the supervisor does the lookup (Step 5).
- **Denials complete as `declined`.** `pub fn note_denied(&mut self, tool_use_id: &str)` inserts
  into the active turn's `denied_tool_use_ids` (`mapper.rs:166`), which `tool_result` mapping
  already reads; no active turn → `warn`. The supervisor calls it on `Decline` and `Cancel`.
- **`system/init` mode check.** `on_init` (`mapper.rs:521`) compares `init.permission_mode`
  with `session.expected_mode` the way `on_status` does (`mapper.rs:564-600`) and emits the same
  `permission_mode_drift` warning and `Notice`. Factor the comparison into one private
  `fn check_mode(&mut self, mode: &str, out)` used by both. The re-emitted `init` after a
  backgrounded task is the frame most likely to show a mode Giskard did not set.
- `set_expected_mode` (`mapper.rs:343`) stays as it is; Step 3 calls it with the CLI's name.

## Step 2: launch mode and the handshake (`src/process.rs`, `src/harness.rs`)

**Decision: launch every session child with `--permission-mode bypassPermissions` when the CLI
accepts it, and set `default` in the handshake before anything else.** `bypassPermissions` can only
be *set* on a child that was *launched* with it (verified above), and `open_thread` cannot know
whether the thread will ever run a `full_access` turn (`OpenThreadOptions` carries no preset). The
alternative, stopping and relaunching the child with `--resume` the first time a `full_access` turn
arrives, would have to keep the thread's `EventLog` alive across a child exit, which the supervisor
is built to close; it is more code in the most delicate part of the adapter for a path most threads
never take. The launch mode is only the *ceiling*: no turn ever runs before the handshake has set
`default`, because nothing reaches stdout before the first user message (milestone 2's first
verified fact), and every turn sets its own mode (Step 4). A child whose handshake fails to set
`default` is killed, never used.

- `process.rs`: `SessionArgs` gains `launch_mode: LaunchMode` (`enum LaunchMode { Bypass,
  Standard }`); `session_argv` (`process.rs:96`) emits `--permission-mode bypassPermissions` for
  `Bypass` and `--permission-mode manual` for `Standard`. `probe_argv` is unchanged (no mode).
- `classify_exit` (`process.rs:507`) gains `ExitKind::BypassRefused` for a stderr tail or
  `result.errors` containing `cannot be used with root/sudo privileges` or, case-insensitively,
  `bypassPermissions` together with `disable` (the `permissions.disableBypassPermissionsMode`
  settings key is documented to refuse the mode; its exact sentence is **[unverified]**). Check it
  before `ResumeMissing` is not needed: the two never co-occur, but keep `ResumeMissing` first.
- `harness.rs`: `ClaudeHarness` gains `bypass_refused: Mutex<Option<String>>` (the sentence the
  CLI gave, once an instance has seen a bypass launch refused; a plain value, not entity state).
  `spawn_and_handshake` (`harness.rs:197`) takes the launch mode; `open_thread` (`harness.rs:823`)
  tries `Bypass` unless `bypass_refused` is set, and on `HandshakeFailure::Exited` classified
  `BypassRefused` records the sentence, logs once at `warn` with `action = "bypass_refused"`,
  and retries the same session flag with `Standard`. The resume-missing fallback (`harness.rs:860`)
  composes with it: order the matches as `BypassRefused` → relaunch with the same session flag;
  `ResumeMissing` → relaunch with `Fresh` and the same launch mode.
- `handshake` (`harness.rs:655`): right after `initialize` succeeds, and before `get_settings`,
  write `set_permission_mode {"mode":"default"}` through `request` under `CONTROL_TIMEOUT`. On
  `Bypass` a refusal, a timeout or an exit is a **failed open** (`HandshakeFailure::Error(
  HarnessError::Spawn("claude did not leave bypassPermissions: …"))` after killing the child):
  the child must never be used in bypass mode by accident. On `Standard` the request is still sent
  (it makes the mode explicit and costs nothing) and a failure is logged at `warn` and ignored, as
  `optional_request` does. The handshake's `early_lines` keep the `system/status` the mode change
  may emit; the supervisor maps it after `set_expected_mode("default")` (Step 3), so it is not
  drift.
- `ChildHandle` (`session.rs:54`) gains `launch_mode: LaunchMode`, `mode: Mutex<String>` is
  **not** added: the supervisor owns the current mode (Step 3).
- The `Handshake` struct records `permission_mode` already (`harness.rs:593`); after the
  `set_permission_mode` it is `default` on both launch modes. Assert that in a test.

## Step 3: the supervisor's request-and-wait helper and new commands (`src/session.rs`)

The supervisor reads every stdout line itself, so a command handler that needs a control
*response* cannot simply await a waiter: the loop that would resolve it is the one awaiting. Add,
on `Supervisor`:

```rust
/// Write one control request and pump frames until its response or `deadline`. Every frame read
/// meanwhile is dispatched normally (the way `pump_until` does while stopping), so a `status` or a
/// late answer is never lost. `Err(Timeout)` when the deadline passes; the waiter is then dropped.
async fn await_control(&mut self, request: &Value, deadline: Instant) -> Result<Value, HarnessError>
```

Implementation: `new_request_id`, `control_line`, `write_line`, insert `Waiter::Value(tx)` into
`waiters` (`session.rs:302`), then loop `tokio::select! { biased; answer = &mut rx => break answer,
pumped = self.pump_until(deadline) => … }` with the existing `pump_until` (`session.rs:548`); on
`false` (deadline) remove the waiter and return `Timeout`; on EOF return `child_stopped()`. The
response's `control_outcome` (`session.rs:151`) already turns `subtype: "error"` into
`HarnessError::Protocol(<error>)`, which is what every refusal below propagates.

New `ChildCommand` variants (`session.rs:32`):

- `StartTurn` gains `settings: TurnSettings { mode: String /* CLI name */, model: Option<String>,
  effort: Option<String> }` beside `line`, `turn`, `model`, `reply`.
- `RespondApproval { id: ApprovalId, ask: PendingAsk, decision: ApprovalDecision, reply }`.
- `RespondServerRequest { id: ServerRequestId, ask: PendingAsk, response: ServerRequestResponse,
  reply }`.
- `Compact { turn: TurnId, reply: oneshot::Sender<Result<(), HarnessError>> }`.

`Supervisor` gains `current_mode: String` (`"default"` after the handshake; updated on every
successful `set_permission_mode`), `current_model: ModelRef` and `current_effort: Option<String>`
(from `ChildHandle.model` at spawn, updated on successful changes). These replace nothing: they
are the supervisor's own view of what the CLI holds, and the mapper's `session.permission_mode`
stays what the CLI *reports*.

## Step 4: per-turn settings in `start_turn` (`src/session.rs`, `src/harness.rs`)

`ClaudeHarness::start_turn` (`harness.rs:1140`) stops ignoring `overrides`:

- **Mode.** `overrides.mode == Mode::Plan` → `"plan"`; else by preset: `AskFirst` → `"default"`,
  `AutoApprove` → `"acceptEdits"`, `FullAccess` → `"bypassPermissions"`. Plan wins over the
  preset (plan §8.2); `auto` and `dontAsk` are never sent (plan §8.1).
- **`full_access` on a child that cannot bypass.** When the mode is `bypassPermissions` and the
  child's `launch_mode` is `Standard`, return `HarnessError::Unsupported(format!("full_access is
  not available: Claude Code refused to start in bypassPermissions mode ({sentence})"))` before
  sending anything, with the sentence from `bypass_refused`, else the generic "the child was not
  launched in that mode". The browser shows the `Display` text (`ws.rs:133`).
- **Model and effort.** `overrides.model` is `Some` on every turn (`ws.rs:617`,
  `routes.rs:1044`): `model = Some(m.model)` when it differs from the supervisor's
  `current_model.model`, `effort = m.reasoning_effort` when it differs from `current_effort`
  (an effort `None` on a model that supports none is left alone: the CLI ignores it, and clearing
  would be a `settings: {effortLevel: null}` whose effect is **[unverified]**).
- The `turn_model_override_ignored` warning and the `debug` about ignored mode and preset go
  away.

In the supervisor's `StartTurn` handler (`session.rs:432`), after the `reply.is_closed()` guard and
the `ThreadBusy` check, and **before** `begin_turn`, in this order, each under
`CONTROL_TIMEOUT` and each failing the hand-off without starting a turn:

1. `mapper.set_expected_mode(&settings.mode)` then `await_control({"subtype":
   "set_permission_mode","mode":<mode>})`; on `Ok` set `current_mode`. Sent on **every** turn
   (plan §8.2: a stale mode must be impossible to carry over). A refusal (`invalid_mode`,
   `bypass_not_launched`) is the `Protocol` error, logged at `warn` with `action =
   "set_permission_mode"`, `mode`, and the CLI's `error_code` when the payload carries one.
2. If `settings.model` is `Some`: `await_control({"subtype":"set_model","model":<id>})`; a
   `catalog_unknown` refusal fails the hand-off with `HarnessError::Unsupported(<the CLI's
   sentence>)` (the picker offered a model the CLI does not know; plan §6 says surface it, never
   pre-filter).
3. If `settings.effort` is `Some`: `await_control({"subtype":"apply_flag_settings","settings":
   {"effortLevel":<level>}})`.
4. If 2 or 3 ran: `await_control({"subtype":"get_settings"})` and read back. `applied.model`
   must equal the requested selector or its catalog `resolvedModel` (the façade passes the
   catalog's answer in `settings.resolved_model: Option<String>`); `applied.effort` must equal
   the requested level (never compare `effective.effortLevel`: a valid `max` clears it, as
   verified). A model mismatch is `HarnessError::Protocol("Claude Code applied model X instead of
   Y")`; an effort mismatch is `HarnessError::Unsupported("Claude Code did not accept effort LEVEL
   for MODEL")` (an invalid value leaves the previous level in place). On success set
   `current_model` / `current_effort` and log `info` `action = "turn_settings"` with `mode`,
   `model`, `effort`.
5. Then the existing `begin_turn`, `note_turn_model(model)` — with the model the read-back
   confirmed — and the write.

A hand-off that failed after step 1 leaves the mode set; that is fine (the next turn sets its
own). Use `START_TURN_TIMEOUT = 30 s` for the façade's `call` on `StartTurn` instead of
`CONTROL_TIMEOUT`, since the hand-off may now carry up to four control requests plus the write.

## Step 5: approvals and server requests (`src/session.rs`, `src/harness.rs`)

`PendingAsk` (`session.rs:76`) gains `tool_name: Option<String>`, `suggestions: Vec<Value>`,
`subtype: String` and `input: Value`, filled from the mapper outputs of Step 1 by `dispatch`
(`session.rs:382`). `PendingRequests` gains `remove_approval(&ApprovalId) -> Option<PendingAsk>`,
`remove_server_request(&ServerRequestId) -> Option<PendingAsk>` and
`remove_by_request_id(&str) -> Option<(RequestKind, PendingAsk)>` for cancellation.

### `respond_approval` (`harness.rs:1184`)

1. `remove_approval(&req)` under the lock; none → `HarnessError::Protocol(format!("approval {req}
   is not pending"))` at `warn` (it was answered, cancelled by an interrupt, or its child is gone).
2. `AcceptWithExecPolicyAmendment` → put the entry back and return `Unsupported` (plan §9.3: not
   advertised, so this is a client bug).
3. `live(ask.thread)` → none → `ThreadNotFound`; send `RespondApproval` and await under
   `CONTROL_TIMEOUT`.

In the supervisor, `RespondApproval` builds the `control_response` line and writes it:

| `ApprovalDecision` | `response` |
| --- | --- |
| `Accept` | `{"behavior":"allow"}` |
| `AcceptForSession` | `{"behavior":"allow","updatedPermissions":[…]}` with every `suggestions` entry whose `type == "addRules"`, cloned raw with `destination` set to `"session"`; no such entry → `{"behavior":"allow"}` and a `warn` with `action = "accept_for_session_degraded"`, `thread_id`, `tool_name`, `suggestions = <count>` (plan §9.3 degradation) |
| `Decline` | `{"behavior":"deny","message":"Declined by the user in Giskard"}` |
| `Cancel` | `{"behavior":"deny","message":"Cancelled by the user in Giskard","interrupt":true}`, then `mapper.note_interrupt_sent()` and `self.interrupt_sent = true` (the `cancel` fixture's result is `aborted_tools`; marking it keeps the turn `Interrupted` and the exit after it expected) |

For `Decline` and `Cancel`, `mapper.note_denied(tool_use_id)` when the ask carried one. Log every
answer at `info` with `action = "respond_approval"`, `thread_id`, `turn_id`, `request_id`,
`tool_name`, `decision`, `rules = <count echoed>`. A write failure is the `Broken` path, as for any
write. Never rewrite `ruleContent` (plan §9.3: echo the CLI's own rule verbatim).

The reply resolves when the line is written; the CLI sends no acknowledgement for a control
response. The browser's card is cleared by the live snapshot's `resolve_approval`
(`thread_runtime/live.rs:216`) and the turn's end; no `AgentEvent` exists for a resolved approval.

### `respond_server_request` (`harness.rs:1202`)

1. `remove_server_request(&req)`; none → `Protocol("server request … is not pending")`.
2. Send `RespondServerRequest`; the supervisor writes one of:
   - **`AskUserQuestion`** (`ask.subtype == "can_use_tool"`): `Result { value }` → read
     `value.answers` as the browser's `{<id>: {answers: [<label>…]}}` map
     (`static/app.js:5173`), map each `id` back to the question at that index in
     `ask.input.questions`, and write `{"behavior":"allow","updatedInput":{"questions":
     <ask.input.questions>, "answers":{<question text>: <labels joined with ", ">}}}`. A question
     with no answer is left out of the map. `Error { message, .. }` → `{"behavior":"deny","message":<message>}` (the
     CLI then reports the question as unanswered). A `value` that is not that shape →
     `HarnessError::Protocol` naming the request, entry put back.
   - **any other control request** (`request_user_dialog`, `rename_session`, `hook_callback`,
     `mcp_message`, …): `Result { value }` → `{"subtype":"success","request_id":…,"response":
     <value>}`; `Error { code, message }` → `{"subtype":"error","request_id":…,"error":<message>}`
     (the control protocol's two response shapes; the CLI's own error responses carry `error`
     plus an `error_code`, so `code` is dropped **[the error shape toward the CLI is
     unverified]**).
3. After the write, append `AgentEvent::ServerRequestResolved { thread, turn: mapper.active_turn(),
   request_id }` to the log: that is what clears the browser's card
   (`thread_runtime/live.rs:231`), and the Codex adapter emits the same (`lib.rs:2106`).

### Cancellation and interrupt

- `MapperOutput::CancelRequest { request_id }` → `remove_by_request_id`: an approval is dropped
  with a `debug` (its card vanishes with the turn, `live.rs:423`); a server request additionally
  appends `ServerRequestResolved`; nothing pending → `debug` (already answered). Log `action =
  "control_cancel_request"`, `request_id`, `kind`.
- `interrupt` (`harness.rs:1127`) needs no change: the CLI withdraws its own asks (verified) and
  the result's `permission_denials` names them. Spec §9.2's "best-effort cancel the pending
  request" is therefore the CLI's, not the adapter's; say so in the README.
- A `respond_*` for an entry the cancel already removed returns the `Protocol` error above,
  which the server rolls back cleanly (`registry.rs:1049-1056`).

## Step 6: `AskUserQuestion` in the browser (`crates/giskard-server/static/app.js`)

The server-request card dispatches on the method name (`app.js:5007`); `claude/ask_user_question`
falls into `renderUnknownServerRequest` (`app.js:5285`), whose only answers are an empty result or
a rejection. Spec §9.2 requires unknown methods to stay answerable, which this satisfies, but a
question with options deserves the question card. Two lines:

- `app.js:5008`: `if (method === "item/tool/requestUserInput" || method ===
  "claude/ask_user_question") renderToolUserInputRequest(body, id, request);`
- `serverRequestTitle` (`app.js:5058`) and `serverRequestDetail` (`app.js:5069`): the same
  alias, so the title reads "Agent needs your answer" and the detail counts the questions.

`renderToolUserInputRequest` reads `id`, `header`, `question`, `options[].label/description` and
`isOther`/`isSecret` (`app.js:5120-5165`), all of which Step 1's params supply or omit harmlessly;
`multiSelect` renders as a single choice (the CLI accepts one label; several joined by `", "` are
**verified** accepted, but the card has no multi-select control, and adding one is not this
milestone's). This is a change with no visible effect on the README screenshots (no scripted
scenario renders it), so `tests/e2e/screenshots.sh` does not run; `tests/e2e/tests/
server-requests.spec.ts` keeps covering the card through the replay harness's own method. The
alternative, emitting `item/tool/requestUserInput` from the mapper and touching no browser code,
was rejected because it would label a Claude ask with a Codex method name on the wire.

## Step 7: `compact_thread` (`src/session.rs`, `src/harness.rs`)

`ClaudeHarness::compact_thread` (replace the trait default): `live(thread)` → none →
`ThreadNotFound`; `TurnId::new()`; send `Compact { turn, reply }` under `CONTROL_TIMEOUT`;
`Ok(())`. The forwarder holds the turn lease and expects a `TurnStarted` then a `TurnCompleted`
it persists as a compaction turn (`event_forwarder.rs:1024`, `:2031`).

Supervisor `Compact`: `ThreadBusy` when `mapper.active_turn().is_some()` (the Codex adapter
answers `Unsupported("context compaction is not available during an active turn")`,
`instance.rs:768`; `ThreadBusy` is the honest variant here and the forwarder treats both as a
failed intent). Else `mapper.begin_turn(turn, TurnKind::Compaction)` → append, write the user line
`{"type":"user","message":{"role":"user","content":[{"type":"text","text":"/compact"}]}}` (the
`compact.in.jsonl` fixture's exact message), reply `Ok`. The mapper does the rest: the degenerate
`result` completes the turn, the `compact_boundary` is the turn's one `Activity`, and the synthetic
summary frames are bookkeeping (`README.md` *Turn completion*). No per-turn settings are sent for a
compaction turn: the mode in force is the previous turn's, and compaction runs no tool.

## Step 8: tests

All against `ScriptedChild` (`session.rs:827`; `Action::Respond`, `RespondError`,
`EmitFixture`, `RespondOnNextWrite`) and the real-process path through `tests/fake-claude.sh`,
which gains `set_permission_mode` (answer `{"mode":<mode>}`, or the `bypass_not_launched` error
when `<mode>` is `bypassPermissions` and `--permission-mode bypassPermissions` was not in `$*`),
`set_model` (success, or the `catalog_unknown` error for a model not in the `initialize` fixture),
`apply_flag_settings` (success) and `get_settings` (echoing the last model and effort it was
told). The script keeps needing only `sh`, `sed` and `cat`.

Each test is named for its invariant:

1. `a_bypass_launch_sets_default_in_the_handshake`: argv carries `--permission-mode
   bypassPermissions`; the written lines are `initialize`, `set_permission_mode default`,
   `get_settings` in that order; the handle opens.
2. `a_refused_bypass_launch_falls_back_to_a_standard_launch`: first spawn exits 1 with the root
   sentence; second spawn has `--permission-mode manual`; `bypass_refused` is set; the warning is
   logged once across two opens.
3. `a_handshake_that_cannot_leave_bypass_fails_the_open` (the `set_permission_mode` error on a
   bypass launch → `Spawn`, child killed).
4. `a_resume_missing_fallback_keeps_the_launch_mode`.
5. `every_turn_sets_its_permission_mode`: two turns under `AskFirst` → two
   `set_permission_mode default` lines; `AutoApprove` → `acceptEdits`; `Mode::Plan` under any
   preset → `plan`.
6. `full_access_needs_a_bypass_launch`: on a `Standard` child → `Unsupported` naming the sentence,
   nothing written; on a `Bypass` child → `set_permission_mode bypassPermissions` written.
7. `the_mode_status_after_a_set_is_not_drift`: the script answers `set_permission_mode` and emits
   `{"type":"system","subtype":"status","permissionMode":"acceptEdits",…}` (built from the
   `text-turn` status line with the field added); no `permission_mode_drift` log, no `Notice`.
8. `a_mode_the_adapter_did_not_set_is_drift_on_init_too`: the `init` line rewritten to
   `permissionMode: "plan"` after a `default` turn start → the warning and the `Notice`.
9. `a_model_change_is_sent_and_read_back`: `overrides.model` differs → `set_model`, then
   `get_settings` answered with the resolved id → the turn starts, `TurnUsageUpdated.model` is the
   requested ref; the same model on the next turn sends nothing.
10. `an_unknown_model_fails_the_turn_start` (`catalog_unknown` → `Unsupported`, no turn, no
    `TurnStarted` in the log).
11. `an_effort_change_is_sent_and_read_back` (`get_settings` answers `applied.effort` equal to the
    request), and `a_refused_effort_fails_the_turn_start` (`applied.effort` still the previous
    level).
12. `a_read_back_mismatch_is_a_protocol_error` (applied model is a third id).
13. `accept_writes_a_bare_allow` (the `tool-allowed` ask → `{"behavior":"allow"}` exactly).
14. `accept_for_session_echoes_the_rule_with_the_session_destination`: the written line's
    `updatedPermissions` equals the `accept-for-session.in.jsonl` reply's, `ruleContent`
    untouched, `destination` `session`, and the `localSettings` value is nowhere in the line
    (the plan §9.3 regression test).
15. `accept_for_session_without_a_rule_suggestion_degrades_to_accept` (suggestions of
    `addDirectories` only → bare allow + the `accept_for_session_degraded` warning).
16. `decline_blocks_the_tool_and_the_turn_completes` (`tool-denied` frames after the deny; the
    item is `declined`, the turn `Completed`).
17. `cancel_interrupts_and_the_turn_is_interrupted` (`cancel` frames; `TurnCompleted {
    Interrupted }`).
18. `an_unknown_or_already_answered_approval_is_a_protocol_error`, and
    `exec_policy_amendments_are_unsupported`.
19. `a_cancelled_ask_is_dropped_and_a_late_answer_is_refused`: `control_cancel_request` for the
    pending ask → `respond_approval` is `Protocol`; for a pending server request →
    `ServerRequestResolved` in the log.
20. `ask_user_question_round_trips`: the ask (a `control_request` line built from the verified
    shape) → `ServerRequestReceived { method: "claude/ask_user_question", params.questions[0].id
    == "0" }`; `respond_server_request(Result { answers: {"0": {answers: ["Cats"]}} })` writes
    `updatedInput.answers == {"Do you prefer cats or dogs?": "Cats"}` with the original
    `questions`; two labels join with `", "`; `Error` writes a deny with the message;
    `ServerRequestResolved` follows.
21. `other_control_requests_round_trip` (`rename_session` from the CLI → success with the value;
    an `Error` → the error shape).
22. `compact_runs_a_compaction_turn`: `compact_thread` writes the `/compact` line, the `compact`
    fixture's frames after the first result yield `TurnStarted`, one `Activity`, `TurnCompleted
    { Completed }`, no `AgentMessage`; busy → `ThreadBusy`.
23. `capabilities_advertise_milestone_three` (`lib.rs:55` test updated).
24. Real process: `a_real_child_sets_mode_model_and_effort_per_turn` and
    `a_real_child_answers_an_ask` through `fake-claude.sh`, which emits the `tool-allowed` ask on
    a user message containing `touch` and the rest of that fixture once answered.

## Step 9: documentation

### Crate README (`crates/giskard-harness-claude/README.md`)

- **Status**: milestone 3; what milestone 4 still gates.
- **Launch** table: the `--permission-mode` row becomes the bypass-launch rule with its root
  fallback and the handshake's `set_permission_mode default`; state plainly that no turn can run
  before that request has succeeded, and that a child that cannot leave bypass is killed.
- New **Permission presets and plan mode** section: the preset table (`ask_first` → `default`,
  `auto_approve` → `acceptEdits`, `full_access` → `bypassPermissions`, Plan → `plan` over any
  preset), "set on every turn", the CLI's names versus the flag's (`manual` / `default`), the
  `full_access` refusal on a standard child and its sentence, the `--disallowedTools` backstop,
  what `ask_first` promises (plan §8.3 / §9.2.1: not "ask about everything"), and the drift check
  on both `init` and `status`.
- New **Per-turn model and effort** section: when `set_model` / `apply_flag_settings` are sent,
  the `get_settings` read-back, that an invalid effort clears the setting and is reported as
  refused, that `applied.effort` is `null` on a model without effort.
- New **Approvals** section: the decision table of Step 5, the session-destination invariant and
  the degradation, `Cancel` as an interrupt, what `control_cancel_request` does, that a late
  answer is refused, that session grants die with the child (archive, delete, shutdown, crash;
  plan §9.3), and that sub-agent asks attach to the primary turn until milestone 5.
- New **Server requests** section: `AskUserQuestion` (`claude/ask_user_question`, the `id`s, the
  answer map keyed by question text, the join for several labels), the generic success/error
  shapes for other subtypes, `ServerRequestResolved`.
- **Manual compaction** section (the Codex README has one at line 68): the `/compact` message,
  `ThreadBusy`, what the mapper persists.
- **Code and tests** and the `fake-claude.sh` description updated.

### Plan amendments (`specs/claude-code-harness-plan.md`)

The facts this milestone verified are already in the plan: the commit that added this document
amended §3.3 (`set_model`, `apply_flag_settings`, `get_settings`, and the `control_cancel_request`
frame) and §8.1 (the launch-time nature of `bypassPermissions`). The implementing commit adds one
sentence to the §11 milestone 3 paragraph, as the earlier milestones did: "Milestone 3 is
implemented in `crates/giskard-harness-claude` and one dispatch line in `static/app.js`."

`AGENTS.md`, the root `README.md`, `config.example.toml` and `docs/api-endpoints.md` do not
change: nothing is reachable from configuration or HTTP, and no route changes. `docs/screenshots`
are not regenerated (Step 6).

## Logging

Stable fields as in milestone 2 (`project_id`, `harness`, `thread_id`, `harness_thread_id`,
`turn_id`, `pid`, `action`, `request_id`). Actions introduced here: `bypass_refused`,
`set_permission_mode`, `turn_settings`, `set_model`, `apply_flag_settings`, `get_settings`,
`respond_approval`, `accept_for_session_degraded`, `respond_server_request`,
`control_cancel_request`, `compact`. Decisions, modes, model ids, effort levels and rule counts are
logged; `ruleContent`, question texts and answers are not (an answer is user content).

## Verification

In order, before the commit:

1. `cargo fmt --all --check`, then `cargo clippy --workspace --all-targets --locked -- -D warnings`.
2. `cargo test -p giskard-harness-claude`, then `cargo test --workspace --locked`.
3. `cargo deny check advisories bans licenses sources` (no manifest change is expected).
4. `grep -rn "unwrap()\|expect(\|panic!\|todo!\|unreachable!" crates/giskard-harness-claude/src`
   matches only inside `#[cfg(test)]` modules.
5. `node --check crates/giskard-server/static/app.js` is not available (no Node toolchain); re-read
   the two edited lines and run `tests/e2e/run.sh` when Docker is available, since
   `server-requests.spec.ts` exercises the same card.
6. With a real CLI on `PATH` and a non-root shell (not in CI): one thread, a turn under each
   preset, an ask answered each of the four ways, an `AskUserQuestion`, a model switch and an
   effort switch, a `/compact`; watch the log for `turn_settings` and `respond_approval` lines.

## Acceptance

- `can_use_tool` asks are answered with exactly the §9.3 shapes; `AcceptForSession` never sends a
  destination other than `session` and never rewrites a rule; `Cancel` interrupts the turn and
  persists it as `Interrupted`.
- `AskUserQuestion` is answered from the browser's question card and the answer map is keyed by
  question text; other inbound control requests round-trip through `respond_server_request`, and
  every answered or withdrawn server request emits `ServerRequestResolved`.
- Every turn sets its permission mode before its message, Plan wins over the preset, and a mode
  the adapter did not set is reported on `init` and on `status`.
- `full_access` works on a bypass-launched child and is refused with the CLI's sentence on a
  child the CLI would not launch in bypass mode; no child ever runs a turn in bypass mode that
  Giskard did not ask for.
- A model or effort change is sent before the turn and read back; a refusal or a mismatch fails
  the turn start with a message naming it, and no `TurnStarted` is emitted.
- `compact_thread` runs a compaction turn the server persists as such, and is `ThreadBusy` during a
  turn.
- `capabilities()` claims exactly what the adapter can now do; the README says the rest.
