# Claude Code Harness Support — Design and Implementation Plan

This note plans a second agent harness for Giskard: Anthropic's **Claude Code CLI**, driven through
its `--print --input-format stream-json --output-format stream-json` protocol, authenticated by the
user's **Claude Pro/Max subscription** (not an API key).

It is a planning note, not an authoritative product spec. Once the direction is agreed, the final
contract folds into `specs/giskard-specification.md` (§4, §6.4, §8, §9, §12.2, §13.5) and
`crates/giskard-harness-claude/README.md`.

Protocol facts below were verified empirically against **Claude Code 2.1.233** (live stream-json
session, plus the shipped CLI binary's own schemas and argv construction). Anything not verified is
marked **[unverified]**.

Section references written as "spec §X" point at `specs/giskard-specification.md`; bare "§X" refers to
this document.

---

## 1. Decisions already taken

| Decision | Choice |
| --- | --- |
| Harness binding granularity | **Per thread inside a project.** One project may hold Codex threads and Claude threads side by side. |
| What selects the harness | **The thread's model.** A thread whose `ModelRef.provider` belongs to an Anthropic/Claude-Code provider runs on the Claude harness. No separate harness selector in the UI. |
| Child-process model | **One persistent `claude` child process per loaded thread**, alive across turns. Spawned only when a thread with an Anthropic model is loaded. **No idle reaping in the MVP.** |
| Structured diffs | **`structured_diffs: false` in v1.** Synthesize `FileChange`/`DiffUpdated` from `Edit`/`Write` tool calls + git in a later phase. |
| `AcceptForSession` when the harness offers no rule to persist | **Keep the button visible anyway** (§9.3). It behaves as a one-off `Accept`; log the degradation and revisit only if users report it. |
| Settings sources for child processes | **`--setting-sources user`** (§8.3): the user's own `~/.claude/settings.json` applies, so extra writable roots and personal rules are configured where the user already keeps them. Project and local scopes stay excluded. The accepted cost is that a user allow-rule can pre-approve a call `ask_first` would otherwise have asked about. |
| Live approvals | **Supported and verified end to end (§9).** MVP uses the `--permission-prompt-tool stdio` channel, in the adapter's first working milestone. The hook route is postponed to a later decision and refactor (§9.4); the MCP-tool route is rejected (§9.1). |

---

## 2. The two harnesses are shaped differently

| | Codex (today) | Claude Code (planned) |
| --- | --- | --- |
| Transport | stdio newline-delimited **JSON-RPC** | stdio newline-delimited **JSON objects** (two overlaid channels: transcript messages, and `control_request`/`control_response`) |
| Protocol crate | `codex-codes` 0.151.2 (typed, versioned) | **`claude-codes` 2.1.259** — same author and repository (`meawoppl/rust-code-agent-sdks`), same feature shape, version tracking the CLI (§3.7) |
| Processes | **1 `codex app-server` per project**, multiplexing every thread | **1 `claude` per session**; a session ≈ one Giskard thread |
| Native thread id | Codex-minted rollout id | **client-minted UUID** via `--session-id`; resumed with `--resume=<uuid>` |
| Concurrency | one worker task fans out to N threads | N independent children, each single-threaded through its own turn. **Verified:** two sessions in the same cwd ran concurrent tool-using turns, each with its own `<uuid>.jsonl` under one cwd-encoded directory, no locking or contention (§3.4). |
| Turn ids | native `turnId` | **none** — Giskard mints every `TurnId`; a turn is "user message → `result`" |
| Model catalog | `model/list` RPC | no RPC; static built-in catalog |
| Session storage | Codex thread store | `~/.claude/projects/<encoded-cwd>/<uuid>.jsonl` — **cwd-scoped** |

Two consequences drive the whole design:

1. **`AgentHarness` does not need to change.** It is already object-safe, project-shaped, and
   thread-addressed. A `ClaudeHarness` is a project-scoped façade that internally owns a
   `HashMap<ThreadId, ChildSession>`. "One working context = one harness instance" (spec §4.7) still
   holds; the instance just fans out to children instead of multiplexing one pipe.
2. **Resume is cwd-scoped**, and the encoding is lossy (§3.7). Resuming works — **verified**: a fresh process launched with
   `--resume=<uuid>` answered a question that could only be answered from the previous process's
   conversation, and reused the same session id rather than forking. But the transcript it reads lives
   under a directory keyed by cwd, so a thread using a per-thread Git worktree
   (`docs/git-worktrees.md`) must always respawn with the *same* cwd, or `--resume` silently cannot
   find the session. Giskard already persists `ThreadFile.git_workspace`, so the adapter must derive
   cwd from the thread, never from the project.

---

## 3. Verified protocol facts (Claude Code 2.1.233)

### 3.1 Invocation

```
claude -p --input-format stream-json --output-format stream-json --verbose \
       --session-id <uuid> --model <id> --effort <level> \
       --permission-mode <mode> --add-dir <root>... \
       --permission-prompt-tool stdio            # routes approvals to Giskard (§9)
       [--resume=<uuid>] [--include-partial-messages] [--replay-user-messages]
```

Stdin stays open; each user turn is one JSON line
(`{"type":"user","message":{"role":"user","content":[{"type":"text","text":"…"}]}}`). The process
keeps serving turns until stdin closes.

### 3.2 Output messages observed

| Message | Use in Giskard |
| --- | --- |
| `system/init` | effective `session_id`, `model`, **`permissionMode`**, `tools`, `mcp_servers`, `slash_commands`, `apiKeySource`, `claude_code_version`. Authoritative — read it, do not assume what was requested was applied. |
| `stream_event` | raw Anthropic streaming events (`content_block_delta`, …) → `ItemDelta` |
| `assistant` | complete message with `text` / `thinking` / `tool_use` blocks → `AgentMessage` / `Reasoning` / `ToolCall` items |
| `user` (tool_result) | tool output + `tool_use_result` (stdout/stderr/interrupted) → `ItemCompleted` |
| `result` | terminal per turn: `usage`, `total_cost_usd`, `modelUsage[model].contextWindow`, `stop_reason`, `is_error`, `permission_denials`, `terminal_reason` → `TurnCompleted` |
| `autocompact_state` | `effective_window` / `threshold` → `ContextWindowUpdated` (the *effective* window, exactly the Codex analogue: 947 000 for a 1 M Sonnet) |
| `rate_limit_event` | `rateLimitType: "five_hour"`, `resetsAt`, `overageStatus` → **subscription-plan headroom**; surface as `Notice` |
| `system/status`, `system/task_summary`, `system/post_turn_summary` | activity/labels; `post_turn_summary` carries `status_category` (`review_ready`, `blocked`, …), `status_detail` and `needs_action` |
| `system/thinking_tokens` | running reasoning-token estimate during a turn |
| `thinking` content blocks | carry an opaque `signature`; map to `Reasoning` items and never re-send the text as input |
| `tool_result_meta` | `non_execution_kind` (e.g. `"permission-rule"`) distinguishes "tool ran and failed" from "tool never ran" |
| `system/permission_denied` | a denial with `decision_reason_type` (`rule`/`mode`/`classifier`/…) → `Notice` |
| `system/commands_changed` | slash-command inventory (large; elide from logs) |

### 3.3 The control channel (verified)

Client → CLI, as `{"type":"control_request","request_id":…,"request":{…}}`:

- `initialize` — accepts `hooks`, `sdkMcpServers`, `systemPrompt`, `appendSystemPrompt`,
  `planModeInstructions`, `toolAliases`, `supportedDialogKinds`. Answered with the command/agent
  inventory. **Verified working.**
- `interrupt` — **verified working**; response `{"still_queued":[]}`. This is `AgentHarness::interrupt`.
- `set_model` (`{subtype:"set_model", model:"<id>"}`) and `set_permission_mode`
  (`{subtype:"set_permission_mode", mode:"<mode>"}`) — **verified working mid-session**: a live child
  started on Sonnet answered turn 1 as `claude-sonnet-5`, accepted `set_model`, and answered turn 2 as
  `claude-haiku-4-5`, with **no respawn and no session change**. `set_permission_mode` echoes the
  applied mode (`{"mode":"plan"}`). Note the CLI **re-emits `system/init`** after a model change, so
  the adapter must treat `init` as a repeatable announcement, not a one-shot handshake, and re-read
  the effective model from it.
- `apply_flag_settings` (`{subtype:"apply_flag_settings", settings:{…}}`) — the general session-settings
  channel, carrying `effortLevel`, `ultracode`, `model`, `fastMode`, `advisorModel`, `viewMode`.
  **Verified for effort:** a child started with `--effort low` reported `effort: "low"`, accepted
  `{settings:{effortLevel:"high"}}`, and then reported `effort: "high"` — so reasoning effort is
  changeable mid-session exactly as the model is. **Invalid values fail silently**: `effortLevel:
  "banana"` was answered `success`, left the previous value in place, and produced no error anywhere
  the client can see.
- `get_settings` — returns `{applied, effective, sources}`, where `applied` carries the session's live
  `model`, `effort` and `ultracode`. This is the read-back channel for anything set through
  `apply_flag_settings`, and the only way to confirm an effort change actually landed.
- `control_cancel_request` — cancels an in-flight control request.

CLI → client:

- `can_use_tool` — the permission ask (§9).
- `request_user_dialog` / `elicitation` — MCP elicitation and host dialogs → maps to Giskard's
  existing `ServerRequestReceived` / `respond_server_request` path.

### 3.4 Simultaneous sessions in one working directory

**Verified supported.** Two children were launched in the same cwd at the same time, each with its own
`--session-id`, and both ran a Bash tool call to completion (`is_error: false`, `stop_reason:
"end_turn"`) with both approvals granted through `can_use_tool`. Transcripts landed as two separate
`<uuid>.jsonl` files inside the single cwd-encoded directory
(`~/.claude/projects/<encoded-cwd>/`). No lock file, no serialization, no cross-talk.

This is what makes the per-thread child model viable: a project's threads share a directory by design,
and the CLI does not treat that as exclusive. The residual hazard is **not** the CLI — it is two agents
editing the same files at once, which is exactly what per-thread Git worktrees
(`docs/git-worktrees.md`) already exist to isolate. Threads sharing the project workspace can collide
on file content under Claude for the same reason they can under Codex.

One caveat to carry into the adapter: `.claude/` project state within the cwd (checkpoints,
project-scoped settings) is shared by every session in that directory. Nothing observed conflicts, but
it means "same cwd" is not full isolation.

### 3.5 Configuration surface

Claude Code's configuration file is `settings.json`, resolved from several scopes with **managed
(policy) settings highest, then command-line flags, then `.claude/settings.local.json`, then the
project's `.claude/settings.json`, then `~/.claude/settings.json`**.

**`~/.claude/settings.json` and `~/.claude.json` are different files and must not be conflated.**

- `~/.claude/settings.json` is the **user scope of the settings schema** — the same shape as the
  project and local files, so it can carry anything they can, `permissions.additionalDirectories`
  included. Scope comes from **where the file is**, not from anything inside it: there is no
  per-project section within a settings file, so `additionalDirectories` set at user scope is global
  to every session that loads user settings, and narrowing it to one project means putting it in that
  project's `.claude/settings.json` instead. **Verified:** a user-scope settings file granting an
  outside directory made a `Write` beyond the workspace proceed with no ask under
  `--setting-sources user` — from a session whose working directory was unrelated to the granted path
  — and the same file was ignored, one ask, under `--setting-sources ""`. (Tested with
  `CLAUDE_CONFIG_DIR` pointed at a throwaway directory, which is also the clean way to sandbox a child
  from the real config.)
- `~/.claude.json` is **account and machine state, not configuration**: a freshly created one held
  `oauthAccount`, `userID`, `machineID`, cached feature flags and experiment data, migration markers,
  notification state, and a `projects` map of per-project *state* — conversation history, MCP servers,
  onboarding flags — which is unrelated to the permission schema. It contained no `permissions` block
  and no `additionalDirectories`. It is where the OAuth session lives, which is why authentication is
  independent of the `--setting-sources` choice. (Observed on a file generated by these headless runs; a long-lived
  one accumulates more per-project entries, so treat this as its shape rather than an exhaustive
  schema.)

The practical consequence for Giskard: the permission surface is settings-schema state, so
`--setting-sources` decides exactly which files reach it. Under the chosen `user` scope (§8.3) the
machine owner's file contributes and a checkout's files do not, while authentication is unaffected
either way because it is not settings-schema state at all.

It is a flat file of many top-level keys rather than a few grouped sections. The ones that matter to a
Giskard adapter:

| Key | Relevance |
| --- | --- |
| `permissions` | `allow` / `deny` / `ask` rule lists, **`additionalDirectories`**, `defaultMode`, `disableBypassPermissionsMode` — the whole permission surface §8.1 and §9 operate on |
| `env` | environment variables applied to the session; the route by which provider selection is configured (below) |
| `model`, `availableModels`, `enforceAvailableModels`, `fallbackModel` | model selection and restriction |
| `apiKeyHelper`, `awsCredentialExport`, `awsAuthRefresh` | credential production for non-subscription auth |
| `autoCompactEnabled`, `autoCompactWindow` | the compaction behaviour whose state arrives as `autocompact_state` (§3.2) |
| `cleanupPeriodDays` | how long session transcripts survive — relevant because Giskard's `--resume` depends on them (default 30 days) |
| `disableAllHooks`, `allowManagedHooksOnly` | constrain the hook route if it is ever adopted (§9.4) |
| `allowedMcpServers`, `deniedMcpServers`, `disabledMcpjsonServers` | MCP surface |

**`--settings` and `--setting-sources` are independent, and this is load-bearing.**
`--setting-sources` selects which settings *files* are consulted; `--settings` supplies an explicit
payload as a file path or inline JSON. **Verified:** an inline `--settings` payload is applied even with
`--setting-sources ""`, so it is not merely an override of whichever files happened to load. The test: in `acceptEdits` mode a
`Write` outside the workspace prompts, and adding
`--settings '{"permissions":{"additionalDirectories":["/tmp/outside-dir"]}}'` made the same write
proceed with no ask.

So the two mechanisms compose: `--setting-sources user` decides which of the user's files apply (§8.3),
while `--settings` can add configuration for one child that exists nowhere on disk. Two consequences:

- **`--add-dir` has a settings-level equivalent**, `permissions.additionalDirectories`, verified above.
  Either mechanism works; the flag is simpler for a fixed list, the payload is better if Giskard ever
  needs to send permission state and directories together.
- **The hook route's open mechanical question is answered** (§9.4): a per-child `--settings` payload is
  a viable way to install a hook ephemerally, without writing to the user's settings files.

**There is no provider registry.** Unlike Codex — whose `[model_providers.<id>]` tables name endpoints
and key sources — Claude Code selects its backend entirely through **environment variables**:
`CLAUDE_CODE_USE_BEDROCK`, `CLAUDE_CODE_USE_VERTEX`, `CLAUDE_CODE_USE_FOUNDRY` for first-party clouds,
and `ANTHROPIC_BASE_URL`, `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_CUSTOM_HEADERS`, `ANTHROPIC_MODEL`,
`ANTHROPIC_SMALL_FAST_MODEL` for a gateway or proxy. These can be set in the child's environment
directly or through the `env` key of a `--settings` payload.

This shapes what a Giskard "provider" means for this harness. For Codex a provider is a routing choice
recorded in `~/.codex/config.toml`; for Claude Code it is **a property of how the child process was
launched**. The environment is the provider configuration, which is what the adapter reports through
`list_providers` (§5.1). Two consequences: a provider is fixed at spawn and cannot change within a live
child — only the model can (§3.3) — and a subscription-backed `anthropic` provider differs from a
Bedrock one only in the environment its children receive, not in anything the protocol carries.

### 3.6 User attachments

Giskard's `UserInput::Text` carries `Vec<UserAttachment>` (`name`, `mime_type`, `size`,
`kind: Image | File`, `data_base64`), so the adapter has to put them somewhere. Claude Code accepts
them **inline in the user message**, as Anthropic content blocks alongside the text block — verified
with tools disabled, so the answers could only have come from the attachment:

| Attachment | Block | Result |
| --- | --- | --- |
| PNG image | `{"type":"image","source":{"type":"base64","media_type":"image/png","data":…}}` | described the image correctly |
| PDF | `{"type":"document","source":{"type":"base64","media_type":"application/pdf","data":…}}` | read text out of the document |
| Plain text | `{"type":"document","source":{"type":"text","media_type":"text/plain","data":…}}` | read the file's contents |

**The encoding differs by type, and getting it wrong fails at the API rather than at the CLI.** A text
document sent as `source.type: "base64"` came back as `API Error: a document in the conversation could
not be processed and was removed`. Since `UserAttachment` always stores `data_base64`, the adapter must
**decode text attachments back to a string** and pass `source.type: "text"`, while images and PDFs keep
their base64.

This is markedly simpler than the Codex path, which uploads non-image files to the harness host with
`fs/createDirectory` + `fs/writeFile`, appends the host path to the prompt, and then has to clean the
upload directory up on turn end, stream loss, failed start, and shutdown. None of that is needed here:
no temp directory, no cleanup, nothing to leak.

Two limits to respect:

- **Inline attachments consume context**, and the underlying API bounds document and image size. Large
  files should be rejected with a clear message rather than silently truncated. The exact thresholds
  are not pinned down here.
- **Other file types** (`.docx`, archives, binaries) have no inline representation. The fallback is the
  Codex-shaped one — write the file into the workspace and name the path in the prompt, letting the
  agent's own `Read` tool open it — but that requires a writable location and cleanup, so v1 may simply
  decline them with a message. [unverified]

Giskard's existing rule that raw attachment bytes stay out of persisted history and the in-memory
history cache applies unchanged: `UserInput`'s serializer already drops `data_base64`.

### 3.7 The `claude-codes` crate

An earlier draft of this plan said no Rust crate existed and Giskard would own the wire types. **That
was wrong.** `claude-codes` 2.1.259 is published by the author of `codex-codes`, from the same
repository (`meawoppl/rust-code-agent-sdks`), under Apache-2.0 — already on `deny.toml`'s allow list —
with MSRV 1.85 against Giskard's 1.88, the same `async-client` feature shape, and a version number
that tracks the CLI release it models.

**What it covers**, verified by reading 2.1.259's source:

- the stream-json message models and streaming parsers (`ClaudeInput`, `ClaudeOutput`);
- the control protocol as `ControlRequestPayload::{CanUseTool, HookCallback, McpMessage, Initialize,
  Interrupt}`, with `PermissionResult::{Allow{updated_input, updated_permissions},
  Deny{message, interrupt}}` — the exact shapes §9 verified by hand, including the `interrupt` flag
  that `Cancel` maps onto;
- the permission vocabulary `PermissionSuggestion`, `PermissionRule`, `PermissionDestination`,
  `PermissionBehavior`, `PermissionModeName` — so §9.3's destination invariant becomes a typed choice
  rather than a string convention;
- an async client with `resume_session(uuid)`, `send`, `receive`, `interrupt`,
  `enable_tool_approval`, `send_control_response`, `session_uuid`, `take_stderr`, `shutdown`;
- **`transcript.rs`**, which encodes the `~/.claude/projects/<encoded-cwd>/<session>.jsonl` location
  rule — including that the encoding is **lossy and not injective**, so `a/b.c` and `a/b/c` collide.
  That is precisely the hazard behind §2's cwd-scoped resume constraint, documented by someone who
  measured it rather than guessed.

**Forward compatibility** is designed in — enums carry `Unknown(String)` variants that round-trip
verbatim, which is the same tolerance §12 asks of the mapper.

### 3.7.1 Gaps in `claude-codes` — candidates to upstream

Audited against 2.1.259 by reading its source. **Nothing here blocks the adapter**: enums carry
`Unknown` variants that round-trip verbatim, `ClaudeInput::Raw(Value)` sends anything unmodelled,
`receive_raw()` reads anything unparsed, and the client constructor takes an already-spawned `Child`
so Giskard can own argv. These are typed-access gaps — each one is a place the adapter would otherwise
hand-build JSON that the crate is the natural home for.

Ordered by how much the design leans on them.

| # | Missing | Shape | Why this design needs it | Evidence |
| --- | --- | --- | --- | --- |
| 1 | `ControlRequestPayload::ApplyFlagSettings` | `{subtype:"apply_flag_settings", settings:{effortLevel, ultracode, model, fastMode, advisorModel, viewMode}}` | The only way to change **reasoning effort** on a live child (§3.3) | verified: `--effort low` child accepted `{effortLevel:"high"}` and reported `high` after |
| 2 | `ControlRequestPayload::SetModel` | `{subtype:"set_model", model:"<id>"}` | Per-turn model switching without respawning (§3.3) | verified: Sonnet child answered as Haiku on the next turn, same session |
| 3 | `ControlRequestPayload::SetPermissionMode` | `{subtype:"set_permission_mode", mode:"<mode>"}` | Plan/Build switching per turn (§8.2) | verified: response echoes `{"mode":"plan"}` |
| 4 | `ControlRequestPayload::GetSettings` + response | request takes no params; response `{applied:{model, effort, ultracode}, effective, sources}` | Read-back for the above. **Load-bearing**, not cosmetic: an invalid `effortLevel` is answered `success` and silently ignored, so this is the only way to confirm a change landed (§3.3) | verified: `applied.effort` moved `low` → `high`; `"banana"` was accepted and ignored |
| 5 | `ContentBlock::Document` | `{"type":"document","source":{…}}` where source is `{type:"base64", media_type, data}` **or** `{type:"text", media_type, data}` | PDF and plain-text user attachments (§3.6). `ContentBlock` has `Image` but no `Document` | verified both directions, including that a text document sent as base64 fails at the API with "document … could not be processed and was removed" |
| 6 | `SystemMessage` subtype `autocompact_state` | `{enabled, effective_window, threshold, enforced, source}` | The **effective** context window, which is what the gauge wants (§6) | observed on every session start |
| 7 | `SystemMessage` subtype `post_turn_summary` | `{summarizes_uuid, status_category, status_detail, needs_action}` | Activity labels; nice-to-have | observed, including `status_category: "blocked"` after a denial |
| 8 | CLI builder flags | `--forward-subagent-text`, `--effort` | The first is **mandatory** for sub-agent child threads (§5.8); the second sets initial effort | present in `claude --help`, absent from `cli.rs` |

Item 5 is the one worth doing carefully: the base64-versus-text distinction is invisible until it
fails remotely, so a typed `DocumentSource` that makes the wrong pairing unrepresentable is worth more
than the block itself.

**Already covered**, and worth not duplicating: `parent_tool_use_id` (the routing key §5.8 depends on),
the whole `task_started` / `task_progress` / `task_updated` / `task_notification` family,
`non_execution_kind`, `blocked_path`, `permission_suggestions`, `RateLimitEvent`, `modelUsage`,
`thinking_tokens`, and the transcript-location rule.

---

## 4. Capability matrix

| Capability | Claude | Basis |
| --- | --- | --- |
| `live_approvals` | **true (verified)** | `can_use_tool` control request; response `{behavior:"allow",updatedInput?,updatedPermissions?}` \| `{behavior:"deny",message?,interrupt?}`. Round trip and blocked execution confirmed — §9. |
| `plan_build_modes` | **true (verified)** | `--permission-mode plan` + `set_permission_mode`, which echoes the applied mode. Semantics differ — see §8. |
| `per_turn_model` | **true (verified)** | `set_model` mid-session, no respawn (§3.3) |
| `reasoning_effort` | **true (verified), and dynamic** | `--effort low\|medium\|high\|xhigh\|max` at spawn, then `apply_flag_settings{effortLevel}` mid-session — verified to change `low` → `high` on a live child, and confirmable through `get_settings.applied.effort` (§3.3). An unknown value is accepted and ignored silently, so the adapter validates against the catalog and may read back. `Effort` is already an open string newtype, so the differing value set costs nothing |
| `structured_diffs` | **false (v1)** | no native diff feed |
| `resumable_threads` | **true (verified)** | a fresh process launched with `--resume=<uuid>` recovered the earlier conversation's content and kept the same session id (no fork). cwd-scoped — §2 |
| `model_listing` | **true (static)** | no RPC; adapter returns a built-in catalog of Claude models, with `provider` set so entries land under the right provider (§6) |
| `provider_listing` | **true (synthetic)** | no provider registry exists, but the child's backend is environment-selected, so the adapter reports what a child would route to (§5.1). This is also what tells Giskard which harness owns a provider id |
| `token_usage` | **true (observed)** | `result.usage` on every turn, plus per-model totals in `result.modelUsage` — §6 |
| `mcp_status` | **true (read-only)** at the harness level [unverified with servers configured] | `system/init.mcp_servers` carries the inventory; only ever seen empty here. The project-scoped MCP *endpoints* stay Codex-only in v1 (§5.4) |
| `mcp_reload` | **false (v1)** | only the interactive `/mcp reconnect` |
| `mcp_oauth_login` | **false** | interactive only |
| `context_compaction` | **true** | `/compact` as a user message [unverified]; `autocompact_state` feeds the gauge |
| Native rename / archive / delete | **unsupported** | no equivalents — see §5.6 |
| `terminate_command` | **unsupported (v1)** | background shells are controlled by the agent's own `KillShell` tool, not from outside |
| Linked sub-agent threads | **supported, as local child threads** | The child is not a resumable session, but its whole transcript is forwarded and can be materialized as a read-only Giskard thread keyed by the Task call's `tool_use_id` — §5.8 |

---

## 5. Multi-harness architecture


### 5.1 Harness identity comes from the provider table

**This section was rewritten after the provider rework on `main`** (`4dfc72a` *Let the harness own
provider configuration*, `cfc4625` *Key the provider table by routing id*, `8e00156` *Drop the unused
wire_api provider field*). The earlier plan proposed adding a `harness` field to `ProviderConfig`.
That is now both impossible and unnecessary.

Impossible, because `ProviderConfig` is deliberately minimal and `#[serde(deny_unknown_fields)]`: a
declaration is keyed by routing id (`[providers.<id>]`) and carries only `model_listing` and `models`.
Everything else — display name, endpoint, key location — is **the harness's** configuration, read back
through `AgentHarness::list_providers` behind the new `provider_listing` capability.

Unnecessary, because that same table already answers "which harness owns this provider":

> **The harness that reports a provider id owns threads on it.**

No config surface, no second source of truth, and it reuses the machinery `main` just built —
`harness_knows_provider` (`routes.rs:5054`) already asks exactly this question to validate configured
ids. Multi-harness turns a per-project lookup into a per-project *aggregation*: ask each harness the
project can use, and the union is the picker's provider set.

**What the Claude adapter reports.** §3.5 found that Claude Code has no provider registry — its
backend is selected by environment variable. That maps onto `HarnessProvider` without straining:
the environment *is* the configuration, so the adapter inspects its own and reports what a child would
actually route to.

| Environment | Reported `HarnessProvider` |
| --- | --- |
| default (subscription OAuth) | `{ id: "anthropic", name: Some("Anthropic (Claude Code)"), base_url: None, auth: None }` |
| `ANTHROPIC_BASE_URL` set | the same id with that `base_url`, so Giskard's `/v1/models` discovery can reach a gateway |
| `CLAUDE_CODE_USE_BEDROCK` / `_VERTEX` / `_FOUNDRY` | the corresponding id, no `base_url` |

`base_url: None` simply means no discovery to run, which §8.3 already treats as unremarkable for a
defaulted-on provider. `auth` stays `None`: the subscription credential is an OAuth session in
`~/.claude.json` (§3.5), not an environment variable or a command, and `HarnessProvider` deliberately
carries only a key's *location*. There is no inline secret to leak because there is nothing to report.

**A Claude provider therefore needs no `config.toml` entry at all**, which is the same promise the
provider rework makes for Codex. A `[providers.anthropic]` block remains optional, for pinning picker
order or declaring models by hand.

**Open: id collisions across harnesses.** With one harness per project the table is unambiguous. With
two, both could report the same id — Codex can be configured with an `anthropic` `[model_providers]`
entry, and then `anthropic/claude-opus-5` names a route both harnesses claim. A rule is required
before this ships; the cheapest is that the project's default harness wins and the loser is reported
as a picker warning, which is also the one concrete job left for `ProjectConfig.harness` (open
question 1).

### 5.2 A second harness inside the project authority

**The constraint that shapes this.** `AGENTS.md` now names `RegistryShared::projects` and
`::threads` as the only strong process-local owner maps for project and thread identity, and forbids
"a peer owning map keyed directly or indirectly by project or thread identity". An earlier draft of
this plan proposed exactly that — `HashMap<(ProjectId, HarnessKind), Arc<dyn AgentHarness>>`. It is
not available, and the rule is right: a second keyed map is a second place a project's liveness can
disagree with itself.

So the second harness lives **on the authority that already owns the first**. Today:

```rust
struct ProjectHarnessSlot { current: Mutex<Option<ProjectHarnessState>> }
enum ProjectHarnessState {
    Active(Arc<dyn AgentHarness>, DriverHandle),
    Deleting(Arc<dyn AgentHarness>, DriverHandle),
}
```

The change is to the slot's *cardinality*, not its ownership:

```rust
struct ProjectHarnessSlot { current: Mutex<HashMap<HarnessKind, ProjectHarnessState>> }
```

Keying by harness kind is not keying by project or thread identity, so this stays inside the rule:
the map is entity-local state on `ProjectAuthority`, reached only through the authority, exactly as the
single slot is now.

**What this preserves for free**, which is the argument for doing it here rather than anywhere else:

- **The transition fence.** `HarnessTransitions` remains the root serialization point; a project slot
  is still only reachable through `HarnessTransitionGuard::project`, and `begin_shutdown` still fences
  creation before draining. Multi-harness changes what a drain *iterates*, not what it means.
- **Driver pairing.** `ProjectHarnessState` already carries its `DriverHandle` alongside the harness,
  and a driver consumes that harness's `DiscoveryStream`. One entry per kind therefore yields one
  driver per harness with no new wiring — which is required anyway, since two harnesses cannot share
  one discovery stream.
- **Deletion and shutdown semantics.** `begin_delete` and `take_for_shutdown` become iterations that
  return every installed pair rather than one; `Deleting` remains per entry, so one harness can be
  draining while another still serves turns.

**Resolution moves from the project to the thread.** `RegistryShared::active_harness(project_id)`
(`registry.rs:294`) is the current answer to "which harness", used by `start_turn`, `interrupt`,
compaction, command termination and the lifecycle operations (`:935`, `:997`, `:1095`, `:1217`,
`:1805`). Each becomes "the harness for *this thread*", resolved from the thread's durable harness kind
(§5.3) and cached on its `ThreadAuthority` alongside the runtime it already holds. `ThreadAuthority`
carries `thread_id`, `project_id`, an owner lock, a coordinator and a runtime slot — the kind belongs
with them rather than in a lookup beside them.

Approval and server-request routing need no new map: both already resolve through the thread, so they
inherit the answer.

**Instantiation stays lazy per kind.** A kind's entry is created when the first thread of that kind
opens, which is what keeps the §1 promise that a `claude` child starts only when a thread with an
Anthropic model is loaded — and it means a Codex-only project never constructs a Claude harness at all.

**`HarnessFactory::create`** takes the kind alongside the config and bootstrap, and the binary's
factory dispatches on it instead of rejecting everything but `"codex"`
(`bin/giskard-server.rs:19`). `HarnessBootstrap.known_threads` must be filtered to the threads
belonging to that kind, or a Claude harness would be handed Codex's rollout ids to install.

### 5.3 Persistence

`ThreadFile` gains the durable harness kind and, so a thread can cross harnesses and come back, the
native id it held under each:

```rust
#[serde(default = "default_harness")]           // "codex" for every existing file
pub harness: HarnessKind,
/// Native id per harness, so switching a thread's model across harnesses and back resumes the
/// original native session instead of orphaning it.
#[serde(default, skip_serializing_if = "HashMap::is_empty")]
pub harness_thread_ids: HashMap<HarnessKind, String>,
```

`harness_thread_id` keeps its meaning — the **active** harness's id — so every existing reader is
unaffected and there is no migration beyond serde defaults. Both fields are ordinary durable metadata:
a mutation advances `ThreadFile.revision` under the same per-thread lock that commits it, like any
other.

Project creation derives the kind from the project's `default_model` rather than hardcoding
`"codex"`, since creation asks for a model and never for a harness (§5.1).

### 5.4 Project-scoped queries become per-harness

§5.2 covers the thread-addressed operations. The rest of the registry's harness surface is
project-scoped and assumes one answer per project:

| Call | Today |
| --- | --- |
| `HarnessRegistry::capabilities` (`registry.rs:1319`) | capabilities of *the* project harness |
| `HarnessRegistry::list_models` (`:1295`) | catalog overlay for `GET /api/projects/{id}/models` |
| `HarnessRegistry::list_providers` (`:1303`) | provider table behind discovery and id validation (§5.1) |
| MCP status / reload / OAuth | the project's MCP endpoints |

Three different rules apply, and conflating them is how this goes wrong:

- **Capabilities are thread-scoped.** The capability-driven UI (spec §13.5) decides whether to render
  approval cards, the effort selector, the diff viewer — answers that now differ between two threads of
  one project. A project-level answer is wrong for one of them. Resolve through the thread; on a draft,
  through the harness that owns the model being selected.
- **Catalogs and provider tables aggregate.** Each harness contributes its own providers and models,
  and the union is the picker. `harness_knows_provider` must consult every installed harness before
  calling an id unknown, or a Claude provider is reported as a mistake by the Codex harness that has
  never heard of it — its existing "every failure answers known" caution extends to "one harness's
  silence is not evidence either".
- **MCP endpoints stay Codex-only in v1** (§10). Claude's MCP status is per-child and read-only, so
  aggregating it would mean starting children to answer a project-level question.

**The tension worth naming:** aggregation wants every harness alive, but instantiating a harness is
exactly what §1 says to avoid until a thread needs it. Two things resolve it. `ProjectAuthority`
already owns a `ProjectModelCatalogSlot`, so the composed catalog is cached per project and the cost is
paid per refresh rather than per request. And the two harnesses are not symmetric: Codex needs a live
app-server to answer `config/read`, while the Claude harness answers `capabilities`, `list_models` and
`list_providers` from static knowledge (§5.1) — so the façade must be constructible without spawning a
child. **Creating a Claude harness must stay free; only `open_thread` spawns.**

### 5.5 Switching a thread across harnesses

Selecting `anthropic/claude-opus-5` on a Codex thread is a **native-thread boundary** — strictly
stronger than the provider switch analyzed in `model-provider-switching-analysis.md`, because there
is no protocol that can carry Codex history into a Claude session. Contract:

1. Giskard's own transcript, ledgers, diffs and worktree are untouched (they are Giskard-owned).
2. The old harness's native session is left intact and remembered in `harness_thread_ids`.
3. The new harness opens a **fresh** native session; the thread's agent-side context starts empty.
4. The UI must say so before it happens (a confirm on the model picker, wording modelled on the
   existing C5 "agent context was lost — your history is intact" notice), and a `Notice` event
   records it in the transcript.
5. Switching back resumes the remembered id.

This is the single most user-visible sharp edge of per-thread harnesses and needs the same
documentation treatment as worktrees.

### 5.6 Operations the Claude harness cannot do

`harness_api_error` (`routes.rs:3784`) maps `HarnessError::Unsupported` to **400**, and
`set_thread_archived` (`registry.rs:1251`), `set_thread_name` (`:1268`) and `delete_thread` (`:1341`)
call the harness *before* touching local state. On a Claude thread — no native rename, archive or
delete — renaming would fail with a 400 and never reach the local mutation.

The trait already declares these optional: its default implementations return `Unsupported`. The
server contradicts that by turning the declaration into a user-visible error. Fix the contradiction on
the server side: treat `Unsupported` from these three as a **soft** path — log at `debug`, perform the
local mutation, and for delete skip only the native step. Error-path tests per `AGENTS.md`.

This is P1 in §11 and stands on its own: it settles whether a harness may decline an operation at all,
independently of Claude.

### 5.7 Process lifecycle (MVP)

- Spawn on `open_thread`: one child per thread, `--session-id <fresh uuid>` or `--resume=<stored>`.
- cwd is the thread's worktree when it has one, else the project workspace root, and it must be the
  same cwd on every respawn — the transcript lives under a cwd-derived directory whose encoding is
  lossy (§3.7). `ThreadHandle.workspace_root` now carries this, so the handle states which root the
  thread was opened against rather than leaving callers to recompute it.
- Extra writable roots come from the user's own `permissions.additionalDirectories` (§8.3); the adapter
  passes `--add-dir` only for roots Giskard itself introduces.
- **Per-thread turn serialization is already provided.** The old `ThreadTurnGate` is gone; a thread's
  `ThreadAuthority` owns an `OwnerLock` and a coordinator consuming `TurnIntent`s (`docs/m5-turn-intents.md`),
  which serializes turns per thread regardless of harness. Children of one project run concurrently in
  one cwd with no coordination between them (§3.4), so the adapter needs no cross-thread locking of its
  own.
- Events reach the server through a retained `EventLog` per thread rather than a broadcast channel —
  the Codex adapter's `EventLogs(HashMap<ThreadId, Arc<EventLog>>)` is the shape to copy — and the
  project's driver consumes the harness's `DiscoveryStream`. For Claude that stream is legitimately
  empty (§5.8), so `discoveries()` returns `DiscoveryStream::closed()` and `claim_native_thread` stays
  `Unsupported`.
- `HarnessBootstrap.known_threads` installs the native↔Giskard identity table before the harness
  dispatches anything. For Claude this is the façade's thread→child routing table, and it must be
  filtered to this harness's threads (§5.2).
- Post-open metadata has a channel: `OpenThreadOptions.updates` accepts
  `ThreadUpdate::ContextWindowRestored`, which is where the `autocompact_state` a child emits at
  startup belongs (§6).
- **No idle reaping in the MVP.** `harness.idle_shutdown_secs` is declared in config and implemented
  nowhere, and the MVP does not change that. A `claude` process was measured at **440–530 MB RSS**, so
  the spec's ~10-thread scale (spec §1.4) is gigabytes if every thread is loaded. Reaping is the first
  post-MVP follow-up; the MVP should at least log the live-child count so the growth is visible before
  it becomes a complaint.
- Child exit during a live turn → `TurnCompleted{Failed}` plus `Error`, thread marked disconnected,
  the same recovery path as a Codex app-server crash.
- Record `claude_code_version` from `system/init` and warn when it differs from the version the mapping
  was tested against — the drift guard the spec already mandates for Codex, and still worth having with
  `claude-codes` carrying the wire types (§3.7).

### 5.8 Sub-agent threads without native sessions

Giskard expresses sub-agent structure **only** as threads: `Item` has no parent field, `ItemDelta` is
just `Text` and `CommandOutput`, and every affordance in the UI — the Sub-agents card, subtree
navigation, per-child activity hoisting — is keyed on `ThreadKind::Subagent` and `parent_thread_id`.
A harness whose children are not threads therefore renders as nothing at all, or as one opaque tool
call.

Claude's children are not sessions: a `Task` runs inside the parent's session and its records are
marked `isSidechain` in the same transcript (§3.6 note). But **the whole child transcript is
forwarded**, so a thread can be materialized from it. With `--forward-subagent-text`, one delegation
produced:

```
assistant parent=None            tool_use   Agent {"description":"Read data.txt for magic number"…}
system/task_started              task_id=add7f09e… tool_use_id=toolu_019ZAnC8…
user      parent=toolu_019ZAnC8  text       Read the file data.txt in the current working directory…
assistant parent=toolu_019ZAnC8  tool_use   Read {"file_path":"…/data.txt"}
user      parent=toolu_019ZAnC8  tool_result "1→the magic number is 4271"
assistant parent=toolu_019ZAnC8  text       The magic number is **4271**.
system/task_updated              patch={"status":"completed","end_time":…}
user      parent=None            tool_result [{"type":"text","text":"The magic number is **4271**"…}]
```

That is a complete turn: delegated prompt as user input, the child's own tool calls and their results,
and its closing message — not a narration of the work but the work itself.

**Design: the Task call's `tool_use_id` becomes the child's `harness_thread_id`**, prefixed to declare
what it is:

```
harness_thread_id = "task:toolu_019ZAnC8ARVNvy7R4aspovTx"
```

`parent_tool_use_id` is then the routing key: every forwarded item carries the id of the call it
belongs to, so items land in the child thread rather than interleaving into the parent's transcript as
if the main agent had run them. The parent's `Agent` item carries a `SubagentLink` with the same id,
`initial_prompt`, and `action`/`status` mapped from the `system/task_*` messages (`task_started` →
`Started`, `task_updated.patch.status` → `Completed`), which is what populates the Sub-agents card.

**Why prefix rather than infer from `ThreadKind::Subagent`.** The kind is available wherever a
`ThreadFile` is loaded, but not on the admission path: `HarnessBootstrap.known_threads` and
`claim_native_thread` deal in bare `(harness_thread_id, thread_id)` pairs. Code there cannot ask "is
this resumable?" without loading the thread. A prefix answers it at the point of use, and synthetic
prefixed identifiers are already established practice here — `app.js` special-cases
`subagent_prompt:` item ids.

**Three requirements this imposes:**

1. **`--forward-subagent-text` is mandatory**, not optional. Without it only the final result surfaces
   and every child thread is an empty shell.
2. **Child threads are permanently read-only.** `open_thread` must never attempt `--resume=task:…`;
   there is no session behind it. The existing read-only path for threads whose harness cannot attach
   (PS1, `read_only_info`) is the right mechanism, so this needs no new UI state — but it is
   *permanent* here rather than a recoverable condition, and the wording should not imply otherwise.
3. **The mapper keys off the tool named `Agent`.** The stream names the tool `Agent` in its `tool_use`
   block even though the CLI and its documentation call it `Task`.

**What makes this honest rather than a fiction.** The id is real, harness-minted and globally unique;
it is a different *category* of identifier, which the prefix states. And the child's transcript is
Giskard's own persisted history, so the thread stays readable forever — its unresumability is a
property it shares with any thread whose native session has expired (§3.5, `cleanupPeriodDays`), not a
special brokenness. What the design must never do is let a `task:` id reach `--resume`.

**Open.** A sub-agent that itself delegates: the inner call's `parent_tool_use_id` should nest one
level deeper, which the thread graph already supports, but it is unverified. Equally unverified is what
arrives when a delegation is interrupted mid-flight.

---

## 6. Models, context windows, tokens, cost

- **Catalog.** `list_models` returns a built-in list (`claude-opus-5`, `claude-sonnet-5`,
  `claude-haiku-4-5`, …) with display names and the `--effort` levels. Since the provider rework a
  `ModelDescriptor` carries `provider` and `is_default`, and `apply_harness_metadata` honours the
  default only when the provider matches or is empty — so the Claude catalog should **set
  `provider: "anthropic"`** rather than leaving it blank as the Codex `model/list` catalog does,
  which keeps a same-named model under another provider from inheriting the default flag. Context
  windows still come from declarations and runtime events, not from the catalog.
- **Context window.** Emit `ContextWindowUpdated` from `autocompact_state.effective_window`, falling
  back to `result.modelUsage[<model>].contextWindow`. This is the effective, post-headroom number,
  which is exactly what the spec's context gauge (§10.3) wants.
- **Tokens.** `TokenUsage { input, output, total }` from `result.usage`, with
  `input = input_tokens + cache_creation_input_tokens + cache_read_input_tokens`. Document the
  consequence: with `tokens.cost_estimation = true`, flat per-Mtok rates **overstate** cost, because
  cache reads bill at a fraction. For a subscription user the euro figure is notional anyway.
- **Ancillary models appear in `by_model`.** `result.modelUsage` always carries a Haiku entry alongside
  the selected model, because Claude Code runs its own summaries and titles on a small model — observed
  on every turn, including Sonnet-only ones. **Record each `modelUsage` entry under its real model id**
  and keep `Turn.model` as the user's selection. Dropping the ancillary usage would make Giskard's
  totals disagree with the provider's; folding it into the selected model would corrupt that model's
  per-Mtok rates.
- **Cost.** Do not use `result.total_cost_usd` as truth for a Pro/Max user — it is priced as if the
  request were API-billed. Prefer the `rate_limit_event` five-hour window as the honest "how much
  budget is left" signal, surfaced as a `Notice` (and, later, a header chip).

---

## 7. Auth with a Pro subscription

Spec §12.2 already says the harness owns its own credentials and Giskard inherits the environment.
Generalize the wording from "Codex" to "the active harness" and add:

- Claude Code must be logged in already — interactive `claude auth` / `/login`, or
  `claude setup-token` (which explicitly requires a Claude subscription) exported as
  `CLAUDE_CODE_OAUTH_TOKEN`.
- **Never set `ANTHROPIC_API_KEY`** in the child environment: an API key routes usage to
  pay-as-you-go API billing instead of the subscription. `system/init.apiKeySource` reports which
  path was used (`"none"` = OAuth/subscription); surface anything else as a warning so a stray key in
  the environment cannot silently start spending credits.
- If the child reports unauthenticated, fail the thread open with a message naming the fix, as spec
  §12.2 already requires for Codex.
- Credentials live in `~/.claude.json`, not in any settings file (§3.5), so the `--setting-sources`
  choice does not affect the subscription login either way. The other provider-selecting
  environment variables (`ANTHROPIC_BASE_URL`, `CLAUDE_CODE_USE_BEDROCK`, …) deserve the same treatment
  as `ANTHROPIC_API_KEY`: the child's environment should be constructed deliberately rather than
  inherited wholesale, or a variable set for some unrelated reason will silently redirect a
  subscription thread to another backend.

---

## 8. Presets, Plan mode, and settings sources

### 8.1 Permission presets

| Giskard preset | `--permission-mode` | Notes |
| --- | --- | --- |
| `ask_first` | `default` | **verified** — this is the mode the §9.2 round trip ran in: no auto-approvals, unmatched calls reach `can_use_tool`. (`manual` is accepted on the command line but `system/init` reports it back as `default`, so use `default` and avoid the discrepancy.) |
| `auto_approve` | `acceptEdits` | file edits and filesystem commands inside the workspace proceed; other escalations still ask |
| `full_access` | `bypassPermissions` | `can_use_tool` is never consulted in this mode. Refuses to start if the server process is running as root — see below |

**`full_access` depends on the identity of the server process.** Launching with
`--permission-mode bypassPermissions` exits non-zero with `--dangerously-skip-permissions cannot be
used with root/sudo privileges for security reasons`. Observed while probing from a root shell; it is a
property of the effective uid, not of Giskard.

This should not arise in the documented setup: Giskard runs as the user whose `$HOME` holds the data
directory and whose harness credentials the child inherits (spec §12.2), and that user is not root. It
is recorded because the failure is a non-obvious spawn error rather than a permission message, so the
adapter should detect the refusal and surface the cause instead of a generic spawn failure — the same
treatment any other unusable preset gets.

### 8.2 Plan mode

**Plan mode collapses the orthogonality.** In Codex, Plan/Build is collaboration mode only and is
orthogonal to the preset (spec §9.1). In Claude Code, `plan` *is* a permission mode, so Plan + preset
occupy one slot. Contract: Plan wins — a Plan-mode turn sends `--permission-mode plan` / 
`set_permission_mode plan` regardless of preset, and the preset applies again in Build. Spec §9.1
must say this explicitly for harnesses without `plan_build_modes` independence.

### 8.3 Settings sources

**Decision: children run with `--setting-sources user`.** The user's own `~/.claude/settings.json`
applies; the project and local scopes do not.

The principle is the one Giskard already applies to Codex, whose adapter reads the user's `~/.codex`
configuration for `sandbox_workspace_write.writable_roots`: the machine's owner configures their agent
where they already configure it, and Giskard does not grow a parallel setting for the same thing. It
answers where extra writable roots come from with no Giskard-side surface at all (§3.5).

It goes further than the Codex adapter, though, and the difference is the cost of the decision. Codex's
adapter takes *one field* and Giskard's preset still drives every approval; loading the user scope here
adopts their whole permission surface, including `permissions.allow`. Those rules are evaluated
**before** `can_use_tool`, so a command the user allowed for their own CLI use is pre-approved inside
Giskard too, and an `ask_first` thread will run it without asking. `ask_first` therefore means "ask
unless you have already said otherwise", not "ask always".

For a single-user tool on the user's own machine that is a coherent contract, but it has two
consequences worth carrying:

- the preset descriptions in the UI must not promise more than this;
- the hook route (§9.4) is the only mechanism that would let a user keep their personal rules *and*
  have `ask_first` be absolute, which raises its value relative to when it was postponed.

**`project` and `local` scopes stay excluded**, because those are the two a checkout can carry, and a
repository is untrusted input. Extending to them is a separate decision needing at least these answers:

- **The agent can write the file that governs it.** `.claude/settings.json` lives inside the workspace
  the agent may edit, so enabling project settings creates a path to self-granted permissions. Unknown:
  whether Claude Code re-reads settings within a running session or only at startup; whether a rule
  written during a turn takes effect in that turn, the next, or the next thread; and whether Giskard's
  presets can be made to win regardless.
- **A cloned repository can ship permissive rules**, and Claude Code's own defence — the workspace trust
  dialog — is documented as skipped in non-interactive mode, the only mode Giskard uses. Nothing in the
  flow would prompt.
- **Per-thread worktrees multiply it.** Each worktree carries its own copy, so "which settings are in
  force" becomes per-thread rather than per-project.

Until those are answered, `user` is the boundary: configuration the machine's owner wrote applies,
configuration that arrived with a checkout does not.

---

## 9. Live approvals — verified end to end

### 9.1 Three routes; stdio for the MVP, the hook deferred

Claude Code can hand a permission decision to an external party three ways. **The MVP takes the stdio
route. The hook route is deliberately postponed to a later decision and refactor (§9.4).** The MCP
route is documented here so it is not rediscovered as a new idea, and rejected.

#### Route A — `--permission-prompt-tool stdio` (**chosen for the MVP**)

`stdio` is a sentinel in an argument that otherwise names an MCP tool: it means "ask my parent process
over the pipe I am already talking on". The ask arrives as a `can_use_tool` control request and is
answered with a `control_response` — the exchange verified in §9.2. The SDK passes exactly this flag
when a `canUseTool` callback is supplied, and refuses to combine the two ("canUseTool callback cannot
be used with permissionPromptToolName"). Not answering has a defined failure mode ("tool permission
stream closed before response received"), so a dropped response fails the tool call rather than
hanging.

Chosen because it needs no extra process, it is the only route whose reply schema can express Giskard's
whole `ApprovalDecision` enum, and it is the path the official SDK itself uses.

#### Route B — an MCP permission tool (**rejected**)

`--permission-prompt-tool` normally names **an MCP tool** that Claude Code calls whenever it needs a
permission decision — the flag's own help is "MCP tool to use for permission prompts". The tool is
addressed by its fully qualified name and receives a `tool_name` + `input` + `tool_use_id` wire (field
names observed, not inferred); it must answer with a single `text` content block whose text is JSON:

```jsonc
// claude … --mcp-config approver.json --permission-prompt-tool mcp__approver__approve
// approver.json: {"mcpServers":{"approver":{"command":"node","args":["approver.js"]}}}

// the approver tool is invoked with, roughly:
{ "tool_name": "Bash", "input": { "command": "rm -rf build", "description": "Clean" } }

// and must return one text block containing:
{ "behavior": "allow", "updatedInput": { "command": "rm -rf build" } }
// or
{ "behavior": "deny", "message": "Not allowed to delete build output" }
```

Rejected for three reasons:

1. **A second process for nothing.** Giskard would ship an MCP server, spawn it per child, and then
   need its own channel from that server back to the browser — a relay whose only job is carrying a
   question the adapter could receive directly.
2. **Its reply schema is strictly weaker**: `{behavior:"allow", updatedInput?}` or
   `{behavior:"deny", message}` — no `updatedPermissions`, no `interrupt`. That deletes
   `AcceptForSession` and `Cancel` from the mapping in §9.3, which is half of Giskard's approval card.
3. Asks needing real user interaction are explicitly unsupported through it ("MCP tool requires user
   interaction; not supported via `--permission-prompt-tool`").

It is also effectively unadopted in the wild, so Giskard would be discovering its sharp edges alone:
[anthropics/claude-code#1175](https://github.com/anthropics/claude-code/issues/1175) requests a minimal
working example and still stands unanswered; the only public implementations
([CLIAI/mcp_permission_server_claude_code](https://github.com/CLIAI/mcp_permission_server_claude_code))
are self-described as possibly non-functional, and the variant inspected returns
`{"approved": bool, "reason": string}` — not the `behavior` contract the CLI actually validates, which
is likely why it does not work.

#### Route C — a `PermissionRequest` / `PreToolUse` hook (**postponed — see §9.4**)

A hook is a command Claude Code runs before a tool call; it receives the request as JSON on stdin and
writes `{"behavior":"allow"}` or `{"behavior":"deny"}` on stdout. Unlike the other two routes it is
**unconditional**: per the official permissions documentation, hooks run before every other step and a
hook's deny applies even in `bypassPermissions` mode. That property is what makes it interesting to
Giskard, and it is why the ecosystem has converged here rather than on MCP — e.g.
[claude-remote-approver](https://github.com/yuuichieguchi/claude-remote-approver) routes approvals to a
phone via a `hooks.PermissionRequest` entry, and
[claude-code-permission-policy](https://github.com/defrex/claude-code-permission-policy) runs a Haiku
policy judge the same way.

### 9.2 The verified stdio exchange

**Confirmed by round trip** against 2.1.233 (`--permission-prompt-tool stdio --setting-sources ""`,
default permission mode, prompt asking for `touch /tmp/spike-probe-file`). The ask, verbatim:

```json
{"type":"control_request","request_id":"15cdfe89-…","request":{
  "subtype":"can_use_tool","tool_name":"Bash","display_name":"Bash",
  "input":{"command":"touch /tmp/spike-probe-file","description":"Create empty probe file in /tmp"},
  "description":"Create empty probe file in /tmp",
  "permission_suggestions":[
    {"type":"addRules","rules":[{"toolName":"Bash","ruleContent":"touch /tmp/spike-probe-file"}],
     "behavior":"allow","destination":"localSettings"},
    {"type":"addDirectories","directories":["/tmp"],"destination":"session"},
    {"type":"setMode","mode":"acceptEdits","destination":"session"}],
  "blocked_path":"/tmp/spike-probe-file",
  "tool_use_id":"toolu_01X3jH3ePoR9cjeZwene5DwQ"}}
```

Answering `{"subtype":"success","request_id":…,"response":{"behavior":"deny","message":"…"}}`
produced a `tool_result` with `is_error: true`, our message as its content, and
`tool_result_meta:[{"non_execution_kind":"permission-rule"}]`. **The command did not run** — the file
was never created — and the turn then completed normally (`stop_reason: "end_turn"`,
`is_error: false`), with `post_turn_summary.status_category: "blocked"` plus a `needs_action` hint.
So a denial blocks the action without failing the turn, which is exactly Giskard's approval-card
semantics.

Two findings from the round trip:

- **Settings allow-rules pre-empt the callback.** With the surrounding environment's settings loaded,
  the same probe auto-approved the command and no ask was ever emitted; it took `--setting-sources ""`
  to see the ask at all. Under the chosen `user` scope this is an accepted limit
  of `ask_first` rather than a defect (§8.3).
- **`permission_suggestions` is typed and carries a `destination`**, which decides whether a granted
  rule is remembered for the session or written to a settings file. Giskard always uses `session`
  (§9.3).

### 9.3 Decision mapping

**This maps onto Giskard's existing approval model almost exactly:**

| `ApprovalDecision` | Claude response |
| --- | --- |
| `Accept` | `{behavior:"allow"}` — **verified** |
| `AcceptForSession` | `{behavior:"allow", updatedPermissions:[<the ask's own addRules suggestion, destination rewritten to "session">]}` — **verified**; see below |
| `Decline` | `{behavior:"deny", message:"Declined"}` — **verified**: tool blocked, turn continues and completes normally |
| `Cancel` | `{behavior:"deny", interrupt:true}` — **verified**, and genuinely distinct from `Decline` (below) |
| `AcceptWithExecPolicyAmendment` | no analogue — do not advertise it in `available` |

**`Cancel` verified.** Replying `{"behavior":"deny","message":…,"interrupt":true}` aborts the whole
turn: `subtype: "error_during_execution"`, `terminal_reason: "aborted_streaming"`, `is_error: true`,
`stop_reason: null`. The plain `Decline` reply on the same setup left the turn running to a normal
`stop_reason: "end_turn"`. So the two decisions really are different operations, as in Codex.

*Mapping consequence:* a turn Giskard itself cancelled must be persisted as
`TurnStatusKind::Interrupted`, **not** `Failed`, even though the harness reports `is_error: true` and
an error-shaped subtype. The adapter knows which it is, because it sent the `interrupt`.

**`AcceptForSession` — use `updatedPermissions` with `destination: "session"`. Verified.**

`updatedPermissions` is a typed, supported mechanism: `addRules` / `replaceRules` / `removeRules` /
`setMode` / `addDirectories` / `removeDirectories`, each with a `destination` of
`userSettings | projectSettings | localSettings | session | cliArg`, applied to the live permission
context and (for persistent destinations) written to disk.

**The recipe that works** — take the `addRules` entry out of the ask's own `permission_suggestions`,
override its `destination` to `"session"`, and send it back with the allow. Verified on both permission
dimensions: a command asked once and then ran twice more with no further ask.

| Command | Asks with the session rule | Rule as the CLI stored it |
| --- | --- | --- |
| `python3 -c "print(1)"` (no file write) | **1** (was 3) | `Bash(python3 -c "print\(1\)")` |
| `touch probe.txt` (file write) | **1** (was 3) | `Bash(touch probe.txt)` |

**Do not synthesize the rule text.** Every earlier failure in this investigation was a hand-written
rule that did not match the command's canonical form — the CLI escapes glob metacharacters and
preserves quoting (`python3 -c "print\(1\)"`), so a rule Giskard composes itself will silently fail to
match while still being reported as applied. Echo the CLI's own `ruleContent` verbatim; change only the
destination.

**Session scope is exactly Giskard's definition of session.** These rules live in the child process's
permission context and die with it, which is precisely spec §9.2.1 — "session" = harness-process
lifetime, fail-closed on respawn. No Giskard-side approval memory is needed, and none should be built:
the harness already provides the semantics the spec asks for.

**Degradation.** Some calls carry no `addRules` suggestion at all — a `Bash` command containing a shell
redirect (`echo A > a.txt`) offers only `addDirectories`, because the ask comes from the write path
rather than the command rule. There is nothing to persist for those, so `AcceptForSession` degrades to
a plain `Accept` and the next identical command asks again. The adapter must handle the empty case
rather than assume a suggestion is always present.

**Decision: the UI keeps offering the button unconditionally**, including for those calls. Consistency
is worth more than a button that appears and disappears depending on whether a command happens to
contain a redirect — a distinction no user should have to reason about. The cost is bounded: the user
occasionally gets asked again after choosing "for session".

Per the `AGENTS.md` rule that degraded-but-usable flows surface rather than fail silently, the adapter
logs when it happens (thread, tool, and the fact that no rule suggestion was offered), so a report can
be confirmed from logs instead of reproduced by guesswork.

**No fix is chosen.** If reports arrive, the investigation starts from those logs and from what the
harness offers for the affected calls; the answer is not known yet and should not be guessed here. One
option is ruled out in advance: Giskard does not maintain its own map of session approvals. Approval
state belongs to the harness process that enforces it (§9.3, above), and a second copy in Giskard would
be a parallel source of truth that can disagree with the one doing the work.

Also observed while establishing this: echoing the CLI's suggestion **unmodified** writes a persistent
rule into the user's project (`.claude/settings.local.json` gained `"Bash(echo A > a.txt)"`), because
the suggested destination is `localSettings`. Hence the invariant below.

**Invariant: rewrite the destination to `session` on every suggestion echoed back.** The other
destinations persist: `localSettings`, `projectSettings` and `userSettings` write the rule to the
corresponding settings file — observed once during this investigation, when an unmodified suggestion
added `"Bash(echo A > a.txt)"` to a project's `.claude/settings.local.json`. Giskard uses none of them,
because an approval click means "let this proceed for now" and not "edit my configuration". Suggestions
that cannot be rewritten are dropped.

The rule is worth a line of test coverage rather than vigilance, since the way to break it is the
obvious one-liner — forwarding `permission_suggestions` unchanged: approve for session, assert the
repeat call does not ask and that no settings file appeared.

*Diagnostic note:* `--debug-file` logs every applied update (`Applying permission update: Adding 1 allow
rule(s) to destination 'session': [...]`), including the rule in the CLI's canonical stored form. That is
the channel for diagnosing an `AcceptForSession` that silently fails to match.

**`ApprovalKind` mapping.** `Bash` → `CommandExecution{command,cwd}`; `Edit`/`Write`/`NotebookEdit` →
`FileChange{path,change}`; `mcp__<server>__<tool>` → `McpToolCall{server,tool_name}`; everything else
→ `Permission{detail}`. `display_name`, `description`, `blocked_path` and the suggestions fill
`ApprovalMetadata`.

**Conclusion.** Advertise `live_approvals: true` and build the approval path in the adapter's first
working milestone. Every row of the table above is verified on the wire, so the adapter is written
against a settled contract rather than a guess; and the alternative — deferring approvals — ships a
Claude harness whose only usable presets are "auto-approve" and "bypass", a downgrade against Codex in
the feature Giskard treats as central.

### 9.4 The hook route, postponed

**Status: not in the MVP.** The stdio route ships first; adopting the hook is a separate decision taken
later, against a working harness, and it is a refactor rather than an addition.

**Why it is on the table at all.** The stdio route has one structural weakness, and it is not a
protocol defect but an ordering one: `can_use_tool` is consulted *last*. Deny rules, ask rules, the
permission mode, and allow rules — including allow rules from the user's own `settings.json` — are all
evaluated first, and anything they approve never reaches the callback. That is the §8.3 hazard: an
`ask_first` thread can execute a command without asking, because the user once allowed it in their own
`~/.claude/settings.json`. Since the MVP deliberately loads that file (§8.3), this is not hypothetical —
it is the accepted cost of the settings decision. A hook is the only route that removes it without also
discarding the user's configuration: hooks run before every other step, and a hook's deny stands even in
`bypassPermissions`.

**What adopting it would change.** This is why it is a refactor and not a flag:

1. **A second inbound channel.** A hook is a separate short-lived process, not the child's pipe. It
   needs a way to reach the Giskard server (a loopback endpoint with a per-child token is the obvious
   shape) and to correlate its request with a thread and a live turn — routing the adapter currently
   gets for free from the pipe it owns.
2. **Approval identity moves.** `ApprovalId → ThreadId` routing (`registry.rs:709`) currently resolves
   through the harness that raised the ask. A hook-raised ask arrives from outside any harness, so the
   registry needs a path that does not assume a `ThreadHandle`.
3. **Hook installation is normally state on the user's machine**, not process arguments — and Giskard
   must not write settings files (the destination invariant, §9.3). A per-child `--settings` payload
   avoids that, and §3.5 verifies such a payload applies even with `--setting-sources ""`. This is the
   one mechanical unknown of the hook route that is now closed.
4. **Both channels would be live at once.** The hook covers every call; `can_use_tool` still fires for
   what the hook passes through. Giskard must not raise two approval cards for one tool call, so
   `tool_use_id` becomes the deduplication key across two independent transports.

Adopting it would also let `ask_first` become absolute *without* reverting the §8.3 decision — the user
keeps their `settings.json` and Giskard stops being pre-empted by it. That combination is the strongest
argument for eventually taking this route.

**Precedent to copy from when the time comes:**
[claude-remote-approver](https://github.com/yuuichieguchi/claude-remote-approver) (hook → ntfy → phone,
answering `{"behavior":"allow"|"deny"}` on stdout) is the same shape as hook → Giskard → browser.

**Trigger for revisiting:** the first time an `ask_first` thread executes something the user expected to
be asked about. Under §8.3 that is a foreseeable report rather than a surprise, so the trigger is less
"if" than "when someone minds".

---

## 10. Not in v1

Structured diffs; native rename/archive/delete (Giskard applies all three locally instead, §5.6);
MCP reload and OAuth; `terminate_command`; linked
sub-agent child threads; idle process reaping; `sdkMcpServers`; **hook-based approval enforcement**
— the stdio channel is the MVP's only approval path, with the hook route deferred to a later decision
and refactor (§9.4); and **honouring a repository's own `.claude/settings.json`** — the `project` and
`local` settings scopes stay excluded, gated on the security review in §8.3, while the user scope is
loaded.

---

## 11. Phasing

Each phase carries the `AGENTS.md` obligations: `cargo fmt`/`clippy -D warnings`, error-path tests,
structured logs at new boundaries, and doc sync in the same change.

### Phase 0 — protocol verification

Establish each protocol behaviour the adapter depends on by running it against the real CLI, before any
of it is assumed in code.

*Verified:* `can_use_tool` allow, deny, deny-with-`interrupt`, and session-scoped `updatedPermissions`
(§9.3); `interrupt`, `set_model`, `set_permission_mode` (§3.3); concurrent same-cwd sessions (§3.4).

*Outstanding:* `/compact` over stream input, and interrupt *mid-tool-call*.

Each run's transcript is sanitized and kept as a fixture for the mapper tests — the §9.2 ask payload is
the first one.

### Phase 1 — preparatory refactors, landable before any Claude code

Every item below can be built, reviewed and merged **without the Claude adapter existing**, each as its
own change. Two of them fix defects that exist today; the rest are structural preparation that leaves
behaviour identical while there is only one harness. All are provable with `ReplayHarness` and
`giskard-server-replay` — registering a second replay instance under a different kind gives a genuine
two-harness test with no CLI involved.

| # | Change | Justification today, without Claude | Depends on |
| --- | --- | --- | --- |
| **P1** | **Soft `Unsupported` for `set_thread_name` / `set_thread_archived` / `delete_thread`** (§5.6) | **Resolves a contradiction inside the current design.** `AgentHarness` declares these optional — its default implementations return `Unsupported` — while the server turns `Unsupported` into a user-visible HTTP 400 (`routes.rs:3784`). The trait says "may be absent", the server says "must exist". Nothing trips it today (Codex implements all three; `ReplayHarness` overrides them with `Ok`), so this is a consistency fix rather than a bug fix — but it is the contract that decides whether a harness can decline an operation at all. | — |
| **P2** | **Thread-scoped capabilities** (§5.4) | **Corrects the shape, before it has consequences.** Capabilities belong to the harness serving a thread, not to a project; today the two coincide, so there is no user-visible symptom — which is precisely why it is cheap now and expensive once a project can hold two harnesses. The capability-driven UI (spec §13.5) is the consumer. | — |
| **P3** | **`HarnessKind` newtype** replacing the bare `String` on `ProjectConfig.harness`, `config.toml`, and the factory | One place parses and validates a harness name instead of string comparisons scattered across the binary and the store. Pure typing; no behaviour change. | — |
| **P4** | **`harness_for(&ModelRef)` resolved from the aggregated provider table** (§5.1) | No config change: the answer comes from `list_providers`, which `main` already added. With one harness the aggregation is the current behaviour, so this is a refactor of `harness_knows_provider` and the discovery path from "the project's harness" to "every harness the project can use". | P6 |
| **P5** | **Dispatching `HarnessFactory`** — a table keyed by `HarnessKind` instead of `bin/giskard-server.rs:19`'s `if config.harness != "codex"` | Turns a hardcoded rejection into an extension point, and lets the replay binary register its own kind by the same mechanism the real binary uses. | P3 |
| **P6** | **A harness map inside `ProjectHarnessSlot`**, plus the harness kind on `ThreadAuthority` (§5.2) | The structural centre of the work and the riskiest thing to combine with adapter development. Landing it alone keeps behaviour identical while there is one kind installed, and makes the two-harness test possible. Note what it is *not*: a new keyed map beside the authorities, which `AGENTS.md` forbids. | P3, P5 |
| **P7** | **`ThreadFile.harness` + `harness_thread_ids`, default-on-read** (§5.3) | A forward-compatible persistence migration. Landing it early means existing installations are already writing files that carry the field before any feature reads it, so the Claude work never needs a migration step of its own. | P3 |
| **P8** | **Harness-scoped native-id lookup** (`registry.rs:1726`) | Hardening: the lookup compares an opaque native id against every thread with no notion of which harness minted it. Harmless while one harness exists, wrong the moment a Claude session UUID and a Codex rollout id share the field — and §5.8's `task:` ids widen that space further. | P6, P7 |

Suggested order: **P1, P2** first — they are self-contained, argue for themselves as design fixes, and
are worth merging whether or not the Claude harness is ever built. Then **P3 → P5 → P6**, the structural
spine. **P4, P7, P8** can land alongside at any point after their dependencies.

Being honest about what this phase is: apart from P1 and P2, these changes buy no user-visible
improvement on a Codex-only installation. Their value is that the risky structural work is separated
from the unfamiliar protocol work, so a regression during Phase 2 has an unambiguous cause. If the
Claude harness were abandoned after Phase 1, P1–P3 would still be worth keeping and P4–P8 would be
harmless but idle.

The two-harness test to add with P6 is the acceptance criterion for the whole phase: two projects, or
one project with two threads, served by two `ReplayHarness` instances registered under different kinds,
asserting that turns, approvals, interrupts and deletion each reach the right instance.

### Phase 2 — `giskard-harness-claude` MVP

New crate + README. **`claude-codes` as the protocol layer** (§3.7), child supervisor, mapper
(`assistant`/`stream_event`/`user`/`result` → items and turns), `open_thread`/`start_turn`/
`subscribe`/`interrupt`/`shutdown`, **`can_use_tool` ↔ `ApprovalRequested` with the §9 decision
mapping**, user attachments as inline content blocks (§3.6), token usage, `ContextWindowUpdated`,
static `list_models`, capability set from §4. Sub-agent child threads (§5.8) can follow in Phase 3
— the parent's `Agent` item is a normal tool call without them, so the MVP degrades to a readable
transcript rather than a broken one. Mapper
unit tests off Phase-0 fixtures, including a denial that must not be reported as an executed-and-failed
tool call.

### Phase 3 — the rest of the control channel

`set_model`, `set_permission_mode` and
`apply_flag_settings{effortLevel}` — per-turn model, mode and reasoning effort with no respawn, which
together make `TurnOverrides` fully supported — elicitation / `request_user_dialog` →
`ServerRequestReceived`,
`rate_limit_event` → `Notice`, `/compact`, `AcceptForSession` via session-destination
`updatedPermissions`.

### Phase 4 — cross-harness UX

Model-picker filtering by provider→harness, the switch confirmation
and `Notice` of §5.5, capability-driven UI checks, screenshot regeneration if the picker changes
(`tests/e2e/screenshots.sh`).

### Phase 5 — polish

Idle reaping (make `idle_shutdown_secs` real), synthesized `FileChange`/
`DiffUpdated`, version-drift warning surfaced in the UI.

### Later, as its own decision — the hook route (§9.4)

Not scheduled here on purpose: it is a
refactor of how an approval reaches the server (second inbound channel, approval routing that does not
assume a `ThreadHandle`, ephemeral hook installation, cross-transport deduplication by `tool_use_id`),
and it should be decided against a working harness rather than designed in advance.

### Documentation to update

`specs/giskard-specification.md` (§4.1/4.2 capability wording, new §4.8 Claude
mapping, §4.7 → per-harness process lifecycle, §6.4, §8.2/8.3, §9.1, §12.2, §13.5); `README.md`
(one-process-per-project claim at line 60, crate list, setup); `config.example.toml` (only if a
`[providers.anthropic]` block is worth showing — §5.1 makes one optional); `docs/subagents.md` (state that linked children
are Codex-only); `AGENTS.md` (9 crates); new `crates/giskard-harness-claude/README.md` mirroring the
Codex adapter's identifier/lifecycle contract.

---

## 12. Risks

| Risk | Mitigation |
| --- | --- |
| Protocol drift as Claude Code ships | Largely answered by `claude-codes` (§3.7), whose version tracks the CLI and whose enums tolerate unknown values. The residual risk is the crate lagging a CLI release — the same relationship Giskard already has with `codex-codes`. Still log `claude_code_version` and warn on drift |
| User settings allow-rules pre-empt `ask_first` — **observed**, and now **accepted** by the §8.3 decision | Not mitigated by design: the UI wording must match what the preset actually promises, and the hook route (§9.4) is the only fix that keeps the user's settings *and* an absolute `ask_first` |
| One process per loaded thread, **measured at 440–530 MB RSS** | MVP accepts the cost and logs the live-child count so growth is visible; reaping in Phase 5. At the spec's ~10-thread scale this is gigabytes, so it is a capacity question, not a detail |
| Cross-harness model switch loses agent context | Explicit confirm + `Notice` + remembered native ids (§5.5) |
| Cost/quota semantics differ under a subscription | Treat euro cost as notional; surface `rate_limit_event` (§6) |
| Registry re-keying touches approval/interrupt/delete routing | Phase 1 lands it separately from any adapter work and proves it with two replay harnesses, so a regression there cannot be confused with a protocol bug |
| `full_access` fails to start when the server process runs as root (§8.1) | Outside the documented setup, but the raw failure is an opaque spawn error: detect the refusal and surface its cause |
| A checkout carries permission rules Giskard would otherwise honour | `project` and `local` scopes stay excluded (§8.3); only the machine owner's user-scope file is loaded |

---

## 13. Open questions

1. Does `ProjectConfig.harness` still earn its place? Project creation never asked for a harness — it
   takes a `default_model` and the field is hardcoded to `"codex"` (`store.rs:578`). Once the harness
   is derived from the provider table (§5.1), the field is redundant for routing. It does have one
   concrete job left: breaking a tie when two harnesses report the same provider id. Keep it as a
   derived-and-persisted default that also serves as that tie-breaker, or drop it and find another
   rule?

