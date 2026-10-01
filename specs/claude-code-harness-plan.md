# Claude Code Harness Support — Design and Implementation Plan

This note plans a second agent harness for Giskard: Anthropic's **Claude Code CLI**, driven through
its `--print --input-format stream-json --output-format stream-json` protocol, authenticated by the
user's **Claude Pro/Max subscription** (not an API key).

It is a planning note, not an authoritative product spec. Once the direction is agreed, the final
contract folds into `specs/giskard-specification.md` (§4, §6.4, §8, §9, §12.2, §13.5) and
`crates/giskard-harness-claude/README.md`.

Protocol facts below are established against **Claude Code 2.1.285** — by driving a real CLI over a
stdio pipe, by reading the shipped binary's own schemas, and against Anthropic's published
documentation. Anything that has not been confirmed that way is marked **[unverified]**.

**Version matters here.** The surface moved substantially over the 2.1.2xx series — permission modes,
the control channel, resume scope — so treat these facts as carrying a version stamp, read
`system/init.capabilities` to feature-detect rather than comparing version strings, and re-check
before shipping against a much later CLI (§12).

Section references written as "spec §X" point at `specs/giskard-specification.md`; bare "§X" refers to
this document.

**Multi-harness is already built.** `docs/multi-harness-design.md` is the authoritative design for
running several harnesses, and its Stages 0–2 are on `main`: named `[harnesses.<name>]` declarations,
a kind-dispatching factory, a durable `ThreadFile.harness`, per-declaration instances and catalogs,
harness-scoped MCP routes, and capabilities on the wire. This note proposes none of that. What it
holds is what only it has: the protocol as it actually behaves, and the decisions an adapter author
has to make.

Milestone 1 is implemented; see `claude-code-harness-plan/milestone-1-plan.md`.

---

## 1. Decisions already taken

| Decision | Choice |
| --- | --- |
| Harness binding granularity | **Per thread inside a project.** One project may hold Codex threads and Claude threads side by side. |
| What selects the harness | **A declaration the user names.** A thread stores `harness` and is bound to it for life; the picker offers `(harness, provider, model)` and derives the harness from the selection. |
| Child-process model | **One persistent `claude` child process per loaded thread**, alive across turns, spawned in `open_thread`, stopped by `delete_thread` / `set_thread_archived(true)` / `shutdown`. **No idle reaping in the MVP** (§5.2); milestone 6 reaps an idle child and respawns it on the thread's next message. |
| Who owns the permission mode | **Giskard.** Every child runs with `--disallowedTools EnterPlanMode ExitPlanMode`, and the adapter sets the mode at spawn and at the start of every turn (§8.2). Without the flag the model can switch modes mid-turn with no ask. |
| Model catalog | **Answered by the harness, not discovered over HTTP.** The `initialize` response carries the session's `models` array and `list_models` refreshes it, so Giskard holds no Anthropic key and the `models.rs` discovery extensions leave the critical path (§3.3, §6). |
| Structured diffs | **`structured_diffs: false` in v1.** Synthesize `FileChange`/`DiffUpdated` from `Edit`/`Write` tool calls + git in a later phase. |
| `AcceptForSession` when the harness offers no rule to persist | **Keep the button visible anyway** (§9.3). It behaves as a one-off `Accept`; log the degradation and revisit only if users report it. |
| Settings sources for child processes | **`--setting-sources user`** (§8.3): the user's own `~/.claude/settings.json` applies, so extra writable roots and personal rules are configured where the user already keeps them. Project and local scopes stay excluded. The accepted cost is that a user allow-rule can pre-approve a call `ask_first` would otherwise have asked about. |
| Live approvals | **Supported (§9).** MVP uses the `--permission-prompt-tool stdio` channel, in milestone 3 (§11). The hook route is postponed to a later decision and refactor (§9.4); the MCP-tool route is rejected (§9.1). |

---

## 2. The two harnesses are shaped differently

| | Codex (today) | Claude Code (planned) |
| --- | --- | --- |
| Transport | stdio newline-delimited **JSON-RPC** | stdio newline-delimited **JSON objects** (two overlaid channels: transcript messages, and `control_request`/`control_response`) |
| Protocol crate | `codex-codes` `=0.155.1` (typed, versioned) | **`claude-codes` 2.1.286** — same author and repository (`meawoppl/rust-code-agent-sdks`), same feature shape, and a version that really does track the CLI: 2.1.286 was published while the installed CLI was 2.1.285 (§3.7) |
| Processes | **1 `codex app-server` per project**, multiplexing every thread | **1 `claude` per session**; a session ≈ one Giskard thread |
| Native thread id | Codex-minted rollout id | **client-minted UUID** via `--session-id`; resumed with `--resume=<uuid>` |
| Concurrency | one worker task fans out to N threads | N independent children, each single-threaded through its own turn. Two sessions in the same cwd run concurrent tool-using turns, each with its own `<uuid>.jsonl` under one cwd-encoded directory, with no locking or contention (§3.4) |
| Turn ids | native `turnId` | **none** — Giskard mints every `TurnId`. A turn is "user message → `result`" **only when nothing was delegated**: a backgrounded delegation emits **two** `result` messages with the sub-agent's work between them, while a foreground one emits one. Completion therefore gates on the agent task's terminal `task_updated`, not on `result` (§5.3) |
| Model catalog | `model/list` RPC | the `initialize` control response already carries the session's `models` array, and `list_models` refreshes it — answered by the child from its own authentication, so Giskard needs no key and makes no HTTP request (§3.3, §6) |
| Session storage | Codex thread store | `~/.claude/projects/<project>/<uuid>.jsonl`, where `<project>` is **derived from cwd** (and can be pinned with `CLAUDE_CODE_PROJECT_DIR_NAME`, §5.2). Storage is cwd-derived; **resume is not cwd-scoped** — see below |

Two consequences follow:

1. **`AgentHarness` does not need to change**, and this is now settled rather than argued: the trait's
   own documentation says so, and `docs/multi-harness-design.md`'s audit reaches the same conclusion
   ("Nothing in the trait needs to change for this design"). The trait states that one value implements
   it per working context, that "how many operating-system processes stand behind an instance is the
   adapter's business", and that "an adapter for a CLI that runs one process per primary thread spawns
   in `open_thread` and stops in `delete_thread`, `set_thread_archived`, `shutdown`, or on its own idle
   policy. Both satisfy this trait unchanged." A `ClaudeHarness` is a façade owning a
   `HashMap<ThreadId, ChildSession>`, and the design's instance model spells out what that means for
   `create`, `subscribe`, stream end, `discoveries`, idle policy and `shutdown`.
2. **Resume is durable and not cwd-scoped.** A fresh process launched with `--resume=<uuid>`
   recovers the earlier conversation and keeps the same session id rather than forking. The
   transcript lives under a cwd-derived directory whose encoding is lossy (§3.7), but that does not
   constrain where a thread may respawn: *"You can run `claude --resume <session-id>` from any
   directory: Claude Code looks for the ID in the current project directory and its git worktrees
   first, then in every other project on this machine."*

   The adapter should still derive cwd from the thread (`ThreadFile.git_workspace`) rather than the
   project, to keep a thread's transcripts in one place and avoid the cross-project fallback, which
   *"resolves the ID only when exactly one other project holds a transcript with messages for it"*.
   That is a tidiness rule, not a correctness constraint.

---

## 3. Protocol facts (Claude Code 2.1.285)

### 3.1 Invocation

```
claude -p --input-format stream-json --output-format stream-json --verbose \
       --session-id <uuid> --model <id> --effort <level> \
       --permission-mode <mode> --add-dir <root>... \
       --permission-prompt-tool stdio            # routes approvals to Giskard (§9)
       --setting-sources user                    # §8.3
       --disallowedTools EnterPlanMode ExitPlanMode   # Giskard owns the mode (§8.2)
       [--resume=<uuid>] [--forward-subagent-text] [--include-partial-messages] [--replay-user-messages]
```

Stdin stays open; each user turn is one JSON line
(`{"type":"user","message":{"role":"user","content":[{"type":"text","text":"…"}]}}`). The process
keeps serving turns until stdin closes.

### 3.2 Output messages observed

| Message | Use in Giskard |
| --- | --- |
| `system/init` | effective `session_id`, `model`, **`permissionMode`**, `tools`, `mcp_servers`, `slash_commands`, `apiKeySource`, `claude_code_version`, and a **`capabilities`** array naming the protocol behaviours this build implements — check it to feature-detect instead of comparing versions. Authoritative for applied *session settings*; for what actually ran, `result.modelUsage` is the truth, since a host-managed provider can serve a different model than `init` reports. **Re-emitted** on three known triggers — after `set_model`, after `/compact`, and when a backgrounded sub-agent's `task_notification` starts the parent's continuation, immediately before the second `result` (§5.3). Treat it as a repeatable announcement rather than a one-shot handshake; three independent triggers suggest there are more. |
| `stream_event` | raw Anthropic streaming events (`content_block_delta`, …) → `ItemDelta` |
| `assistant` | complete message with `text` / `thinking` / `tool_use` blocks → `AgentMessage` / `Reasoning` / `ToolCall` items |
| `user` (tool_result) | tool output + `tool_use_result` (stdout/stderr/interrupted) → `ItemCompleted` |
| `result` | terminal per turn: `usage`, `total_cost_usd`, `modelUsage[model].contextWindow`, `stop_reason`, `is_error`, `permission_denials`, `terminal_reason` → `TurnCompleted` |
| `autocompact_state` | **A top-level frame type, not a `system` subtype**: `{type:"autocompact_state", value:{enabled, effective_window, threshold, enforced, source}}`, beside a similar top-level `active_goal`. `claude-codes` has no variant for either, so both are among the frames the mapper must read raw (§3.7). `effective_window` / `threshold` → `TurnUsageUpdated.context_window` on the next turn (the *effective* window, exactly the Codex analogue: 947 000 for a 1 M Sonnet). There is no `ContextWindowUpdated` event; `AgentEvent::TurnUsageUpdated` is the only channel that carries a window. Emitted at session start only in some environments (a scrubbed one emitted none), so it is an optional refinement of the `modelUsage` fallback |
| `rate_limit_event` | `rateLimitType: "five_hour"`, `resetsAt`, `overageStatus` → **subscription-plan headroom**; surface as `Notice` |
| `system/status`, `system/task_summary`, `system/post_turn_summary` | activity/labels; `post_turn_summary` carries `status_category` (`review_ready`, `blocked`, …), `status_detail` and `needs_action` |
| `system/thinking_tokens` | running reasoning-token estimate during a turn |
| `thinking` content blocks | carry an opaque `signature`; map to `Reasoning` items and never re-send the text as input |
| `tool_result_meta` | `non_execution_kind` (e.g. `"permission-rule"`) distinguishes "tool ran and failed" from "tool never ran" |
| `system/permission_denied` | a denial with `decision_reason_type` (`rule`/`mode`/`classifier`/…) → `Notice` |
| `system/api_retry` | `attempt`, `max_retries`, `retry_delay_ms`, `error_status`, `error` category. The natural source for retry observability and for a `Notice` when a thread stalls on retries |
| `system/session_title_changed` | emitted after a `rename_session` control request (§3.3) |
| `system/compact_boundary` | `compact_metadata { trigger, pre_tokens, post_tokens }`; `trigger` separates a manual `/compact` from automatic compaction |
| `system/status` | `status: "compacting"`, then `status: null` with `compact_result`; also carries activity labels, and **`permissionMode` whenever the mode changes** — key the adapter's mode tracking off this frame, and **warn if it reports a mode the adapter did not set**. With `EnterPlanMode` / `ExitPlanMode` disallowed (§8.2) that should never happen, so it is the regression signal for the flag silently ceasing to work |
| `system/commands_changed` | slash-command inventory (large; elide from logs) |

### 3.3 The control channel

A second channel overlaid on the same stdio pipe, carrying
`{"type":"control_request","request_id":…,"request":{"subtype":…}}` in both directions. Everything
below is established against a real CLI; response shapes are as observed.

#### Client → CLI, used by this adapter

| Subtype | Request | Response | Notes |
| --- | --- | --- | --- |
| `initialize` | accepts `hooks`, `sdkMcpServers`, `systemPrompt`, `appendSystemPrompt`, `planModeInstructions`, `toolAliases`, `supportedDialogKinds` | `{commands, agents, models, output_style, account:{subscriptionType, apiProvider}, pid, current_permission_mode, …}` | The handshake, and the **model catalog arrives here** — see §6. Answers with no credentials and no network, which is what makes §6's probe viable. `current_permission_mode` uses the CLI's own mode names: `--permission-mode manual` is reported as **`default`** here and in `system/init.permissionMode` / `system/status.permissionMode`, so mode tracking compares against the CLI's name, never the flag's. **`account` is informational, not a credential signal**: with zero credentials it still reports `{subscriptionType: "Claude API", apiProvider: "firstParty"}`, so it is a default until an authenticated request happens (§7) |
| `interrupt` | — | `{"still_queued":[]}` | `AgentHarness::interrupt`. Works mid-tool-call: it kills the running tool and ends the turn with `terminal_reason: "aborted_tools"`, distinct from `"aborted_streaming"` when it lands during generation |
| `set_model` | `{model:"<id>"}` | `null` | Mid-session, no respawn, no session change. No frame follows the request itself; the next turn's re-emitted `system/init` and its `result.modelUsage` carry the new model. An unknown model answers `{"subtype":"error","error":"Model '…' not found","error_code":"catalog_unknown"}` and leaves the model unchanged. Also accepted mid-turn, and the rest of that turn then runs on the new model, so send it only while idle |
| `set_permission_mode` | `{mode:"<mode>"}` | echoes `{"mode":"plan"}` | Per-turn Plan/Build (§8.2). Takes the CLI's names (`default`, not `manual`; `manual` is tolerated and echoed as `default`); a change is followed by a `system/status {permissionMode}` frame; an unknown mode answers an error with `error_code: "invalid_mode"`; `bypassPermissions` answers `error_code: "bypass_not_launched"` unless the child was launched in that mode (§8.1) |
| `apply_flag_settings` | `{settings:{effortLevel, ultracode, model, fastMode, advisorModel, viewMode}}` | success | The general session-settings channel. **Invalid values fail silently** — `effortLevel: "banana"` is answered `success`, leaves the previous value, and produces no error the client can see. The read-back is `get_settings.applied.effort`, not `effective`: a valid `max` was applied yet cleared `effective` to `{}` |
| `get_settings` | — | `{applied:{model, effort, advisor, ultracode, ultracodeRequested, ultracodeAvailable}, effective, sources}` | The read-back for `apply_flag_settings`, and **the only way to confirm an effort change landed**: `applied.model` is the resolved id, `applied.effort` the model-effective level (`null` on a model without effort, whatever the flag setting) |
| `list_models` | — | `{models:[{value, resolvedModel, displayName, description, supportsEffort, supportedEffortLevels, supportsAdaptiveThinking, supportsAutoMode, supportsFastMode?}]}` | Refreshes the catalog `initialize` already delivered (§6) |
| `mcp_status` | — | `{mcpServers:[{name, status, error, config, scope, source}]}` | Inventory with failure detail |
| `get_context_usage` | — | `{categories:[{name, tokens, kind}], totalTokens, maxTokens, rawMaxTokens, autocompactSource, percentage}` | A third context-window source beside `autocompact_state` and `modelUsage` |
| `rename_session` | `{title, source:"remote"\|"host", session_id?}` | `null`, plus a `system/session_title_changed` frame | Backs `set_thread_name` (§5.2). `source: "host"` is documented as a rename made in the hosting application and counted as a user rename — Giskard's exact case |
| `control_cancel_request` | — | — | Cancels an in-flight control request |

#### Client → CLI, available but not used in v1

Each works from a stdio host; the `false` capability rows in §4 that correspond to them are scope
decisions rather than missing protocol.

| Subtype | Request / response | Why it is not in v1 |
| --- | --- | --- |
| `stop_task` | `{task_id}` → `{}` (`null` on 2.1.285; `{}` verified on 2.1.287) | Kills a live task. `task_started` classifies them: `local_bash` for shell commands, `local_agent` for sub-agents (§5.3) — the distinction that scopes the turn-completion rule. **The reply is not a confirmation**: it is the same whether or not anything was stopped, and the real signal is a `task_updated` frame with `patch.status: "killed"`; a pending `can_use_tool` of the killed task is withdrawn first with `control_cancel_request`, and a task already `completed` still gets a `killed` update. Milestone 5 wires it as a sub-agent thread's `interrupt`; wiring it for `local_bash` tasks is what `terminate_command` needs |
| `background_tasks` | `{tool_use_id?}` → `null` | Backgrounds in-flight foreground tasks; without `tool_use_id`, all of them |
| `mcp_reconnect` / `mcp_toggle` / `mcp_set_servers` | `{serverName}` / `{serverName, enabled}` | The MVP configures no MCP servers |
| `get_workspace_diff` | `{diff:{stats, perFileStats, hunks}}`, `@internal` | Not the `structured_diffs` capability — see §6.1 |
| `list_permission_rules` | `{state:{rules, workspaceDirectories, originalCwd, managedOnly}}` | Candidate for warning the user which of their own rules pre-empt `ask_first` (§8.3) |
| `update_settings` | a write path into settings | **Deliberately unused**: §9.3's destination invariant says an approval click must not edit the user's configuration, and that applies to this mechanism by name |

The enumeration also carries roughly a dozen more subtypes this adapter has no use for —
`add_directory`, `set_cwd`, `set_max_thinking_tokens`, `export_conversation`, `fork_conversation`,
`rewind_conversation`, `rewind_files`, `generate_session_title`, `get_plan`, `get_hooks_listing`,
`reload_plugins` / `reload_skills` / `reload_output_styles`, and the Remote Control and OAuth families.

#### CLI → client

- `can_use_tool` — the permission ask (§9). Its keys are `agent_id`, `blocked_path`, `description`,
  `display_name`, `input`, `permission_suggestions`, `subtype`, `tool_name`, `tool_use_id`. **There is
  no `parent_tool_use_id`**, which decides how a sub-agent's approval is routed (§5.3, §9.3).
- `request_user_dialog` / `elicitation` — MCP elicitation and host dialogs → Giskard's existing
  `ServerRequestReceived` / `respond_server_request` path.
- `rename_session` — the CLI can also ask the *host* to rename, when the host registers the callback.
- `control_cancel_request {request_id}` — the CLI withdraws a pending `can_use_tool` when an
  `interrupt` lands while it waits; the interrupt's own response, the user frames and a `result`
  (`aborted_tools`, the tool in `permission_denials`) follow. A control response written after it is
  ignored, and the next turn runs normally.

#### `claude-codes` models almost none of this

The crate types `CanUseTool`, `HookCallback`, `McpMessage`, `Initialize` and `Interrupt` and nothing
else, so every other row above is hand-built JSON through `ClaudeInput::Raw` until it is upstreamed
(§3.7.1).

### 3.4 Simultaneous sessions in one working directory

**Supported.** Two children launched in the same cwd at the same time, each with its own
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
  project's `.claude/settings.json` instead. A user-scope settings file granting an
  outside directory makes a `Write` beyond the workspace proceed with no ask under
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
| `cleanupPeriodDays` | how long session transcripts survive (default 30 days). `--resume` fails once one expires, which §5.2 handles by respawning under the same id rather than erroring |
| `disableAllHooks`, `allowManagedHooksOnly` | constrain the hook route if it is ever adopted (§9.4) |
| `allowedMcpServers`, `deniedMcpServers`, `disabledMcpjsonServers` | MCP surface |

**`--settings` and `--setting-sources` are independent, and this is load-bearing.**
`--setting-sources` selects which settings *files* are consulted; `--settings` supplies an explicit
payload as a file path or inline JSON. An inline `--settings` payload applies even with
`--setting-sources ""`, so it is not merely an override of whichever files happened to load: in
`acceptEdits` mode a `Write` outside the workspace prompts, and adding
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
them **inline in the user message**, as Anthropic content blocks alongside the text block. Established
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

**Three implementation details from the Messages API contract:**

1. **The document block goes *before* the text block**, not merely alongside it. The documented
   examples place it first, and the ordering is stated rather than incidental.
2. **The base64 string must contain no newlines.** This is a live hazard for a Rust adapter: MIME-style
   encoders wrap at 76 characters by default, and `UserAttachment.data_base64` arrives from elsewhere
   in Giskard, so the adapter must not assume it is unwrapped. Strip line breaks before sending.
3. **The documented route for plain text is the Files API**, uploading with MIME type `text/plain` and
   referencing by `file_id`. The inline `source.type: "text"` form used above is not documented — it
   works, and the base64-vs-text distinction is real, but treat it as **verified-but-unpublished**,
   like `destination: "session"` (§9.3), rather than as a contract.

#### Three ceilings, and Giskard's is the loosest

**The adapter has to enforce the lower two itself**, because nothing upstream of it will.

| Ceiling | Value | Whose |
| --- | --- | --- |
| Attachments per message | 8 | Giskard (`routes.rs:49`) |
| Bytes per attachment | 25 MiB, on **decoded** bytes (`routes.rs:1218`) | Giskard (`routes.rs:50`) |
| Bytes per message, all attachments | **25 MiB total**, not 8 × 25 (`routes.rs:51`) | Giskard |
| HTTP request body | 40 MiB | Giskard (`routes.rs:52`) |
| **One stdin line** | **10 MiB** (`10485760` in the binary) | **Claude Code** |
| Whole API request | 32 MB, and 600 pages (100 under a 1M context window) | Anthropic |

Giskard admits well over what either downstream ceiling allows, so a message it accepts can still be
unsendable. Two things make the gap wider than the numbers suggest:

- **Base64 inflates by ~4/3.** The stream-json line carries base64, while Giskard's 25 MiB is measured
  on decoded bytes. A 10 MiB line therefore holds only about **7.5 MiB of raw attachment bytes** — so
  the effective limit at send time is roughly 7.5 MiB, not 25 MiB, and it is the CLI's line cap that
  bites first, before the API's 32 MB.
- **Everything shares the line.** The prompt text and every attachment go in one JSON object, so the
  budget is per message, not per file.

So the adapter checks the encoded payload against ~10 MiB before writing it and **rejects with a clear
message rather than truncating** — a truncated base64 blob would fail remotely as a corrupt document
rather than as a size error, which is much harder to diagnose.

Two further limits, neither of them about size:

- **Inline attachments consume context**, so even a legal attachment can crowd out the conversation.
- **Binary formats are unsupported**: *"Binary formats such as .xlsx or .docx are not supported in
  document blocks and must be converted to text or PDF first."* Declining them in v1 is therefore
  correct, and the message can say precisely why. The Codex-shaped fallback — write the file into the
  workspace and name the path — remains available later, at the cost of a writable location and
  cleanup.

Giskard's existing rule that raw attachment bytes stay out of persisted history and the in-memory
history cache applies unchanged: `UserInput`'s serializer already drops `data_base64`.

### 3.7 The `claude-codes` crate

**Giskard does not need to own the wire types.** `claude-codes` (2.1.286 at the time of writing) is
published by the author of `codex-codes`, from the same
repository (`meawoppl/rust-code-agent-sdks`), under Apache-2.0 — already on `deny.toml`'s allow list —
with MSRV 1.85 against Giskard's 1.89, the same `async-client` feature shape, and a version number
that tracks the CLI release it models.

**What it covers**, from its source at 2.1.286:

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
  The collision does not affect resume (§2), but it is real, and there is a documented fix:
  `CLAUDE_CODE_PROJECT_DIR_NAME` pins the directory name instead of deriving it from cwd, for exactly
  the embedding-host case Giskard is.

**Forward compatibility is partial, and the adapter must not rely on it at the frame boundary.**
String enums carry `Unknown(String)` variants that round-trip verbatim, `ContentBlock` has
`Unknown(Value)`, and `SystemMessage` keeps its whole payload in a flattened `data` map, so an unknown
`system` subtype still parses. But three things fail outright rather than degrading: an unknown
top-level `type` (`ClaudeOutput` has no fallback variant), an unknown inbound `control_request`
subtype (`ControlRequestPayload` types only `can_use_tool`, `hook_callback`, `mcp_message`,
`initialize` and `interrupt`, so a `request_user_dialog` or `rename_session` ask fails the whole
line), and a missing required field (`AssistantMessageContent.id` / `.model`,
`ThinkingBlock.signature`, `ResultMessage.total_cost_usd`). `stream_event.event` is an untyped
`Value`, and `post_turn_summary` / `autocompact_state` have no typed view. The mapper therefore
reads every line as raw JSON first, dispatches on `type` and `subtype` itself, and converts into the
crate's structs per frame with `serde_json::from_value`; a frame the crate cannot type is logged
with its `type` and `subtype` and skipped, never allowed to fail the stream (§12, milestone 1).

### 3.7.1 Gaps in `claude-codes` — candidates to upstream

Audited by reading the crate source at 2.1.286. `ControlRequestPayload` has exactly five variants —
`CanUseTool`, `HookCallback`, `McpMessage`, `Initialize`, `Interrupt` — and none of
`ApplyFlagSettings`, `SetModel`, `SetPermissionMode`, `GetSettings` or a `Document` content block
appears anywhere. **Nothing here blocks the adapter**: enums carry
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
| 8 | CLI builder flags | `--forward-subagent-text`, `--effort` | The first is **mandatory** for sub-agent child threads (§5.3); the second sets initial effort | present in `claude --help`, absent from `cli.rs` |

Item 5 is the one worth doing carefully: the base64-versus-text distinction is invisible until it
fails remotely, so a typed `DocumentSource` that makes the wrong pairing unrepresentable is worth more
than the block itself. The inline text form is also verified-but-unpublished (§3.6), so a typed API is
doing the work documentation is not.

**A ninth gap, larger than the other eight: the rest of the control channel.** The crate types five
control payloads; §3.3 documents roughly twenty more, nine of them with response shapes confirmed
against a running CLI. Since `list_models` is what makes §6's catalog work and `stop_task` is what
`terminate_command` would need, these are the variants an adapter otherwise hand-rolls through
`ClaudeInput::Raw`, and they are the most valuable thing to upstream after item 1.

**Already covered**, and worth not duplicating: `parent_tool_use_id` (the routing key §5.3 depends on),
the whole `task_started` / `task_progress` / `task_updated` / `task_notification` family,
`non_execution_kind`, `blocked_path`, `permission_suggestions`, `RateLimitEvent`, `modelUsage`,
`thinking_tokens`, and the transcript-location rule.


---

## 4. Capability matrix

| Capability | Claude | Basis |
| --- | --- | --- |
| `live_approvals` | **true** | `can_use_tool` control request; response `{behavior:"allow",updatedInput?,updatedPermissions?}` \| `{behavior:"deny",message?,interrupt?}`. Round trip and blocked execution confirmed — §9. |
| `plan_build_modes` | **true** | `--permission-mode plan` at spawn + `set_permission_mode` per turn, which echoes the applied mode. Requires `--disallowedTools EnterPlanMode ExitPlanMode`, or the model can change the mode itself without the adapter being consulted. Semantics differ from Codex — see §8.2 |
| `per_turn_model` | **true** | `set_model` mid-session, no respawn (§3.3) |
| `reasoning_effort` | **true, and dynamic** | `--effort low\|medium\|high\|xhigh\|max` at spawn, then `apply_flag_settings{effortLevel}` mid-session — verified to change `low` → `high` on a live child, and confirmable through `get_settings.applied.effort` (§3.3). An unknown value is accepted and ignored silently, so the adapter validates against the catalog and may read back. `Effort` is already an open string newtype, so the differing value set costs nothing |
| `structured_diffs` | **false**, and not merely "v1" | No per-item diff feed exists. `get_workspace_diff` (§3.3) is not this capability: the trait defines `structured_diffs` as a per-file diff *stream* feeding `DiffUpdated { thread, turn, diff }` and persisting as `Turn.diffs` — diffs attributed to the turn that produced them — while `get_workspace_diff` is a workspace snapshot against a resolved base ref, with no turn attribution and fixed caps. Giskard computes that class of diff itself via `giskard-git-parser`. See §6.1 |
| `resumable_threads` | **true** | `--resume=<uuid>` recovers the conversation and keeps the same session id (no fork). Not cwd-scoped — a session id resolves from any directory (§2). When the transcript has expired, resume fails and the thread reopens writable under the same id with a fresh session (§5.2), so this never becomes `false` for an old thread |
| `model_listing` | **true** | The `initialize` response carries the session's `models` array and `list_models` refreshes it (§3.3) — answered by the child from its own authentication, so Giskard holds no key. The catalog carries no context window, which comes from elsewhere (§6) |
| `provider_listing` | **true** | one provider, `anthropic`. It no longer carries a key location, because Giskard makes no discovery request of its own (§6, §7.1). Not an ownership signal either — the thread's `harness` field says which instance it belongs to |
| `token_usage` | **true** | `result.usage` on every turn, plus per-model totals in `result.modelUsage` — §6 |
| `mcp_status` | **true (read-only)** | `system/init.mcp_servers` carries the inventory, and the `mcp_status` control request returns it with failure detail (§3.3). MCP routes are already harness-scoped (`/api/projects/{id}/harnesses/{name}/mcp`), so this needs no special casing |
| `mcp_reload` | **false (v1)** — scope, not absence | `mcp_reconnect`, `mcp_toggle` and `mcp_set_servers` all work from a stdio host (§3.3). False only because the MVP configures no MCP servers; revisiting it needs no new protocol work |
| `mcp_oauth_login` | **false** | interactive only |
| `turn_steering` | **false** | The trait means "additional text input can be sent to an acknowledged *active* turn", and the protocol offers no such thing. The documented model is queue-and-interrupt: streaming input gives *"queued messages: send multiple messages that process sequentially, with ability to interrupt"*, so changing direction mid-flight is an `interrupt` followed by a fresh turn, not a message into a live one. Confirmed: a turn whose Bash call was still executing ignored a `CHANGE OF PLAN` message sent 4 s in and completed on its original instruction. **Two details the adapter needs**: the text is not lost — it joins the conversation and the next turn quotes it verbatim — and it does **not** start a turn of its own, verified by waiting 90 s after the turn ended with no further input and seeing no second `result`. So `steer_turn` would be silently ineffective, and `start_turn` remains the only thing that drives a turn |
| `context_compaction` | **true** | `/compact` as a user message over stream-json: `system/status` → `system/compact_boundary` → re-emitted `system/init`, with the conversation surviving and a degenerate `result` (empty text, `stop_reason: null`) that must not be persisted as an assistant turn. `autocompact_state` feeds the gauge when emitted, with `result.modelUsage[].contextWindow` as the fallback (§6) |
| Native rename | **supported** | `set_thread_name` forwards to `rename_session` with `source: "host"` (§3.3), or returns `Ok` when no child is live |
| Native archive / delete | **supported** | Not because the CLI has archive or delete, but because these are where a per-thread adapter stops its process: `delete_thread` and `set_thread_archived(true)` stop the child and return `Ok` (§5.2). Leaving them at the trait default would keep a 450 MB process alive for a thread the user has deleted |
| `terminate_command` | **unsupported (v1)** — scope, not absence | `stop_task` kills a live `task_type: "local_bash"` task from a stdio host (§3.3). Supporting it means tracking `task_started` → `task_id` per item and keying completion off `task_updated` rather than the control reply. A candidate for after milestone 4 (§11) |
| Linked sub-agent threads | **supported, as local child threads** | The child is not a resumable session, but its whole transcript is forwarded and is materialized as a read-only Giskard thread keyed by the Task call's `tool_use_id`. **Requires implementing `claim_native_thread`** — that is the only path by which such a thread is created, and the trait default fails every delegation into an unbounded retry (§5.3) |

*Every flag above reaches the browser.* Stage 0 serializes `HarnessCapabilities` as
`HarnessCapabilitiesInfo` per harness group on the models response, and the UI gates its controls on it
(spec §13.5). A capability this adapter reports `false` is a control the user does not see, rather than
one that fails when pressed.

---

## 5. Where this adapter plugs in

**Multi-harness landed while this plan was being written.** `docs/multi-harness-design.md` is the
authoritative design and its Stages 0–2 are implemented on `main`. Everything this section used to
propose — per-kind slots on the project authority, a durable harness field on the thread, per-harness
catalogs and MCP routes, harness-scoped bootstrap filtering — exists. The earlier text argued for
mechanisms that are now built, and in one case argued for the wrong one: harness ownership was to be
*inferred* from which instance reported a provider id, and it is instead **declared**. That section is
deleted rather than corrected.

What follows is only what an adapter author still needs to know, and what is still unbuilt.

### 5.1 The seams, as they now exist

| Concern | Where it lives | What the Claude adapter does |
| --- | --- | --- |
| Declaration | `[harnesses.<name>]` with `kind`, `command`, `args`, `env`, `default`, plus an opaque `options` table (`giskard-persist/src/config.rs`) | declares `kind = "claude-code"`; `command` defaults to `claude` on `PATH` |
| Kind dispatch | `HarnessKind` + `HarnessKindFactory` + `HarnessInstanceSpec` (`giskard-server/src/harness_kinds.rs`) | a `ClaudeCodeKind` beside `CodexKind` in `bin/giskard-server.rs` |
| Thread binding | `ThreadFile.harness`, fixed at native creation, constant-defaulted to `codex` (`store.rs`) | nothing; the server owns it |
| Instance ownership | one slot per declaration name on `ProjectAuthority`, one driver each | nothing; the registry owns it |
| Bootstrap | `known_thread_bindings(project, harness)` filtered by that field | receives only its own `(native id, ThreadId)` pairs |
| Process model | the adapter's business, per the trait's own doc | one `claude` process per primary thread |

`CodexKind` (`bin/giskard-server.rs:19-65`) is the template, and it is short: `name()`,
`validate(declaration)` deserializing the kind-specific `options` with `deny_unknown_fields`, and
`create(spec, bootstrap)` building launch options from `command` / `args` / `env` and handing them to
the adapter. A `ClaudeCodeKind` is the same shape.

**Kind-specific options: probably none.** Everything this plan needs from the environment —
`CLAUDE_CONFIG_DIR` to separate two declarations, any credential variable — is the neutral `env`
overlay, which the design anticipated in as many words ("for Claude Code whatever that CLI reads from
its environment for its API key, config directory, or routing"). `--setting-sources` (§8.3) and
`--permission-prompt-tool` are adapter policy, not user configuration. If an option is ever needed it
goes in `options` and is type-checked at boot like Codex's `profile`.

**Two declarations of this kind are free.** Two `[harnesses.claude-*]` entries with different
`CLAUDE_CONFIG_DIR` values are two instances with separate credentials, settings and transcript
stores, which is exactly the isolation §3.5 describes — and it costs the adapter nothing, because the
overlay is applied by the layer above.

### 5.2 Process lifecycle

- Spawn in `open_thread`: one child per thread, `--session-id <fresh uuid>` or `--resume=<stored>`.
- **When `--resume` fails, respawn with `--session-id <the same uuid>` and keep the thread writable.**
  Resume fails whenever the transcript is gone — `cleanupPeriodDays` expiry (30 days by default,
  §3.5), a `claude project purge`, or a child that died before anything was persisted. The failure is
  immediate and unambiguous: exit 1, `No conversation found with session ID: <uuid>` on stderr, and a
  `result` with `subtype: "error_during_execution"`, `is_error: true`, `num_turns: 0`.

  The recovery is cheaper here than for Codex, because **Claude's session ids are client-minted**.
  Relaunching with `--session-id <the same uuid>` succeeds, recreates the transcript under that id,
  and reports the same `session_id` — so the `(native id, ThreadId)` binding never changes. It
  succeeds **only because the transcript is gone**: `--session-id` naming an existing transcript
  exits 1 at once with `Error: Session ID <uuid> is already in use.`, so the respawn is taken only
  for `No conversation found with session ID`, and a resume that fails for any other reason is an
  error, not a retry. There is
  no new identity to adopt, nothing to rewrite in `ThreadFile`, and **no route replacement**, so
  `AGENTS.md`'s rule that a resume-fallback replacement must require the exact prior binding simply
  never comes into play. Codex needs that rule because its ids are provider-minted and a failed
  resume yields a *different* one (`replace_thread_route`, C5); this adapter keeps its id.

  What the user sees is the Codex behaviour: the thread opens **writable**, not read-only, because
  Giskard's own history is intact and only the agent's context is lost. `open_thread` returns
  success with a `HarnessNotice` — the analogue of Codex's `codex_resume_failed` — saying agent
  context was lost and a fresh session was started, with the CLI's message as `detail`. **It must not
  error**, or a thread becomes permanently unopenable 30 days after its last turn.

  This is distinct from a `task:` child thread (§5.3), which is permanently read-only and whose id
  must never reach `--resume` at all.
- cwd is the thread's worktree when it has one, else the project workspace root, and it **should** be
  the same cwd on every respawn. Not for correctness — a session id resolves from any directory
  (§2) — but to keep a thread's transcripts in one place and avoid the cross-project fallback.
  `ThreadHandle.workspace_root` carries it. `CLAUDE_CODE_PROJECT_DIR_NAME` pins the directory name
  outright and sidesteps §3.7's lossy cwd encoding; it requires `CLAUDE_CONFIG_DIR` to be set too,
  which §5.1 already contemplates per declaration.
- `subscribe` must answer before the child has produced anything: create the retained `EventLog` at
  open, fill it from that process's reader.
- A child's exit ends **that thread's** stream only. One crash must not close its siblings'.
- **A `result` does not always end the turn.** A backgrounded delegation emits two (§5.3), so turn bookkeeping
  stays open until every **agent-type** task (`task_type: "local_agent"`) has a terminal
  `task_updated`. Background *command* tasks (`local_bash`) must not gate the turn — they legitimately
  outlive it — and belong in `RunningTaskState` instead.
- `discoveries()` is `DiscoveryStream::closed()`: a per-thread process produces no *unsolicited*
  native traffic, so there is nothing to discover.
- **`claim_native_thread` must be implemented**, for `task:` ids. It cannot stay at the trait default,
  which returns `Unsupported` — that is the only path by which a sub-agent thread comes into
  existence (§5.3).
- `ApprovalId` and `ServerRequestId` must be unique across the instance's children before they are
  published. Claude's `tool_use_id` happens to be globally unique, so the correct implementation and
  a lazy pass-through are indistinguishable under test — mint them in the façade anyway.
- **A respawn rebuilds the whole argv.** Resume restores the conversation, not the launch
  configuration: `--mcp-config`, `--settings`, `--plugin-dir`, `--fallback-model` and `--add-dir` must
  all be passed again — as must `--disallowedTools EnterPlanMode ExitPlanMode` (§8.2) and
  `--permission-prompt-tool`, since a respawn that drops either silently gives the mode back to the
  model or the approvals to nobody. Settings *files* are re-read, so `--setting-sources` survives by
  itself. A
  `-p` resume also does not restore the permission mode — it starts in the mode a new `-p` run would —
  which is already correct here because the adapter always passes `--permission-mode`. A tool still
  running when the previous process died is shown to Claude as cut off, with an instruction to check
  whether it took effect before retrying.
- **`delete_thread` and `set_thread_archived(true)` stop that thread's child**, and
  `set_thread_name` forwards to `rename_session` (§3.3). None of the three may be left at the trait
  default: the trait's own doc names them as where a per-thread adapter stops its processes — *"an
  adapter for a CLI that runs one process per primary thread spawns in `open_thread` and stops in
  `delete_thread`, `set_thread_archived`, `shutdown`, or on its own idle policy"* — and
  `registry.rs` calls all three and propagates whatever they return.
  - `delete_thread` stops the child and returns `Ok`. It does **not** delete the CLI's transcript:
    `~/.claude` is the user's, not Giskard's, and `cleanupPeriodDays` retires it on its own (§3.5).
  - `set_thread_archived(true)` stops the child; `(false)` is `Ok` and does nothing, because the next
    `open_thread` respawns with `--resume`.
  - `set_thread_name` sends `rename_session` when a child is live and returns `Ok` without it when
    there is none. The CLI's session name is cosmetic for Giskard, which keeps its own thread name,
    so a missing child is not a failure.
  - On a `task:` child thread (§5.3) all three are `Ok` no-ops: there is no process behind it.
- `shutdown` stops every live child — **but interrupts first**. SIGTERM leaves an in-flight turn
  unfinished and records no result for it (exit 143), while SIGINT or an `interrupt` control request
  ends the turn properly. A shutdown that skips the interrupt leaves Giskard holding turns the
  harness never resolved.

**Idle policy is the adapter's, and it matters more here than for Codex.** A `claude` process was
measured at **440–530 MB RSS**, and the server never tells a harness that a thread was dropped from
memory: `retire_thread` and `forget_thread` are invisible to it, deliberately. Implementing
`delete_thread` and `set_thread_archived` (above) bounds the worst case — a deleted or archived thread
releases its child immediately — but it does not close the gap: **a thread the user merely stops
looking at keeps its process**, so memory tracks threads *opened* rather than threads *open*. At the
spec's ~10-thread scale (§1.4) that is gigabytes.

The MVP should at minimum log the live-child count. Milestone 6 closes the gap: a child idle for
`idle_shutdown_secs` (a key on the `claude-code` declaration, default 600 s, `0` never) is
stopped while its thread stays bound and its stream open, and the thread's next message respawns
it with `--resume`. Memory then tracks threads *in use*. This answers the Claude Code half of
`docs/multi-harness-design.md`'s "idle shutdown" question; the Codex half stays open.

### 5.3 Sub-agent threads without native sessions

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

`parent_tool_use_id` is then the routing key **for forwarded items**: every forwarded message carries
the id of the call it belongs to, so items land in the child thread rather than interleaving into the
parent's transcript as if the main agent had run them. **It is not on the approval ask** — a
sub-agent's `can_use_tool` carries `agent_id` and `tool_use_id` instead, so approvals route by
`agent_id`, the `task_id` of the sub-agent's `task_started` (§3.3, §9.3). The parent's `Agent`
item carries a `SubagentLink` with the same id, `initial_prompt`, and `action`/`status` mapped from
the `system/task_*` messages (`task_started` → `Started`, `task_updated.patch.status` →
`Completed`), which is what populates the Sub-agents card.

**These threads are created through `claim_native_thread`, which the adapter must therefore
implement.** The path is fixed by the server and leaves the adapter no choice:

1. the thread's event forwarder sees an item carrying a `SubagentLink`
   (`registry/event_forwarder.rs`, on both `ItemStarted` and `ItemCompleted`);
2. it sends a `Link` to the project event driver;
3. admission calls `harness.claim_native_thread(ThreadId::new(), native_thread_id, workspace_root)`
   (`registry/admission.rs`).

So leaving `claim_native_thread` at the trait default does not merely skip the feature, it **fails
every delegation**: a forwarder-originated link carries no reply channel, so an `Err` result is
re-queued by `defer_admission` with an incremented attempt count and retried on every subsequent
driver event, with a `failed to admit linked native thread` warning each time and **no attempt cap**.
One sub-agent call would leave a permanent entry in the deferred queue and a log line per driver
event.

What the adapter's implementation does:

- **bind `task:<tool_use_id>` to the proposed `ThreadId`** in the façade, alongside the primary
  threads it already owns;
- **return a `ThreadHandle` whose `harness_thread_id` is exactly the id it was given** — admission
  compares the two and raises `HarnessError::Protocol` ("linked-thread claim returned native thread X
  instead of Y") on a mismatch, so the handle must echo the claim rather than mint a new id;
- **make `subscribe` on that handle yield the forwarded items whose `parent_tool_use_id` matches**,
  which is what fills the child thread.

Admission does the rest once the claim succeeds: it sets `file.parent_thread_id` to the linking
parent and `file.kind = ThreadKind::Subagent`, which is what the Sub-agents card and subtree
navigation key on. The adapter supplies identity and a stream; it does not build the thread graph.

**The "never `--resume` a `task:` id" rule belongs in `open_thread`, not here.** Refusing the claim
would destroy the feature; refusing the *resume* is the actual invariant. `open_thread` on a `task:`
id must never reach `--resume`, and should surface the thread through the existing read-only path
instead.

**Why prefix rather than infer from `ThreadKind::Subagent`.** The kind is available wherever a
`ThreadFile` is loaded, but not on the admission path: `HarnessBootstrap.known_threads` and
`claim_native_thread` deal in bare `(harness_thread_id, thread_id)` pairs. Code there cannot ask "is
this resumable?" without loading the thread. A prefix answers it at the point of use, and synthetic
prefixed identifiers are already established practice here — `app.js` special-cases
`subagent_prompt:` item ids.

**And the parent turn may not end when it looks like it does.** A delegation runs in one of two
shapes, and **`task_started.is_backgrounded` is the discriminator**:

| `is_backgrounded` | What follows |
| --- | --- |
| `false` | The parent waits. One `result`, after the sub-agent finishes. |
| `true` | The parent's `tool_result` is the internal metadata *"Async agent launched successfully"* and its `result` lands immediately. The sub-agent's tool calls, approval asks and answer arrive afterwards, then `task_updated` → `task_notification` → **a re-emitted `system/init`** → a **second `result`** carrying the outcome. |

Which shape a call takes is decided per delegation, not by configuration: the same prompt produced
each. So **the mapper must handle both**, and must not assume a second `result` either arrives or
does not.

This is a property of the Agent tool, not of the environment: the backgrounded shape reproduces with a
child whose only `CLAUDE_*` variables are `CLAUDE_CONFIG_DIR` and a token-file path — no
`CLAUDE_AUTO_BACKGROUND_TASKS`, no `CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST`, no subagent-depth setting.

**The rule this imposes, and its scope.** The mapper keeps the parent turn open until every
**agent-type** task it started has reached a terminal `task_updated`:

> **Gate turn completion on `task_type: "local_agent"` tasks only.**

**Scoping matters, because the task registry also carries background shell commands.** A
`task_type: "local_bash"` task can be `is_backgrounded: true` and legitimately outlive its turn — a dev
server is the obvious case, and its terminal `task_updated` may not arrive for hours or at all (§3.3's
`stop_task` only produced one because the probe killed the task). Gating on *every* `task_started`
would therefore hold such a turn open forever. Background command tasks belong in Giskard's existing
running-task projection instead — `RunningTaskState` in `thread_runtime/tasks.rs`, fed from the
command item's own status — which is where a long-lived shell already surfaces to the UI and where
`terminate_command` would hook in.

**The agent-task rule is load-bearing in both directions, because an interrupted delegation produces no
second `result` at all.** Interrupting while the sub-agent's tool is in flight yields:

```
system/task_updated  patch={"status":"killed","end_time":…}
tool_result  parent=toolu_…  is_error=true   "The user doesn't want to proceed with this tool use…"
text         parent=toolu_…  "[Request interrupted by user for tool use]"
```

and then nothing — the sub-agent's command is killed, and the second `result` never comes. An adapter
that waits for a `result` to close the turn would hang; one that closes on the first would have closed
too early. **A terminal `task_updated` — `completed` or `killed` — on the agent task is the only signal
that works for both cases.** The `[Request interrupted by user for tool use]` text block is a useful
marker to surface in the child thread, and the child's `tool_result` again carries *rejection* wording
for what was an interrupt (§9.3), so it must not be mapped as a denial.

**Three requirements this imposes:**

1. **`--forward-subagent-text` is mandatory**, not optional. Without it only the final result surfaces
   and every child thread is an empty shell. Its completeness has been version-dependent across the
   2.1.2xx series — sub-agents spawned by a forked skill, and a sub-agent moved to the background
   mid-run, were both dropped at various points — so **a child thread that renders as a partial
   transcript is a plausible CLI-version symptom, not necessarily a mapper bug**.
2. **Child threads are permanently read-only.** The existing read-only path for threads whose harness
   cannot attach (PS1, `read_only_info`) is the right mechanism, so this needs no new UI state — but
   the condition is *permanent* here rather than recoverable, and the wording should not imply
   otherwise. The `--resume` invariant that enforces it lives in `open_thread`, above.
3. **The mapper keys off the tool named `Agent`.** The stream names the tool `Agent` in its `tool_use`
   block even though the CLI and its documentation call it `Task`.

The CLI's own transcript format is documented as internal and unstable — *"the entry format is
internal to Claude Code and changes between versions"* — so this design's reliance on the **stream**
rather than the `.jsonl` file is the supported route and must stay that way.

**What makes this honest rather than a fiction.** The id is real, harness-minted and globally unique;
it is a different *category* of identifier, which the prefix states. And the child's transcript is
Giskard's own persisted history, so the thread stays readable forever. Its unresumability is
narrower than that of a primary thread whose transcript expired, though: that one reopens writable
under the same id with a fresh session (§5.2), whereas a `task:` id has no session behind it and never
will. What the design must never do is let a `task:` id reach `--resume`.

**Nesting works.** Forwarding covers *"subagents at every nesting depth"*, and a nested agent's
messages carry the id of the Agent or Skill call that started it, *"so you can rebuild the full
nesting tree by following those IDs"* — exactly the thread graph this design builds.

---

## 6. Models, context windows, tokens, cost

- **Catalog: from the harness, not from HTTP.** The adapter reports `model_listing: true`, and the
  rule is:

  > **`list_models` returns the freshest `initialize.models` any live child of the instance has
  > reported. When no child is live, the instance spawns a probe child, sends `initialize`, reads
  > `models`, closes stdin, and caches the result.**

  **This is on milestone 2's critical path (§11), not a later milestone's.** The picker calls `list_models` on the
  *instance*, before any thread exists, so without an answer the first Claude thread of a project
  cannot be created at all unless the user hand-declares `[providers.anthropic.models]`.

  **The probe is cheap and leaves nothing behind.** `initialize` answers in
  **1.5 s wall on a cold start**, exit 0, and **no transcript was written** — the config directory
  gained only machine state (`.claude.json` and its backup, `policy-limits.json`,
  `remote-settings.json`) and **no `projects/` directory at all**. No `--session-id` is needed, and one
  request is enough: `initialize.models` and `list_models.models` came back byte-identical.
  **Re-verified on 2.1.287 with `mcp_status` as the follow-up** (the MCP-status-per-thread change):
  a probe launched with the adapter's exact protocol flags, sent `initialize` alone or `initialize`
  then `mcp_status`, then closed, created **no `projects/<cwd>/` directory, no session `.jsonl`,
  no `sessions/` entry and no project entry in `.claude.json`**; the only files touched were caches
  (`cache/model-catalog/*`, the growth-book features in `.claude.json` and its backup,
  `policy-limits.json`, `remote-settings.json`). It does start the user's configured MCP servers for
  the second it lives, which is the price of reporting their status. **The rule** (`AGENTS.md`):
  the probe never becomes a session, and every new control request sent on it is verified this
  way first.

  The probe runs with the declaration's `command`, `args` and `env` overlay and
  `--setting-sources user` — the §8.3 choice — **against the user's real `CLAUDE_CONFIG_DIR`, not a
  throwaway**, so it reflects their configuration. That matters: a `--settings` payload restricting
  `availableModels` to `sonnet,haiku` narrowed the answer to `default`, `sonnet`, `haiku`, so the
  user's `availableModels` and `enforceAvailableModels` reach the picker.

- **What the catalog means.** It is **a table built into the binary, filtered by settings — not a list
  fetched for the account**. With no credentials, no configuration and no network, the same five
  entries came back: `default` → `claude-sonnet-5-5`, `sonnet`, `claude-fable-5-1`, `opus` →
  `claude-opus-5-5`, `haiku` → `claude-haiku-4-5-20251001`. Three consequences:

  - **The picker offers what the CLI knows**, not what the subscription can run.
  - **A model the subscription cannot run is refused at the first turn**, not hidden from the picker.
    The adapter should surface that failure clearly rather than pre-filtering a list it cannot check.
  - **`result.modelUsage` remains the truth for what actually ran**, which is what the ledgers record.

  **What it gives**: `value` (the selector — an alias like `sonnet` or a full id), `resolvedModel`,
  `displayName`, `description`, and per-model `supportedEffortLevels` plus `supportsEffort` /
  `supportsAdaptiveThinking` / `supportsAutoMode` / `supportsFastMode`. The effort levels are a direct
  win for §4's `reasoning_effort` row, which wanted exactly this to validate against.

  **What it does not give is a context window.** That costs nothing, because the window never came
  from the catalog — see the next bullet. `[providers.anthropic.models]` remains available for
  hand-declaring models, and composition merges it as before.

- **Context window.** Emit it as `AgentEvent::TurnUsageUpdated { context_window }` — the only event
  that carries a window (`giskard-core/src/event.rs`); there is no `ContextWindowUpdated` variant —
  from `autocompact_state.effective_window`, falling back to
  `result.modelUsage[<model>].contextWindow`. This is the effective, post-headroom number,
  which is exactly what the spec's context gauge (§10.3) wants. `autocompact_state` often arrives
  unprompted at session start (`{enabled, effective_window, threshold, enforced, source}`), but
  **it is not guaranteed** — some sessions emit none — so the `modelUsage` fallback is load-bearing
  and the gauge may be empty until the first turn completes. `get_context_usage` (§3.3) is a third
  source, reporting `maxTokens` / `rawMaxTokens` plus a live category breakdown on demand; not needed
  for the gauge, but the natural backing for a richer context view later.
- **Tokens.** `TokenUsage { input, output, total }` from `result.usage`, with
  `input = input_tokens + cache_creation_input_tokens + cache_read_input_tokens`. This is the
  documented arithmetic — *"`input_tokens` is the uncached remainder only"* — and reading
  `input_tokens` alone would undercount badly in exactly the long agentic sessions this harness
  produces.

  **Flat per-Mtok rates cannot price this correctly in either direction.** Cache reads bill at
  **~0.1×** base input price (0.05× on Opus 5.5, 0.025× on Fable 5.1), so a flat rate *overstates*
  them; cache writes bill at **1.25×** for the 5-minute TTL and **2×** for the 1-hour, so the same
  flat rate *understates* those. The three summands carry three different prices and the net
  direction depends on the read/write mix. With `tokens.cost_estimation = true` the honest
  description is "an estimate that cannot be right for this harness", not "an overestimate". For a
  subscription user the euro figure is notional anyway.
- **Ancillary models may appear in `by_model`.** `result.modelUsage` can carry a Haiku entry alongside
  the selected model, because Claude Code runs summaries and titles on a small model — **but it often
  does not**, because the background auto-title request was removed from `claude -p` runs launched
  outside an SDK or IDE. The handling below is necessary because ancillary work can still appear; it
  must not assume a second entry exists. **Record each `modelUsage` entry under its real model id**
  and keep `Turn.model` as the user's selection. Dropping the ancillary usage would make Giskard's
  totals disagree with the provider's; folding it into the selected model would corrupt that model's
  per-Mtok rates.
- **Cost.** Do not use `result.total_cost_usd` as truth for a Pro/Max user — it is priced as if the
  request were API-billed. Prefer the `rate_limit_event` five-hour window as the honest "how much
  budget is left" signal, surfaced as a `Notice` (and, later, a header chip).

### 6.1 Why `get_workspace_diff` is not `structured_diffs`

`get_workspace_diff` (§3.3) answers from a stdio host, returning real stats, per-file counts and
hunks. It is nevertheless the wrong source for this capability, and the distinction matters because it
decides whether the milestone 7 work exists at all.

**`structured_diffs` is a turn-attribution feature, not a diff feature.** The trait defines it as a
"structured, per-file diff *stream*"; it feeds `DiffUpdated { thread, turn, diff }` and persists as
`Turn.diffs`, "file diffs produced during this turn". The question it answers is *what did this turn
change* — which is why it renders beside a turn in the transcript rather than in a workspace view.

`get_workspace_diff` answers a different question: *what does this workspace look like against a base
ref right now*. It resolves one base ref for stats and hunks (merge-base with the session's base
branch, else the default branch, else working tree vs `HEAD`) and applies fixed caps — 5s git timeout,
50 files. There is no turn in it, no stream, and no way to attribute a hunk to the turn that produced
it. Two turns editing one file are indistinguishable in its output.

**And Giskard already does this better.** `giskard-git-parser` exists precisely to parse git output,
Giskard controls its own base-ref policy (which it must, given per-thread worktrees —
`docs/git-worktrees.md`), and it is not bound by another tool's 50-file cap or `@internal` stability.
Routing a workspace diff through a child process to get an answer Giskard can compute directly would
add a dependency and lose control, for nothing.

So the milestone 7 plan is unchanged: synthesize `FileChange` / `DiffUpdated` from the `Edit` / `Write` /
`NotebookEdit` tool calls — which *are* turn-attributed, because they arrive inside a turn — using git
for the before/after content. `get_workspace_diff` is worth knowing about for a future workspace-level
diff view, where its base-ref resolution is a reasonable default to copy. It is not worth wiring into
this capability.

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
  choice does not affect the subscription login either way.

**The declaration's `env` is an overlay, and that has a sharp edge here.** `HarnessEnv` sets variables
*on top of* what Giskard itself was started with; nothing is removed. **A child inherits the operator's whole `CLAUDE_*` environment**, and that is observable: variables
such as `CLAUDE_CODE_CHILD_SESSION`, `CLAUDE_CODE_SESSION_ATTENDED`, `CLAUDE_CODE_REMOTE` and
`CLAUDE_AUTO_BACKGROUND_TASKS` reach every child whether or not the declaration mentions them, and at
least one changes behaviour — `CLAUDE_AUTO_BACKGROUND_TASKS=true` makes a Bash call run backgrounded
that nothing asked to background.

The practical consequence is for verification rather than production: **a child that inherits an
unknown environment cannot be reasoned about from its declaration alone**, so any protocol check
Giskard relies on must run with a scrubbed environment and `CLAUDE_CONFIG_DIR` pointed at a throwaway
directory (§3.5), and a surprising result must be isolated by A/B rather than attributed to the first
plausible cause. So a stray `ANTHROPIC_API_KEY`
in the operator's own shell reaches every child of every declaration, and no `[harnesses.claude.env]`
entry can unset it — only overwrite it with another value. For this harness that is not cosmetic: an
API key silently moves a subscriber from their plan onto pay-as-you-go credits. The same applies to
`ANTHROPIC_BASE_URL` and `CLAUDE_CODE_USE_BEDROCK`/`_VERTEX`, which redirect the backend outright.

The mitigation inside Giskard is detection rather than prevention, and **the signal is
`system/init.apiKeySource` on the first turn** (`"none"` = OAuth/subscription). The adapter warns when
it is anything else.

`initialize`'s `account:{subscriptionType, apiProvider}` arrives earlier but **must not be used for
this**: a probe with no credentials at all still reports `{"Claude API", "firstParty"}`, so the field
is a default rather than a statement about how the session authenticated. Treat it as informational.

**Prevention does exist, but it belongs to the machine's owner, not to Giskard.** `allowedProviders`
is a *managed* settings key restricting which API providers the machine may use.
Managed settings outrank command-line flags and every other scope (§3.5), so an operator who sets it
constrains the provider whatever the environment holds. Giskard should not write it — that is policy
about the machine, and §9.3's destination invariant is the same principle — but the documentation
should name it as the answer for a user who wants the guarantee rather than the warning.

Whether a declaration should be able to *remove* an inherited variable remains a question for
`docs/multi-harness-design.md`, not for this adapter to solve locally.

### 7.1 Why `GET /v1/models` stays out of this harness

**Giskard needs no credential of its own for the catalog.** §6's probe answers the no-live-child case
— the picker before any thread exists — so there is no gap left for an HTTP call to fill.

Anthropic's Models API stays out of v1 for two reasons:

- **It needs an API key.** An unauthenticated `GET /v1/models` answers
  `{"type":"authentication_error","message":"x-api-key header is required"}`. Whether Claude Code's
  subscription OAuth token is accepted there is **[unverified]** — and a subscription user is exactly
  who this plan is for, so the credential a key-based route needs may not exist on their machine at
  all.
- **Its one extra field is already covered.** `max_input_tokens` is the only thing it carries that the
  harness catalog does not, and the context window already comes from `autocompact_state` and
  `result.modelUsage[].contextWindow` (below). Building the route would add a credential dependency to
  obtain a number Giskard already has.

If it is ever built — for a provider with no Claude Code behind it — the `models.rs` work is a third
response body shape, auth placement on `ProviderAuth` (`x-api-key`, not bearer), and the
`anthropic-version` header. `[providers.anthropic.models]` remains the hand-declared fallback in the
meantime, and composition merges it over whatever the probe returns.

---

## 8. Presets, Plan mode, and settings sources

### 8.1 Permission presets

| Giskard preset | `--permission-mode` | Notes |
| --- | --- | --- |
| `ask_first` | `manual` (echoed back as `default`) | **Not "ask about everything":** the CLI approves a built-in read-only command set itself, below the settings layer, with no settings loaded at all (§9.2.1). Calls with an effect reach `can_use_tool`. `default` still passes validation but is no longer among the advertised choices, while `manual` is — so send `manual` and expect `system/init.permissionMode` to echo `default`, treating that echo as success rather than a discrepancy to correct. |
| `auto_approve` | `acceptEdits` | file edits and filesystem commands inside the workspace proceed; other escalations still ask |
| `full_access` | `bypassPermissions` | **Not** "never consulted" — see the list below; some calls still reach `can_use_tool` in this mode. Refuses to start if the server process is running as root — see below, and this is documented rather than merely observed. **Launch-time only:** `set_permission_mode bypassPermissions` answers `error_code: "bypass_not_launched"` on a child launched in any other mode, while a child launched with `--permission-mode bypassPermissions` can be set to `default` and back. So the adapter launches in bypass mode where the CLI allows it, sets `default` in the open handshake before any turn, and falls back to a `manual` launch (with `full_access` refused per turn) where the launch is refused (milestone 3) |

**Two modes this table does not use, recorded so they are not rediscovered:**

- **`auto`** — "Use a model classifier to approve/deny permission prompts". It is a *third* answer
  between `ask_first` and `auto_approve`, and it has no Giskard preset. It is deliberately not mapped
  here: Giskard's presets promise the user a rule they can predict, and a model classifier is neither
  `ask_first`'s "you decide" nor `auto_approve`'s "edits proceed". Adopting it would be a product
  decision about what a preset means, not an adapter change.
- **`dontAsk`** — "Don't prompt for permissions, deny if not pre-approved". Fail-closed rather than
  fail-open, so it is the natural mode for an unattended thread, and it pairs with the new
  `--permission-prompts none` flag. Also unmapped: Giskard has no unattended preset today.

**Always pass `--permission-mode` explicitly.** As of 2.1.267 a `claude -p` session with no permission
mode configured starts in `auto` mode. The adapter always sets the flag, so this is already correct —
but it means the flag can never be dropped on the grounds that the default matches the preset.

**Some actions no mode auto-approves, `bypassPermissions` included.** This is documented
([permission-modes § Actions no mode auto-approves](https://code.claude.com/docs/en/permission-modes)),
and it corrects this table's original claim that `full_access` means the callback is never consulted:

- tools matched by an explicit `ask` rule;
- connector tools an organization has set to `ask`;
- **tools that require user interaction** — the built-in `AskUserQuestion`, and MCP tools marked
  `requiresUserInteraction`;
- **`rm` / `rmdir` removals targeting a "critical path"** (e.g. `rm -rf /`, `rm -rf ~`), *"which no
  allow rule or `PreToolUse` hook `"allow"` approves"*;
- the cross-session messaging safeguards;
- reads outside the working directories when `permissions.blockReadsOutsideWorkingDirectories` is on.

Two consequences for the adapter. **The approval path must stay live in every preset**, including
`full_access` — an adapter that skips wiring `can_use_tool` when the preset is `bypassPermissions`
will hang the first time one of these fires. And **`AskUserQuestion` arriving through `can_use_tool`
is not an approval at all**: it is a question with structured options, whose reply is
`{behavior:"allow", updatedInput:{questions, answers}}`. That is Giskard's `ServerRequestReceived` /
`respond_server_request` path (§3.3), not its approval card, and the adapter must branch on
`tool_name == "AskUserQuestion"` before mapping an `ApprovalKind`.

**`full_access` depends on the identity of the server process.** Launching with
`--permission-mode bypassPermissions` exits non-zero with `--dangerously-skip-permissions cannot be
used with root/sudo privileges for security reasons`. Observed while probing from a root shell, and
**documented**: *"On Linux and macOS, Claude Code refuses to start in this mode as root or under
`sudo` outside a recognized sandbox, and the query fails before the first turn"*
([agent-sdk/permissions](https://code.claude.com/docs/en/agent-sdk/permissions)). It is a property of
the effective uid, not of Giskard.

This should not arise in the documented setup: Giskard runs as the user whose `$HOME` holds the data
directory and whose harness credentials the child inherits (spec §12.2), and that user is not root. It
is recorded because the failure is a non-obvious spawn error rather than a permission message, so the
adapter should detect the refusal and surface the cause instead of a generic spawn failure — the same
treatment any other unusable preset gets.

### 8.2 Plan mode

**Plan mode collapses the orthogonality.** In Codex, Plan/Build is collaboration mode only and is
orthogonal to the preset (spec §9.1). In Claude Code, `plan` *is* a permission mode, so Plan + preset
occupy one slot. Spec §9.1 must say this explicitly for harnesses without `plan_build_modes`
independence.

#### Contract: Giskard owns the mode

Three parts, and the first is not optional:

1. **Every child gets `--disallowedTools EnterPlanMode ExitPlanMode`.** A bare tool name in a deny rule
   removes the tool from the model's context entirely, so it cannot be called. Without the
   flag, a `manual`-mode child asked to enter plan mode called `EnterPlanMode` and the CLI emitted
   `system/status {"permissionMode": "plan"}` — **with no `can_use_tool` at all** — and the mode
   persisted into later turns. With the flag, the model's own tool search reported *"No matching
   deferred tools found"*, no `system/status` frame appeared, and the mode did not move.
2. **The adapter sets the mode with `--permission-mode` at spawn and `set_permission_mode` at the start
   of every turn** — not only when the mode changes. A per-turn set costs one control request and makes
   a stale mode impossible to carry over.
3. **Plan wins over the preset**: a Plan-mode turn sends `plan` regardless of preset, and the preset
   applies again in Build.

**Answering the callback is not a substitute for the flag.** `ExitPlanMode` does reach `can_use_tool`,
and denying it is clean (below) — but `EnterPlanMode` never reaches the callback at all, so a
denial-only strategy leaves the model free to switch a Build session into plan mode unobserved. The
flag is what makes the mode Giskard's.

#### What a plan turn promises: plan wins, and edits ask

Per the SDK permissions documentation, in plan mode *"file edits are never auto-approved, even when an
allow rule matches; they prompt through your `canUseTool` callback"*, and since 2.1.212 file-modifying
shell commands such as `touch` and `rm` do the same.

So **a plan turn under Claude is stricter than under Codex**: an edit reaches Giskard's approval card
even under `auto_approve`, and the user decides. That is the contract to state — not an adapter that
auto-denies edits during a plan turn. The harness already enforces the boundary; the adapter's job is
to surface the ask.

#### Side effect: plan turns write files under the config directory

A plan turn writes its plan to `~/.claude/plans/<slug>.md` with no ask. Recorded so nobody reads it as
a leak past the workspace: it lands in the user's own Claude Code config directory, which is
`CLAUDE_CONFIG_DIR` for the declaration (§5.1), not in the project or the worktree.

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

**And this is not the only thing pre-empting `ask_first`.** §9.2.1 verifies that the CLI approves
effect-free commands on its own, with no settings loaded at all. So the gap between what `ask_first`
promises and what it does exists even for a user who has written no rules — the settings decision
above widens a gap that is already there rather than creating it. Both consequences apply with more
force.

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

## 9. Live approvals

### 9.1 Three routes; stdio for the MVP, the hook deferred

Claude Code can hand a permission decision to an external party three ways. **The MVP takes the stdio
route. The hook route is deliberately postponed to a later decision and refactor (§9.4).** The MCP
route is documented here so it is not rediscovered as a new idea, and rejected.

**Where an ask comes from.** A tool call is resolved in six steps — hooks → deny rules → ask rules →
permission mode → allow rules → the callback — so `can_use_tool` is consulted **last**, and anything
approved earlier never reaches it. That ordering drives §8.3's accepted cost and §9.4's argument for
the hook route.

**Part of this surface is published and part is not, which is a stability signal worth carrying.** The
permission *model* is documented: the evaluation order, the rule grammar, the built-in read-only
command set (§9.2.1), the callback contract `{behavior:"allow", updatedInput?, updatedPermissions?}` /
`{behavior:"deny", message, interrupt?, toolUseID?}`, and the rule that auto-approved tools never reach
the callback. The **transport this adapter uses is not**: `--permission-prompt-tool stdio` as a
sentinel, the `control_request` envelope, `request_id` correlation, and the `permission_suggestions` /
`tool_use_id` / `blocked_path` fields appear in no published document, and neither does
`destination: "session"` (§9.3). Those are the parts that could change without a documentation diff to
warn anyone, so they are the ones to cover with regression tests.

One version floor to respect: before v2.1.207 an allow result that omitted `updatedInput` was
rejected. §9.3 maps `Accept` to a bare `{behavior:"allow"}`, which is correct for the versions this
plan targets.

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
**near-unconditional**, and this is documented: *"hooks run before every other step, and a hook deny
applies even in `bypassPermissions` mode"*
([agent-sdk/user-input](https://code.claude.com/docs/en/agent-sdk/user-input)). **The symmetry does
not hold for allow**, which matters if the hook is ever adopted as the whole approval path: a hook's
`allow` does not skip the deny and ask rules beneath it, and it does not approve an `rm`/`rmdir`
removal targeting a critical path (§8.1). A hook can therefore veto anything, but it cannot grant
everything. That property is what makes it interesting to
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

Three findings from the round trip:

- **Settings allow-rules pre-empt the callback.** With the surrounding environment's settings loaded,
  the same probe auto-approved the command and no ask was ever emitted; it took `--setting-sources ""`
  to see the ask at all. Under the chosen `user` scope this is an accepted limit
  of `ask_first` rather than a defect (§8.3).
- **A built-in classification pre-empts it even with no settings at all** — see §9.2.1, which was
  found later and corrects what this section originally implied.
- **`permission_suggestions` is typed and carries a `destination`**, which decides whether a granted
  rule is remembered for the session or written to a settings file. Giskard always uses `session`
  (§9.3).

### 9.2.1 `default` mode does not ask about everything

Below the settings layer, the CLI classifies some commands itself and runs them with no ask. With no
settings loaded at all and an empty config directory:

| Bash command | `can_use_tool` fired |
| --- | --- |
| `echo HELLO` | **no** |
| `sleep 2 && echo HELLO` | **no** |
| `ls -la` | **no** |
| `touch probe-sidefx.txt` | **yes** |
| `cat /etc/hostname` | **yes** |

No settings file could have allowed these: none were loaded, and the config directory was empty. So
**`--permission-mode default` does not mean "every tool call reaches the callback"**.

**This is documented, and the plan simply had not consulted it.** The
[permissions reference](https://code.claude.com/docs/en/permissions#read-only-commands) names the
mechanism and enumerates the set: *"Claude Code recognizes a built-in set of Bash commands as
read-only and runs them without a permission prompt in every mode… The set includes `ls`, `cat`,
`echo`, `pwd`, `head`, `tail`, `grep`, `find`, `wc`, `which`, `diff`, `stat`, `du`, `cd`, and
read-only forms of `git`. **The set is not configurable**; to require a prompt for one of these
commands, add an `ask` or `deny` rule for it."* The same page scopes it to the working directories,
which is why `ls -la` did not ask and `cat /etc/hostname` did — not a read-versus-write split, but a
path-scope one. The documented list also carries exceptions the probe never reached: unquoted globs
for write-capable commands, `cd` combined with `git` or a redirect, commands the parser cannot fully
analyse, and anything over 10,000 characters.

Treat that page as the specification rather than re-deriving the boundary by experiment.

**Two consequences for this design.**

1. **`ask_first` means "ask before anything with an effect", not "ask before anything".** The UI
   wording must promise no more than that, and the gap exists for a user who has configured nothing —
   so §8.3's settings decision *widens* a pre-existing gap rather than creating one.
2. **The hook route (§9.4) is the only way to close it.** Its case does not depend on §8.3: even with
   zero settings loaded, `ask_first` is not absolute, and hooks are the only mechanism documented to
   run before every other step.

The denial path is unaffected — an effectful call still asks, a `{"behavior":"deny"}` reply still
blocks it, and the turn still completes normally — so every row of §9.3's mapping holds.

### 9.3 Decision mapping

**This maps onto Giskard's existing approval model almost exactly:**

| `ApprovalDecision` | Claude response |
| --- | --- |
| `Accept` | `{behavior:"allow"}` — **verified** |
| `AcceptForSession` | `{behavior:"allow", updatedPermissions:[<the ask's own addRules suggestion, destination rewritten to "session">]}` — **verified**; see below |
| `Decline` | `{behavior:"deny", message:"Declined"}` — **verified**: tool blocked, turn continues and completes normally |
| `Cancel` | `{behavior:"deny", interrupt:true}` — **verified**, and genuinely distinct from `Decline` (below) |
| `AcceptWithExecPolicyAmendment` | no analogue — do not advertise it in `available` |
| `ExitPlanMode` / `EnterPlanMode` asks | **never appear, by construction** — both tools are disallowed on every child (§8.2). If a future CLI version reintroduces them despite the flag, deny with a message like *"Giskard chooses the mode per turn"*: verified safe, since a denied `ExitPlanMode` ends the turn normally (`end_turn`, `is_error: false`, `post_turn_summary.status_category: "review_ready"`) with the plan preserved in `result.permission_denials[0]` |

**`Cancel` verified.** Replying `{"behavior":"deny","message":…,"interrupt":true}` aborts the whole
turn: `subtype: "error_during_execution"`, `terminal_reason: "aborted_streaming"`, `is_error: true`,
`stop_reason: null`. The plain `Decline` reply on the same setup left the turn running to a normal
`stop_reason: "end_turn"`. So the two decisions really are different operations, as in Codex.

*Mapping consequence:* a turn Giskard itself cancelled must be persisted as
`TurnStatusKind::Interrupted`, **not** `Failed`, even though the harness reports `is_error: true` and
an error-shaped subtype. The adapter knows which it is, because it sent the `interrupt`.

**`AcceptForSession` — use `updatedPermissions` with `destination: "session"`.**

`updatedPermissions` is a typed, supported mechanism: `addRules` / `replaceRules` / `removeRules` /
`setMode` / `addDirectories` / `removeDirectories`, each with a `destination` of
`userSettings | projectSettings | localSettings | session | cliArg`, applied to the live permission
context and (for persistent destinations) written to disk.

**The recipe that works** — take the `addRules` entry out of the ask's own `permission_suggestions`,
override its `destination` to `"session"`, and send it back with the allow. This holds on both
permission dimensions: a command asked once and then ran twice more with no further ask.

| Command | Asks with the session rule | Rule as the CLI stored it |
| --- | --- | --- |
| `python3 -c "print(1)"` (no file write) | **1** (was 3) | `Bash(python3 -c "print\(1\)")` |
| `touch probe.txt` (file write) | **1** (was 3) | `Bash(touch probe.txt)` |

**Do not synthesize the rule text.** Every earlier failure in this investigation was a hand-written
rule that did not match the command's canonical form — the CLI escapes glob metacharacters and
preserves quoting (`python3 -c "print\(1\)"`), so a rule Giskard composes itself will silently fail to
match while still being reported as applied. Echo the CLI's own `ruleContent` verbatim; change only the
destination.

**Evidence status.** `updatedPermissions` and the `suggestions` argument are documented, and the
"approve and remember" pattern of echoing a suggestion back is the documented idiom. The
**`destination` enum is not fully published** — the guide shows only `localSettings`, by example. The
`session` destination this decision depends on rests on the §9.3 round trip plus `claude-codes`'
typed `PermissionDestination`. Treat it as verified-but-unpublished: it works, and it could change
without a documentation diff to warn anyone, which is another reason for the regression test below.

**Session scope is what spec §9.2.1 asks for.** These rules live in the child process's permission
context and die with it, so the boundary is the process that enforces them — exactly the reason
§9.2.1 gives: *"the approval memory is a property of the running
agent process, which is what actually enforces it, so the boundary must match that process."*

**What that means concretely here.** The process serves one thread, so a grant made in one Claude
thread does not cover a sibling Claude thread in the same project, and it dies when that thread's
child stops — which includes `delete_thread` and `set_thread_archived(true)` (§5.2), not only shutdown
and crash.

**No spec change is needed for the behaviour**, only for the wording. §9.2.1 already anticipates this:
*"Scope of a grant follows what the harness scopes it to; Giskard does not broaden it."* And the UI
string it specifies — *"Approved for this session — resets if the agent restarts"* — is accurate
unchanged, because for this harness "the agent" is that thread's child. What is Codex-specific is only
the definitional parenthetical, *"the current harness process for that project (i.e. the
`codex app-server` child spawned for the project)"*, which should be generalised to the process that
enforces the grant (see *Documentation to update*, §11).

No Giskard-side approval memory is needed, and none should be built: the harness provides the
semantics, and a second copy could disagree with the process doing the work.

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

**Sub-agent approvals reach this channel too.** An ask can belong to work running inside a §5.3
`task:` child thread rather than the primary one, and the adapter must route it there or the decision
attaches to the wrong transcript.

**Routing goes through `agent_id`, not `parent_tool_use_id`.** The ask carries **no
`parent_tool_use_id`** — its keys are `agent_id`, `blocked_path`, `description`, `display_name`,
`input`, `permission_suggestions`, `subtype`, `tool_name`, `tool_use_id` (§3.3). `parent_tool_use_id`
is on the *message* carrying the `tool_use` block, not on the control request, and **that message
can arrive after the ask**: in four of five 2.1.287 recordings the `can_use_tool` preceded the
forwarded `assistant` frame carrying its block (the `subagent-stop` and `subagent-ask-withdrawn`
fixtures show it). So the adapter routes the ask by `agent_id`, which equals the `task_id` of the
sub-agent's `task_started` — a frame that always precedes the child's first frame — and falls
back to matching `tool_use_id` against the `tool_use` blocks already seen only when the task is
unknown. (This paragraph originally required the block to be processed first, on the strength of one
recording where they landed ~0.1 s apart in that order; the milestone 5 recordings corrected it.)

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
protocol defect but an ordering one: `can_use_tool` is consulted *last*. **§9.2.1 makes this
argument independent of the §8.3 settings decision**: even with no settings loaded, the CLI approves
effect-free commands itself, so `ask_first` is not absolute out of the box. Reverting §8.3 would not
fix that; only a hook would. Deny rules, ask rules, the
permission mode, and allow rules — including allow rules from the user's own `settings.json` — are all
evaluated first, and anything they approve never reaches the callback. That is the §8.3 hazard: an
`ask_first` thread can execute a command without asking, because the user once allowed it in their own
`~/.claude/settings.json`. Since the MVP deliberately loads that file (§8.3), this is not hypothetical —
it is the accepted cost of the settings decision. A hook is the only route that removes it without also
discarding the user's configuration: hooks run before every other step, and a hook's deny stands even in
`bypassPermissions` — with the allow-side limits noted in Route C above.

**What adopting it would change.** This is why it is a refactor and not a flag:

1. **A second inbound channel.** A hook is a separate short-lived process, not the child's pipe. It
   needs a way to reach the Giskard server (a loopback endpoint with a per-child token is the obvious
   shape) and to correlate its request with a thread and a live turn — routing the adapter currently
   gets for free from the pipe it owns.
2. **Approval identity moves.** `respond_approval` (`registry.rs:1023`) resolves a decision by
   taking the `ThreadId` it is given, looking up the loaded thread's binding, and calling the harness
   named by `binding.harness`. A hook-raised ask arrives from outside any harness and carries no
   binding to resolve through, so the registry needs a path that does not assume a `ThreadHandle`.
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

Structured diffs; MCP reload and OAuth; `terminate_command`; idle process reaping; `sdkMcpServers`;
**hook-based approval enforcement**
— the stdio channel is the MVP's only approval path, with the hook route deferred to a later decision
and refactor (§9.4); and **honouring a repository's own `.claude/settings.json`** — the `project` and
`local` settings scopes stay excluded, gated on the security review in §8.3, while the user scope is
loaded.

"v1" here means the MVP that milestone 4 (§11) makes reachable. Linked sub-agent child threads (§5.3) are **not** on this list: if
they are deferred to milestone 5 the transcript stays readable without them — but only if `SubagentLink`
is deferred too, since emitting it without `claim_native_thread` retries forever (milestone 4). Rename, archive and delete are not on it either — they are implemented, because
they are where this adapter stops a child process (§5.2).

---

## 11. Milestones

The work is cut into milestones that are each **one commit**. A milestone leaves `cargo fmt --all
--check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace
--locked` and `cargo deny check` green, carries its own tests, and syncs every document it affects, as
`AGENTS.md` requires. They land in order; each one's detailed plan, written for an implementing agent,
lives in `specs/claude-code-harness-plan/milestone-<n>-plan.md` and is written when the previous
milestone has merged, against the tree it left.

Nothing is user-visible before milestone 4: the crate exists and is tested from milestone 1, but no
`[harnesses.<name>]` declaration can name it until the kind is registered. That is deliberate. The
adapter is built bottom-up in units that can each be reviewed on their own, and the first reachable
version already has approvals, the catalog and the lifecycle methods, so no intermediate shape ships
with a preset that hangs or a thread that cannot be created.

| # | Milestone | Ships | Depends on |
| --- | --- | --- | --- |
| 1 | Crate, fixtures, output mapper | `giskard-harness-claude` as a workspace member with `claude-codes`; recorded and sanitized protocol fixtures; the pure mapper from stream-json frames to `AgentEvent`s and control replies, tested on the fixtures | nothing |
| 2 | Child supervisor and thread lifecycle | one `claude` process per thread; `open_thread` / resume / resume-fallback, `subscribe`, `start_turn` with attachments, `interrupt`, `shutdown`, `delete_thread`, `set_thread_archived`, `set_thread_name`; the catalog probe and provider report | 1 |
| 3 | Approvals, server requests, per-turn settings | `can_use_tool` ↔ `respond_approval` with the §9.3 mapping, `AskUserQuestion` and dialogs ↔ `respond_server_request`, the permission mode per turn, `set_model` / effort with read-back, `/compact` | 2 |
| 4 | Registration and documentation | `ClaudeCodeKind` in the server binary, `config.example.toml`, README, spec mapping section, the adapter README; `list_mcp_servers`; P8 hygiene and the per-harness filter of config-declared models. **The MVP becomes reachable here** | 3 |
| 5 | Sub-agent child threads | `--forward-subagent-text`, `SubagentLink` on the `Agent` item, `claim_native_thread` for `task:` ids, `parent_tool_use_id` routing, sub-agent approvals, `docs/subagents.md` | 4 |
| 6 | Supervisor hardening and idle reaping | the supervisor's in-flight control requests as a state the main loop drives (replacing milestone 3's polled `await_control`), then an adapter-level idle policy over children built on it, answering the design doc's open question | 4 |
| 7 | Synthesized diffs | `FileChange` / `DiffUpdated` from `Edit` / `Write` / `NotebookEdit` plus git | 4 |
| 8 | Drift and headroom surfacing | `system/init.capabilities` feature detection, a version-drift warning, and `rate_limit_event` headroom in the UI | 4 |

### Milestone 1 — crate, fixtures, output mapper

The crate skeleton (`Cargo.toml`, workspace membership, `deny.toml` unchanged since `claude-codes` is
Apache-2.0 with MSRV 1.85), a README that mirrors the Codex adapter's identifier and lifecycle
contract for what exists so far, and the `AGENTS.md` / root README crate lists.

**Fixtures are this milestone's first deliverable**, and they replace the old Phase 0. Each is a
sanitized stream-json transcript recorded against a real CLI with the exact argv in its metadata
file: the catalog probe, a plain text turn with partial messages, an allowed tool call, a denied one,
a session-scoped `AcceptForSession`, a `Cancel` (deny with `interrupt`), a foreground delegation, an
interrupted delegation, a `/compact`, a denied `ExitPlanMode`, a backgrounded shell command that
outlives its turn, and a failed `--resume`. They live in
`crates/giskard-harness-claude/tests/fixtures/` and every mapper test reads one of them, so a test's
input is a frame the CLI actually produced.

**The mapper** is a pure state machine like `CodexMapper`: one raw JSON line in, a list of outputs
out, no I/O. It owns the turn state a single child needs (active turn, item ids keyed by tool-use id
or message block, open agent tasks, the pending effective window) and produces `AgentEvent`s plus
the control replies that need no user (a denied `ExitPlanMode`, a `control_response` correlation).
It covers every frame in §3.2 and the `can_use_tool` and `AskUserQuestion` shapes in §9, including
the two invariants that fail silently when wrong: the agent-task gate on turn completion (§5.3) and
the three-summand input-token arithmetic (§6). Sub-agent frames with a `parent_tool_use_id` are
routed to a child route when one is claimed and dropped with a debug log otherwise; milestone 5
supplies the routes.

**Deliberately not in it:** no process, no `AgentHarness` implementation, no kind registration. The
remaining open verification (whether a mid-session `set_model` re-bases the auto-compact window) is
not a milestone; it needs a direct-provider machine and §6 does not depend on it.

### Milestone 2 — child supervisor and thread lifecycle

`ClaudeHarness` as the façade over `HashMap<ThreadId, ChildSession>`, one supervisor task per
child owning its stdin, its raw-line reader, its mapper and its retained `EventLog` (created at
open, so `subscribe` answers before the first frame). Giskard owns argv: the full §3.1 invocation is
built on `tokio::process::Command` directly, because `ClaudeCliBuilder` cannot emit
`--setting-sources`, `--effort`, `--forward-subagent-text`, `--include-partial-messages` or
`--replay-user-messages` (§3.7.1). The `initialize` handshake, `open_thread` for a fresh id and for
`--resume`, the same-id respawn when resume fails (§5.2), `start_turn` with inline attachments and
the encoded-size ceilings (§3.6), `interrupt`, `shutdown` that interrupts before it stops, and the
three lifecycle methods that stop a child. `list_models` from the freshest `initialize` or the probe
(§6), `list_providers` reporting `anthropic`. Capabilities as §4, except that `live_approvals`,
`plan_build_modes`, `per_turn_model`, `reasoning_effort` and `context_compaction` are reported only
from milestone 3, and `mcp_status` only from milestone 4, which wires `list_mcp_servers` over the
`mcp_status` control request. Verified while planning it: nothing reaches stdout before the first
user message, so the open handshake is the `initialize` control response; a missing transcript
makes `--resume` exit before answering it, so the fallback is decided at open; closing stdin lets
a running turn finish and exits the idle CLI; `get_context_usage` answers `maxTokens` at open, which
a resumed thread reports through `ThreadUpdate::ContextWindowRestored`. Milestone 2 is implemented
in `crates/giskard-harness-claude` (`ClaudeHarness`, `ClaudeLaunchOptions`).

Tested without a real CLI: a scripted fake `claude` (a small test binary in the crate that replays a
milestone-1 fixture and answers control requests) drives the supervisor in CI, the way the Codex
crate's `FakeCodexTransport` does.

### Milestone 3 — approvals, server requests, per-turn settings

`can_use_tool` → `ApprovalRequested` with the §9.3 decision mapping, including the
session-destination rewrite and its regression test, `Cancel` as a deny with `interrupt`, and the
degradation to a plain `Accept` when no rule suggestion is offered. `AskUserQuestion`,
`request_user_dialog` and elicitation → `ServerRequestReceived` / `respond_server_request`. The
permission mode per turn: `--permission-mode` at spawn, `set_permission_mode` at every turn start,
`--disallowedTools EnterPlanMode ExitPlanMode` (§8.2), presets per §8.1. `set_model` and
`apply_flag_settings{effortLevel}` with `get_settings` read-back (§3.3), which makes `TurnOverrides`
fully honoured. `compact_thread` as a `/compact` user message, with the degenerate `result` not
persisted as an assistant turn. `rate_limit_event` and `api_retry` → `Notice`. Milestone 3 is
implemented in `crates/giskard-harness-claude` and one dispatch line in `static/app.js`.

### Milestone 4 — registration and documentation

`ClaudeCodeKind` beside `CodexKind` in `bin/giskard-server.rs` (§5.1), with the startup tests
extended for a two-kind catalog; `config.example.toml` gains a `[harnesses.claude]` declaration;
README's *Supported harnesses* entry changes; `list_mcp_servers` is implemented over the
`mcp_status` control request (§3.3) and `mcp_status` is advertised; the spec gains a Claude Code
mapping section beside §4.6 and the §9.1 / §9.2.1 amendments (§8.2, §9.3);
`docs/api-endpoints.md` only gains the `claude-code` value of `kind` because no route moves; the
adapter README is completed. Two
server-side hygiene items land here because a second *kind* makes them observable: P8
(`resolve_reverse_subagent_target` filtered by `ThreadFile.harness`), and config-declared models
under a provider a harness does not report leaving that harness's picker group (today they are
offered with a warning that they cannot be routed). Stage 3 of `docs/multi-harness-design.md` is
**not** a prerequisite: verified while planning, the registry resolves every thread operation's
harness from the thread file's name and passes a detached handle for a cold thread, which the
adapter handles, so Stage 3 remains desirable plumbing. The picker already groups by declaration
name and `GET /api/harnesses` already carries `kind`, so no browser change and no screenshot
regeneration is expected. Milestone 4 is implemented: `ClaudeCodeKind` in `bin/giskard-server.rs`,
`list_mcp_servers` in the adapter, P8 and the per-harness model filter in the server.

### Milestone 5 — sub-agent child threads

`--forward-subagent-text` on every child, `SubagentLink` on the parent's `Agent` item,
`claim_native_thread` for `task:<tool_use_id>` ids, `subscribe` on a claimed child handle yielding
the frames whose `parent_tool_use_id` matches, sub-agent approvals routed by `agent_id` (§9.3),
`stop_task` as a sub-agent thread's `interrupt` (§3.3), and the `open_thread` refusal to ever
`--resume` a `task:` id (§5.3). `docs/subagents.md` gains the no-native-session model. The two
halves ship together, never one without the other. Its detailed plan is
`claude-code-harness-plan/milestone-5-plan.md`, written against milestone 4's tree and 2.1.287:
the mapper mints one route (a `ThreadId` and a retained log) per `Agent` call, the claim adopts
that id or binds a silent cold route for a session that is gone, and a killed sub-agent's trailing
frames are dropped in favour of its `Interrupted` status. Two fixtures recorded for it
(`subagent-stop`, `subagent-ask-withdrawn`) settled the ask ordering and the `stop_task` shape.
Milestone 5 is implemented: the routes in the adapter's mapper and supervisor, `claim_native_thread`
and `stop_task` in its façade, and no change to the server or `app.js`.

### Milestones 6 to 8 — polish

Supervisor hardening with idle reaping (§5.2, and the design doc's open question), synthesized
structured diffs (§6.1), and surfacing `system/init.capabilities`, `claude_code_version` drift and
subscription headroom in the UI (§12). Each is independent of the others and follows milestone 4.

**Milestone 6 has two halves that belong together.** Milestone 3's `await_control` waits for a
control response by pumping frames in 50 ms slices, which is a pragmatic answer rather than a
design: the supervisor's main loop is the one place that reads stdout, so a handler that needs a
response cannot await it directly. The first half makes the in-flight request a state the main
loop drives — a `TurnSetup` state for the per-turn settings (mode, model, effort, read-back, then
the write), with responses routed by the existing `ControlResponse` dispatch, no slices, and
`Stop` / shutdown handled as ordinary loop inputs. The second half, idle reaping, adds a timer as
one more loop input and must know what is in flight (a child is never reaped mid-handshake,
mid-settings or mid-answer), which is exactly the state the first half makes explicit. Milestones 4
and 5 do not build on the polling: 4 touches no supervisor code and 5 changes the mapper's routes
and the pending map's keys, not how responses are awaited. Its detailed plan is
`claude-code-harness-plan/milestone-6-plan.md`, written against the tree with MCP status per
thread merged: the hand-off becomes a `TurnSetup` the main loop advances and the stop sequence
a phase, so commands are refused at once while a child stops; a child idle for
`idle_shutdown_secs` (a key on the `claude-code` declaration, default 600 s, `0` never) is
stopped with its thread's log open and its entry kept, and the next message respawns it with
`--resume` under the open's own fallbacks. Idle means no turn, no sub-agent route, no open task
(`local_bash` included), no outstanding control request, no in-flight hand-off and no pending
ask. The server is not told and must not be: it reuses the binding and keeps reading the log.

Milestone 6 is implemented: the supervisor is one `select!` loop over stdout, commands, the
shutdown flag, the stage deadline and the idle timer, in a serving or a stopping phase; a
`StartTurn`'s settings are a `TurnSetup` advanced by its responses and failed by its deadline
(`await_control`, its polling and the deferred-command queue are gone); `stop_task` is a waiter; a
stop refuses arriving commands at once and joins a second stop. A child idle (no hand-off, ask,
route, open task, outstanding control request or turn) for `idle_shutdown_secs` is reaped: its
façade entry (session id, log, model) is kept, its log stays open, and the next `start_turn`,
`compact_thread` or `open_thread` respawns it through the open's own spawn path, a lost
transcript surfacing as a `Notice` after the turn's `TurnStarted`. See the adapter README.

### After milestone 5, as its own change — MCP status per thread

`list_mcp_servers` is instance-scoped (`AgentHarness::list_mcp_servers(&self)`, and the route
`GET /api/projects/{id}/harnesses/{name}/mcp` names a declaration, not a thread), which fits Codex's
one process per project. On this harness the configured server set is the same in every child
(`--setting-sources user`), but the state the panel shows (`pending`, `failed`) is per process, so
milestone 4's adapter asks whichever live child its map yields first, and does not fall back to a
probe when that child's request fails. The browser already opens the MCP menu from a displayed
thread. The change: `list_mcp_servers` takes an optional `ThreadHandle`, the route takes an optional
`thread` query parameter the browser sends from `loadMcpServers`, the Claude adapter asks that
thread's child and falls through to the probe otherwise (also when the child just exited), Codex
ignores the handle; `docs/api-endpoints.md` and the adapter README follow. It touches the trait,
the route and `app.js`, so it is one small commit of its own after milestone 5 merges, not part of
5 or 6. Its detailed plan is `claude-code-harness-plan/mcp-status-per-thread-plan.md`, written
against milestone 5's tree: the hint is resolved from the open thread's binding on the server (a
thread of another project is `404`, of another declaration `400`, one that is not open answers for
the instance), a sub-agent hint asks the child that carries it, no hint or a hint without a live
child asks the thread-less probe (never an arbitrary child), and a live child's failure is
returned rather than probed around. MCP status per thread is implemented; see
`claude-code-harness-plan/mcp-status-per-thread-plan.md`.

### Later, as its own decision — the hook route (§9.4)

Not scheduled here on purpose: it is a refactor of how an approval reaches the server (second
inbound channel, approval routing that does not assume a `ThreadHandle`, ephemeral hook
installation, cross-transport deduplication by `tool_use_id`), and it should be decided against a
working harness rather than designed in advance.

### Documentation to update

Much less than before: Stage 0 already rewrote spec §4.7 and §6.4 around instances, and Stage 2 took
the endpoint inventory. What this adapter still owns, by milestone:

- Milestone 1: `AGENTS.md` and the root README crate lists; the new
  `crates/giskard-harness-claude/README.md`, started with the identifier model and the mapper's
  contract.
- Milestone 4: `specs/giskard-specification.md` (a Claude Code mapping section beside §4.6; §9.1 and
  §9.2.1 amended for a per-thread process and plan mode as a permission mode); `README.md`'s
  *Supported harnesses* entry; `config.example.toml`'s `[harnesses.claude]` declaration; the adapter
  README completed, including that a `task:` native id is an item id and must never reach
  `--resume`.
- Milestone 5: `docs/subagents.md`, sub-agent threads without native sessions (§5.3), beside the
  Codex model.

---

## 12. Risks

| Risk | Mitigation |
| --- | --- |
| Protocol drift as Claude Code ships — **measured, not hypothetical** | Over ~50 patch releases the 2.1.2xx series moved a documented flag value to undocumented, added two permission modes, changed the headless default mode, and grew the control channel by roughly twenty subtypes. `claude-codes` (§3.7) helps — its version tracks the CLI and its enums tolerate unknown values — but enum tolerance does not protect a capability decided on a premise that has expired. **Read `system/init.capabilities` and feature-detect** rather than comparing version strings; log `claude_code_version` as diagnostics; **re-check §3 against the version the adapter ships against**. Drift cuts both ways: several of §3.3's subtypes *removed* planned work, so a re-check is as likely to simplify the plan as to complicate it |
| `ask_first` does not ask about everything — **two independent causes, both verified** | User settings allow-rules pre-empt the callback (accepted by the §8.3 decision), *and* the CLI approves effect-free commands itself below the settings layer, with nothing configured (§9.2.1). Not mitigated by design: the UI wording must match what the preset actually promises, and the hook route (§9.4) is the only fix for either |
| One process per loaded thread, **measured at 440–530 MB RSS** | `delete_thread` and `set_thread_archived(true)` release a child immediately (§5.2), which bounds the worst case but not the common one: a thread the user merely stops looking at keeps its process, because `retire_thread` / `forget_thread` are invisible to the harness. MVP logs the live-child count so growth is visible; since milestone 6 a child idle for `idle_shutdown_secs` (default 600 s) is reaped and respawned on the thread's next message. At the spec's ~10-thread scale this is gigabytes, so it is a capacity question, not a detail |
| **A `SubagentLink` without `claim_native_thread`** | Fails loudly but endlessly rather than silently: every delegation is re-queued by `defer_admission` with no attempt cap and warns once per driver event. Ship `SubagentLink` and the claim together, or neither (§5.3, milestone 5) |
| A stray `ANTHROPIC_API_KEY` in Giskard's own environment silently bills a subscriber to API credits | Not preventable *by Giskard*: `env` is an overlay and cannot unset an inherited variable (§7). Detect it — `initialize`'s `account:{subscriptionType, apiProvider}` at handshake, `system/init.apiKeySource` per turn. Prevention exists for the operator: the managed `allowedProviders` setting outranks every other scope (§7) |
| Cost/quota semantics differ under a subscription | Treat euro cost as notional; surface `rate_limit_event` (§6) |
| `full_access` fails to start when the server process runs as root (§8.1) | Outside the documented setup, but the raw failure is an opaque spawn error: detect the refusal and surface its cause |
| A checkout carries permission rules Giskard would otherwise honour | `project` and `local` scopes stay excluded (§8.3); only the machine owner's user-scope file is loaded. The exclusion covers `.mcp.json` as well as `settings.json` — a separate surface that would otherwise start a checkout's MCP servers unprompted |
| **A backgrounded delegation emits two `result` messages** (§5.3) | The highest-severity mapper hazard in this plan, because it fails silently: closing the turn on the first `result` persists "I have delegated this" and drops the work. Worse, whether a delegation backgrounds is decided per call, so a mapper that closes on the first `result` works intermittently. Keep the turn open until every **agent-type** task has a terminal `task_updated` — and no longer, since a backgrounded `local_bash` task legitimately outlives its turn. Cover both halves with the milestone 1 delegation and backgrounded-command fixtures |

---

## 13. Open questions

1. **Idle shutdown.** Answered by milestone 6 for this adapter: the declaration carries the timeout
   (`idle_shutdown_secs`), applied per process (§5.2). The Codex half of
   `docs/multi-harness-design.md`'s question stays open.
2. **Should a declaration be able to remove an inherited environment variable?** The `env` overlay can
   only add or overwrite (§7). For this harness that is the difference between "warn that the user is
   on API billing" and "make sure they are not".
3. **Is `destination: "session"` a supported contract or an implementation detail?** (§9.3.) It works
   and `claude-codes` types it, but the public guide shows only `localSettings`. Everything §9.3 says
   about `AcceptForSession` — and the decision not to keep approval state in Giskard — rests on it.
   Worth asking upstream rather than assuming: the answer is a sentence from someone who knows, and
   otherwise a regression test that fails one day without explanation.
4. **Should `ask_first` warn about pre-empting rules?** `list_permission_rules` (§3.3) makes it
   possible to tell the user, at thread open, which of their own allow-rules will bypass the ask that
   §8.3 promises. It does not make `ask_first` absolute — only the hook route (§9.4) does — but it
   turns a silent surprise into a stated one. Whether that belongs in the MVP is a product call.
5. **Does a subscription OAuth token work against `GET /v1/models`?** Not needed by this harness
   (§7.1) — the probe covers the catalog and the context window comes from the session. It matters
   only for a provider with no Claude Code behind it, which is `docs/multi-harness-design.md`'s
   question rather than this adapter's.
