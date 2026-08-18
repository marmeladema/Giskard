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
| Settings sources for child processes | **`--setting-sources user`** (§8): the user's own `~/.claude/settings.json` applies, so extra writable roots and personal rules are configured where the user already keeps them. Project and local scopes stay excluded. The accepted cost is that a user allow-rule can pre-approve a call `ask_first` would otherwise have asked about. |
| Live approvals | **Supported and verified end to end (§9).** MVP uses the `--permission-prompt-tool stdio` channel, in the adapter's first working milestone. The hook route is postponed to a later decision and refactor (§9.4); the MCP-tool route is rejected (§9.1). |

---

## 2. The two harnesses are shaped differently

| | Codex (today) | Claude Code (planned) |
| --- | --- | --- |
| Transport | stdio newline-delimited **JSON-RPC** | stdio newline-delimited **JSON objects** (two overlaid channels: transcript messages, and `control_request`/`control_response`) |
| Protocol crate | `codex-codes` 0.143.2 (typed, versioned) | **none exists for Rust** — Giskard owns the wire types |
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
2. **Resume is cwd-scoped.** A thread using a per-thread Git worktree
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
`--setting-sources` decides exactly which files reach it. Under the chosen `user` scope (§8) the
machine owner's file contributes and a checkout's files do not, while authentication is unaffected
either way because it is not settings-schema state at all.

It is a flat file of many top-level keys rather than a few grouped sections. The ones that matter to a
Giskard adapter:

| Key | Relevance |
| --- | --- |
| `permissions` | `allow` / `deny` / `ask` rule lists, **`additionalDirectories`**, `defaultMode`, `disableBypassPermissionsMode` — the whole permission surface §8 and §9 operate on |
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

So Giskard can hand a child exactly the configuration it intends, while ignoring every settings file on
the machine. Two consequences:

- **`--add-dir` has a settings-level equivalent**, `permissions.additionalDirectories`, verified above.
  Either mechanism works; the flag is simpler for a fixed list, the payload is better if Giskard ever
  needs to send permission state and directories together.
- **The hook route's open mechanical question is answered** (§9.4): a per-child `--settings` payload is
  a viable way to install a hook ephemerally, without writing to the user's settings files.

**There is no provider registry.** Unlike Codex — where `[[providers]]` and `model_providers` name
endpoints and wire APIs — Claude Code selects its backend entirely through **environment variables**:
`CLAUDE_CODE_USE_BEDROCK`, `CLAUDE_CODE_USE_VERTEX`, `CLAUDE_CODE_USE_FOUNDRY` for first-party clouds,
and `ANTHROPIC_BASE_URL`, `ANTHROPIC_AUTH_TOKEN`, `ANTHROPIC_CUSTOM_HEADERS`, `ANTHROPIC_MODEL`,
`ANTHROPIC_SMALL_FAST_MODEL` for a gateway or proxy. These can be set in the child's environment
directly or through the `env` key of a `--settings` payload.

This shapes what a Giskard "provider" means for this harness. For Codex a provider is a per-request
routing choice; for Claude Code it is **a property of how the child process was launched**. A
subscription-backed `anthropic` provider and a hypothetical `bedrock` provider would both have
`harness = "claude"` and differ only in the environment their children receive — which fits §5.1's
provider→harness lookup without changing it, but means the provider must be resolved at spawn time and
cannot change within a live child. Only the *model* can (§3.3). Nothing in the MVP needs this; it is
recorded so the provider field is not mistaken for something the protocol carries.

---

## 4. Capability matrix

| Capability | Claude | Basis |
| --- | --- | --- |
| `live_approvals` | **true (verified)** | `can_use_tool` control request; response `{behavior:"allow",updatedInput?,updatedPermissions?}` \| `{behavior:"deny",message?,interrupt?}`. Round trip and blocked execution confirmed — §9. |
| `plan_build_modes` | **true (verified)** | `--permission-mode plan` + `set_permission_mode`, which echoes the applied mode. Semantics differ — see §8. |
| `per_turn_model` | **true (verified)** | `set_model` mid-session, no respawn (§3.3) |
| `reasoning_effort` | **true** | `--effort low\|medium\|high\|xhigh\|max` (`Effort` is already an open string newtype, so the differing value set costs nothing) |
| `structured_diffs` | **false (v1)** | no native diff feed |
| `resumable_threads` | **true** | `--session-id` / `--resume`, cwd-scoped |
| `model_listing` | **true (static)** | no RPC; adapter returns a built-in catalog of Claude models |
| `token_usage` | **true** | `result.usage` |
| `mcp_status` | **true (read-only)** at the harness level | `system/init.mcp_servers` lists servers and status. The project-scoped MCP *endpoints* stay Codex-only in v1 (§5.4) |
| `mcp_reload` | **false (v1)** | only the interactive `/mcp reconnect` |
| `mcp_oauth_login` | **false** | interactive only |
| `context_compaction` | **true** | `/compact` as a user message [unverified]; `autocompact_state` feeds the gauge |
| Native rename / archive / delete | **unsupported** | no equivalents — see §5.6 |
| `terminate_command` | **unsupported (v1)** | background shells are controlled by the agent's own `KillShell` tool, not from outside |
| Linked sub-agent threads | **unsupported** | Claude `Task` subagents are not resumable sessions, so `SubagentLink.harness_thread_id` has no value to carry. Map them as `ToolCall` items; optionally nest their text with `--forward-subagent-text` + `parent_tool_use_id`. |

---

## 5. Multi-harness architecture

### 5.1 Harness identity comes from the provider

Add an optional `harness` field to `ProviderConfig` (`crates/giskard-persist/src/config.rs`),
defaulting to `"codex"` so every existing `config.toml` keeps working:

```toml
[[providers]]
id = "anthropic"
name = "Anthropic (Claude Code)"
harness = "claude"          # new
wire_api = "anthropic"
model_listing = false
  [[providers.models]]
  id = "claude-opus-5"
  display_name = "Opus 5"
  context_window = 1000000
  supports_reasoning_effort = true
```

Then `harness_for(model: &ModelRef, config) -> HarnessKind` is a pure lookup, and the rule "a thread
runs on the harness of its current model's provider" needs no new UI — the model picker is already the
only place the choice is expressed, since project creation asks for a `default_model` and nothing else.

`ProjectConfig.harness` is therefore redundant for routing. It exists today but is hardcoded to
`"codex"` at creation (`store.rs:578`) and never chosen by anyone; under this design it would carry at
most the project's default and a label for warnings. Whether to keep or drop it is open question 1.

### 5.2 Registry re-keying

`crates/giskard-server/src/registry.rs:391` holds `HashMap<ProjectId, Arc<dyn AgentHarness>>` and
`get_or_create_harness(project, config)` (`:490`). Both become harness-aware:

```rust
harnesses: HashMap<(ProjectId, HarnessKind), Arc<dyn AgentHarness>>
async fn get_or_create_harness(&self, project, kind, config) -> …
```

`ThreadBinding` (`:178`) gains the `HarnessKind` that opened it, so every existing lookup path
resolves the right instance rather than "the project's harness":

- `start_turn`, `interrupt`, `compact_thread`, `terminate_command` — from the binding;
- `respond_approval` (`:709`) and `respond_server_request` (`:745`) — via the `ApprovalId → ThreadId`
  and `ServerRequestId → ThreadId` maps, then the binding. These already route by thread, so they
  need the binding's kind, not a new map;
- project delete / shutdown — must iterate **every** kind for that project, not one entry;
- `find_thread_by_harness_id` (`routes.rs:560`) — must compare `(harness, harness_thread_id)`, since
  a Claude UUID and a Codex rollout id live in the same field and must not alias.

`HarnessFactory::create` takes `(kind, &ProjectConfig, cwd)` and the binary's factory dispatches
`"codex" | "claude"` (`bin/giskard-server.rs:14` currently rejects anything but `"codex"`).

### 5.3 Persistence

`ThreadFile` (`crates/giskard-persist/src/store.rs:61`) gains:

```rust
#[serde(default = "default_harness")]           // "codex" for every existing file
pub harness: String,
/// Native thread id per harness, so switching a thread's model across harnesses and back
/// resumes the original native session instead of orphaning it.
#[serde(default, skip_serializing_if = "HashMap::is_empty")]
pub harness_thread_ids: HashMap<String, String>,
```

`harness_thread_id` stays as the **active** harness's id (no migration, no reader changes), with
`harness_thread_ids` as the archive. `store.rs:578` stops hardcoding `harness: "codex"` for new
projects and derives it from the project's `default_model` instead, since creation asks for a model and
never for a harness (§5.1).

### 5.4 Project-scoped harness queries become ambiguous

§5.2 covers the thread-addressed operations. The other half of the registry is **project-scoped** and
silently assumes a project has exactly one harness:

| Call site | Today |
| --- | --- |
| `registry.capabilities(&project_config)` (`routes.rs:3374`, `:3425`, `:3474`, `:3518`) | capabilities of *the* project harness |
| `registry.list_models(&project_config)` (`routes.rs:3393`) | catalog overlay for `GET /api/projects/{id}/models` |
| `registry.list_mcp_servers` / `reload_mcp_servers` / `start_mcp_oauth_login` | MCP endpoints, per project |

With two harnesses in one project each needs a defined rule:

- **Capabilities must become thread-scoped.** The capability-driven UI (spec §13.5) decides what to
  render — approval cards, the effort selector, the diff viewer — and those answers now differ between
  two threads of the same project. A project-level capability answer would be wrong for one of them.
  Resolving capabilities through the thread's harness (and, on a draft, through the model being
  selected) is the correct shape; the project-level endpoints keep a project answer only where they
  genuinely describe the project.
- **Model catalogs merge rather than choose.** Each harness's catalog should overlay only the models
  whose provider maps to it, so a project offering both Codex and Claude models gets accurate metadata
  for both instead of one harness's view of the other's models.
- **MCP endpoints need an explicit decision**, deferred with the rest of MCP (§10): Claude's MCP status
  is per-child and read-only, so the honest v1 answer is that the MCP endpoints describe the Codex
  harness and report nothing for Claude threads.

**A consequence for process lifecycle.** `capabilities()` currently calls `get_or_create_harness`, so
answering it *spawns* the harness. Under the MVP's rule — a child process only when a thread with an
Anthropic model is loaded (§1) — merely opening a project's model picker must not start a `claude`
process. This is why the Claude harness is a **façade**: the `Arc<dyn AgentHarness>` registered for a
project answers `capabilities()` and `list_models()` from static knowledge, and spawns child processes
only in `open_thread`. Creating the façade must stay free.

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

`harness_api_error` (`routes.rs:3549`) maps `HarnessError::Unsupported` → **400**, and
`set_thread_name` / `set_thread_archived` / `delete_thread` call the harness *first* (spec TN2/TD3,
`registry.rs:990–1072`). On a Claude thread, renaming would therefore fail with a 400 and never touch
local state. Fix: treat `Unsupported` from these three lifecycle calls as a **soft** path — log at
`debug`, proceed with the local mutation, and (for delete) skip only the native step. This is a
server change, not an adapter workaround, and it needs error-path tests per `AGENTS.md`.

### 5.7 Process lifecycle (MVP)

- Spawn on `open_thread`, one child per thread, `--session-id <fresh uuid>` or `--resume=<stored>`.
- cwd = the thread's worktree if it has one, else the project workspace root; `--add-dir` for extra
  writable roots (the analogue of Codex's `runtimeWorkspaceRoots`).
- Threads of one project may run **concurrently in the same cwd** with no coordination between children
  (§3.4); the adapter needs no cross-thread locking, only per-thread turn serialization, which the
  server's existing `ThreadTurnGate` already provides.
- Keep alive across turns. **No idle reaping in the MVP** — `harness.idle_shutdown_secs` is declared
  in config but implemented nowhere today, and the MVP does not change that. The cost is larger than a
  guess would suggest: a `claude` process was **measured at 440–530 MB RSS**, so Giskard's ~10-thread
  target scale (spec §1.4) implies multiple gigabytes of resident memory if every thread is loaded.
  Reaping is therefore the first post-MVP follow-up, and the MVP should at least log the count of live
  children so the growth is visible before it becomes a complaint.
- Child exit while a turn is live → `TurnCompleted{Failed}` + `Error`, thread marked disconnected,
  same recovery UX as a Codex app-server crash.
- Record `claude_code_version` from `system/init` and warn when it differs from the pinned tested
  version — the same drift guard the spec already mandates for Codex, and more important here
  because Giskard owns the wire types.

---

## 6. Models, context windows, tokens, cost

- **Catalog.** `list_models` returns a built-in list (`claude-opus-5`, `claude-sonnet-5`,
  `claude-haiku-4-5`, …) with display names and the `--effort` levels. `models.rs:159`
  (`apply_harness_metadata`) already overlays names/efforts by model id and deliberately ignores
  harness context windows — keep that; windows come from `[[providers.models]]` and from runtime
  events.
- **Context window.** Emit `ContextWindowUpdated` from `autocompact_state.effective_window`, falling
  back to `result.modelUsage[<model>].contextWindow`. This is the effective, post-headroom number,
  which is exactly what the spec's context gauge (§10.3) wants.
- **Tokens.** `TokenUsage { input, output, total }` from `result.usage`, with
  `input = input_tokens + cache_creation_input_tokens + cache_read_input_tokens`. Document the
  consequence: with `tokens.cost_estimation = true`, flat per-Mtok rates **overstate** cost, because
  cache reads bill at a fraction. For a subscription user the euro figure is notional anyway.
- **Ancillary models pollute `by_model`.** `result.modelUsage` always carries a Haiku entry alongside
  the selected model, because Claude Code runs its own summaries/titles on a small model. Observed on
  every turn, including a Sonnet-only one. Decide deliberately: attribute the turn to the selected
  model (matching `Turn.model` and the Codex ledger's meaning) and either fold the ancillary usage
  into the same entry or record it under its own `(provider, model)` key. Dropping it makes Giskard's
  totals disagree with Anthropic's; hiding it under the selected model makes per-model rates wrong.
  Recommendation: record each `modelUsage` entry under its real model id, so `by_model` stays truthful,
  and keep `Turn.model` as the user's selection.
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

## 8. Presets and Plan mode

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

**Plan mode collapses the orthogonality.** In Codex, Plan/Build is collaboration mode only and is
orthogonal to the preset (spec §9.1). In Claude Code, `plan` *is* a permission mode, so Plan + preset
occupy one slot. Contract: Plan wins — a Plan-mode turn sends `--permission-mode plan` / 
`set_permission_mode plan` regardless of preset, and the preset applies again in Build. Spec §9.1
must say this explicitly for harnesses without `plan_build_modes` independence.

**Do not let the user's local allow-rules silently pre-approve.** The CLI's own warning is explicit:
allow rules from settings files and bare names in `--allowedTools` are applied *before* the
permission callback and are invisible to it. So an `ask_first` Giskard thread could execute a command
without ever asking, purely because `~/.claude/settings.json` allows it.

**Decision: children run with `--setting-sources user`.** The user's own
`~/.claude/settings.json` applies; the project and local scopes do not. This mirrors how Giskard
already treats Codex, whose adapter reads the user's `~/.codex` configuration for
`sandbox_workspace_write.writable_roots` — the machine's owner configures their agent where they
already configure it, and Giskard does not grow a parallel setting for the same thing. It also answers
where extra writable roots come from (§3.5, open question 2) with no Giskard-side surface at all.

**The accepted cost, stated plainly.** User-scope `permissions.allow` rules are evaluated *before*
`can_use_tool`, so a user who has allowed something for their own CLI use gets it pre-approved inside
Giskard too — an `ask_first` thread can then execute that command without asking. This was observed
directly: the first probe in this investigation saw no ask at all because the surrounding environment's
settings already allowed `Bash`. `ask_first` therefore means "ask unless *you* have already said
otherwise", not "ask always".

That is a coherent contract for a single-user, self-hosted tool — it is the user's machine and the
user's rules — but it is weaker than Codex's, where the adapter takes one field from the user's config
and Giskard's preset still drives every approval decision. Two consequences follow:

- the preset descriptions in the UI should not promise more than this;
- the hook route (§9.4) is the only mechanism that would let a user keep their personal rules *and*
  have `ask_first` be absolute, which raises its value relative to when it was postponed.

**Excluded on purpose: `project` and `local` scopes.** A repository is untrusted input, and those two
scopes are the ones a repository can carry:

Extending to them is a separate decision that must not be taken without answering, at minimum:

- **The agent can write the file it is governed by.** `.claude/settings.json` lives inside the
  workspace the agent has write access to, so enabling project settings creates a path where an agent
  grants itself permissions by editing a file. Needs answering: does Claude Code re-read settings
  within a running session or only at startup; does a rule written during a turn take effect in that
  turn, the next turn, or the next thread; and can Giskard's presets be made to win regardless.
- **A repository is untrusted input.** A cloned repo can ship a permissive `.claude/settings.json`, and
  Claude Code's own defence against this — the workspace trust dialog — is documented as **skipped in
  non-interactive mode**, which is the only mode Giskard uses. Enabling project settings would adopt a
  stranger's permission rules with no prompt anywhere in the flow.
- **Per-thread worktrees multiply it.** Each worktree carries its own copy of the file, so "which
  settings are in force" becomes per-thread rather than per-project.

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
  to see the ask at all. Under the chosen `user` scope (§8) this is an accepted limit of `ask_first`,
  not a defect — but it is why the preset cannot be described as "always asks".
- **`permission_suggestions` is typed and carries a `destination`.** Echoing a `localSettings`
  suggestion back **writes a permanent rule into the user's project** — observed, see §9.3 — which
  Giskard must never do as a side effect of one approval card. Hence the destination invariant in
  §9.3: rewrite every suggestion to `session` before returning it.

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

**Invariant: always override the destination to `session`; never forward a persistent one.**
`localSettings`, `projectSettings` and `userSettings` all write permission rules to disk on the user's
machine. An approval click means "let this proceed for now", never "change my configuration
permanently", so the adapter must rewrite the destination on every suggestion it echoes and drop any it
cannot rewrite.

The trap is specific and easy to walk into: the ask's suggestions arrive with
`destination: "localSettings"`, so forwarding them **unchanged** — the obvious one-liner — writes to the
user's repository. That is how the write above was produced. It is also pointless under the chosen
`user` scope: `localSettings` writes land in the project's `.claude/settings.local.json`, which is a
scope Giskard does not load, so the file is written and never read. With per-thread worktrees each
worktree would accumulate its own copy.

A regression test belongs here: approve for session, then assert both that the repeat call does not ask
**and** that no settings file appeared under the workspace.

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
evaluated first, and anything they approve never reaches the callback. That is the §8 hazard: an
`ask_first` thread can execute a command without asking, because the user once allowed it in their own
`~/.claude/settings.json`. Since the MVP deliberately loads that file (§8), this is not hypothetical —
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

Adopting it would also let `ask_first` become absolute *without* reverting the §8 decision — the user
keeps their `settings.json` and Giskard stops being pre-empted by it. That combination is the strongest
argument for eventually taking this route.

**Precedent to copy from when the time comes:**
[claude-remote-approver](https://github.com/yuuichieguchi/claude-remote-approver) (hook → ntfy → phone,
answering `{"behavior":"allow"|"deny"}` on stdout) is the same shape as hook → Giskard → browser.

**Trigger for revisiting:** the first time an `ask_first` thread executes something the user expected to
be asked about. Under §8 that is a foreseeable report rather than a surprise, so the trigger is less
"if" than "when someone minds".

---

## 10. Not in v1

Structured diffs; native rename/archive/delete (Giskard applies all three locally instead, §5.6);
MCP reload and OAuth; `terminate_command`; linked
sub-agent child threads; idle process reaping; `sdkMcpServers`; **hook-based approval enforcement**
— the stdio channel is the MVP's only approval path, with the hook route deferred to a later decision
and refactor (§9.4); and **honouring a repository's own `.claude/settings.json`** — the `project` and
`local` settings scopes stay excluded, gated on the security review in §8, while the user scope is
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
| **P1** | **Soft `Unsupported` for `set_thread_name` / `set_thread_archived` / `delete_thread`** (§5.6) | **Resolves a contradiction inside the current design.** `AgentHarness` declares these optional — its default implementations return `Unsupported` — while the server turns `Unsupported` into a user-visible HTTP 400 (`routes.rs:3549`). The trait says "may be absent", the server says "must exist". Nothing trips it today (Codex implements all three; `ReplayHarness` overrides them with `Ok`), so this is a consistency fix rather than a bug fix — but it is the contract that decides whether a harness can decline an operation at all. | — |
| **P2** | **Thread-scoped capabilities** (§5.4) | **Corrects the shape, before it has consequences.** Capabilities belong to the harness serving a thread, not to a project; today the two coincide, so there is no user-visible symptom — which is precisely why it is cheap now and expensive once a project can hold two harnesses. The capability-driven UI (spec §13.5) is the consumer. | — |
| **P3** | **`HarnessKind` newtype** replacing the bare `String` on `ProjectConfig.harness`, `config.toml`, and the factory | One place parses and validates a harness name instead of string comparisons scattered across the binary and the store. Pure typing; no behaviour change. | — |
| **P4** | **`ProviderConfig.harness` + `harness_for(&ModelRef, &Config)`** (§5.1) | Additive config field defaulting to `"codex"`; every existing `config.toml` keeps working and the lookup returns `codex` for everything. Establishes provider→harness as the single source of truth before anything depends on it. | P3 |
| **P5** | **Dispatching `HarnessFactory`** — a table keyed by `HarnessKind` instead of `bin/giskard-server.rs:19`'s `if config.harness != "codex"` | Turns a hardcoded rejection into an extension point, and lets the replay binary register its own kind by the same mechanism the real binary uses. | P3 |
| **P6** | **Registry re-keying to `(ProjectId, HarnessKind)`** plus the `HarnessKind` on `ThreadBinding` (§5.2) | The structural centre of the work, and the riskiest to combine with adapter development. Landing it alone keeps behaviour identical with one kind while making the two-harness test possible. | P3, P5 |
| **P7** | **`ThreadFile.harness` + `harness_thread_ids`, default-on-read** (§5.3) | A forward-compatible persistence migration. Landing it early means existing installations are already writing files that carry the field before any feature reads it, so the Claude work never needs a migration step of its own. | P3 |
| **P8** | **Harness-scoped `find_thread_by_harness_id`** (`routes.rs:560`) | Hardening: the lookup compares an opaque native id with no notion of which harness minted it. Harmless today, wrong the moment two id namespaces share the field. | P6, P7 |

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

New crate + README. Wire types, child supervisor, mapper
(`assistant`/`stream_event`/`user`/`result` → items and turns), `open_thread`/`start_turn`/
`subscribe`/`interrupt`/`shutdown`, **`can_use_tool` ↔ `ApprovalRequested` with the §9 decision
mapping**, token usage, `ContextWindowUpdated`, static `list_models`, capability set from §4. Mapper
unit tests off Phase-0 fixtures, including a denial that must not be reported as an executed-and-failed
tool call.

### Phase 3 — the rest of the control channel

`set_model` / `set_permission_mode` (per-turn model and
mode without respawning), elicitation / `request_user_dialog` → `ServerRequestReceived`,
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
(one-process-per-project claim at line 60, crate list, setup); `config.example.toml` (provider
`harness` key, `[[providers]]` block for Anthropic); `docs/subagents.md` (state that linked children
are Codex-only); `AGENTS.md` (9 crates); new `crates/giskard-harness-claude/README.md` mirroring the
Codex adapter's identifier/lifecycle contract.

---

## 12. Risks

| Risk | Mitigation |
| --- | --- |
| Giskard owns unversioned wire types; Claude Code ships often | Pin a tested version, log `claude_code_version`, warn on drift, keep the mapper tolerant of unknown message types (log at `debug`, never fail a turn) |
| User settings allow-rules pre-empt `ask_first` — **observed**, and now **accepted** by the §8 decision | Not mitigated by design: the UI wording must match what the preset actually promises, and the hook route (§9.4) is the only fix that keeps the user's settings *and* an absolute `ask_first` |
| One process per loaded thread, **measured at 440–530 MB RSS** | MVP accepts the cost and logs the live-child count so growth is visible; reaping in Phase 5. At the spec's ~10-thread scale this is gigabytes, so it is a capacity question, not a detail |
| Cross-harness model switch loses agent context | Explicit confirm + `Notice` + remembered native ids (§5.5) |
| Cost/quota semantics differ under a subscription | Treat euro cost as notional; surface `rate_limit_event` (§6) |
| Registry re-keying touches approval/interrupt/delete routing | Phase 1 lands it separately from any adapter work and proves it with two replay harnesses, so a regression there cannot be confused with a protocol bug |
| `full_access` fails to start when the server process runs as root (§8) | Outside the documented setup, but the raw failure is an opaque spawn error: detect the refusal and surface its cause |
| A checkout carries permission rules Giskard would otherwise honour | `project` and `local` scopes stay excluded (§8); only the machine owner's user-scope file is loaded |

---

## 13. Open questions

1. Does `ProjectConfig.harness` still earn its place? Project creation never asked for a harness — it
   takes a `default_model` and the field is hardcoded to `"codex"` (`store.rs:578`). Once the harness
   is derived from a thread's model provider (§5.1), the field is redundant for routing and survives
   only as the project's default and as a label in warnings (`routes.rs:3387`). Keep it as a
   derived-and-persisted default, or drop it and derive everything from `default_model`?

