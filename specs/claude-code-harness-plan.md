# Claude Code Harness Support — Design and Implementation Plan

This note plans a second agent harness for Giskard: Anthropic's **Claude Code CLI**, driven through
its `--print --input-format stream-json --output-format stream-json` protocol, authenticated by the
user's **Claude Pro/Max subscription** (not an API key).

It is a planning note, not an authoritative product spec. Once the direction is agreed, the final
contract folds into `specs/giskard-specification.md` (§4, §6.4, §8, §9, §12.2, §13.5) and
`crates/giskard-harness-claude/README.md`.

Protocol facts below were verified empirically against **Claude Code 2.1.233** (live stream-json
session, plus the shipped CLI binary's own schemas and argv construction). Anything not verified is
marked **[spike]**.

---

## 1. Decisions already taken

| Decision | Choice |
| --- | --- |
| Harness binding granularity | **Per thread inside a project.** One project may hold Codex threads and Claude threads side by side. |
| What selects the harness | **The thread's model.** A thread whose `ModelRef.provider` belongs to an Anthropic/Claude-Code provider runs on the Claude harness. No separate harness selector in the UI. |
| Child-process model | **One persistent `claude` child process per loaded thread**, alive across turns. Spawned only when a thread with an Anthropic model is loaded. **No idle reaping in the MVP.** |
| Structured diffs | **`structured_diffs: false` in v1.** Synthesize `FileChange`/`DiffUpdated` from `Edit`/`Write` tool calls + git in a later phase. |
| Live approvals | **Open — see §9.** Evidence is now in hand; the decision is scope/phasing. |

---

## 2. The two harnesses are shaped differently

| | Codex (today) | Claude Code (planned) |
| --- | --- | --- |
| Transport | stdio newline-delimited **JSON-RPC** | stdio newline-delimited **JSON objects** (two overlaid channels: transcript messages, and `control_request`/`control_response`) |
| Protocol crate | `codex-codes` 0.143.2 (typed, versioned) | **none exists for Rust** — Giskard owns the wire types |
| Processes | **1 `codex app-server` per project**, multiplexing every thread | **1 `claude` per session**; a session ≈ one Giskard thread |
| Native thread id | Codex-minted rollout id | **client-minted UUID** via `--session-id`; resumed with `--resume=<uuid>` |
| Concurrency | one worker task fans out to N threads | N independent children, each single-threaded through its own turn |
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
       --permission-prompt-tool stdio            # only if live approvals are adopted (§9)
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
| `system/status`, `system/task_summary`, `system/post_turn_summary` | activity/labels; `post_turn_summary` carries `status_category` + `status_detail` |
| `system/permission_denied` | a denial with `decision_reason_type` (`rule`/`mode`/`classifier`/…) → `Notice` |
| `system/commands_changed` | slash-command inventory (large; elide from logs) |

### 3.3 The control channel (verified)

Client → CLI, as `{"type":"control_request","request_id":…,"request":{…}}`:

- `initialize` — accepts `hooks`, `sdkMcpServers`, `systemPrompt`, `appendSystemPrompt`,
  `planModeInstructions`, `toolAliases`, `supportedDialogKinds`. Answered with the command/agent
  inventory. **Verified working.**
- `interrupt` — **verified working**; response `{"still_queued":[]}`. This is `AgentHarness::interrupt`.
- `set_permission_mode`, `set_model` — present in the CLI's control dispatch, so **mode and model can
  change without respawning the child** [spike: confirm request shapes].
- `control_cancel_request` — cancels an in-flight control request.

CLI → client:

- `can_use_tool` — the permission ask (§9).
- `request_user_dialog` / `elicitation` — MCP elicitation and host dialogs → maps to Giskard's
  existing `ServerRequestReceived` / `respond_server_request` path.

---

## 4. Capability matrix

| Capability | Claude | Basis |
| --- | --- | --- |
| `live_approvals` | **true (pending §9 decision)** | `can_use_tool` control request; response `{behavior:"allow",updatedInput?,updatedPermissions?}` \| `{behavior:"deny",message?,interrupt?}` |
| `plan_build_modes` | **true** | `--permission-mode plan` + `set_permission_mode`. Semantics differ — see §8. |
| `per_turn_model` | **true** | `set_model` control request; fallback = respawn with `--resume --model` |
| `reasoning_effort` | **true** | `--effort low\|medium\|high\|xhigh\|max` (`Effort` is already an open string newtype, so the differing value set costs nothing) |
| `structured_diffs` | **false (v1)** | no native diff feed |
| `resumable_threads` | **true** | `--session-id` / `--resume`, cwd-scoped |
| `model_listing` | **true (static)** | no RPC; adapter returns a built-in catalog of Claude models |
| `token_usage` | **true** | `result.usage` |
| `mcp_status` | **true (read-only)** | `system/init.mcp_servers` |
| `mcp_reload` | **false (v1)** | only the interactive `/mcp reconnect` |
| `mcp_oauth_login` | **false** | interactive only |
| `context_compaction` | **true** | `/compact` as a user message [spike]; `autocompact_state` feeds the gauge |
| Native rename / archive / delete | **unsupported** | no equivalents — see §5.5 |
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
runs on the harness of its current model's provider" needs no new UI. `ProjectConfig.harness` stays
as the project's **default** for new threads (and for `list_models` overlay), no longer as a hard
binding.

### 5.2 Registry re-keying

`crates/giskard-server/src/registry.rs:391` holds `HashMap<ProjectId, Arc<dyn AgentHarness>>` and
`get_or_create_harness(project, config)` (`:490`). Both become harness-aware:

```rust
harnesses: HashMap<(ProjectId, HarnessKind), Arc<dyn AgentHarness>>
async fn get_or_create_harness(&self, project, kind, config) -> …
```

`ThreadBinding` (`:217`) gains the `HarnessKind` that opened it, so every existing lookup path
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
projects and takes the requested kind.

### 5.4 Switching a thread across harnesses

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

### 5.5 Operations the Claude harness cannot do

`harness_api_error` (`routes.rs:3549`) maps `HarnessError::Unsupported` → **400**, and
`set_thread_name` / `set_thread_archived` / `delete_thread` call the harness *first* (spec TN2/TD3,
`registry.rs:990–1072`). On a Claude thread, renaming would therefore fail with a 400 and never touch
local state. Fix: treat `Unsupported` from these three lifecycle calls as a **soft** path — log at
`debug`, proceed with the local mutation, and (for delete) skip only the native step. This is a
server change, not an adapter workaround, and it needs error-path tests per `AGENTS.md`.

### 5.6 Process lifecycle (MVP)

- Spawn on `open_thread`, one child per thread, `--session-id <fresh uuid>` or `--resume=<stored>`.
- cwd = the thread's worktree if it has one, else the project workspace root; `--add-dir` for extra
  writable roots (the analogue of Codex's `runtimeWorkspaceRoots`).
- Keep alive across turns. **No idle reaping in the MVP** — `harness.idle_shutdown_secs` is declared
  in config but implemented nowhere today, and the MVP does not change that. Document the cost
  honestly: one Node/Bun process (~150–300 MB RSS) per *loaded* Claude thread, so ~10 open threads is
  a real memory line item. Reaping is the first post-MVP follow-up.
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
  which is exactly what §10.3 wants.
- **Tokens.** `TokenUsage { input, output, total }` from `result.usage`, with
  `input = input_tokens + cache_creation_input_tokens + cache_read_input_tokens`. Document the
  consequence: with `tokens.cost_estimation = true`, flat per-Mtok rates **overstate** cost, because
  cache reads bill at a fraction. For a subscription user the euro figure is notional anyway.
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
- If the child reports unauthenticated, fail the thread open with a message naming the fix, as §12.2
  already requires for Codex.

---

## 8. Presets and Plan mode

| Giskard preset | `--permission-mode` | Notes |
| --- | --- | --- |
| `ask_first` | `manual` | only meaningful with live approvals (§9); without them it has nothing to ask |
| `auto_approve` | `acceptEdits` | edits proceed, escalations still ask |
| `full_access` | `bypassPermissions` | needs `--allow-dangerously-skip-permissions`; the CLI warns that in this mode `can_use_tool` is never consulted |

**Plan mode collapses the orthogonality.** In Codex, Plan/Build is collaboration mode only and is
orthogonal to the preset (spec §9.1). In Claude Code, `plan` *is* a permission mode, so Plan + preset
occupy one slot. Contract: Plan wins — a Plan-mode turn sends `--permission-mode plan` / 
`set_permission_mode plan` regardless of preset, and the preset applies again in Build. Spec §9.1
must say this explicitly for harnesses without `plan_build_modes` independence.

**Do not let the user's local allow-rules silently pre-approve.** The CLI's own warning is explicit:
allow rules from settings files and bare names in `--allowedTools` are applied *before* the
permission callback and are invisible to it. So an `ask_first` Giskard thread could execute a command
without ever asking, purely because `~/.claude/settings.json` allows it. Run children with a
controlled `--setting-sources` (and document what is honoured) so the preset means what the UI says.

---

## 9. Live approvals — the open decision, with evidence

**What the CLI actually implements** (from its own argv construction and schemas):

- Passing **`--permission-prompt-tool stdio`** routes every permission ask to the client as
  `control_request` / `can_use_tool`. The SDK sets exactly this flag when a `canUseTool` callback is
  supplied, and rejects combining it with a real MCP prompt tool ("canUseTool callback cannot be used
  with permissionPromptToolName").
- The ask carries `tool_name`, `input`, `tool_use_id`, `title`, and `permission_suggestions`
  (the "always allow …" candidates).
- The response schema is
  `{behavior:"allow", updatedInput?, updatedPermissions?}` | `{behavior:"deny", message?, interrupt?}`.
- Not answering has a defined failure mode ("tool permission stream closed before response
  received"), so a dropped response fails the tool call rather than hanging forever.

**This maps onto Giskard's existing approval model almost exactly:**

| `ApprovalDecision` | Claude response |
| --- | --- |
| `Accept` | `{behavior:"allow"}` |
| `AcceptForSession` | `{behavior:"allow", updatedPermissions:[…from permission_suggestions]}`, or Giskard-side session memory — §9.2.1 already defines "session" as harness-process lifetime, fail-closed on respawn, which is precisely a child's lifetime |
| `Decline` | `{behavior:"deny", message:"Declined"}` |
| `Cancel` | `{behavior:"deny", interrupt:true}` — the `interrupt` flag is Codex's Cancel semantics |
| `AcceptWithExecPolicyAmendment` | no analogue — do not advertise it in `available` |

`ApprovalKind` mapping: `Bash` → `CommandExecution{command,cwd}`; `Edit`/`Write`/`NotebookEdit` →
`FileChange{path,change}`; `mcp__<server>__<tool>` → `McpToolCall{server,tool_name}`; everything else
→ `Permission{detail}`. `title` and `permission_suggestions` fill `ApprovalMetadata`.

**Why my live probe did not see an ask** (so the evidence is not contradictory): this container
pre-grants Bash, and per the CLI's own warning, settings allow-rules are consulted *before* the
callback. That is a property of the sandbox, not of the protocol — and it is the same hazard §8
already flags.

**Residual risk:** the `can_use_tool` request/response field names above come from the shipped
binary's schemas, not from a round trip Giskard performed. A half-day spike on a machine with no
pre-granted Bash rules would confirm the wire shape and the deny/interrupt behaviour.

**Recommendation:** ship v1 with `live_approvals: false` (presets only, UI degrades per §13.5) **and**
run the spike in parallel, then turn the capability on in the same phase as the mapper hardening.
That keeps the first milestone small without designing the approval path out of the architecture.

---

## 10. Not in v1

Structured diffs; native rename/archive/delete; MCP reload and OAuth; `terminate_command`; linked
sub-agent child threads; idle process reaping; using Claude Code's own hooks or `sdkMcpServers`.

---

## 11. Phasing

Each phase carries the `AGENTS.md` obligations: `cargo fmt`/`clippy -D warnings`, error-path tests,
structured logs at new boundaries, and doc sync in the same change.

**Phase 0 — spikes (≈1 day).** Confirm `can_use_tool` end-to-end on a clean permission environment;
confirm `set_model` / `set_permission_mode` request shapes; confirm `/compact` over stream input;
confirm interrupt mid-tool-call. Capture sanitized transcripts as test fixtures.

**Phase 1 — multi-harness plumbing (no Claude yet).** `HarnessKind`; `ProviderConfig.harness`;
`ThreadFile.harness` + `harness_thread_ids` (with default-on-read migration); registry re-keying and
binding-based routing; harness-scoped `find_thread_by_harness_id`; dispatching `HarnessFactory`; soft
`Unsupported` handling for rename/archive/delete. Provable entirely with `ReplayHarness` +
`giskard-server-replay` — a second replay instance registered under a different kind gives a real
two-harness test without any CLI.

**Phase 2 — `giskard-harness-claude` MVP.** New crate + README. Wire types, child supervisor, mapper
(`assistant`/`stream_event`/`user`/`result` → items and turns), `open_thread`/`start_turn`/
`subscribe`/`interrupt`/`shutdown`, token usage, `ContextWindowUpdated`, static `list_models`,
capability set from §4 with `live_approvals: false`. Mapper unit tests off Phase-0 fixtures.

**Phase 3 — approvals + model/mode switching.** `can_use_tool` ↔ `ApprovalRequested`, the decision
mapping in §9, `set_model` / `set_permission_mode`, elicitation → `ServerRequestReceived`,
`rate_limit_event` → `Notice`, `/compact`.

**Phase 4 — cross-harness UX.** Model-picker filtering by provider→harness, the switch confirmation
and `Notice` of §5.4, capability-driven UI checks, screenshot regeneration if the picker changes
(`tests/e2e/screenshots.sh`).

**Phase 5 — polish.** Idle reaping (make `idle_shutdown_secs` real), synthesized `FileChange`/
`DiffUpdated`, version-drift warning surfaced in the UI.

**Docs to update:** `specs/giskard-specification.md` (§4.1/4.2 capability wording, new §4.8 Claude
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
| Local settings allow-rules undercut `ask_first` | Controlled `--setting-sources`; document exactly what is honoured (§8) |
| One process per loaded thread | MVP accepts it and documents the cost; reaping in Phase 5 |
| Cross-harness model switch loses agent context | Explicit confirm + `Notice` + remembered native ids (§5.4) |
| Cost/quota semantics differ under a subscription | Treat euro cost as notional; surface `rate_limit_event` (§6) |
| Registry re-keying touches approval/interrupt/delete routing | Phase 1 is harness-agnostic and fully testable with two replay harnesses before any CLI is involved |

---

## 13. Open questions

1. **§9** — `live_approvals` in v1, or Phase 3 as recommended?
2. Should the **project-level** default harness still be offered at project creation, or is the
   model picker the only place a harness is ever chosen?
3. Do Claude threads need `--add-dir` fed from anything beyond the workspace root (Codex reads
   `sandbox_workspace_write.writable_roots` from its own config for this)?
