# Recorded Claude Code protocol fixtures

Stream-json transcripts recorded against **Claude Code 2.1.286** (the two `subagent-*`, the
`replay-compact`, the five `background-{stop,complete,taskstop,fail,stop-next-turn}`, the
`prompt-not-replayed` and the `mcp-status` scenarios against **2.1.287**, the two `forked-skill*`
against **2.1.289**) by driving a real
`claude -p` child over a stdio pipe, for the mapper tests of
[`specs/claude-code-harness-plan.md`](../../../../specs/claude-code-harness-plan.md) (§11). They
live in `crates/giskard-harness-claude/tests/fixtures/`, where the mapper tests read them.

Each scenario has up to four files:

| File | Content |
| --- | --- |
| `<name>.out.jsonl` | every line the CLI wrote to stdout, in order, one JSON frame per line |
| `<name>.in.jsonl` | every line the recorder wrote to stdin, in order (user messages, control requests, control responses) |
| `<name>.meta.json` | the exact argv, the exit code, how many `result` frames arrived, how many `can_use_tool` asks were answered, and the wall time |
| `<name>.stderr.txt` | stderr, present only when the CLI wrote any |

Every scenario ran with `--model haiku`, `--permission-prompt-tool stdio`, `--setting-sources ""`
(the `forked-skill*` scenarios: `project`, since the CLI loads no project skill without it),
a fresh `--session-id`, a throwaway working directory holding one file `data.txt` whose content is
`the magic number is 4271`, and an environment reduced to `PATH`, `HOME` and the variables that
carry authentication. The recorder answered every `can_use_tool` per the scenario's policy below and
closed stdin when the scenario's stop condition was met.

| Scenario | Mode | What it exercises | Policy / stop |
| --- | --- | --- | --- |
| `initialize` | manual | `initialize` and `list_models` control requests, no user message | stdin closed at once |
| `text-turn` | manual, `--include-partial-messages` | a one-word reply with the full `stream_event` sequence | 1 result |
| `tool-allowed` | manual | a `Bash` ask answered allow; `tool_use_result` with stdout/stderr | allow; 1 result |
| `tool-denied` | manual | the same ask answered deny; `tool_result_meta.non_execution_kind` and `permission_denials` | deny; 1 result |
| `accept-for-session` | manual | the ask's `addRules` suggestion echoed back with `destination: "session"`; three identical commands, one ask | session; 1 result |
| `cancel` | manual | deny with `interrupt: true`; the rejection tool result, the interruption text, and an `error_during_execution` result | cancel; 1 result |
| `delegation` | acceptEdits, `--forward-subagent-text` | a **foreground** delegation: `task_started` with `is_backgrounded: false`, the child's frames tagged with `parent_tool_use_id`, one `result` | allow; 1 result then 200 s |
| `delegation-interrupted` | manual, `--forward-subagent-text` | a **backgrounded** delegation interrupted while the child's tool ran: first `result`, then `task_updated` killed, and no second `result` | interrupt 2 s after the child's first tool call; 12 s quiet |
| `compact` | manual | a text turn, then `/compact`: `status compacting`, the re-emitted `init`, `compact_boundary`, and the degenerate `result` | 2 results |
| `plan-exit-denied` | plan | the plan file write, the `ExitPlanMode` ask answered deny, the plan in `permission_denials` | deny ExitPlanMode; 1 result |
| `background-bash` | manual | a `run_in_background` shell command: `task_started` of type `local_bash`, the turn's `result`, then the task's completion, a re-emitted `init` and a **second `result`** for the continuation | allow; 25 s quiet |
| `resume-missing` | manual, `--resume <unknown uuid>` | the failure shape: exit 1, `No conversation found` on stderr, an `error_during_execution` result with `num_turns: 0` | 1 result |
| `subagent-stop` | default, `--forward-subagent-text` | a **foreground** delegation whose sub-agent runs `touch marker.txt && sleep 120 && cat data.txt`: the sub-agent's `can_use_tool` (with `agent_id`, **before** the forwarded `tool_use` frame), answered allow; `stop_task` 4 s later → `task_updated` killed, the child's rejection `tool_result` and interruption marker, the parent's `Agent` `tool_result` with `is_error`, one `result` | allow; stop_task; 1 result then 6 s quiet |
| `subagent-ask-withdrawn` | default, `--forward-subagent-text` | the same delegation with `touch marker.txt && cat data.txt`, the sub-agent's ask **left pending**; `stop_task` 3 s later → `control_cancel_request` for the ask, then the same ending | no answer; stop_task; 1 result then 6 s quiet |
| `replay-compact` | manual, `--replay-user-messages` | a text turn, then `/compact`: the prompt replayed; the `/compact` line itself not, the compaction summary `isSynthetic: true, isReplay: false` and the `<local-command-stdout>` frame `isReplay: true`, as without the flag. Recorded with `--replay-user-messages`, which the adapter no longer passes: the mapper ignores its `isReplay` frames. | 2 results |
| `background-stop` | manual, `--replay-user-messages` | a background `Bash` command (`for i in 1 2 3 4 5 6; do echo line $i; sleep 5; done; echo finished`): the ask answered allow and **the answer echoed back on stdout** (line 9, an effect of the flag), `task_started` of type `local_bash` **before** the `tool_result` whose `tool_use_result.backgroundTaskId` names the same task, the turn's `result`; `stop_task` 7 s after `task_started` → `task_updated` killed, `task_notification` stopped with `output_file`, then the `{}` reply | allow; stop_task; 1 result |
| `background-complete` | manual, `--replay-user-messages` | the same command left to finish: `task_updated` completed, `task_notification` completed with `summary: "… completed (exit code 0)"` and `output_file`, then the CLI's own continuation turn (`init`, text, a second `result`) | allow; 2 results |
| `background-taskstop` | manual, `--replay-user-messages` | the same background command, then a foreground `sleep 7` and the model's own `TaskStop` tool on the background task: the foreground call's `task_started` with `is_backgrounded: false` and its `task_notification` with an empty `output_file` and **no** `task_updated` (lines 15–16); `TaskStop` → `task_updated` killed then `task_notification` stopped (lines 26–27), as for `stop_task` | allow; 1 result then 12 s quiet |
| `background-fail` | manual, `--replay-user-messages` | a background `sleep 4; echo oops >&2; exit 3`: `task_updated` failed then `task_notification` failed with `output_file` (lines 20–21), then the CLI's continuation turn | allow; 2 results |
| `background-stop-next-turn` | manual, `--replay-user-messages` | a background `sleep 60`, `stop_task` once the turn ended (`killed`, `stopped`, `{}`; no continuation turn), then a second message: after its `init`, the CLI first replays the queued `<task-notification>` as a user frame with `isReplay: true`, a string `content` and `origin: {"kind": "task-notification"}` (line 19), then the message itself (line 22). Recorded with `--replay-user-messages`, which the adapter no longer passes: the mapper ignores its `isReplay` frames. | 2 results |
| `prompt-not-replayed` | manual, `--replay-user-messages` | a prompt quoting `` `<task-notification>` `` inline: the CLI answers it but replays nothing (no `isReplay` frame); which prompts the CLI does not replay is in the adapter README, *Process control*, **Why not `--replay-user-messages`** ([`../../README.md`](../../README.md)). Recorded with `--replay-user-messages`, which the adapter no longer passes: the mapper ignores its `isReplay` frames. | 1 result |
| `mcp-status` | manual, `--mcp-config` naming a minimal stdio server (`mini`: one tool, one resource, one template, written for the recording) and a broken one, `--strict-mcp-config` | `initialize`, then `mcp_status` answering a **connected** entry (`serverInfo`, `tools: [{name, annotations}]`) beside a failed one (`error`, no `serverInfo`, no `tools`), then one text turn whose `init` lists `mcp_servers` and the `mcp__mini__echo` tool. The recorder's stdin was reconstructed after the fact; the paths are rewritten | 1 result |
| `forked-skill` | default, `--forward-subagent-text`, `--setting-sources project`; the working directory also holds `.claude/skills/magic/SKILL.md` (`context: fork`: "Read data.txt in the current directory with the Read tool and reply with the magic number only.") | a skill run as a **forked** sub-agent: the `Skill` call, `task_started` of type `local_agent` naming it (`description: "/magic"`, the skill's text as `prompt`), the sub-agent's prompt, `Read` and `Bash` calls and answer tagged with the `Skill` call's id, then `task_notification` completed and **no** `task_updated`, the call's `tool_result` (`tool_use_result.status: "forked"`) and one `result` | no asks; 1 result then 8 s quiet |
| `forked-skill-stop` | as `forked-skill`, the skill telling the sub-agent to run `sleep 60 && cat data.txt` | `stop_task` on the forked task while its sub-agent's command ran (the CLI blocked the `sleep` chain and the sub-agent ran it in the background): `task_updated` killed then `task_notification` stopped for the forked task (lines 17–18), the `{}` answer, the sub-agent's trailing `[Request interrupted by user]`, its background command killed too, the `Skill` `tool_result` with `is_error` and `non_execution_kind: interrupted` (line 24); then the model ran the skill again, a second forked task that completed on its notification alone, and one `result` | stop_task 3 s after the sub-agent's first `Bash` call; 1 result then 8 s quiet |
| `autocompact-state` | — | the two top-level frames `active_goal` and `autocompact_state` a session emits before its first turn in some environments; two frames only, recorded separately with an environment that sets autocompact overrides | — |

## Sanitization

The frames are the CLI's, with these rewrites so nothing environment-specific reaches the
repository:

- the working directory is `/work/project`, its encoded form is `-work-project`, and the config
  directory is `/home/user/.claude`;
- thinking `signature` values are `REDACTED` (the real value embeds an account identifier);
- API `request_id` values on assistant frames are `req_REDACTED`;
- the `system/init` inventories `tools`, `slash_commands`, `terminal_slash_commands`, `skills`,
  `plugins`, `agents`, `memory_paths` and `messaging_socket_path` are replaced with neutral values,
  and the `initialize` response's `commands` and `agents` lists are trimmed to built-ins. Nothing
  the mapper reads is among them;
- session ids, message ids, tool-use ids, task ids and frame uuids are the recorded ones. They are
  random and identify nothing;
- a background command's output file lives under the CLI's temporary directory,
  `/tmp/claude-<uid>/<encoded cwd>/…`. `background-taskstop` and `background-fail` keep that
  prefix (`/tmp/claude-1000`); `background-stop` and `background-complete` read
  `/home/user/.claude/…` there instead, an artefact of their sanitization.

`rate_limit_event` frames are kept as recorded: they carry utilization fractions and reset
timestamps, not identifiers.
