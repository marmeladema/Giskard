# Recorded Claude Code protocol fixtures

Stream-json transcripts recorded against **Claude Code 2.1.286** by driving a real `claude -p` child
over a stdio pipe, for the milestone 1 mapper tests of
[`../../claude-code-harness-plan.md`](../../claude-code-harness-plan.md) (§11). Milestone 1 moves
this directory to `crates/giskard-harness-claude/tests/fixtures/`; until then it lives beside the
plan so the plan can cite it.

Each scenario has up to four files:

| File | Content |
| --- | --- |
| `<name>.out.jsonl` | every line the CLI wrote to stdout, in order, one JSON frame per line |
| `<name>.in.jsonl` | every line the recorder wrote to stdin, in order (user messages, control requests, control responses) |
| `<name>.meta.json` | the exact argv, the exit code, how many `result` frames arrived, how many `can_use_tool` asks were answered, and the wall time |
| `<name>.stderr.txt` | stderr, present only when the CLI wrote any |

Every scenario ran with `--model haiku`, `--permission-prompt-tool stdio`, `--setting-sources ""`,
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
  random and identify nothing.

`rate_limit_event` frames are kept as recorded: they carry utilization fractions and reset
timestamps, not identifiers.
