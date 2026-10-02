#!/bin/sh
# A stand-in for `claude -p --input-format stream-json --output-format stream-json`, for the
# adapter's real-process tests. It replays the recorded fixtures next to it and needs only `sh`,
# `sed`, `grep`, `cut` and `cat`. Behaviour by argv and environment:
#
# - `--resume <id>`: the missing-transcript failure (stderr sentence, the recorded result, exit 1);
#   with `FAKE_CLAUDE_RESUME_OK=1`, a successful resume that behaves as a fresh child.
# - `--permission-mode bogus`: the commander usage error, exit 1.
# - `--permission-mode bypassPermissions` with `FAKE_CLAUDE_REFUSE_BYPASS=1`: the root refusal
#   sentence on stderr, exit 1.
# - `FAKE_CLAUDE_STDERR_FLOOD=1`: 50 stderr lines of 1000 characters, then exit 0.
# - otherwise one stdin line at a time: `initialize`, `get_settings` (echoing the model and effort
#   it was last told), `get_context_usage`, `interrupt`, `stop_task` (an empty success),
#   `mcp_status` (no servers) and `rename_session` control requests are answered;
#   `set_permission_mode` echoes the mode (`bypassPermissions` on a child not launched with it is
#   the `bypass_not_launched` error); `set_model` succeeds for a model of the `initialize` fixture's
#   catalog, else the `catalog_unknown` error; `apply_flag_settings` succeeds and remembers
#   `effortLevel`. A user message containing `touch` replays the `tool-allowed` frames up to its
#   `can_use_tool` ask, and the rest once a control response answers it; any other user message
#   replays the `text-turn` frames (`FAKE_CLAUDE_EXIT_MID_TURN=<n>`: only the first n, then exit 3).
#   EOF exits 0 (`FAKE_CLAUDE_IGNORE_EOF=1`: sleeps 30 s instead).
# - `--replay-user-messages` (every session child carries it): each stdin `user` line is echoed
#   back with `"isReplay":true` and a fixed `uuid` before that turn's frames, and each stdin
#   `control_response` line is echoed back verbatim, as the CLI does.

fixtures="$(dirname "$0")/fixtures"
model=""
resume=""
mode=""
replay=""
previous=""
for argument in "$@"; do
    [ "$argument" = "--replay-user-messages" ] && replay=1
    case "$previous" in
        --model) model="$argument" ;;
        --resume) resume="$argument" ;;
        --permission-mode) mode="$argument" ;;
    esac
    previous="$argument"
done

if [ -n "$FAKE_CLAUDE_STDERR_FLOOD" ]; then
    padding=""
    i=0
    while [ $i -lt 100 ]; do
        padding="${padding}xxxxxxxxxx"
        i=$((i + 1))
    done
    i=1
    while [ $i -le 50 ]; do
        echo "$i $padding" >&2
        i=$((i + 1))
    done
    exit 0
fi

if [ "$mode" = "bogus" ]; then
    echo "error: option '--permission-mode <mode>' argument 'bogus' is invalid." >&2
    exit 1
fi

if [ "$mode" = "bypassPermissions" ] && [ -n "$FAKE_CLAUDE_REFUSE_BYPASS" ]; then
    echo "--dangerously-skip-permissions cannot be used with root/sudo privileges for security reasons" >&2
    exit 1
fi
launch_mode="$mode"
# The `tool-allowed` fixture's line holding its `can_use_tool` ask.
ask_line=$(grep -n '"type": "control_request"' "$fixtures/tool-allowed.out.jsonl" | cut -d: -f1)
effort="null"
asked=""

if [ -n "$resume" ] && [ -z "$FAKE_CLAUDE_RESUME_OK" ]; then
    echo "No conversation found with session ID: $resume" >&2
    cat "$fixtures/resume-missing.out.jsonl"
    exit 1
fi

respond() {
    printf '{"type":"control_response","response":{"subtype":"success","request_id":"%s","response":%s}}\n' "$1" "$2"
}

refuse() {
    printf '{"type":"control_response","response":{"subtype":"error","request_id":"%s","error":"%s","error_code":"%s"}}\n' "$1" "$2" "$3"
}

# The value of one string field of the request line.
field() {
    printf '%s\n' "$line" | sed -n 's/.*"'"$1"'":"\([^"]*\)".*/\1/p'
}

while IFS= read -r line; do
    request_id=$(printf '%s\n' "$line" | sed -n 's/.*"request_id":"\([^"]*\)".*/\1/p')
    case "$line" in
        *'"subtype":"initialize"'*)
            sed -n '1s/"request_id": "[^"]*"/"request_id": "'"$request_id"'"/p' \
                "$fixtures/initialize.out.jsonl"
            ;;
        *'"subtype":"get_settings"'*)
            respond "$request_id" '{"applied":{"model":"'"$model"'","effort":'"$effort"'},"effective":{},"sources":[]}'
            ;;
        *'"subtype":"set_permission_mode"'*)
            requested=$(field mode)
            if [ "$requested" = "bypassPermissions" ] && [ "$launch_mode" != "bypassPermissions" ]; then
                refuse "$request_id" "Cannot set permission mode to bypassPermissions because the session was not launched with --dangerously-skip-permissions" bypass_not_launched
            else
                respond "$request_id" '{"mode":"'"$requested"'"}'
            fi
            ;;
        *'"subtype":"set_model"'*)
            requested=$(field model)
            if grep -q '"value": "'"$requested"'"' "$fixtures/initialize.out.jsonl"; then
                model="$requested"
                printf '{"type":"control_response","response":{"subtype":"success","request_id":"%s"}}\n' "$request_id"
            else
                refuse "$request_id" "Model '$requested' not found" catalog_unknown
            fi
            ;;
        *'"subtype":"apply_flag_settings"'*)
            effort='"'"$(field effortLevel)"'"'
            printf '{"type":"control_response","response":{"subtype":"success","request_id":"%s"}}\n' "$request_id"
            ;;
        *'"subtype":"get_context_usage"'*)
            respond "$request_id" '{"totalTokens":0,"maxTokens":200000,"percentage":0,"categories":[]}'
            ;;
        *'"subtype":"interrupt"'*)
            respond "$request_id" '{"still_queued":[]}'
            ;;
        *'"subtype":"stop_task"'*)
            respond "$request_id" '{}'
            ;;
        *'"subtype":"mcp_status"'*)
            respond "$request_id" '{"mcpServers":[]}'
            ;;
        *'"subtype":"rename_session"'*)
            printf '{"type":"control_response","response":{"subtype":"success","request_id":"%s"}}\n' "$request_id"
            ;;
        *'"type":"control_response"'*)
            [ -n "$replay" ] && printf '%s\n' "$line"
            if [ -n "$asked" ]; then
                asked=""
                sed -n "$((ask_line + 1)),\$p" "$fixtures/tool-allowed.out.jsonl"
            fi
            ;;
        *'"type":"user"'*)
            if [ -n "$replay" ]; then
                printf '%s\n' "$line" \
                    | sed 's/^{/{"isReplay":true,"uuid":"00000000-0000-4000-8000-0000000000aa","session_id":"f18693ff-2d11-4f87-9556-2b527e19e081","parent_tool_use_id":null,/'
            fi
            case "$line" in
                *touch*)
                    asked=1
                    sed -n "1,${ask_line}p" "$fixtures/tool-allowed.out.jsonl"
                    continue
                    ;;
            esac
            if [ -n "$FAKE_CLAUDE_EXIT_MID_TURN" ]; then
                sed '/"type": "control_response"/d' "$fixtures/text-turn.out.jsonl" \
                    | sed -n "1,${FAKE_CLAUDE_EXIT_MID_TURN}p"
                echo "fake-claude: exiting mid-turn" >&2
                exit 3
            fi
            sed '/"type": "control_response"/d' "$fixtures/text-turn.out.jsonl"
            ;;
    esac
done

if [ -n "$FAKE_CLAUDE_IGNORE_EOF" ]; then
    exec sleep 30
fi
exit 0
