#!/bin/sh
# A stand-in for `claude -p --input-format stream-json --output-format stream-json`, for the
# adapter's real-process tests. It replays the recorded fixtures next to it and needs only `sh`,
# `sed` and `cat`. Behaviour by argv and environment:
#
# - `--resume <id>`: the missing-transcript failure (stderr sentence, the recorded result, exit 1).
# - `--permission-mode bogus`: the commander usage error, exit 1.
# - `FAKE_CLAUDE_STDERR_FLOOD=1`: 50 stderr lines of 1000 characters, then exit 0.
# - otherwise one stdin line at a time: `initialize`, `get_settings`, `get_context_usage`,
#   `interrupt` and `rename_session` control requests are answered; a user message replays the
#   `text-turn` frames (`FAKE_CLAUDE_EXIT_MID_TURN=<n>`: only the first n, then exit 3); EOF exits
#   0 (`FAKE_CLAUDE_IGNORE_EOF=1`: sleeps 30 s instead).

fixtures="$(dirname "$0")/fixtures"
model=""
resume=""
mode=""
previous=""
for argument in "$@"; do
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

if [ -n "$resume" ]; then
    echo "No conversation found with session ID: $resume" >&2
    cat "$fixtures/resume-missing.out.jsonl"
    exit 1
fi

respond() {
    printf '{"type":"control_response","response":{"subtype":"success","request_id":"%s","response":%s}}\n' "$1" "$2"
}

while IFS= read -r line; do
    request_id=$(printf '%s\n' "$line" | sed -n 's/.*"request_id":"\([^"]*\)".*/\1/p')
    case "$line" in
        *'"subtype":"initialize"'*)
            sed -n '1s/"request_id": "[^"]*"/"request_id": "'"$request_id"'"/p' \
                "$fixtures/initialize.out.jsonl"
            ;;
        *'"subtype":"get_settings"'*)
            respond "$request_id" '{"applied":{"model":"'"$model"'","effort":null},"effective":{},"sources":[]}'
            ;;
        *'"subtype":"get_context_usage"'*)
            respond "$request_id" '{"totalTokens":0,"maxTokens":200000,"percentage":0,"categories":[]}'
            ;;
        *'"subtype":"interrupt"'*)
            respond "$request_id" '{"still_queued":[]}'
            ;;
        *'"subtype":"rename_session"'*)
            printf '{"type":"control_response","response":{"subtype":"success","request_id":"%s"}}\n' "$request_id"
            ;;
        *'"type":"user"'*)
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
