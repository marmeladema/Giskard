#!/usr/bin/env bash
# Validate the mounted replay binary against the exact E2E runtime and its HTTP startup path.

set -euo pipefail

server_bin=/usr/local/bin/giskard-server-replay
check_dir=/tmp/giskard-prebuilt-check
check_log=/tmp/giskard-prebuilt-check.log

if [[ ! -x "$server_bin" ]]; then
  echo "replay binary is not executable inside the E2E container" >&2
  exit 1
fi

GISKARD_BIND=127.0.0.1:18787 GISKARD_DATA_DIR="$check_dir" \
  "$server_bin" >"$check_log" 2>&1 &
server_pid="$!"

cleanup() {
  kill "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
  rm -rf "$check_dir" "$check_log"
}
trap cleanup EXIT

for _attempt in $(seq 1 40); do
  if node -e "fetch('http://127.0.0.1:18787/', { signal: AbortSignal.timeout(250) }).then(() => process.exit(0)).catch(() => process.exit(1))"; then
    exit 0
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    break
  fi
  sleep 0.1
done

cat "$check_log" >&2
echo "replay binary is incompatible with the E2E image" >&2
exit 1
