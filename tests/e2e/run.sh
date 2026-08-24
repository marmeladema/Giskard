#!/usr/bin/env bash
# Build the e2e image and run the Playwright suite in a container. No Node/npm/Rust needed on the
# host — only Docker.
#
# Usage:
#   tests/e2e/run.sh                       # run the whole suite
#   tests/e2e/run.sh tests/login.spec.ts   # run one spec
#   tests/e2e/run.sh --headed              # (does nothing useful; container is headless)
#   tests/e2e/run.sh --reporter=line       # any extra `playwright test` args pass through
#
# The HTML report is written to tests/e2e/playwright-report/ on the host.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
image="${GISKARD_E2E_IMAGE:-giskard-e2e}"
binary_dir="$(mktemp -d)"
trap 'rm -rf "$binary_dir"' EXIT

"$repo_root/tests/e2e/build-image.sh" "$repo_root" "$image" "$binary_dir"
binary_path="$(<"$binary_dir/resolved-path")"

echo "==> Running Playwright tests"
# --ipc=host avoids Chromium crashing on small /dev/shm. The report volume surfaces results on the
# host even when tests fail.
mkdir -p "$repo_root/tests/e2e/playwright-report"
docker run --rm --ipc=host \
  -v "$binary_path:/usr/local/bin/giskard-server-replay:ro" \
  -v "$repo_root/tests/e2e/playwright-report:/e2e/playwright-report" \
  "$image" "$@"
