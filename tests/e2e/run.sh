#!/usr/bin/env bash
# Build the replay server and the e2e image, then run the Playwright suite in a container. The host
# needs Docker and a Rust toolchain; Node, npm and the browsers stay inside the image.
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
# --ipc=host avoids Chromium crashing on small /dev/shm. The suite is mounted rather than baked into
# the image, so edits to it need no rebuild; the HTML report lands in it, on the host, even when
# tests fail.
docker run --rm --ipc=host \
  --user "$(id -u):$(id -g)" \
  -e GISKARD_PLAYWRIGHT_OUTPUT_DIR=/tmp/giskard-playwright-test-results \
  -v "$binary_path:/usr/local/bin/giskard-server-replay:ro" \
  -v "$repo_root/tests/e2e:/e2e/suite" \
  "$image" "$@"
