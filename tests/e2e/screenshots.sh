#!/usr/bin/env bash
# Regenerate the README screenshots (desktop + mobile, IDE theme) in a container. Like run.sh, this
# needs only Docker on the host — no Node/npm/browsers.
#
# Output: PNGs are written to docs/screenshots/ in the repo (ide-desktop.png, ide-mobile.png).
#
# Usage:
#   tests/e2e/screenshots.sh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
image="${GISKARD_E2E_IMAGE:-giskard-e2e}"
out_dir="$repo_root/docs/screenshots"

echo "==> Building $image (context: $repo_root)"
docker build -f "$repo_root/tests/e2e/Dockerfile" -t "$image" "$repo_root"

echo "==> Generating screenshots into $out_dir"
mkdir -p "$out_dir"
# The image's entrypoint is `npx playwright test`; select the screenshots config and point its
# output at the bind-mounted directory. --ipc=host keeps Chromium happy on small /dev/shm.
docker run --rm --ipc=host \
  -e SCREENSHOT_DIR=/out \
  -v "$out_dir:/out" \
  "$image" --config=screenshots.config.ts "$@"

echo "==> Done. Screenshots:"
ls -1 "$out_dir"
