#!/usr/bin/env bash
# Build the E2E image, preferring an explicitly supplied replay binary when it exists.

set -euo pipefail

if [[ "$#" -ne 3 ]]; then
  echo "usage: build-image.sh <repository-root> <image> <binary-output-directory>" >&2
  exit 2
fi

repo_root="$1"
image="$2"
binary_dir="$3"
dockerfile="$repo_root/tests/e2e/Dockerfile"
prebuilt_bin="${GISKARD_E2E_PREBUILT_BIN:-}"
resolved_bin="$binary_dir/giskard-server-replay"
resolved_path_file="$binary_dir/resolved-path"

if [[ ! -d "$binary_dir" ]]; then
  echo "binary output directory does not exist: $binary_dir" >&2
  exit 1
fi

echo "==> Building Playwright runtime image $image"
docker build --target e2e-runtime -f "$dockerfile" -t "$image" "$repo_root"

if [[ -n "$prebuilt_bin" && -e "$prebuilt_bin" ]]; then
  if [[ ! -f "$prebuilt_bin" ]]; then
    echo "GISKARD_E2E_PREBUILT_BIN is not a regular file: $prebuilt_bin" >&2
    exit 1
  fi
  if [[ ! -x "$prebuilt_bin" ]]; then
    echo "GISKARD_E2E_PREBUILT_BIN is not executable: $prebuilt_bin" >&2
    exit 1
  fi

  echo "==> Using pre-built replay server: $prebuilt_bin"
  prebuilt_dir="$(cd "$(dirname "$prebuilt_bin")" && pwd -P)"
  resolved_bin="$prebuilt_dir/$(basename "$prebuilt_bin")"
elif [[ -n "$prebuilt_bin" ]]; then
  echo "==> Pre-built replay server not found at $prebuilt_bin; building it in Docker"
else
  echo "==> No pre-built replay server supplied; building it in Docker"
fi

if [[ ! -e "$resolved_bin" ]]; then
  docker build \
    --target replay-binary \
    --output "type=local,dest=$binary_dir" \
    -f "$dockerfile" \
    "$repo_root"
fi

echo "==> Validating replay server in the Playwright runtime"
docker run --rm \
  --entrypoint /e2e/validate-replay-binary.sh \
  -v "$resolved_bin:/usr/local/bin/giskard-server-replay:ro" \
  "$image"

printf '%s\n' "$resolved_bin" >"$resolved_path_file"
