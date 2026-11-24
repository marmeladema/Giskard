#!/usr/bin/env bash
# Build the E2E image and the replay server it runs against, then check the two work together.
#
# The replay server is built on the host with cargo, unless GISKARD_E2E_PREBUILT_BIN names an
# existing binary to use instead. Its path is written to <binary-output-directory>/resolved-path.

set -euo pipefail

if [[ "$#" -ne 3 ]]; then
  echo "usage: build-image.sh <repository-root> <image> <binary-output-directory>" >&2
  exit 2
fi

repo_root="$1"
image="$2"
binary_dir="$3"
prebuilt_bin="${GISKARD_E2E_PREBUILT_BIN:-}"
resolved_path_file="$binary_dir/resolved-path"

if [[ ! -d "$binary_dir" ]]; then
  echo "binary output directory does not exist: $binary_dir" >&2
  exit 1
fi

echo "==> Building Playwright runtime image $image"
docker build -t "$image" "$repo_root/tests/e2e"

if [[ -n "$prebuilt_bin" ]]; then
  if [[ ! -f "$prebuilt_bin" ]]; then
    echo "GISKARD_E2E_PREBUILT_BIN is not a regular file: $prebuilt_bin" >&2
    exit 1
  fi
  if [[ ! -x "$prebuilt_bin" ]]; then
    echo "GISKARD_E2E_PREBUILT_BIN is not executable: $prebuilt_bin" >&2
    exit 1
  fi
  echo "==> Using pre-built replay server: $prebuilt_bin"
  resolved_bin="$(cd "$(dirname "$prebuilt_bin")" && pwd -P)/$(basename "$prebuilt_bin")"
else
  echo "==> Building replay server (release)"
  # Asked of cargo rather than assumed, so a configured target directory is honoured.
  resolved_bin="$(
    cd "$repo_root" &&
      cargo build --release --locked -p giskard-server --bin giskard-server-replay \
        --message-format=json-render-diagnostics |
      sed -n 's/.*"executable":"\([^"]*giskard-server-replay\)".*/\1/p' | tail -n 1
  )"
  if [[ -z "$resolved_bin" || ! -x "$resolved_bin" ]]; then
    echo "cargo did not report a giskard-server-replay executable" >&2
    exit 1
  fi
fi

# The binary runs in the image, not on the host it was built on: a host with a newer glibc than the
# image's Ubuntu release can produce one that does not start there. Fail here, before any test.
echo "==> Validating replay server in the Playwright runtime"
docker run --rm \
  --entrypoint /e2e/suite/validate-replay-binary.sh \
  -v "$resolved_bin:/usr/local/bin/giskard-server-replay:ro" \
  -v "$repo_root/tests/e2e:/e2e/suite:ro" \
  "$image"

printf '%s\n' "$resolved_bin" >"$resolved_path_file"
