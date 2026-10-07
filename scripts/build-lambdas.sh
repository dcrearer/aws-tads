#!/usr/bin/env bash
# Build all workspace Lambda binaries for arm64 Linux inside a container, then
# extract them to target/lambda/<name>/bootstrap where CDK reads them.
#
# macOS cannot cross-compile these crates (zig cannot link aws-lc-sys/ring); this
# builds natively on Linux instead. On a Linux host you can skip this and run
# `cargo lambda build --release --arm64` directly.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

CONTAINER_CMD="${CONTAINER_CMD:-$(command -v podman || command -v docker)}"
IMAGE="tads-lambda-build"

echo "==> Building Lambda binaries in container ($CONTAINER_CMD)..."
"$CONTAINER_CMD" build -f Dockerfile.lambda -t "$IMAGE" .

echo "==> Extracting artifacts to target/lambda/ ..."
CONTAINER="$("$CONTAINER_CMD" create "$IMAGE")"
trap '"$CONTAINER_CMD" rm "$CONTAINER" >/dev/null 2>&1 || true' EXIT
mkdir -p target/lambda
"$CONTAINER_CMD" cp "$CONTAINER:/lambda/." target/lambda/

echo "==> Done. Built binaries:"
find target/lambda -name bootstrap -maxdepth 2 | sort
