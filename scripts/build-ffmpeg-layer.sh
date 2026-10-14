#!/usr/bin/env bash
# Build and publish an ARM64 ffmpeg Lambda layer for the vu-analysis-worker.
#
# The worker remuxes TAMS segment media to MP4 with ffmpeg before sending it to Pegasus, so the
# (provided.al2023, arm64) Lambda needs an ffmpeg binary at /opt/bin/ffmpeg. This downloads a static
# aarch64 ffmpeg build, packages it as a layer, publishes it, and prints the LayerVersionArn to pass
# to `cdk deploy -c ffmpeg_layer_arn=<arn>`.
#
# Requires: aws CLI (authenticated), curl, tar, zip.
#   AWS_REGION=us-east-1 ./scripts/build-ffmpeg-layer.sh
set -euo pipefail

LAYER_NAME="${LAYER_NAME:-tads-vu-ffmpeg}"
REGION="${AWS_REGION:-us-east-1}"
# Static aarch64 ffmpeg (GPL build). Override FFMPEG_URL to pin a specific build or mirror.
FFMPEG_URL="${FFMPEG_URL:-https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-arm64-static.tar.xz}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

echo ">> downloading $FFMPEG_URL"
curl -fsSL "$FFMPEG_URL" -o "$work/ffmpeg.tar.xz"

# Best-effort integrity check against the publisher's md5, when available.
if curl -fsSL "$FFMPEG_URL.md5" -o "$work/ffmpeg.tar.xz.md5" 2>/dev/null; then
  expected="$(awk '{print $1}' "$work/ffmpeg.tar.xz.md5")"
  if command -v md5sum >/dev/null 2>&1; then
    actual="$(md5sum "$work/ffmpeg.tar.xz" | awk '{print $1}')"
  else
    actual="$(md5 -q "$work/ffmpeg.tar.xz")"
  fi
  [ "$expected" = "$actual" ] || {
    echo "!! md5 mismatch: expected $expected got $actual" >&2
    exit 1
  }
  echo ">> md5 verified"
fi

echo ">> extracting"
tar -xf "$work/ffmpeg.tar.xz" -C "$work"
bin="$(find "$work" -type f -name ffmpeg | head -1)"
[ -n "$bin" ] || {
  echo "!! ffmpeg binary not found in archive" >&2
  exit 1
}

echo ">> packaging layer (bin/ffmpeg -> /opt/bin/ffmpeg)"
mkdir -p "$work/layer/bin"
cp "$bin" "$work/layer/bin/ffmpeg"
chmod +x "$work/layer/bin/ffmpeg"
(cd "$work/layer" && zip -qr "$work/ffmpeg-layer.zip" bin)

echo ">> publishing layer '$LAYER_NAME' in $REGION"
arn="$(aws lambda publish-layer-version \
  --layer-name "$LAYER_NAME" \
  --description "Static arm64 ffmpeg for the TADS video-understanding worker" \
  --zip-file "fileb://$work/ffmpeg-layer.zip" \
  --compatible-architectures arm64 \
  --compatible-runtimes provided.al2023 \
  --region "$REGION" \
  --query LayerVersionArn --output text)"

echo
echo "ffmpeg layer published:"
echo "  $arn"
echo
echo "Deploy the stack with it (add your other -c creds):"
echo "  cd cdk && uv run cdk deploy -c ffmpeg_layer_arn=$arn"
