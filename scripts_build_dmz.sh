#!/usr/bin/env bash
set -euo pipefail
SRC="$(cd "$(dirname "$0")" && pwd)"
OUT="${1:-$PWD/migration-failure-agent-dmz}"
rm -rf "$OUT"; mkdir -p "$OUT"
cp -a "$SRC/config" "$SRC/engine" "$SRC/harness" "$OUT/"
echo "DMZ artifact created at: $OUT"
echo "Simulator, fixtures, evaluation and local runners excluded."
