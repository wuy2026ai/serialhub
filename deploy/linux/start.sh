#!/usr/bin/env bash
# Start one serialhub per config (com-*.yaml) in this folder.
# Usage: ./start.sh [--host 0.0.0.0]
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; ROOT="$(cd "$HERE/../.." && pwd)"
HOST_ARGS=(); [ "${1:-}" = "--host" ] && HOST_ARGS=(--host "$2")
mkdir -p "$HERE/run"; cd "$ROOT"
for cfg in "$HERE"/com-*.yaml; do
  n="$(basename "$cfg" .yaml)"; pidf="$HERE/run/$n.pid"
  if [ -f "$pidf" ] && kill -0 "$(cat "$pidf")" 2>/dev/null; then echo "$n already running (pid $(cat "$pidf"))"; continue; fi
  nohup "$ROOT/.venv/bin/serialhub" serve --config "$cfg" ${HOST_ARGS[@]+"${HOST_ARGS[@]}"} > "$HERE/run/$n.log" 2>&1 &
  echo $! > "$pidf"
  port="$(grep -oE "port: *[0-9]+" "$cfg" | head -1 | grep -oE "[0-9]+")"
  echo "$n started (pid $!) -> http://${2:-127.0.0.1}:$port/   log: $HERE/run/$n.log"
done
