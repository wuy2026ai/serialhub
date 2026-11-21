#!/usr/bin/env bash
# Stop every instance started by start.sh (waits up to 10s each, then force-kills).
HERE="$(cd "$(dirname "$0")" && pwd)"
for pidf in "$HERE"/run/*.pid; do
  [ -e "$pidf" ] || continue
  pid="$(cat "$pidf")"; n="$(basename "$pidf" .pid)"
  if kill "$pid" 2>/dev/null; then
    for _ in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid"
    echo "stopped $n"
  fi
  rm -f "$pidf"
done
