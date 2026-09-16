#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
for pidfile in "$ROOT"/logs/*.pid; do
  [ -f "$pidfile" ] || continue
  pid="$(cat "$pidfile")"
  if kill "$pid" 2>/dev/null; then
    echo "stopped pid=$pid ($(basename "$pidfile" .pid))"
  else
    echo "already stopped pid=$pid ($(basename "$pidfile" .pid))"
  fi
  rm -f "$pidfile"
done
