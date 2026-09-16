#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$ROOT/logs"
echo "Building jars (skip tests)..."
mvn -q -DskipTests package
start_node() {
  local jar="$1" config="$2" name="$3"
  local pidfile="$ROOT/logs/$name.pid"
  if [ -f "$pidfile" ]; then
    kill "$(cat "$pidfile")" 2>/dev/null || true
    rm -f "$pidfile"
  fi
  nohup java -jar "$jar" "$config" >"$ROOT/logs/$name.out.log" 2>"$ROOT/logs/$name.err.log" &
  echo $! > "$pidfile"
  echo "started $name pid=$(cat "$pidfile") config=$config"
}
start_node "$ROOT/predisched-scheduler/target/predisched-scheduler.jar" "$ROOT/configs/local/scheduler-1.yaml" "scheduler-1"
sleep 3
start_node "$ROOT/predisched-worker/target/predisched-worker.jar" "$ROOT/configs/local/worker-1.yaml" "worker-1"
echo "Cluster up. Logs in logs/."
