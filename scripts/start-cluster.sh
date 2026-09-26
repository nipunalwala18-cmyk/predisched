#!/usr/bin/env bash
# Starts the local cluster: 5 schedulers (ports 51051-51055, election ids 1-5) and 3 workers
# (51061-51063) in the background, output in logs/<node>.out, PID in logs/pids/<node>.pid.
#
#   scripts/start-cluster.sh            # Bully (the default in configs/cluster.yaml)
#   scripts/start-cluster.sh ring
#   SCHEDULERS=3 WORKERS=1 scripts/start-cluster.sh
set -euo pipefail
cd "$(dirname "$0")/.."
ALGORITHM="${1:-bully}"
CONFIG="${CONFIG:-configs/cluster.yaml}"
SCHEDULERS="${SCHEDULERS:-5}"
WORKERS="${WORKERS:-3}"
mkdir -p logs/pids

start_node() {
  local name="$1"; shift
  if [[ -f "logs/pids/$name.pid" ]] && kill -0 "$(cat "logs/pids/$name.pid")" 2>/dev/null; then
    echo "$name already running (pid $(cat "logs/pids/$name.pid"))"
    return
  fi
  nohup java "$@" > "logs/$name.out" 2>&1 &
  echo $! > "logs/pids/$name.pid"
  echo "started $name (pid $!)"
}

for n in $(seq 1 "$SCHEDULERS"); do
  start_node "scheduler-$n" -jar predisched-scheduler/target/predisched-scheduler.jar \
    --config "$CONFIG" --id "scheduler-$n" --election-algorithm "$ALGORITHM"
done
for n in $(seq 1 "$WORKERS"); do
  start_node "worker-$n" -jar predisched-worker/target/predisched-worker.jar \
    --config "$CONFIG" --id "worker-$n" --port "$((51060 + n))"
done
echo "cluster starting ($ALGORITHM); check it with:"
echo "  java -jar predisched-client/target/predisched-client.jar cluster leader"
