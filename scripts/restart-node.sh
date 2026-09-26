#!/usr/bin/env bash
# Starts one node again after a crash, as start-cluster.sh started it (Exp 8). A scheduler comes
# back empty, rejoins as a backup and catches up with SyncFrom; a worker registers as new.
#
#   scripts/restart-node.sh scheduler 5
#   scripts/restart-node.sh worker 2
#   CONFIG=configs/other.yaml ALGORITHM=ring scripts/restart-node.sh scheduler 3
set -euo pipefail
cd "$(dirname "$0")/.."
role="${1:?usage: restart-node.sh scheduler|worker id}"
id="${2:?id required}"
CONFIG="${CONFIG:-configs/cluster.yaml}"
ALGORITHM="${ALGORITHM:-bully}"
name="$role-$id"
mkdir -p logs/pids
if [[ -f "logs/pids/$name.pid" ]] && kill -0 "$(cat "logs/pids/$name.pid")" 2>/dev/null; then
  echo "$name already running (pid $(cat "logs/pids/$name.pid"))"
  exit 0
fi
case "$role" in
  scheduler) args=(-jar predisched-scheduler/target/predisched-scheduler.jar --config "$CONFIG"
                   --id "$name" --election-algorithm "$ALGORITHM") ;;
  worker)    args=(-jar predisched-worker/target/predisched-worker.jar --config "$CONFIG"
                   --id "$name" --port "$((51060 + id))") ;;
  *) echo "role must be scheduler or worker"; exit 2 ;;
esac
nohup java "${args[@]}" >> "logs/$name.out" 2>&1 &
echo $! > "logs/pids/$name.pid"
echo "restarted $name (pid $!)"
