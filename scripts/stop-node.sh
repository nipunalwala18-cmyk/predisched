#!/usr/bin/env bash
# Kills one node started by start-cluster.sh, the way a crash would.
#
#   scripts/stop-node.sh scheduler 5
#   scripts/stop-node.sh worker 2
#   scripts/stop-node.sh all
set -uo pipefail
cd "$(dirname "$0")/.."
role="${1:?usage: stop-node.sh scheduler|worker|all [id]}"
if [[ "$role" == "all" ]]; then
  files=(logs/pids/*.pid)
else
  files=("logs/pids/$role-${2:?id required}.pid")
fi
status=1
for file in "${files[@]}"; do
  [[ -f "$file" ]] || continue
  pid="$(cat "$file")"
  # A PID written by start-cluster.ps1 is a Windows process tree; one from start-cluster.sh
  # is a plain child of bash.
  kill -9 "$pid" 2>/dev/null || taskkill //T //F //PID "$pid" >/dev/null 2>&1
  rm -f "$file"
  echo "stopped $(basename "$file" .pid) (pid $pid)"
  status=0
done
[[ $status -eq 0 ]] || echo "no PID file for $*"
exit $status
