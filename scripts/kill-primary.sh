#!/usr/bin/env bash
# Kills the primary scheduler the way a crash would (Exp 8): asks the cluster who leads
# (`cluster leader`), then kills that node's process with stop-node.sh.
#
#   scripts/kill-primary.sh                        # configs/cluster.yaml
#   scripts/kill-primary.sh configs/other.yaml
set -euo pipefail
cd "$(dirname "$0")/.."
CONFIG="${1:-configs/cluster.yaml}"
answers="$(java -jar predisched-client/target/predisched-client.jar --config "$CONFIG" cluster leader || true)"
echo "$answers"
# The leader most live nodes name.
leader="$(echo "$answers" | grep -o 'leader=[0-9]*' | cut -d= -f2 | sort | uniq -c | sort -rn \
  | head -1 | awk '{print $2}')"
if [[ -z "$leader" ]]; then
  echo "no leader known: nothing killed"
  exit 1
fi
echo "primary is scheduler $leader: killing it"
scripts/stop-node.sh scheduler "$leader"
