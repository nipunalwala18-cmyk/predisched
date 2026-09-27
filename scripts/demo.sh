#!/usr/bin/env bash
# The lab demo (prompt 24): a narrated walk through the ten lab topics on the running product.
# Each step prints its topic, runs the real command and waits for Enter.
#
#   scripts/demo.sh                 # against whatever is running: the Docker stack, else native
#   scripts/demo.sh --no-pause      # CI smoke test: no prompts, exit 1 if any step failed
#   scripts/demo.sh --docker | --native
#
# Docker: docker compose -f docker/docker-compose.yml up --build -d
# Native: CLOCK_OFFSETS="-300 500 200" CONFIG=configs/dashboard.yaml scripts/start-cluster.sh,
#         the prediction server (ml/) and the dashboard API (docs/components/deployment.md).
set -uo pipefail
cd "$(dirname "$0")/.."

PAUSE=1
MODE=auto
for arg in "$@"; do
  case "$arg" in
    --no-pause) PAUSE=0 ;;
    --docker) MODE=docker ;;
    --native) MODE=native ;;
    -h|--help) sed -n 2,11p "$0"; exit 0 ;;
    *) echo "unknown option $arg" >&2; exit 2 ;;
  esac
done

COMPOSE="docker compose -f docker/docker-compose.yml"
API="${API_URL:-http://localhost:8080}"
ADMIN_KEY="${ADMIN_KEY:-dev-admin}"
if [[ "$MODE" == auto ]]; then
  MODE=native
  if command -v docker >/dev/null 2>&1 && [[ -n "$($COMPOSE ps -q scheduler-1 2>/dev/null)" ]]; then
    MODE=docker
  fi
fi

PY=python3
for candidate in .venv/Scripts/python.exe .venv/bin/python; do
  [[ -x "$candidate" ]] && PY="$candidate" && break
done
WINDOWS=0
[[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* ]] && WINDOWS=1

if [[ "$MODE" == docker ]]; then
  CONFIG=configs/docker.yaml
  # The dashboard API's container is never killed by the demo, so the client runs there.
  client() { $COMPOSE exec -T dashboard-api predisched client --config "$CONFIG" "$@"; }
  bench() { $COMPOSE exec -T dashboard-api predisched benchmark "$@"; }
  node_log() { docker logs "predisched-scheduler-$1" 2>&1; }
  merge_events() { $COMPOSE exec -T prediction python /app/scripts/merge-events.py /app/logs "$@"; }
  kill_scheduler() { docker kill "predisched-scheduler-$1"; }   # SIGKILL: a crash, not a shutdown
  # The client runs in a container, so a file it reads has to be copied in first.
  stage_file() { $COMPOSE exec -T dashboard-api sh -c "cat > /tmp/$(basename "$1")" < "$1"; echo "/tmp/$(basename "$1")"; }
  restart_scheduler() { docker start "predisched-scheduler-$1"; }
else
  CONFIG="${CONFIG:-configs/dashboard.yaml}"
  client() { java -jar predisched-client/target/predisched-client.jar --config "$CONFIG" "$@"; }
  bench() { java -jar predisched-benchmark/target/predisched-benchmark.jar "$@"; }
  node_log() { cat "logs/scheduler-$1.out"; }
  merge_events() { "$PY" scripts/merge-events.py logs "$@"; }
  kill_scheduler() { scripts/stop-node.sh scheduler "$1"; }
  stage_file() { echo "$1"; }
  restart_scheduler() { CONFIG="$CONFIG" scripts/restart-node.sh scheduler "$1"; }
fi

FAILED=()
STEP=0
bold() { printf '\033[1m%s\033[0m\n' "$*"; }
step() {
  STEP=$((STEP + 1))
  echo
  bold "=== $STEP/10  $1 ==="
  echo "$2" | fold -s -w 100
  echo
}
run() {  # prints the command (on stderr, so captures and pipes keep only its output), runs it
  echo "\$ $*" >&2
  "$@"
}
fail() { echo "!! $1"; FAILED+=("$STEP: $1"); }
pause() {
  if [[ "$PAUSE" == 1 ]]; then
    read -r -p "[Enter] for the next step " _ </dev/tty || true
  fi
}
task_ids() { grep -o 'task_id=[^ ]*' | cut -d= -f2; }
leader() { curl -fsS "$API/api/cluster/leader" | grep -o '"leader":[0-9]*' | head -1 | cut -d: -f2; }
wait_leader() {  # waits until the API names a leader other than $1; prints it
  local old="$1" deadline=$((SECONDS + ${2:-30})) now
  while (( SECONDS < deadline )); do
    now="$(leader 2>/dev/null)"
    if [[ -n "$now" && "$now" != "$old" ]]; then echo "$now"; return 0; fi
    sleep 0.5
  done
  return 1
}

bold "PrediSched lab demo ($MODE, config $CONFIG, API $API)"

# 1 ------------------------------------------------------------------------------------------
step "Client-server communication with RPC (Exp 1)" \
  "The client calls SubmitTask on the scheduler over gRPC (proto/task.proto). Followers answer with a leader hint and the client follows it; the leader dispatches to a worker over worker.proto and the client polls GetTaskStatus until the task is done."
out="$(run client submit --type CPU_TASK --input n=2000000 --priority 5)"
echo "$out"
TASK="$(echo "$out" | task_ids | head -1)"
if [[ -n "$TASK" ]]; then
  run client watch "$TASK" | tail -1
else
  fail "submit was not accepted"
fi
pause

# 2 ------------------------------------------------------------------------------------------
step "Multithreading: worker thread pools (Exp 2)" \
  "Twelve one-second SLEEP_TASKs at once. Each worker runs a fixed thread pool, so they run side by side instead of one after another: twelve seconds of work should finish in a few seconds."
batch="$(mktemp)"
for _ in $(seq 1 12); do echo "SLEEP_TASK 5 ms=1000" >> "$batch"; done
out="$(client submit-file "$(stage_file "$batch")")"
rm -f "$batch"
ids=($(echo "$out" | task_ids))
echo "submitted ${#ids[@]} tasks; waiting for them through the dashboard API"
records="$(mktemp)"
for id in "${ids[@]}"; do
  record=""
  for _ in $(seq 1 120); do
    record="$(curl -fsS "$API/api/tasks/$id" 2>/dev/null)"
    [[ "$record" == *'"status":"COMPLETED"'* ]] && break
    sleep 0.5
  done
  [[ "$record" == *'"status":"COMPLETED"'* ]] || fail "task $id did not complete"
  echo "$record" >> "$records"
done
# Wall time from the first submission to the last completion, against the summed execution time.
"$PY" - "$records" <<'EOF' || fail "could not read the task records"
import json, sys
from collections import Counter
from datetime import datetime
tasks = [json.loads(line)["task"] for line in open(sys.argv[1]) if line.strip()]
at = lambda s: datetime.fromisoformat(s)
span = (max(at(t["completed_at"]) for t in tasks) - min(at(t["submitted_at"]) for t in tasks)).total_seconds()
busy = sum(t["exec_time_ms"] for t in tasks) / 1000
per_worker = ", ".join(f"{w} {n}" for w, n in sorted(Counter(t["worker_id"] for t in tasks).items()))
print(f"{len(tasks)} tasks, {busy:.1f} s of execution, done {span:.1f} s after the first submission "
      f"({busy / span:.1f} running at once on average); per worker: {per_worker}")
EOF
rm -f "$records"
[[ ${#ids[@]} == 12 ]] || fail "expected 12 accepted tasks, got ${#ids[@]}"
pause

# 3 ------------------------------------------------------------------------------------------
step "Clock synchronisation: Berkeley and Lamport (Exp 3)" \
  "The workers start with skewed clocks. The leader's Berkeley daemon polls every node, averages the offsets and sends each a correction. Every event also carries a Lamport time, so merging the nodes' event logs by Lamport order respects causality even where physical order does not."
L="$(leader)"
echo "\$ grep 'Berkeley round' <scheduler-$L log>"
rounds="$(node_log "$L" | grep "Berkeley round" | sed 's/^.*Berkeley round/Berkeley round/')"
if [[ -z "$rounds" ]]; then
  echo "(no Berkeley round yet: the daemon runs every clock.syncIntervalMs)"
else
  # Each round logs two lines: the offsets it measured, then the corrections it sent.
  echo "the widest spread this leader measured, and the corrections it sent:"
  echo "$rounds" | awk '/offsets before/ { match($0, /spread -?[0-9]+/);
      s = substr($0, RSTART + 7, RLENGTH - 7) + 0; if (!seen || s > best) { best = s; line = $0; want = 1; seen = 1 } next }
    want { fix = $0; want = 0 } END { print line; print fix }'
  echo "the latest round:"
  echo "$rounds" | tail -1
fi
if [[ -n "${TASK:-}" ]]; then
  echo "\$ scripts/merge-events.py logs --task $TASK"
  merge_events --task "$TASK" | sed -n '1,14p' || fail "merge-events failed"
fi
pause

# 4 ------------------------------------------------------------------------------------------
step "Leader election and primary-backup failover (Exp 4, Exp 8)" \
  "Kill the primary. The followers stop getting its pings, the Bully election picks the highest live id, and the new primary (already holding a replica of every task) takes over. A task submitted afterwards completes without the client being told anything."
run client cluster leader
OLD="$(leader)"
if [[ -z "$OLD" ]]; then
  fail "no leader before the kill"
else
  run kill_scheduler "$OLD"
  t0=$SECONDS
  if NEW="$(wait_leader "$OLD" 30)"; then
    echo "new leader: scheduler-$NEW after about $((SECONDS - t0)) s"
    out="$(client submit --type CPU_TASK --input n=1000000 --priority 5)"
    id="$(echo "$out" | task_ids | head -1)"
    if [[ -n "$id" ]]; then client watch "$id" | tail -1; else fail "submit after failover"; fi
  else
    fail "no new leader within 30 s"
  fi
  echo "restarting scheduler-$OLD: it rejoins as a backup and catches up with SyncFrom"
  restart_scheduler "$OLD" >/dev/null
  for _ in $(seq 1 60); do
    curl -fsS "$API/api/cluster/leader" 2>/dev/null | grep -q "\"nodeId\":\"scheduler-$OLD\"" && break
    sleep 0.5
  done
  run client cluster leader
fi
pause

# 5 ------------------------------------------------------------------------------------------
step "Replication and consistency models (Exp 5)" \
  "Three replicas, one lagging 50 ms. Strong consistency (quorum writes and reads, W=2 R=2) never returns a stale read; eventual consistency acknowledges at once, reads stale data and converges later. Watch the stale_reads column."
run bench consistency-compare --writes 100 --out logs/demo-exp5.csv 2>&1 | grep -v "Read repair" \
  || fail "consistency-compare failed"
pause

# 6 ------------------------------------------------------------------------------------------
step "Load balancing: switch strategy at runtime (Exp 6)" \
  "Strategies are pluggable (SchedulingStrategy). The dashboard API's admin endpoint tells the primary to switch from the reactive least_loaded to the predictive strategy, with no restart."
echo "\$ curl -X POST $API/api/admin/strategy -d '{\"strategy\":\"predictive\"}'"
curl -fsS -X POST -H "X-Admin-Key: $ADMIN_KEY" -H "Content-Type: application/json" \
  -d '{"strategy":"predictive"}' "$API/api/admin/strategy" || fail "strategy switch refused"
echo
curl -fsS "$API/api/overview" | grep -o '"strategy":"[^"]*"' | head -1
pause

# 7 ------------------------------------------------------------------------------------------
step "MapReduce with Spark (Exp 7)" \
  "Spark's map + reduceByKey over the execution history: mean execution time per task type and tasks per worker. It runs natively when spark-submit is installed, else in the predisched-spark image."
input=spark/tests/fixtures/execution_history_20.csv
# In Docker the job runs as the image's spark user, which must be able to write the output.
mkdir -p spark/out/demo && chmod a+rwx spark/out spark/out/demo 2>/dev/null
if "$PY" spark/export_history.py --output spark/data >/dev/null 2>&1; then
  input=spark/data/execution_history.csv
  echo "exported the live execution history: $(($(wc -l < "$input") - 1)) executions"
fi
if [[ "$WINDOWS" == 1 ]]; then
  run powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run-spark.ps1 exec_stats.py \
    --input "$input" --output spark/out/demo 2>&1 | grep -v -E "WARN|INFO|UserWarning|^\s*$|Using Spark" | tail -25 \
    || fail "Spark job failed"
else
  run scripts/run-spark.sh exec_stats.py --input "$input" --output spark/out/demo 2>&1 \
    | grep -v -E "WARN|INFO|UserWarning|^\s*$|Using Spark" | tail -25 || fail "Spark job failed"
fi
pause

# 8 ------------------------------------------------------------------------------------------
step "MPI collectives and matrix multiplication (Exp 9, Exp 10)" \
  "Rank 0 broadcasts the config, scatters 20 tasks over 4 ranks and gathers the results. Then a 400 x 400 product: B broadcast, A's rows scattered, local multiply, rows gathered, checked against A @ B."
run scripts/run-mpi.sh 4 collectives --generate 20 --seed 42 --out ../logs/demo-exp9.csv 2>&1 | tail -8 \
  || fail "MPI collectives failed"
run scripts/run-mpi.sh 4 matmul --size 400 2>&1 | tail -6 || fail "MPI matmul failed"
pause

# 9 ------------------------------------------------------------------------------------------
step "Predictive scheduling: explain a decision (F13)" \
  "With the predictive strategy on, the primary asks the prediction server for each worker's expected wait and execution time and overload risk, and keeps the per-worker scores. explain prints them."
out="$(client submit --type CPU_TASK --input n=3000000 --priority 5)"
id="$(echo "$out" | task_ids | head -1)"
if [[ -n "$id" ]]; then
  client watch "$id" | tail -1
  run client explain "$id" || fail "explain failed"
else
  fail "submit for explain"
fi
pause

# 10 -----------------------------------------------------------------------------------------
step "The dashboard" \
  "Everything above is live on the dashboard: cluster topology and leader, workers, the task table with the same explanation, predictions, benchmarks and chaos controls."
overview="$(curl -fsS "$API/api/overview")" || fail "dashboard API unreachable"
workers="$(echo "$overview" | grep -o '"activeWorkers":[0-9]*' | cut -d: -f2)"
leaders="$(curl -fsS "$API/api/cluster/leader" | grep -o '"isLeader":true' | wc -l)"
echo "/api/overview: activeWorkers=$workers; /api/cluster/leader: $leaders node(s) leading"
[[ "$workers" == 3 ]] || fail "expected 3 workers, got '$workers'"
[[ "$leaders" == 1 ]] || fail "expected 1 leader, got $leaders"
echo "Dashboard: http://localhost:5173   API docs: $API/swagger-ui"
if [[ "$PAUSE" == 1 ]]; then
  if [[ "$WINDOWS" == 1 ]]; then start "" http://localhost:5173 2>/dev/null
  elif command -v xdg-open >/dev/null; then xdg-open http://localhost:5173 >/dev/null 2>&1
  elif command -v open >/dev/null; then open http://localhost:5173; fi
fi

echo
if (( ${#FAILED[@]} )); then
  bold "Demo finished with ${#FAILED[@]} failed step(s):"
  printf '  %s\n' "${FAILED[@]}"
  exit 1
fi
bold "Demo finished: all ten steps ran."
