#!/usr/bin/env bash
# Entry point of the Java image (docker/java.Dockerfile): the first argument picks the role.
set -euo pipefail
role="${1:-client}"
shift || true
case "$role" in
  scheduler)     exec java -jar /app/jars/predisched-scheduler.jar "$@" ;;
  worker)        exec java -jar /app/jars/predisched-worker.jar "$@" ;;
  client)        exec java -jar /app/jars/predisched-client.jar "$@" ;;
  benchmark)     exec java -jar /app/jars/predisched-benchmark.jar "$@" ;;
  dashboard-api) exec java -jar /app/jars/predisched-dashboard-api.jar "$@" ;;
  *) echo "unknown role '$role': scheduler, worker, client, benchmark or dashboard-api" >&2; exit 2 ;;
esac
