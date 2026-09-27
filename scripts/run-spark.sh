#!/usr/bin/env bash
# Runs one of the spark/predisched_spark jobs with spark-submit --master local[*]:
# natively when spark-submit is found (PATH, $SPARK_HOME or the repo's .venv), else in
# docker/spark.Dockerfile.
#
#   scripts/run-spark.sh exec_stats.py --input spark/data/execution_history.csv --output spark/out
#   scripts/run-spark.sh features.py --input spark/data/execution_history.csv --output ml/data/features
set -euo pipefail
cd "$(dirname "$0")/.."
job="${1:?usage: run-spark.sh <job.py> [--input ...] [--output ...]}"
shift

submit=""
if [ -n "${SPARK_HOME:-}" ] && [ -x "$SPARK_HOME/bin/spark-submit" ]; then
  submit="$SPARK_HOME/bin/spark-submit"
elif command -v spark-submit >/dev/null 2>&1; then
  submit="$(command -v spark-submit)"
elif [ -x .venv/bin/spark-submit ]; then
  submit=".venv/bin/spark-submit"
  export PYSPARK_PYTHON="$PWD/.venv/bin/python"
fi

if [ -n "$submit" ]; then
  echo "run-spark: native ($submit)"
  exec "$submit" --master 'local[*]' "spark/predisched_spark/$job" "$@"
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "run-spark: neither spark-submit nor docker found; see docs/components/spark.md" >&2
  exit 1
fi
echo "run-spark: docker (predisched-spark)"
docker build -q -f docker/spark.Dockerfile -t predisched-spark . >/dev/null
exec docker run --rm -v "$PWD:/work" predisched-spark "$job" "$@"
