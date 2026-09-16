# Prompt 11 — Log analytics and feature pipeline (Spark MapReduce)

**Spec sections:** §4 (FR20), §9 "Exp 7", §10.1, §15 (Spark / MPI)
**Depends on:** 10
**Lab topics:** MapReduce with Hadoop / Spark
**Commit message:** `Analytics: Spark MapReduce builds execution statistics and the ML training dataset`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. The product's analytics layer turns raw telemetry into
(a) operational statistics and (b) the labelled training dataset for the prediction models. Build it
with PySpark in local mode, using the **RDD API with explicit map and reduce phases** for the
aggregations.

### Step 0 — collect the data (a script, part of the repo)

`scripts/collect-dataset.(ps1|sh)`: for each of steady, bursty, periodic, mixed, ramp × profiles
homogeneous-3 and heterogeneous-3 × strategies round-robin, random, least-loaded, resource-aware,
generate a trace with a fixed seed, replay it, and tag the run. Target ≥ 30,000 completed tasks in
total (spec §10.1). Then export from PostgreSQL to `ml/data/raw/` as CSV
(`execution_history`, `worker_metrics`, `events`, `scheduling_decisions`) with
`scripts/export_telemetry.py`.

### Jobs (`spark/predisched_spark/`)

1. `event_counts.py` — MapReduce warm-up over the event log: map each line to `(event_type, 1)` and
   `(node_id, 1)`, `reduceByKey(add)`. Output `results/analytics/event_counts.csv`.
2. `exec_stats.py` — map each history row to `(task_type, (exec_ms, exec_ms², 1))` and
   `(worker_id, …)` and `((task_type, input_size_bucket, worker_id), …)`; `reduceByKey` sums; derive
   count, mean, std dev. Also P50/P95 per task type using `aggregateByKey` over a bounded sample.
   Output `results/analytics/exec_stats_*.csv`.
3. `build_features.py` — produces the training dataset:
   - join each history row to the per-(task_type, input bucket, worker) historical mean exec time
     computed **only from rows earlier in time** (no label leakage);
   - derive labels from `worker_metrics`: `queue_len_future` = the worker's queue length `horizonS`
     seconds (default 5) after dispatch; `overloaded_future` = 1 if CPU > 85% or queue >
     `overloadQueue` at any point within the horizon (spec §10.1);
   - write `ml/data/features.parquet` plus `ml/data/features_schema.json` (column, dtype, meaning).
   The windowed label derivation may use DataFrames; the aggregations above must stay RDD
   map/reduce so the MapReduce phases are explicit. Say which is which in comments.
- `spark/run.(ps1|sh)` runs all three with `spark-submit --master local[*]`. On Windows, document the
  `winutils`/`HADOOP_HOME` setup or point to WSL/Docker (spec §6.1).

### Tests (`spark/tests`, pytest with a local SparkSession fixture)

- `exec_stats` on a 200-row fixture equals the same computation in pandas (spec §15).
- `build_features` never uses a future row for the historical mean (construct a fixture where leakage
  would change the value).
- Labels: a hand-built metrics timeline produces the expected `queue_len_future` and
  `overloaded_future`.

### Acceptance checks

```bash
scripts/collect-dataset.sh
spark/run.sh
python -m pytest spark/tests
```

Capture a screenshot of the job DAG and stages from the Spark UI (port 4040) into
`docs/components/img/spark-dag.png`.

### Docs

`docs/components/analytics.md` (map and reduce phases for each job, the leakage rule);
MapReduce row in `docs/LAB-COVERAGE.md`.
