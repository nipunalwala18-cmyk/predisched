# Prompt 12: MapReduce over execution logs with Spark (Exp 7)

## Goal

A PySpark job reads PrediSched execution history and computes, with explicit map and reduce phases on
the RDD API, average execution time and task count per task type and per worker. It also produces the
aggregated features the ML pipeline uses. The scheduler can run it as a `MAPREDUCE_TASK`. This is lab
Exp 7.

## Read first

- Spec §4 FR20, §11 Exp 7, §12.1, §7.2 (`MAPREDUCE_TASK`), §8.1 (Spark on Windows note)

## Build

1. **`spark/`** (Python package `predisched_spark`, `requirements.txt` pinned to PySpark 3.5.x):
   - `export_history.py`: dumps `execution_history` and `events` from PostgreSQL to
     `spark/data/execution_history.csv` and `spark/data/events.jsonl` (psycopg, no Spark needed).
   - `wordcount.py`: the warm-up. Word count over the event log's `type` and `details` fields with
     `flatMap` → `map` → `reduceByKey`.
   - `exec_stats.py`: the Exp 7 job, RDD API only:
     - map: each CSV row → `(task_type, (exec_time_ms, 1))` and `(worker_id, (exec_time_ms, 1))`
     - reduce: `reduceByKey` summing time and count; then average.
     - output: `spark/out/by_type.csv`, `spark/out/by_worker.csv`, and a printed table.
   - `features.py`: DataFrame job building the ML aggregates: per `(task_type, worker_id)` mean,
     p50, p95 and std of exec time; per worker, rolling arrival rate and queue length over 10 s
     windows. Writes Parquet to `ml/data/features/`.
   - All jobs run with `spark-submit --master local[*]` and accept `--input` / `--output`.
2. **Windows**: document the `winutils.exe` / `HADOOP_HOME` setup, and provide
   `docker/spark.Dockerfile` (based on the official `apache/spark-py` image) plus
   `scripts/run-spark.ps1` / `.sh` that pick native or Docker automatically.
3. **`MAPREDUCE_TASK`** executor in `predisched-worker`: input `dataset=…, job=avg_exec|wordcount`,
   runs `spark-submit` as a subprocess with a timeout, and returns the output path plus row counts.
   Resource profile `PARALLEL`. Disabled (validation error) when `spark.home` is not configured.

## Tests

- `pytest` in `spark/` with a local SparkSession fixture: `exec_stats` on a 20-row fixture CSV equals
  a pandas `groupby().mean()` of the same data (spec §17 Spark row).
- `features.py` produces the expected columns and no nulls for workers with data.
- Java: `MapReduceTaskExecutor` builds the right command line; disabled without `spark.home`.

## Acceptance checks

```bash
python spark/export_history.py
```
```bash
spark-submit --master local[*] spark/predisched_spark/exec_stats.py --input spark/data/execution_history.csv --output spark/out
```
```bash
java -jar predisched-client/target/predisched-client.jar submit --type MAPREDUCE_TASK --input "dataset=spark/data/execution_history.csv, job=avg_exec" --priority 5
```

Paste the per-type and per-worker tables, and a screenshot (saved to `docs/img/spark-dag.png`) of the
job DAG from the Spark UI at http://localhost:4040.

## Docs and commit

- `docs/components/spark.md`: map and reduce phases, how to run on Windows and Linux, real output.
- `docs/LAB-COVERAGE.md` Exp 7 row.
- CI: add a Python job running `pytest` in `spark/`.
- Commit: `Analytics: Spark MapReduce over execution history, feeding ML features`
