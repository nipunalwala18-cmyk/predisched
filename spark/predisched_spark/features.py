"""ML aggregates from the execution history (spec §12.1), DataFrame API.

- per (task_type, worker_id): count, mean, p50, p95 and standard deviation of exec time;
- per worker, over 10 s windows sliding every 5 s: arrival rate (dispatches per second) and mean
  and max queue length at dispatch.

Written as Parquet to ``--output`` (default ``ml/data/features``): ``type_worker.parquet`` and
``worker_windows.parquet``. The frames are small, so they are collected and written with pandas and
pyarrow, which needs no winutils on Windows.

    spark-submit --master local[*] spark/predisched_spark/features.py \
        --input spark/data/execution_history.csv --output ml/data/features
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from predisched_spark.common import parse_args, session, stop  # noqa: E402

WINDOW = "10 seconds"
SLIDE = "5 seconds"


def load(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.csv(path, header=True, inferSchema=True)
    return (df.where(F.col("status") == "COMPLETED")
              .withColumn("dispatched_at", F.to_timestamp("dispatched_at"))
              .withColumn("exec_time_ms", F.col("exec_time_ms").cast("double"))
              .withColumn("queue_len", F.coalesce(F.col("queue_len").cast("double"), F.lit(0.0))))


def type_worker_stats(df: DataFrame) -> DataFrame:
    return (df.groupBy("task_type", "worker_id")
              .agg(F.count("*").alias("tasks"),
                   F.mean("exec_time_ms").alias("exec_mean_ms"),
                   F.percentile_approx("exec_time_ms", 0.5).alias("exec_p50_ms"),
                   F.percentile_approx("exec_time_ms", 0.95).alias("exec_p95_ms"),
                   # One sample has no sample deviation; call it 0 rather than null.
                   F.coalesce(F.stddev_samp("exec_time_ms"), F.lit(0.0)).alias("exec_std_ms"))
              .orderBy("task_type", "worker_id"))


def worker_windows(df: DataFrame) -> DataFrame:
    seconds = 10.0
    return (df.groupBy("worker_id", F.window("dispatched_at", WINDOW, SLIDE).alias("w"))
              .agg((F.count("*") / F.lit(seconds)).alias("arrival_rate"),
                   F.mean("queue_len").alias("queue_len_mean"),
                   F.max("queue_len").alias("queue_len_max"))
              .select("worker_id",
                      F.col("w.start").alias("window_start"),
                      F.col("w.end").alias("window_end"),
                      "arrival_rate", "queue_len_mean", "queue_len_max")
              .orderBy("worker_id", "window_start"))


def main(argv=None) -> int:
    args = parse_args("ML aggregates from the execution history",
                      "spark/data/execution_history.csv", "ml/data/features", argv)
    spark = session("predisched-features", args.master)
    try:
        df = load(spark, args.input)
        by_type_worker = type_worker_stats(df).toPandas()
        windows = worker_windows(df).toPandas()
    finally:
        stop(spark, args)
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    by_type_worker.to_parquet(out / "type_worker.parquet", index=False)
    windows.to_parquet(out / "worker_windows.parquet", index=False)
    print(by_type_worker.head(12).to_string(index=False))
    print(f"ROWS type_worker={len(by_type_worker)} worker_windows={len(windows)} output={out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
