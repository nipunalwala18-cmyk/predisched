"""Exp 7: average execution time and task count per task type and per worker, as MapReduce.

RDD API only, so each phase is explicit:

    map     each CSV row -> (task_type, (exec_time_ms, 1)) and (worker_id, (exec_time_ms, 1))
    reduce  reduceByKey adds the pairs: (sum of exec_time_ms, count)
    then    sum / count per key

Writes ``by_type.csv`` and ``by_worker.csv`` to ``--output`` and prints both tables.

    spark-submit --master local[*] spark/predisched_spark/exec_stats.py \
        --input spark/data/execution_history.csv --output spark/out
"""

from __future__ import annotations

import csv
import io
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from predisched_spark.common import parse_args, print_table, session, stop, write_csv  # noqa: E402

COLUMNS = ("tasks", "mean_exec_ms", "total_exec_ms")


def parse_line(line: str, columns: list[str]) -> dict[str, str] | None:
    """One CSV line as a dict, or None for the header or a malformed line."""
    values = next(csv.reader(io.StringIO(line)))
    if values == columns or len(values) != len(columns):
        return None
    return dict(zip(columns, values))


def to_pairs(row: dict[str, str]) -> list[tuple[str, tuple[int, int]]]:
    """Map phase: one row becomes a (key, (exec_ms, 1)) pair per grouping."""
    exec_ms = int(float(row["exec_time_ms"]))
    return [
        ("type:" + row["task_type"], (exec_ms, 1)),
        ("worker:" + row["worker_id"], (exec_ms, 1)),
    ]


def add(a: tuple[int, int], b: tuple[int, int]) -> tuple[int, int]:
    """Reduce phase: (sum, count) + (sum, count)."""
    return a[0] + b[0], a[1] + b[1]


def compute(sc, input_path: str) -> dict[str, list[tuple[str, int, float, int]]]:
    """Runs the MapReduce; returns rows per grouping, sorted by key."""
    lines = sc.textFile(input_path)
    columns = next(csv.reader(io.StringIO(lines.first())))
    totals = (lines
              .map(lambda line: parse_line(line, columns))
              .filter(lambda row: row is not None and row.get("status", "COMPLETED") == "COMPLETED")
              .flatMap(to_pairs)
              .reduceByKey(add)
              .collect())
    result: dict[str, list[tuple[str, int, float, int]]] = {"type": [], "worker": []}
    for key, (total, count) in totals:
        group, name = key.split(":", 1)
        result[group].append((name, count, total / count, total))
    for rows in result.values():
        rows.sort()
    return result


def main(argv=None) -> int:
    args = parse_args("Exp 7: exec time per task type and worker (MapReduce)",
                      "spark/data/execution_history.csv", "spark/out", argv)
    spark = session("predisched-exec-stats", args.master)
    try:
        result = compute(spark.sparkContext, args.input)
    finally:
        stop(spark, args)
    out = Path(args.output)
    by_type = write_csv(out / "by_type.csv", ("task_type",) + COLUMNS, result["type"])
    by_worker = write_csv(out / "by_worker.csv", ("worker_id",) + COLUMNS, result["worker"])
    print_table("Per task type", ("task_type",) + COLUMNS, result["type"])
    print_table("Per worker", ("worker_id",) + COLUMNS, result["worker"])
    # One machine-readable line for MapReduceTaskExecutor.
    print(f"ROWS by_type={by_type} by_worker={by_worker} output={out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
