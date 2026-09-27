"""What every job shares: arguments, the SparkSession, and writing small results.

Results here are aggregates (tens of rows), so jobs collect them to the driver and write them with
Python rather than with Spark's own writers. That keeps every job runnable on Windows without
``winutils.exe``: Spark reads local files fine there, but its Hadoop-based writers need it.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from pathlib import Path
from typing import Iterable, Sequence

from pyspark.sql import SparkSession

SPARK_DIR = Path(__file__).resolve().parents[1]


def parse_args(description: str, default_input: str, default_output: str,
               argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--input", default=default_input, help="input file")
    parser.add_argument("--output", default=default_output, help="output directory")
    parser.add_argument("--master", default=None,
                        help="Spark master when run with python instead of spark-submit")
    parser.add_argument("--hold-ui", type=int, default=0, metavar="SECONDS",
                        help="keep the Spark UI (http://localhost:4040) up this long after the job")
    return parser.parse_args(argv)


def session(app_name: str, master: str | None = None) -> SparkSession:
    """The job's SparkSession. Under spark-submit the master comes from --master there."""
    # Workers must run the same Python as the driver (a venv on Windows especially).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = configure(SparkSession.builder.appName(app_name))
    if master:
        builder = builder.master(master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop(spark: SparkSession, args: argparse.Namespace) -> None:
    """Stops the session, first holding the Spark UI open when --hold-ui asks for it."""
    if args.hold_ui > 0:
        print(f"Spark UI at {spark.sparkContext.uiWebUrl} for {args.hold_ui} s", flush=True)
        time.sleep(args.hold_ui)
    spark.stop()


def configure(builder: SparkSession.Builder) -> SparkSession.Builder:
    """Settings every session needs; on Windows, the flushing worker entry (see ``pyworker``)."""
    if os.name == "nt":
        builder = (builder.config("spark.python.worker.module", "predisched_spark.pyworker")
                   .config("spark.executorEnv.PYTHONPATH", str(SPARK_DIR)))
    return builder


def write_csv(path: Path, header: Sequence[str], rows: Iterable[Sequence]) -> int:
    """Writes rows with a header; returns how many rows were written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def print_table(title: str, header: Sequence[str], rows: Sequence[Sequence]) -> None:
    widths = [max(len(str(h)), *(len(_cell(r[i])) for r in rows)) if rows else len(str(h))
              for i, h in enumerate(header)]
    print(title)
    print("  " + "  ".join(str(h).ljust(w) for h, w in zip(header, widths)))
    for row in rows:
        print("  " + "  ".join(_cell(v).ljust(w) for v, w in zip(row, widths)))


def _cell(value) -> str:
    return f"{value:.1f}" if isinstance(value, float) else str(value)

