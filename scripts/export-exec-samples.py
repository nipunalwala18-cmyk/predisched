"""Execution-time samples for the what-if simulator (prompt 20, F17).

    python scripts/export-exec-samples.py [--dataset ml/data/dataset.parquet]
                                          [--out workloads/benchmark/exec-samples.csv]

One row per completed campaign execution: task type, input size and execution time divided by
the worker's simulated slowdown, so the simulator can scale it to any worker it models.
"""

import argparse
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dataset", default=str(REPO / "ml" / "data" / "dataset.parquet"))
    parser.add_argument("--out", default=str(REPO / "workloads" / "benchmark" / "exec-samples.csv"))
    args = parser.parse_args()
    df = pd.read_parquet(args.dataset)
    df = df[df["status"] == "COMPLETED"]
    out = pd.DataFrame({
        "task_type": df["task_type"],
        "input_size": df["input_size"].astype("int64"),
        "exec_ms": (df["exec_time_ms"] / df["worker_slowdown"]).round(1),
    }).sort_values(["task_type", "input_size", "exec_ms"], kind="mergesort")
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"{len(out)} samples of {out['task_type'].nunique()} task types -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
