"""Retrain M1 on recent executions and register it as a shadow (prompt 21, F15).

    python -m predisched_ml.retrain [--since-hours 24] [--family linear] [--params '{"alpha": 0.1}']

Called by the scheduler when drift is detected and ``drift.autoRetrain`` is on, or by hand. It
reads the last ``since-hours`` of completed executions from ``execution_history``, adds the
leak-free prior statistics as the dataset builder does, splits by time (80/20), fits the M1
family, scores it and the live M1 on the same held-out part, and saves it with status ``shadow``.
It never makes anything live: promotion is a separate, checked step (``registry promote``).

Recent history has no queue or overload labels without the metrics series, so only M1 is
retrained here. The workers' pool sizes come from the ``workers`` table; their simulated slowdown
is not recorded in the history and is taken as 1 (a known gap, see docs/components/model-lifecycle.md).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys

import numpy as np
import pandas as pd

from . import ML_DIR, metrics
from .models import ExecTimeModel, make
from .registry import load, save

sys.path.insert(0, str(ML_DIR))
import build_dataset  # noqa: E402

DSN = os.environ.get("PREDISCHED_DSN",
                     "host=localhost port=5432 dbname=predisched user=postgres password=predisched")


def load_recent(since_hours: float) -> pd.DataFrame:
    import psycopg
    with psycopg.connect(DSN) as conn:
        cur = conn.execute(
            "SELECT id, task_id, task_type, resource_profile, attempt, status, strategy, priority,"
            " input_size, worker_id, worker_cores, concurrent_tasks_on_worker, cpu_pct, mem_pct,"
            " active_threads, queue_len, arrival_rate, avg_exec_recent, dispatched_at,"
            " wait_time_ms, exec_time_ms FROM execution_history"
            " WHERE dispatched_at > now() - make_interval(secs => %s)"
            " ORDER BY dispatched_at, id", (since_hours * 3600,))
        history = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
        pools = dict(conn.execute("SELECT worker_id, pool_size FROM workers").fetchall())
    for column in ("cpu_pct", "mem_pct", "arrival_rate", "avg_exec_recent"):
        history[column] = pd.to_numeric(history[column], errors="coerce")
    history["dispatched_at"] = pd.to_datetime(history["dispatched_at"], utc=True)
    history["worker_pool_size"] = history["worker_id"].map(pools).fillna(1)
    history["worker_slowdown"] = 1.0
    return history


def prepare(history: pd.DataFrame) -> pd.DataFrame:
    df = build_dataset.prior_stats(history)
    df = df[df["status"] == "COMPLETED"].reset_index(drop=True)
    return df.sort_values(["dispatched_at", "id"], kind="mergesort").reset_index(drop=True)


def retrain(df: pd.DataFrame, family: str, params: dict, seed: int = 42,
            live=None, test_fraction: float = 0.2) -> tuple[object, dict]:
    """Fits M1 on the earliest part; returns the served model and its metrics (and live's)."""
    cut = int(round(len(df) * (1 - test_fraction)))
    train, test = df.iloc[:cut], df.iloc[cut:]
    y = train["exec_time_ms"].to_numpy(dtype=float)
    model = make("m1", family, params, seed).fit(train, y)
    served = ExecTimeModel(model, set(train["task_type"]), float(np.mean(y)))
    result = {"test": metrics.regression(test["exec_time_ms"], served.predict(test)),
              "rows": {"train": len(train), "test": len(test)}}
    if live is not None:
        result["live_on_same_test"] = metrics.regression(test["exec_time_ms"], live.predict(test))
    return served, result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--since-hours", type=float, default=24.0)
    parser.add_argument("--family", default=None, help="default: the live M1's family")
    parser.add_argument("--params", default=None, help="JSON, default: the live M1's")
    parser.add_argument("--min-rows", type=int, default=500)
    args = parser.parse_args(argv)
    live, live_meta = load("m1")
    family = args.family or live_meta["kept"]["family"]
    params = json.loads(args.params) if args.params else live_meta["kept"]["params"]
    if family == "baseline":
        print("the live M1 is a baseline: pass --family and --params", file=sys.stderr)
        return 2
    df = prepare(load_recent(args.since_hours))
    if len(df) < args.min_rows:
        print(f"only {len(df)} completed executions in the last {args.since_hours:g} h;"
              f" {args.min_rows} needed", file=sys.stderr)
        return 1
    served, result = retrain(df, family, params, live=live)
    meta = {"name": "exec_time", "target": "exec_time_ms", "kind": "regression",
            "primary_metric": "mae",
            "kept": {"family": family, "candidate": served.model.describe(), "params": params,
                     "reason": f"retrained on the last {args.since_hours:g} h of executions"
                               " (prompt 21); registered as shadow, never live"},
            "metrics": {"test": result["test"]},
            "live_on_same_test": result.get("live_on_same_test"),
            "split": result["rows"],
            "dataset": f"execution_history, last {args.since_hours:g} h",
            "trained_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "seed": 42}
    folder = save("m1", served, meta, status="shadow")
    live_mae = (result.get("live_on_same_test") or {}).get("mae")
    print(f"m1 v{meta['version']} ({served.model.describe()}) trained on {result['rows']['train']:,}"
          f" executions, registered as shadow in {folder.name}")
    print(f"  held-out {result['rows']['test']:,}: MAE {result['test']['mae']:.2f} ms"
          + (f" (live v{live_meta['version']} on the same rows: {live_mae:.2f} ms)"
             if live_mae is not None else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
