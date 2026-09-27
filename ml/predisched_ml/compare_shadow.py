"""Live vs shadow on the same completed tasks (prompt 21, F14).

    python -m predisched_ml.compare_shadow --model m1 [--predictions logs/predictions.jsonl]

The prediction server logs every answer with the live and the shadow predictions per worker. A
task's actual execution time is known only for the worker it ran on (``execution_history``), so
both versions are scored on exactly those (task, worker) pairs: same tasks, same workers. Only M1
(execution time) can be scored this way; the queue and overload labels need the future metrics
series (``ml/build_dataset.py``).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from . import REPO
from .registry import load_registry

DSN = os.environ.get("PREDISCHED_DSN",
                     "host=localhost port=5432 dbname=predisched user=postgres password=predisched")


def compare(records: list[dict], actual: dict[tuple[str, str], float], live: int | None = None,
            shadow: int | None = None) -> dict:
    """MAE of live and shadow exec-time predictions on the tasks both predicted and one ran.

    ``records``: prediction-log lines; ``actual``: (task_id, worker_id) -> actual exec ms.
    ``live`` / ``shadow`` restrict to answers from those M1 versions.
    """
    live_err, shadow_err = [], []
    seen = set()
    for r in records:
        shadow_preds = r.get("shadow_predictions") or []
        if not shadow_preds:
            continue
        if live is not None and r.get("m1_version") not in (None, live):
            continue
        if shadow is not None and r.get("shadow_version") != shadow:
            continue
        task = r.get("task_id")
        by_worker = {p["worker_id"]: p for p in r.get("predictions", [])}
        shadow_by_worker = {p["worker_id"]: p for p in shadow_preds}
        for worker, p in by_worker.items():
            key = (task, worker)
            if key in actual and worker in shadow_by_worker and key not in seen:
                seen.add(key)
                live_err.append(abs(p["pred_exec_ms"] - actual[key]))
                shadow_err.append(abs(shadow_by_worker[worker]["pred_exec_ms"] - actual[key]))
    n = len(live_err)
    return {"tasks": n,
            "live_mae": sum(live_err) / n if n else None,
            "shadow_mae": sum(shadow_err) / n if n else None,
            "live_version": live, "shadow_version": shadow}


def read_log(path: Path) -> list[dict]:
    records = []
    if path.exists():
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    return records


def actual_exec(task_ids: set[str]) -> dict[tuple[str, str], float]:
    import psycopg
    out = {}
    ids = list(task_ids)
    with psycopg.connect(DSN) as conn:
        for i in range(0, len(ids), 1000):
            rows = conn.execute(
                "SELECT task_id, worker_id, exec_time_ms FROM execution_history"
                " WHERE status = 'COMPLETED' AND task_id = ANY(%s)", (ids[i:i + 1000],)).fetchall()
            for task, worker, ms in rows:
                out[(task, worker)] = float(ms)
    return out


def compare_from_logs(model: str, shadow: int | None = None,
                      predictions: Path | None = None) -> dict | None:
    if model != "m1":
        return None
    entry = load_registry()["models"].get(model, {})
    shadow = shadow if shadow is not None else entry.get("shadow")
    records = read_log(predictions or REPO / "logs" / "predictions.jsonl")
    records = [r for r in records if r.get("shadow_predictions")]
    if not records:
        return {"tasks": 0, "live_mae": None, "shadow_mae": None,
                "live_version": entry.get("live"), "shadow_version": shadow}
    actual = actual_exec({r["task_id"] for r in records})
    return compare(records, actual, live=entry.get("live"), shadow=shadow)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--model", default="m1", choices=["m1"])
    parser.add_argument("--predictions", default=str(REPO / "logs" / "predictions.jsonl"))
    parser.add_argument("--shadow", type=int, default=None, help="default: the current shadow")
    args = parser.parse_args(argv)
    result = compare_from_logs(args.model, args.shadow, Path(args.predictions))
    if not result or not result["tasks"]:
        print(f"no completed tasks with both live and shadow {args.model} predictions in"
              f" {args.predictions}")
        return 1
    better = result["shadow_mae"] < result["live_mae"]
    print(f"{args.model}: {result['tasks']} completed tasks predicted by both versions")
    print(f"  live   v{result['live_version']}: MAE {result['live_mae']:.2f} ms")
    print(f"  shadow v{result['shadow_version']}: MAE {result['shadow_mae']:.2f} ms"
          f" ({(result['shadow_mae'] / result['live_mae'] - 1):+.1%} vs live:"
          f" {'better' if better else 'not better'})")
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
