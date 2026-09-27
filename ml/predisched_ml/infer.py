"""Batch inference entry point for ML_INFER_TASK (prompt 16, step 3).

    python -m predisched_ml.infer --model exec_time|queue_forecast|overload|m1|m2|m3
                                  --batch N [--seed S] [--json]

Loads the live model, draws ``batch`` rows from the dataset (seeded, with replacement), builds
their features and predicts. Prints one JSON line: timings (load, feature + predict), the model
version and a checksum of the predictions, so a worker can report reproducible output.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

import numpy as np

from .data import load_config, load_dataset
from .registry import load

ALIASES = {"exec_time": "m1", "queue_forecast": "m2", "queue": "m2", "overload": "m3",
           "m1": "m1", "m2": "m2", "m3": "m3"}


def run(model_name: str, batch: int, seed: int, config: dict | None = None) -> dict:
    model_id = ALIASES[model_name]
    config = config or load_config()
    started = time.perf_counter()
    model, meta = load(model_id)
    load_ms = (time.perf_counter() - started) * 1000
    df = load_dataset(config)
    rows = df.iloc[np.random.default_rng(seed).integers(0, len(df), batch)].reset_index(drop=True)
    started = time.perf_counter()
    if meta["kind"] == "classification":
        values = model.predict_proba(rows)
    else:
        values = model.predict(rows)
    predict_ms = (time.perf_counter() - started) * 1000
    values = np.asarray(values, dtype=float)
    return {"model": model_id, "name": meta["name"], "version": meta["version"],
            "candidate": meta["kept"]["candidate"], "batch": batch, "seed": seed,
            "load_ms": round(load_ms, 3), "predict_ms": round(predict_ms, 3),
            "per_row_us": round(predict_ms * 1000 / max(1, batch), 3),
            "mean": round(float(values.mean()), 6),
            "checksum": round(float(values.sum()), 6)}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--model", default="exec_time", choices=sorted(ALIASES))
    parser.add_argument("--batch", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--json", action="store_true", help="print one JSON line (default)")
    args = parser.parse_args(argv)
    if args.batch < 1:
        parser.error("--batch must be at least 1")
    print(json.dumps(run(args.model, args.batch, args.seed)), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
