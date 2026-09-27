"""Train M1 (execution time), M2 (queue forecast) and M3 (overload risk), spec §12.2.

    python -m predisched_ml.train --model m1|m2|m3|all [--config ml/config.yaml]

For each model:

1. Time split: the earliest 80 % of the non-held-out rows (by dispatch time) train, the rest test.
   The held-out pattern and task type are separate test sets (``data.split``).
2. Candidates in order: the baselines (always), then linear / logistic regression, random forest
   and XGBoost, each with a small grid searched by time-series CV on the training part only.
3. Selection on the CV score, never the test set: the simplest family whose CV primary metric
   beats the best baseline's by the configured margin is kept, and the reason is recorded.
4. Every candidate (best grid point per family) is refitted on the whole training part and scored
   on the test, held-out-pattern and held-out-type sets. The kept one is saved as a new version.
"""

from __future__ import annotations

import argparse
import datetime as dt
import itertools
import json
import math
import sys
import time

import numpy as np
import pandas as pd
import sklearn
import xgboost
from sklearn.model_selection import TimeSeriesSplit

from . import ML_DIR, metrics
from .data import Splits, dataset_path, file_hash, load_config, load_dataset, split
from .features import FEATURE_NAMES
from .models import FAMILIES, Candidate, ExecTimeModel, baselines, make
from .registry import save

REPORTS = ML_DIR / "reports"
MODELS = ["m1", "m2", "m3"]


def target(frame: pd.DataFrame, spec: dict) -> np.ndarray:
    values = frame[spec["target"]]
    return values.astype(int).to_numpy() if spec["kind"] == "classification" \
        else values.to_numpy(dtype=float)


def grid(config: dict, family: str, model: str) -> list[dict]:
    space = dict(config["grids"][family])
    if family == "linear":
        space = {"C": space["C"]} if model == "m3" else {"alpha": space["alpha"]}
    keys = sorted(space)
    return [dict(zip(keys, values)) for values in itertools.product(*(space[k] for k in keys))]


def cross_validate(build, train: pd.DataFrame, y: np.ndarray, spec: dict, folds: int,
                   tune_threshold: bool = True) -> tuple[float, float]:
    """Pooled out-of-fold primary metric over TimeSeriesSplit folds; (score, threshold).

    For M3 the threshold with the best F1 on the pooled out-of-fold probabilities is chosen here,
    so the test set never picks it.
    """
    oof_true, oof_pred = [], []
    for fit_idx, val_idx in TimeSeriesSplit(n_splits=folds).split(train):
        model = build().fit(train.iloc[fit_idx], y[fit_idx])
        part = train.iloc[val_idx]
        oof_true.append(y[val_idx])
        oof_pred.append(model.predict_proba(part) if spec["kind"] == "classification"
                        else model.predict(part))
    y_true, y_pred = np.concatenate(oof_true), np.concatenate(oof_pred)
    if spec["kind"] == "classification":
        if tune_threshold:
            threshold, f1 = metrics.best_f1_threshold(y_true, y_pred)
        else:
            threshold = 0.5
            f1 = metrics.classification(y_true, y_pred, threshold)["f1"]
        return f1, threshold
    return metrics.regression(y_true, y_pred)[spec["primary"]], math.nan


def score(model: Candidate, frame: pd.DataFrame, y: np.ndarray, spec: dict) -> dict:
    if spec["kind"] == "classification":
        return metrics.classification(y, model.predict_proba(frame), model.threshold)
    return metrics.regression(y, model.predict(frame))


def train_model(model_id: str, config: dict, splits: Splits, log=print) -> dict:
    spec = config["models"][model_id]
    seed = int(config["seed"])
    folds = int(config["cvSplits"])
    primary = spec["primary"]
    train = splits.train
    y = target(train, spec)
    positive_weight = float((y == 0).sum() / max(1, (y == 1).sum())) \
        if spec["kind"] == "classification" else 1.0

    log(f"\n== {model_id} ({spec['name']}): target {spec['target']}, {len(train)} training rows,"
        f" {folds}-fold time-series CV, primary metric {primary}")
    cv_rows = []
    best: dict[str, dict] = {}   # family -> {"score", "params", "threshold", "build"}

    for base in baselines(model_id):
        tune = not base.name.startswith("majority")
        started = time.time()
        cv_score, threshold = cross_validate(lambda b=base: _clone(b), train, y, spec, folds,
                                             tune_threshold=tune)
        cv_rows.append({"family": "baseline", "candidate": base.describe(), "params": {},
                        "cv": cv_score, "threshold": threshold, "seconds": time.time() - started})
        entry = best.get("baseline")
        if entry is None or metrics.better(cv_score, entry["score"], primary):
            best["baseline"] = {"score": cv_score, "params": {}, "threshold": threshold,
                                "build": lambda b=base: _clone(b), "name": base.describe()}
        best.setdefault("_baselines", []).append({"build": lambda b=base: _clone(b),
                                                  "threshold": threshold, "cv": cv_score})
    for family in FAMILIES[1:]:
        for params in grid(config, family, model_id):
            started = time.time()
            build = (lambda f=family, p=params: make(model_id, f, p, seed, positive_weight))
            cv_score, threshold = cross_validate(build, train, y, spec, folds)
            cv_rows.append({"family": family, "candidate": build().describe(), "params": params,
                            "cv": cv_score, "threshold": threshold,
                            "seconds": time.time() - started})
            entry = best.get(family)
            if entry is None or metrics.better(cv_score, entry["score"], primary):
                best[family] = {"score": cv_score, "params": params, "threshold": threshold,
                                "build": build, "name": build().describe()}
    for row in cv_rows:
        cut = "" if math.isnan(row["threshold"]) else f"  threshold={row['threshold']:.3f}"
        log(f"  cv {row['family']:<14} {row['candidate']:<60} {primary}={row['cv']:.4f}{cut}")

    # Selection: the simplest family that clearly beats the best baseline (on CV).
    margin = float(config["margin"])
    base_score = best["baseline"]["score"]
    kept_family, reason = "baseline", None
    for family in FAMILIES[1:]:
        if metrics.beats(best[family]["score"], base_score, primary, margin):
            kept_family = family
            reason = (f"{best[family]['name']} is the simplest candidate whose CV {primary}"
                      f" ({best[family]['score']:.4f}) beats the best baseline"
                      f" ({best['baseline']['name']}, {base_score:.4f}) by at least"
                      f" {margin:.0%}")
            skipped = [f for f in FAMILIES[1:FAMILIES.index(family)]]
            if skipped:
                reason += "; " + ", ".join(
                    f"{f.replace('_', ' ')} ({best[f]['score']:.4f}) did not" for f in skipped)
            better_later = [f for f in FAMILIES[FAMILIES.index(family) + 1:]
                            if metrics.better(best[f]["score"], best[family]["score"], primary)]
            if better_later:
                reason += "; " + ", ".join(
                    f"{f.replace('_', ' ')} scores better ({best[f]['score']:.4f}) but is more"
                    f" complex" for f in better_later)
            break
    if reason is None:
        reason = (f"no candidate beat the best baseline ({best['baseline']['name']},"
                  f" CV {primary} {base_score:.4f}) by {margin:.0%}: the baseline is kept")
    log(f"  kept: {kept_family} -- {reason}")

    # Refit every family's best grid point (and every baseline) on the whole training part.
    fitted: list[tuple[str, Candidate]] = []
    for base in best["_baselines"]:
        model = base["build"]().fit(train, y)
        if spec["kind"] == "classification" and not math.isnan(base["threshold"]):
            model.threshold = base["threshold"]
        fitted.append(("baseline", model))
    for family in FAMILIES[1:]:
        model = best[family]["build"]().fit(train, y)
        if spec["kind"] == "classification":
            model.threshold = best[family]["threshold"]
        fitted.append((family, model))

    kept_name = best[kept_family]["name"]
    kept = next(m for f, m in fitted if f == kept_family and m.describe() == kept_name)
    served: object = kept
    if model_id == "m1":
        served = ExecTimeModel(kept, set(train["task_type"]), float(np.mean(y)))

    results = []
    for set_name, frame in splits.evaluation_sets().items():
        y_set = target(frame, spec)
        for family, model in fitted:
            row = {"set": set_name, "family": family, "candidate": model.describe(),
                   "kept": model is kept, **score(model, frame, y_set, spec)}
            results.append(row)
        if model_id == "m1":
            pred, cold = served.predict_with_flags(frame)
            results.append({"set": set_name, "family": kept_family,
                            "candidate": f"{kept.describe()} + cold-start fallback",
                            "kept": False, "cold_start_rows": int(cold.sum()),
                            **metrics.regression(y_set, pred)})

    kept_metrics = {r["set"]: {k: v for k, v in r.items()
                               if k not in ("set", "family", "candidate", "kept")}
                    for r in results if r["kept"]}
    meta = {
        "name": spec["name"], "target": spec["target"], "kind": spec["kind"],
        "primary_metric": primary, "features": FEATURE_NAMES,
        "kept": {"family": kept_family, "candidate": kept.describe(),
                 "params": best[kept_family]["params"],
                 "threshold": None if spec["kind"] != "classification" else kept.threshold,
                 "cv": best[kept_family]["score"], "reason": reason,
                 "cold_start": model_id == "m1"},
        "metrics": kept_metrics,
        "candidates": results,
        "cv": [{k: v for k, v in r.items()} for r in cv_rows],
        "split": {"train": len(splits.train), "test": len(splits.test),
                  "holdout_pattern": len(splits.holdout_pattern),
                  "holdout_type": len(splits.holdout_type),
                  "train_until": str(splits.train["dispatched_at"].max()),
                  "test_from": str(splits.test["dispatched_at"].min()),
                  "test_fraction": config["testFraction"], "cv_splits": folds},
        "margin": margin, "seed": seed,
        "dataset": relative(dataset_path(config)),
        "data_hash": file_hash(dataset_path(config)),
        "trained_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "libraries": {"scikit-learn": sklearn.__version__, "xgboost": xgboost.__version__,
                      "pandas": pd.__version__, "numpy": np.__version__},
    }
    folder = save(model_id, served, meta)
    log(f"  saved {relative(folder)}")
    write_candidate_table(model_id, spec, meta)
    return meta


def relative(path) -> str:
    try:
        return str(path.relative_to(ML_DIR)).replace("\\", "/")
    except ValueError:
        return str(path)


def _clone(candidate: Candidate) -> Candidate:
    """A fresh, unfitted copy of a baseline (they hold their grouping key only)."""
    fresh = type(candidate).__new__(type(candidate))
    fresh.__dict__.update({k: v for k, v in candidate.__dict__.items()})
    for attr in ("means", "rates"):
        if hasattr(fresh, attr):
            setattr(fresh, attr, {})
    return fresh


def fmt(value, digits: int = 3) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "n/a"
    if isinstance(value, float):
        return f"{value:,.{digits}f}"
    return f"{value:,}"


def candidate_table(spec: dict, rows: list[dict], set_name: str) -> str:
    if spec["kind"] == "classification":
        cols = ["precision", "recall", "f1", "roc_auc"]
    else:
        cols = ["mae", "rmse", "r2"]
    lines = ["| candidate | " + " | ".join(c.upper().replace("_", "-") for c in cols) + " |",
             "| --- |" + " --- |" * len(cols)]
    for r in rows:
        if r["set"] != set_name:
            continue
        name = f"**{r['candidate']}** (kept)" if r["kept"] else r["candidate"]
        if r.get("cold_start_rows"):
            name += f" [{r['cold_start_rows']} cold-start rows]"
        lines.append(f"| {name} | " + " | ".join(fmt(r[c]) for c in cols) + " |")
    return "\n".join(lines)


def write_candidate_table(model_id: str, spec: dict, meta: dict) -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)
    parts = [f"# {model_id}: {spec['name']} ({spec['target']}), v{meta['version']}", "",
             f"Kept: **{meta['kept']['candidate']}**. {meta['kept']['reason']}.", "",
             f"Split: {meta['split']['train']:,} train / {meta['split']['test']:,} test rows"
             f" (time split), {meta['split']['holdout_pattern']:,} held-out-pattern rows,"
             f" {meta['split']['holdout_type']:,} held-out-type rows.", "",
             "## Cross-validation (training part only; drives the selection)", "",
             f"| family | candidate | CV {spec['primary']} |", "| --- | --- | --- |"]
    for r in meta["cv"]:
        extra = "" if r["threshold"] is None or (isinstance(r["threshold"], float) and
                                                 math.isnan(r["threshold"])) \
            else f" (threshold {r['threshold']:.3f})"
        parts.append(f"| {r['family']} | {r['candidate']} | {fmt(r['cv'], 4)}{extra} |")
    titles = {"test": "Time-split test set", "holdout_pattern": "Held-out pattern",
              "holdout_type": "Held-out task type"}
    for set_name, title in titles.items():
        parts += ["", f"## {title}", "", candidate_table(spec, meta["candidates"], set_name)]
    (REPORTS / f"{model_id}-candidates.md").write_text("\n".join(parts) + "\n", encoding="utf-8")


def main(argv=None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="Train the prediction models (spec 12.2).")
    parser.add_argument("--model", default="all", choices=MODELS + ["all"])
    parser.add_argument("--config", default=None)
    args = parser.parse_args(argv)
    config = load_config(args.config)
    df = load_dataset(config)
    splits = split(df, float(config["testFraction"]))
    print(f"dataset {dataset_path(config).name}: {len(df):,} rows; train {len(splits.train):,},"
          f" test {len(splits.test):,}, held-out pattern {len(splits.holdout_pattern):,},"
          f" held-out type {len(splits.holdout_type):,}")
    summary = {}
    for model_id in (MODELS if args.model == "all" else [args.model]):
        meta = train_model(model_id, config, splits)
        spec = config["models"][model_id]
        print()
        print(candidate_table(spec, meta["candidates"], "test"))
        summary[model_id] = {"version": meta["version"], "kept": meta["kept"]["candidate"],
                             "test": meta["metrics"].get("test")}
    print("\n" + json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
