"""Evaluate the live models (spec §12.2): metrics per evaluation set, per task type, and plots.

    python -m predisched_ml.evaluate --model m1|m2|m3|all

Re-splits the dataset exactly as training did and scores the live version of each model. It
writes ``ml/reports/<m>-report.md`` and these plots:

- M1 and M2: predicted vs actual, and residuals;
- M3: the ROC curve;
- all three: feature importance (impurity importance for the trees, |coefficient| on
  standardised features for the linear models).

The training-time candidate tables are in ``ml/reports/<m>-candidates.md``.
"""

from __future__ import annotations

import argparse
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from sklearn.metrics import roc_curve  # noqa: E402

from . import ML_DIR, metrics  # noqa: E402
from .data import file_hash, dataset_path, load_config, load_dataset, split  # noqa: E402
from .models import ExecTimeModel  # noqa: E402
from .registry import load  # noqa: E402
from .train import MODELS, REPORTS, candidate_table, fmt, target  # noqa: E402

# Reference palette slots (docs/components/mpi.md uses the same three).
BLUE, ORANGE, GREEN = "#2a78d6", "#eb6834", "#1baf7a"
SET_COLORS = {"test": BLUE, "holdout_pattern": ORANGE, "holdout_type": GREEN}
SET_TITLES = {"test": "time-split test", "holdout_pattern": "held-out pattern",
              "holdout_type": "held-out task type"}


def predict(model, frame: pd.DataFrame, kind: str) -> np.ndarray:
    if kind == "classification":
        return model.predict_proba(frame)
    return model.predict(frame)


def inner(model):
    return model.model if isinstance(model, ExecTimeModel) else model


def style(ax, title: str, xlabel: str, ylabel: str) -> None:
    ax.set_title(title, fontsize=11, loc="left")
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(True, color="#e5e5e5", linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)


def plot_regression(model_id: str, spec: dict, sets: dict, preds: dict) -> list[str]:
    files = []
    log_scale = model_id == "m1"
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    for name, frame in sets.items():
        y = target(frame, spec)
        p = preds[name]
        jitter = 0 if log_scale else np.random.default_rng(0).uniform(-0.15, 0.15, len(y))
        axes[0].scatter(y + jitter, p, s=8, alpha=0.35, color=SET_COLORS[name],
                        label=SET_TITLES[name], edgecolors="none")
        axes[1].scatter(p, y - p, s=8, alpha=0.35, color=SET_COLORS[name],
                        label=SET_TITLES[name], edgecolors="none")
    lo = 1 if log_scale else 0
    hi = max(max(target(f, spec).max() for f in sets.values()),
             max(np.max(v) for v in preds.values())) * 1.05
    axes[0].plot([lo, hi], [lo, hi], linestyle="--", color="#777777", linewidth=1,
                 label="perfect")
    if log_scale:
        axes[0].set_xscale("log")
        axes[0].set_yscale("log")
        axes[1].set_xscale("log")
    unit = " (ms)" if model_id == "m1" else ""
    style(axes[0], f"{model_id} {spec['name']}: predicted vs actual", f"actual{unit}",
          f"predicted{unit}")
    style(axes[1], "residuals", f"predicted{unit}", f"actual - predicted{unit}")
    axes[1].axhline(0, color="#777777", linewidth=1, linestyle="--")
    axes[0].legend(frameon=False, fontsize=9)
    fig.tight_layout()
    path = REPORTS / f"{model_id}-predicted-vs-actual.png"
    fig.savefig(path, dpi=130)
    plt.close(fig)
    files.append(path.name)
    return files


def plot_roc(model_id: str, spec: dict, sets: dict, preds: dict) -> list[str]:
    fig, ax = plt.subplots(figsize=(5.5, 5))
    for name, frame in sets.items():
        y = target(frame, spec)
        if len(np.unique(y)) < 2:
            continue
        fpr, tpr, _ = roc_curve(y, preds[name])
        auc = metrics.classification(y, preds[name], 0.5)["roc_auc"]
        ax.plot(fpr, tpr, color=SET_COLORS[name], linewidth=2,
                label=f"{SET_TITLES[name]} (AUC {auc:.3f})")
    ax.plot([0, 1], [0, 1], linestyle="--", color="#777777", linewidth=1, label="chance")
    style(ax, f"{model_id} {spec['name']}: ROC", "false positive rate", "true positive rate")
    ax.legend(frameon=False, fontsize=9, loc="lower right")
    fig.tight_layout()
    path = REPORTS / f"{model_id}-roc.png"
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return [path.name]


def plot_importance(model_id: str, spec: dict, model) -> list[str]:
    importances = inner(model).importances()
    if importances is None:
        return []
    top = importances.head(15)[::-1]
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.barh(top.index, top.to_numpy(), color=BLUE, height=0.6)
    style(ax, f"{model_id} {spec['name']}: feature importance (top 15)",
          "importance" if hasattr(inner(model).estimator, "feature_importances_")
          else "|coefficient| (standardised features)", "")
    ax.grid(True, axis="x", color="#e5e5e5")
    ax.grid(False, axis="y")
    fig.tight_layout()
    path = REPORTS / f"{model_id}-feature-importance.png"
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return [path.name]


def per_type(spec: dict, frame: pd.DataFrame, pred: np.ndarray, threshold: float) -> str:
    rows = []
    for task_type, part in frame.groupby("task_type"):
        idx = frame.index.get_indexer(part.index)
        y = target(part, spec)
        if spec["kind"] == "classification":
            m = metrics.classification(y, pred[idx], threshold)
            rows.append(f"| {task_type} | {m['n']:,} | {m['positives']:,} | {fmt(m['precision'])}"
                        f" | {fmt(m['recall'])} | {fmt(m['f1'])} |")
        else:
            m = metrics.regression(y, pred[idx])
            rows.append(f"| {task_type} | {m['n']:,} | {fmt(float(np.mean(y)), 1)}"
                        f" | {fmt(m['mae'])} | {fmt(m['rmse'])} |")
    if spec["kind"] == "classification":
        head = ["| task_type | rows | positives | precision | recall | F1 |",
                "| --- | --- | --- | --- | --- | --- |"]
    else:
        head = ["| task_type | rows | mean actual | MAE | RMSE |", "| --- | --- | --- | --- | --- |"]
    return "\n".join(head + rows)


def evaluate_model(model_id: str, config: dict, splits, log=print) -> dict:
    spec = config["models"][model_id]
    model, meta = load(model_id)
    if meta.get("data_hash") != file_hash(dataset_path(config)):
        log(f"warning: {model_id} v{meta['version']} was trained on a different dataset file")
    sets = splits.evaluation_sets()
    preds, results = {}, {}
    threshold = meta["kept"].get("threshold") or 0.5
    for name, frame in sets.items():
        preds[name] = predict(model, frame, spec["kind"])
        y = target(frame, spec)
        results[name] = (metrics.classification(y, preds[name], threshold)
                         if spec["kind"] == "classification" else metrics.regression(y, preds[name]))
    REPORTS.mkdir(parents=True, exist_ok=True)
    plots = (plot_roc(model_id, spec, sets, preds) if spec["kind"] == "classification"
             else plot_regression(model_id, spec, sets, preds))
    plots += plot_importance(model_id, spec, model)

    cols = (["n", "positives", "precision", "recall", "f1", "roc_auc"]
            if spec["kind"] == "classification" else ["n", "mae", "rmse", "r2"])
    lines = [f"# {model_id}: {spec['name']} v{meta['version']} ({meta['kept']['candidate']})", "",
             f"Kept because: {meta['kept']['reason']}.", "",
             "## Live model on each evaluation set", "",
             "| set | " + " | ".join(c.upper().replace("_", "-") for c in cols) + " |",
             "| --- |" + " --- |" * len(cols)]
    for name, m in results.items():
        lines.append(f"| {SET_TITLES[name]} | " + " | ".join(fmt(m[c]) for c in cols) + " |")
    if isinstance(model, ExecTimeModel):
        cold = sets["holdout_type"]
        _, flags = model.predict_with_flags(cold)
        lines += ["", f"Cold start: {int(flags.sum()):,} of {len(cold):,} held-out-type rows"
                  f" ({', '.join(sorted(set(cold['task_type'])))}) were flagged `cold_start` and"
                  " predicted from the type's running mean (else the global mean)."]
    for name in sets:
        lines += ["", f"## Per task type: {SET_TITLES[name]}", "",
                  per_type(spec, sets[name], preds[name], threshold)]
    lines += ["", "## Candidates (from training)", ""]
    for name in sets:
        lines += [f"### {SET_TITLES[name]}", "", candidate_table(spec, meta["candidates"], name), ""]
    lines += ["## Plots", ""] + [f"![{p}]({p})" for p in plots]
    (REPORTS / f"{model_id}-report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    log("\n".join(lines[:9 + len(results)]))
    log(f"plots: {', '.join('ml/reports/' + p for p in plots)}")
    return results


def main(argv=None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="Evaluate the live prediction models.")
    parser.add_argument("--model", default="all", choices=MODELS + ["all"])
    parser.add_argument("--config", default=None)
    args = parser.parse_args(argv)
    config = load_config(args.config)
    splits = split(load_dataset(config), float(config["testFraction"]))
    for model_id in (MODELS if args.model == "all" else [args.model]):
        evaluate_model(model_id, config, splits)
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
