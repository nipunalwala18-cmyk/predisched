"""Metrics (spec §12.2): MAE, RMSE, R² for regression; precision, recall, F1, ROC-AUC for
classification."""

from __future__ import annotations

import math

import numpy as np
from sklearn.metrics import (f1_score, mean_absolute_error, mean_squared_error,
                             precision_recall_curve, precision_score, r2_score, recall_score,
                             roc_auc_score)

LOWER_IS_BETTER = {"mae", "rmse"}


def regression(y_true, y_pred) -> dict:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    if len(y_true) == 0:
        return {"n": 0, "mae": math.nan, "rmse": math.nan, "r2": math.nan}
    return {"n": int(len(y_true)),
            "mae": float(mean_absolute_error(y_true, y_pred)),
            "rmse": float(math.sqrt(mean_squared_error(y_true, y_pred))),
            "r2": float(r2_score(y_true, y_pred)) if len(y_true) > 1 else math.nan}


def classification(y_true, proba, threshold: float) -> dict:
    y_true = np.asarray(y_true, dtype=int)
    proba = np.asarray(proba, dtype=float)
    y_pred = (proba >= threshold).astype(int)
    both_classes = len(np.unique(y_true)) == 2
    return {"n": int(len(y_true)), "positives": int(y_true.sum()),
            "precision": float(precision_score(y_true, y_pred, zero_division=0)),
            "recall": float(recall_score(y_true, y_pred, zero_division=0)),
            "f1": float(f1_score(y_true, y_pred, zero_division=0)),
            "roc_auc": float(roc_auc_score(y_true, proba)) if both_classes else math.nan}


def best_f1_threshold(y_true, proba) -> tuple[float, float]:
    """The probability cut-off with the highest F1, and that F1 (0.5 and 0 without positives)."""
    y_true = np.asarray(y_true, dtype=int)
    if y_true.sum() == 0:
        return 0.5, 0.0
    precision, recall, thresholds = precision_recall_curve(y_true, proba)
    f1 = np.where(precision + recall > 0, 2 * precision * recall / (precision + recall + 1e-12), 0)
    best = int(np.argmax(f1[:-1])) if len(thresholds) else 0
    return float(thresholds[best]) if len(thresholds) else 0.5, float(f1[best])


def beats(candidate: float, baseline: float, metric: str, margin: float) -> bool:
    """True when ``candidate`` is better than ``baseline`` by at least the relative margin."""
    if math.isnan(candidate):
        return False
    if metric in LOWER_IS_BETTER:
        return candidate <= (1.0 - margin) * baseline
    if baseline <= 0:
        return candidate >= margin
    return candidate >= (1.0 + margin) * baseline


def better(a: float, b: float, metric: str) -> bool:
    if math.isnan(b):
        return not math.isnan(a)
    return a < b if metric in LOWER_IS_BETTER else a > b
