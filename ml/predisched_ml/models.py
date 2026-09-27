"""Candidate models: the baselines and the feature-based estimators, behind one small interface.

Every candidate takes the dataset frame (never a prepared matrix), so a saved model carries its own
feature step and the prediction server only hands it rows. Regressors return predictions in the
target's own unit (ms, queue length); classifiers return the probability of the positive class
and a label at their ``threshold``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier, XGBRegressor

from .features import FEATURE_NAMES, feature_matrix, from_log_ms, to_log_ms

# Simplest first: the order the selection rule walks.
FAMILIES = ["baseline", "linear", "random_forest", "xgboost"]


class Candidate:
    family = "baseline"
    kind = "regression"

    def __init__(self, name: str, params: dict | None = None):
        self.name = name
        self.params = dict(params or {})
        self.threshold = 0.5

    def fit(self, frame: pd.DataFrame, y: np.ndarray) -> "Candidate":
        raise NotImplementedError

    def predict(self, frame: pd.DataFrame) -> np.ndarray:
        if self.kind == "classification":
            return self.predict_proba(frame) >= self.threshold
        raise NotImplementedError

    def predict_proba(self, frame: pd.DataFrame) -> np.ndarray:
        raise NotImplementedError

    def importances(self) -> pd.Series | None:
        return None

    def describe(self) -> str:
        params = ", ".join(f"{k}={v}" for k, v in self.params.items())
        return f"{self.name}({params})" if params else self.name


# --- baselines ------------------------------------------------------------------------------
class GroupMeanRegressor(Candidate):
    """The historical mean of the target per group (task type for M1, worker for M2)."""

    def __init__(self, key: str):
        super().__init__(f"mean per {key}")
        self.key = key
        self.means: dict = {}
        self.global_mean = 0.0

    def fit(self, frame, y):
        y = pd.Series(np.asarray(y, dtype=float), index=frame.index)
        self.means = y.groupby(frame[self.key]).mean().to_dict()
        self.global_mean = float(y.mean())
        return self

    def predict(self, frame):
        return frame[self.key].map(self.means).fillna(self.global_mean).to_numpy(dtype=float)


class PersistenceRegressor(Candidate):
    """Naive forecast: the value in N seconds is the value now (the queue at dispatch)."""

    def __init__(self, column: str):
        super().__init__(f"persistence ({column} now)")
        self.column = column

    def fit(self, frame, y):
        return self

    def predict(self, frame):
        return frame[self.column].to_numpy(dtype=float)


class MajorityClassifier(Candidate):
    kind = "classification"

    def __init__(self):
        super().__init__("majority class")
        self.rate = 0.0

    def fit(self, frame, y):
        self.rate = float(np.mean(y))
        self.threshold = 0.5  # always the majority label; the rate still ranks nothing
        return self

    def predict_proba(self, frame):
        return np.full(len(frame), self.rate)


class GroupRateClassifier(Candidate):
    """The historical positive rate per group (per worker for M3)."""
    kind = "classification"

    def __init__(self, key: str):
        super().__init__(f"rate per {key}")
        self.key = key
        self.rates: dict = {}
        self.global_rate = 0.0

    def fit(self, frame, y):
        y = pd.Series(np.asarray(y, dtype=float), index=frame.index)
        self.rates = y.groupby(frame[self.key]).mean().to_dict()
        self.global_rate = float(y.mean())
        return self

    def predict_proba(self, frame):
        return frame[self.key].map(self.rates).fillna(self.global_rate).to_numpy(dtype=float)


# --- feature-based --------------------------------------------------------------------------
class FeatureRegressor(Candidate):
    def __init__(self, family: str, estimator, params: dict, log_target: bool):
        super().__init__(family.replace("_", " "), params)
        self.family = family
        self.estimator = estimator
        self.log_target = log_target

    def fit(self, frame, y):
        target = to_log_ms(y) if self.log_target else np.asarray(y, dtype=float)
        self.estimator.fit(feature_matrix(frame), target)
        return self

    def predict(self, frame):
        raw = self.estimator.predict(feature_matrix(frame))
        return from_log_ms(raw) if self.log_target else np.clip(raw, 0.0, None)

    def importances(self):
        return _importances(self.estimator)


class FeatureClassifier(Candidate):
    kind = "classification"

    def __init__(self, family: str, estimator, params: dict):
        super().__init__(family.replace("_", " "), params)
        self.family = family
        self.estimator = estimator

    def fit(self, frame, y):
        self.estimator.fit(feature_matrix(frame), np.asarray(y, dtype=int))
        return self

    def predict_proba(self, frame):
        return self.estimator.predict_proba(feature_matrix(frame))[:, 1]

    def importances(self):
        return _importances(self.estimator)


def _importances(estimator) -> pd.Series | None:
    model = estimator.steps[-1][1] if hasattr(estimator, "steps") else estimator
    if hasattr(model, "feature_importances_"):
        values = np.asarray(model.feature_importances_, dtype=float)
    elif hasattr(model, "coef_"):
        # Coefficients on standardised features: comparable in size.
        values = np.abs(np.asarray(model.coef_, dtype=float)).ravel()
    else:
        return None
    return pd.Series(values, index=FEATURE_NAMES).sort_values(ascending=False)


# --- factories ------------------------------------------------------------------------------
def baselines(model: str) -> list[Candidate]:
    """Always evaluated. M1: mean per task type. M2: persistence and mean per worker.
    M3: majority class and the positive rate per worker."""
    if model == "m1":
        return [GroupMeanRegressor("task_type")]
    if model == "m2":
        return [PersistenceRegressor("queue_len"), GroupMeanRegressor("worker_id")]
    return [MajorityClassifier(), GroupRateClassifier("worker_id")]


def make(model: str, family: str, params: dict, seed: int, positive_weight: float = 1.0
         ) -> Candidate:
    """A feature-based candidate of ``family`` with ``params`` for model m1, m2 or m3."""
    classification = model == "m3"
    if family == "linear":
        if classification:
            estimator = make_pipeline(StandardScaler(), LogisticRegression(
                C=params["C"], class_weight="balanced", max_iter=5000))
        else:
            estimator = make_pipeline(StandardScaler(), Ridge(alpha=params["alpha"]))
    elif family == "random_forest":
        cls = RandomForestClassifier if classification else RandomForestRegressor
        extra = {"class_weight": "balanced_subsample"} if classification else {}
        estimator = cls(random_state=seed, n_jobs=-1, **params, **extra)
    elif family == "xgboost":
        if classification:
            estimator = XGBClassifier(random_state=seed, n_jobs=4, tree_method="hist",
                                      scale_pos_weight=positive_weight, eval_metric="logloss",
                                      **params)
        else:
            estimator = XGBRegressor(random_state=seed, n_jobs=4, tree_method="hist", **params)
    else:
        raise ValueError(f"unknown family {family}")
    if classification:
        return FeatureClassifier(family, estimator, params)
    return FeatureRegressor(family, estimator, params, log_target=(model == "m1"))


class ExecTimeModel:
    """M1 as served: the kept regressor plus the cold-start rule (prompt 16, step 2).

    A task type the model never saw in training gets no useful signal from its one-hot column,
    so its prediction falls back to the type's running mean (``type_running_mean_ms`` from the
    scheduler, or the dataset's leak-free ``prior_type_worker_mean_ms``), else the global mean,
    and is flagged ``cold_start``.
    """

    def __init__(self, model: Candidate, known_types: set[str], global_mean_ms: float):
        self.model = model
        self.known_types = set(known_types)
        self.global_mean_ms = float(global_mean_ms)

    def predict_with_flags(self, frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        pred = np.asarray(self.model.predict(frame), dtype=float)
        cold = ~frame["task_type"].isin(self.known_types).to_numpy()
        if cold.any():
            fallback = pd.Series(self.global_mean_ms, index=frame.index, dtype=float)
            for column, count in (("prior_type_worker_mean_ms", "prior_type_worker_count"),
                                  ("type_running_mean_ms", None)):
                if column in frame.columns:
                    value = pd.to_numeric(frame[column], errors="coerce")
                    ok = value.notna() & (value > 0)
                    if count and count in frame.columns:
                        ok &= pd.to_numeric(frame[count], errors="coerce").fillna(0) > 0
                    fallback = fallback.where(~ok, value)
            pred = np.where(cold, fallback.to_numpy(dtype=float), pred)
        return pred, cold

    def predict(self, frame: pd.DataFrame) -> np.ndarray:
        return self.predict_with_flags(frame)[0]
