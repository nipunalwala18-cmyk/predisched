"""The feature matrix (spec §12.1), shared by training and the prediction server.

``build_features`` is the only place a dataset frame (or a single serving request) becomes model
input, so training and serving cannot drift: same columns, same order, same transforms. Columns
that a serving request leaves out get the neutral values listed in ``DEFAULTS``.
"""

from __future__ import annotations

import math
import re
from typing import Iterable, Mapping

import numpy as np
import pandas as pd

# Catalogue types a worker can run (proto TaskType, minus WORKFLOW_TASK, which the scheduler
# expands, and IMAGE_TASK, which is reserved). Fixed so the one-hot columns never depend on data.
TASK_TYPES = [
    "CPU_TASK", "MATRIX_TASK", "SLEEP_TASK", "MAPREDUCE_TASK", "SORT_TASK", "HASH_TASK",
    "COMPRESS_TASK", "MONTE_CARLO_TASK", "FILE_IO_TASK", "HTTP_TASK", "DB_QUERY_TASK",
    "GRAPH_TASK", "ML_INFER_TASK",
]
RESOURCE_PROFILES = ["CPU_BOUND", "MEMORY_BOUND", "IO_BOUND", "NETWORK_BOUND", "PARALLEL"]

# The size of the work per type: the first required numeric key, as TaskInputSpec.inputSize.
SIZE_KEYS = {
    "CPU_TASK": "n", "SLEEP_TASK": "ms", "MATRIX_TASK": "size", "HASH_TASK": "rounds",
    "MONTE_CARLO_TASK": "samples", "SORT_TASK": "n", "COMPRESS_TASK": "size_mb",
    "GRAPH_TASK": "nodes", "FILE_IO_TASK": "size_mb", "HTTP_TASK": "timeout",
    "DB_QUERY_TASK": "rows", "ML_INFER_TASK": "batch",
}

NUMERIC = [
    "priority", "worker_cores", "worker_pool_size", "worker_slowdown",
    "concurrent_tasks_on_worker", "cpu_pct", "mem_pct", "active_threads", "queue_len",
    "arrival_rate", "win_arrival_rate", "win_queue_len_mean", "win_queue_len_max",
]
DEFAULTS = {
    "priority": 5, "worker_cores": 1, "worker_pool_size": 1, "worker_slowdown": 1.0,
    "concurrent_tasks_on_worker": 0, "cpu_pct": 0.0, "mem_pct": 0.0, "active_threads": 0,
    "queue_len": 0, "arrival_rate": 0.0, "avg_exec_recent": 0.0, "win_arrival_rate": 0.0,
    "win_queue_len_mean": 0.0, "win_queue_len_max": 0.0, "prior_type_worker_mean_ms": np.nan,
    "prior_type_worker_count": 0, "resource_profile": "", "input_size": np.nan,
}

FEATURE_NAMES = (
    [f"type_{t}" for t in TASK_TYPES]
    + [f"profile_{p}" for p in RESOURCE_PROFILES]
    + ["log_size"] + [f"log_size_x_{t}" for t in TASK_TYPES]
    + NUMERIC
    + ["busy_ratio", "queue_ratio", "log_avg_exec_recent",
       "has_prior", "log_prior_mean_ms", "log_prior_count"]
)


def input_size(task_type: str, text: str | None) -> float:
    """The work size parsed from a raw ``key=value, ...`` input, for serving requests."""
    if not text:
        return 0.0
    key = SIZE_KEYS.get(task_type)
    if key:
        match = re.search(rf"(?:^|,)\s*{re.escape(key)}\s*=\s*([0-9.]+)", text)
        if match:
            return float(match.group(1))
    return float(len(text))


def as_frame(rows: pd.DataFrame | Mapping | Iterable[Mapping]) -> pd.DataFrame:
    if isinstance(rows, pd.DataFrame):
        return rows
    if isinstance(rows, Mapping):
        return pd.DataFrame([dict(rows)])
    return pd.DataFrame([dict(r) for r in rows])


def build_features(rows: pd.DataFrame | Mapping | Iterable[Mapping]) -> pd.DataFrame:
    """One row of ``FEATURE_NAMES`` (float64) per input row; the index is preserved."""
    df = as_frame(rows)
    n = len(df)

    def col(name: str) -> pd.Series:
        if name in df.columns:
            return df[name]
        return pd.Series([DEFAULTS[name]] * n, index=df.index)

    types = df["task_type"].astype(str)
    if "input_size" in df.columns:
        size = pd.to_numeric(df["input_size"], errors="coerce")
    else:
        size = pd.Series(np.nan, index=df.index)
    if "input" in df.columns:
        parsed = [input_size(t, s) for t, s in zip(types, df["input"])]
        size = size.fillna(pd.Series(parsed, index=df.index))
    log_size = np.log1p(size.fillna(0.0).clip(lower=0.0).astype(float))

    out = {}
    for t in TASK_TYPES:
        out[f"type_{t}"] = (types == t).astype(float)
    profiles = col("resource_profile").astype(str)
    for p in RESOURCE_PROFILES:
        out[f"profile_{p}"] = (profiles == p).astype(float)
    out["log_size"] = log_size
    for t in TASK_TYPES:
        out[f"log_size_x_{t}"] = log_size * out[f"type_{t}"]
    for name in NUMERIC:
        out[name] = pd.to_numeric(col(name), errors="coerce").fillna(DEFAULTS[name]).astype(float)
    pool = out["worker_pool_size"].clip(lower=1.0)
    out["busy_ratio"] = out["active_threads"] / pool
    out["queue_ratio"] = out["queue_len"] / pool
    avg_exec = pd.to_numeric(col("avg_exec_recent"), errors="coerce").fillna(0.0)
    out["log_avg_exec_recent"] = np.log1p(avg_exec.clip(lower=0.0))
    count = pd.to_numeric(col("prior_type_worker_count"), errors="coerce").fillna(0.0)
    mean = pd.to_numeric(col("prior_type_worker_mean_ms"), errors="coerce")
    has_prior = (count > 0) & mean.notna()
    out["has_prior"] = has_prior.astype(float)
    out["log_prior_mean_ms"] = np.where(has_prior, np.log1p(mean.fillna(0.0).clip(lower=0.0)), 0.0)
    out["log_prior_count"] = np.log1p(count.clip(lower=0.0))

    features = pd.DataFrame(out, index=df.index)[FEATURE_NAMES].astype(float)
    return features


def to_log_ms(ms) -> np.ndarray:
    """M1's target transform: execution times span 1 ms to seconds, so the model fits log1p(ms)."""
    return np.log1p(np.asarray(ms, dtype=float))


def from_log_ms(values) -> np.ndarray:
    return np.expm1(np.asarray(values, dtype=float)).clip(min=0.0)


def is_finite(value) -> bool:
    return value is not None and not (isinstance(value, float) and math.isnan(value))
