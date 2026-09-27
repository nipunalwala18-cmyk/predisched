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


class _Columns:
    """Column access over a DataFrame, one mapping (a serving request) or a list of mappings."""

    def __init__(self, rows):
        if isinstance(rows, pd.DataFrame):
            self.frame, self.n, self.index = rows, len(rows), rows.index
            self.names = set(rows.columns)
        else:
            records = [dict(rows)] if isinstance(rows, Mapping) else [dict(r) for r in rows]
            self.frame, self.n, self.index = None, len(records), pd.RangeIndex(len(records))
            self.names = set().union(*records) if records else set()
            self.records = records

    def has(self, name: str) -> bool:
        return name in self.names

    def raw(self, name: str) -> np.ndarray:
        if name not in self.names:
            return np.full(self.n, DEFAULTS[name], dtype=object)
        if self.frame is not None:
            return self.frame[name].to_numpy()
        return np.array([r.get(name, DEFAULTS.get(name)) for r in self.records], dtype=object)

    def number(self, name: str, default: float | None = None) -> np.ndarray:
        values = self.raw(name)
        try:
            out = np.asarray(values, dtype=float)
        except (TypeError, ValueError):
            out = pd.to_numeric(pd.Series(values), errors="coerce").to_numpy(dtype=float)
        if default is not None:
            out = np.where(np.isnan(out), default, out)
        return out


def as_frame(rows: pd.DataFrame | Mapping | Iterable[Mapping]) -> pd.DataFrame:
    if isinstance(rows, pd.DataFrame):
        return rows
    if isinstance(rows, Mapping):
        return pd.DataFrame([dict(rows)])
    return pd.DataFrame([dict(r) for r in rows])


def feature_matrix(rows: pd.DataFrame | Mapping | Iterable[Mapping]) -> np.ndarray:
    """The features as an (n, len(FEATURE_NAMES)) float64 array. NumPy only, so one serving
    request of a few rows costs well under a millisecond (prompt 17's latency budget)."""
    cols = _Columns(rows)
    n = cols.n
    types = cols.raw("task_type").astype(str)
    size = cols.number("input_size") if cols.has("input_size") else np.full(n, np.nan)
    if cols.has("input"):
        texts = cols.raw("input")
        missing = np.isnan(size)
        if missing.any():
            size = size.copy()
            size[missing] = [input_size(t, x) for t, x in zip(types[missing], texts[missing])]
    log_size = np.log1p(np.clip(np.nan_to_num(size, nan=0.0), 0.0, None))

    type_hot = np.stack([(types == t) for t in TASK_TYPES], axis=1).astype(float)         if n else np.zeros((0, len(TASK_TYPES)))
    profiles = cols.raw("resource_profile").astype(str)
    profile_hot = np.stack([(profiles == p) for p in RESOURCE_PROFILES], axis=1).astype(float)         if n else np.zeros((0, len(RESOURCE_PROFILES)))
    numeric = np.stack([cols.number(name, DEFAULTS[name]) for name in NUMERIC], axis=1)         if n else np.zeros((0, len(NUMERIC)))
    pool = np.clip(numeric[:, NUMERIC.index("worker_pool_size")], 1.0, None)
    busy_ratio = numeric[:, NUMERIC.index("active_threads")] / pool
    queue_ratio = numeric[:, NUMERIC.index("queue_len")] / pool
    avg_exec = cols.number("avg_exec_recent", 0.0)
    count = cols.number("prior_type_worker_count", 0.0)
    mean = cols.number("prior_type_worker_mean_ms")
    has_prior = (count > 0) & ~np.isnan(mean)
    log_prior = np.where(has_prior, np.log1p(np.clip(np.nan_to_num(mean, nan=0.0), 0.0, None)),
                         0.0)
    return np.column_stack([
        type_hot, profile_hot, log_size, type_hot * log_size[:, None], numeric,
        busy_ratio, queue_ratio, np.log1p(np.clip(avg_exec, 0.0, None)),
        has_prior.astype(float), log_prior, np.log1p(np.clip(count, 0.0, None)),
    ]).astype(float)


def build_features(rows: pd.DataFrame | Mapping | Iterable[Mapping]) -> pd.DataFrame:
    """``feature_matrix`` as a DataFrame with ``FEATURE_NAMES`` columns; the index is preserved."""
    index = rows.index if isinstance(rows, pd.DataFrame) else None
    matrix = feature_matrix(rows)
    return pd.DataFrame(matrix, columns=FEATURE_NAMES,
                        index=index if index is not None else pd.RangeIndex(len(matrix)))


def to_log_ms(ms) -> np.ndarray:
    """M1's target transform: execution times span 1 ms to seconds, so the model fits log1p(ms)."""
    return np.log1p(np.asarray(ms, dtype=float))


def from_log_ms(values) -> np.ndarray:
    return np.expm1(np.asarray(values, dtype=float)).clip(min=0.0)


def is_finite(value) -> bool:
    return value is not None and not (isinstance(value, float) and math.isnan(value))
