"""Shared test data: a small dataset with the real columns (see test_models, test_server)."""

import numpy as np
import pandas as pd

T0 = pd.Timestamp("2026-09-27 10:00:00", tz="UTC")
TYPES = ["CPU_TASK", "SLEEP_TASK", "SORT_TASK", "GRAPH_TASK"]
PROFILES = {"CPU_TASK": "CPU_BOUND", "SLEEP_TASK": "IO_BOUND", "SORT_TASK": "MEMORY_BOUND",
            "GRAPH_TASK": "MEMORY_BOUND"}


def synthetic(n=600, seed=0):
    """A small dataset with the real columns; exec time depends on size, type and slowdown."""
    rng = np.random.default_rng(seed)
    types = rng.choice(TYPES, n)
    workers = rng.choice(["worker-1", "worker-2"], n)
    slowdown = np.where(workers == "worker-1", 2.0, 1.0)
    size = rng.integers(10, 1000, n)
    scale = pd.Series(types).map({"CPU_TASK": 0.05, "SLEEP_TASK": 1.0, "SORT_TASK": 0.2,
                                  "GRAPH_TASK": 0.5}).to_numpy()
    exec_ms = (scale * size * slowdown * rng.uniform(0.9, 1.1, n)).round().astype(int) + 1
    queue = rng.integers(0, 3, n) * (workers == "worker-1")
    return pd.DataFrame({
        "id": np.arange(n), "dispatched_at": [T0 + pd.Timedelta(seconds=i) for i in range(n)],
        "task_type": types, "resource_profile": [PROFILES[t] for t in types],
        "priority": rng.integers(1, 10, n), "input_size": size, "strategy": "round_robin",
        "worker_id": workers, "worker_cores": 8, "worker_pool_size": np.where(slowdown > 1, 2, 4),
        "worker_slowdown": slowdown, "concurrent_tasks_on_worker": queue, "cpu_pct": 5.0,
        "mem_pct": 1.0, "active_threads": queue, "queue_len": queue, "arrival_rate": 1.0,
        "avg_exec_recent": 50.0, "prior_type_worker_mean_ms": np.nan,
        "prior_type_worker_count": 0, "win_arrival_rate": 1.0, "win_queue_len_mean": 0.0,
        "win_queue_len_max": 0.0, "exec_time_ms": exec_ms, "wait_time_ms": 1,
        "queue_len_future": queue, "overloaded_future": queue >= 2,
        "holdout_pattern": rng.random(n) < 0.2, "holdout_task_type": types == "GRAPH_TASK",
    })
