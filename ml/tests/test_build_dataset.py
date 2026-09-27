"""Labels on a hand-made metrics series, no future leakage, and a deterministic campaign plan."""

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import build_dataset as bd

REPO = Path(__file__).resolve().parents[2]
T0 = pd.Timestamp("2026-09-27 10:00:00", tz="UTC")


def at(seconds: float) -> pd.Timestamp:
    return T0 + pd.Timedelta(seconds=seconds)


@pytest.fixture
def metrics():
    # worker-1 (pool 2): one sample per second; queue 0 until 6 s, then 2 (= pool: overloaded)
    # from 6 to 8 s, 1 afterwards. CPU crosses 85 % once, at 13 s. worker-2 goes silent after 3 s.
    rows = []
    for s in range(0, 16):
        queue = 2 if 6 <= s <= 8 else (0 if s < 6 else 1)
        rows.append(("worker-1", at(s), 90.0 if s == 13 else 20.0, queue))
    for s in range(0, 4):
        rows.append(("worker-2", at(s), 10.0, 0))
    return pd.DataFrame(rows, columns=["worker_id", "ts", "cpu_pct", "queue_len"])


def labels(history, metrics):
    return bd.compute_labels(history, metrics, {"worker-1": 2, "worker-2": 4}, horizon_s=5,
                             cpu_threshold=85.0, queue_factor=1.0, tolerance_s=2.0)


def test_future_queue_and_overload_on_a_known_series(metrics):
    history = pd.DataFrame({"worker_id": ["worker-1"] * 5,
                            "dispatched_at": [at(0.5), at(2.0), at(3.5), at(8.5), at(9.0)]})
    out = labels(history, metrics)
    # t=0.5: t+5 = 5.5 -> sample at 5 s (queue 0); (0.5, 5.5] has no overload.
    # t=2.0: t+5 = 7.0 -> sample at 7 s (queue 2); 6 and 7 s are overloaded.
    # t=3.5: t+5 = 8.5 -> sample at 8 s (queue 2).
    # t=8.5: t+5 = 13.5 -> sample at 13 s (queue 1); CPU 90 % at 13 s -> overloaded.
    # t=9.0: (9, 14] includes 13 s -> overloaded; the queue at 14 s is 1.
    assert out["queue_len_future"].tolist() == [0, 2, 2, 1, 1]
    assert out["overloaded_future"].tolist() == [False, True, True, True, True]


def test_missing_future_samples_give_missing_labels(metrics):
    history = pd.DataFrame({"worker_id": ["worker-2", "worker-2", "worker-3"],
                            "dispatched_at": [at(0.0), at(2.5), at(1.0)]})
    out = labels(history, metrics)
    # worker-2: t+5 = 5 s, last sample 3 s is 2 s old -> within tolerance; t+5 = 7.5 is 4.5 s
    # after its last sample -> missing, and (2.5, 7.5] holds one sample (3 s) -> not overloaded.
    assert out["queue_len_future"].iloc[0] == 0
    assert np.isnan(out["queue_len_future"].iloc[1])
    assert out["overloaded_future"].iloc[1] is False or out["overloaded_future"].iloc[1] == False  # noqa: E712
    # worker-3 has no metrics at all.
    assert np.isnan(out["queue_len_future"].iloc[2])
    assert pd.isna(out["overloaded_future"].iloc[2])


def test_prior_stats_only_use_executions_finished_before_dispatch():
    history = pd.DataFrame({
        "task_type": ["CPU_TASK"] * 4,
        "worker_id": ["worker-1"] * 4,
        "status": ["COMPLETED", "COMPLETED", "FAILED", "COMPLETED"],
        "dispatched_at": [at(0), at(1), at(2), at(3)],
        "wait_time_ms": [0, 0, 0, 0],
        # finishes at 0.5 s, 5 s, 2.1 s, 3.2 s
        "exec_time_ms": [500, 4000, 100, 200],
    })
    out = bd.prior_stats(history)
    assert out["prior_type_worker_count"].tolist() == [0, 1, 1, 1]
    assert np.isnan(out["prior_type_worker_mean_ms"].iloc[0])
    assert out["prior_type_worker_mean_ms"].iloc[1:].tolist() == [500.0, 500.0, 500.0]
    assert out["prior_stats_ts"].iloc[3] == at(0.5)


def test_no_feature_timestamp_is_after_dispatch(metrics):
    rng = np.random.default_rng(3)
    n = 200
    dispatched = [at(s) for s in np.sort(rng.uniform(0, 10, n))]
    history = pd.DataFrame({
        "id": range(n), "task_type": rng.choice(["CPU_TASK", "SORT_TASK"], n),
        "worker_id": rng.choice(["worker-1", "worker-2"], n),
        "status": "COMPLETED", "dispatched_at": dispatched,
        "wait_time_ms": rng.integers(0, 200, n), "exec_time_ms": rng.integers(10, 3000, n)})
    windows = pd.DataFrame([(w, (T0 + pd.Timedelta(seconds=s)).tz_localize(None),
                             (T0 + pd.Timedelta(seconds=s + 10)).tz_localize(None), 1.0, 0.5, 1)
                            for w in ("worker-1", "worker-2") for s in range(-10, 15, 5)],
                           columns=["worker_id", "window_start", "window_end", "arrival_rate",
                                    "queue_len_mean", "queue_len_max"])
    out = bd.join_windows(bd.prior_stats(history), windows)
    has_prior = out["prior_stats_ts"].notna()
    assert has_prior.any()
    assert (out.loc[has_prior, "prior_stats_ts"] <= out.loc[has_prior, "dispatched_at"]).all()
    has_window = out["win_window_end"].notna()
    assert has_window.all()
    assert (out["win_window_end"] <= out["dispatched_at"]).all()
    # The window is the latest one that had ended: none ends in between.
    assert ((out["dispatched_at"] - out["win_window_end"]) < pd.Timedelta(seconds=5)).all()


def load_campaign_module():
    spec = importlib.util.spec_from_file_location("collect_dataset",
                                                  REPO / "scripts" / "collect-dataset.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["collect_dataset"] = module  # dataclasses look their module up while defining
    spec.loader.exec_module(module)
    return module


def test_campaign_plan_is_deterministic():
    campaign = load_campaign_module()
    config = campaign.load_config(REPO / "configs" / "campaign.yaml")
    first, second = campaign.plan(config), campaign.plan(config)
    assert first == second
    assert len(first) == (len(config["profiles"]) * len(config["patterns"])
                          * len(config["strategies"]))
    assert len({r.run_id for r in first}) == len(first)
    # Every strategy replays the same trace for a (profile, pattern) pair.
    traces = {}
    for run in first:
        traces.setdefault((run.profile, run.pattern), set()).add((run.trace, run.seed, run.rate))
    assert all(len(v) == 1 for v in traces.values())
    # Task ids stay unique across runs: prefix per pair, suffix per strategy.
    assert len({(r.task_prefix, r.id_suffix) for r in first}) == len(first)
