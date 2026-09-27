"""Features match between training and serving; the time split is clean; models round-trip."""

import numpy as np
import pandas as pd
import pytest

from predisched_ml import metrics, registry
from predisched_ml.data import split
from predisched_ml.features import FEATURE_NAMES, build_features, input_size
from predisched_ml.models import ExecTimeModel, baselines, make
from predisched_ml.train import train_model

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


def test_training_and_single_row_serving_features_are_identical():
    df = synthetic(50)
    batch = build_features(df)
    assert list(batch.columns) == FEATURE_NAMES
    for i in (0, 7, 33):
        row = df.iloc[i].to_dict()
        single = build_features(row)
        assert list(single.columns) == FEATURE_NAMES
        np.testing.assert_allclose(single.to_numpy()[0], batch.to_numpy()[i])


def test_serving_input_can_be_the_raw_input_string():
    served = build_features({"task_type": "CPU_TASK", "input": "n=2000000",
                             "resource_profile": "CPU_BOUND"})
    trained = build_features({"task_type": "CPU_TASK", "input_size": 2000000,
                              "resource_profile": "CPU_BOUND"})
    np.testing.assert_allclose(served.to_numpy(), trained.to_numpy())
    assert input_size("MATRIX_TASK", "size=400, mode=mpi, procs=4") == 400
    assert input_size("HTTP_TASK", "url=http://x/y, timeout=3000") == 3000
    # Unknown types still get every column (all type one-hots zero).
    unknown = build_features({"task_type": "NEW_TASK", "input": "x=1"})
    assert list(unknown.columns) == FEATURE_NAMES
    assert unknown.filter(like="type_").to_numpy().sum() == 0


def test_time_split_has_no_overlap_and_keeps_order():
    df = synthetic(500).sample(frac=1.0, random_state=3)  # shuffled on purpose
    s = split(df, 0.2)
    assert s.train["dispatched_at"].is_monotonic_increasing
    assert s.test["dispatched_at"].is_monotonic_increasing
    assert s.train["dispatched_at"].max() < s.test["dispatched_at"].min()
    ids = [set(x["id"]) for x in (s.train, s.test, s.holdout_pattern, s.holdout_type)]
    assert sum(len(x) for x in ids) == len(df)
    assert all(not (a & b) for i, a in enumerate(ids) for b in ids[i + 1:])
    assert not s.train["holdout_task_type"].any() and not s.train["holdout_pattern"].any()
    assert (s.holdout_type["task_type"] == "GRAPH_TASK").all()


def test_model_round_trips_through_joblib(tmp_path):
    df = synthetic(300)
    model = make("m1", "random_forest", {"n_estimators": 20, "max_depth": 6,
                                         "min_samples_leaf": 1}, seed=1)
    model.fit(df, df["exec_time_ms"].to_numpy())
    served = ExecTimeModel(model, set(df["task_type"]), float(df["exec_time_ms"].mean()))
    registry.save("m1", served, {"name": "exec_time", "kind": "regression",
                                 "kept": {"candidate": model.describe()}}, root=tmp_path)
    loaded, meta = registry.load("m1", root=tmp_path)
    assert meta["version"] == 1
    # Trees are summed in parallel, so allow float rounding.
    np.testing.assert_allclose(loaded.predict(df), served.predict(df), rtol=1e-9)
    assert registry.load_registry(tmp_path)["models"]["m1"]["live"] == 1
    registry.save("m1", served, {"kept": {}}, root=tmp_path)
    assert registry.load_registry(tmp_path)["models"]["m1"]["live"] == 2


def test_cold_start_falls_back_and_flags():
    df = synthetic(300)
    known = df[df["task_type"] != "GRAPH_TASK"]
    model = make("m1", "linear", {"alpha": 1.0}, seed=1).fit(known, known["exec_time_ms"])
    served = ExecTimeModel(model, set(known["task_type"]), 123.0)
    rows = df[df["task_type"] == "GRAPH_TASK"].head(3).copy()
    rows["prior_type_worker_mean_ms"] = [np.nan, 40.0, 55.0]
    rows["prior_type_worker_count"] = [0, 4, 0]
    pred, cold = served.predict_with_flags(rows)
    assert cold.all()
    assert pred.tolist() == [123.0, 40.0, 123.0]
    rows["type_running_mean_ms"] = [70.0, np.nan, 90.0]   # the scheduler's running mean wins
    pred, _ = served.predict_with_flags(rows)
    assert pred.tolist() == [70.0, 40.0, 90.0]
    warm = known.head(5)
    assert not served.predict_with_flags(warm)[1].any()


@pytest.mark.parametrize("model_id", ["m1", "m2", "m3"])
def test_baselines_are_always_evaluated(model_id, tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "MODELS_DIR", tmp_path)
    monkeypatch.setattr("predisched_ml.train.save",
                        lambda m, obj, meta: registry.save(m, obj, meta, root=tmp_path))
    monkeypatch.setattr("predisched_ml.train.write_candidate_table", lambda *a: None)
    monkeypatch.setattr("predisched_ml.train.file_hash", lambda p: "test")
    df = synthetic(400)
    config = {"seed": 1, "testFraction": 0.2, "cvSplits": 2, "margin": 0.1,
              "dataset": str(tmp_path / "unused.parquet"),
              "models": {"m1": {"name": "exec_time", "target": "exec_time_ms",
                                "kind": "regression", "primary": "mae"},
                         "m2": {"name": "queue_forecast", "target": "queue_len_future",
                                "kind": "regression", "primary": "mae"},
                         "m3": {"name": "overload", "target": "overloaded_future",
                                "kind": "classification", "primary": "f1"}},
              "grids": {"linear": {"alpha": [1.0], "C": [1.0]},
                        "random_forest": {"n_estimators": [10], "max_depth": [4],
                                          "min_samples_leaf": [1]},
                        "xgboost": {"n_estimators": [20], "max_depth": [2],
                                    "learning_rate": [0.1]}}}
    monkeypatch.setattr("predisched_ml.train.dataset_path",
                        lambda c: tmp_path / "unused.parquet")
    (tmp_path / "unused.parquet").write_bytes(b"")
    meta = train_model(model_id, config, split(df, 0.2), log=lambda *a: None)
    names = {b.describe() for b in baselines(model_id)}
    for set_name in ("test", "holdout_pattern", "holdout_type"):
        evaluated = {r["candidate"] for r in meta["candidates"] if r["set"] == set_name}
        assert names <= evaluated
    assert {r["candidate"] for r in meta["cv"] if r["family"] == "baseline"} == names
    assert meta["kept"]["reason"]


def test_margin_rule():
    assert metrics.beats(8.9, 10.0, "mae", 0.1)
    assert not metrics.beats(9.5, 10.0, "mae", 0.1)
    assert metrics.beats(0.55, 0.5, "f1", 0.1)
    assert not metrics.beats(0.52, 0.5, "f1", 0.1)
