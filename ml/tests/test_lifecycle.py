"""Model lifecycle (prompt 21): statuses, promotion rules, shadow comparison, shadow serving."""

import json

import grpc
import pytest

from predisched_ml import registry
from predisched_ml.compare_shadow import compare
from predisched_ml.models import ExecTimeModel, GroupMeanRegressor, make
from predisched_ml.prediction_server import serve
from predisched_ml.proto import prediction_pb2_grpc as pb_grpc
from predisched_ml.retrain import retrain

from helpers import synthetic
from test_server import request, save_models


def test_statuses_follow_saves_and_shadow_assignment(tmp_path):
    save_models(tmp_path)
    m1 = registry.load("m1", tmp_path)[0]
    registry.save("m1", m1, {"kept": {}}, root=tmp_path, status="shadow")
    entry = registry.load_registry(tmp_path)["models"]["m1"]
    assert (entry["live"], entry["shadow"]) == (1, 2)
    assert [v["status"] for v in entry["versions"]] == ["live", "shadow"]
    registry.save("m1", m1, {"kept": {}}, root=tmp_path, status="shadow")
    entry = registry.load_registry(tmp_path)["models"]["m1"]
    assert entry["shadow"] == 3
    assert [v["status"] for v in entry["versions"]] == ["live", "retired", "shadow"]
    with pytest.raises(ValueError):
        registry.set_shadow("m1", 1, tmp_path)   # the live version cannot also be the shadow


@pytest.mark.parametrize("comparison, why", [
    ({"tasks": 150, "live_mae": 30.0, "shadow_mae": 10.0}, "only 150 compared tasks"),
    ({"tasks": 500, "live_mae": 30.0, "shadow_mae": 29.0}, "does not beat"),
    (None, "no comparison"),
])
def test_promotion_is_refused_below_the_margin_or_sample_size(tmp_path, comparison, why):
    save_models(tmp_path)
    registry.save("m1", registry.load("m1", tmp_path)[0], {"kept": {}}, root=tmp_path,
                  status="shadow")
    with pytest.raises(registry.PromotionRefused) as refused:
        registry.promote("m1", 2, comparison, margin=0.05, min_tasks=200, root=tmp_path,
                         log=lambda *a: None)
    assert why in str(refused.value)
    assert registry.load_registry(tmp_path)["models"]["m1"]["live"] == 1


def test_promotion_is_allowed_above_and_force_is_logged(tmp_path):
    save_models(tmp_path)
    m1 = registry.load("m1", tmp_path)[0]
    registry.save("m1", m1, {"kept": {}}, root=tmp_path, status="shadow")
    record = registry.promote("m1", 2, {"tasks": 500, "live_mae": 30.0, "shadow_mae": 25.0},
                              margin=0.05, min_tasks=200, root=tmp_path, log=lambda *a: None)
    entry = registry.load_registry(tmp_path)["models"]["m1"]
    assert (entry["live"], entry["shadow"]) == (2, None)
    assert [v["status"] for v in entry["versions"]] == ["retired", "live"]
    assert record["forced"] is False
    # A forced promotion of a worse version goes through, and says so.
    registry.save("m1", m1, {"kept": {}}, root=tmp_path, status="shadow")
    lines = []
    forced = registry.promote("m1", 3, {"tasks": 10, "live_mae": 30.0, "shadow_mae": 40.0},
                              margin=0.05, min_tasks=200, force=True, root=tmp_path,
                              log=lines.append)
    assert forced["forced"] is True and len(forced["reasons_overridden"]) == 2
    assert any("WARNING" in line and "--force" in line for line in lines)
    log = [json.loads(l) for l in (tmp_path / "promotions.log").read_text().splitlines()]
    assert [r["version"] for r in log] == [2, 3] and log[1]["forced"] is True


def test_compare_scores_both_versions_on_the_same_task_worker_pairs():
    records = [
        {"task_id": "a", "m1_version": 1, "shadow_version": 2,
         "predictions": [{"worker_id": "w1", "pred_exec_ms": 100}, {"worker_id": "w2", "pred_exec_ms": 50}],
         "shadow_predictions": [{"worker_id": "w1", "pred_exec_ms": 90}, {"worker_id": "w2", "pred_exec_ms": 70}]},
        {"task_id": "b", "m1_version": 1, "shadow_version": 2,
         "predictions": [{"worker_id": "w1", "pred_exec_ms": 10}],
         "shadow_predictions": [{"worker_id": "w1", "pred_exec_ms": 30}]},
        {"task_id": "c", "m1_version": 1, "predictions": [{"worker_id": "w1", "pred_exec_ms": 5}]},
    ]
    actual = {("a", "w2"): 60.0, ("b", "w1"): 20.0, ("c", "w1"): 5.0}
    result = compare(records, actual, live=1, shadow=2)
    # a ran on w2: live |50-60| = 10, shadow |70-60| = 10; b: live 10, shadow 10; c has no shadow.
    assert result["tasks"] == 2
    assert result["live_mae"] == 10.0 and result["shadow_mae"] == 10.0
    assert compare(records, actual, live=1, shadow=9)["tasks"] == 0


def test_the_server_answers_shadow_predictions_beside_live(tmp_path):
    save_models(tmp_path / "models")
    df = synthetic(300)
    shadow = make("m1", "random_forest", {"n_estimators": 10, "max_depth": 4,
                                          "min_samples_leaf": 1}, seed=2).fit(df, df["exec_time_ms"])
    registry.save("m1", ExecTimeModel(shadow, set(df["task_type"]), 100.0), {"kept": {}},
                  root=tmp_path / "models", status="shadow")
    running = serve(port=0, root=tmp_path / "models", log_path=tmp_path / "p.jsonl", dsn=None,
                    reload_interval=0.1, reflection=False)
    try:
        with grpc.insecure_channel(f"localhost:{running.port}") as channel:
            response = pb_grpc.PredictionServiceStub(channel).Predict(request(), timeout=5)
        assert response.shadow_version == 2 and response.model_version == 1
        assert [p.worker_id for p in response.shadow_predictions] == \
            [p.worker_id for p in response.predictions]
        assert any(s.pred_exec_ms != p.pred_exec_ms
                   for s, p in zip(response.shadow_predictions, response.predictions))
    finally:
        running.stop()
    record = json.loads((tmp_path / "p.jsonl").read_text().splitlines()[0])
    assert record["shadow_version"] == 2 and len(record["shadow_predictions"]) == 3


def test_retrain_fits_on_recent_rows_and_scores_live_on_the_same_test():
    df = synthetic(400).sort_values("dispatched_at").reset_index(drop=True)
    df["status"] = "COMPLETED"
    live = ExecTimeModel(GroupMeanRegressor("task_type").fit(df, df["exec_time_ms"]),
                         set(df["task_type"]), 100.0)
    served, result = retrain(df, "linear", {"alpha": 1.0}, live=live)
    assert result["rows"] == {"train": 320, "test": 80}
    assert result["test"]["mae"] < result["live_on_same_test"]["mae"]
