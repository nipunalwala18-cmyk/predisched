"""PredictionService: one prediction per worker, INVALID_ARGUMENT on bad input, hot reload."""

import json
import time

import grpc
import pytest

from predisched_ml import registry
from predisched_ml.models import ExecTimeModel, GroupMeanRegressor, make
from predisched_ml.prediction_server import serve
from predisched_ml.proto import prediction_pb2 as pb
from predisched_ml.proto import prediction_pb2_grpc as pb_grpc
from predisched_ml.proto import task_pb2

from helpers import synthetic


def save_models(root):
    df = synthetic(300)
    known = df[df["task_type"] != "GRAPH_TASK"]
    m1 = make("m1", "linear", {"alpha": 1.0}, seed=1).fit(known, known["exec_time_ms"])
    registry.save("m1", ExecTimeModel(m1, set(known["task_type"]), 100.0),
                  {"name": "exec_time", "kind": "regression", "kept": {"candidate": "linear"}},
                  root=root)
    registry.save("m2", GroupMeanRegressor("worker_id").fit(df, df["queue_len_future"]),
                  {"name": "queue_forecast", "kind": "regression", "kept": {"candidate": "mean"}},
                  root=root)
    m3 = make("m3", "linear", {"C": 1.0}, seed=1).fit(df, df["overloaded_future"])
    registry.save("m3", m3, {"name": "overload", "kind": "classification",
                             "kept": {"candidate": "logistic"}}, root=root)
    return ExecTimeModel(m1, set(known["task_type"]), 100.0)


@pytest.fixture
def server(tmp_path):
    save_models(tmp_path / "models")
    running = serve(port=0, root=tmp_path / "models", log_path=tmp_path / "predictions.jsonl",
                    dsn=None, reload_interval=0.1, reflection=False)
    channel = grpc.insecure_channel(f"localhost:{running.port}")
    stub = pb_grpc.PredictionServiceStub(channel)
    yield running, stub, tmp_path
    channel.close()
    running.stop()


def request(workers=("worker-1", "worker-2", "worker-3"), task_type="CPU_TASK"):
    req = pb.PredictRequest(request_id="r1", task=task_pb2.TaskRequest(
        task_id="t1", type=task_pb2.TaskType.Value(task_type), input="n=50000", priority=5))
    for i, w in enumerate(workers):
        req.workers.add(worker_id=w, cores=8, cpu_pct=10.0 * i, mem_pct=1.0, active_threads=i,
                        queue_len=i, avg_exec_ms=20.0, arrival_rate=1.0, pool_size=2 + 2 * i,
                        slowdown=2.0 if i == 0 else 1.0)
    return req


def test_answers_one_prediction_per_worker_and_logs_it(server):
    running, stub, tmp_path = server
    response = stub.Predict(request(), timeout=5)
    assert [p.worker_id for p in response.predictions] == ["worker-1", "worker-2", "worker-3"]
    assert response.model_version == 1
    assert response.model_versions == "m1=v1,m2=v1,m3=v1"
    for p in response.predictions:
        assert p.pred_exec_ms > 0
        assert p.pred_queue_len >= 0
        assert 0.0 <= p.overload_prob <= 1.0
        assert not p.cold_start
    # GRAPH_TASK was not in M1's training data: cold start.
    cold = stub.Predict(request(task_type="GRAPH_TASK"), timeout=5)
    assert all(p.cold_start and p.pred_exec_ms == 100.0 for p in cold.predictions)
    assert stub.Health(pb.HealthRequest(), timeout=5).predictions == 2
    running.prediction_log.close()
    lines = (tmp_path / "predictions.jsonl").read_text(encoding="utf-8").splitlines()
    record = json.loads(lines[0])
    assert record["task_id"] == "t1" and record["request_id"] == "r1"
    assert [p["worker_id"] for p in record["predictions"]] == ["worker-1", "worker-2", "worker-3"]
    assert record["model_versions"] == "m1=v1,m2=v1,m3=v1" and record["latency_ms"] > 0


@pytest.mark.parametrize("mutate, message", [
    (lambda r: r.ClearField("workers"), "at least one"),
    (lambda r: r.workers.add(worker_id="worker-1"), "twice"),
    (lambda r: setattr(r.workers[0], "worker_id", ""), "no worker_id"),
    (lambda r: setattr(r.workers[1], "cpu_pct", float("nan")), "cpu_pct"),
    (lambda r: setattr(r.workers[1], "queue_len", -3), "queue_len"),
    (lambda r: setattr(r.task, "priority", 42), "priority"),
    (lambda r: setattr(r.task, "type", 99), "unknown task type"),
])
def test_bad_input_is_invalid_argument(server, mutate, message):
    _, stub, _ = server
    req = request()
    mutate(req)
    with pytest.raises(grpc.RpcError) as error:
        stub.Predict(req, timeout=5)
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert message in error.value.details()
    # Still serving afterwards.
    assert len(stub.Predict(request(), timeout=5).predictions) == 3


def test_hot_reload_changes_the_model_version(server):
    running, stub, tmp_path = server
    assert stub.Predict(request(), timeout=5).model_version == 1
    m1 = registry.load("m1", tmp_path / "models")[0]
    registry.save("m1", m1, {"name": "exec_time", "kind": "regression",
                             "kept": {"candidate": "linear"}}, root=tmp_path / "models")
    deadline = time.time() + 5
    while time.time() < deadline and stub.Predict(request(), timeout=5).model_version != 2:
        time.sleep(0.05)
    response = stub.Predict(request(), timeout=5)
    assert response.model_version == 2
    assert response.model_versions == "m1=v2,m2=v1,m3=v1"
    assert stub.Health(pb.HealthRequest(), timeout=5).reloads == 1


def test_a_broken_registry_keeps_the_old_models(server):
    running, stub, tmp_path = server
    (tmp_path / "models" / "registry.json").write_text("{not json", encoding="utf-8")
    time.sleep(0.4)
    response = stub.Predict(request(), timeout=5)
    assert response.model_versions == "m1=v1,m2=v1,m3=v1"
    assert stub.Health(pb.HealthRequest(), timeout=5).reloads == 0
