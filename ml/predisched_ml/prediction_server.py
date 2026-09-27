"""gRPC PredictionService (spec §10.4, §12.3, prompt 17).

    python -m predisched_ml.prediction_server [--port 50070] [--max-workers 4] [--no-db]

- Loads the live version of M1, M2 and M3 from ``ml/models/registry.json`` once. The loaded
  models are shared read-only by every request thread.
- ``Predict``: one feature row per candidate worker (``features.feature_matrix``, the same code as
  training), each model run once over the batch. Returns per-worker ``pred_exec_ms``,
  ``pred_queue_len``, ``overload_prob`` and ``cold_start``. Invalid requests get
  ``INVALID_ARGUMENT``; nothing a client sends can crash the server.
- Every answer is logged, off the request path, as JSONL in ``logs/predictions.jsonl`` and in the
  ``predictions`` table when PostgreSQL is reachable.
- Hot reload: a watcher polls ``registry.json``. When it changes, the new live versions are loaded
  and swapped in with one reference assignment, so a request uses either the old set or the new one,
  never a mix. A reload that fails keeps the old models.
- Server reflection is on, so ``grpcurl -plaintext localhost:50070 list`` works.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import logging
import math
import os
import queue
import signal
import sys
import threading
import time
from concurrent import futures
from pathlib import Path

import grpc
import numpy as np
import pandas as pd

from . import REPO, registry
from .proto import prediction_pb2 as pb
from .proto import prediction_pb2_grpc as pb_grpc
from .proto import task_pb2

log = logging.getLogger("prediction_server")

MODELS = ("m1", "m2", "m3")
MAX_WORKERS_PER_REQUEST = 256
DSN = os.environ.get("PREDISCHED_DSN",
                     "host=localhost port=5432 dbname=predisched user=postgres password=predisched")
# Resource profile per task type, as each Java executor's profile() (spec 7.2).
PROFILES = {
    "CPU_TASK": "CPU_BOUND", "HASH_TASK": "CPU_BOUND", "MONTE_CARLO_TASK": "CPU_BOUND",
    "ML_INFER_TASK": "CPU_BOUND", "SORT_TASK": "MEMORY_BOUND", "COMPRESS_TASK": "MEMORY_BOUND",
    "GRAPH_TASK": "MEMORY_BOUND", "SLEEP_TASK": "IO_BOUND", "FILE_IO_TASK": "IO_BOUND",
    "DB_QUERY_TASK": "IO_BOUND", "HTTP_TASK": "NETWORK_BOUND", "MATRIX_TASK": "PARALLEL",
    "MAPREDUCE_TASK": "PARALLEL",
}


@dataclasses.dataclass(frozen=True)
class ModelSet:
    m1: object          # models.ExecTimeModel
    m2: object
    m3: object
    versions: dict      # {"m1": 1, ...}
    shadows: dict = dataclasses.field(default_factory=dict)   # {"m1": (obj, version)} (F14)
    m1_test_mae: float = 0.0

    @property
    def label(self) -> str:
        return ",".join(f"{m}=v{self.versions[m]}" for m in MODELS)

    @property
    def shadow_label(self) -> str:
        return ",".join(f"{m}=v{self.shadows[m][1]}" for m in MODELS if m in self.shadows)

    @classmethod
    def load(cls, root: Path = registry.MODELS_DIR) -> "ModelSet":
        loaded, versions, shadows = {}, {}, {}
        test_mae = 0.0
        for model_id in MODELS:
            obj, meta = registry.load(model_id, root)
            loaded[model_id] = obj
            versions[model_id] = int(meta["version"])
            if model_id == "m1":
                test_mae = float((meta.get("metrics") or {}).get("test", {}).get("mae") or 0.0)
            shadow = registry.shadow_version(model_id, root)
            if shadow:
                shadows[model_id] = (registry.load(model_id, root, shadow)[0], int(shadow))
        return cls(loaded["m1"], loaded["m2"], loaded["m3"], versions, shadows, test_mae)


class InvalidRequest(ValueError):
    pass


def _finite(value: float, what: str) -> float:
    if not math.isfinite(value) or value < 0:
        raise InvalidRequest(f"{what} must be a finite number >= 0, got {value}")
    return value


def request_frame(request: pb.PredictRequest) -> pd.DataFrame:
    """The dataset-shaped rows for one request (one per worker), or InvalidRequest."""
    workers = request.workers
    if not workers:
        raise InvalidRequest("workers is empty: send the state of at least one candidate worker")
    if len(workers) > MAX_WORKERS_PER_REQUEST:
        raise InvalidRequest(f"{len(workers)} workers in one request; at most"
                             f" {MAX_WORKERS_PER_REQUEST}")
    task = request.task
    try:
        task_type = task_pb2.TaskType.Name(task.type)
    except ValueError:
        raise InvalidRequest(f"unknown task type {task.type}") from None
    if task.priority < 0 or task.priority > 10:
        raise InvalidRequest(f"priority must be 0..10 (0 = default 5), got {task.priority}")
    type_mean = _finite(request.type_mean_ms, "type_mean_ms")
    seen = set()
    columns: dict[str, list] = {name: [] for name in (
        "worker_id", "worker_cores", "worker_pool_size", "worker_slowdown", "cpu_pct", "mem_pct",
        "active_threads", "queue_len", "arrival_rate", "avg_exec_recent",
        "concurrent_tasks_on_worker", "prior_type_worker_mean_ms", "prior_type_worker_count")}
    for i, w in enumerate(workers):
        if not w.worker_id:
            raise InvalidRequest(f"workers[{i}] has no worker_id")
        if w.worker_id in seen:
            raise InvalidRequest(f"worker {w.worker_id} appears twice")
        seen.add(w.worker_id)
        for field in ("cores", "active_threads", "queue_len", "pool_size", "concurrent_tasks",
                      "type_worker_count"):
            if getattr(w, field) < 0:
                raise InvalidRequest(f"workers[{i}].{field} must be >= 0")
        for field in ("cpu_pct", "mem_pct", "avg_exec_ms", "arrival_rate", "slowdown",
                      "type_worker_mean_ms"):
            _finite(getattr(w, field), f"workers[{i}].{field}")
        columns["worker_id"].append(w.worker_id)
        columns["worker_cores"].append(w.cores)
        columns["worker_pool_size"].append(w.pool_size or w.cores or 1)
        columns["worker_slowdown"].append(w.slowdown if w.slowdown >= 1.0 else 1.0)
        columns["cpu_pct"].append(w.cpu_pct)
        columns["mem_pct"].append(w.mem_pct)
        columns["active_threads"].append(w.active_threads)
        columns["queue_len"].append(w.queue_len)
        columns["arrival_rate"].append(w.arrival_rate)
        columns["avg_exec_recent"].append(w.avg_exec_ms)
        columns["concurrent_tasks_on_worker"].append(w.concurrent_tasks)
        columns["prior_type_worker_mean_ms"].append(
            w.type_worker_mean_ms if w.type_worker_count > 0 else np.nan)
        columns["prior_type_worker_count"].append(w.type_worker_count)
    n = len(workers)
    columns["task_type"] = [task_type] * n
    columns["input"] = [task.input] * n
    columns["priority"] = [task.priority or 5] * n
    columns["resource_profile"] = [PROFILES.get(task_type, "")] * n
    # The Spark window features (training) are not known to the scheduler at dispatch: the current
    # state stands in for the last window (docs/components/prediction-server.md).
    columns["win_arrival_rate"] = columns["arrival_rate"]
    columns["win_queue_len_mean"] = [float(q) for q in columns["queue_len"]]
    columns["win_queue_len_max"] = columns["win_queue_len_mean"]
    columns["type_running_mean_ms"] = [type_mean if type_mean > 0 else np.nan] * n
    frame = pd.DataFrame(columns)
    return frame


class PredictionLog:
    """Writes prediction records off the request path: JSONL always, PostgreSQL when reachable."""

    def __init__(self, path: Path | None, dsn: str | None):
        self.path = path
        self.dsn = dsn
        self.queue: queue.Queue = queue.Queue(maxsize=100_000)
        self.dropped = 0
        self.db_ok = dsn is not None
        self.thread = threading.Thread(target=self._run, name="prediction-log", daemon=True)
        self.stopping = threading.Event()
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
        self.thread.start()

    def add(self, record: dict) -> None:
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            self.dropped += 1

    def _connect(self):
        if not self.db_ok:
            return None
        try:
            import psycopg
            conn = psycopg.connect(self.dsn, connect_timeout=3, autocommit=True)
            log.info("logging predictions to the predictions table")
            return conn
        except Exception as e:  # noqa: BLE001 - the DB is optional
            log.warning("predictions table unavailable (%s); JSONL only", e)
            self.db_ok = False
            return None

    def _run(self) -> None:
        conn = self._connect()
        out = open(self.path, "a", encoding="utf-8") if self.path else None
        try:
            while not (self.stopping.is_set() and self.queue.empty()):
                batch = []
                try:
                    batch.append(self.queue.get(timeout=0.2))
                    while len(batch) < 500:
                        batch.append(self.queue.get_nowait())
                except queue.Empty:
                    pass
                if not batch:
                    continue
                if out:
                    out.write("".join(json.dumps(r) + "\n" for r in batch))
                    out.flush()
                if conn is not None:
                    try:
                        with conn.cursor() as cur:
                            cur.executemany(
                                "INSERT INTO predictions (task_id, worker_id, pred_exec_ms,"
                                " overload_prob, model_version, ts, pred_queue_len)"
                                " VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                [(r["task_id"], p["worker_id"], p["pred_exec_ms"],
                                  p["overload_prob"], r["model_versions"], r["ts"],
                                  p.get("pred_queue_len"))
                                 for r in batch for p in r["predictions"]])
                    except Exception as e:  # noqa: BLE001
                        log.warning("predictions insert failed (%s); JSONL only from now", e)
                        conn = None
        finally:
            if out:
                out.close()
            if conn is not None:
                conn.close()

    def close(self, timeout: float = 5.0) -> None:
        self.stopping.set()
        self.thread.join(timeout)


class PredictionService(pb_grpc.PredictionServiceServicer):
    def __init__(self, models: ModelSet, prediction_log: PredictionLog | None = None):
        self.models = models
        self.log = prediction_log
        self.served = 0
        self.reloads = 0
        self._count_lock = threading.Lock()

    def predict(self, request: pb.PredictRequest) -> pb.PredictResponse:
        started = time.perf_counter()
        frame = request_frame(request)
        models = self.models   # one set for the whole request, even during a reload
        exec_ms, cold = models.m1.predict_with_flags(frame)
        queue_len = np.clip(np.asarray(models.m2.predict(frame), dtype=float), 0.0, None)
        overload = np.asarray(models.m3.predict_proba(frame), dtype=float)
        response = pb.PredictResponse(model_version=models.versions["m1"],
                                      model_versions=models.label)
        records = []
        for i, worker in enumerate(frame["worker_id"]):
            p = response.predictions.add(worker_id=worker, pred_exec_ms=float(exec_ms[i]),
                                         pred_queue_len=float(queue_len[i]),
                                         overload_prob=float(overload[i]),
                                         cold_start=bool(cold[i]))
            records.append({"worker_id": worker, "pred_exec_ms": round(p.pred_exec_ms, 3),
                            "pred_queue_len": round(p.pred_queue_len, 4),
                            "overload_prob": round(p.overload_prob, 5),
                            "cold_start": p.cold_start})
        shadow_records = self._shadow(models, frame, exec_ms, queue_len, overload, response)
        response.server_ms = (time.perf_counter() - started) * 1000
        with self._count_lock:
            self.served += 1
        if self.log:
            record = {"ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                      "request_id": request.request_id, "task_id": request.task.task_id,
                      "task_type": frame["task_type"].iat[0],
                      "model_versions": models.label, "m1_version": models.versions["m1"],
                      "latency_ms": round(response.server_ms, 3), "predictions": records}
            if shadow_records:
                record.update(shadow_versions=models.shadow_label,
                              shadow_version=response.shadow_version,
                              shadow_predictions=shadow_records)
            self.log.add(record)
        return response

    @staticmethod
    def _shadow(models: ModelSet, frame, exec_ms, queue_len, overload, response) -> list[dict]:
        """Shadow predictions (F14): a model's shadow version where it has one, else live."""
        if not models.shadows:
            return []
        s_exec, s_queue, s_over = exec_ms, queue_len, overload
        if "m1" in models.shadows:
            s_exec = models.shadows["m1"][0].predict_with_flags(frame)[0]
            response.shadow_version = models.shadows["m1"][1]
        if "m2" in models.shadows:
            s_queue = np.clip(np.asarray(models.shadows["m2"][0].predict(frame), dtype=float),
                              0.0, None)
        if "m3" in models.shadows:
            s_over = np.asarray(models.shadows["m3"][0].predict_proba(frame), dtype=float)
        out = []
        for i, worker in enumerate(frame["worker_id"]):
            p = response.shadow_predictions.add(worker_id=worker, pred_exec_ms=float(s_exec[i]),
                                                pred_queue_len=float(s_queue[i]),
                                                overload_prob=float(s_over[i]))
            out.append({"worker_id": worker, "pred_exec_ms": round(p.pred_exec_ms, 3),
                        "pred_queue_len": round(p.pred_queue_len, 4),
                        "overload_prob": round(p.overload_prob, 5)})
        return out

    # --- gRPC ------------------------------------------------------------------------------
    def Predict(self, request, context):  # noqa: N802 - gRPC method name
        try:
            return self.predict(request)
        except InvalidRequest as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        except Exception as e:  # noqa: BLE001 - a model bug must not take the server down
            log.exception("prediction failed")
            context.abort(grpc.StatusCode.INTERNAL, f"prediction failed: {e}")

    def Health(self, request, context):  # noqa: N802
        return pb.HealthResponse(ready=True, model_versions=self.models.label,
                                 predictions=self.served, reloads=self.reloads,
                                 m1_test_mae_ms=self.models.m1_test_mae,
                                 shadow_versions=self.models.shadow_label)

    def reload(self, root: Path = registry.MODELS_DIR) -> bool:
        try:
            fresh = ModelSet.load(root)
        except Exception as e:  # noqa: BLE001 - keep serving the old set
            log.error("model reload failed, keeping %s: %s", self.models.label, e)
            return False
        if (fresh.versions != self.models.versions
                or fresh.shadow_label != self.models.shadow_label):
            old = self.models.label + (f" shadow {self.models.shadow_label}"
                                       if self.models.shadows else "")
            self.models = fresh
            self.reloads += 1
            log.info("models reloaded: %s -> %s%s", old, fresh.label,
                     f" shadow {fresh.shadow_label}" if fresh.shadows else "")
        return True


class RegistryWatcher(threading.Thread):
    """Polls registry.json and reloads the models when it changes."""

    def __init__(self, service: PredictionService, root: Path, interval: float):
        super().__init__(name="registry-watcher", daemon=True)
        self.service, self.root, self.interval = service, root, interval
        self.stopping = threading.Event()
        self.path = registry.registry_path(root)
        self.stamp = self._stamp()

    def _stamp(self):
        try:
            stat = self.path.stat()
            return stat.st_mtime_ns, stat.st_size
        except FileNotFoundError:
            return None

    def run(self) -> None:
        while not self.stopping.wait(self.interval):
            stamp = self._stamp()
            if stamp != self.stamp:
                self.stamp = stamp
                self.service.reload(self.root)

    def stop(self) -> None:
        self.stopping.set()


@dataclasses.dataclass
class Running:
    server: grpc.Server
    service: PredictionService
    watcher: RegistryWatcher
    prediction_log: PredictionLog | None
    port: int

    def stop(self, grace: float = 1.0) -> None:
        self.watcher.stop()
        self.server.stop(grace).wait()
        if self.prediction_log:
            self.prediction_log.close()


def serve(port: int = 50070, root: Path = registry.MODELS_DIR, max_workers: int = 4,
          log_path: Path | None = REPO / "logs" / "predictions.jsonl", dsn: str | None = DSN,
          reload_interval: float = 1.0, reflection: bool = True) -> Running:
    models = ModelSet.load(root)
    prediction_log = PredictionLog(log_path, dsn) if (log_path or dsn) else None
    service = PredictionService(models, prediction_log)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers,
                                                    thread_name_prefix="predict"))
    pb_grpc.add_PredictionServiceServicer_to_server(service, server)
    if reflection:
        from grpc_reflection.v1alpha import reflection as grpc_reflection
        grpc_reflection.enable_server_reflection(
            (pb.DESCRIPTOR.services_by_name["PredictionService"].full_name,
             grpc_reflection.SERVICE_NAME), server)
    bound = server.add_insecure_port(f"[::]:{port}")
    if bound == 0:
        raise SystemExit(f"cannot bind port {port}")
    server.start()
    watcher = RegistryWatcher(service, root, reload_interval)
    watcher.start()
    log.info("PredictionService on port %d with %s (%d threads)", bound, models.label, max_workers)
    return Running(server, service, watcher, prediction_log, bound)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="PrediSched prediction server (gRPC).")
    parser.add_argument("--port", type=int, default=50070)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--log", default=str(REPO / "logs" / "predictions.jsonl"),
                        help="JSONL prediction log ('' to disable)")
    parser.add_argument("--no-db", action="store_true", help="do not write the predictions table")
    parser.add_argument("--dsn", default=DSN)
    parser.add_argument("--models", default=str(registry.MODELS_DIR),
                        help="model folder holding registry.json")
    parser.add_argument("--reload-interval", type=float, default=1.0,
                        help="seconds between registry.json checks")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    running = serve(args.port, root=Path(args.models), max_workers=args.max_workers,
                    log_path=Path(args.log) if args.log else None,
                    dsn=None if args.no_db else args.dsn, reload_interval=args.reload_interval)
    done = threading.Event()
    signal.signal(signal.SIGINT, lambda *a: done.set())
    signal.signal(signal.SIGTERM, lambda *a: done.set())
    print(f"ready on port {running.port}: {running.service.models.label}", flush=True)
    while not done.wait(0.5):
        pass
    running.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
