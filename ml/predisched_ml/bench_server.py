"""Latency bench for the prediction server (prompt 17).

    python -m predisched_ml.bench_server --workers 3 --requests 2000 [--target localhost:50070]
                                         [--spawn] [--concurrency 1]

Sends ``requests`` Predict calls, each for one task over ``workers`` candidate workers. Tasks and
worker states are drawn from the dataset (seeded). Reports p50 / p95 / p99 / max of the latency
the client sees and of the server's own ``server_ms``, after a warm-up. ``--spawn`` starts a
server in this process on a free port (no JSONL or DB logging), for a self-contained run.
Results are appended to ``results/prediction-latency.csv``.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
import time
from concurrent import futures

import grpc
import numpy as np

from . import REPO
from .data import load_config, load_dataset
from .features import SIZE_KEYS
from .proto import prediction_pb2 as pb
from .proto import prediction_pb2_grpc as pb_grpc
from .proto import task_pb2


def make_requests(n: int, workers: int, seed: int) -> list[pb.PredictRequest]:
    df = load_dataset(load_config())
    rng = np.random.default_rng(seed)
    tasks = df.iloc[rng.integers(0, len(df), n)]
    states = df.iloc[rng.integers(0, len(df), n * workers)]
    requests = []
    for i, (_, task) in enumerate(tasks.iterrows()):
        # The input carries the size under the type's own key, parsed by the server as in training.
        size_key = SIZE_KEYS.get(task["task_type"], "n")
        req = pb.PredictRequest(request_id=f"bench-{i}", task=task_pb2.TaskRequest(
            task_id=f"bench-{i}", type=task_pb2.TaskType.Value(task["task_type"]),
            input=f"{size_key}={int(task['input_size'])}", priority=int(task["priority"])))
        for w in range(workers):
            s = states.iloc[i * workers + w]
            req.workers.add(worker_id=f"worker-{w + 1}", cores=int(s["worker_cores"]),
                            cpu_pct=float(s["cpu_pct"]), mem_pct=float(s["mem_pct"]),
                            active_threads=int(s["active_threads"]),
                            queue_len=int(s["queue_len"]), avg_exec_ms=float(s["avg_exec_recent"]),
                            arrival_rate=float(s["arrival_rate"]),
                            pool_size=int(s["worker_pool_size"]),
                            slowdown=float(s["worker_slowdown"]),
                            concurrent_tasks=int(s["concurrent_tasks_on_worker"]))
        requests.append(req)
    return requests


def percentiles(values) -> dict:
    a = np.asarray(values, dtype=float)
    return {"p50": float(np.percentile(a, 50)), "p95": float(np.percentile(a, 95)),
            "p99": float(np.percentile(a, 99)), "max": float(a.max()), "mean": float(a.mean())}


def run(target: str, requests: list, warmup: int, concurrency: int, timeout_s: float):
    channel = grpc.insecure_channel(target)
    grpc.channel_ready_future(channel).result(timeout=10)
    stub = pb_grpc.PredictionServiceStub(channel)
    for req in requests[:warmup]:
        stub.Predict(req, timeout=timeout_s)
    requests = requests[warmup:]

    def one(req):
        started = time.perf_counter()
        response = stub.Predict(req, timeout=timeout_s)
        return (time.perf_counter() - started) * 1000, response.server_ms, len(response.predictions)

    started = time.perf_counter()
    if concurrency <= 1:
        results = [one(r) for r in requests]
    else:
        with futures.ThreadPoolExecutor(concurrency) as pool:
            results = list(pool.map(one, requests))
    wall = time.perf_counter() - started
    health = stub.Health(pb.HealthRequest(), timeout=timeout_s)
    channel.close()
    return results, wall, health


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target", default="localhost:50070")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--warmup", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--timeout-ms", type=float, default=1000)
    parser.add_argument("--spawn", action="store_true", help="start a server in this process")
    args = parser.parse_args(argv)

    running = None
    target = args.target
    if args.spawn:
        from .prediction_server import serve
        running = serve(port=0, log_path=None, dsn=None, reflection=False)
        target = f"localhost:{running.port}"
    try:
        requests = make_requests(args.requests + args.warmup, args.workers, args.seed)
        results, wall, health = run(target, requests, args.warmup, args.concurrency,
                                    args.timeout_ms / 1000)
    finally:
        if running:
            running.stop()
    client = percentiles([r[0] for r in results])
    server = percentiles([r[1] for r in results])
    assert all(r[2] == args.workers for r in results), "one prediction per worker"
    print(f"{len(results)} requests x {args.workers} workers, concurrency {args.concurrency},"
          f" models {health.model_versions}: {len(results) / wall:.0f} req/s")
    print(f"{'':8}{'p50':>9}{'p95':>9}{'p99':>9}{'max':>9}  (ms)")
    for name, p in (("client", client), ("server", server)):
        print(f"{name:8}{p['p50']:9.3f}{p['p95']:9.3f}{p['p99']:9.3f}{p['max']:9.3f}")
    out = REPO / "results" / "prediction-latency.csv"
    out.parent.mkdir(exist_ok=True)
    new = not out.exists()
    with open(out, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if new:
            writer.writerow(["ts", "workers", "requests", "concurrency", "models",
                             "client_p50_ms", "client_p95_ms", "client_p99_ms",
                             "server_p50_ms", "server_p95_ms", "server_p99_ms", "req_per_s"])
        writer.writerow([dt.datetime.now().isoformat(timespec="seconds"), args.workers,
                         len(results), args.concurrency, health.model_versions,
                         *(round(client[k], 3) for k in ("p50", "p95", "p99")),
                         *(round(server[k], 3) for k in ("p50", "p95", "p99")),
                         round(len(results) / wall, 1)])
    return 0


if __name__ == "__main__":
    sys.exit(main())
