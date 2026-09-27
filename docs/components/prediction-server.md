# Prediction server

Prompt 17, spec §10.4, §12.3. A Python gRPC `PredictionService` serves the three prompt 16 models
(`docs/components/ml-models.md`) to the scheduler. One `Predict` call covers one task and all of
its candidate workers, and returns a prediction per worker. The Java `PredictionClient` calls it
with a 10 ms deadline behind a circuit breaker.

## Contract (`proto/prediction.proto`)

```proto
service PredictionService {
  rpc Predict(PredictRequest) returns (PredictResponse);
  rpc Health(HealthRequest) returns (HealthResponse);
}
message PredictRequest  { TaskRequest task = 1; repeated WorkerState workers = 2;
                          string request_id = 3; double type_mean_ms = 4; }
message WorkerPrediction { string worker_id = 1; double pred_exec_ms = 2; double pred_queue_len = 3;
                           double overload_prob = 4; bool cold_start = 5; }
message PredictResponse { repeated WorkerPrediction predictions = 1; int32 model_version = 2;
                          string model_versions = 3; double server_ms = 4; }
```

Additions to spec §10.4:

- `WorkerState` has fields 1–8 from the spec, plus the rest of what the models were trained on
  (spec §12.1):
  - `pool_size` and `slowdown`;
  - `concurrent_tasks`;
  - `type_worker_mean_ms` and `type_worker_count`, the scheduler's running mean of this task type
    on this worker.
- `PredictRequest.type_mean_ms` is the running mean over all workers. It is used for cold start.
- `WorkerPrediction.cold_start` is true when M1 never saw the task type, so `pred_exec_ms` is that
  running mean rather than a model output.
- `PredictResponse` carries `model_version` (M1's live version), `model_versions`
  (`m1=v1,m2=v1,m3=v1`) and the server's own time.
- A `Health` RPC.

Python stubs are generated into `ml/generated/` with the command in RULES. Java stubs are generated
by the Maven build as for every other proto.

## Serving

`python -m predisched_ml.prediction_server --port 50070` (`ml/predisched_ml/prediction_server.py`)
works like this:

- **Loading.** At start it loads the live version of M1, M2 and M3 from `ml/models/registry.json`
  into one immutable `ModelSet`. Requests share it read-only on a `ThreadPoolExecutor` with 4
  threads (`--max-workers`).
- **Predict.** Each request becomes one dataset-shaped row per worker. The rows go through
  `features.feature_matrix`, the same function training used, and each model runs once over the
  whole batch.
  - The task's resource profile comes from a type-to-profile table that mirrors the Java
    executors.
  - The input size is parsed from the task input with the training rules.
  - A pool size of 0 falls back to cores, and a slowdown of 0 to 1.
- **Train/serve gap.** The Spark window features (`win_*`) exist in training but are unknown to
  the scheduler at dispatch. The server fills them from the worker's current arrival rate and
  queue. This gap is documented rather than hidden.
- **Validation.** A request is rejected with `INVALID_ARGUMENT` and a message saying what is wrong
  when:
  - it has no workers, or more than 256;
  - a `worker_id` is missing or appears twice;
  - a count is negative, or a number is non-finite or negative;
  - the priority is outside 0..10;
  - the task type is not in the enum.

  Any other exception becomes `INTERNAL`. The server keeps serving in every case, which the tests
  check after each bad request.
- **Logging.** Every answer is queued and written by a background thread, so the request path only
  does a `put_nowait`. It goes to:
  - `logs/predictions.jsonl`: request id, task id, type, model versions, latency and the
    per-worker predictions;
  - the `predictions` table, in batches of up to 500.

  PostgreSQL is optional: if it is unreachable or an insert fails, the log carries on as JSONL
  only.
- **Hot reload.** A watcher polls `registry.json` every second (`--reload-interval`). On a change
  it loads the new live set and swaps it in with one reference assignment. A request reads
  `self.models` once, so it uses either the old set or the new one, never a mix. A registry that
  fails to load (broken JSON, missing file) keeps the old models and logs the error.
- **Reflection.** Server reflection is on, so `grpcurl -plaintext localhost:50070 list` works.

## Java client (`predisched-scheduler/.../prediction/`)

`PredictionClient.predict(task, workers)` never throws. It returns the per-worker predictions, or
an empty `Result` with the reason, and then the dispatcher falls back to Least Loaded (FR19,
prompt 18).

- **Deadline.** Each call has `prediction.timeoutMs`, 10 ms by default. A call past its deadline is
  cancelled and returns empty.
- **Circuit breaker.**
  - After `failureThreshold` consecutive failures (default 5) the breaker opens, and calls return
    at once without touching the network.
  - After `coolDownMs` (default 5 s) exactly one trial call goes out. Its success closes the
    breaker; its failure reopens it.
  - Timeouts, `UNAVAILABLE` and `INTERNAL` count as failures. `INVALID_ARGUMENT` does not, because
    that is the caller's bug, not the server's health.
- **Warm-up.** `create()` starts connecting at once, and `warmUp()` sends a `Health` call with a
  generous deadline. The first call on a fresh channel pays for the TCP and HTTP/2 set-up, which
  alone is longer than 10 ms. Without the warm-up, the first five 10 ms calls time out and open
  the breaker. That happened in the first bench run: 2,000 of 2,000 empty.
- **Latency log.** Client-observed latency is kept for the last 1,024 calls. Every
  `latencyLogEvery` calls (default 500) the client logs p50, p95 and p99 with the failure,
  short-circuit and breaker counts.

The config block (`configs/ml.yaml`) is `prediction.enabled`, `host`, `port`, `timeoutMs`,
`failureThreshold`, `coolDownMs` and `latencyLogEvery`. Prompt 18 turns it on.

## Latency

These results are from a laptop i5-8350U (4 cores, 15 W) with a server of 4 threads.

**Python bench.** `python -m predisched_ml.bench_server --workers 3 --requests 2000` sends requests
one at a time after 100 warm-up calls. Tasks and worker states are drawn from the dataset. Each
run appends a row to `results/prediction-latency.csv`.

| Run | Client p50 | Client p95 | Client p99 | Server p50 | Server p95 | Server p99 | Req/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| First version (pandas features) | 5.03 | 5.50 | 6.28 | 4.20 | 4.62 | 5.42 | 196 |
| NumPy features, one-shot frame | 3.69 | 4.26 | 5.00 | 2.89 | 3.40 | 4.12 | 263 |
| Standalone server (JSONL + DB logging on) | 4.60 | 5.25 | 5.90 | 3.48 | 4.08 | 4.79 | 215 |
| 10 workers per request | 3.86 | 4.33 | 5.06 | 3.05 | 3.46 | 4.18 | 254 |
| 4 concurrent clients | 15.56 | 22.94 | 27.18 | 3.56 | 7.56 | 10.24 | 250 |

All times are in ms.

**Java client.** `java -jar predisched-benchmark.jar prediction-latency --requests 2000 --workers 3`
goes through the real `PredictionClient` with its 10 ms deadline:

```
2000 requests x 3 workers via PredictionClient (deadline 10 ms): 2000 answered, 0 empty (breaker CLOSED)
              p50      p95      p99      max  (ms)
client      5.436    7.175    7.881   10.801
server      3.922    5.609    6.220    7.468
```

The client's own log line: `Prediction latency (client, last 1024 calls): p50=5.27 ms p95=7.12 ms
p99=7.67 ms; 2000 calls, 0 failures, 0 short-circuited, breaker CLOSED`.

What the numbers mean:

- **The p95 is under 10 ms everywhere except with concurrent callers**, which meets the prompt's
  bar. From Java, 7.2 ms p95 leaves about 13 ms of the 20 ms decision budget for the rest of the
  dispatch.
- **What changed to get here** (the prompt asks this be recorded): the first `features.py` built
  the matrix with pandas column operations. That cost 7.5 ms for 3 rows, and M1 and M3 each paid
  it, for about 18 ms per request measured in-process. `feature_matrix` now does the same work in
  NumPy: 0.29 ms, identical output (the sums match to 1e-10 over all 11,318 rows, and the tests
  check equality). Building the request frame in one step then took the server p95 from 4.6 ms to
  3.4 ms.
- **Adding workers is almost free.** 10 workers cost the same as 3: the models run once per
  request, as a batch.
- **Concurrency does not scale.** With 4 callers, throughput stays at about 250 req/s, because
  Python's GIL serialises the model code. Calls queue, and the client p95 reaches 23 ms. The
  scheduler dispatches one task at a time, so this is acceptable here. A busier scheduler would
  need several server processes, or a smaller model in the JVM.

## Demo

A request in `ml/testdata/predict-request.json`: a `MATRIX_TASK size=300` over the three campaign
workers. grpcurl is not installed on this machine. `python -m predisched_ml.client` sends the same
JSON with the same JSON mapping and prints the answer the same way; with reflection on, the grpcurl
command in the prompt works against the same server.

```
$ python -m predisched_ml.client < testdata/predict-request.json
{
  "predictions": [
    { "workerId": "worker-1", "predExecMs": 170.68731690099705,
      "predQueueLen": 0.21524422973698337, "overloadProb": 0.9805260565167925 },
    { "workerId": "worker-2", "predExecMs": 73.36779529821122,
      "overloadProb": 0.00026604185742782206 },
    { "workerId": "worker-3", "predExecMs": 214.9117416724543,
      "overloadProb": 9.164995385946113e-05 }
  ],
  "modelVersion": 1,
  "modelVersions": "m1=v1,m2=v1,m3=v1",
  "serverMs": 3.3859999966807663
}
```

- **worker-1**, the slow one, is full (2 active of 2, queue 2) and gets a 98 % overload risk.
- **worker-3** gets the highest execution time although it is idle. It has no history for this
  type (`type_worker_count` 0) and no recent average, so the linear M1 loses its two strongest
  inputs. That is the cost of a cold (type, worker) pair: prompt 18's cost function should not
  over-trust it.
- JSON omits zero fields (`predQueueLen` 0), as grpcurl does.

A bad request:

```
$ echo '{"task":{"type":"CPU_TASK"},"workers":[]}' | python -m predisched_ml.client
ERROR:
  Code: INVALID_ARGUMENT
  Message: workers is empty: send the state of at least one candidate worker
```

Logged: the JSONL line for `demo-task-1` has the three per-worker predictions and
`"latency_ms": 3.386`. The `predictions` table has 19,503 rows, which is 6,501 requests × 3
workers, including the demo task's three rows with `model_version = m1=v1,m2=v1,m3=v1`.

Hot reload, live: a second server runs on port 50071 over a copy of `ml/models`, and a new m3
version is saved into that registry:

```
Health before:  "modelVersions": "m1=v1,m2=v1,m3=v1"
saved m3 v2 into the served registry
INFO prediction_server - models reloaded: m1=v1,m2=v1,m3=v1 -> m1=v1,m2=v1,m3=v2
Predict after:  "modelVersions": "m1=v1,m2=v1,m3=v2"
Health after:   "reloads": "1"
```

## Failure behaviour

| What fails | What happens |
| --- | --- |
| Malformed request | `INVALID_ARGUMENT` with the reason; the server keeps serving; the Java breaker is not affected |
| Model code raises | `INTERNAL` with the error, logged with a stack trace; the server keeps serving |
| Server slow (above 10 ms) | Java call cancelled at the deadline, returns empty; the dispatcher falls back (prompt 18) |
| Server down | `UNAVAILABLE`, returns empty; after 5 in a row the breaker opens and calls stop for 5 s, then one trial |
| New models broken | The reload is refused and the old models keep serving |
| PostgreSQL down | Predictions are still served and logged to JSONL |

## Tests

- `pytest ml/tests/test_server.py` runs a real gRPC server on models trained on synthetic data, so
  it does not need the model binaries:
  - one prediction per worker, in order, with probabilities in [0, 1] and versions;
  - an unseen type answered as cold start with the fallback;
  - the JSONL record;
  - seven kinds of bad input, each returning `INVALID_ARGUMENT` with the server still serving
    afterwards;
  - a hot reload that changes the reported version and counts one reload;
  - a broken registry that keeps the old models.
- `PredictionClientTest` (JUnit, in-process fake server with an injected delay or failure):
  - empty on deadline exceeded, given up at the deadline and not at the server's 200 ms;
  - opens after K failures;
  - makes no call while open;
  - half-open after the cool-down, with one trial that closes the breaker;
  - a failed trial reopens it;
  - `INVALID_ARGUMENT` does not count;
  - never throws with the server gone;
  - latency-window percentiles.
