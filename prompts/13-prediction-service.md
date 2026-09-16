# Prompt 13 — Prediction service (Python gRPC)

**Spec sections:** §3.4, §5 (Performance), §8.4, §10.3
**Depends on:** 12
**Lab topics:** RPC (cross-language service)
**Commit message:** `Prediction service: models served over gRPC with batching, versioning and health checks`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Serve the trained models to the scheduler over gRPC, fast
enough that a prediction call fits inside a scheduling decision budget of < 20 ms P95 (spec §5).

### Contract

`proto/prediction.proto` as §8.4, with two additions (new field numbers only):

- `PredictRequest.request_id` (string) and `WorkerState.matrix_backend` (string).
- `rpc Health(Empty) returns (HealthResponse)` with the served model versions and uptime.

### Server (`ml/prediction_server.py`, package `ml/predisched_ml/serving/`)

- Loads the `CURRENT` version of each model at start using `features.make_matrix` (Prompt 12) — the
  same feature code as training.
- `Predict` builds **one matrix with a row per worker** and calls each model once (vectorised), never
  once per worker.
- Derives `pred_queue_len` from the queue model and `overload_prob` from the classifier, and returns
  `model_version` as an integer derived from the three versions.
- Input validation: missing or NaN fields are imputed from training medians saved in `features.json`;
  a request with no workers or an unknown task type returns `INVALID_ARGUMENT`, never a crash.
- `grpc.server` with a thread pool; models are read-only after load so no locking is needed. A
  `SIGHUP` (or `ReloadModels` admin RPC on Windows) reloads `CURRENT` atomically.
- Logs each prediction (request id, worker, predicted values) to a JSONL file and, when a DB is
  configured, to the `predictions` table via a background batch writer.
- Port 50070 from config.

### Java client (`predisched-scheduler`, package `prediction`)

- `PredictionClient` with a per-call deadline (`settings.predictionTimeoutMs`, default 10) and a
  circuit breaker: after 5 consecutive failures or timeouts it stops calling for 10 s and reports
  `unavailable`, then probes with one call. Exposes latency and failure counters.
- No strategy uses it yet — Prompt 14 does.

### Measurement

`ml/bench_prediction.py`: P50/P95/P99 latency of `Predict` for 3, 5 and 10 workers at 1, 10 and 50
concurrent callers; writes `results/prediction-latency.csv`.

### Tests

- Python: server round-trip with a tiny fixture model; bad input returns `INVALID_ARGUMENT`; reload
  switches the served version without dropping in-flight calls.
- Java: `PredictionClientTest` against an in-process fake: timeout → empty result within deadline + 5 ms;
  breaker opens after 5 failures and half-opens after the cool-down.

### Acceptance checks

```bash
python ml/prediction_server.py --config configs/local/prediction.yaml
grpcurl -plaintext -d @ localhost:50070 predisched.PredictionService/Predict < ml/examples/predict.json
python ml/bench_prediction.py
mvn -q verify
```

### Docs

`docs/components/prediction-service.md` with the latency table.
