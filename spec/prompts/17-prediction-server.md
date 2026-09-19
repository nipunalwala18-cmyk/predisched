# Prompt 17: Prediction server over gRPC

## Goal

A Python gRPC `PredictionService` loads the live models and answers batched predictions for one task
across all workers fast enough for the scheduler's p95 < 20 ms decision budget. Every prediction is
logged for later comparison with the actual outcome.

## Read first

- Spec §10.4, §12.3, §5 (Performance), §3.4

## Build

1. **Protos**: `proto/prediction.proto` as spec §10.4 (`WorkerState`, `PredictRequest`,
   `WorkerPrediction`, `PredictResponse`, `PredictionService.Predict`). Add a `bool cold_start` to
   `WorkerPrediction` and a `Health` RPC. Generate Python stubs into `ml/generated/` with the command
   in RULES.
2. **`ml/predisched_ml/prediction_server.py`**
   - Loads the live version of each model from `registry.json` at start; `--port 50070`.
   - `Predict`: builds one feature row per worker with `features.py`, runs the three models as one
     batch each, returns per-worker `pred_exec_ms`, `pred_queue_len`, `overload_prob`, and the model
     version. Invalid requests → `INVALID_ARGUMENT`, never a crash (spec §17 ML row).
   - Uses a thread pool server with a small max-workers count; the models are loaded once and shared
     read-only.
   - Logs each prediction (request id, task id, worker, predictions, model versions, latency) as JSONL
     to `logs/predictions.jsonl`, and asynchronously into the `predictions` table.
   - A `reload` signal (file watch on `registry.json`) swaps models without restart.
3. **Java client** in `predisched-scheduler`: `PredictionClient` with a strict deadline
   (`prediction.timeout.ms`, default 10) and a circuit breaker (open after K consecutive
   failures/timeouts, half-open after a cool-down). It only returns predictions or an empty result;
   it never throws into the dispatcher.
4. **Latency bench**: `python -m predisched_ml.bench_server --workers 3 --requests 2000` reports
   p50/p95/p99 server latency, and the Java side logs client-observed latency.

## Tests

- `pytest`: the server answers a valid request with one prediction per worker; bad input returns
  `INVALID_ARGUMENT`; a hot reload changes the reported model version.
- Java: `PredictionClient` returns empty on deadline exceeded and opens the breaker after K failures
  (in-process fake server with an injected delay).

## Acceptance checks

```bash
python -m predisched_ml.prediction_server --port 50070
```
```bash
python -m predisched_ml.bench_server --workers 3 --requests 2000
```
```bash
grpcurl -plaintext -d @ localhost:50070 predisched.PredictionService/Predict < ml/testdata/predict-request.json
```

Paste the latency percentiles and the grpcurl response. If p95 exceeds 10 ms, reduce model size or
batch work before moving on, and record what changed.

## Docs and commit

- `docs/components/prediction-server.md`: contract, loading, latency numbers, failure behaviour.
- Commit: `Prediction: gRPC prediction server with batched inference and hot model reload`
