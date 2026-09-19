# Prompt 21: Shadow models, drift detection and predictive auto-scaling (F14, F15, F12)

## Goal

A new model version can run in shadow mode beside the live one and be promoted when it proves better.
Live prediction error is watched for drift, which raises an alert and can trigger retraining. When the
queue forecast exceeds capacity, extra workers start; when load drops, idle workers stop. This prompt
is stretch work: do it after 22–24 if time is short.

## Read first

- Spec §4 FR29, FR33, FR34, FR41, §6 Tier 2 (F12, F14, F15), §12.4 (proactive actions), §20

## Build

1. **Model registry and shadow mode (F14)**
   - `registry.json` statuses: `live`, `shadow`, `retired`. The prediction server loads live and
     shadow versions; `PredictResponse` gains `repeated WorkerPrediction shadow_predictions` and
     `int32 shadow_version` (new field numbers). The scheduler uses only live predictions.
   - Shadow predictions are logged beside live ones. `ml/predisched_ml/compare_shadow.py` computes
     both MAEs on the same completed tasks.
   - Promotion: `python -m predisched_ml.registry promote <model> <version>` (and the dashboard API
     endpoint in prompt 22). It refuses unless the shadow beats live by the configured margin over
     at least N tasks, with `--force` to override (logged).
2. **Drift detection (F15)**
   - The scheduler's rolling MAE (prompt 18) is compared with the model's test MAE from `meta.json`.
     Sustained ratio above `drift.threshold` for `drift.window` tasks raises a `DRIFT` event.
   - Alerts (F23, minimal): `AlertSink` posting JSON to a configured webhook URL for `DRIFT`,
     `SLA_BREACH` and `NODE_FAILURE` events; no-op when unset.
   - `drift.auto_retrain: true` runs `build_dataset` + `train` on recent data, registers the result as
     `shadow`, never as `live`.
3. **Predictive auto-scaling (F12)**
   - `AutoScaler` on the primary: every `autoscale.interval.ms`, if the summed queue forecast over
     the horizon exceeds capacity × `autoscale.up.ratio` for two checks in a row, start a worker;
     if cluster utilisation stays under `autoscale.down.ratio` for the cool-down, drain then stop the
     most idle scaled worker. Min/max worker counts from config.
   - `WorkerLauncher` interface with `ProcessLauncher` (local `java -jar`) and `DockerLauncher`
     (`docker compose up --scale` or `docker run`). Every scale event is logged and emitted.
   - A reactive variant (queue length now instead of forecast) behind the same interface, so the
     benchmark can compare them fairly.
4. **Benchmark**: add a `bursty-autoscale` scenario to `configs/benchmark.yaml` comparing no scaling,
   reactive scaling and predictive scaling.

## Tests

- Promotion rules: refused below margin or sample size, allowed above, `--force` logged.
- Drift: synthetic error series crossing the threshold raises exactly one event per episode.
- AutoScaler with a fake launcher and scripted forecasts: scales up after two high checks, down after
  cool-down, never outside min/max.

## Acceptance checks

```bash
java -jar predisched-client/target/predisched-client.jar chaos burst 400 --profile bursty
```
```bash
python -m predisched_ml.compare_shadow --model m1
```
```bash
java -jar predisched-benchmark/target/predisched-benchmark.jar run-suite --config configs/benchmark.yaml --scenario bursty-autoscale --reps 3
```

Paste the scale-up and scale-down log lines, the shadow comparison, and the auto-scaling scenario
summary.

## Docs and commit

- `docs/components/model-lifecycle.md` and `docs/components/autoscaling.md`.
- Commit: `ML ops: shadow models, drift alerts and predictive auto-scaling`
