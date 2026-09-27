# Model lifecycle: shadow models, drift, alerts

Prompt 21, F14, F15, F23 (minimal), FR29, FR33, FR34. This covers how a new model version gets
from training to serving without risk, and how the system notices when the live one goes stale.

## Registry and shadow mode (F14)

`ml/models/registry.json` gives every version of each model a status (`ml/predisched_ml/registry.py`):

- `live` serves the scheduler.
- `shadow` predicts beside live on every request, is logged, and is never used for placement.
- `retired` is a former live or shadow version.

Each model has at most one live and one shadow version. Older registry entries get their statuses
derived when loaded.

```
python -m predisched_ml.registry list
python -m predisched_ml.registry shadow m1 2         # run v2 beside live
python -m predisched_ml.registry promote m1 2 [--force]
```

**Serving the shadow.** The prediction server loads the live and shadow versions, and a registry
change reloads both without a restart.

- `PredictResponse` gains `repeated WorkerPrediction shadow_predictions = 5` and
  `int32 shadow_version = 6`. For each model, the shadow list holds its shadow version's
  predictions where it has one, and live's otherwise.
- The scheduler reads only `predictions`.
- `Health` now reports `shadow_versions`, and `m1_test_mae_ms`, the baseline for drift.
- Every logged answer in `logs/predictions.jsonl` carries `m1_version`, `shadow_version` and
  `shadow_predictions` beside the live ones.

**Comparing.** `python -m predisched_ml.compare_shadow --model m1` scores both versions on the same
(task, worker) pairs. A task's actual execution time exists only for the worker it ran on
(`execution_history`), so both versions are judged on exactly those pairs. Only M1 can be compared
this way: queue and overload labels need the future metrics series.

**Promotion.** `promote` refuses unless the version beats live's MAE by `promotion.margin` (5 %)
over at least `promotion.minTasks` (200) compared tasks, both set in `ml/config.yaml`. `--force`
overrides and says so. Every promotion is appended to `ml/models/promotions.log` with its
comparison, and the prediction server picks up the new live version by itself.

**Retraining.** `python -m predisched_ml.retrain [--since-hours 24]` trains M1 on recent completed
executions and registers the result as `shadow`, never `live`:

- **Data**: `execution_history`, with the dataset builder's leak-free prior statistics.
- **Training**: a time split of 80/20 and the live version's family and parameters.
- **Scoring**: the new model and live are scored on the same held-out rows.
- **Gap**: the history does not record the workers' simulated slowdown, so it is taken as 1.

## Drift (F15)

`PredictiveStrategy` feeds each completed prediction to a `DriftDetector`. The detector divides the
rolling MAE of the last 500 tasks by the live M1's test MAE, which it gets from the prediction
server's `Health`. A ratio above `drift.threshold` (1.5) for `drift.window` (50) tasks in a row
starts a **drift episode**. An episode is reported exactly once and ends when the ratio falls back
to the threshold or below. Each episode produces:

- a `DRIFT` event and log line;
- a `DRIFT` alert;
- with `drift.autoRetrain: true`, one run of `drift.retrainCommand` at a time (`RetrainLauncher`,
  output in `logs/retrain-<time>.log`). That command defaults to `python -m predisched_ml.retrain`,
  so drift can produce a shadow model but never a live one.

## Alerts (F23, minimal)

`AlertSink` (`predisched-common`) posts one JSON object per alert to `alerts.webhookUrl`,
asynchronously, for three events:

- `DRIFT` from the predictive strategy;
- `NODE_FAILURE` when a worker is declared dead;
- `SLA_BREACH` when a task completes after its deadline.

There is at most one alert per type every `alerts.minIntervalMs` (5 s), and the next one sent
carries `suppressed_since_last`. Without a URL, no alerts are sent.

```json
{"type":"DRIFT","node":"scheduler-1","at":"…","suppressed_since_last":1,"details":{"ratio":"2.3"}}
```

## Live run

Setup:

- `python -m predisched_ml.retrain --since-hours 24` trained M1 v2 on the day's 34,940 executions
  (the benchmark suites included) and registered it as shadow. On its held-out 8,735 rows it scored
  MAE 50.06 ms, against 63.31 ms for live v1 on the same rows.
- A scheduler ran on `configs/benchmark-node.yaml` with `--strategy predictive --autoscale
  predictive` and one worker with a pool of 2.
- The prediction server reported `m1=v1,m2=v1,m3=v1 ...; shadow m1=v2`.
- The load was `predisched chaos burst 400 --profile bursty`.

**Drift** during the burst:

```
DRIFT: live exec-time MAE 56.4 ms is 2.01x the model's test MAE 28.0 ms for 50 tasks in a row (threshold 1.5x)
```

**Shadow comparison** afterwards:

```
$ python -m predisched_ml.compare_shadow --model m1
m1: 309 completed tasks predicted by both versions
  live   v1: MAE 43.22 ms
  shadow v2: MAE 32.53 ms (-24.7% vs live: better)
```

**Promotion**, which met the 5 % margin over at least 200 tasks. The server reloaded by itself:

```
$ python -m predisched_ml.registry promote m1 2
m1 v2 is live (was v1)
prediction_server - models reloaded: m1=v1,m2=v1,m3=v1 shadow m1=v2 -> m1=v2,m2=v1,m3=v1
```

`promotions.log` recorded
`{"model": "m1", "version": 2, "previous_live": 1, "forced": false, "comparison": {"tasks": 309, "live_mae": 43.22, "shadow_mae": 32.53, ...}}`.

**Afterwards.** v2 was trained on this machine's database, which the repository does not contain,
so the committed registry stays at the reproducible v1 (`python -m predisched_ml.train` rebuilds
it). The demo's v2 and its promotion log were kept aside under `logs/`.

## Tests

- `ml/tests/test_lifecycle.py`:
  - statuses across saves and shadow assignment, and a live version cannot also be the shadow;
  - promotion refused below the sample size, below the margin, and without a comparison;
  - promotion allowed above them, with the old live retired;
  - `--force` promotes a worse version and logs a warning and a `forced` record;
  - `compare` scores both versions only on the (task, worker) pairs that ran;
  - a live server answers `shadow_predictions` beside live and logs them;
  - `retrain` fits on recent rows and scores live on the same test rows.
- `AutoScalerTest.driftIsOneEventPerEpisode`: a synthetic error series gives exactly one event per
  episode (at the 3rd and 10th values), and no baseline means no drift.
- `AlertSinkTest`: JSON posted to a local HTTP server, rate-limited per type, other types not held
  back, the suppressed count, and a no-op without a URL.
