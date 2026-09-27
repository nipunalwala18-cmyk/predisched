# Dataset: campaign runner and labelled execution dataset

Prompt 15, spec §12.1, §7.3, §7.4. This is the training data for the prediction models (prompt 16):
one row per task execution, with features known at dispatch and future-looking targets.

## What it does

1. **Heterogeneous workers.** `configs/campaign.yaml` defines three workers:

   | worker | pool | slowdown | Docker limits (prompt 24) |
   | --- | --- | --- | --- |
   | worker-1 | 2 | 2.0x | cpus 1.0, 512m |
   | worker-2 | 4 | 1.0x | cpus 2.0, 1g |
   | worker-3 | 8 | 1.0x | cpus 4.0, 2g |

   All three run on one machine, so a slow worker is **simulated**. `worker.slowdown` (or
   `--slowdown`) makes `ExecutionEngine` wait out `(factor - 1) x` the real execution time after
   every task, so a CPU_TASK that takes 5 ms reports 10 ms on worker-1. A cancel interrupts the
   wait. Values below 1 are clamped to 1, because it cannot speed a worker up.
2. **Campaign runner.** `scripts/collect-dataset.py` crosses 6 profiles, 3 patterns and 4
   strategies for 72 runs.
   - **Traces.** Each (profile, pattern) pair gets one trace, generated with seed `1500 + pair index`
     and saved under `workloads/campaign/`. All four strategies replay that same trace, so they see
     identical work.
   - **Rates.** A pair's base arrival rate is drawn from `[6, 15, 30, 45]` tasks/s with
     `Random(seed)`. Load therefore ranges from idle to saturated, and the rate is not tied to a
     profile or pattern.
   - **Cluster.** Runs are grouped by strategy, and each group starts a real scheduler
     (`--strategy`) plus the three workers.
   - **Replay.** Each trace is replayed with `--id-suffix ~c15-<strategy code>`, which keeps task
     ids unique across runs.
   - **Recording.** Each finished run is recorded in `benchmark_runs` (scenario `campaign:c15`).
     That record tags rows with their `run_id` and makes the runner resumable: completed runs are
     skipped.
   - `--plan` prints the run list, which is deterministic for a given config. `--only` reruns
     single runs.
   - Logs go to `logs/campaign/`.
3. **Label builder.** `ml/build_dataset.py` reads `execution_history`, `worker_metrics` and
   `benchmark_runs` from PostgreSQL, plus `ml/data/features/worker_windows.parquet` from the prompt 12
   Spark job. It writes `ml/data/dataset.parquet` and `ml/data/dataset-card.md`.

## Labels

Labels use N = 5 s after the dispatch time t. N, the thresholds and the tolerance are all under
`labels:` in the campaign config.

- `exec_time_ms`, `wait_time_ms`: measured by the worker and the scheduler.
- `queue_len_future`: the worker's queue length at t + N. It is taken from the last
  `worker_metrics` sample at or before t + N, which must be at most 2 s old. Workers report every
  second.
- `overloaded_future`: true if any sample in (t, t + N] has CPU > 85 % or a queue length of at least
  1.0 x the worker's pool size.

Rows without a usable future sample are dropped, and the card counts them by reason. That happens
mostly at the end of a run, when the cluster stops less than N seconds after the last dispatch.

## Features and leakage

Every feature is known at dispatch:

- the task (type, resource profile, priority, input size);
- the worker (cores, pool, slowdown);
- the worker's metrics snapshot the scheduler recorded with the dispatch (CPU, memory, active
  threads, queue, arrival rate, recent average execution time).

Two joins add history, and both only look backwards:

- **`prior_type_worker_mean_ms` / `_count`**: the expanding mean execution time of the same
  (task type, worker) pair, over executions that *finished* before this dispatch.
  `prior_stats_ts` records when the newest one finished.
- **`win_*`**: the latest Spark `worker_windows` row whose window ended at or before dispatch
  (`merge_asof`). `win_window_end` records the window's end.

Spark's `type_worker.parquet` is deliberately not joined. It aggregates over the whole history, so a
row's own execution time would leak into its features. The tests assert
`prior_stats_ts <= dispatched_at` and `win_window_end <= dispatched_at` on every row.

## Hold-out

Two slices are flagged for prompt 16 with the `holdout_pattern` and `holdout_task_type` columns.
Nothing is removed from the dataset.

- The `periodic` pattern (3,682 rows) tests generalisation to an unseen arrival pattern.
- `GRAPH_TASK` (268 rows) tests cold start on an unseen task type.

## Run it

```bash
python scripts/collect-dataset.py --config configs/campaign.yaml     # about 20 min; resumable
python spark/export_history.py
spark-submit --master "local[*]" spark/predisched_spark/features.py \
    --input spark/data/execution_history.csv --output ml/data/features
python ml/build_dataset.py
pytest ml/tests
```

The runner needs the jars (`mvn package -DskipTests`), PostgreSQL, and free ports 51051, 51061-51063
and 8100. `configs/campaign-node.yaml` turns the result cache off, so each replay really executes
its tasks.

## Results

Campaign run:

```
campaign c15: 72 runs planned, 1 already in the database, 71 to go
[ 2/72] c15-cpu_heavy-steady-round_robin: 160 tasks {'COMPLETED': 160} -> 160 history rows in 7.3 s
[ 3/72] c15-cpu_heavy-bursty-round_robin: 160 tasks {'COMPLETED': 160} -> 160 history rows in 12.5 s
...
[49/72] c15-long_tail-steady-least_loaded: 160 tasks {'COMPLETED': 160} -> 160 history rows in 21.2 s
...
[72/72] c15-deadline-periodic-resource_aware: 160 tasks {'COMPLETED': 160} -> 160 history rows in 7.1 s
campaign c15 done: 11520 execution rows in execution_history
```

All 72 runs completed their 160 tasks. The first run came from the trial that sized the rates.

The dataset card (`ml/data/dataset-card.md`), abridged:

- **11,318 rows** of the 11,520 campaign executions. 202 were dropped because no metrics sample
  existed within 2 s of t + 5 s: the workers stopped at the end of their strategy group.
- **Class balance of `overloaded_future`**: 796 true (7.0 %), 10,522 false (93.0 %).
  - By worker: worker-1 23.8 %, worker-2 0.0 %, worker-3 0.0 %.
- **`queue_len_future`**: 0 in 10,903 rows, 1 in 127, 2 in 288.

Execution time per task type (ms):

| task_type | rows | mean | p50 | p95 | max |
| --- | --- | --- | --- | --- | --- |
| COMPRESS_TASK | 492 | 347.3 | 179 | 1373 | 3,363 |
| CPU_TASK | 2,290 | 7.2 | 5 | 19 | 215 |
| FILE_IO_TASK | 980 | 42.1 | 35 | 98 | 246 |
| GRAPH_TASK | 268 | 73.9 | 34 | 294 | 634 |
| HASH_TASK | 916 | 21.5 | 17 | 53 | 271 |
| HTTP_TASK | 947 | 219.4 | 208 | 514 | 686 |
| MATRIX_TASK | 1,640 | 48.8 | 13 | 262 | 1,245 |
| MONTE_CARLO_TASK | 772 | 92.8 | 79 | 225 | 387 |
| SLEEP_TASK | 1,754 | 401.7 | 338 | 923 | 1,597 |
| SORT_TASK | 1,259 | 62.3 | 5 | 276 | 1,765 |

Rows per strategy: least_loaded 2,794, random 2,858, resource_aware 2,826, round_robin 2,840.
There are 18 runs each.

### Why overload only shows on worker-1

The overload label is imbalanced, and I kept the spec's definition rather than tune it until the
classes looked balanced. Two things explain the result:

- **The scheduler caps each worker's queue.** It keeps at most
  `outstandingPerWorkerFactor (2.0) x pool` tasks in flight per worker. Extra tasks wait in the
  scheduler queue, not the worker's. Only worker-1 is slow enough, at 2x slowdown with a pool of 2,
  to hold a full queue. Worker-2 and worker-3 never queued a task in this campaign; worker-3 peaked
  at 4 active threads out of 8.
- **CPU rarely reaches 85 %.** `cpu_pct` is the worker process's share of the whole 8-thread
  machine, and three workers share it. The highest sample was 34 %.

So `overloaded_future` means "worker-1 is about to be saturated". That is the realistic case this
cluster produces. The positive rate (7 %) is low, so prompt 16 should use class weights and report
precision and recall rather than accuracy.
