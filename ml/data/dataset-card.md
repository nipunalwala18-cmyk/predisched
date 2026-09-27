# Dataset card: PrediSched execution dataset

Built 2026-09-27 12:57 by `ml/build_dataset.py` from campaign `c15` (`configs/campaign.yaml`). Spec §12.1.

## Size

- **11,318 rows** (task executions; one per attempt), of 11,520 campaign rows in `execution_history`.
- 72 runs: 6 profiles x 3 patterns x 4 strategies.
- Dispatch times 2026-09-27 12:36:26 to 12:55:28 IST.
- Workers: worker-1 (pool 2, slowdown 2.0x), worker-2 (pool 4, slowdown 1.0x), worker-3 (pool 8, slowdown 1.0x). The slowdown is simulated: the worker stretches each execution.

Dropped while building:

- not part of a recorded campaign run: 0
- no worker_metrics sample within 2.0 s of t + 5 s: 202
- no worker_metrics sample in (t, t + N]: 0

## Labels

- `exec_time_ms`, `wait_time_ms`: measured by the worker / scheduler.
- `queue_len_future`: the worker's queue length 5 s after dispatch (last `worker_metrics` sample at or before t + 5 s, at most 2.0 s old).
- `overloaded_future`: some sample in (t, t + 5 s] has CPU > 85.0 % or queue length >= 1.0 x the worker's pool size.

Class balance of `overloaded_future`:

| value | rows | share |
| --- | --- | --- |
| true | 796 | 7.0% |
| false | 10,522 | 93.0% |

By worker (share overloaded): worker-1 23.8%, worker-2 0.0%, worker-3 0.0%

`queue_len_future` distribution: 0: 10,903, 1: 127, 2: 288

## Execution time per task type (ms, completed executions)

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

Rows by status: COMPLETED 11,318.

## Runs

| strategy | rows | runs |
| --- | --- | --- |
| least_loaded | 2,794 | 18 |
| random | 2,858 | 18 |
| resource_aware | 2,826 | 18 |
| round_robin | 2,840 | 18 |

| profile | rows |
| --- | --- |
| bursty | 1,876 |
| cpu_heavy | 1,920 |
| deadline | 1,762 |
| io_heavy | 1,920 |
| long_tail | 1,920 |
| mixed | 1,920 |

| pattern | rows |
| --- | --- |
| bursty | 3,796 |
| periodic | 3,682 |
| steady | 3,840 |

## Hold-out (prompt 16)

- Pattern `periodic` (3,682 rows): kept out of training to test generalisation to an unseen arrival pattern.
- Task type `GRAPH_TASK` (268 rows): kept out to test cold start on an unseen task type.
- Flagged by the `holdout_pattern` / `holdout_task_type` columns; nothing is removed.

## Columns

Features (all known at dispatch; `prior_stats_ts` and `win_window_end` are <= `dispatched_at`): `task_type`, `resource_profile`, `priority`, `input_size`, `strategy`, `worker_id`, `worker_cores`, `worker_pool_size`, `worker_slowdown`, `concurrent_tasks_on_worker`, `cpu_pct`, `mem_pct`, `active_threads`, `queue_len`, `arrival_rate`, `avg_exec_recent`, `prior_type_worker_mean_ms`, `prior_type_worker_count`, `win_arrival_rate`, `win_queue_len_mean`, `win_queue_len_max`.

Targets: `exec_time_ms`, `wait_time_ms`, `queue_len_future`, `overloaded_future`.

Bookkeeping: `id`, `task_id`, `run_id`, `profile`, `pattern`, `rate`, `attempt`, `status`, `dispatched_at`, `prior_stats_ts`, `win_window_end`, `holdout_pattern`, `holdout_task_type`.

Not joined on purpose: Spark's `type_worker.parquet` aggregates over the whole history, so each row's own execution time would leak into its features; `prior_type_worker_*` is the leak-free equivalent.
