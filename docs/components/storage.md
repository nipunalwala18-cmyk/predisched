# Storage: PostgreSQL history, result cache and deadline SLA tracking

Task state, worker state and metrics, scheduling decisions, executions (with the ML features
captured at dispatch), events, replication, clock synchronisation and failures are stored in
PostgreSQL. Identical deterministic work is served from a result cache. A task can carry a deadline,
and on-time completion is reported per strategy and per task type. `DB_QUERY_TASK` runs against the
same database.

Built by `spec/prompts/11-persistence-cache.md`. Spec sections: §14, §4 FR13, FR24, FR27, §6 Tier 1
(F3, F6), §7.2 (`DB_QUERY_TASK`).

## Running a database

```bash
docker run -d --name predisched-pg -e POSTGRES_PASSWORD=predisched -e POSTGRES_DB=predisched -p 5432:5432 postgres:16
```

`configs/storage.yaml` points at exactly that (user `postgres`, password `predisched`, database
`predisched`) and turns on `db` and `cache`. Without Docker, the PostgreSQL 16 Windows binaries work
the same way:

```bash
initdb -D pgdata -U postgres --pwfile=<file containing predisched> -A scram-sha-256 -E UTF8
pg_ctl -D pgdata -o "-p 5432" start
psql -h localhost -U postgres -c "CREATE DATABASE predisched"
```

The scheduler applies the Flyway migrations when it starts. Workers never migrate; they only open
their `DB_QUERY_TASK` pool.

## Schema (`db/migrations`)

`V1__init.sql` creates every table of spec §14; `V2__db_query_data.sql` creates and seeds
`db_query_data`. The migrations live at the repo root and ship inside the `predisched-common` jar
(`classpath:db/migration`).

| Table | Written when | Notes |
| --- | --- | --- |
| `tasks` | every state change made on the primary | upsert by `task_id`; `deadline_at`, `sla_met`, `strategy`, `client_id`, `trace_id`, `attempts`, `workflow_id` |
| `workers` | register, heartbeat, declared dead | upsert; `status` UP / DEAD |
| `worker_metrics` | every heartbeat (primary only) | index `(worker_id, ts)` |
| `execution_history` | every attempt that ran | spec §12.1 features at dispatch; `queue_len_future` and `overloaded_future` are left for the Spark job (prompt 15) |
| `scheduling_decisions` | every worker choice | chosen worker's score as `cost`, all scores as `jsonb` |
| `events` | every event-log event | same stream as `logs/<node>.jsonl`, `details` as `jsonb` |
| `replication_log` | every change a replica applies | `(node_id, seq_no)` |
| `clock_sync` | every Berkeley round, per node | offset before and after |
| `failures` | worker declared dead, primary lost | `recovered_at` set when the worker re-registers or the scheduler rejoins |
| `predictions`, `benchmark_runs` | created now, filled by prompts 17 and 20 | |
| `result_cache` | first cacheable result for a key | `hits` counted |
| `db_query_data` | seeded by V2 | 200 000 rows from a fixed formula, not `random()` |

Every table that is read by task or by worker and time has an index on `(task_id)` or
`(worker_id, ts)`.

## Write path

```
scheduler threads --(HistorySink call, returns at once)--> bounded buffer --> drain thread
   dispatcher, registry, event log,                           (50 000 rows)      batches of 500 or
   replication, clock daemon, store                                              every 200 ms,
                                                                                 one JDBC batch per
                                                                                 statement, one
                                                                                 transaction
```

- **`History`** holds the JVM's `HistorySink`, the same pattern as `EventLog` and `Clocks`. A node
  without a database gets a sink that drops everything, so none of the call sites branch.
- **`HistoryWriter`**: nothing that reports history ever waits on the database.
  - When the buffer is full, the oldest `worker_metrics` row is dropped to make room: metrics come
    every second and a gap in them costs little.
  - Decisions, executions and task states are written once, so they are kept. Only with no metric
    left to drop is a new row itself dropped.
  - Both kinds of drop are counted. A failed batch is logged and counted, and the thread carries on.
- **`PostgresTaskStore`** wraps the scheduler's store (in memory, or replicated between schedulers,
  which stays the source of truth). Each write goes there first, then to `tasks` as an upsert
  through the writer. Reads never touch the database.
  - On a primary-backup cluster only the primary writes through the store, so only the primary's
    copy is kept.
  - Likewise only the primary records worker metrics: workers heartbeat every scheduler.

## Result cache (F6)

- **Key.** SHA-256 of the task type and the normalised input: parameters parsed, trimmed and sorted
  by name. `samples=10, seed=3` and `seed=3,samples=10` are the same work.
- **What may be cached.** Each `TaskExecutor` has `deterministic()`. `SLEEP_TASK`, `HTTP_TASK`,
  `FILE_IO_TASK` and `DB_QUERY_TASK` say false. The worker sets `ExecuteResult.cacheable` from it,
  and the scheduler stores only results marked cacheable. The scheduler never needs to know
  executor classes (rule 3).
- **Where it lives.** Caffeine (10 000 entries) in front of `result_cache`. A key missing from
  memory is read from the table once, on the submit path, never the dispatch path; an unreachable
  database counts as a miss. New entries and hit counts are written asynchronously. Because the
  table survives restarts, a new scheduler finds what an old one computed.
- **A hit** stores the task and completes it at once: worker and strategy `cache`, one SUCCEEDED
  attempt, exec time 0. Status, SLA, quotas and workflows see an ordinary finished task.
  `submit --no-cache` skips the lookup.
- Off by default (`cache.enabled`), so benchmarks that replay identical inputs measure scheduling,
  not the cache.

## Deadlines and SLA (F3)

- **Setting one.** `TaskRequest.deadline_ms` is relative to submit. The scheduler records
  `deadline_at = submitted_at + deadline_ms`. The task is **on time** if it COMPLETED no later than
  that, and is marked `sla_met` when it finishes. It is **late** if it finished any other way
  (completed after the deadline, failed, cancelled), and **open** while unfinished.
- **The report.** On-time % = on time / (on time + late). `SlaReport` computes it per strategy and
  per task type from `tasks`: `predisched report sla [--since 30m|2h|1d]`.
- **Where deadlines come from.** `submit --deadline-ms N`, and `workload replay --deadline-ms N` for
  every task of a trace. Each finished task also emits an `SLA_MET` or `SLA_MISSED` event, and a
  missed one is logged with how late it was.

## DB_QUERY_TASK

Input `rows=N` (1–200 000). It returns count, sum, average, distinct categories and max over the
first N rows of `db_query_data`. It runs on the worker's own HikariCP pool of `db.queryPoolSize` (2)
connections, so a burst of queries queues on those two and cannot take connections from anything
else. A worker without `db.enabled` has no executor for it and fails such a task.

## ML features in `execution_history`

Captured when the attempt is sent:

- the strategy;
- the worker's cores and its last heartbeat's CPU %, memory %, active threads, queue length and
  average execution time;
- tasks this scheduler had in flight on that worker;
- accepted submits per second over the last 10 s;
- the input size (the first required numeric parameter: `n`, `size`, `ms`, `rounds`, ...).

Written with the attempt's wait time, exec time and status. For an attempt a new primary only
collected the result of (prompt 10), the features are null.

## How to run and demo

```bash
# database as above, then:
mvn -q verify
python scripts/mock-http.py --port 8100 &                      # for the trace's HTTP_TASKs
scripts/start-cluster.ps1 -Config configs/storage.yaml -Schedulers 1 -Workers 3
predisched workload replay workloads/mixed-steady-7.jsonl --deadline-ms 3000
psql -h localhost -U postgres -d predisched -c "SELECT count(*) FROM tasks"   # etc.
predisched workload replay workloads/mixed-steady-7.jsonl --deadline-ms 3000 --id-suffix -r2
predisched report sla
```

`--id-suffix` replays the trace as new tasks. Without it the scheduler answers every submit as
already accepted: a repeated id is the same task (prompt 10).

Tests that need a database find one in this order:

1. `PREDISCHED_TEST_DB_URL` (an admin database such as
   `jdbc:postgresql://localhost:5432/postgres`, with `PREDISCHED_TEST_DB_USER` and
   `PREDISCHED_TEST_DB_PASSWORD`, defaulting to `postgres` and `predisched`). Each test class gets a
   throwaway database there.
2. A Testcontainers `postgres:16`, if Docker is available, as on CI.
3. Neither: the test classes are skipped, not failed.

## Real output

Windows 11, the portable PostgreSQL 16.4 binaries on port 5432, one scheduler and three workers
from `configs/storage.yaml`, the mock HTTP server running.

Scheduler start:

```
Database schema at version 2 (2 migrations applied now)
Scheduler scheduler-1 listening on 51051, waiting for workers to register (clock offset 0 ms, sync berkeley); history in jdbc:postgresql://localhost:5432/predisched; result cache on
```

`workload replay workloads/mixed-steady-7.jsonl --deadline-ms 3000`: 400 completed, 0 failed.
Row counts then:

```
      table_name      | count
----------------------+--------
 tasks                |    400
 workers              |      3
 worker_metrics       |    345
 execution_history    |    383
 scheduling_decisions |    383
 events               |   2360
 replication_log      |      0
 clock_sync           |     44
 failures             |      0
 predictions          |      0
 benchmark_runs       |      0
 result_cache         |    258
 db_query_data        | 200000
```

- 400 tasks, 383 dispatched: the trace repeats 17 deterministic inputs, and those were already
  served from the cache on the first pass.
- `replication_log` is empty because a standalone scheduler replicates nothing. On a cluster with
  replication, every node logs every change it applies.

Replaying the trace a second time as new tasks (`--id-suffix -r2`), all 275 of its deterministic
tasks came from the cache. SLEEP, FILE_IO and HTTP ran again:

```
 served_from_cache | in_second_replay | deterministic_in_second
-------------------+------------------+-------------------------
               292 |              275 |                     275

 result_cache_hits | entries
-------------------+---------
               292 |     258
```

`report sla` after both (3 s deadlines, 5 tasks/s: everything made it):

```
strategy            on time     late     open         on-time %
cache                   292        0        0         100.0
round_robin             508        0        0         100.0
all                     800        0        0         100.0
```

A third replay at 4× speed with a 200 ms deadline (`--speed 4 --deadline-ms 200 --id-suffix -r3`),
`report sla --since 1m`:

```
strategy            on time     late     open         on-time %
cache                   275        0        0         100.0
round_robin              74       51        0         59.2
all                     349       51        0         87.3

task type           on time     late     open         on-time %
COMPRESS_TASK            41        0        0         100.0
CPU_TASK                 37        0        0         100.0
FILE_IO_TASK             39        4        0         90.7
GRAPH_TASK               25        0        0         100.0
HASH_TASK                46        0        0         100.0
HTTP_TASK                29       19        0         60.4
MATRIX_TASK              48        0        0         100.0
MONTE_CARLO_TASK         36        0        0         100.0
SLEEP_TASK                6       28        0         17.6
SORT_TASK                42        0        0         100.0
all                     349       51        0         87.3
```

`SLEEP_TASK` averages about 390 ms, so most miss a 200 ms deadline. The deterministic types all
came from the cache in 0 ms.

## Tests

- `StorageTest` (real PostgreSQL):
  - migrations create every table and seed 200 000 rows, and a second run applies nothing;
  - 10 000 metric rows arrive;
  - with a full, undrained buffer of 100, all 60 decisions are written and 260 of 300 metrics are
    dropped, the oldest first;
  - tight and loose deadlines give 3/5 on time per strategy and 60 % for the type.
- `ResultCacheTest`:
  - key normalisation;
  - a second identical `CPU_TASK` is served by `cache` and the worker ran it once;
  - `SLEEP_TASK` runs every time; a different input misses; `--no-cache` runs it;
  - a deadline gives `sla_met`;
  - a result stored by one cache is found in the table by a fresh one, and the hit is counted.
- `DbQueryTaskExecutorTest`: the aggregate over the seeded table (same answer twice, rows
  validated); which executors are cacheable.
- Existing tests updated: `DB_QUERY_TASK` is no longer a reserved type.

Not done yet: `benchmark_runs` and `predictions` are only created (prompts 20 and 17 fill them), and
the `*_future` targets in `execution_history` are computed by the Spark job (prompt 15).
