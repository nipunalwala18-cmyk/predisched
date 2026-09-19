# Prompt 11: PostgreSQL persistence, result cache and SLA tracking (spec §14, F6, F3)

## Goal

Tasks, workers, metrics, decisions, events, replication, clock-sync and failure history are stored in
PostgreSQL. Repeat work is served from a result cache. Tasks can carry a deadline, and SLA compliance
is reported per strategy. `DB_QUERY_TASK` runs against the same database.

## Read first

- Spec §14 (all tables), §4 FR13, FR24, FR27, §6 Tier 1 (F3, F6), §7.2 (`DB_QUERY_TASK`)

## Build

1. **Schema**: `db/migrations/V1__init.sql` (Flyway) creating every table in spec §14 with sensible
   types, primary keys and indexes on `(worker_id, ts)` and `(task_id)`. `benchmark_runs.metrics`
   is `jsonb`. Add `result_cache(key, task_type, output, created_at, hits)` and
   `tasks.deadline_at`, `tasks.client_id`, `tasks.trace_id`, `tasks.attempts`.
2. **`predisched-common`**: a small `Db` helper (HikariCP + plain JDBC, no ORM here) and `Flyway`
   migration on startup when `db.enabled: true`. Local dev uses `docker run postgres:16` (document the
   exact command); tests use Testcontainers.
3. **Persistence** (all asynchronous and batched, so the dispatch path never waits on the database):
   - `HistoryWriter`: a bounded queue drained by one thread that batch-inserts into `worker_metrics`,
     `scheduling_decisions`, `events`, `replication_log`, `clock_sync`, `failures`, and
     `execution_history`. If the queue fills, drop the oldest metrics rows and count drops; never
     block the scheduler.
   - `execution_history` rows carry the feature columns of spec §12.1 captured at dispatch time
     (worker load at dispatch, concurrent tasks, arrival rate over the last 10 s, recent average
     exec time, input size). This is the ML dataset source in prompt 15.
   - `PostgresTaskStore implements TaskStore` for the primary's durable copy; replication (prompt 07)
     stays the source of truth between schedulers.
4. **Result cache (F6)**: key = SHA-256 of `type + normalised input`. Caffeine in front of the
   `result_cache` table. Only deterministic task types are cacheable (a flag on `TaskExecutor`;
   `SLEEP_TASK`, `HTTP_TASK`, `FILE_IO_TASK` are not). A cache hit completes the task immediately
   with `worker_id = "cache"` and is counted. `--no-cache` on submit bypasses it.
5. **Deadlines and SLA (F3)**: add `int64 deadline_ms` (relative to submit) to `TaskRequest`. The
   scheduler records `deadline_at`, and on completion marks `sla_met`. `SlaReport` computes on-time %
   per strategy and per task type from the database. CLI: `predisched report sla [--since]`.
6. **`DB_QUERY_TASK`**: input `rows=…` runs an aggregate over a seeded `db_query_data` table (created
   by migration `V2`), through a small separate pool so it cannot starve the scheduler's pool.

## Tests

- Migrations apply on an empty Testcontainers Postgres.
- `HistoryWriter` under load: 10,000 metric rows arrive in the database; a full queue drops metrics,
  not decisions.
- Cache: second identical `CPU_TASK` is a hit; `SLEEP_TASK` is never cached; different input misses.
- SLA: tasks with tight and loose deadlines give the expected on-time %.

## Acceptance checks

```bash
docker run -d --name predisched-pg -e POSTGRES_PASSWORD=predisched -e POSTGRES_DB=predisched -p 5432:5432 postgres:16
```
```bash
java -jar predisched-client/target/predisched-client.jar workload replay workloads/mixed-steady-7.jsonl
```
```bash
java -jar predisched-client/target/predisched-client.jar report sla
```

Paste row counts per table after the replay (a `psql` query), the cache hit count after replaying
the trace twice, and the SLA report.

## Docs and commit

- `docs/components/storage.md`: schema, write path, cache rules, SLA definition.
- Commit: `Storage: PostgreSQL history, result cache and deadline SLA tracking`
