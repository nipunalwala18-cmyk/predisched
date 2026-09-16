# Prompt 08 — Persistence and telemetry (PostgreSQL)

**Spec sections:** §4 (FR13), §5 (Observability), §10.1, §12
**Depends on:** 07
**Lab topics:** none directly (records the data that analytics, ML, benchmarks and the dashboard use)
**Commit message:** `Telemetry: tasks, metrics, decisions, events and failures recorded in PostgreSQL`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Everything the product already observes in memory —
task outcomes, worker metrics, scheduling decisions, events, replication, clock sync, failures — is now
written to PostgreSQL, without ever slowing down or blocking scheduling.

### Schema

- `db/migrations/V1__schema.sql` creating every table in spec §12 with sensible types, primary keys and
  indexes on `(worker_id, ts)`, `(task_id)`, `(run_id)`. Add a nullable `run_id` column to `tasks`,
  `execution_history`, `scheduling_decisions` and `predictions` so benchmark runs can be separated.
- Flyway (pinned in the parent POM) runs migrations at primary-scheduler start.
- `configs/local/db.yaml` + environment overrides `PREDISCHED_DB_URL`, `_USER`, `_PASSWORD`.
- `scripts/start-postgres.(ps1|sh)` running `postgres:16` in Docker on 5432 for local development.

### Build (`predisched-common`, package `telemetry`; wiring in the scheduler)

- `TelemetrySink` interface with `record(…)` methods per record type, and two implementations:
  `PostgresTelemetrySink` (HikariCP, batched inserts from a bounded queue flushed every 500 ms or 500
  rows by one background thread; when the queue is full, drop the **oldest metrics rows** and count the
  drops, but never drop task, decision or failure rows — block briefly for those) and `NoopTelemetrySink`
  when no DB is configured, so every earlier test still runs without a database.
- `MetricsCollector` on the primary: writes each heartbeat to `worker_metrics`.
- `execution_history` row on every task completion containing **the features as they were at dispatch
  time** (spec §10.1: input size, priority, worker cores, cpu/mem %, active threads, queue length,
  arrival rate, recent mean exec time) plus wait and exec time. Capture the snapshot when dispatching,
  not when the result arrives — note this in the docs, it matters for the ML.
- Future-looking labels (`queue_len_future`, `overloaded_future`) are **not** computed here; Prompt 11
  derives them from `worker_metrics`.
- Wire the existing in-memory rings (decisions, events, replication log, clock sync, failures) to the
  sink.
- Only the primary writes; backups do not, so nothing is double-counted after failover.

### Tests

- `PostgresTelemetrySinkIT` with Testcontainers PostgreSQL: rows appear, batches flush, a full queue
  drops only metrics and reports the count.
- `ExecutionHistoryIT`: a completed task's history row matches the worker snapshot at dispatch.
- Existing test suites still pass with `NoopTelemetrySink`.

### Acceptance checks

```bash
mvn -q verify
scripts/start-postgres.sh
scripts/start-cluster.sh
java -jar predisched-client/target/predisched-client.jar submit --type MATRIX_TASK --input 200
psql -h localhost -U predisched -c "select count(*) from worker_metrics; select * from execution_history limit 5;"
```

### Docs

`docs/components/telemetry.md` with the schema diagram (Mermaid `erDiagram`).
