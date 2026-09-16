# Prompt 02 — Concurrent workers and cluster membership

**Spec sections:** §4 (FR5–FR7, FR13), §8.2, §9 "Exp 2"
**Depends on:** 01
**Lab topics:** Multithreading in a distributed system
**Commit message:** `Workers: thread pools, self-registration, heartbeats and live metrics`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Make workers concurrent and self-describing, and make
the scheduler track a changing set of them.

### Contracts

Add `RegisterRequest`, `Heartbeat` and `RegistryService` to `proto/worker.proto` as in §8.2.

### Worker

- `ThreadPoolExecutor` sized from config (`poolSize`) with a bounded `LinkedBlockingQueue`
  (`queueCapacity`); threads named `worker-<id>-exec-<n>`. Saturation returns `success=false`,
  "worker saturated", and the scheduler requeues the task.
- `ExecuteTask` completes its gRPC call asynchronously when the pool finishes, so gRPC threads never
  block on task work. Record `wait_time_ms` (accepted → started) and `exec_time_ms`.
- `WorkerMetrics`: completed count (`AtomicLong`), active threads, local queue length, rolling mean
  exec time over the last 50 tasks, CPU % and memory % from `com.sun.management.OperatingSystemMXBean`.
- `RegistrationClient`: registers on start with retry and backoff until a scheduler answers, then sends
  a heartbeat every `heartbeatMs` (default 1000).
- Optional `cpuLimitFactor` in config that slows executors proportionally, to simulate heterogeneous
  hardware without Docker.

### Scheduler

- `WorkerRegistry` (`ConcurrentHashMap<String, WorkerInfo>`): register, heartbeat update, `alive()`
  list. `WorkerInfo` in `common` holds static capacity plus the latest heartbeat snapshot and
  `lastSeen`. A heartbeat from an unregistered worker gets `ok=false` so it re-registers.
- The dispatcher now picks from `registry.alive()`; replace `FirstWorkerStrategy` with a temporary
  rotation until Prompt 03.
- Arrival-rate tracker: tasks/second over a sliding 10 s window (needed later as a feature).
- Scheduler handles many simultaneous clients: `SubmitTask` does no blocking work.

### Measuring it (reusable, not a demo copy)

Add `pool-scaling` to the benchmark module: for pool sizes 1, 2, 4, 8 it starts one in-process worker,
replays the same batch (16 × `SLEEP_TASK 500`, 16 × `CPU_TASK 5000000`) through the real scheduler,
repeats 3 times, and writes `results/pool-scaling.csv` with makespan, throughput, mean latency and
speedup (mean ± std dev).

### Tests

- `WorkerMetricsTest`: correct counts after 8 threads × 1,000 updates.
- `WorkerRegistryTest`: register, heartbeat, unknown-worker heartbeat rejected.
- `ConcurrentSubmitIT`: 10 client threads × 100 submits against 3 in-process workers → exactly 1,000
  COMPLETED records, no duplicate execution (each worker logs task ids; union has no repeats).

### Acceptance checks

```bash
mvn -q verify
scripts/start-cluster.sh          # now 1 scheduler + 3 workers
java -jar predisched-benchmark/target/predisched-benchmark.jar pool-scaling
```

Logs show interleaved worker thread names. Sleep tasks scale near-linearly; CPU tasks flatten near the
core count — explain why in the docs.

### Docs

`docs/components/workers.md`; multithreading row in `docs/LAB-COVERAGE.md` with the real speedup table.
