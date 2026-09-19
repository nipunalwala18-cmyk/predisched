# Prompt 02: Workers, thread pools, registration and heartbeats (Exp 2)

## Goal

Workers run many tasks at once on a bounded thread pool, register themselves with the scheduler, and
send heartbeats with live metrics. Several clients can submit concurrently without lost or duplicated
tasks. A measured speedup table for pool sizes 1, 2, 4, 8 exists. This is lab Exp 2.

## Read first

- Spec §4 FR5–FR7, FR13, §10.2, §11 Exp 2, §17 (Concurrency row)

## Build

1. **Protos**: add `RegisterRequest`, `Heartbeat`, and `RegistryService` (`Register`,
   `SendHeartbeat`) to `proto/worker.proto`, as in spec §10.2. `RegistryService` is served by the
   scheduler.
2. **`predisched-worker`**
   - `ExecutionEngine`: a `ThreadPoolExecutor` with configurable core/max size and a bounded
     `LinkedBlockingQueue`. Thread names `worker-<id>-exec-<n>`. When the queue is full the worker
     rejects the task with a clear error (the scheduler retries elsewhere later).
   - `WorkerServiceImpl.executeTask` becomes asynchronous: submit to the engine, complete the gRPC
     response from the future. Measure `wait_time_ms` (queued in the pool) separately from
     `exec_time_ms`.
   - `WorkerMetrics`: `AtomicLong`/`AtomicInteger` counters for active threads, pool queue length,
     tasks completed, rolling average exec time (last 50 tasks), CPU % and memory % from
     `OperatingSystemMXBean` / `Runtime`.
   - `RegistrationClient`: registers on start (retrying with backoff until the scheduler answers),
     then sends a heartbeat every `heartbeat.interval.ms` (config, default 1000).
   - `MatrixTaskExecutor` gains an optional `threads=k` parameter that splits row blocks across a
     local `ForkJoinPool`, so the thread-pool demo has a CPU-heavy parallel task.
3. **`predisched-scheduler`**
   - `WorkerRegistry`: `ConcurrentHashMap<String, WorkerInfo>` updated by `Register` and
     `SendHeartbeat`. `WorkerInfo` is immutable; updates replace it.
   - `RegistryServiceImpl` serving the two RPCs.
   - The dispatcher now picks from registered workers (first healthy one; prompt 08 adds strategies)
     and dispatches on a small thread pool so a slow task does not block the queue.
4. **`predisched-benchmark`**
   - `PoolSizeBenchmark` command: starts an in-process scheduler and one worker per pool size
     1, 2, 4, 8, submits the same fixed batch (e.g. 40 × `CPU_TASK n=3000000`) from 4 concurrent
     client threads, and records throughput, mean and p95 latency.
   - Writes `results/exp2-pool-size.csv` and prints a speedup table relative to pool size 1.

## Tests

- `ExecutionEngineTest`: N tasks on a pool of k run with at most k in flight; rejection when full.
- `WorkerRegistryTest`: concurrent heartbeats from many threads leave one consistent entry per worker.
- `ConcurrentSubmitTest`: 8 client threads × 100 tasks each → exactly 800 distinct records, all
  terminal, none duplicated.

## Acceptance checks

```bash
java -jar predisched-benchmark/target/predisched-benchmark.jar pool-size
```

Also run a worker with pool size 4 and submit 8 sleep tasks at once; paste the worker log lines
showing four different `worker-…-exec-n` thread names running in parallel.

## Docs and commit

- `docs/components/worker.md`: engine, backpressure, metrics, registration and heartbeat flow.
- `docs/LAB-COVERAGE.md` Exp 2 row, with the speedup table from `results/exp2-pool-size.csv`.
- Commit: `Workers: thread pools, self-registration, heartbeats and live metrics`
