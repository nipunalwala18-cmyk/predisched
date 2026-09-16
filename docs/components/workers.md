# Concurrent workers and membership (Prompt 02)

Workers run tasks concurrently on a bounded pool and describe themselves to the
scheduler; the scheduler tracks the changing worker set through registration and
heartbeats instead of static config.

## Contracts

`proto/worker.proto` gains `RegisterRequest`, `Heartbeat` and `RegistryService`
exactly as spec §8.2 (`Ack` is reused from `proto/common.proto`, same package).

## Worker (`predisched-worker`)

- `ThreadPoolExecutor` sized from config (`poolSize`, default 4) with a bounded
  `LinkedBlockingQueue` (`queueCapacity`, default 100); threads named
  `worker-<id>-exec-<n>`. A full pool rejects with `success=false`,
  `"worker saturated"`, and the scheduler requeues the task.
- `ExecuteTask` hands work to the pool and completes the gRPC call from the pool
  thread when it finishes, so gRPC threads never block. It records `wait_time_ms`
  (accepted → started) and `exec_time_ms`. A `Throwable` anywhere still completes
  the call — a task can never hang its RPC.
- `WorkerMetrics`: completed count (`AtomicLong`), active threads
  (`AtomicInteger`), local queue length (from the pool), rolling mean exec time
  over the last 50 tasks (monitor-guarded deque), CPU % and memory % from
  `com.sun.management.OperatingSystemMXBean` (-1 when unavailable).
- `RegistrationClient`: registers on start with retry and doubling backoff
  (500 ms → 5 s) until a scheduler answers, then heartbeats every `heartbeatMs`
  (default 1000). A heartbeat answered `ok=false` triggers re-registration.
- `cpuLimitFactor` (default 1.0) stretches genuine execution time by
  `1/factor − 1`, simulating heterogeneous hardware without Docker.
- `WorkerNode` reads `poolSize`, `queueCapacity`, `heartbeatMs`,
  `cpuLimitFactor`, `cores`, `memoryMb` from `settings`; the scheduler address
  comes from `settings.scheduler` (`host:port`), else the first peer.

## Scheduler (`predisched-scheduler`)

- `WorkerRegistry` (`ConcurrentHashMap<String, WorkerInfo>`): register, heartbeat
  snapshot update, `alive()` (last-seen within `workerTimeoutMs`, default 5 s,
  sorted by id). Heartbeats from unknown workers get `ok=false` so they
  re-register. `WorkerInfo` (in `common`) already held the snapshot fields; they
  are now filled from heartbeats.
- `RegistryServiceImpl` serves `RegistryService`.
- The dispatcher picks from `registry.alive()`. `FirstWorkerStrategy` is replaced
  by a temporary `RotatingStrategy` (cursor over the id-sorted alive list) until
  Prompt 03.
- The queue thread only marks tasks RUNNING; the blocking worker call runs on a
  sender pool (`dispatcher-send-*` threads) so many tasks are in flight at once —
  without this the worker pools could never fill. A saturated worker or a failed
  call requeues the task (RUNNING → QUEUED) with a 50 ms breather against hot
  spinning; the saturation marker is matched as a string literal because the
  scheduler never imports worker classes.
- `ArrivalRate`: tasks/second over a sliding 10 s window, marked on every
  accepted submit (an ML feature later). `SubmitTask` still does no blocking work.

## How to run and demo it

```powershell
powershell -ExecutionPolicy Bypass -File scripts/start-cluster.ps1   # 1 scheduler + 3 workers
java -jar predisched-client/target/predisched-client.jar submit --type CPU_TASK --input 100000
java -jar predisched-benchmark/target/predisched-benchmark.jar pool-scaling
powershell -ExecutionPolicy Bypass -File scripts/stop-cluster.ps1
```

Workers announce themselves in the logs:

```
RegistrationClient - registered with scheduler localhost:50051   # × 3, one per worker
WorkerServiceImpl - task bench-… done in 514 ms on worker-bench-w-exec-3
```

Six sequential submits rotate across the cluster:

```
… COMPLETED worker=worker-3 …   … COMPLETED worker=worker-1 …
… COMPLETED worker=worker-2 …   … COMPLETED worker=worker-3 …
… COMPLETED worker=worker-1 …   … COMPLETED worker=worker-2 …
```

## Measured: `pool-scaling` (2026-09-16)

Same batch (16 × `SLEEP_TASK 500`, 16 × `CPU_TASK 5000000`) through the real
scheduler, 3 reps per pool size, in `results/pool-scaling.csv`:

| pool | makespan ms (mean±sd) | throughput tps | mean latency ms | sleep lat ms | CPU lat ms | speedup |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 8512±329 | 3.8±0.1 | 4680±475 | 4561 | 4799 | 1.00 |
| 2 | 4139±11 | 7.7±0.0 | 2153±41 | 2334 | 1972 | 2.06 |
| 4 | 2089±5 | 15.3±0.0 | 1116±30 | 1315 | 917 | 4.08 |
| 8 | 1075±1 | 29.8±0.0 | 580±15 | 799 | 362 | 7.92 |

Why this shape: SLEEP tasks only wait, so doubling the pool halves their queueing
and they scale near-linearly (4561 → 799 ms). CPU tasks genuinely burn CPU — the
executor always runs the real 1..N loop — so their *execution* time is core-bound
and would flatten near the core count if the pool exceeded it; here their *latency*
still falls (4799 → 362 ms) because at small pools they mostly wait behind sleeps,
and that queue wait shrinks with the pool. Makespan is dominated by the 16 × 500 ms
sleeps: 8 s serial, ~1 s with 8 threads.

## Design choices

- Registration runs on a background thread: a worker starts serving even if the
  scheduler is down and joins when it appears.
- `alive()` returns a sorted snapshot copy, so dispatch never holds registry locks
  and churn cannot corrupt iteration.
- The benchmark jar is shaded like the others and also needs
  `ServicesResourceTransformer` (see `docs/components/task-api.md`).
