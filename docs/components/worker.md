# Worker (lab Exp 2: multithreading in a distributed system)

A worker is a node that runs tasks. It executes them concurrently on a bounded thread pool,
announces itself to the scheduler on startup, and reports live metrics by heartbeat so the scheduler
knows what each worker is doing before it places the next task.

Built by `spec/prompts/02-workers-multithreading.md`. Spec sections: §4 (FR5–FR7, FR13), §10.2,
§11 Exp 2, §17 (Concurrency).

## Contract

`proto/worker.proto` gained the registry side, which the **scheduler** serves:

| RPC | Direction | Purpose |
| --- | --- | --- |
| `RegistryService.Register` | worker → scheduler | id, host, port, cores, memory, pool size |
| `RegistryService.SendHeartbeat` | worker → scheduler | CPU %, memory %, active threads, queue length, tasks completed, mean execution time |
| `WorkerService.ExecuteTask` | scheduler → worker | Unchanged, but now answers asynchronously |

`ExecuteResult` gained `bool rejected` (field 7): the worker's queue was full and nothing ran, so
the task is re-queued rather than failed.

## Execution engine

`ExecutionEngine` wraps a `ThreadPoolExecutor` with a fixed pool and a bounded
`LinkedBlockingQueue`. Threads are named `<workerId>-exec-<n>`, which makes parallelism visible in
the logs.

- **Backpressure is explicit.** When the queue is full the task is rejected, not run on the caller's
  thread and not queued without limit. The scheduler sees `rejected=true`, puts the task back on its
  own queue and tries again. A worker therefore degrades by refusing work rather than by growing an
  invisible backlog.
- **Waiting and running are measured separately.** `wait_time_ms` is time spent in the pool queue,
  `exec_time_ms` is time the executor actually ran. The ML dataset in prompt 15 needs both.
- **The gRPC call never blocks a pool thread.** `WorkerServiceImpl` submits to the engine and
  completes the response from the future.

## Metrics

`WorkerMetrics` holds `AtomicLong` counters (completed, failed, rejected) and a fixed 50-entry
ring buffer of recent execution times behind its own lock. CPU comes from
`com.sun.management.OperatingSystemMXBean.getProcessCpuLoad()` where available, with the system load
average per core as fallback; memory is heap in use over maximum heap.

`RegistrationClient` registers on startup, retrying with exponential backoff (200 ms doubling to
5 s), so a worker can be started before its scheduler. It then heartbeats every
`heartbeatIntervalMs` (default 1000). If the scheduler answers a heartbeat with "unknown worker"
(it restarted), the worker registers again on the next beat.

## Scheduler side

- `WorkerInfo` is an immutable record: registration facts plus the latest heartbeat. An update
  replaces the whole record, so no reader ever sees a half-updated worker (rule 5).
- `WorkerRegistry` is a `ConcurrentHashMap` of those records. `healthy()` filters out workers whose
  last heartbeat is older than `workerStaleAfterMs` (default 5 s), which is what the dispatcher
  dispatches to. The registry takes an injected clock so staleness is tested without sleeping.
- `Dispatcher` now keeps one queue-owning thread but makes the gRPC calls on a small pool
  (`dispatchThreads`, default 4), so one slow task no longer blocks everything behind it. Worker
  choice is still "the first healthy one by id"; strategies arrive in prompt 08. With no healthy
  worker, the task goes back on the queue and the loop pauses for `noWorkerRetryMs`.
- `WorkerClients` caches one gRPC channel per worker address. `Dispatcher` depends on the
  `WorkerStubs` interface, so tests can hand it in-process stubs.
- `ClusterReporter` logs one line per worker every `clusterReportIntervalMs` (default 5 s) with the
  latest heartbeat metrics. Until the dashboard exists, this is how the cluster is observed.

## Configuration

```yaml
scheduler:
  dispatchThreads: 4
  workerStaleAfterMs: 5000
  noWorkerRetryMs: 200
  clusterReportIntervalMs: 5000
worker:
  poolSize: 4
  queueCapacity: 100
  heartbeatIntervalMs: 1000
```

`--pool-size` on the worker command line overrides the file, which is how the benchmark and the demo
vary it.

## How to run and demo

```bash
java -jar predisched-scheduler/target/predisched-scheduler.jar --config configs/local.yaml
```
```bash
java -jar predisched-worker/target/predisched-worker.jar --pool-size 4 --config configs/local.yaml
```
```bash
java -jar predisched-benchmark/target/predisched-benchmark.jar pool-size
```

## Real output

Registration, then heartbeat metrics on the scheduler while six CPU tasks run and after they finish:

```
Registered worker worker-1 at localhost:51061 (8 cores, pool 4, 4056 MB)
Cluster: worker-1 UP cpu=16.9% mem=2.6% active=4/4 queued=0 completed=0 avgExec=0ms
Cluster: worker-1 UP cpu=0.4% mem=1.2% active=0/4 queued=0 completed=6 avgExec=440ms
```

Eight three-second sleep tasks submitted at once to a pool of four: four run together, the rest wait
and run in a second wave.

```
09:41:45.168 [worker-1-exec-1] Executed task-0f96fd94 type=SLEEP_TASK in 3013 ms (waited 14 ms)
09:41:45.168 [worker-1-exec-4] Executed task-817c2cf6 type=SLEEP_TASK in 3013 ms (waited 14 ms)
09:41:45.168 [worker-1-exec-3] Executed task-e9e3b925 type=SLEEP_TASK in 3015 ms (waited 12 ms)
09:41:45.168 [worker-1-exec-2] Executed task-172b9004 type=SLEEP_TASK in 3015 ms (waited 12 ms)
09:41:48.207 [worker-1-exec-4] Executed task-6db0e4a3 type=SLEEP_TASK in 3009 ms (waited 0 ms)
09:41:48.207 [worker-1-exec-2] Executed task-05f9b30e type=SLEEP_TASK in 3012 ms (waited 0 ms)
09:41:48.207 [worker-1-exec-3] Executed task-afb702cf type=SLEEP_TASK in 3009 ms (waited 0 ms)
09:41:48.207 [worker-1-exec-1] Executed task-de8a8510 type=SLEEP_TASK in 3012 ms (waited 0 ms)
```

## Measured: pool size vs throughput

`predisched-benchmark pool-size`, 40 × `CPU_TASK n=20000000` from 4 concurrent clients, one warm-up
batch discarded, median of 3 repetitions, on an 8-core machine. Full data in
`results/exp2-pool-size.csv`.

| Pool size | Makespan (ms) | Throughput (tasks/s) | Mean latency (ms) | P95 latency (ms) | Speedup vs pool 1 |
| --- | --- | --- | --- | --- | --- |
| 1 | 7042 | 5.68 | 3664.2 | 6661 | 1.00x |
| 2 | 6226 | 6.42 | 3345.1 | 5912 | 1.13x |
| 4 | 4867 | 8.22 | 2527.1 | 4746 | 1.45x |
| 8 | 4790 | 8.35 | 2863.0 | 4729 | 1.47x |

Reading this honestly: more threads help, but far less than the core count suggests, and the gain is
almost gone between 4 and 8. `CPU_TASK` is a prime sieve that allocates a 20-million-entry array per
task, so several of them at once compete for memory bandwidth and GC rather than for arithmetic
units. The 1 → 2 step is also small because the first measured batch still overlaps with background
JIT. A compute-bound task with a small working set (for example `MONTE_CARLO_TASK` in prompt 05)
should scale closer to linearly, and prompt 05 is where that comparison becomes possible.

The warm-up pass matters: without it, the first configuration measured looked roughly twice as slow
as it really is, which would have made pool size 1 look artificially bad.

## Tests

`mvn -q verify` runs 42 tests. New in this prompt:

- `ExecutionEngineTest`: at most pool-size tasks in flight; wait time recorded separately from
  execution time; a full queue rejects instead of blocking; the rolling average only covers the
  recent window.
- `WorkerRegistryTest`: registration facts survive heartbeats, unknown workers are refused, silent
  workers stop being healthy and recover on the next beat, and 12 threads × 500 heartbeats leave one
  consistent record per worker.
- `ConcurrentSubmitTest`: 8 client threads × 100 tasks give exactly 800 records, all completed, each
  executed exactly once.
- `ExecutorTest`: the parallel matrix mode produces the same checksum as the sequential one.

## Known limits, resolved later

- One worker per scheduler in the default config; the registry already holds many, and prompt 06
  starts a real cluster.
- Worker choice ignores load (prompt 08).
- A dead worker's running tasks are not yet reassigned; failure detection is prompt 10.
- Heartbeats carry `lamport_time = 0` until prompt 03.
