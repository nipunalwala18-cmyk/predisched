# Task API and execution path (Prompt 01)

The product's core path: a client submits a task to the scheduler over gRPC, the
scheduler validates it, queues it and dispatches it to a worker, the worker runs it,
the client reads the result. Every hop is gRPC.

## Contracts (`proto/`)

- `proto/task.proto` — exactly spec §8.1: `TaskType` (`CPU_TASK`, `MATRIX_TASK`,
  `SLEEP_TASK`, `MAPREDUCE_TASK`), `TaskStatus` (`QUEUED`, `RUNNING`, `COMPLETED`,
  `CANCELLED`, `FAILED`), `TaskRequest`, `TaskResponse`, `TaskStatusRequest`,
  `TaskStatusResponse`, `SchedulerService` (`SubmitTask`, `GetTaskStatus`,
  `CancelTask`).
- `proto/worker.proto` — `ExecuteRequest`, `ExecuteResult`, `WorkerService`
  (`ExecuteTask`). Registration and heartbeats arrive in Prompt 02.
- `predisched-common` compiles every `proto/*.proto` and exposes the generated
  classes; scheduler, worker and client all use them. New fields later use new field
  numbers only — none are reused or renumbered.

## Scheduler (`predisched-scheduler`)

- `SchedulerNode` main: loads `NodeConfig`, starts the gRPC server, shuts down
  cleanly on SIGTERM. Worker list comes from `settings.workers` in config
  (defaults to `worker-1` on `localhost:50261`).
- `TaskValidator` (FR2) returns **every** violation: id non-empty and unique; type
  supported (`MAPREDUCE_TASK` rejected until its executor exists); input
  well-formed per type (`CPU_TASK` 1–10,000,000; `MATRIX_TASK` 1–1000; `SLEEP_TASK`
  0–60,000 ms); input ≤ 1 KB; priority 1–10.
- `TaskRecord` (in `common/model`): id, type, input, priority, status, workerId,
  result, submitted/started/completed timestamps, wait and exec times.
- `TaskStore` interface + in-memory `ConcurrentHashMap` implementation. Transitions
  follow spec §3.3 exactly; anything else throws `IllegalStateException`. Prompt 06
  adds a replicated implementation behind the same interface.
- `TaskQueue`: `PriorityBlockingQueue` ordered by priority (high first), then
  submit time.
- `Dispatcher`: a daemon thread takes from the queue and sends to a worker. Worker
  selection is a placeholder `FirstWorkerStrategy` behind the `SchedulingStrategy`
  interface (`Optional<WorkerInfo> select(task, alive)` plus `name()` and an
  `onOutcome` hook) — the real strategies arrive in Prompt 03 without changing the
  dispatcher.
- `SubmitTask` validates, stores QUEUED, enqueues, returns immediately.
  `GetTaskStatus` reads the store. `CancelTask` succeeds only for QUEUED tasks and
  removes them from the queue.

## Worker (`predisched-worker`)

- `WorkerNode` main + `WorkerServiceImpl`.
- `TaskExecutor` interface, one implementation per type in a
  `Map<TaskType, TaskExecutor>`: `CpuTaskExecutor` (sum 1..N), `MatrixTaskExecutor`
  (seeded random N×N multiply with fixed seed 42, returns `checksum=<sum of C>`,
  so the MPI backend in Prompt 09 can reproduce the same result),
  `SleepTaskExecutor`. A thrown exception becomes `success=false` with the message,
  never a gRPC error. `exec_time_ms` is measured with `System.nanoTime()`.

## Client (`predisched-client`)

- `predisched` CLI (picocli): `submit --type --input [--priority]`,
  `status <id>`, `cancel <id>`, `watch <id>` (polls to a terminal state). Task ids
  are UUIDs generated client-side.
- `SchedulerClient` library class reused later by the workload generator and
  the benchmark.

## How to run and demo it

```powershell
powershell -ExecutionPolicy Bypass -File scripts/start-cluster.ps1
java -jar predisched-client/target/predisched-client.jar submit --type CPU_TASK --input 100000
java -jar predisched-client/target/predisched-client.jar watch <id>
powershell -ExecutionPolicy Bypass -File scripts/stop-cluster.ps1
```

Real output from the acceptance run (2026-09-16, scheduler-1 + worker-1 local):

```
0fd67f26-f72f-405c-b3b1-56c520ecf2d8 accepted=true QUEUED
0fd67f26-f72f-405c-b3b1-56c520ecf2d8 COMPLETED worker=worker-1 execMs=1 result=5000050000
38b16966-034c-434a-b9b8-eaf73781f81b COMPLETED worker=worker-1 execMs=30 result=checksum=252680.97756552164
8e0c5076-954b-4b4d-9e09-e9ea208b9301 COMPLETED worker=worker-1 execMs=507 result=slept 500 ms
14169125-db79-4b76-91b9-12bafc8985f9 accepted=false CPU_TASK input must be an integer 1..10000000
8477028a-e50c-4969-b9ba-5dc455206a2f accepted=false priority must be 1-10, got 99
27027ebf-8989-4af0-9b34-752836091c45 accepted=true CANCELLED
6804d1f3-b27f-4b54-b3bd-0ce5a05ac579 accepted=false only QUEUED tasks can be cancelled (status=RUNNING)
```

## Why gRPC satisfies "RPC / RMI"

The lab topic asks for client-server communication using RPC or Java RMI. gRPC is
an RPC framework: the client invokes what looks like a local method
(`SchedulerClient.submit`), the call is marshalled into a Protocol Buffers message,
sent over HTTP/2, and dispatched to the server implementation. That is the same
programming model as Java RMI (remote interfaces, stubs, marshalling), with three
differences that matter for this product: contracts are declared in `.proto` files
instead of Java interfaces, so the Python prediction service (Prompt 13) and the
MPI/Spark components can implement the same contracts; the transport is HTTP/2
with multiplexing and deadlines instead of JRMP; and stubs are generated at build
time by `protobuf-maven-plugin` rather than `rmic`. No RMI registry is needed —
endpoints come from `NodeConfig` and, later, leader discovery.

## Design choices

- **Shaded jars must merge `META-INF/services`.** The client/scheduler/worker jars
  are built with `maven-shade-plugin`, which by default lets one jar's
  `META-INF/services/io.grpc.NameResolverProvider` overwrite the others. That left
  only the UDS (`unix`) resolver registered, so every `ManagedChannelBuilder`
  failed with `Address types of NameResolver 'unix' ... not supported by transport`.
  All shade configs include `ServicesResourceTransformer`. Any module that adds a
  shaded jar later (e.g. benchmark) must include it too.
- **Duplicate-submit race:** validation checks id uniqueness, but two concurrent
  submits with the same id are still safe — `TaskStore.create` is atomic and the
  loser gets `accepted=false`.
- **Dispatch failures requeue:** if the worker call throws, the task transitions
  RUNNING → QUEUED and goes back on the queue instead of being lost.
- Wall-clock reads use `System.currentTimeMillis()` for now; Prompt 04 routes all
  of them through `NodeClock`.
