# Prompt 01 — Task API and execution path

**Spec sections:** §3.3, §4 (FR1–FR4, FR18), §8.1, §8.2 (`WorkerService`), §9 "Exp 1"
**Depends on:** 00
**Lab topics:** Client-server communication using RPC
**Commit message:** `Task API: clients submit, track and cancel tasks; workers execute them over gRPC`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Build the product's core path: a client submits a task
to the scheduler, the scheduler dispatches it to a worker, the worker runs it, the client reads the
result. Every hop is gRPC.

### Contracts

- `proto/task.proto` exactly as §8.1.
- `proto/worker.proto` with `ExecuteRequest`, `ExecuteResult` and `WorkerService` from §8.2
  (registration and heartbeats come in Prompt 02).

### Scheduler (`predisched-scheduler`)

- `SchedulerNode` main: loads `NodeConfig`, starts the gRPC server, shuts down cleanly on SIGTERM.
- `TaskValidator` (FR2), returning every violation: id unique and non-empty; type supported; input
  well-formed for its type (`CPU_TASK` positive integer ≤ 10,000,000; `MATRIX_TASK` size 1–1000;
  `SLEEP_TASK` 0–60,000 ms); input ≤ 1 KB; priority 1–10.
- `TaskRecord` in `common`: id, type, input, priority, status, workerId, result, submittedAt,
  startedAt, completedAt, waitTimeMs, execTimeMs.
- `TaskStore` interface with an in-memory `ConcurrentHashMap` implementation. Transitions follow
  §3.3 exactly; an illegal transition throws `IllegalStateException`. (Prompt 06 adds a replicated
  implementation behind the same interface — design for that.)
- `TaskQueue`: `PriorityBlockingQueue` ordered by priority, then submit time.
- `Dispatcher`: a thread that takes from the queue and sends to a worker. For now the worker list
  comes from config and selection is a placeholder `FirstWorkerStrategy`, behind the
  `SchedulingStrategy` interface from §9 "Exp 6" (`WorkerInfo select(TaskRecord, List<WorkerInfo>)`).
- `SubmitTask` validates, stores QUEUED, enqueues, and returns immediately. `GetTaskStatus` reads the
  store. `CancelTask` succeeds only for QUEUED tasks and removes them from the queue.

### Worker (`predisched-worker`)

- `WorkerNode` main + `WorkerServiceImpl`.
- `TaskExecutor` interface, one implementation per type in a `Map<TaskType, TaskExecutor>`:
  `CpuTaskExecutor` (sum 1..N), `MatrixTaskExecutor` (seeded random N×N multiply, returns a
  checksum), `SleepTaskExecutor`. Prompt 09 adds an MPI-backed matrix executor behind this interface.
- Measures `exec_time_ms` with `System.nanoTime()`. A thrown exception becomes `success=false` with
  the message, never a gRPC error.

### Client (`predisched-client`)

- `predisched` CLI (picocli): `submit --type --input [--priority]`, `status <id>`, `cancel <id>`,
  `watch <id>` (polls until terminal state). UUID ids generated client-side.
- `SchedulerClient` library class that the CLI, the workload generator and the benchmark all reuse.

### Scripts

`scripts/start-cluster.(ps1|sh)` starting scheduler-1 and worker-1 as background processes with logs
in `logs/`, and `scripts/stop-cluster.(ps1|sh)`.

### Tests

- `TaskValidatorTest` for every FR2 rule; `TaskStoreTest` for every legal and illegal transition;
  executor tests (sum 1..100 = 5050; matrix checksum stable for a seed).
- `TaskFlowIT`: scheduler and worker in-process via `InProcessServerBuilder`; submit each type, reach
  COMPLETED; submit invalid tasks, get every violation back; cancel a queued task.

### Acceptance checks

```bash
mvn -q verify
scripts/start-cluster.sh
java -jar predisched-client/target/predisched-client.jar submit --type CPU_TASK --input 100000
java -jar predisched-client/target/predisched-client.jar watch <id>
```

### Docs

`docs/components/task-api.md`. Update the RPC row in `docs/LAB-COVERAGE.md`, including a short note
on why gRPC satisfies "RPC / RMI" and how it compares with Java RMI.
