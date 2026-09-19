# Prompt 01: Task API over gRPC (Exp 1)

## Goal

A client submits, tracks and cancels tasks on a scheduler over gRPC, and the scheduler dispatches
them to a worker that executes `CPU_TASK`, `SLEEP_TASK` and `MATRIX_TASK`. This is lab Exp 1.

## Read first

- Spec §3.3 Task lifecycle, §4 FR1–FR3 and FR18, §7.1 and §7.5, §10.1, §10.2, §11 Exp 1

## Build

1. **Protos**
   - `proto/task.proto`: `TaskType` (all 15 values from spec §10.1, numbers as written),
     `TaskStatus`, `TaskRequest`, `TaskResponse`, `TaskStatusRequest`, `TaskStatusResponse`,
     `SchedulerService` (`SubmitTask`, `GetTaskStatus`, `CancelTask`). Keep the `lamport_time`
     fields now; they are filled in prompt 03.
   - `proto/worker.proto`: `ExecuteRequest`, `ExecuteResult`, `WorkerService.ExecuteTask`.
     Leave registration and heartbeats for prompt 02.
2. **`predisched-common`**
   - `TaskRecord`: immutable view of a task (id, type, input, priority, status, worker id, result,
     timestamps, exec time). State changes return a new record.
   - `TaskStateMachine`: the only place that knows legal transitions (spec §3.3). Illegal moves throw.
   - `TaskStore` interface (`put`, `get`, `update(id, fn)`, `list`) with `InMemoryTaskStore` backed
     by `ConcurrentHashMap`. Prompts 07 and 11 add other implementations.
   - `TaskValidator` implementing FR2: unique non-empty id, supported type, well-formed input within
     a size limit (from config), priority 1–10. Returns a list of errors, not the first one.
   - `NodeConfig`: loads a YAML file from `configs/` (SnakeYAML). Start with `configs/local.yaml`
     holding one scheduler and one worker address.
3. **`predisched-worker`**
   - `TaskExecutor` interface exactly as spec §7.5, plus `ResourceProfile` enum and
     `ExecutionResult` record, in `common` so the ML side can share names.
   - `ExecutorRegistry`: a map from `TaskType` to executor, filled at startup. Unknown type → failed
     result, never an exception across gRPC.
   - Executors: `CpuTaskExecutor` (`n=…`, count primes below n), `SleepTaskExecutor` (`ms=…`),
     `MatrixTaskExecutor` (`size=…`, single-threaded multiply of two seeded random matrices,
     output = checksum of C). Inputs use the `key=value[, key=value]` format of spec §7.2.
   - `WorkerServiceImpl.executeTask` runs the executor synchronously for now (prompt 02 adds pools)
     and reports wall-clock exec time.
   - `WorkerMain`: `--id`, `--port`, `--config`.
4. **`predisched-scheduler`**
   - `SchedulerServiceImpl`: validate → store as `QUEUED` → hand to `Dispatcher` → reply accepted.
     Invalid requests get `accepted=false` with every validation message.
   - `Dispatcher`: a single dispatch thread taking from a `BlockingQueue`, calling the worker's
     `ExecuteTask` over a gRPC channel and moving the record `RUNNING` → `COMPLETED`/`FAILED`.
     One configured worker for now; prompt 08 adds strategies.
   - `CancelTask` succeeds only for `QUEUED` tasks (FR18).
   - `SchedulerMain`: `--id`, `--port`, `--config`.
5. **`predisched-client`**
   - `SchedulerClient`: thin wrapper over the blocking stub.
   - `PredischedCli` with subcommands `submit --type --input --priority [--id]`, `status <id>`,
     `cancel <id>`, `watch <id>` (polls until a terminal state). Use picocli.

## Tests

- `TaskValidatorTest`: each FR2 rule, including several errors at once.
- `TaskStateMachineTest`: every legal and illegal transition from spec §3.3.
- Executor tests: deterministic output for fixed inputs; bad input → failed result.
- `SchedulerIntegrationTest`: in-process gRPC server for scheduler and worker (`InProcessServerBuilder`);
  submit one task of each type, poll to `COMPLETED`, cancel a queued task, reject a duplicate id.

## Acceptance checks

Start a worker and a scheduler in two terminals, then from a third:

```bash
java -jar predisched-client/target/predisched-client.jar submit --type CPU_TASK --input n=2000000 --priority 5
```
```bash
java -jar predisched-client/target/predisched-client.jar submit --type SLEEP_TASK --input ms=abc --priority 11
```
```bash
java -jar predisched-client/target/predisched-client.jar watch <id from the first submit>
```

Paste the acknowledgement, the validation errors, and the final result with exec time.

## Docs and commit

- `docs/components/task-api.md`: the contract, lifecycle, validation rules, how to run, real output.
- `docs/LAB-COVERAGE.md` Exp 1 row: code paths, demo command, observed round-trip.
- A note in the component doc on gRPC vs Java RMI (spec §11 Exp 1).
- Commit: `Task API: clients submit, track and cancel tasks; workers execute them over gRPC`
