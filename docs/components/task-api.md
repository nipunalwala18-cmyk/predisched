# Task API (lab Exp 1: client-server communication over RPC)

The task API is the front door of PrediSched. A client submits a task to a scheduler over gRPC, the
scheduler validates and queues it, a dispatcher hands it to a worker over gRPC, and the worker runs it
on a registered executor. Clients poll status and can cancel work that has not started.

Built by `spec/prompts/01-task-api-rpc.md`. Spec sections: §3.3, §4 (FR1–FR3, FR18), §7.1, §7.5,
§10.1, §10.2, §11 Exp 1.

## Contract

`proto/task.proto` (client ↔ scheduler):

| RPC | Request → Response | Behaviour |
| --- | --- | --- |
| `SubmitTask` | `TaskRequest` → `TaskResponse` | Validates, stores as `QUEUED`, enqueues for dispatch |
| `GetTaskStatus` | `TaskStatusRequest` → `TaskStatusResponse` | Status, worker, result, execution time |
| `CancelTask` | `TaskStatusRequest` → `TaskResponse` | Succeeds only while the task is `QUEUED` |

`proto/worker.proto` (scheduler ↔ worker): `WorkerService.ExecuteTask(ExecuteRequest) →
ExecuteResult`. Registration and heartbeats arrive in prompt 02.

`lamport_time` fields are carried in the messages but always 0 until prompt 03 fills them.

## Lifecycle

```
QUEUED ──dispatch──> RUNNING ──success──> COMPLETED
  │                     └─────error────> FAILED
  └──CancelTask──> CANCELLED
```

`TaskStateMachine` is the only place that knows these transitions; every other class asks it, so an
illegal move throws instead of silently corrupting a record.

## Design choices

- **The scheduler never imports worker classes.** The dispatcher holds a gRPC stub, nothing more.
  `TaskExecutor`, `ResourceProfile` and `ExecutionResult` live in `predisched-common` so both sides
  share the vocabulary without sharing code.
- **Validation returns every error, not the first.** `TaskValidator` (FR2) checks id, type, input and
  priority and returns a list, so one round trip tells the client everything that is wrong.
- **One place defines what input is well-formed.** `TaskInputSpec` holds the required keys and ranges
  per task type. The scheduler uses it to reject bad input at submit time and the worker's executors
  use it before doing any work, so the two can never disagree. A type with no executor yet is
  rejected with the list of supported types.
- **Executors are a registry, not a switch.** `ExecutorRegistry` maps `TaskType` to a `TaskExecutor`;
  adding a type is one class plus one registration (rule 4). An unknown type returns a failed result
  rather than throwing across gRPC.
- **The store is an interface.** `TaskStore` with `InMemoryTaskStore` today; replication (prompt 07)
  and PostgreSQL (prompt 11) plug in behind the same interface.
- **Records are immutable.** `TaskRecord` state changes return a new record, kept in a
  `ConcurrentHashMap` (rule 5).

## Task types in this prompt

| Type | Input | Work | Output |
| --- | --- | --- | --- |
| `CPU_TASK` | `n=2..100000000` | Counts primes below n (sieve) | `primes_below_<n>=<count>` |
| `SLEEP_TASK` | `ms=0..60000` | Sleeps, interruptibly | `slept_ms=<ms>` |
| `MATRIX_TASK` | `size=1..1000`, optional `threads=1..64` | Multiplies two seeded matrices | `size=<n> threads=<k> checksum=<sum>` |

The rest of the catalogue (spec §7.2) arrives in prompt 05.

## gRPC as the "RPC or RMI" requirement

The lab topic allows RPC or Java RMI. PrediSched uses gRPC, which is an RPC framework: the `.proto`
file is the interface definition, `protoc` generates client stubs and server skeletons, and a call on
a stub looks like a local method call while travelling over the network. Compared with Java RMI it
keeps the contract language-neutral (the Python prediction server in prompt 17 generates from the
same `proto/` folder), uses HTTP/2 with binary Protocol Buffers instead of Java serialisation, and
needs no registry process or `Serializable` types. RMI would have tied every node to the JVM.

## How to run

Two terminals for the nodes, one for the client:

```bash
java -jar predisched-worker/target/predisched-worker.jar --id worker-1 --port 51061 --config configs/local.yaml
```
```bash
java -jar predisched-scheduler/target/predisched-scheduler.jar --id scheduler-1 --port 51051 --config configs/local.yaml
```
```bash
java -jar predisched-client/target/predisched-client.jar submit --type CPU_TASK --input n=2000000 --priority 5
```

CLI subcommands: `submit`, `status <id>`, `cancel <id>`, `watch <id>`.

Ports are 51051 / 51061, moved +1000 from spec §16 because Hyper-V reserves 50000–50159 on the dev
machine (recorded in `CLAUDE.md`).

## Real output

Accepted submit, then `watch`:

```
accepted=true task_id=task-9b5421c7 message='queued'
task_id=task-9b5421c7 status=COMPLETED worker=worker-1 exec_ms=19 result='primes_below_2000000=148933'
```

Matrix task:

```
accepted=true task_id=task-53ebf0e7 message='queued'
task_id=task-53ebf0e7 status=COMPLETED worker=worker-1 exec_ms=45 result='size=200 threads=1 checksum=1997209.445178'
```

(The output gained its `threads=` field in prompt 02, which added the parallel matrix mode.)

Both validation errors reported from one submit (`--input ms=abc --priority 11`):

```
accepted=false task_id=task-e40112e6 message='ms must be a number (got 'abc'); priority must be between 1 and 10 (got 11)'
```

A type with no executor yet:

```
accepted=false task_id=task-cd6dc9df message='type GRAPH_TASK has no executor yet; supported types are [CPU_TASK, SLEEP_TASK, MATRIX_TASK]'
```

Cancellation, queued then already finished:

```
accepted=true task_id=task-33bfb1d8 message='cancelled'
task_id=task-33bfb1d8 status=CANCELLED worker= exec_ms=0 result=''
accepted=false task_id=task-843786fc message='only QUEUED tasks can be cancelled (status=COMPLETED)'
```

## Tests

At the end of prompt 01, `mvn -q verify` ran 30 tests: validator rules (10), state machine transitions (4), input spec (7),
executors (5), the generated-proto smoke test (1), and an in-process gRPC integration test (3) that
submits one task of each type, cancels a queued task and rejects a duplicate id.

## Known limits, resolved later

- Workers are chosen without a strategy: the first healthy one (prompt 08).
- gRPC's default logging is noisy on the console; Logback configuration lands in prompt 03.
