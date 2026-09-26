The canonical copy of these rules lives in `spec/RULES.md`.

# PrediSched: rules for any coding agent working in this repo

PrediSched is a predictive distributed task scheduler: one product, built component by component. It
covers the ten distributed-systems lab topics (RPC, multithreading, clock sync, election, replication,
load balancing, MapReduce, primary-backup, MPI collectives, MPI matrix multiplication), and each one
is a working part of the product rather than a separate program. The full specification is
`spec/PROJECT-CONTEXT.md`, and `docs/LAB-COVERAGE.md` maps each topic to the code that implements it.
The build is driven by the numbered prompts in `spec/prompts/`, run in order. Read the spec sections a
prompt names before writing code.

## Non-negotiable rules

1. **One product, not demo programs.** Every feature lives in the module that owns it and is used by
   the running system. No `ExpN` packages, no throwaway `*Demo` mains that duplicate real code. A
   demonstration is a script or CLI command that drives the real components.
2. **Contracts first.** Every cross-process call is gRPC, defined in `proto/*.proto`. Change the
   `.proto`, regenerate, then write code. Java and Python both generate from the same `proto/` folder.
   Field numbers are never reused or renumbered once committed.
3. **Module ownership** (spec §9). `predisched-common` holds generated protos, models, clocks and
   config loading. Other Java modules may depend on `common`; `common` depends on none of them. The
   scheduler never imports worker classes and vice versa: they talk over gRPC only.
4. **Scheduling strategies are pluggable.** A new strategy is one class implementing
   `SchedulingStrategy`, selected by config. No `if (strategy == ...)` chains in the scheduler.
   Task types follow the same rule with `TaskExecutor` (spec §7.5).
5. **Thread safety is explicit.** Shared state uses `ConcurrentHashMap`, `Atomic*`, or a documented
   lock. No unsynchronised mutable fields reachable from more than one thread.
6. **Every log line carries the node id and Lamport time** once prompt 03 is done (SLF4J + Logback,
   MDC), plus the trace id when one is in scope.
7. **Improvement is measured, never assumed.** No README line, log message or chart title claims the
   predictive scheduler is better until a benchmark run in `results/` shows it, with the numbers.
8. **Reproducible.** Random behaviour takes a seed from config. Workloads are saved as trace files and
   replayed, not regenerated, when comparing strategies.
9. **Keep it simple.** No framework, abstraction layer or module the current prompt does not need.
   Kafka, Kubernetes, Raft and RL are future work (spec §20). Do not add them.
10. **Stay in scope.** Build what the current prompt asks for. If something from a later prompt is
    needed, add the smallest seam for it and leave a `TODO(prompt NN)` comment.

## Versions (pinned)

| Tool | Version |
| --- | --- |
| Java | 17 (Temurin) |
| Maven | 3.9.x |
| gRPC Java | 1.66.0 |
| Protobuf | 3.25.5 |
| JUnit | 5.10.x |
| Mockito | 5.x |
| Spring Boot | 3.3.x |
| jjwt (JWT, F9) | 0.12.6 (added in prompt 09) |
| Python | 3.10+ |
| grpcio / grpcio-tools | 1.66.x |
| PySpark | 3.5.x |
| mpi4py | 3.1.x (MS-MPI on Windows, OpenMPI on Linux) |
| Node | 20 LTS |
| PostgreSQL | 16 |

Change a version only when a prompt says so, and update this table in the same commit.

## Commands

| What | Command |
| --- | --- |
| Build + unit tests (Java) | `mvn -q verify` |
| Python tests | `python -m pytest` (from `ml/`, `spark/` or `mpi/`) |
| Regenerate Python stubs | `python -m grpc_tools.protoc -I proto --python_out=ml/generated --grpc_python_out=ml/generated proto/*.proto` |
| Start local cluster | `scripts/start-cluster.ps1` (Windows) / `scripts/start-cluster.sh` |
| Whole stack | `docker compose -f docker/docker-compose.yml up --build` |

## Ports (spec §16)

Schedulers 51051–51055 (ids 1–5) · Workers 51061–51063 · Prediction 51070 · Spark UI 4040 ·
Dashboard API 8080 · React dev 5173 · PostgreSQL 5432.

Moved +1000 from spec §16 (2026-09-20, prompt 01): Hyper-V excluded 50000–50159 on the
dev machine, so the whole 500xx group moved to 510xx in `configs/`.

On Windows, check `netsh interface ipv4 show excludedportrange protocol=tcp` before the first run.
If Hyper-V reserves a range that covers one of these ports, move the whole group to a free range in
`configs/` and record the change in this table in the same commit.

## Definition of done for every prompt

- `mvn -q verify` is green (and `pytest` for any Python touched).
- The prompt's acceptance checks were actually run, and their output is pasted in the report.
- `docs/components/<name>.md` is written or updated: what the component does, its design choices,
  how to run and demo it, and real output.
- `docs/LAB-COVERAGE.md` is updated for any lab topic the prompt implements: where the code is, the
  command that demonstrates it, and the measured result.
- The root `README.md` progress table is ticked.
- One commit per prompt, message `<Component>: <what now works>`.
