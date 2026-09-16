# Build prompts

PrediSched is built by giving these prompts to a coding agent (Claude Code or similar), **one at a time,
in order**. Each prompt adds one working part of the product on top of everything before it. The ten
distributed-systems lab topics are implemented where the product needs them, not as separate
programs. `docs/LAB-COVERAGE.md` shows where each one lives.

## How to run a prompt

1. Start from a clean `main` that is green (`mvn -q verify`).
2. Open a fresh agent session in the repo root, so `CLAUDE.md` loads.
3. Paste: `Implement prompts/<nn>-<name>.md. Follow CLAUDE.md.`
4. Review the agent's report: acceptance-check output must be real, not described.
5. Commit with the prompt's commit message, tick the row in the root `README.md`, and push.

If a prompt turns out wrong once you meet real code (an interface does not fit, a number is
unrealistic), fix the prompt file in the same commit, so the prompts stay a true record of the build.

## Order

| # | Prompt | Adds to the product | Lab topics |
| --- | --- | --- | --- |
| 00 | [Foundation](00-foundation.md) | Maven modules, shared `proto/`, config, logging, CI | — |
| 01 | [Task API and execution](01-task-api-and-execution.md) | Submit, track and cancel tasks; workers execute them | RPC |
| 02 | [Concurrent workers and membership](02-concurrent-workers-and-membership.md) | Thread pools, registration, heartbeats, metrics | Multithreading |
| 03 | [Scheduling strategies](03-scheduling-strategies.md) | Round Robin, Random, Least Loaded, Resource-Aware | Load balancing |
| 04 | [Time and event ordering](04-time-and-event-ordering.md) | Lamport clocks on every call, Berkeley/Cristian sync, event log | Clock synchronization |
| 05 | [Scheduler leadership](05-scheduler-leadership.md) | Scheduler cluster with Bully or Ring election | Election |
| 06 | [Replicated task state](06-replicated-task-state.md) | Quorum or eventual consistency for task state | Consistency and replication |
| 07 | [Fault tolerance and failover](07-fault-tolerance-and-failover.md) | Primary-backup promotion, worker failure recovery, zero loss | Primary-backup fault tolerance |
| 08 | [Persistence and telemetry](08-persistence-and-telemetry.md) | PostgreSQL history of everything observed | — |
| 09 | [Parallel compute (MPI)](09-parallel-compute-mpi.md) | MPI batch engine and MPI matrix backend for workers | MPI collectives; MPI matrix multiplication |
| 10 | [Workload generator](10-workload-generator.md) | Seeded traces and accurate replay | — |
| 11 | [Log analytics (Spark)](11-log-analytics-spark.md) | MapReduce statistics and the ML training dataset | MapReduce |
| 12 | [ML models](12-ml-models.md) | Execution-time, queue-forecast and overload models | — |
| 13 | [Prediction service](13-prediction-service.md) | Python gRPC model server + Java client with circuit breaker | RPC (cross-language) |
| 14 | [Predictive scheduler](14-predictive-scheduler.md) | Cost-based placement, fallback, proactive actions | Load balancing |
| 15 | [Benchmark harness](15-benchmark-harness.md) | Reproducible strategy comparison with statistics | Load balancing (evaluation) |
| 16 | [Dashboard API](16-dashboard-api.md) | Spring Boot REST + WebSocket | — |
| 17 | [Dashboard UI](17-dashboard-ui.md) | React live operator dashboard | — |
| 18 | [Deployment](18-deployment-docker.md) | One-command Docker stack, fault drills | — |
| 19 | [Hardening and release](19-hardening-and-release.md) | Requirements trace, soak and chaos runs, report, demo, v1.0.0 | All, verified |

## Why this order

- The request path (00–02) comes first because everything else observes or changes it.
- Strategies (03) come early because they are simple and every later measurement needs a baseline.
- Time (04) comes before leadership and replication (05–07) because both use Lamport timestamps.
- Telemetry (08) comes after the cluster is stable, so the recorded data describes the final system.
- MPI (09) comes before data collection (10–11) so the matrix backend is part of the dataset.
- ML (12–14) needs the dataset; benchmarks (15) need the predictive strategy; the dashboard and
  deployment (16–18) show a system that is already complete.

## Timeline mapping (spec §16)

| Weeks | Prompts |
| --- | --- |
| 1–3 | 00–04 |
| 4–5 | 05–06 |
| 6–8 | 07–08, 11 |
| 9–10 | 09–10 |
| 11 | collect dataset (11 step 0) |
| 12–13 | 12–13 |
| 14 | 14 |
| 15 | 15 |
| 16 | 16–19 |

Weeks 6–11 overlap deliberately: Spark (11) can be built against a small dataset early and rerun on
the full dataset in week 11.
