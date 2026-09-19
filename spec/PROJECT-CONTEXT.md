# PrediSched: Predictive Distributed Task Scheduler

**Complete Project Context Document**

---

## 1. Project Overview

PrediSched is a distributed task scheduling system that **predicts** the near-future state of a cluster and uses those predictions to allocate tasks, instead of only reacting to the current state.

A client submits computational tasks. A scheduler (the elected coordinator of a scheduler cluster) queues them and places each task on one of several worker nodes. Workers run tasks concurrently and continuously report metrics. A prediction engine uses current and historical metrics to forecast:

- how long a task will take on each worker,
- how the queue will grow,
- whether a worker is about to become overloaded.

The predictive scheduler then picks the worker expected to finish the task soonest with the least risk.

### 1.1 Core idea

| Reactive scheduling (conventional) | Predictive scheduling (PrediSched) |
| --- | --- |
| Task arrives → check current load → choose worker | Task arrives → current metrics + history → ML forecast → choose worker |
| Decisions based on "now" | Decisions based on "now + next few seconds" |
| Handles spikes after they happen | Anticipates spikes and spreads load early |
| Treats tasks as equal-cost | Estimates each task's cost per worker |

### 1.2 Research question

> Can predictions of workload and task execution time improve distributed task scheduling (latency, throughput, balance, SLA compliance) compared with reactive policies such as Round Robin, Least Loaded, and Resource-Aware scheduling, and at what overhead?

The answer must come from controlled benchmarks. Improvement is measured, never assumed.

### 1.3 Project philosophy

The ten lab experiments are **building blocks of one system**, not ten separate programs. Every experiment adds a reusable component to the same codebase.

```
RPC → Multithreading → Clock Sync → Leader Election → Replication & Consistency
    → Load Balancing → Spark MapReduce → Primary-Backup Fault Tolerance
    → MPI Collectives → MPI Matrix Multiplication
    → ML Pipeline → Predictive Scheduler → Benchmarking → Dashboard
```

### 1.4 Lab experiment coverage

Every lab experiment maps to a real PrediSched component.

| Exp | Lab topic | PrediSched component | Technology |
| --- | --- | --- | --- |
| 1 | Client-server communication using RPC / RMI | Client ↔ Scheduler and Scheduler ↔ Worker calls | gRPC + Protocol Buffers |
| 2 | Multithreading in a distributed system | Worker thread pools executing tasks concurrently | Java `ExecutorService` |
| 3 | Clock synchronization (logical / physical) | Lamport clocks for event ordering + Berkeley sync of physical clocks (Cristian's as alternative) | Java + gRPC |
| 4 | Bully and Ring election | Electing the primary scheduler | Java + gRPC |
| 5 | Data consistency and replication models | Task state replicated across scheduler nodes; strong (quorum) vs eventual consistency | Java + gRPC |
| 6 | Load balancing | Round Robin, Random, Least Loaded, Resource-Aware (baselines for the predictive scheduler) | Java |
| 7 | MapReduce with Hadoop / Spark | MapReduce analytics over execution logs; produces ML features | Apache Spark (PySpark) |
| 8 | Fault tolerance with primary-backup replication | Primary scheduler + backups; automatic failover with no task loss | Java + gRPC |
| 9 | MPI collectives: Broadcast, Scatter, Gather | Root rank broadcasts config, scatters tasks, gathers results | mpi4py |
| 10 | Parallel matrix multiplication using MPI | `MATRIX_TASK` executor and benchmark workload | mpi4py + NumPy |

---

## 2. Research Gaps Addressed

| # | Gap | PrediSched solution |
| --- | --- | --- |
| 1 | No predictive workload awareness | Queue-length / arrival-rate forecasting model |
| 2 | No execution-time prediction | Per-task, per-worker execution-time regression model |
| 3 | Reactive worker selection | Worker scoring based on predicted future state |
| 4 | Poor handling of workload spikes | Queue-growth prediction triggers early redistribution |
| 5 | Worker heterogeneity ignored | Worker-specific features (cores, speed, history) |
| 6 | Scheduler is a single point of failure | Bully/Ring coordinator election + failover |
| 7 | Lack of rigorous comparison | Benchmark vs Round Robin, Random, Least Loaded, Resource-Aware |
| 8 | Scheduler state lost on failover | Replicated task state + primary-backup failover |

**Primary gap:** reactive vs predictive scheduling (Gaps 1–4).

---

## 3. System Architecture

### 3.1 High-level architecture

```mermaid
flowchart TD
    C[Client] -->|gRPC SubmitTask| L
    subgraph SC[Scheduler Cluster]
        L[Primary Scheduler - Elected Leader]
        S2[Backup Scheduler]
        S3[Backup Scheduler]
        L -->|replicate state| S2
        L -->|replicate state| S3
    end
    L --> Q[Task Queue]
    Q --> PS[Predictive Scheduler]
    PS -->|gRPC ExecuteTask| W1[Worker 1]
    PS -->|gRPC ExecuteTask| W2[Worker 2]
    PS -->|gRPC ExecuteTask| W3[Worker 3]
    W1 -->|Heartbeat + Metrics| MC[Metrics Collector]
    W2 -->|Heartbeat + Metrics| MC
    W3 -->|Heartbeat + Metrics| MC
    MC --> DB[(PostgreSQL)]
    MC --> PE[Prediction Engine - Python]
    PE -->|Predictions| PS
    DB --> API[Spring Boot API]
    API --> UI[React Dashboard]
```

### 3.2 Components

| Component | Language | Responsibility |
| --- | --- | --- |
| **Client** | Java | Submits tasks, queries status, runs workload generators |
| **Scheduler Node** | Java | Accepts tasks, maintains queue, participates in election, dispatches tasks; acts as primary or backup |
| **Election Module** | Java | Bully (primary) and Ring (comparison) coordinator election |
| **Clock Module** | Java | Lamport timestamps for event ordering + Berkeley / Cristian physical clock sync |
| **Replication Module** | Java | Replicates task state across scheduler nodes; strong (quorum) and eventual consistency modes |
| **Worker Node** | Java | Executes tasks with a thread pool; reports metrics and heartbeats |
| **Metrics Collector** | Java | Aggregates worker metrics, writes history to storage |
| **Scheduling Strategies** | Java | Pluggable: Round Robin, Random, Least Loaded, Resource-Aware, Predictive |
| **Prediction Engine** | Python | Trains and serves ML models over gRPC |
| **Fault Tolerance Module** | Java | Primary-backup replication, heartbeat failure detection, failover, task reassignment |
| **Spark Analytics Job** | PySpark | MapReduce over execution logs; aggregates features for ML |
| **MPI Compute Module** | Python (mpi4py) | Broadcast / Scatter / Gather collectives and parallel matrix multiplication |
| **Benchmark Harness** | Java + Python | Replays identical workloads across strategies, analyzes results |
| **Dashboard API** | Spring Boot | REST + WebSocket endpoints for UI |
| **Dashboard** | React + TypeScript | Live view of workers, tasks, predictions, metrics |

### 3.3 Task lifecycle

```mermaid
stateDiagram-v2
    [*] --> QUEUED: SubmitTask accepted
    QUEUED --> RUNNING: Dispatched to worker
    QUEUED --> CANCELLED: CancelTask
    RUNNING --> COMPLETED: Success
    RUNNING --> FAILED: Error / retries exhausted
    RUNNING --> QUEUED: Worker died (reassign)
    COMPLETED --> [*]
    FAILED --> [*]
    CANCELLED --> [*]
```

### 3.4 Predictive dispatch flow

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Scheduler (Primary)
    participant B as Backup Schedulers
    participant P as Prediction Engine
    participant W as Chosen Worker
    C->>S: SubmitTask(id, type, input, priority)
    S->>S: Validate, Lamport tick, enqueue
    S->>B: Replicate(ENQUEUE)
    B-->>S: Ack
    S->>P: PredictBatch(task features, all worker states)
    P-->>S: exec_time[w], overload_prob[w], queue_forecast[w]
    S->>S: Score workers, pick lowest cost
    S->>W: ExecuteTask(task)
    W-->>S: TaskResult(output, exec_time, metrics)
    S->>S: Record actual vs predicted
    C->>S: GetTaskStatus(id)
    S-->>C: COMPLETED + result
```

---

## 4. Functional Requirements

| ID | Requirement |
| --- | --- |
| FR1 | Clients submit tasks via gRPC. |
| FR2 | The scheduler validates Task ID (unique, non-empty), type (supported), input (well-formed, within size limits), and priority (1–10). |
| FR3 | The system tracks task states: QUEUED, RUNNING, COMPLETED, CANCELLED, FAILED. |
| FR4 | Workers execute the full task catalogue of Section 7, spanning CPU-bound, memory-bound, I/O-bound, network-bound, and distributed task types. |
| FR5 | Workers execute multiple tasks concurrently using configurable thread pools. |
| FR6 | Workers register with the scheduler (ID, host, port, cores, memory, thread-pool size). |
| FR7 | The scheduler collects worker metrics through periodic heartbeats. |
| FR8 | All nodes maintain Lamport logical clocks for event ordering and synchronize physical clocks using the Berkeley algorithm (Cristian's as an alternative). |
| FR9 | Scheduler nodes elect a coordinator using Bully or Ring (configurable). |
| FR10 | Scheduler task state is replicated across nodes with a selectable consistency model: strong (quorum) or eventual. |
| FR11 | The scheduler distributes tasks using a selectable strategy. |
| FR12 | The scheduler cluster uses primary-backup replication: the primary forwards every state change to backups before acknowledging; on primary failure a backup is promoted through election with no task loss. Tasks on failed workers are reassigned. |
| FR13 | The system records execution time, queue length, CPU, memory, throughput, availability, and arrival rate. |
| FR14 | The prediction engine forecasts execution time, queue growth, and overload probability. |
| FR15 | The predictive scheduler uses predictions to select workers. |
| FR16 | A benchmark harness compares all strategies under identical workloads. |
| FR17 | A dashboard shows workers, tasks, queue, utilization, leader, predictions, and benchmark results. |
| FR18 | Clients can query status and cancel queued tasks. |
| FR19 | The predictive scheduler falls back to Least Loaded if the prediction engine is unavailable or slow. |
| FR20 | A Spark job processes execution history with MapReduce to produce aggregated statistics and ML features. |
| FR21 | MPI computation supports Broadcast, Scatter, and Gather, and parallel matrix multiplication as a benchmark workload. |
| FR22 | Tasks may declare dependencies, forming a DAG; a task runs only after its parents complete. |
| FR23 | The queue is priority-ordered with ageing so low-priority tasks cannot starve. |
| FR24 | Tasks may carry a deadline; the scheduler tracks and reports SLA compliance. |
| FR25 | Failed tasks are retried with exponential backoff up to a limit, after which they move to a dead-letter queue. |
| FR26 | Tasks that exceed a timeout are cancelled and optionally re-dispatched elsewhere. |
| FR27 | Identical task inputs may be served from a result cache instead of re-executed. |
| FR28 | Stragglers are detected and a speculative duplicate is launched on a faster worker; the first result wins. |
| FR29 | The system can start and stop worker instances based on predicted load (auto-scaling). |
| FR30 | Clients authenticate with an API key or token; per-client rate limits and quotas are enforced. |
| FR31 | An admin API allows changing the scheduling strategy at runtime, draining a worker, and pausing the queue. |
| FR32 | Every scheduling decision is explainable: the per-worker score breakdown is recorded and viewable. |
| FR33 | Models are versioned; a new model can run in shadow mode before being promoted. |
| FR34 | Prediction drift is monitored and triggers a retraining alert. |
| FR35 | A chaos/fault-injection API can kill workers, add latency, or spike CPU on demand. |
| FR36 | A workload generator produces steady, bursty, periodic, and trace-replay profiles. |
| FR37 | Tasks are grouped into per-user queues scheduled by weighted fair share. |
| FR38 | Every request carries a trace ID propagated across nodes and visible in logs. |
| FR39 | Prometheus-compatible metrics are exposed for scraping. |
| FR40 | The benchmark harness generates a shareable HTML/PDF report with charts. |
| FR41 | SLA breaches and node failures raise alerts through a webhook. |

## 5. Non-Functional Requirements

| Category | Requirement | Measurable target (prototype) |
| --- | --- | --- |
| Performance | Low scheduling overhead | Scheduling decision < 20 ms (p95), including prediction call |
| Scalability | Grow with workers/tasks | Tested with 3–10 workers, 1,000+ tasks per run |
| Availability | Survive recoverable failures | New leader elected within ~5 s of leader failure |
| Reliability | No accepted task is lost | 0 lost tasks in fault-injection tests |
| Concurrency | Thread-safe shared state | No race conditions under stress tests |
| Consistency | Correct, replicated state | Invalid transitions rejected; strong mode never returns stale reads; eventual mode replicas converge |
| Clock accuracy | Synchronized physical clocks | Offsets reduced to a few ms after a Berkeley round on a LAN |
| Maintainability | Modular code | Separate Maven modules; strategy interface |
| Extensibility | Add new strategies/models easily | New strategy = one class implementing `SchedulingStrategy` |
| Observability | Logs and metrics everywhere | Structured logs with node ID + Lamport time |
| Security | Secure in production mode | TLS for gRPC optional; not required for lab demo |
| Usability | Simple to operate | CLI client + dashboard; one-command Docker startup |
| Reproducibility | Repeatable experiments | Fixed random seeds, saved workload traces, config files |
| Testability | Automated tests | Unit, integration, and benchmark suites |
| ML quality | Prediction error measured | Report MAE/RMSE/R², F1/ROC-AUC on held-out data |

---

## 6. Feature Catalogue

The ten experiments give the system its distributed-systems backbone. These features turn it into a complete mini project. They are grouped in build order: **Tier 1** makes the system usable and demo-ready, **Tier 2** is what makes it stand out, and **Tier 3** is optional polish if time allows.

### Tier 1: Core product features (build these)

| # | Feature | What it adds | Where it lives |
| --- | --- | --- | --- |
| F1 | **Task DAG / workflows** | A task can depend on others; the scheduler runs a job graph in topological order (e.g., map tasks → reduce task) | Scheduler queue |
| F2 | **Priority queue with ageing** | High-priority tasks jump the queue, but waiting tasks gain priority over time so nothing starves | Scheduler queue |
| F3 | **Deadlines and SLA tracking** | Each task can carry a deadline; the system reports on-time percentage per strategy | Scheduler + metrics |
| F4 | **Retries with backoff + dead-letter queue** | Transient failures retry with exponential backoff; permanently failing tasks are parked for inspection | Fault module |
| F5 | **Timeouts and cancellation** | Runaway tasks are killed and re-dispatched; clients can cancel queued tasks | Scheduler + worker |
| F6 | **Result cache** | A hash of (task type + input) returns a cached result instantly for repeat work | Scheduler + PostgreSQL |
| F7 | **Admin control API** | Switch scheduling strategy live, drain a worker, pause/resume the queue, trigger an election | Spring Boot API |
| F8 | **Workload generator** | Reproducible steady, bursty, periodic, and mixed workloads, plus trace replay from a saved file | Client module |
| F9 | **Auth + rate limiting** | API key or JWT per client, per-client quotas, optional TLS on gRPC | Scheduler interceptor |
| F10 | **Structured logs + trace IDs** | One trace ID follows a task across client, scheduler, and worker, alongside its Lamport timestamp | Common module |

### Tier 2: Differentiators (these make the project memorable)

| # | Feature | What it adds | Why it impresses |
| --- | --- | --- | --- |
| F11 | **Speculative execution** | A slow task (straggler) gets a duplicate on a faster worker; the first result wins and the loser is cancelled | Real schedulers (Hadoop, Spark) do exactly this |
| F12 | **Predictive auto-scaling** | When the queue forecast exceeds capacity, new worker containers start; when load drops, idle workers stop | Turns prediction into visible action, not just worker choice |
| F13 | **Explainable scheduling** | Every decision stores the per-worker score breakdown: predicted wait, predicted execution, overload penalty | The dashboard can answer "why worker 3?" for any task |
| F14 | **Shadow mode + model registry** | A new model predicts alongside the live one without affecting decisions until it proves better; models are versioned | Genuine ML engineering practice |
| F15 | **Drift detection + retraining** | Live prediction error is tracked; sustained degradation raises a retrain alert and can trigger automatic retraining | Addresses the "workload patterns change" limitation head-on |
| F16 | **Chaos engineering panel** | Buttons to kill a worker, kill the primary, inject latency, or spike CPU, so failures can be demonstrated on demand | Makes the fault-tolerance demo interactive |
| F17 | **What-if simulator** | Replay a saved workload against any strategy at accelerated speed and compare outcomes without running the cluster | Strong live-demo feature |
| F18 | **Auto-generated benchmark report** | One command produces an HTML/PDF report with tables and charts comparing all five strategies | Doubles as your project report |

### Tier 3: Optional polish

| # | Feature | What it adds |
| --- | --- | --- |
| F19 | **Multi-tenant fair share** | Per-user queues with weights, so one heavy user cannot monopolize the cluster |
| F20 | **Cost / energy-aware scheduling** | Each worker has a simulated cost or power profile; a strategy minimizes cost within the deadline |
| F21 | **Data-locality-aware placement** | MapReduce tasks prefer the worker that already holds the input chunk |
| F22 | **Prometheus + Grafana** | Standard metrics scraping and prebuilt dashboards next to the custom UI |
| F23 | **Webhook alerts** | Notify on SLA breach, node failure, or model drift |
| F24 | **REST + CLI submission** | A REST endpoint and a small CLI in addition to gRPC, for easy testing |
| F25 | **Task artifacts** | Store and download task output files (e.g., a result matrix or a Spark report) |

### Minimum feature set for a complete mini project

If time is tight, build all ten experiments plus **F1–F8, F11, F13, F16, F18**. That gives a working scheduler with workflows, priorities, retries, an admin API, a straggler mitigation feature, explainable decisions, a failure demo, and an automated benchmark report, which is enough for a strong submission and a solid resume line.

---

## 7. Task Catalogue

Three task types are too few: a scheduler that only ever sees CPU, matrix, and sleep work has almost nothing to predict, and the ML models would learn a trivial mapping. A **mix of resource profiles** is what makes both the scheduling problem and the prediction problem real. A CPU-bound task and an I/O-bound task of the same "size" behave completely differently on a loaded worker, and a good scheduler has to know the difference.

### 7.1 Design rule

Every task type defines:

1. an **input string** parsed by the worker,
2. a **size knob** that scales the work, so execution time can be varied smoothly,
3. a **resource profile** (CPU / memory / disk / network / parallel), which becomes an ML feature,
4. a **deterministic output** that can be verified.

### 7.2 The catalogue

#### CPU-bound

| Task | Input | What it does | Notes |
| --- | --- | --- | --- |
| `CPU_TASK` | `n=5000000` | Counts primes below n, or sums a series | The baseline CPU load; execution time scales predictably |
| `HASH_TASK` | `rounds=200000` | Repeated SHA-256 / PBKDF2 hashing of a payload | Very steady per-round cost, so prediction should be near-exact |
| `MONTE_CARLO_TASK` | `samples=10000000` | Estimates π by random sampling | Embarrassingly parallel, also good for the MPI and thread-pool demos |
| `MATRIX_TASK` | `size=500` | Matrix multiplication (threads locally, MPI when distributed) | Heavy CPU and memory; the main heavy benchmark workload |

#### Memory-bound

| Task | Input | What it does | Notes |
| --- | --- | --- | --- |
| `SORT_TASK` | `n=5000000, type=random` | Generates and sorts a large array | Allocation-heavy; shows memory pressure and GC effects |
| `COMPRESS_TASK` | `size_mb=50, level=6` | Compresses and decompresses a generated payload with gzip | CPU and memory mix; the level knob changes the cost curve |
| `GRAPH_TASK` | `nodes=100000, algo=bfs` | BFS or PageRank on a generated graph | Irregular memory access, so execution time is harder to predict |

#### I/O and network-bound

| Task | Input | What it does | Notes |
| --- | --- | --- | --- |
| `FILE_IO_TASK` | `size_mb=100, mode=write` | Writes and reads a temporary file, then checksums it | Disk-bound: CPU stays low while the task is slow, which breaks naive CPU-based scheduling |
| `DB_QUERY_TASK` | `rows=50000` | Runs an aggregate query against PostgreSQL | Tests contention on a shared resource |
| `HTTP_TASK` | `url=..., timeout=2000` | Calls a local mock HTTP service with configurable latency | Network-bound; good for demonstrating timeouts and retries |
| `SLEEP_TASK` | `ms=1500` | Sleeps for a fixed time | The control case: known duration, near-zero resource use |

#### Distributed and composite

| Task | Input | What it does | Notes |
| --- | --- | --- | --- |
| `MAPREDUCE_TASK` | `dataset=logs.csv, job=avg_exec` | Submits a Spark MapReduce job (Exp 7) | Bridges the scheduler and the big-data experiment |
| `ML_INFER_TASK` | `model=exec_time, batch=1000` | Runs batch inference with a saved model | Variable cost depending on batch size; the system schedules its own ML work |
| `WORKFLOW_TASK` | `dag=job.json` | A DAG of other tasks (F1) | E.g., 4 `CPU_TASK` maps → 1 `SORT_TASK` reduce; exercises dependency scheduling |

### 7.3 Workload mixes for benchmarking

The generator (F8) composes these into named profiles:

| Profile | Mix | Tests |
| --- | --- | --- |
| `cpu_heavy` | 80% CPU / HASH / MONTE_CARLO | Pure compute scheduling |
| `io_heavy` | 70% FILE_IO / HTTP / DB_QUERY | Whether the scheduler over-trusts low CPU readings |
| `mixed` | Even spread across all profiles | The realistic case, and the hardest for prediction |
| `bursty` | Idle, then a sudden flood of MATRIX and SORT tasks | Spike handling and predictive auto-scaling |
| `long_tail` | Mostly short tasks with a few very long ones | Straggler handling and speculative execution (F11) |
| `deadline` | Tasks with tight deadlines and mixed priorities | SLA compliance and priority ageing |

### 7.4 Why this matters for the ML component

With only three task types, predicting execution time is nearly a lookup table. With this catalogue:

- **Resource profile becomes a feature.** The model learns that an I/O task on a CPU-busy worker is fine, while a CPU task on the same worker is not.
- **Interference becomes visible.** Two SORT tasks on one worker slow each other far more than two SLEEP tasks do.
- **Heterogeneity matters.** A worker with more cores helps MATRIX tasks far more than it helps HTTP tasks.
- **The cold-start problem becomes demonstrable.** Hold out one task type during training, then show how the system handles it and recovers.

This is what makes the predictive scheduler beat Least Loaded: reactive strategies look at a single CPU number, while PrediSched knows what kind of work it is about to place.

### 7.5 Implementation

Each type is a small class implementing a common interface, registered in a map:

```
public interface TaskExecutor {
    TaskType type();
    ResourceProfile profile();        // CPU_BOUND, MEMORY_BOUND, IO_BOUND, NETWORK_BOUND, PARALLEL
    ExecutionResult execute(String input) throws Exception;
}
```

Adding a task type means writing one class and registering it, with no scheduler changes. Start with `CPU_TASK`, `SLEEP_TASK`, and `MATRIX_TASK` in Experiment 1, then add the rest as the workload generator and ML dataset need them.

---

## 8. Technology Stack

| Layer | Technology | Why |
| --- | --- | --- |
| Language (core) | **Java 17+** | Strong concurrency (`ExecutorService`, `ConcurrentHashMap`), mature gRPC support |
| RPC | **gRPC + Protocol Buffers** | Typed contracts, generated stubs, efficient binary transport, already used in Exp 1 |
| Build | **Maven** (multi-module) | Dependency management, protobuf code generation, test lifecycle |
| Logging | **SLF4J + Logback** | Structured, configurable logging |
| Testing (Java) | **JUnit 5, Mockito** | Unit and integration tests |
| MPI | **mpi4py** with **MS-MPI** (Windows) or **OpenMPI** (Linux / Docker); MPJ Express if Java is mandatory | Experiments 9 and 10: collectives and parallel matrix multiplication |
| MapReduce | **Apache Spark (PySpark)** in local mode | Experiment 7; also aggregates ML features |
| ML | **Python 3.10+**, pandas, NumPy, scikit-learn, XGBoost, joblib, matplotlib | Standard, well-documented ML toolkit |
| Model serving | **Python gRPC server** (`grpcio`) | Same protocol as the rest of the system; Java calls it like any service |
| Storage | **PostgreSQL** | Persistent tasks, metrics history, predictions, decisions |
| DB access (Java) | JDBC / Spring Data JPA | Simple persistence |
| Event streaming (optional, late) | **Apache Kafka** | High-volume metric streams; only if scale requires it |
| Dashboard API | **Spring Boot 3** | REST + WebSocket, health checks, Actuator metrics |
| Frontend | **React + TypeScript** (Vite) | Component-based live dashboard |
| Charts | **Recharts** (standard charts), **D3** (heatmap, Gantt), **React Flow** (topology, DAG) | Rich visualizations |
| UI kit | **Tailwind CSS + shadcn/ui**, **TanStack Query** | Fast, consistent UI and data fetching |
| Live updates | **WebSocket (Spring `@MessageMapping` / STOMP)** | Streaming metrics and task events |
| Containers | **Docker + Docker Compose** | Run many scheduler/worker instances on one machine |
| Version control | **Git + GitHub** | Collaboration, CI |
| CI (optional) | **GitHub Actions** | Build + test on push |
| API testing | **grpcurl / Postman** | Manual RPC testing |
| Auth | **JWT (jjwt)** or static API keys + gRPC interceptors | Client authentication and quotas (F9) |
| Caching | **Caffeine** (in-process) or **Redis** (optional) | Result cache (F6) |
| Metrics export | **Micrometer + Prometheus** (optional Grafana) | Standard observability (F22) |
| Reporting | **Jinja2 + matplotlib**, or WeasyPrint for PDF | Auto-generated benchmark report (F18) |
| Scheduling utilities | **JGraphT** (optional) | DAG validation and topological ordering (F1) |

### 8.1 Resources required

**Software:** JDK 17+, Maven, IntelliJ IDEA or VS Code, Git, Python 3.10+, Node.js + npm, PostgreSQL, Docker Desktop, grpcurl or Postman, a modern browser, Apache Spark (`pip install pyspark`), mpi4py, and MS-MPI (Windows) or OpenMPI (Linux). If Spark gives trouble on native Windows, run it inside WSL or Docker.

**Hardware:** One laptop (8 GB RAM minimum, 16 GB recommended, 4+ cores) is enough. Nodes are simulated as separate processes on different ports or as Docker containers. For stronger results, use 2–3 machines on the same network.

**Data:** A self-generated workload dataset (described in Section 12). No external dataset is required.

---

## 9. Repository Structure

```
predisched/
├── pom.xml                         # parent Maven POM
├── proto/                          # shared .proto contracts
│   ├── task.proto
│   ├── worker.proto
│   ├── election.proto
│   ├── replication.proto
│   ├── clock.proto
│   └── prediction.proto
├── predisched-common/              # models, Lamport + Berkeley clocks, config, utils
├── predisched-client/              # CLI client + workload generator
├── predisched-scheduler/           # scheduler node, queue, strategies
│   └── strategy/
│       ├── SchedulingStrategy.java
│       ├── RoundRobinStrategy.java
│       ├── RandomStrategy.java
│       ├── LeastLoadedStrategy.java
│       ├── ResourceAwareStrategy.java
│       └── PredictiveStrategy.java
├── predisched-worker/              # worker node, executors, metrics reporter
├── predisched-election/            # Bully + Ring implementations
├── predisched-replication/         # replication + consistency models (Exp 5)
├── predisched-fault/               # primary-backup, heartbeats, failover (Exp 8)
├── spark/                          # PySpark MapReduce jobs (Exp 7)
├── mpi/                            # mpi4py collectives + matrix multiplication (Exp 9, 10)
├── predisched-benchmark/           # benchmark harness
├── predisched-dashboard-api/       # Spring Boot
├── ml/                             # Python
│   ├── data/                       # generated CSVs
│   ├── notebooks/                  # exploration
│   ├── features.py
│   ├── train.py
│   ├── evaluate.py
│   ├── models/                     # saved .joblib models
│   └── prediction_server.py        # gRPC PredictionService
├── dashboard/                      # React + TypeScript
├── docker/
│   └── docker-compose.yml
├── configs/                        # cluster + benchmark configs (YAML)
├── scripts/                        # start-cluster, run-benchmark
└── docs/                           # experiment reports, diagrams
```

---

## 10. Service Contracts (Protocol Buffers)

### 10.1 Client ↔ Scheduler

```
syntax = "proto3";
package predisched;

enum TaskType {
  CPU_TASK = 0; MATRIX_TASK = 1; SLEEP_TASK = 2; MAPREDUCE_TASK = 3;
  SORT_TASK = 4; HASH_TASK = 5; COMPRESS_TASK = 6; MONTE_CARLO_TASK = 7;
  IMAGE_TASK = 8; FILE_IO_TASK = 9; HTTP_TASK = 10; DB_QUERY_TASK = 11;
  GRAPH_TASK = 12; ML_INFER_TASK = 13; WORKFLOW_TASK = 14;
}
enum TaskStatus { QUEUED = 0; RUNNING = 1; COMPLETED = 2; CANCELLED = 3; FAILED = 4; }

message TaskRequest {
  string task_id = 1;
  TaskType type = 2;
  string input = 3;       // e.g. "100000", "matrix size 200", "1500 ms"
  int32 priority = 4;     // 1 (low) to 10 (high)
  int64 lamport_time = 5;
}

message TaskResponse {
  string task_id = 1;
  bool accepted = 2;
  string message = 3;
  int64 lamport_time = 4;
}

message TaskStatusRequest  { string task_id = 1; }
message TaskStatusResponse {
  string task_id = 1;
  TaskStatus status = 2;
  string result = 3;
  string worker_id = 4;
  int64 exec_time_ms = 5;
}

service SchedulerService {
  rpc SubmitTask   (TaskRequest)       returns (TaskResponse);
  rpc GetTaskStatus(TaskStatusRequest) returns (TaskStatusResponse);
  rpc CancelTask   (TaskStatusRequest) returns (TaskResponse);
}
```

### 10.2 Scheduler ↔ Worker

```
message RegisterRequest {
  string worker_id = 1; string host = 2; int32 port = 3;
  int32 cores = 4; int64 memory_mb = 5; int32 pool_size = 6;
}
message Heartbeat {
  string worker_id = 1;
  double cpu_pct = 2; double mem_pct = 3;
  int32 active_threads = 4; int32 queue_len = 5;
  int64 tasks_completed = 6; double avg_exec_ms = 7;
  int64 lamport_time = 8;
}
message Ack { bool ok = 1; string message = 2; }

message ExecuteRequest { TaskRequest task = 1; }
message ExecuteResult  {
  string task_id = 1; bool success = 2; string output = 3;
  int64 exec_time_ms = 4; int64 wait_time_ms = 5; int64 lamport_time = 6;
}

service WorkerService {
  rpc ExecuteTask(ExecuteRequest) returns (ExecuteResult);
}
service RegistryService {
  rpc Register     (RegisterRequest) returns (Ack);
  rpc SendHeartbeat(Heartbeat)       returns (Ack);
}
```

### 10.3 Election

```
message ElectionMsg    { int32 sender_id = 1; repeated int32 ring_ids = 2; }
message CoordinatorMsg { int32 leader_id = 1; }

service ElectionService {
  rpc Election   (ElectionMsg)    returns (Ack);   // Bully: "OK" reply
  rpc RingPass   (ElectionMsg)    returns (Ack);   // Ring: forward ID list
  rpc Coordinator(CoordinatorMsg) returns (Ack);
  rpc Ping       (Ack)            returns (Ack);   // leader liveness
}
```

### 10.4 Scheduler ↔ Prediction Engine

```
message WorkerState {
  string worker_id = 1; int32 cores = 2;
  double cpu_pct = 3; double mem_pct = 4;
  int32 active_threads = 5; int32 queue_len = 6;
  double avg_exec_ms = 7; double arrival_rate = 8;
}
message PredictRequest  { TaskRequest task = 1; repeated WorkerState workers = 2; }
message WorkerPrediction {
  string worker_id = 1;
  double pred_exec_ms = 2;
  double pred_queue_len = 3;     // queue length in next N seconds
  double overload_prob = 4;      // 0..1
}
message PredictResponse { repeated WorkerPrediction predictions = 1; int32 model_version = 2; }

service PredictionService {
  rpc Predict(PredictRequest) returns (PredictResponse);
}
```

---

### 10.5 Replication (Exp 5 and Exp 8)

```
message ReplicateRequest {
  int64 seq_no = 1;
  string op = 2;            // ENQUEUE, UPDATE_STATUS, CANCEL
  string task_id = 3;
  bytes payload = 4;        // serialized task record
  int64 version = 5;
  int64 lamport_time = 6;
}
message ReadRequest  { string task_id = 1; }
message ReadResponse { string task_id = 1; bytes payload = 2; int64 version = 3; }
message SyncRequest  { int64 from_seq = 1; }

service ReplicationService {
  rpc Replicate(ReplicateRequest) returns (Ack);
  rpc Read     (ReadRequest)      returns (ReadResponse);
  rpc SyncFrom (SyncRequest)      returns (stream ReplicateRequest); // catch-up for new/recovered backups
}
```

### 10.6 Clock synchronization (Exp 3)

```
message TimeRequest  { int64 requester_time_ms = 1; }
message TimeResponse { int64 node_time_ms = 1; }
message ClockAdjust  { int64 offset_ms = 1; }

service ClockService {
  rpc GetTime(TimeRequest) returns (TimeResponse);  // Berkeley poll / Cristian request
  rpc Adjust (ClockAdjust) returns (Ack);           // Berkeley correction
}
```

---

## 11. Experiment-by-Experiment Implementation

### Exp 1: Client-server communication using RPC

- Define `task.proto`; generate stubs with `protobuf-maven-plugin`.
- Scheduler server validates tasks and stores them in a `ConcurrentHashMap<String, TaskRecord>`.
- Client CLI submits CPU / MATRIX / SLEEP tasks and polls status.
- gRPC is an RPC framework, so it satisfies the "RPC or Java RMI" requirement. A short RMI comparison can be added to the report.
- **Output:** submission acknowledgements, validation errors for bad inputs, final results.
- **Reused later:** all node-to-node communication.

### Exp 2: Multithreading in a distributed system

- Worker uses `ThreadPoolExecutor` with a configurable pool size and a bounded `LinkedBlockingQueue`.
- Multiple clients submit at once; the scheduler handles them concurrently.
- Thread-safe counters with `AtomicLong`; shared maps with `ConcurrentHashMap`.
- Measure throughput and latency for pool sizes 1, 2, 4, 8.
- **Output:** tasks running in parallel with thread names; speedup table.
- **Reused later:** worker execution engine.

### Exp 3: Clock synchronization (logical and physical)

- **Logical:** `LamportClock` with `tick()` before local events and sends, and `update(received)` setting `max(local, received) + 1`. Every RPC message and log line carries a Lamport timestamp.
- **Physical, Berkeley algorithm:** the leader acts as time daemon. It polls every node with `GetTime`, adjusts each reading for round-trip delay, discards outliers, computes the average, and sends each node its own `offset_ms` correction through `Adjust`.
- **Physical, Cristian's algorithm (alternative):** a worker requests the time from the scheduler and sets its clock to `server_time + RTT / 2`.
- Simulate clock drift by giving each node an artificial offset (e.g., −300 ms to +500 ms) and drift rate.
- **Output:** node offsets before and after synchronization; a Lamport-ordered event log across client, scheduler, and workers.
- **Reused later:** event ordering, last-writer-wins in replication (Exp 5), and trustworthy timestamps in the ML dataset.

### Exp 4: Bully and Ring election

- Five scheduler nodes, IDs 1–5 (ID can reflect capacity).
- **Bully:** on leader timeout, send `Election` to higher IDs; if no `OK` arrives within the timeout, declare self leader and broadcast `Coordinator`.
- **Ring:** pass an `ElectionMsg` with an accumulating ID list around the ring, skipping dead nodes; when it returns to the initiator, the highest ID becomes leader and a `Coordinator` message circulates.
- Both implement an `ElectionAlgorithm` interface. Bully is the default for PrediSched.
- **Output:** election logs; kill node 5 and node 4 is elected; comparison of message count and election time.
- **Reused later:** the elected node becomes the primary scheduler (Exp 8).

### Exp 5: Data consistency and replication models

- Three scheduler nodes each hold a `ReplicatedTaskStore` in which every record has a version number and a Lamport timestamp.
- **Strong consistency (quorum):** N = 3, W = 2, R = 2, so W + R > N. A write succeeds only after 2 replicas acknowledge; a read queries 2 replicas and returns the highest version. Reads are never stale.
- **Eventual consistency:** the write is applied locally and acknowledged immediately, then replicated asynchronously with an injected delay. Conflicts are resolved by last-writer-wins using Lamport timestamps.
- **Client-centric models (bonus):** read-your-writes and monotonic reads, by having the client remember the last version it saw.
- Demonstrations:
  1. In eventual mode, read from a lagging replica, show a stale value, then show the replicas converging.
  2. In strong mode, repeat the same read and show it is always current.
  3. Compare write latency and availability for both modes, including with one replica down.
- **Output:** consistency demo logs and a latency vs consistency table.
- **Reused later:** the replication layer underneath primary-backup (Exp 8).

### Exp 6: Load balancing

- `SchedulingStrategy` interface: `WorkerInfo select(Task task, List<WorkerInfo> workers)`.
- Implement Round Robin, Random, Least Loaded (lowest queue + active threads), and Resource-Aware (weighted CPU / memory / queue score).
- **Output:** distribution of tasks per worker, latency, and imbalance under each strategy.
- **Reused later:** the baselines for the predictive scheduler.

### Exp 7: MapReduce with Apache Spark

- Input: PrediSched execution logs (CSV) collected during Exp 2 and Exp 6. A classic word count on the event log serves as a warm-up.
- Use the Spark **RDD API** so the map and reduce phases are explicit:
  - **Map:** each log line → `(task_type, (exec_time_ms, 1))` and `(worker_id, (exec_time_ms, 1))`.
  - **Reduce:** `reduceByKey` sums times and counts.
  - **Result:** average execution time and task count per task type and per worker.
- Run with `spark-submit` in local mode (`local[*]`) and show the job stages in the Spark UI at port 4040.
- **Output:** aggregated tables and the Spark job DAG.
- **Reused later:** feature aggregation for the ML pipeline (Section 12).

### Exp 8: Fault tolerance with primary-backup replication

- The node elected in Exp 4 is the **primary**; the others are **backups**.
- Write path: client → primary → primary logs the change with a sequence number → forwards `Replicate` to backups → backups apply it in sequence order and acknowledge → primary acknowledges the client.
- Backups watch the primary through `Ping` heartbeats. On timeout, Bully elects the highest-ID backup as the new primary; it already holds the full task state and resumes dispatching.
- Clients keep a list of scheduler addresses and retry against the new primary.
- A recovered node rejoins as a backup and catches up using `SyncFrom`.
- Worker failures are also handled: after 3 missed heartbeats, RUNNING tasks return to QUEUED and are reassigned.
- **Output:** submit 100 tasks, kill the primary at task 50, and show all 100 tasks complete with none lost or duplicated.

### Exp 9: MPI collective communication

- Implemented with mpi4py and run with `mpiexec -n 4 python mpi/collectives.py`.
- Rank 0 plays the scheduler (root); the other ranks are workers.
  - **Broadcast:** rank 0 sends the scheduling configuration (task type, parameters) to all ranks with `comm.bcast`.
  - **Scatter:** rank 0 splits a batch of task inputs into chunks and sends one chunk to each rank with `comm.scatter` (and `Scatter` on NumPy arrays).
  - **Gather:** each rank executes its chunk and returns results and timings to rank 0 with `comm.gather`.
- **Output:** rank-wise logs showing what each rank received and returned, and the combined result on rank 0.

### Exp 10: Parallel matrix multiplication using MPI

- Rank 0 creates matrices A and B (n × n).
- **Scatter** the rows of A across ranks (`Scatterv` when n is not divisible by the process count).
- **Broadcast** B to all ranks.
- Each rank computes its row block of A × B locally.
- **Gather** the partial results into C on rank 0 and verify C against a sequential result.
- Measure time for n = 200, 400, 800 with 1, 2, and 4 processes; report speedup and efficiency.
- **Output:** correctness check, timing table, speedup graph.
- **Reused later:** an MPI-backed executor for `MATRIX_TASK` and a heavy benchmark workload for the predictive scheduler.

---

## 12. ML Pipeline

### 12.1 Data generation

Run controlled workloads with the non-predictive strategies and log every task execution.

| Column | Description |
| --- | --- |
| timestamp | Dispatch time |
| task_id, task_type | Identity and type |
| resource_profile | CPU / memory / IO / network / parallel |
| concurrent_tasks_on_worker | How many other tasks were running (interference) |
| input_size | Number / matrix dimension / sleep ms |
| priority | 1–10 |
| worker_id, worker_cores | Target worker and capacity |
| cpu_pct, mem_pct | Worker utilization at dispatch |
| active_threads, queue_len | Worker load at dispatch |
| arrival_rate | Tasks/sec over the last 10 s |
| avg_exec_recent | Worker's mean exec time over recent tasks |
| wait_time_ms | Time spent queued (target) |
| exec_time_ms | Actual execution time (target) |
| queue_len_future | Worker queue length N seconds later (target) |
| overloaded_future | 1 if CPU > 85% or queue > threshold within N seconds (target) |
| status | COMPLETED / FAILED |

**Workload patterns to generate** (so the model sees variety):

- steady (constant rate),
- bursty (sudden spikes),
- periodic (sine-wave rate),
- mixed task types across all resource profiles,
- heterogeneous workers (different pool sizes / CPU limits via Docker).

Target: at least 10,000–50,000 rows. Raw logs are cleaned and aggregated with the Spark job from Exp 7 before training.

### 12.2 Models

| Model | Type | Target | Metrics |
| --- | --- | --- | --- |
| M1: Execution time | Regression | `exec_time_ms` | MAE, RMSE, R² |
| M2: Queue forecast | Regression / time series | `queue_len_future` | MAE, RMSE |
| M3: Overload risk | Classification | `overloaded_future` | Precision, Recall, F1, ROC-AUC |

**Model progression:** naive baseline (historical mean per task type) → Linear/Logistic Regression → Random Forest → XGBoost. Keep the simplest model that clearly beats the baseline.

**Validation:** time-based train/test split (no random shuffling of time-series data), plus a held-out workload pattern to test generalization.

### 12.3 Serving

- `prediction_server.py` loads saved models with joblib and exposes `PredictionService` over gRPC.
- The scheduler calls it with a strict timeout (e.g., 10 ms). On timeout or error, it falls back to Least Loaded (FR19).
- Log every prediction next to the actual outcome for monitoring and retraining.

### 12.4 Predictive scheduling logic

For each candidate worker *w*:

```
expected_completion(w) = predicted_wait(w) + predicted_exec(task, w)
cost(w) = expected_completion(w) × (1 + λ × overload_prob(w))
```

- `predicted_wait(w)` is derived from the queue forecast and the worker's recent average execution time.
- λ (penalty weight) is tuned during benchmarking.
- Choose the worker with the lowest cost.
- High-priority tasks can use a stricter overload threshold.

**Proactive actions (stretch goals):** hold low-priority tasks when all workers show high overload risk; rebalance queued tasks when a queue spike is forecast; signal a "scale up" event when the cluster-wide forecast exceeds capacity.

---

## 13. Benchmarking Methodology

**Strategies compared:** Round Robin, Random, Least Loaded, Resource-Aware, Predictive.

**Controls:** the same saved workload trace, same worker configuration, same seed, and at least 5 repetitions per configuration. Report mean and standard deviation.

**Scenarios:**

1. Steady load
2. Bursty load
3. Heterogeneous workers
4. Mixed task types across all resource profiles
5. Worker failure mid-run

**Metrics:**

| Metric | Meaning |
| --- | --- |
| Average latency | Submit → completion |
| P95 / P99 latency | Tail behaviour |
| Throughput | Tasks completed per second |
| Makespan | Time to finish the whole workload |
| CPU utilization | Mean and variance across workers |
| Worker imbalance | Std. dev. of load across workers |
| Max queue length | Spike handling |
| SLA violations | % of tasks exceeding a deadline |
| Scheduling overhead | Time spent choosing a worker |
| Prediction error | Live MAE of exec-time predictions |

**Outputs:** CSV results, comparison tables, and charts (latency CDF, throughput bars, queue-over-time lines).

---

## 14. Data Storage (PostgreSQL)

| Table | Key columns |
| --- | --- |
| `tasks` | task_id, type, input, priority, status, worker_id, submitted_at, started_at, completed_at, result |
| `workers` | worker_id, host, port, cores, memory_mb, pool_size, status, last_heartbeat |
| `worker_metrics` | worker_id, ts, cpu_pct, mem_pct, active_threads, queue_len |
| `execution_history` | task_id, worker_id, features…, wait_time_ms, exec_time_ms |
| `predictions` | task_id, worker_id, pred_exec_ms, overload_prob, model_version, ts |
| `scheduling_decisions` | task_id, strategy, chosen_worker, cost, decision_ms |
| `events` | node_id, lamport_time, event_type, details |
| `replication_log` | seq_no, node_id, op, task_id, version, lamport_time, applied_at |
| `clock_sync` | node_id, ts, offset_before_ms, offset_after_ms, algorithm |
| `failures` | node_id, type, detected_at, recovered_at |
| `benchmark_runs` | run_id, strategy, scenario, metrics JSON |

Early experiments (1–4) use in-memory maps only; the database is added from Exp 8 onward.

---

## 15. Dashboard

The dashboard is where an examiner actually *sees* the distributed system working. It should make four things obvious within ten seconds of loading: which node is the leader, how loaded each worker is, what the scheduler is predicting, and whether predictions are turning out to be right.

### 15.1 Page structure

```
┌──────────────────────────────────────────────────────────────────────┐
│ PrediSched   ● LIVE   Leader: S3   Strategy: PREDICTIVE   Model v7   │
├──────────────────────────────────────────────────────────────────────┤
│  Overview │ Cluster │ Tasks │ Predictions │ Benchmarks │ Chaos │ Logs │
└──────────────────────────────────────────────────────────────────────┘
```

A persistent top bar always shows connection status, the current leader, the active strategy, the model version, and a big red **STOP/PAUSE QUEUE** control.

### 15.2 Overview page

| Widget | Visualization | What it shows |
| --- | --- | --- |
| KPI strip | 6 stat cards with sparklines | Tasks/sec, queue depth, mean latency, P95 latency, active workers, SLA compliance |
| Cluster topology | Animated node graph (React Flow or D3 force layout) | Client → leader → workers, with edges that pulse when a task is dispatched; the leader wears a crown and failed nodes fade to grey |
| Live queue chart | Area chart, actual vs predicted | The forecast line runs ahead of the actual line, which is the clearest possible picture of the project's core idea |
| Throughput gauge | Radial gauge | Current tasks/sec against the measured cluster capacity |
| Alert feed | Toast list | SLA breaches, elections, worker failures, drift warnings |

### 15.3 Cluster page

| Widget | Visualization | What it shows |
| --- | --- | --- |
| Worker cards | Card grid with radial CPU/memory rings | ID, host, cores, CPU, memory, active threads, queue length, status, uptime, tasks completed |
| Load heatmap | Worker × time heatmap | Hot and cold workers over the last few minutes; imbalance becomes visible as colour banding |
| Thread-pool view | Stacked bar per worker | Busy threads vs idle threads vs queued tasks |
| Timeline / Gantt | Horizontal swimlanes, one row per worker | Every task as a coloured bar; stragglers and speculative duplicates stand out immediately |
| Election timeline | Step chart of "who is leader" over time | Each failover marked with the algorithm used and the time it took |
| Clock sync panel | Diverging bar chart | Each node's offset before and after a Berkeley round (Exp 3) |
| Replication panel | Replica table + lag chart | Per-replica version, sequence lag, consistency mode; stale reads highlighted (Exp 5) |

### 15.4 Tasks page

| Widget | Visualization | What it shows |
| --- | --- | --- |
| Task table | Virtualized table, live updating | ID, type, profile, priority, deadline, status, worker, wait, execution time, retries; filter by any column |
| Status funnel | Funnel or stacked bar | Queued → running → completed / failed / cancelled |
| Task detail drawer | Slide-over panel | Full lifecycle timeline, Lamport timestamps, trace ID, retry history, result, and the decision explainer |
| Decision explainer | Horizontal bar chart per worker | The score breakdown for the chosen task: predicted wait + predicted execution + overload penalty, with the winner highlighted (F13) |
| DAG view | Node graph with status colours | Workflow tasks, showing which branch is blocking (F1) |
| Latency distribution | Histogram + CDF | Where the tail lives, per task type |
| Dead-letter queue | Table with a retry button | Tasks that exhausted their retries (F4) |

### 15.5 Predictions page

This is the page that carries the project's research claim.

| Widget | Visualization | What it shows |
| --- | --- | --- |
| Predicted vs actual | Scatter plot with a y = x reference line | Every completed task; tight clustering on the diagonal means good predictions |
| Error over time | Line chart of rolling MAE | Whether accuracy is holding, with the drift threshold drawn in |
| Queue forecast | Dual line chart with a shaded confidence band | Forecast horizon vs what actually happened |
| Overload risk | Worker × time risk heatmap (green → red) | Which worker the model expects to saturate next |
| Feature importance | Horizontal bar chart | Which inputs drive the execution-time model |
| Live vs shadow model | Side-by-side accuracy cards | Shadow model performance before promotion (F14) |
| Cold-start monitor | Table of task types with prediction confidence | Newly seen task types and how quickly accuracy improves |
| Model registry | Version table | Version, trained date, metrics, status (live / shadow / retired), with a promote button |

### 15.6 Benchmarks page

| Widget | Visualization | What it shows |
| --- | --- | --- |
| Strategy comparison | Grouped bar chart | Mean latency, P95, throughput, SLA compliance for all five strategies |
| Latency CDF overlay | Multi-line CDF | The clearest way to show tail-latency improvement |
| Radar chart | 5-axis spider per strategy | Latency, throughput, balance, SLA, overhead in one glance |
| Imbalance chart | Box plot of per-worker load | How evenly each strategy spreads work |
| Run history table | Sortable table | Every benchmark run with its scenario, config, and results |
| What-if simulator | Replay controls with a live-updating chart | Replay a saved workload against any strategy at 10× speed (F17) |
| Export | Button | Generates the HTML/PDF report with all charts (F18) |

### 15.7 Chaos page

| Control | Effect |
| --- | --- |
| Kill worker | Stops a worker container; watch reassignment |
| Kill primary | Triggers election and backup promotion; watch the election timeline |
| Inject latency | Adds artificial network delay to one node |
| Spike CPU | Loads a worker so overload prediction fires |
| Partition node | Isolates a replica to demonstrate eventual consistency |
| Drain worker | Graceful removal: finish current work, accept no new tasks |
| Submit burst | Fires a configurable burst of tasks to trigger auto-scaling |

Each action writes a marker onto every time-series chart, so cause and effect line up visually.

### 15.8 Logs page

Unified event stream with a trace-ID search box, node and event-type filters, and a **Lamport view** that reorders events by logical clock instead of wall clock, side by side with the physical ordering. Seeing the two differ is the clearest demonstration of Experiment 3.

### 15.9 Cross-cutting UI features

- **Live updates over WebSocket**, with automatic reconnect and a visible connection indicator.
- **Time-range selector** (last 1m / 5m / 15m / whole run) applied to every chart on the page.
- **Pause / scrub**: freeze the live view and scrub back through buffered history.
- **Demo mode**: a scripted sequence that submits a workload, kills the primary, spikes a worker, and triggers auto-scaling, with on-screen captions. Removes all live-demo risk.
- **Dark mode**, responsive layout, and a projector-friendly high-contrast theme.
- **Empty and error states** on every panel, so the UI degrades gracefully when the cluster is down.
- **Keyboard shortcuts** for switching pages and pausing the queue.

### 15.10 API endpoints behind the dashboard

```
GET  /api/overview                 KPI strip + topology snapshot
GET  /api/workers                  worker list with live metrics
GET  /api/workers/{id}/history     time series for one worker
GET  /api/tasks?status=&type=      filtered task list
GET  /api/tasks/{id}               detail + decision breakdown + lifecycle
GET  /api/queue/forecast           actual vs predicted queue series
GET  /api/predictions/accuracy     rolling error, scatter data
GET  /api/models                   registry, live/shadow, drift status
POST /api/models/{v}/promote       promote a shadow model
GET  /api/cluster/leader           leader + election history
GET  /api/replication/status       replica versions and lag
GET  /api/clock/offsets            physical clock offsets
GET  /api/benchmarks               run history and results
POST /api/benchmarks/run           start a benchmark run
POST /api/simulate                 what-if replay
POST /api/admin/strategy           switch scheduling strategy
POST /api/admin/drain/{workerId}   drain a worker
POST /api/admin/pause              pause/resume the queue
POST /api/chaos/{action}           chaos controls
WS   /ws/stream                    live events, metrics, task updates
```

### 15.11 Frontend implementation notes

- **React + TypeScript (Vite)** with **TanStack Query** for polling endpoints and a WebSocket hook for the live stream.
- **Recharts** for standard charts; **D3** for the heatmap and Gantt timeline; **React Flow** for the topology and DAG graphs.
- **Tailwind CSS** plus **shadcn/ui** for consistent components with little styling effort.
- Keep a **bounded client-side buffer** (e.g., the last 5 minutes of metrics) so charts stay smooth without re-fetching history.
- **Throttle** WebSocket updates to about 4 per second; a 60 Hz stream will make the browser stutter during a demo.
- Colour-code consistently across the whole app: one colour per worker, one per task status, one per strategy.

### 15.12 Build priority

If the dashboard has to be cut back, build in this order: **KPI strip → worker cards → live queue chart (actual vs predicted) → task table → predicted vs actual scatter → strategy comparison bars → chaos buttons**. Those seven panels alone tell the entire project story.

---

## 16. Deployment

**Local development:** run each node as a separate Java process with different ports:

```
Scheduler nodes: 50051–50055   (IDs 1–5)
Workers:         50061–50063
Prediction:      50070
Spark UI:        4040
Dashboard API:   8080
React dev:       5173
PostgreSQL:      5432
```

**Docker Compose:** one service per node, a `postgres` service, a `prediction` service, `dashboard-api`, and `dashboard`. Worker heterogeneity is simulated with `cpus:` and `mem_limit:` settings.

**Fault demos:** `docker stop scheduler-5` (the primary) to trigger election and backup promotion; `docker stop worker-2` to trigger task reassignment.

---

## 17. Testing Strategy

| Level | Examples |
| --- | --- |
| Unit | Lamport clock rules, task validation, each scheduling strategy, cost function |
| Concurrency | Many clients submitting simultaneously; no duplicate or lost tasks |
| Integration | Client → scheduler → worker end-to-end over gRPC |
| Election | Leader killed; exactly one new leader; simultaneous elections resolve correctly |
| Clock sync | Offsets shrink after a Berkeley round; Lamport ordering preserved across nodes |
| Replication | Strong mode never returns stale reads; eventual mode replicas converge |
| Primary-backup | Primary killed mid-run; zero lost or duplicated tasks |
| Spark / MPI | Spark aggregates match a pandas check; MPI matrix result equals sequential result |
| Fault injection | Worker killed mid-task; task completes elsewhere |
| ML | Model beats baseline on held-out data; server handles bad input |
| Benchmark | Repeatable results within acceptable variance |

---

## 18. Suggested Timeline

| Phase | Weeks | Deliverable |
| --- | --- | --- |
| Foundation | 1–3 | Exp 1–3: RPC, thread pools, logical + physical clock sync |
| Coordination | 4–5 | Exp 4–5: Bully / Ring election, replication and consistency models |
| Distribution | 6–8 | Exp 6–8: load balancing, Spark MapReduce, primary-backup fault tolerance |
| Parallelism | 9–10 | Exp 9–10: MPI collectives, MPI matrix multiplication |
| Data | 11 | Workload generator, dataset collection, PostgreSQL |
| ML | 12–13 | Train/evaluate models, prediction server |
| Integration | 14 | Predictive strategy, fallback, logging |
| Evaluation | 15 | Full benchmark suite and analysis |
| Presentation | 16 | Dashboard, auto-generated report, demo video |

Feature work is layered on top rather than squeezed in at the end: build **F2, F4, F5, F8, F10** alongside Experiments 1–3; **F1, F7, F9** alongside Experiments 5–6; **F11, F16** alongside Experiment 8; and **F12–F15, F17, F18** during the ML and evaluation phases.

---

## 19. Risks and Limitations

- ML predictions can be wrong, and a poor model can make scheduling worse than a simple baseline.
- Prediction calls add overhead to each scheduling decision.
- Historical data may not capture sudden, unseen spikes.
- New task types face a cold-start problem (mitigated by falling back to type-average estimates).
- Monitoring and heartbeats add network and CPU overhead.
- A single active coordinator can become a throughput bottleneck.
- Workload patterns drift, so models may need periodic retraining.
- Simulating a cluster on one laptop limits how realistic the results are.
- Benefits must be shown through controlled benchmarks; they cannot be claimed without them.

These limitations are part of the evaluation and future work, not something to hide.

## 20. Future Work

- Online learning / periodic automatic retraining
- Reinforcement-learning scheduler
- Auto-scaling workers on Kubernetes
- Consensus-based replication (Raft) replacing primary-backup
- Distributed mutual exclusion (Ricart–Agrawala) for shared resources
- Deadline-aware and energy-aware scheduling
- Real cluster traces (e.g., public datacenter workload traces) for validation

---

## 21. Resume Description

**PrediSched: Predictive Distributed Task Scheduler** | Java, gRPC, Python, XGBoost, Apache Spark, MPI, PostgreSQL, Spring Boot, React, Docker

- Built an ML-assisted distributed task scheduler with gRPC communication, thread-pooled workers, Lamport and Berkeley clock synchronization, Bully/Ring coordinator election, and replicated scheduler state with strong and eventual consistency modes.
- Trained execution-time, queue-forecast, and overload-risk models on self-generated workload traces and served them over gRPC with a fallback to reactive scheduling.
- Benchmarked predictive scheduling against Round Robin, Random, Least Loaded, and Resource-Aware strategies on latency, throughput, utilization, and SLA violations under steady, bursty, and failure scenarios.
- Implemented primary-backup failover and heartbeat-based task reassignment with zero lost tasks in fault-injection tests.
- Processed execution logs with Spark MapReduce for feature engineering and ran MPI-based parallel matrix multiplication as a benchmark workload.
- Added production-style features including DAG workflows, priority ageing, retries with backoff, result caching, speculative execution for stragglers, predictive auto-scaling, explainable scheduling decisions, and shadow-mode model deployment with drift detection.

*(Replace qualitative claims with your measured numbers once benchmarks are complete, e.g., "reduced P95 latency by X% under bursty load".)*

---

## 22. One-Paragraph Summary

PrediSched is a distributed task scheduler that evolves, experiment by experiment, from simple gRPC task submission into a fault-tolerant, ML-assisted system. Clients submit tasks to an elected primary scheduler (with its state replicated to backups), which dispatches them to multithreaded workers that continuously report metrics. A Python prediction engine forecasts execution time, queue growth, and overload risk, and the predictive scheduler uses those forecasts to place tasks proactively. The project's core contribution is a rigorous benchmark showing whether, and by how much, predictive scheduling outperforms conventional reactive strategies.
