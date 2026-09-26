# Scheduling strategies (lab Exp 6: load balancing)

The scheduler picks the worker for each task through a `SchedulingStrategy` chosen by config:
Round Robin, Random, Least Loaded or Resource-Aware. Every choice is recorded. The four are compared
by replaying one saved trace against the same heterogeneous workers.

Built by `spec/prompts/08-load-balancing.md`. Spec sections: §4 FR11, §5 (Extensibility),
§11 Exp 6, §13, RULES rule 4.

## The interface

```java
public interface SchedulingStrategy {
    String name();
    WorkerInfo select(TaskRecord task, List<WorkerInfo> candidates);
    default Map<String, Double> scores(TaskRecord task, List<WorkerInfo> candidates) { ... }
}
```

Candidates are the healthy workers (heartbeat within `workerStaleAfterMs`) below their outstanding
limit (`poolSize × outstandingPerWorkerFactor`), never an empty list. Each candidate's load is raised
to at least this scheduler's in-flight count on it (`WorkerInfo.withLiveLoad`): heartbeats are up to
a second old, so without this a burst would all go to whichever worker looked idle at the last one.
Draining workers are not excluded yet; draining arrives with the admin API (prompt 22).

A task whose last attempt **timed out or was rejected** skips that worker when another candidate
has room, so a retry is not sent straight back to the same trouble. An ordinary failure (bad input,
executor error) is not the worker's fault, and the strategy decides as usual.

## The four strategies (`predisched-scheduler/.../strategy/`)

| Name | Rule | Ties |
| --- | --- | --- |
| `round_robin` | Next candidate in id order; an `AtomicInteger` cursor. Ignores load. | n/a |
| `random` | Uniform pick from a `Random` seeded by `scheduling.seed` (rule 8); candidates sorted by id first. | n/a |
| `least_loaded` | Lowest `queue_len + active_threads`. Ignores pool size. | lowest id |
| `resource_aware` | Lowest `cpuWeight·cpu%/100 + memWeight·mem%/100 + queueWeight·load/poolSize`; weights 0.4 / 0.2 / 0.4 by default. | lowest id |

`StrategyRegistry.standard()` maps each name to a factory. `scheduling.strategy` (or `--strategy`)
picks one at startup; an unknown name stops the scheduler with the list of valid names. Adding a
strategy is its own class plus one `register(...)` line; `PluggableStrategyTest` proves it by
registering a strategy defined only in the test and dispatching through it. The dispatcher holds
the strategy in an `AtomicReference`, so `setStrategy` swaps it between two dispatches (the admin
API in prompt 22 uses this).

## Decisions

Each choice is a `SchedulingDecision`: task id, strategy, chosen worker, per-candidate scores
(empty for round robin and random) and the time `select` took in µs. The last 1000 are kept in a
`DecisionLog` ring buffer, and each is written to the event log as `SCHEDULE_DECISION`:

```
{"node":"scheduler-1","lamport":35,"physical_ms":1790448437025,"wall_ms":1790448436985,
 "type":"SCHEDULE_DECISION","task_id":"task-dde4cdf2","trace":"","candidates":"1",
 "worker":"worker-1","scores":"worker-1=0.000","decision_us":"765","strategy":"least_loaded"}
```

(A live scheduler started with `--strategy least_loaded` and one worker; the first decision after
start-up includes class loading, hence 765 µs.)

## Config

```yaml
scheduling:
  strategy: "round_robin"   # round_robin | random | least_loaded | resource_aware
  seed: 42
  cpuWeight: 0.4
  memWeight: 0.2
  queueWeight: 0.4
```

## How to run and demo

```bash
java -jar predisched-client/target/predisched-client.jar workload generate --profile mixed --pattern steady --tasks 400 --seed 7
python scripts/mock-http.py --port 8100          # HTTP_TASK needs it
java -jar predisched-benchmark/target/predisched-benchmark.jar strategy-compare --trace workloads/mixed-steady-7.jsonl
java -jar predisched-scheduler/target/predisched-scheduler.jar --strategy least_loaded   # live
```

## Real output

`strategy-compare` replays `workloads/mixed-steady-7.jsonl` (400 tasks of all ten types, Poisson
arrivals at 5 tasks/s, seed 7) against an in-process scheduler and three real workers with pools of
2, 4 and 8 threads, once per strategy with the same trace and seed. Workers register and heartbeat
through the real `RegistryService` every 250 ms. Latency is submit to completion, accurate to the
20 ms replay poll; imbalance is the standard deviation of tasks per worker. All three workers share
one JVM, so their CPU and memory readings are the same process-wide numbers: the resource-aware
score is driven by its load term here. `results/exp6-strategies.csv`:

| Strategy | w1 (pool 2) | w2 (pool 4) | w3 (pool 8) | Imbalance | Mean latency ms | p95 ms | Tasks/s | Makespan ms | Decision µs | Failed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| round_robin | 134 | 133 | 133 | 0.47 | 166.2 | 552 | 5.06 | 79065 | 67.3 | 0 |
| random | 133 | 126 | 141 | 6.13 | 149.1 | 549 | 5.06 | 79096 | 49.0 | 0 |
| least_loaded | 220 | 127 | 53 | 68.32 | 148.3 | 539 | 5.06 | 79065 | 28.2 | 0 |
| resource_aware | 176 | 135 | 89 | 35.54 | 146.6 | 512 | 5.06 | 79084 | 21.2 | 0 |

At 5 tasks/s the 14 worker threads are mostly idle: throughput equals the arrival rate and the
makespan is the trace's length for every strategy, and mean latencies are within 20 ms of each
other. `least_loaded` sent 220 tasks to the 2-thread worker because at this load every worker is
often at zero, and the tie goes to the lowest id.

The same trace replayed 4x faster (`--speed 4`, about 20 tasks/s), `results/exp6-strategies-speed4.csv`:

| Strategy | w1 (pool 2) | w2 (pool 4) | w3 (pool 8) | Imbalance | Mean latency ms | p95 ms | Tasks/s | Makespan ms | Decision µs | Failed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| round_robin | 110 | 133 | 157 | 19.19 | 431.9 | 1417 | 20.09 | 19914 | 54.2 | 0 |
| random | 116 | 129 | 155 | 16.21 | 214.8 | 663 | 20.05 | 19952 | 33.2 | 0 |
| least_loaded | 159 | 139 | 102 | 23.61 | 174.1 | 569 | 19.95 | 20051 | 22.7 | 0 |
| resource_aware | 76 | 129 | 195 | 48.68 | 184.1 | 607 | 20.05 | 19955 | 16.3 | 0 |

What the numbers show, without ranking the strategies:

- **Distribution.** Round robin spreads tasks most evenly at low load (imbalance 0.47), but even it
  sends fewer to the 2-thread worker at 20 tasks/s (110): that worker hits its outstanding limit
  (pool × 2 = 4) and drops out of the candidate list. Resource-aware sent the most to the 8-thread
  worker (195), since it divides load by pool size.
- **Latency.** At 20 tasks/s the round-robin mean was 431.9 ms and p95 1417 ms, against 174.1 and
  569 ms for least-loaded, 184.1 and 607 for resource-aware, and 214.8 and 663 for random.
- **Throughput and makespan** stay equal to the arrival rate in both runs: neither load level
  saturates the 14 threads for long, so the strategies differ in where tasks wait, not in how many
  finish.
- **Decision time** is tens of microseconds for every strategy. The ordering follows run order
  (round robin runs first, while the JIT is still warming up), so it is not a property of the rule.

## Tests

`StrategiesTest`: round robin cycles in id order whatever the list order; random is reproducible for
a seed (in any list order) and reaches every candidate; least-loaded picks the lowest
`queue_len + active_threads` and breaks ties by id; resource-aware computes the documented score
(0.5 and 0.86 in the worked example) and breaks ties by id; every strategy returns a lone candidate.
`StrategyRegistryTest`: the four names, an unknown name fails listing the valid ones, a name cannot be
registered twice. `PluggableStrategyTest`: a strategy defined only in the test is registered and
drives six real dispatches, all recorded as decisions; a retry after a timeout avoids that worker,
after an ordinary failure it does not.
