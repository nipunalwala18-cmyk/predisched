# Prompt 08: Pluggable load-balancing strategies (Exp 6)

## Goal

The scheduler picks a worker through a `SchedulingStrategy` selected by config, with Round Robin,
Random, Least Loaded and Resource-Aware implemented. Every decision is recorded. A measured comparison
of task distribution, latency and imbalance across the four strategies exists. This is lab Exp 6.

## Read first

- Spec §4 FR11, §5 (Extensibility), §11 Exp 6, §13 (metrics), RULES rule 4

## Build

1. **`predisched-scheduler`**, package `strategy`:
   - `SchedulingStrategy`: `WorkerInfo select(TaskRecord task, List<WorkerInfo> candidates)` and
     `String name()`. Candidates are healthy, non-draining workers with pool capacity.
   - `RoundRobinStrategy` (`AtomicInteger` cursor), `RandomStrategy` (seeded `Random` from config),
     `LeastLoadedStrategy` (lowest `queue_len + active_threads`, ties by id),
     `ResourceAwareStrategy` (weighted score of CPU %, memory %, queue length relative to pool size;
     weights in config).
   - `StrategyRegistry`: name → factory, filled once; config picks `scheduling.strategy`. Changing
     strategy at runtime is a single `AtomicReference` swap (used by the admin API in prompt 22).
   - `SchedulingDecision` record: task id, strategy, chosen worker, per-candidate scores, decision
     time in µs. Logged to the `EventLog` and kept in a bounded in-memory ring buffer.
   - Timed-out and rejected dispatches now retry on a different worker (finishes the TODO from
     prompt 04).
2. **`predisched-benchmark`**: `StrategyCompare` command.
   - Replays one saved trace (prompt 05) against an in-process cluster of 3 heterogeneous workers
     (pool sizes 2, 4, 8), once per strategy, same seed.
   - Metrics per strategy: tasks per worker, mean and p95 latency, throughput, makespan, worker
     imbalance (std dev of tasks per worker), mean decision time.
   - Writes `results/exp6-strategies.csv` and prints a comparison table.

## Tests

- One unit test per strategy with hand-built `WorkerInfo` lists, including ties and a single
  candidate.
- `StrategyRegistryTest`: unknown name fails at startup with the list of valid names.
- A test that adding a strategy needs no change outside its own class and the registry entry
  (register a fake strategy in the test and dispatch through it).

## Acceptance checks

```bash
java -jar predisched-client/target/predisched-client.jar workload generate --profile mixed --pattern steady --tasks 400 --seed 7
```
```bash
java -jar predisched-benchmark/target/predisched-benchmark.jar strategy-compare --trace workloads/mixed-steady-7.jsonl
```

Paste the comparison table. Do not describe any strategy as "best"; state the numbers.

## Docs and commit

- `docs/components/strategies.md`: the interface, each strategy's rule, the measured table.
- `docs/LAB-COVERAGE.md` Exp 6 row.
- Commit: `Scheduling: Round Robin, Random, Least Loaded and Resource-Aware strategies`
