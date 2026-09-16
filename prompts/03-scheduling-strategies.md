# Prompt 03 — Pluggable scheduling strategies

**Spec sections:** §4 (FR11), §5 (Extensibility), §9 "Exp 6", §11 (strategies list)
**Depends on:** 02
**Lab topics:** Load balancing
**Commit message:** `Scheduling: Round Robin, Random, Least Loaded and Resource-Aware strategies`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Make task placement a pluggable policy. These strategies
are the product's reactive schedulers and, later, the baselines the predictive scheduler is measured
against, so they must be correct and cheap.

### Build (`predisched-scheduler`, package `strategy`)

- `SchedulingStrategy` interface: `String name()`,
  `Optional<WorkerInfo> select(TaskRecord task, List<WorkerInfo> alive)`, and a default
  `void onOutcome(TaskRecord task, WorkerInfo worker, long execMs)` no-op hook (the predictive strategy
  uses it later).
- `RoundRobinStrategy` — `AtomicInteger` cursor over the alive list sorted by id, so worker churn does
  not skip or repeat.
- `RandomStrategy` — `SplittableRandom` seeded from config.
- `LeastLoadedStrategy` — lowest `queueLen + activeThreads`, ties by id.
- `ResourceAwareStrategy` — lowest weighted score
  `wCpu·cpuPct + wMem·memPct + wQueue·(queueLen+activeThreads)/poolSize`, weights from config
  (defaults 0.4 / 0.2 / 0.4), normalised to 0–1.
- `StrategyFactory` reads `settings.strategy` and returns the instance. Unknown names fail at startup.
- The dispatcher records a `SchedulingDecision` (task id, strategy, chosen worker, candidate count,
  decision time in µs) for every placement and keeps it in a bounded in-memory ring (persisted from
  Prompt 08).
- Heartbeat data can be up to a second old, so the scheduler adds a local in-flight counter per
  worker (incremented on dispatch, decremented on result) to the load that Least Loaded and
  Resource-Aware see. Document this choice.
- Admin RPC `SetStrategy(name)` on a new `AdminService` in `proto/admin.proto`, plus CLI
  `predisched admin strategy <name>`, so the benchmark can switch without a restart.

### Tests

- One unit test class per strategy with hand-built `WorkerInfo` lists: Round Robin visits every worker
  once per cycle, even with a worker removed mid-cycle; Random is reproducible for a seed; Least Loaded
  and Resource-Aware pick the expected worker, including ties and an empty list.
- `StrategySwitchIT`: switch strategy at runtime; subsequent decisions carry the new name.

### Acceptance checks

```bash
mvn -q verify
scripts/start-cluster.sh   # 3 workers, one with cpuLimitFactor 0.5
java -jar predisched-benchmark/target/predisched-benchmark.jar strategy-spread --tasks 300
```

`strategy-spread` (added to the benchmark module) runs the same 300-task batch under each strategy
and prints tasks per worker, mean and P95 latency, and load imbalance (std dev of tasks per worker),
written to `results/strategy-spread.csv`.

### Docs

`docs/components/scheduling-strategies.md`; load-balancing row in `docs/LAB-COVERAGE.md` with the table.
