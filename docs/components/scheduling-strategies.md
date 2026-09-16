# Scheduling strategies (Prompt 03)

Task placement is a pluggable policy: one class implementing `SchedulingStrategy`,
selected by config, switched at runtime. These are the product's reactive
schedulers and later the baselines the predictive scheduler is measured against.

## Strategies (`predisched-scheduler/strategy`)

All implement `SchedulingStrategy`: `name()`, `Optional<WorkerInfo>
select(task, alive)`, default no-op `onOutcome` (used by the predictive strategy
in Prompt 14).

- `RoundRobinStrategy` — `AtomicInteger` cursor over the alive list sorted by id,
  so worker churn cannot skip or repeat.
- `RandomStrategy` — `SplittableRandom` seeded from config (`seed`), reproducible.
- `LeastLoadedStrategy` — lowest `queueLen + activeThreads`, ties by id.
- `ResourceAwareStrategy` — lowest `wCpu·cpuPct/100 + wMem·memPct/100 +
  wQueue·(queueLen+activeThreads)/poolSize`, weights from `settings`
  (`wCpu`/`wMem`/`wQueue`, defaults 0.4/0.2/0.4). Each term is clamped to 0–1
  (missing readings count as 0, queue load saturates at one full pool); ties by id.
- `StrategyFactory` reads `settings.strategy` (`round-robin`, `random`,
  `least-loaded`, `resource-aware`). Unknown names fail at startup — the
  scheduler refuses to start rather than run the wrong policy.

## Load visibility

Heartbeat snapshots can be a second old, so the dispatcher keeps a local
in-flight counter per worker (`InFlight`: incremented on dispatch, decremented on
the result) and folds it into the `queueLen` copy the strategies see. Strategies
therefore account for dispatched-but-unacknowledged tasks; no service code touches
clocks or counters by hand — `Dispatcher.adjustedAlive()` builds the copies.

Every placement records a `SchedulingDecision` (task id, strategy, chosen worker,
candidate count, decision time in µs) in a bounded in-memory ring (`DecisionLog`,
1,000 entries; persisted from Prompt 08).

## Runtime switching

`AdminService` (`proto/admin.proto`): `SetStrategy(name)` → `Ack`. The scheduler
serves it; `predisched admin strategy <name>` calls it; unknown names return
`ok=false` with the valid options. Live check:

```
ok=true least-loaded
ok=false unknown strategy: bogus, expected one of [round-robin, random, least-loaded, resource-aware]
```

## Measured: `strategy-spread` (2026-09-16)

Same 300-task batch (100 × SLEEP 100, 100 × CPU 1000000, 100 × MATRIX 20) under
each strategy on a fresh 3-worker in-process cluster, in
`results/strategy-spread.csv`:

| strategy | w1 | w2 | w3 | mean ms | p95 ms | imbalance |
| --- | --- | --- | --- | --- | --- | --- |
| round-robin | 100 | 100 | 100 | 2080 | 3535 | 0.00 |
| random | 105 | 88 | 107 | 551 | 930 | 8.52 |
| least-loaded | 97 | 108 | 95 | 535 | 934 | 5.72 |
| resource-aware | 288 | 9 | 3 | 1289 | 2459 | 132.96 |

Two honest findings, both mechanism-verified, not noise:

1. **Round-robin balances counts, not work.** The batch repeats
   sleep,cpu,matrix with period 3 and so does the 3-worker rotation, so w1
   receives all 100 sleeps while w2/w3 get only fast tasks. Equal counts
   (100/100/100, imbalance 0.00) with 4× worse mean latency — periodic workloads
   are round-robin's worst case.
2. **Resource-aware herds on stale snapshots.** The benchmark uses static
   snapshots (no heartbeats in-process: w1 idle, w2 warm, w3 hot). The queue
   penalty saturates at one full pool while the base-score gap persists, so once
   all workers saturate, w1 (0.48) still beats w2 (0.68) and w3 (0.86) on every
   decision: 288/9/3. With live heartbeats the snapshots would move and the herd
   would rebalance — which is exactly why Prompt 02's heartbeat path exists.

## Design choices

- The saturation marker (`"worker saturated"`) stays a string literal in the
  dispatcher: the scheduler never imports worker classes (module ownership), they
  talk gRPC only.
- `RotatingStrategy` (Prompt 02's placeholder) is deleted; `RoundRobinStrategy`
  is its permanent replacement. All in-process harnesses construct it directly.
- `strategy-spread` lives in the benchmark module next to `pool-scaling` and is
  dispatched through `BenchmarkMain`; both shaded jars need
  `ServicesResourceTransformer` (see `docs/components/task-api.md`).
