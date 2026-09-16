# Prompt 14 — Predictive scheduler

**Spec sections:** §1.1, §3.4, §4 (FR14, FR15, FR19), §10.4
**Depends on:** 13
**Lab topics:** Load balancing (the predictive policy)
**Commit message:** `Predictive scheduling: cost-based placement from forecasts, with fallback and proactive actions`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Add the product's headline feature: a strategy that
places each task on the worker predicted to finish it soonest at the least risk.

### Strategy (`predisched-scheduler`, `strategy/PredictiveStrategy`)

- Implements `SchedulingStrategy`. For each task, send one `Predict` call with all alive workers'
  latest state (heartbeat snapshot + local in-flight counts from Prompt 03 + arrival rate).
- Cost per worker (spec §10.4):
  ```
  predicted_wait(w)       = max(0, pred_queue_len(w) - free_threads(w)) × recent_mean_exec(w)
  expected_completion(w)  = predicted_wait(w) + pred_exec(task, w)
  cost(w)                 = expected_completion(w) × (1 + λ × overload_prob(w))
  ```
  Choose the lowest cost; ties broken by lower overload probability, then worker id. λ from config
  (default 1.0), tuned in Prompt 15.
- **Priority:** tasks with priority ≥ `highPriorityThreshold` (default 8) exclude workers whose
  `overload_prob` exceeds `highPriorityMaxRisk` (default 0.3) unless every worker does.
- **Fallback (FR19):** if the prediction client is unavailable, times out, or returns a malformed
  response, delegate to `LeastLoadedStrategy` for that decision and mark the decision `fallback=true`.
  Scheduling never waits longer than the prediction deadline.
- **Cold start:** a task type or worker with fewer than `minHistory` observations uses the type-mean
  estimate for `pred_exec` (spec §17).
- `onOutcome` records predicted vs actual exec time; the scheduler keeps a live rolling MAE per model
  version, exposed through `AdminService.GetStats` and written to `predictions`.
- Every decision persists: strategy, chosen worker, cost of every candidate, fallback flag, decision
  time in µs.

### Proactive actions (behind flags, all default off, spec §10.4)

- `holdLowPriority`: when every worker's `overload_prob` > `holdThreshold`, keep tasks with priority ≤ 3
  in the queue (re-evaluated every 200 ms, bounded by `maxHoldMs` so nothing starves).
- `rebalanceOnSpike`: when the cluster queue forecast exceeds capacity, move not-yet-started tasks from
  the worker with the highest forecast queue back to the scheduler queue (requires a new
  `WorkerService.Revoke(taskId)` that succeeds only for tasks not yet started).
- `scaleSignal`: emit a `SCALE_UP_SUGGESTED` event when the forecast exceeds capacity for 10 s.

### Tests

- `CostFunctionTest`: hand-computed costs for 3 workers, including λ = 0, high overload, ties.
- `PredictiveStrategyTest` with a fake prediction client: picks the lowest cost; high-priority task avoids
  risky workers; timeout → Least Loaded chosen and `fallback=true` within deadline.
- `ColdStartTest`: unseen type uses the type mean.
- `RevokeIT`: revoking a started task fails; a queued one is re-dispatched exactly once.
- `PredictiveIT`: full cluster + prediction server (fixture model) + 200 tasks → all complete; decision
  time P95 < 20 ms (spec §5).

### Acceptance checks

```bash
mvn -q verify
scripts/start-cluster.sh --profile heterogeneous-3 --with-prediction
java -jar predisched-client/target/predisched-client.jar admin strategy predictive
java -jar predisched-client/target/predisched-client.jar workload replay workloads/bursty-42.jsonl --run-id predictive-smoke
java -jar predisched-client/target/predisched-client.jar admin stats
# stop the prediction server mid-run: decisions switch to fallback, nothing fails
```

### Docs

`docs/components/predictive-scheduler.md`: the cost function, fallback behaviour, proactive actions, and
the dispatch sequence diagram from spec §3.4.
