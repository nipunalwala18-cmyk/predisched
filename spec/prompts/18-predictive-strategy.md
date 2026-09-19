# Prompt 18: The predictive scheduling strategy with explainable decisions (spec §12.4, F13)

## Goal

`PredictiveStrategy` picks the worker with the lowest predicted cost, falls back to Least Loaded when
predictions are missing or slow, and records a per-worker score breakdown for every decision so any
placement can be explained. Predicted vs actual execution time is tracked live.

## Read first

- Spec §12.4, §3.4, §4 FR15, FR19, FR32, §5 (Performance), §6 Tier 2 (F13), RULES rule 7

## Build

1. **`PredictiveStrategy implements SchedulingStrategy`** (one class plus helpers in the `strategy`
   package, registered as `predictive`):
   - Builds a `PredictRequest` from the task and every candidate's latest `WorkerInfo` (arrival rate
     from the scheduler's own 10 s window).
   - For each worker: `predicted_wait = pred_queue_len × avg_exec_recent / pool_size`,
     `expected_completion = predicted_wait + pred_exec_ms`,
     `cost = expected_completion × (1 + λ × overload_prob)`. λ and the stricter overload threshold for
     high-priority tasks (priority ≥ `predictive.high.priority`) come from config.
   - Workers whose `overload_prob` is above the threshold are skipped when any other candidate is
     below it.
   - Fallback (FR19): empty result, timeout, or breaker open → delegate to `LeastLoadedStrategy` and
     mark the decision `fallback=true` with the reason.
   - The whole decision, including the prediction call, is timed; the p95 target is < 20 ms.
2. **Explainability (F13)**: `SchedulingDecision` gains, per candidate: predicted wait, predicted
   exec, overload probability, overload penalty, cost, and whether it was skipped and why. Stored in
   `scheduling_decisions` (add a `jsonb breakdown` column with a new migration). CLI:
   `predisched explain <taskId>` prints the breakdown as a table with the winner marked.
3. **Accuracy tracking**: on completion the scheduler joins the actual exec time with the prediction
   used for the chosen worker and updates a rolling MAE (last 500 tasks, per task type too), exposed
   via the event log and the database for the dashboard later.
4. **Tuning λ**: `predisched-benchmark` command `tune-lambda` replays one *training-period* trace (never
   a benchmark trace) for λ in {0, 0.5, 1, 2, 4} and writes `results/lambda-tuning.csv`. Set the
   config default to the best value from that run and note it in the doc.

## Tests

- Cost function unit tests with hand-set predictions, including the overload skip and the stricter
  high-priority threshold.
- Fallback: fake prediction client returning empty, timing out, and breaker open → Least Loaded's
  choice, with `fallback=true` and the right reason.
- Decision timing is recorded; `explain` output contains every candidate.

## Acceptance checks

```bash
python -m predisched_ml.prediction_server --port 50070
```
```bash
java -jar predisched-client/target/predisched-client.jar workload replay <any mixed trace in workloads/campaign/> --strategy predictive
```
```bash
java -jar predisched-client/target/predisched-client.jar explain <any taskId from the replay>
```

Then stop the prediction server mid-replay and paste the log lines showing fallback, plus the decision
latency p50/p95 and the live MAE. Make no claim about predictive being better here; that is prompt 20.

## Docs and commit

- `docs/components/predictive-strategy.md`: cost function, fallback, explain output, λ tuning table.
- Commit: `Scheduling: predictive strategy with fallback and explainable per-worker scores`
