# Predictive strategy

Prompt 18, spec §12.4, FR15, FR19, F13. The fifth scheduling strategy, `predictive`, places each
task on the worker with the lowest predicted cost. It asks the prediction server
(`docs/components/prediction-server.md`) once per task, falls back to Least Loaded whenever there
is no answer, and keeps a per-worker breakdown of every decision so any placement can be
explained.

## Cost function

`PredictiveStrategy` (`predisched-scheduler/.../strategy/`) builds one `PredictRequest` per task.
It covers every candidate worker (healthy and under its outstanding limit) with:

- the worker's latest heartbeat: CPU, memory, active threads, queue, recent average;
- its registration: cores, pool size, and the slowdown, which workers now report in
  `RegisterRequest.slowdown`;
- the live load: `concurrent_tasks`;
- the scheduler's own 10 s arrival rate;
- the running mean execution time of this task type on this worker (`ExecStats`, from completed
  attempts).

For each worker `w`:

```
predicted_wait(w)      = pred_queue_len(w) × avg_exec_recent(w) / pool_size(w)
expected_completion(w) = predicted_wait(w) + pred_exec_ms(task, w)
cost(w)                = expected_completion(w) × (1 + λ × overload_prob(w))
```

- **No history yet.** A worker with no recent average (it has not finished a task) uses
  `pred_exec_ms` as its per-task time in the wait term.
- **Overload skip.** A worker whose `overload_prob` is above `overloadThreshold` (0.8) is skipped
  while any other candidate is at or below it. If every worker is above it, nobody is skipped and
  the cost decides.
- **High priority.** Tasks of priority `highPriority` (8) or more use the stricter
  `highPriorityOverloadThreshold` (0.5).
- **Ties** go to the lowest worker id, as in the other strategies.

Config lives under `scheduling:` (see `configs/predictive.yaml`):

- `lambda: 1.0`
- `overloadThreshold: 0.8`
- `highPriority: 8`
- `highPriorityOverloadThreshold: 0.5`

The `prediction:` block is the client (prompt 17): `enabled`, `host`, `port`, `timeoutMs: 10`,
`failureThreshold: 5`, `coolDownMs: 5000`.

## Fallback (FR19)

If the prediction call returns nothing, the strategy delegates to `LeastLoadedStrategy`. It marks
the decision `fallback=true` with the reason, for example `least_loaded: DEADLINE_EXCEEDED`,
`least_loaded: circuit breaker open` or `least_loaded: UNAVAILABLE: io exception`. The same
happens when:

- a worker is missing from the answer;
- the predictor throws;
- `prediction.enabled` is false. The strategy still starts, and every decision says why it fell
  back.

Fallback warnings are logged the first time, whenever the reason changes, and every 100
fallbacks, so an outage does not flood the log.

## Explainability (F13)

**Decision records.** Every decision now carries a list of `CandidateScore`s:

- predicted queue, wait, execution, overload probability, overload penalty and cost;
- whether the candidate was skipped, and why;
- cold start;
- which one was chosen.

The reactive strategies get the same list, with only their own score filled in.

**Storage.** Migration `V3__predictive_decisions.sql` adds `breakdown jsonb`, `fallback`,
`fallback_reason` and `model_versions` to `scheduling_decisions`. The scheduler also keeps the last
10,000 decisions in memory.

**Timing.** The dispatcher times the whole decision, including the prediction call. It logs
p50/p95/p99 every 100 decisions: `Decision latency (predictive, last 100): ...`.

**CLI.** `predisched explain <taskId>` calls the new `ExplainDecision` RPC and prints the table,
with `*` on the winner. `workload replay --strategy <name>` switches the scheduler first, through
the new `SetStrategy` RPC. Only the leader accepts it, and the new strategy stays in place.

```
$ predisched --config configs/predictive.yaml explain "mixed-1507-00050~p18b"
task mixed-1507-00050~p18b: strategy predictive chose worker-2 in 7.75 ms at 2026-09-27T08:28:54.287Z
models: m1=v1,m2=v1,m3=v1
  worker       pred_queue   pred_wait   pred_exec  overload     penalty        cost  note
  worker-1          0.22      26.1 ms     71.2 ms     0.768     74.7 ms    172.0 ms
* worker-2          0.00       0.0 ms     29.1 ms     0.000      0.0 ms     29.1 ms
  worker-3          0.00       0.0 ms     44.0 ms     0.000      0.0 ms     44.0 ms
cost = (pred_wait + pred_exec) x (1 + lambda x overload); lowest wins (* = chosen)
```

Reading it: worker-1, the pool of 2 at 2x slowdown, is predicted slower (71 ms), may have a queue
in 5 s, and has a 77 % overload risk. Its cost doubles to 172 ms. Worker-2 wins on the fastest
predicted execution.

A decision made while the server was down:

```
$ predisched --config configs/predictive.yaml explain "mixed-1507-00120~p18b"
task mixed-1507-00120~p18b: strategy predictive chose worker-2 in 0.08 ms at 2026-09-27T08:28:58.935Z
fallback: least_loaded: circuit breaker open
  worker           score
  worker-1         2.000
* worker-2         0.000
  worker-3         0.000
score: the strategy's own number, lower is better (* = chosen)
```

## Accuracy tracking

When an attempt completes, the dispatcher calls the strategy's new `completed()` hook. If the task
ran on the worker it was predicted for, the absolute error goes into `PredictionAccuracy`: a
rolling MAE over the last 500 tasks, overall and per task type. Tasks placed by the fallback, or
retried elsewhere, are left out.

Each such task produces:

- a `PREDICTION_OUTCOME` event with prediction, actual, error and both MAEs;
- a row in the new `prediction_outcomes` table, for the dashboard (prompt 22) and drift detection
  (prompt 21);
- every 50 tasks, the log line `Live exec-time MAE over the last N predicted tasks: ...`.

## Acceptance run

The setup:

- the prediction server on port 50070;
- a scheduler on `configs/predictive.yaml`, started as `least_loaded`;
- the three campaign workers: pools 2/4/8, slowdown 2/1/1;
- `scripts/mock-http.py` on port 8100.

Then:

```
$ predisched --config configs/predictive.yaml workload replay workloads/campaign/mixed-bursty-1507.jsonl --strategy predictive --id-suffix "~p18b"
strategy: least_loaded -> predictive (strategy changed)
...
overall: completed=160 failed=0
```

The prediction server was killed 22 s into the replay, at 13:58:57.497, during the burst. The
scheduler log shows the switch, the live accuracy, the latency, and the fallback taking over with
the breaker opening:

```
13:58:35.391 INFO  SchedulerServiceImpl - SetStrategy predictive: strategy changed (least_loaded -> predictive)
13:58:54.055 INFO  PredictiveStrategy - Live exec-time MAE over the last 50 predicted tasks: 24.0 ms (by type: GRAPH=35.8, FILE_IO=12.8, CPU=0.8, SORT=26.5, COMPRESS=19.3, SLEEP=74.2, MATRIX=3.8, HASH=4.6, HTTP=33.9, MONTE_CARLO=74.6)
13:58:57.424 INFO  PredictionClient - Prediction latency (client, last 100 calls): p50=6.32 ms p95=10.25 ms p99=15.60 ms; 100 calls, 0 failures, 0 short-circuited, breaker CLOSED
13:58:57.424 INFO  Dispatcher - Decision latency (predictive, last 100): p50=6.53 ms p95=10.36 ms p99=16.04 ms
13:58:57.514 WARN  PredictiveStrategy - Predictive fallback to least_loaded for mixed-1507-00100~p18b (first): UNAVAILABLE: io exception
13:58:57.556 INFO  PredictiveStrategy - Live exec-time MAE over the last 100 predicted tasks: 20.0 ms (by type: GRAPH=21.7, FILE_IO=12.0, CPU=0.8, SORT=26.5, COMPRESS=19.1, SLEEP=73.7, MATRIX=2.7, HASH=3.2, HTTP=30.2, MONTE_CARLO=21.3)
13:58:57.770 WARN  PredictiveStrategy - Predictive fallback to least_loaded for mixed-1507-00105~p18b (6 so far): circuit breaker open
```

From the database, over the run's 160 decisions:

| | Decisions | Decision p50 | Decision p95 |
| --- | --- | --- | --- |
| With predictions | 100 | 6.55 ms | 10.37 ms |
| Fallback (server down) | 60 | 0.11 ms | 1.01 ms |

- **Latency.** The p95 with the prediction call is 10.4 ms, inside the 20 ms target. This is with
  the scheduler, three workers, the prediction server and the replay client all on one 4-core
  laptop. The fallback costs nothing measurable.
- **Breaker.** After 5 failures in a row the breaker opened, and the next calls made no network
  call at all. Each 5 s cool-down let one trial through (the `UNAVAILABLE` lines), and each failed
  trial reopened it.
- **Outcome.** All 160 tasks completed. `prediction_outcomes` holds the 100 predicted tasks, with a
  mean absolute error of 20.0 ms.
- **Cold start.** `GRAPH_TASK`, the held-out type, runs on M1's cold-start rule and so shows a
  larger error early on.

This run shows the mechanism works, not that predictive scheduling is better. Prompt 20 makes that
comparison, with repetitions and every strategy on the same traces.

## λ tuning

`predisched-benchmark tune-lambda` replays one training-period trace for each λ in
{0, 0.5, 1, 2, 4}:

- The trace is `workloads/campaign/mixed-steady-1506.jsonl`, 160 tasks at 15/s. It is from the
  dataset campaign, and the command refuses traces from anywhere else, so λ is never tuned on a
  benchmark trace.
- It runs on the in-process cluster of `strategy-compare` with the campaign workers: pools 2/4/8,
  slowdown 2/1/1.
- There are 3 repetitions, and the rows go to `results/lambda-tuning.csv`.

| λ | Mean latency (ms), 3 reps | Std dev | Tasks w1/w2/w3 (rep 1) | Fallbacks per run | Live MAE (ms) |
| --- | --- | --- | --- | --- | --- |
| 0 | 113.3 | 2.5 | 4 / 85 / 71 | 5–10 | 27–35 |
| 0.5 | 114.7 | 3.3 | 4 / 51 / 105 | 4–13 | 33–42 |
| **1** | **111.3** | **2.0** | 7 / 49 / 104 | 5–12 | 30–54 |
| 2 | 112.0 | 3.9 | 8 / 12 / 140 | 10–11 | 32–38 |
| 4 | 112.3 | 4.9 | 5 / 4 / 151 | 5–15 | 25–31 |

What the table shows:

- **λ = 1 has the lowest mean latency**, so it stays the default (`scheduling.lambda: 1.0`).
- **But the differences are within the noise.** The spread between λ values (111–115 ms) is about
  one standard deviation. On this trace, λ mostly moves work between worker-2 and worker-3:
  worker-3's larger pool gives a lower overload risk. It does not change latency, because at
  15 tasks/s neither worker is saturated.
- **Worker-1** (2x slowdown) gets 4–12 tasks whatever λ is: its predicted execution time alone
  already keeps it out.
- **Fallbacks** (4–15 per run of 160) are prediction calls that missed the 10 ms deadline while the
  in-process cluster and the server shared the CPU. The decision p95 in these runs was 9–12.5 ms.

## Tests

- `PredictiveStrategyTest` (10 tests):
  - the cost function on hand-set predictions, with wait, penalty and cost worked out in the test;
  - the no-history wait;
  - λ = 0;
  - the overload skip, and the "all above threshold" case;
  - the stricter high-priority threshold;
  - the request contents: pool, slowdown, concurrency, arrival rate, running means;
  - fallback on an empty result for `DEADLINE_EXCEEDED`, breaker open and `UNAVAILABLE`, each
    giving Least Loaded's choice with `fallback=true` and the reason;
  - fallback on a missing worker, on prediction disabled, and on a throwing predictor;
  - rolling MAE only for the predicted worker;
  - the MAE window.
- `PredictiveDispatchTest`: the strategy inside a real dispatcher and in-process workers.
  - A decision is timed (`decisionMicros > 0`) and has a 3-candidate breakdown.
  - The registered slowdown reaches the prediction request.
  - `explain` output lists every candidate, marks the winner and shows the model versions.
  - The live MAE is updated.
  - With the predictor down, the next decision is a fallback with its reason in `explain`.
  - `SetStrategy` switches strategies and rejects unknown names.
- `StrategyRegistryTest` now expects five strategies.
