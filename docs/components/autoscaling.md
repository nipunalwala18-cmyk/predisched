# Auto-scaling

Prompt 21, F12, FR41, spec §12.4 (proactive actions). The primary scheduler adds workers when
demand is about to exceed capacity, and removes the ones it added when they sit idle.

## How it decides

`AutoScaler` (`predisched-scheduler/.../autoscale/`) runs on the primary every
`autoscale.intervalMs`. Followers skip every check.

- **Capacity** is the pool slots of the healthy, non-draining workers.
- **Demand** comes from a `Forecaster`, one of two behind the same interface so the benchmark can
  compare them fairly:
  - **reactive**: the scheduler's own queue plus every task outstanding on a worker, as of now;
  - **predictive**: the scheduler's queue, plus each worker's running tasks, plus the M2 queue
    forecast for that worker over the horizon (5 s, the dataset's N). It makes one prediction call
    per check, for a probe task, and falls back to reactive when there is no answer.
- **Scale up** when demand > capacity × `upRatio` (1.0) on **two checks in a row**, and the cluster
  is under `maxWorkers`. Workers still starting count toward the maximum, so a burst cannot
  overshoot.
- **Scale down** when utilisation (busy slots over capacity) stays under `downRatio` (0.3) for
  `coolDownMs`, and the cluster is over `minWorkers`. The scaler takes the most idle worker it
  started itself and drains it: the registry's new `markDraining`, which heartbeats do not undo,
  stops new dispatches. Once its in-flight tasks end, or after `drainTimeoutMs`, it stops the
  worker and drops it from the registry. The next scale-down waits a full cool-down again.
  Workers the scaler did not start are never stopped.
- Every decision is a log line and a `SCALE_UP` or `SCALE_DOWN` event.

**Launchers** implement `WorkerLauncher`:

- `LocalProcess` runs `java -jar` on the worker jar with the scheduler's config, on
  `basePort + n`, with ids `worker-a<n>` and logs in `logs/autoscale/`.
- `Docker` runs `docker run --network host <image>` and removes with `docker stop`. It is untested
  here, because the build machine has no Docker. Prompt 24's compose setup is where it runs.

**Configuration** is the `autoscale:` block (see `configs/benchmark-node.yaml`): `mode`
(none, reactive or predictive), `intervalMs`, `upRatio`, `downRatio`, `coolDownMs`, `minWorkers`,
`maxWorkers`, `drainTimeoutMs`, `launcher`, `poolSize`, `basePort`. The scheduler flag
`--autoscale <mode>` overrides `mode`.

## Live run

The setup: one worker with a pool of 2, the predictive strategy with predictive scaling, and
`predisched chaos burst 400 --profile bursty` (400 tasks submitted in 1.9 s):

```
15:45:45.011 AutoScaler - Scale up: predictive demand 370.2 > capacity 2 x 1.0 on two checks; starting worker-a1 (2 of max 4 workers)
15:45:47.064 AutoScaler - Scale up: predictive demand 306.3 > capacity 6 x 1.0 on two checks; starting worker-a2 (3 of max 4 workers)
15:45:49.128 AutoScaler - Scale up: predictive demand 122.3 > capacity 6 x 1.0 on two checks; starting worker-a3 (4 of max 4 workers)
15:45:58.268 AutoScaler - Scale down: utilisation 0.00 < 0.3 for 8124 ms; draining worker-a1 (0 in flight)
15:45:59.304 AutoScaler - Scale down: stopped worker-a1 after draining (0 in flight left); 2 workers, capacity 10 slots
15:46:07.401 AutoScaler - Scale down: utilisation 0.00 < 0.3 for 8116 ms; draining worker-a2 (0 in flight)
15:46:08.474 AutoScaler - Scale down: stopped worker-a2 after draining (0 in flight left); 1 workers, capacity 6 slots
15:46:16.629 AutoScaler - Scale down: utilisation 0.00 < 0.3 for 8202 ms; draining worker-a3 (0 in flight)
15:46:17.676 AutoScaler - Scale down: stopped worker-a3 after draining (0 in flight left); 0 workers, capacity 2 slots
```

- **Scale-up.** It went to the maximum of 4 workers within 4 s of the burst.
- **Scale-down.** After the burst it scaled back down one worker per cool-down, each drained
  before being stopped.
- **A log fix.** The worker count on the "stopped" lines was one too low: the drained worker had
  already left the snapshot. It has since been fixed to report the workers that remain (2, 1, 1).

## Benchmark: `bursty-autoscale`

`configs/benchmark.yaml` gains a scenario whose runs compare **auto-scaling modes** (`variants`)
instead of strategies:

- 400 bursty tasks at base rate 20/s (seed 2006), starting from one pool-2 worker;
- least-loaded placement in every run;
- `SuitePlan` labels the arms `scale:none`, `scale:reactive` and `scale:predictive`;
- `run-suite` passes `--autoscale <mode>` to the scheduler, stops scaled workers with the
  scheduler's process tree, and counts scale events from its log;
- the summary compares `scale:predictive` with the best other arm.

```
java -jar predisched-benchmark/target/predisched-benchmark.jar run-suite --config configs/benchmark.yaml --scenario bursty-autoscale --reps 3 --suite-id s21-autoscale
```

**Suite `s21-autoscale`**: 9 runs, all 3,600 tasks completed. The report is at
`results/benchmark/s21-autoscale/report.html`. Mean ± std over 3 repetitions:

| Arm | Mean latency (ms) | p95 latency (ms) | Throughput (tasks/s) | Makespan (ms) | Max queue | SLA violations (%) | Scale-ups |
| --- | --- | --- | --- | --- | --- | --- | --- |
| No scaling | 4,940 ± 182 | 13,323 ± 193 | 13.84 ± 0.09 | 28,905 ± 186 | 207 ± 8 | 48.8 ± 2.3 | 0 |
| Reactive scaling | 2,227 ± 211 | 5,159 ± 518 | 18.49 ± 0.33 | 21,643 ± 388 | 131 ± 12 | 51.6 ± 10.1 | 2 |
| **Predictive scaling** | **1,750 ± 238** | **4,209 ± 393** | **19.00 ± 0.58** | 21,062 ± 649 | 109 ± 12 | 28.1 ± 17.0 | 2 |

**Headlines** (predictive vs the best other arm):

- mean latency −21.4 % vs reactive, **p = 0.061**;
- p95 −18.4 %, **p = 0.069**;
- throughput +2.8 %, p = 0.265;
- SLA violations −42.4 % vs no scaling, p = 0.167.

**None of these is significant at 0.05 with 3 repetitions.**

What the numbers show:

- **Either kind of scaling helps a lot.** Both roughly halve mean latency and cut p95 by 60 %
  against no scaling.
- **Predictive looks a little better, but it is not established.** It was ahead in all three
  repetitions, but three is too few to call the difference. More repetitions would settle it.
- **Why the modes can differ at all.** Both scaled up twice per run, so the difference is in
  timing. The M2 forecast is weak, a per-worker mean (see ml-models.md), so the predictive demand
  is mostly the same queue term as the reactive one.
- **Why there are no scale-downs.** Each run ends before the 8 s cool-down completes.

## Tests

`AutoScalerTest`, with a fake launcher, a fake cluster view, scripted forecasts and a fake clock:

- **Scale up only after two high checks in a row.** High, low, high does nothing, and sustained
  demand stops at `maxWorkers`.
- **Scale down only after the cool-down.**
  - Nothing happens before it ends.
  - Then the most idle started worker is drained, and stopped on the next check once it is empty.
  - The next worker waits a full cool-down, and is stopped at the drain timeout even with a task
    left.
  - Down at `minWorkers`, nothing more is removed.
- **Never below `minWorkers`.**
- **Reactive demand** is the scheduler queue plus the in-flight tasks.

`SuiteTest.anAutoscalingScenarioComparesItsVariantsUnderOneStrategy` checks the variant plan.
