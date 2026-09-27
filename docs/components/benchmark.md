# Benchmark: strategy comparison, what-if simulator, report

Prompt 20, spec §13, FR16, FR40, F17, F18. One command runs the full comparison: five strategies ×
five scenarios × five repetitions on identical saved traces, each run on a fresh cluster. A second
command turns the results into an HTML and PDF report. A discrete-event simulator answers
"what if" questions without a cluster.

**Suite `s20-20260927`**: 125 runs, all 30,000 tasks completed. The results are in
`results/benchmark/s20-20260927/`: [report.html](../../results/benchmark/s20-20260927/report.html),
[report.pdf](../../results/benchmark/s20-20260927/report.pdf), `runs.csv`, `summary.csv`, and one
replay CSV per run in `runs/`.

## Method and controls

**Traces.** `configs/benchmark.yaml` lists the scenarios. Their traces were generated once into
`workloads/benchmark/` with seeds 2001–2005. The dataset campaign used seeds 1500–1517 and λ
tuning used 1506, so neither the models nor λ ever saw these traces.
`workloads/benchmark/manifest.txt` holds each trace's SHA-256, and every run checks it: the traces
are replayed, never regenerated.

| Scenario (spec §13) | Profile | Pattern | Rate | Tasks | Workers | Fault |
| --- | --- | --- | --- | --- | --- | --- |
| steady | mixed | steady | 20/s | 240 | homogeneous: 3 × pool 4 | |
| bursty | bursty | bursty | 12/s | 240 | homogeneous | |
| heterogeneous | mixed | steady | 15/s | 240 | heterogeneous: pools 2/4/8, worker-1 at 2x slowdown | |
| mixed (task types across all profiles) | long_tail | periodic | 15/s | 240 | heterogeneous | |
| failure | mixed | steady | 15/s | 240 | heterogeneous | worker-2 crashed through the chaos API 6 s into the replay |

**Runs.** Each run is `predisched-benchmark run-suite`, as follows:

- **Fresh cluster.** It starts a fresh cluster as child processes: one scheduler on
  `configs/benchmark-node.yaml` with `--strategy <s>`, and three workers with the scenario's pools
  and slowdowns. The node config has:
  - PostgreSQL history on;
  - the result cache off, so every task executes;
  - speculation off, so only placement differs between strategies;
  - the chaos API on, for the failure scenario.
- **Replay and shutdown.** It waits for three healthy workers, replays the trace with the client
  library, collects the metrics, and stops the cluster.
- **Shared services.** The prediction server (no database logging) and the mock HTTP target run
  once for the whole suite.

**Order.** The order is repetition-major: every scenario and strategy runs once before any runs
twice. Within each repetition the strategy order rotates by one, so machine drift (heat,
background load) spreads over all strategies.

**Warm-up.** The first 10 % of each trace's tasks are left out of the latency percentiles.

**Deadline.** Every task carries a 2 s deadline, and a task that fails or finishes later is an SLA
violation.

**Resuming.** `run-suite` is resumable: runs already in `runs.csv` are skipped.

**Metrics per run.** These follow the spec §13 table and are written to `runs.csv` and to the
`benchmark_runs` table (scenario `benchmark:<suite>:<scenario>`):

- **Latency** (mean, p95, p99): submit to completion as the client sees it. Status is polled every
  50 ms.
- **Throughput and makespan**: completed tasks over first submit to last completion.
- **CPU**: `worker_metrics` over the run's window, the mean across workers and the variance
  between them.
- **Imbalance**: the population standard deviation of completed tasks per worker, idle workers
  included.
- **Max queue**: the most tasks waiting at once (submitted, not yet started).
- **SLA violation %**.
- **Scheduling overhead**: mean and p95 `decision_us` from `scheduling_decisions`.
- **Live exec-time MAE**: from `prediction_outcomes`, predictive only.

**Summary.** `summary.csv` holds the mean ± standard deviation per scenario × strategy × metric. For
the predictive strategy, each metric also gets:

- the **best baseline**: the reactive strategy with the best mean in that metric's direction;
- the relative difference;
- a two-sided **Welch t-test** p-value;
- **Cohen's d**.

`SuiteSummary` implements the test with the regularised incomplete beta, and the test is checked
against scipy.

**Rerun:**

```bash
python scripts/mock-http.py --port 8100 &   # optional: run-suite starts it and the prediction server itself
java -jar predisched-benchmark/target/predisched-benchmark.jar run-suite --config configs/benchmark.yaml --reps 5 [--suite-id ID]
java -jar predisched-benchmark/target/predisched-benchmark.jar run-suite --config configs/benchmark.yaml --suite-id ID --summarize
python scripts/make-report.py ID
```

A full suite takes about 50 minutes: 125 runs at about 25 s each.

## Results: suite s20-20260927

Mean ± standard deviation over five repetitions:

| Scenario | Strategy | Mean latency (ms) | p95 latency (ms) | Throughput (tasks/s) | SLA violations (%) | Imbalance |
| --- | --- | --- | --- | --- | --- | --- |
| steady | Round robin | 343 ± 92 | 938 ± 211 | 21.23 ± 0.03 | 0.0 ± 0.0 | 0.0 ± 0.0 |
| steady | Random | 435 ± 150 | 1054 ± 252 | 21.22 ± 0.04 | 0.0 ± 0.0 | 6.2 ± 0.0 |
| steady | Least loaded | 436 ± 102 | 1134 ± 210 | 21.20 ± 0.06 | 0.0 ± 0.0 | 16.6 ± 4.9 |
| steady | Resource-aware | 515 ± 232 | 1158 ± 291 | 21.22 ± 0.03 | 0.0 ± 0.0 | 11.9 ± 3.5 |
| steady | **Predictive** | 368 ± 116 | 925 ± 175 | 21.07 ± 0.27 | 0.0 ± 0.0 | 10.1 ± 9.3 |
| bursty | Round robin | 111 ± 53 | 351 ± 135 | 12.56 ± 0.03 | 0.0 ± 0.0 | 0.0 ± 0.0 |
| bursty | Random | 102 ± 30 | 334 ± 96 | 12.57 ± 0.05 | 0.0 ± 0.0 | 6.2 ± 0.0 |
| bursty | Least loaded | 97 ± 18 | 318 ± 26 | 12.59 ± 0.02 | 0.0 ± 0.0 | 27.3 ± 2.0 |
| bursty | Resource-aware | 87 ± 1 | 289 ± 19 | 12.58 ± 0.01 | 0.0 ± 0.0 | 6.4 ± 3.5 |
| bursty | **Predictive** | 98 ± 6 | 310 ± 7 | 12.59 ± 0.01 | 0.0 ± 0.0 | 82.3 ± 19.0 |
| heterogeneous | Round robin | 214 ± 8 | 635 ± 13 | 14.50 ± 0.02 | 0.0 ± 0.0 | 0.0 ± 0.0 |
| heterogeneous | Random | 250 ± 17 | 750 ± 72 | 14.47 ± 0.03 | 0.0 ± 0.0 | 6.8 ± 0.4 |
| heterogeneous | Least loaded | 190 ± 9 | 596 ± 37 | 14.48 ± 0.02 | 0.0 ± 0.0 | 14.0 ± 3.6 |
| heterogeneous | Resource-aware | 171 ± 9 | 547 ± 55 | 14.51 ± 0.02 | 0.0 ± 0.0 | 37.1 ± 4.8 |
| heterogeneous | **Predictive** | 167 ± 18 | 513 ± 82 | 14.48 ± 0.02 | 0.0 ± 0.0 | 51.9 ± 13.3 |
| mixed | Round robin | 2906 ± 193 | 4057 ± 236 | 12.48 ± 0.20 | 74.0 ± 3.8 | 4.0 ± 0.8 |
| mixed | Random | 2225 ± 234 | 3246 ± 312 | 12.88 ± 0.28 | 58.2 ± 4.9 | 8.9 ± 0.8 |
| mixed | Least loaded | 4383 ± 774 | 6621 ± 1223 | 10.59 ± 0.29 | 85.3 ± 1.7 | 24.1 ± 13.0 |
| mixed | Resource-aware | 3942 ± 419 | 6222 ± 706 | 11.06 ± 0.54 | 82.3 ± 5.7 | 18.6 ± 5.3 |
| mixed | **Predictive** | 1121 ± 164 | 2429 ± 233 | 13.53 ± 0.02 | 18.1 ± 11.2 | 52.1 ± 8.4 |
| failure | Round robin | 271 ± 22 | 799 ± 92 | 13.97 ± 0.03 | 0.2 ± 0.2 | 37.6 ± 0.1 |
| failure | Random | 267 ± 36 | 957 ± 141 | 14.00 ± 0.01 | 1.2 ± 0.9 | 34.9 ± 1.1 |
| failure | Least loaded | 211 ± 50 | 738 ± 303 | 13.97 ± 0.03 | 0.7 ± 1.5 | 35.9 ± 4.9 |
| failure | Resource-aware | 160 ± 47 | 608 ± 271 | 13.99 ± 0.02 | 0.7 ± 1.5 | 64.7 ± 1.9 |
| failure | **Predictive** | 207 ± 74 | 838 ± 432 | 13.97 ± 0.06 | 1.6 ± 2.5 | 69.5 ± 5.8 |

Predictive vs the best baseline, per scenario:

| Scenario | Metric | Best baseline | Baseline | Predictive | Difference | p (Welch) | Cohen's d |
| --- | --- | --- | --- | --- | --- | --- | --- |
| steady | Mean latency (ms) | Round robin | 343 | 368 | +7.4 % | 0.712 | 0.24 |
| steady | p95 latency (ms) | Round robin | 938 | 925 | −1.4 % | 0.918 | −0.07 |
| steady | Throughput (tasks/s) | Round robin | 21.23 | 21.07 | −0.7 % | 0.265 | −0.82 |
| bursty | Mean latency (ms) | Resource-aware | 87 | 98 | **+12.9 %** | **0.012** | 2.69 |
| bursty | p95 latency (ms) | Resource-aware | 289 | 310 | +7.3 % | 0.070 | 1.44 |
| bursty | Throughput (tasks/s) | Least loaded | 12.59 | 12.59 | −0.0 % | 0.551 | −0.40 |
| heterogeneous | Mean latency (ms) | Resource-aware | 171 | 167 | −2.2 % | 0.694 | −0.26 |
| heterogeneous | p95 latency (ms) | Resource-aware | 547 | 513 | −6.3 % | 0.462 | −0.49 |
| heterogeneous | Throughput (tasks/s) | Resource-aware | 14.51 | 14.48 | **−0.3 %** | **0.020** | −1.85 |
| mixed | Mean latency (ms) | Random | 2225 | 1121 | **−49.6 %** | **< 0.001** | −5.46 |
| mixed | p95 latency (ms) | Random | 3246 | 2429 | **−25.1 %** | **0.002** | −2.96 |
| mixed | Throughput (tasks/s) | Random | 12.88 | 13.53 | **+5.0 %** | **0.007** | 3.21 |
| mixed | SLA violations (%) | Random | 58.2 | 18.1 | **−69.0 %** | **< 0.001** | −4.66 |
| failure | Mean latency (ms) | Resource-aware | 160 | 207 | +29.5 % | 0.270 | 0.76 |
| failure | p95 latency (ms) | Resource-aware | 608 | 838 | +38.0 % | 0.347 | 0.64 |
| failure | SLA violations (%) | Round robin | 0.2 | 1.6 | +850 % | 0.270 | 0.81 |

No strategy had any SLA violations in the steady, bursty or heterogeneous scenarios.

**Generated headlines** (`make-report.py`):

- **steady:** no significant difference vs round robin in mean latency (+7.4 %, p = 0.712), p95
  (−1.4 %, p = 0.918) or throughput (p = 0.265).
- **bursty:** predictive's mean latency is **+12.9 % vs resource-aware (p = 0.012, d = 2.69):
  worse**. There is no significant difference in p95 (+7.3 %, p = 0.070) or throughput.
- **heterogeneous:** no significant difference in mean (−2.2 %, p = 0.694) or p95 latency
  (−6.3 %, p = 0.462). Throughput is −0.3 % vs resource-aware (p = 0.020): worse, but by 0.03
  tasks/s at an arrival-bound rate.
- **mixed:** mean latency **−49.6 %** (p < 0.001), p95 **−25.1 %** (p = 0.002), throughput
  **+5.0 %** (p = 0.007) and SLA violations **−69.0 %** (p < 0.001), all vs random: better.
- **failure:** no significant difference. Predictive's mean was +29.5 % vs resource-aware
  (p = 0.270), with a wide spread (± 74 ms).

**What this says.** The honest answer to the research question is that predictive scheduling
helps a lot in one scenario, and not otherwise:

- **Where it wins.** In the mixed scenario, long-tail task sizes run on heterogeneous workers.
  Some tasks take seconds, and worker-1 is twice as slow. There, predicting each task's execution
  time on each worker halves the mean latency and cuts SLA violations from 58 % to 18 %. The
  reactive strategies count tasks, not their size. Least loaded and resource-aware do worst of
  all: they keep sending long tasks to whichever worker looks emptiest, including the slow one.
- **Where it is a tie.** In the other four scenarios, predictive is statistically tied with the
  best baseline.
- **Where it loses.** Its mean latency is significantly worse on bursty traffic (+12.9 % vs
  resource-aware). There the tasks are short and similar, so the 7.7 ms prediction call per
  decision is a real cost.
- **Which baseline is best** depends on the scenario: round robin on steady homogeneous load,
  resource-aware on bursty, heterogeneous and failure, and random on mixed.

**Overhead and prediction error.**

- **Decision time.** Predictive takes 7.7 ms mean and 16.1 ms p95 per decision, against 69–92 µs
  for the reactive strategies. That is inside the 20 ms budget but about 100 times more.
- **Live MAE.** The live exec-time MAE was 22 ms on bursty, 25 on heterogeneous, 42 on steady,
  36 on failure, and 80 on mixed, where the long tail lives.
- **CPU.** Mean worker CPU was 7.1–7.6 % for every strategy. The share is per process on the
  8-thread machine.

## What-if simulator (F17)

`predisched-benchmark simulate --trace … --strategy … [--workers heterogeneous] [--speed 10]` is a
discrete-event simulation (`sim/Simulator.java`) that drives the real `SchedulingStrategy` classes.
Its output starts with `SIMULATION (discrete-event model, not a measurement of the cluster)`.

- **Execution times.** Workers have pool slots and a FIFO queue. A task's execution time is drawn
  from `workloads/benchmark/exec-samples.csv`: the 11,318 completed campaign executions,
  normalised to a worker with no slowdown by `scripts/export-exec-samples.py`. A draw picks among
  the 20 samples of the task's type nearest in input size (log scale), then multiplies by the
  worker's slowdown.
- **Dispatch.** The scheduler side keeps a priority queue and the dispatcher's 2 × pool
  outstanding limit.
- **What strategies see.** Strategies see exact worker state (the real ones see heartbeats), with
  CPU and memory at 0.
- **Overhead.** A flat 30 ms per task stands in for dispatch, the reply and the client's polling.
- **Predictive.** The predictive strategy asks a running prediction server.
- **Determinism.** It is deterministic for a seed. A 240-task trace simulates in about 50 ms.

**Validation** on the heterogeneous trace compares the simulated mean latency (average of seeds
1–3) with the real suite's 5-repetition mean. The stated tolerance is ±25 %:

| Strategy | Real (ms) | Simulated (ms) | Error | Within ±25 % |
| --- | --- | --- | --- | --- |
| Round robin | 214.4 | 165.0 | −23 % | yes |
| Random | 249.6 | 164.6 | −34 % | **no** |
| Least loaded | 190.5 | 162.3 | −15 % | yes |
| Resource-aware | 170.6 | 154.5 | −9 % | yes |
| Predictive | 166.9 | 128.0 | −23 % | yes |

Reading the validation:

- **Ranking holds.** The simulator gets the ranking of the four reactive strategies right:
  resource-aware first, random last.
- **It is optimistic.** It underestimates every strategy. Tasks on one real machine interfere with
  each other in ways the measured samples only partly carry, and the real strategies act on
  heartbeats up to a second old, while the model's strategies see exact state.
- **Use it for ranking, not absolute numbers.** It is good enough to rank strategies and try
  settings quickly, not to quote absolute latencies. Random is outside the tolerance, and
  predictive looks better in the model (−23 %) than on the cluster.

## Report (F18)

`python scripts/make-report.py <suite-id>` renders `report.html` with Jinja2 and matplotlib. It
contains:

- headlines generated from `summary.csv`: a difference is only called one at p < 0.05;
- setup and controls;
- per-scenario tables with the predictive-vs-best-baseline line;
- a latency CDF overlay per scenario;
- throughput and p95 bars with standard deviations;
- queue-over-time lines;
- imbalance box plots;
- a radar chart of normalised latency, throughput, balance, SLA and overhead;
- overhead and MAE tables;
- the limitations of spec §19, plus this setup's own.

The five strategies use categorical slots 1–5 of the reference palette, in a fixed order. The PDF
is printed from the HTML by headless Edge: WeasyPrint, which the prompt names, needs GTK libraries
that Windows does not ship.

## Tests

- `SuiteTest`:
  - metrics on a hand-made results file: warm-up exclusion, mean, p95/p99, makespan, throughput,
    imbalance with an idle worker, SLA % with a failure and a late task, and the queue sweep;
  - percentiles;
  - Welch p-values and Cohen's d against scipy;
  - the summary's direction-aware best baseline;
  - the plan is deterministic, rotates strategies, and has unique run ids and task suffixes per
    suite;
  - traces are generated once, a second `ensure` rewrites nothing, and a tampered trace is refused
    by checksum.
- `SimulatorTest`:
  - a fixed seed gives identical runs and another seed does not;
  - every task gets one decision from the real strategy class, and only workers under 2 × pool are
    candidates;
  - samples follow input size, and slowdown scales execution time.
