# Prompt 20: Benchmark harness, what-if simulator and auto-generated report (spec §13, F17, F18)

## Goal

One command runs the full benchmark of spec §13: five strategies × five scenarios × at least five
repetitions on identical saved traces. It produces CSV results and an HTML/PDF report with the
comparison charts. This is where the research question gets its answer, whatever that answer is.
A what-if simulator replays traces against any strategy without the cluster.

## Read first

- Spec §1.2, §13 (all), §4 FR16, FR40, §6 Tier 2 (F17, F18), §19, RULES rules 7 and 8

## Build

1. **Benchmark traces**: `configs/benchmark.yaml` lists the scenarios (steady, bursty, heterogeneous
   workers, mixed, worker failure mid-run) with trace seeds that were **not** used in dataset
   collection or λ tuning. Traces are generated once into `workloads/benchmark/` and committed.
2. **Harness** (`predisched-benchmark` `run-suite` command):
   - For each scenario × strategy × repetition: fresh cluster via the start scripts (or Docker
     Compose when `--docker`), warm-up period excluded, replay the trace, collect.
   - The failure scenario kills a worker at a fixed offset using the chaos API.
   - Metrics per run (spec §13 table): mean, p95, p99 latency; throughput; makespan; mean and variance
     of CPU utilisation; worker imbalance; max queue length; SLA violation %; scheduling overhead;
     live prediction MAE (predictive only). Rows go to `results/benchmark/<suite-id>/runs.csv` and
     `benchmark_runs`.
   - `summary.csv`: mean ± std per scenario × strategy, and for each metric a Welch t-test p-value and
     effect size of predictive vs the best baseline in that scenario.
3. **What-if simulator (F17)**: `predisched-benchmark simulate --trace … --strategy … --speed 10`: a
   discrete-event simulation of workers (pool slots, exec times sampled from the measured
   distribution per type and worker, or M1 predictions), driving the real `SchedulingStrategy`
   classes. State clearly in the output that it is a simulation. Validate it once: for one trace,
   simulated vs real mean latency within a stated tolerance, reported in the doc.
4. **Report (F18)**: `scripts/make-report.py <suite-id>` (Jinja2 + matplotlib, WeasyPrint for PDF):
   setup and controls; per-scenario tables; latency CDF overlay; throughput bars; queue-over-time
   lines; imbalance box plots; radar chart; overhead and prediction-error section; a limitations
   section from spec §19. Headlines are generated from the numbers: "predictive vs best baseline:
   p95 latency −X % (p = …)" or "no significant difference". Output
   `results/benchmark/<suite-id>/report.html` and `.pdf`.

## Tests

- Metric calculations on a hand-made results file (percentiles, imbalance, SLA %).
- The suite plan is deterministic; traces are replayed, not regenerated (checksum check).
- The simulator with a fixed seed is deterministic and runs the real strategy classes.

## Acceptance checks

```bash
java -jar predisched-benchmark/target/predisched-benchmark.jar run-suite --config configs/benchmark.yaml --reps 5
```
```bash
python scripts/make-report.py <suite-id>
```

Paste `summary.csv` as a table and the generated headlines. Report the result honestly, including
scenarios where predictive loses. Only now may README and docs state a comparison, quoting these
numbers and the suite id.

## Docs and commit

- `docs/components/benchmark.md`: method, controls, how to rerun, the summary table, link to report.
- Root README: a results section quoting the summary with the suite id.
- Commit: `Benchmark: full strategy comparison suite, what-if simulator and generated report`
