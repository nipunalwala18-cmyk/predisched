# Prompt 15 — Benchmark harness and analysis

**Spec sections:** §1.2, §4 (FR16), §5 (Reproducibility), §11
**Depends on:** 14
**Lab topics:** Load balancing (comparative evaluation)
**Commit message:** `Benchmarks: reproducible strategy comparison across scenarios with statistical analysis`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Answer the research question with evidence. Build a
harness that runs every strategy under identical conditions, and an analysis that reports the result
truthfully — including if predictive scheduling does not win.

### Harness (`predisched-benchmark`, command `suite`)

- `configs/benchmark/*.yaml` define a suite: scenarios, strategies, repetitions, cluster profile, trace
  file, warm-up seconds, SLA deadline per task type, λ values.
- Scenarios (spec §11): `steady`, `bursty`, `heterogeneous`, `mixed`, `worker-failure` (kill a worker
  at 40% of the trace using the Prompt 07 fault injection).
- Strategies: round-robin, random, least-loaded, resource-aware, predictive (and predictive with each λ
  in the sweep).
- For every (scenario, strategy, repetition): fresh cluster from the profile, clear worker state, wait
  for all workers to register and clocks to sync, replay the **same saved trace** with a unique
  `run_id`, wait for completion, then stop the cluster. Order of runs is shuffled with a fixed seed to
  avoid time-of-day bias. At least 5 repetitions (spec §11).
- Per run it records into `benchmark_runs` and `results/raw/<suite>/<run_id>.json` every metric in
  §11: mean, P95, P99 latency; throughput; makespan; mean and variance of worker CPU; imbalance
  (std dev of per-worker load); max queue length; SLA violation %; scheduling overhead (decision time
  P50/P95); live prediction MAE and fallback rate (predictive only). Warm-up tasks excluded.
- Resumable: completed runs are skipped when a suite is restarted.

### Analysis (`predisched-benchmark/analysis/analyze.py`)

- Mean ± std dev and 95% confidence intervals per (scenario, strategy) for every metric.
- Paired comparison of predictive vs each baseline per scenario (same trace, paired by repetition):
  percentage change and a Wilcoxon signed-rank p-value. Say "no significant difference" when p ≥ 0.05.
- Charts per scenario: latency CDF, throughput bars with CI whiskers, queue length over time (actual vs
  forecast for predictive), per-worker utilisation, λ sweep line chart.
- Writes `results/<suite>/summary.md` (tables + charts + a plain-language findings section generated
  from the numbers, with no adjectives the numbers don't support) and `summary.csv`.

### Tests

- Metric computation tests on a synthetic run with known answers (P95, imbalance, SLA %, makespan).
- Resume test: an interrupted suite re-runs only missing runs.
- Determinism test: two runs of the same strategy on the same trace with Round Robin produce the same
  task-to-worker assignment.

### Acceptance checks

```bash
java -jar predisched-benchmark/target/predisched-benchmark.jar suite configs/benchmark/smoke.yaml     # 1 rep, short traces
java -jar predisched-benchmark/target/predisched-benchmark.jar suite configs/benchmark/full.yaml      # 5 reps, all scenarios
python predisched-benchmark/analysis/analyze.py results/full
```

### Docs

`docs/components/benchmarks.md` (method, controls, threats to validity); copy the headline table from
`results/full/summary.md` into the root README only after the full suite has run. Update the resume
bullets (spec §19) with measured numbers.
