# PrediSched

PrediSched is a distributed task scheduler that evolves, experiment by experiment, from simple gRPC task submission into a fault-tolerant, ML-assisted system. Clients submit tasks to an elected primary scheduler (with its state replicated to backups), which dispatches them to multithreaded workers that continuously report metrics. A Python prediction engine forecasts execution time, queue growth, and overload risk, and the predictive scheduler uses those forecasts to place tasks proactively. The project's core contribution is a rigorous benchmark showing whether, and by how much, predictive scheduling outperforms conventional reactive strategies.

## Quick start

Placeholder — full start instructions arrive with the first runnable prompts.

```bash
mvn -q verify
```

See `spec/PROJECT-CONTEXT.md` for the full specification and `spec/prompts/` for the numbered build prompts.

## Results

Benchmark suite **`s20-20260927`** (prompt 20; method, full tables and the simulator in
[docs/components/benchmark.md](docs/components/benchmark.md), charts in
[results/benchmark/s20-20260927/report.html](results/benchmark/s20-20260927/report.html)):

- **Setup**: 5 strategies × 5 scenarios × 5 repetitions, 125 runs. Each run is a fresh cluster (one
  scheduler, three workers) on one 4-core laptop, replaying identical saved traces the models never
  saw. All 30,000 tasks completed.
- **Comparison**: predictive against the best reactive baseline in each scenario, two-sided Welch
  t-test.

| Scenario | Best baseline (mean latency) | Predictive | Difference | p |
| --- | --- | --- | --- | --- |
| steady (homogeneous workers) | round robin 343 ms | 368 ms | +7.4 % | 0.712 (no significant difference) |
| bursty | resource-aware 87 ms | 98 ms | **+12.9 % (worse)** | 0.012 |
| heterogeneous workers | resource-aware 171 ms | 167 ms | −2.2 % | 0.694 (no significant difference) |
| mixed long-tail tasks, heterogeneous workers | random 2,225 ms | 1,121 ms | **−49.6 % (better)** | < 0.001 |
| worker failure mid-run | resource-aware 160 ms | 207 ms | +29.5 % | 0.270 (no significant difference) |

**Where predictive wins.** Task sizes vary widely on uneven workers ("mixed"). Predicting each
task's execution time on each worker roughly halves the mean latency, cuts p95 by 25 % and cuts
SLA violations from 58 % to 18 %.

**Where it doesn't.** In the other scenarios it is statistically tied with the best reactive
strategy, or worse on bursty traffic of short tasks. There, its 7.7 ms prediction call per decision
(against about 0.1 ms for the reactive strategies) is a real cost.

## Progress

| # | Prompt | Status |
| --- | --- | --- |
| 00 | Bootstrap | [x] |
| 01 | Task API over gRPC | [x] |
| 02 | Workers and thread pools | [x] |
| 03 | Clocks and tracing | [x] |
| 04 | Queue discipline | [x] |
| 05 | Task catalogue and workload generator | [x] |
| 06 | Leader election | [x] |
| 07 | Replication and consistency | [x] |
| 08 | Load-balancing strategies | [x] |
| 09 | Workflows and client auth | [x] |
| 10 | Primary-backup fault tolerance | [x] |
| 11 | PostgreSQL persistence and result cache | [x] |
| 12 | Spark MapReduce | [x] |
| 13 | MPI collectives | [x] |
| 14 | MPI matrix multiplication | [x] |
| 15 | Dataset collection | [x] |
| 16 | ML models | [x] |
| 17 | Prediction server | [x] |
| 18 | Predictive strategy | [x] |
| 19 | Speculative execution and chaos | [x] |
| 20 | Benchmark harness and report | [x] |
| 21 | Model lifecycle and auto-scaling | [x] |
| 22 | Dashboard API | [x] |
| 23 | Dashboard UI | [x] |
| 24 | Deployment and final report | [ ] |
