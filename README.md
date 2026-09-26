# PrediSched

PrediSched is a distributed task scheduler that evolves, experiment by experiment, from simple gRPC task submission into a fault-tolerant, ML-assisted system. Clients submit tasks to an elected primary scheduler (with its state replicated to backups), which dispatches them to multithreaded workers that continuously report metrics. A Python prediction engine forecasts execution time, queue growth, and overload risk, and the predictive scheduler uses those forecasts to place tasks proactively. The project's core contribution is a rigorous benchmark showing whether, and by how much, predictive scheduling outperforms conventional reactive strategies.

## Quick start

Placeholder — full start instructions arrive with the first runnable prompts.

```bash
mvn -q verify
```

See `spec/PROJECT-CONTEXT.md` for the full specification and `spec/prompts/` for the numbered build prompts.

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
| 10 | Primary-backup fault tolerance | [ ] |
| 11 | PostgreSQL persistence and result cache | [ ] |
| 12 | Spark MapReduce | [ ] |
| 13 | MPI collectives | [ ] |
| 14 | MPI matrix multiplication | [ ] |
| 15 | Dataset collection | [ ] |
| 16 | ML models | [ ] |
| 17 | Prediction server | [ ] |
| 18 | Predictive strategy | [ ] |
| 19 | Speculative execution and chaos | [ ] |
| 20 | Benchmark harness and report | [ ] |
| 21 | Model lifecycle and auto-scaling | [ ] |
| 22 | Dashboard API | [ ] |
| 23 | Dashboard UI | [ ] |
| 24 | Deployment and final report | [ ] |
