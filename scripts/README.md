# scripts — start-cluster and run-benchmark helpers, added as prompts need them.

- `start-cluster`, `stop-node`, `restart-node`, `kill-primary` (`.sh` / `.ps1`): local clusters (prompts 06, 10).
- `run-spark` (`.sh` / `.ps1`): run a Spark job natively or in Docker (prompt 12, `docs/components/spark.md`).
- `run-mpi` (`.sh` / `.ps1`): run an MPI program on N ranks, or on each of 1, 2, 4, natively or in Docker (prompts 13-14, `docs/components/mpi.md`).
- `collect-dataset.py`: the ML dataset campaign, every profile x pattern x strategy run on heterogeneous workers, resumable (prompt 15, `docs/components/dataset.md`).
- `plot-matmul.py`: Exp 10 speedup table and chart from `results/exp10-matmul.csv` (prompt 14).
- `merge-events.py`, `mock-http.py`: event-log merge (prompt 03) and a local HTTP target (prompt 05).
- `make-report.py`: HTML/PDF report of a benchmark suite from its runs.csv and summary.csv (prompt 20, `docs/components/benchmark.md`).
- `export-exec-samples.py`: execution-time samples for the what-if simulator (prompt 20).
