# scripts — start-cluster and run-benchmark helpers, added as prompts need them.

- `start-cluster`, `stop-node`, `restart-node`, `kill-primary` (`.sh` / `.ps1`): local clusters (prompts 06, 10).
- `run-spark` (`.sh` / `.ps1`): run a Spark job natively or in Docker (prompt 12, `docs/components/spark.md`).
- `run-mpi` (`.sh` / `.ps1`): run an MPI program on N ranks natively or in Docker (prompt 13, `docs/components/mpi.md`).
- `merge-events.py`, `mock-http.py`: event-log merge (prompt 03) and a local HTTP target (prompt 05).
