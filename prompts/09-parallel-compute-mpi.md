# Prompt 09 — Parallel compute engine (MPI)

**Spec sections:** §4 (FR21), §6 (MPI row), §9 "Exp 9" and "Exp 10", §15 (Spark / MPI)
**Depends on:** 08
**Lab topics:** MPI collectives (Broadcast, Scatter, Gather); Parallel matrix multiplication using MPI
**Commit message:** `Compute: MPI engine runs task batches and parallel matrix multiplication for MATRIX_TASK`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Add a parallel compute engine that workers use for heavy
work. It is a real execution backend in the product, not a side program: a worker can run a
`MATRIX_TASK` through MPI, and the engine can run a whole batch of tasks across ranks.

### Engine (`mpi/`, Python + mpi4py + NumPy)

- `mpi/predisched_mpi/collectives.py` — batch runner. Rank 0 is the root:
  1. **Broadcast** a job config (task type, parameters, seed) with `comm.bcast`.
  2. **Scatter** the batch of task inputs: `comm.scatter` for Python objects (uneven lists chunked
     evenly) and `comm.Scatterv` for NumPy arrays of numeric inputs.
  3. Each rank executes its chunk with the same task semantics as the Java executors
     (CPU sum 1..N, sleep, seeded matrix checksum) and times each task.
  4. **Gather** results and per-task timings with `comm.gather` / `Gatherv`.
  Prints rank-wise logs (`[rank r] received k tasks … returned …`) and writes a JSON result.
- `mpi/predisched_mpi/matmul.py` — parallel matrix multiplication:
  rank 0 builds seeded A and B (n×n, float64); **Scatterv** the rows of A with correct counts and
  displacements when n is not divisible by the process count; **Bcast** B; each rank computes its
  row-block of A·B; **Gatherv** into C; rank 0 verifies `np.allclose(C, A @ B)` and outputs a checksum
  identical in definition to the Java `MatrixTaskExecutor` checksum.
- `mpi/predisched_mpi/cli.py` — `run-batch <input.json>` and `matmul --n N --seed S`, both printing a
  single JSON line on stdout for machine consumption, with human logs on stderr.
- Windows: MS-MPI (`mpiexec` from MS-MPI v10); Linux/Docker: OpenMPI. `mpi/README.md` states the
  install steps for both.

### Integrating it with the product (`predisched-worker`)

- `MpiMatrixTaskExecutor implements TaskExecutor`: runs
  `mpiexec -n <ranks> python -m predisched_mpi.cli matmul --n N --seed S`, parses the JSON line, and
  returns the checksum and time. Enabled per worker with `settings.matrixBackend: mpi` and
  `settings.mpiRanks`; falls back to the Java executor if `mpiexec` is missing, logging why once.
- Because the checksum definitions match, the same `MATRIX_TASK` gives the same result on either
  backend — test this.
- Worker registration reports its backend so the scheduler and the ML features can tell them apart
  (add `string matrix_backend = 7;` to `RegisterRequest`).

### Measurement

`mpi/bench_matmul.py`: n ∈ {200, 400, 800} × processes ∈ {1, 2, 4}, 3 repetitions; writes
`results/mpi-matmul.csv` (time mean ± std dev, speedup, efficiency) and `results/mpi-matmul.png`.

### Tests

- `mpi/tests/test_matmul.py`: run under `mpiexec -n 1/2/3` (3 exercises uneven Scatterv), result
  equals `A @ B`.
- `mpi/tests/test_collectives.py`: every input is executed exactly once across ranks; results return in
  input order.
- `MpiMatrixTaskExecutorIT` (skipped automatically when `mpiexec` is absent): checksum equals the Java
  executor's for the same n and seed.

### Acceptance checks

```bash
mpiexec -n 4 python -m predisched_mpi.cli run-batch mpi/examples/batch.json
mpiexec -n 4 python -m predisched_mpi.cli matmul --n 400 --seed 7
python mpi/bench_matmul.py
mvn -q verify
```

### Docs

`docs/components/mpi-compute.md`; both MPI rows in `docs/LAB-COVERAGE.md` (rank logs; speedup table).
