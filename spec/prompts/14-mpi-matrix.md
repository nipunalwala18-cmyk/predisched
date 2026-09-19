# Prompt 14: Parallel matrix multiplication with MPI (Exp 10)

## Goal

Parallel matrix multiplication with mpi4py (Scatterv rows of A, Broadcast B, local multiply, Gatherv
into C), verified against a sequential result, with a measured speedup and efficiency table for n =
200, 400, 800 on 1, 2 and 4 processes. `MATRIX_TASK` can run through MPI as a heavy benchmark
workload. This is lab Exp 10.

## Read first

- Spec §4 FR21, §7.2 (`MATRIX_TASK`), §11 Exp 10, §17 (Spark / MPI row)

## Build

1. **`mpi/predisched_mpi/matmul.py`**
   - Rank 0 builds seeded A and B (`n × n`, float64).
   - Row counts and displacements for `Scatterv` so any `n` works with any process count.
   - `comm.Bcast(B)`, `comm.Scatterv` of A's rows, local `A_block @ B` (NumPy), `comm.Gatherv` into C.
   - Rank 0 verifies `np.allclose(C, A @ B)` and reports the checksum used by the Java
     `MatrixTaskExecutor` for the same seed and size, so both sides agree.
   - Timing with `MPI.Wtime()` split into scatter/bcast, compute and gather.
   - `--sizes 200,400,800 --repeat 3` mode prints and writes `results/exp10-matmul.csv`.
2. **Speedup chart**: `scripts/plot-matmul.py` (matplotlib) reads the CSV and writes
   `docs/img/exp10-speedup.png` (speedup vs processes, one line per n, ideal line dashed).
   A runner script loops over `-n 1, 2, 4`.
3. **MPI-backed `MATRIX_TASK`**: `MatrixTaskExecutor` gains `mode=local|threads|mpi`. `mpi` runs
   `mpiexec -n <k> python -m predisched_mpi.matmul --size … --seed …` as a subprocess and parses the
   JSON line it prints. Resource profile `PARALLEL`. Disabled when `mpi.exec` is not configured.

## Tests

- `pytest`: `Scatterv` counts for awkward sizes (n=7 on 4 ranks); C equals the sequential product for
  n = 50 on 1 and 3 ranks (subprocess, skipped without `mpiexec`).
- The checksum for `size=64, seed=1` equals the value asserted in the Java executor test (shared
  `testdata/task-outputs.json`).

## Acceptance checks

```bash
scripts/run-mpi.sh matmul --sizes 200,400,800 --repeat 3
```
```bash
python scripts/plot-matmul.py results/exp10-matmul.csv
```
```bash
java -jar predisched-client/target/predisched-client.jar submit --type MATRIX_TASK --input "size=400, mode=mpi, procs=4" --priority 5
```

Paste the correctness check lines, the timing table with speedup and efficiency, and the task result.

## Docs and commit

- `docs/components/mpi.md` (matrix section) with the table and `docs/img/exp10-speedup.png`.
- `docs/LAB-COVERAGE.md` Exp 10 row.
- Commit: `MPI: parallel matrix multiplication with measured speedup, available as MATRIX_TASK`
