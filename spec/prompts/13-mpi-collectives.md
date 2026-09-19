# Prompt 13: MPI collectives: Broadcast, Scatter, Gather (Exp 9)

## Goal

An mpi4py program where rank 0 plays the scheduler: it broadcasts the scheduling configuration,
scatters a batch of PrediSched task inputs across ranks, each rank executes its chunk with the same
task semantics as the Java workers, and rank 0 gathers results and timings. This is lab Exp 9.

## Read first

- Spec §4 FR21, §8 (MPI row), §8.1, §11 Exp 9

## Build

1. **`mpi/`** (Python package `predisched_mpi`, `requirements.txt` with mpi4py and NumPy):
   - `tasks.py`: Python versions of `CPU_TASK`, `HASH_TASK`, `MONTE_CARLO_TASK` and `SLEEP_TASK` with
     the same `key=value` input format and the same deterministic outputs as the Java executors
     (same prime count, same hash, same seeded estimate), so results can be cross-checked.
   - `collectives.py`:
     - rank 0 loads a batch from a trace file (prompt 05 format) or `--generate N --seed S`;
     - **Broadcast**: `comm.bcast` of a config dict (task filter, parameters, seed) to all ranks;
     - **Scatter**: rank 0 splits the batch into `size` chunks and `comm.scatter`s them; also show the
       buffer version `comm.Scatter` on a NumPy array of task sizes;
     - each rank executes its chunk and times each task;
     - **Gather**: `comm.gather` of `(task_id, output, exec_ms, rank)` back to rank 0; also
       `comm.Gather` of per-rank total time into a NumPy array;
     - rank 0 prints a per-rank table and the combined result, and writes
       `results/exp9-collectives.csv`.
   - Every rank logs what it received and returned, prefixed `[rank r/size]`.
2. **Setup docs**: MS-MPI install steps on Windows (runtime + SDK, `mpiexec` on PATH) and OpenMPI in
   Docker (`docker/mpi.Dockerfile`), plus `scripts/run-mpi.ps1` / `.sh`.

## Tests

- `pytest` in `mpi/` for `tasks.py`: outputs match fixed expected values that the Java tests also
  assert (put the shared fixtures in `testdata/task-outputs.json` at the repo root and read it
  from both sides).
- A test that runs `mpiexec -n 2 python -m predisched_mpi.collectives --generate 8 --seed 1` in a
  subprocess and checks all 8 results come back (skipped with a clear reason if `mpiexec` is missing).

## Acceptance checks

```bash
mpiexec -n 4 python -m predisched_mpi.collectives --generate 20 --seed 42
```

Paste the rank-wise logs (what each rank received and returned) and the combined table on rank 0.

## Docs and commit

- `docs/components/mpi.md` (collectives section): the three collectives, how they map to the
  scheduler's broadcast/dispatch/collect, and real output.
- `docs/LAB-COVERAGE.md` Exp 9 row.
- CI: add `mpi/` pytest to the Python job, using OpenMPI on the Linux runner.
- Commit: `MPI: rank 0 broadcasts config, scatters tasks and gathers results with mpi4py`
