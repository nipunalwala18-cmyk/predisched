# MPI: collectives over PrediSched tasks (Exp 9)

An mpi4py program in which rank 0 plays the scheduler:

1. It broadcasts the scheduling configuration.
2. It scatters a batch of PrediSched tasks across the ranks.
3. Each rank runs its chunk with the same task semantics as the Java workers.
4. Rank 0 gathers the results and timings.

Built by `spec/prompts/13-mpi-collectives.md`. Spec sections: §4 FR21, §8 (MPI row), §8.1, §11
Exp 9. Prompt 14 adds matrix multiplication to this document.

## Layout

| Path | What |
| --- | --- |
| `mpi/predisched_mpi/tasks.py` | `CPU_TASK`, `HASH_TASK`, `MONTE_CARLO_TASK`, `SLEEP_TASK` in Python, with the Java executors' exact outputs |
| `mpi/predisched_mpi/collectives.py` | The Exp 9 program |
| `testdata/task-outputs.json` | Shared fixtures (type, input, success, output), asserted by both `mpi/tests/test_tasks.py` and `TaskOutputsFixtureTest` (Java) |
| `mpi/requirements.txt` | mpi4py 4.1.2, NumPy, pytest; on Windows also `impi_rt` |
| `docker/mpi.Dockerfile`, `scripts/run-mpi.ps1` / `.sh` | Run on N ranks natively or in Docker (OpenMPI), whichever is available |

## Collectives and what they stand for in the scheduler

| Step | mpi4py call | Scheduler equivalent |
| --- | --- | --- |
| Broadcast | `comm.bcast(config, root=0)`: task filter, seed, source, batch size, split policy | Every worker shares one configuration |
| Scatter (objects) | `comm.scatter(chunks, root=0)`: task *i* goes to rank *i* mod *size* | Dispatch: each worker gets its share of the queue |
| Scatter (buffer) | `comm.Scatter` of an int64 matrix `(size, ceil(n/size))` of task sizes, padded with -1 | The same dispatch with fixed-size typed buffers. Each rank checks the row it gets against its object chunk |
| Execute | `tasks.execute` per task, timed with `perf_counter` | Worker thread pool |
| Gather (objects) | `comm.gather` of `(task_id, type, input, output, exec_ms, rank)` | Result collection |
| Gather (buffer) | `comm.Gather` of each rank's busy time into a float64 array | Heartbeat metrics |

Rank 0 then prints two tables. The per-rank table shows task count, size sum and busy time, plus
makespan and balance (max/mean). The combined table is sorted by task id. Rank 0 also writes
`results/exp9-collectives.csv`.

Every rank prefixes its log lines with `[rank r/size]`. Each rank logs the config it received, its
chunk, its size buffer, and each result it returns.

- A task whose type is not in the broadcast filter comes back as `SKIPPED`.
- An input error comes back as `FAILED: <reason>`.
- With `--trace ../workloads/<file>.jsonl`, rank 0 reads a prompt 05 trace instead of generating.
  Trace types other than the four above are skipped.

## Same results as the Java workers

`tasks.py` parses inputs with the same rules as `InputParser` (comma-separated `key=value`, trimmed)
and returns the same strings:

- **CPU_TASK:** a sieve counting primes below `n`.
- **HASH_TASK:** SHA-256 chained `rounds` times, starting from `predisched-hash-payload`.
- **MONTE_CARLO_TASK:** a port of `java.util.Random` (the 48-bit LCG and `nextDouble`), so seed 7
  gives the same count of points inside the circle as the JVM.
- **SLEEP_TASK:** a sleep. Its optional injected failure draws `Random(seed + attempt)` on the first
  attempt. Without a seed it uses a port of `String.hashCode` of the input, as the Java executor
  does.

`testdata/task-outputs.json` pins 14 cases, including a 2,000,000-prime sieve, 100,000 hash rounds,
1,000,000 Monte Carlo samples and an injected failure. The Python tests and the Java
`TaskOutputsFixtureTest` both assert every case, so the two implementations cannot drift apart. The
outputs also match live tasks run through the scheduler earlier:

- `primes_below_2000000=148933`
- `samples=1000000 seed=7 inside=785563`
- `digest=9a640c9c...388` for 100,000 rounds

A side finding: `new Random(seed).nextDouble()` is about 0.73 for every small seed, so
`failRate=0.5` never fails on its first attempt for seeds 0–19. The failure fixture therefore uses
`failRate=0.9`.

## Setup

### Windows: Intel MPI from PyPI (used here)

```bash
python -m pip install -r mpi/requirements.txt
```

On Windows that installs `impi_rt`, Intel's MPI runtime, as a wheel. It puts `mpiexec.exe` and
`impi.dll` in `<venv>\Library\bin`, and mpi4py finds the library there. Nothing else needs
installing, and no admin rights or `hydra_service` are required for local runs. Put
`<venv>\Library\bin` on `PATH`, or use `scripts/run-mpi.ps1`, which does it for you.

The spec names MS-MPI, which needs two system installers. Both work with mpi4py:

1. Download `msmpisetup.exe` (the runtime) and `msmpisdk.msi` (the SDK) from Microsoft's MS-MPI
   page, and install both.
2. Open a new terminal. `mpiexec` should now be on `PATH` (`C:\Program Files\Microsoft MPI\Bin`),
   and `MSMPI_BIN` should be set.
3. `pip install mpi4py numpy`, then run the same commands. `run-mpi.ps1` prefers an `mpiexec` on
   `PATH`.

### Linux

```bash
sudo apt-get install -y openmpi-bin libopenmpi-dev
python -m pip install -r mpi/requirements.txt
```

In Docker, with no local MPI needed:

```bash
docker build -f docker/mpi.Dockerfile -t predisched-mpi .
docker run --rm -v "$PWD:/work" predisched-mpi 4 predisched_mpi.collectives --generate 20 --seed 42
```

The image sets `OMPI_ALLOW_RUN_AS_ROOT` and `oversubscribe`, so it runs on any core count.
`scripts/run-mpi.sh <ranks> <module> [args]` picks a native `mpiexec` (`PATH`, then the repo's
`.venv`) and falls back to Docker. CI installs OpenMPI on the Linux runner and runs `pytest` in
`mpi/`.

## Real output

Measured on 2026-09-27 (Windows 11, 8 cores, Python 3.12, mpi4py 4.1.2, Intel MPI 2021.18.1):

```bash
cd mpi && mpiexec -n 4 python -m predisched_mpi.collectives --generate 20 --seed 42
```

Rank-wise logs (excerpt; the lines of different ranks interleave):

```
[rank 0/4] broadcasting config {'task_filter': ['CPU_TASK', 'HASH_TASK', 'MONTE_CARLO_TASK', 'SLEEP_TASK'], 'seed': 42, 'source': 'generate 20 seed 42', 'batch_size': 20, 'split': 'round_robin'}
[rank 0/4] received config: filter=['CPU_TASK', 'HASH_TASK', 'MONTE_CARLO_TASK', 'SLEEP_TASK'] batch=20
[rank 2/4] received config: filter=['CPU_TASK', 'HASH_TASK', 'MONTE_CARLO_TASK', 'SLEEP_TASK'] batch=20
[rank 3/4] received config: filter=['CPU_TASK', 'HASH_TASK', 'MONTE_CARLO_TASK', 'SLEEP_TASK'] batch=20
[rank 1/4] received config: filter=['CPU_TASK', 'HASH_TASK', 'MONTE_CARLO_TASK', 'SLEEP_TASK'] batch=20
[rank 1/4] received 5 tasks: ['mpi-42-00001', 'mpi-42-00005', 'mpi-42-00009', 'mpi-42-00013', 'mpi-42-00017']
[rank 0/4] received 5 tasks: ['mpi-42-00000', 'mpi-42-00004', 'mpi-42-00008', 'mpi-42-00012', 'mpi-42-00016']
[rank 2/4] received 5 tasks: ['mpi-42-00002', 'mpi-42-00006', 'mpi-42-00010', 'mpi-42-00014', 'mpi-42-00018']
[rank 3/4] received 5 tasks: ['mpi-42-00003', 'mpi-42-00007', 'mpi-42-00011', 'mpi-42-00015', 'mpi-42-00019']
[rank 0/4] received sizes buffer [102451, 18, 197700, 193013, 34] (matches the object scatter)
[rank 3/4] received sizes buffer [1288352, 1227016, 11703, 36793, 147] (matches the object scatter)
[rank 2/4] received sizes buffer [36868, 70990, 160, 50758, 21390] (matches the object scatter)
[rank 1/4] received sizes buffer [74196, 246493, 66, 97, 100165] (matches the object scatter)
[rank 2/4] returning mpi-42-00002 in 38.0 ms: rounds=36868 digest=1c1c72852846c87d3851dfc0705c17dc9db123426265809a0d42738a5fc450f7
[rank 0/4] returning mpi-42-00000 in 8.7 ms: primes_below_102451=9809
[rank 3/4] returning mpi-42-00003 in 144.8 ms: primes_below_1288352=99212
[rank 1/4] returning mpi-42-00001 in 179.0 ms: samples=74196 seed=228 inside=58302 pi_estimate=3.143134
...
```

Rank 0:

```
Per rank (4 ranks, 20 tasks)
  rank  tasks    size_sum    busy_ms
     0      5      493216      499.7
     1      5      421017      581.3
     2      5      180166      442.7
     3      5     2564011      502.2
  makespan 581.3 ms, total work 2025.9 ms, balance (max/mean) 1.15

Combined result
  mpi-42-00000  CPU_TASK          rank 0      8.7 ms  primes_below_102451=9809
  mpi-42-00001  MONTE_CARLO_TASK  rank 1    179.0 ms  samples=74196 seed=228 inside=58302 pi_estimate=3.143134
  mpi-42-00002  HASH_TASK         rank 2     38.0 ms  rounds=36868 digest=1c1c72852846c87d3851dfc0705c17dc9db123426265809a0d42738a5fc450f7
  mpi-42-00003  CPU_TASK          rank 3    144.8 ms  primes_below_1288352=99212
  mpi-42-00004  SLEEP_TASK        rank 0     18.4 ms  slept_ms=18
  mpi-42-00005  CPU_TASK          rank 1     17.0 ms  primes_below_246493=21749
  ...

RESULTS 20 written to C:\CODING\predisched\results\exp9-collectives.csv
```

The same batch on different rank counts, one run each:

| Ranks | Makespan | Total work | Balance (max/mean) |
| --- | --- | --- | --- |
| 1 | 2092.8 ms | 2092.8 ms | 1.00 |
| 2 | 1305.5 ms | 2519.6 ms | 1.04 |
| 4 | 864.9 ms (581.3 ms in the run above) | 2864.8 ms | 1.21 |

With 4 ranks the makespan falls 2.4–3.6x. Total work grows with the rank count because the ranks
compete for memory bandwidth and CPU boost on one machine. Round-robin balances task *counts*, not
sizes: rank 3 got 2.56 M of size units against 0.18 M for rank 2. Busy times still stay within
1.2x, because the size units of different task types are not comparable. That is the imbalance a
size-aware or predictive split (prompt 18) would address.

## Tests

- `mpi/tests/test_tasks.py`:
  - every case in `testdata/task-outputs.json`, successes and the injected failure;
  - `JavaRandom` against known `java.util.Random` values;
  - `String.hashCode`;
  - input validation;
  - input size.
- `mpi/tests/test_collectives.py`:
  - the round-robin split and a reproducible generator;
  - `mpiexec -n 2 python -m predisched_mpi.collectives --generate 8 --seed 1` in a subprocess. It
    checks that both ranks log the config and that all 8 results come back from both ranks, each
    equal to `tasks.execute`. It is skipped, with the reason, when no `mpiexec` is found.
  - Result: 21 passed on Windows, including the `mpiexec` run.
- `predisched-worker/.../TaskOutputsFixtureTest.java`: the Java executors produce the same fixture
  outputs.
