"""Exp 10: parallel matrix multiplication, C = A @ B, with MPI.

    rank 0      builds A and B (n x n, float64) from java.util.Random(seed), exactly as the Java
                MatrixTaskExecutor does, so both report the same checksum
    Bcast       B to every rank
    Scatterv    A's rows: counts/displacements let any n run on any number of ranks
    compute     local A_block @ B (NumPy, one BLAS thread per rank)
    Gatherv     the C blocks back into C on rank 0
    verify      np.allclose(C, A @ B) on rank 0

Phases are timed with MPI.Wtime(); each phase reports its slowest rank.

    mpiexec -n 4 python -m predisched_mpi.matmul --size 400 --seed 42 [--json]
    mpiexec -n 4 python -m predisched_mpi.matmul --sizes 200,400,800 --repeat 3
"""

from __future__ import annotations

import os

# One BLAS thread per rank: otherwise the 1-process baseline already uses every core and the
# speedup measures nothing. Must be set before NumPy loads its BLAS.
for _var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
    os.environ.setdefault(_var, "1")

import argparse  # noqa: E402
import csv  # noqa: E402
import json  # noqa: E402
import statistics  # noqa: E402
import sys  # noqa: E402
from pathlib import Path  # noqa: E402

import numpy as np  # noqa: E402
from mpi4py import MPI  # noqa: E402

from predisched_mpi import tasks  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
DEFAULT_OUT = REPO / "results" / "exp10-matmul.csv"
CSV_HEADER = ["n", "procs", "repeats", "scatter_bcast_ms", "compute_ms", "gather_ms", "total_ms",
              "verified", "checksum"]


def counts_and_displacements(n: int, size: int) -> tuple[list[int], list[int]]:
    """Rows per rank (the first n % size ranks get one extra) and where each block starts."""
    base, extra = divmod(n, size)
    counts = [base + (1 if r < extra else 0) for r in range(size)]
    displacements = [sum(counts[:r]) for r in range(size)]
    return counts, displacements


def multiply(comm: MPI.Comm, n: int, seed: int, verify: bool = True) -> dict:
    """One distributed product; the returned dict is complete on rank 0 only."""
    rank, size = comm.Get_rank(), comm.Get_size()
    counts, displacements = counts_and_displacements(n, size)
    if rank == 0:
        a, b = tasks.matrices(n, seed)
        a = np.ascontiguousarray(a)
        b = np.ascontiguousarray(b)
        c = np.empty((n, n), dtype=np.float64)
    else:
        a = c = None
        b = np.empty((n, n), dtype=np.float64)
    block = np.empty((counts[rank], n), dtype=np.float64)
    element_counts = [k * n for k in counts]
    element_displacements = [d * n for d in displacements]

    comm.Barrier()
    t0 = MPI.Wtime()
    comm.Bcast(b, root=0)
    comm.Scatterv([a, element_counts, element_displacements, MPI.DOUBLE], block, root=0)
    t1 = MPI.Wtime()
    c_block = block @ b
    t2 = MPI.Wtime()
    comm.Gatherv(c_block, [c, element_counts, element_displacements, MPI.DOUBLE], root=0)
    t3 = MPI.Wtime()

    phases = np.array([t1 - t0, t2 - t1, t3 - t2, t3 - t0]) * 1000.0
    slowest = np.empty(4) if rank == 0 else None
    comm.Reduce(phases, slowest, op=MPI.MAX, root=0)
    if rank != 0:
        return {}
    result = {"size": n, "procs": size, "seed": seed, "rows_per_rank": counts,
              "scatter_bcast_ms": round(float(slowest[0]), 3),
              "compute_ms": round(float(slowest[1]), 3),
              "gather_ms": round(float(slowest[2]), 3),
              "total_ms": round(float(slowest[3]), 3),
              "checksum": float(c.sum())}
    if verify:
        expected = a @ b
        result["verified"] = bool(np.allclose(c, expected))
        result["max_abs_diff"] = float(np.max(np.abs(c - expected)))
    return result


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--size", type=int, help="one product of this size")
    mode.add_argument("--sizes", help="benchmark: comma-separated sizes, e.g. 200,400,800")
    parser.add_argument("--seed", type=int, default=42, help="java.util.Random seed (Java: 42)")
    parser.add_argument("--repeat", type=int, default=3, help="benchmark runs per size (median)")
    parser.add_argument("--json", action="store_true",
                        help="print one JSON line for MatrixTaskExecutor (single mode)")
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="benchmark CSV")
    return parser.parse_args(argv)


def write_rows(path: Path, procs: int, rows: list[dict]) -> None:
    """Replaces this process count's rows in the CSV, keeping the others (one run per -n)."""
    kept = []
    if path.exists():
        with path.open(encoding="utf-8") as f:
            kept = [r for r in csv.DictReader(f) if int(r["procs"]) != procs]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()
        for row in sorted(kept + rows, key=lambda r: (int(r["n"]), int(r["procs"]))):
            writer.writerow({k: row[k] for k in CSV_HEADER})


def main(argv=None) -> int:
    comm = MPI.COMM_WORLD
    rank, size = comm.Get_rank(), comm.Get_size()
    args = parse_args(argv)

    if args.sizes:
        rows = []
        for n in (int(s) for s in args.sizes.split(",")):
            runs = [multiply(comm, n, args.seed) for _ in range(args.repeat)]
            if rank != 0:
                continue
            median = {k: statistics.median(r[k] for r in runs)
                      for k in ("scatter_bcast_ms", "compute_ms", "gather_ms", "total_ms")}
            verified = all(r["verified"] for r in runs)
            print(f"check n={n} procs={size}: np.allclose(C, A @ B) = {verified}"
                  f" (max |diff| {max(r['max_abs_diff'] for r in runs):.2e}),"
                  f" checksum {runs[0]['checksum']:.6f}", flush=True)
            rows.append({"n": n, "procs": size, "repeats": args.repeat,
                         **{k: f"{v:.3f}" for k, v in median.items()},
                         "verified": verified, "checksum": f"{runs[0]['checksum']:.6f}"})
        if rank == 0:
            print(f"\nprocs={size} (median of {args.repeat})")
            print(f"  {'n':>5}  {'scatter+bcast':>13}  {'compute':>9}  {'gather':>8}  {'total':>9}")
            for row in rows:
                print(f"  {row['n']:>5}  {row['scatter_bcast_ms']:>10} ms  {row['compute_ms']:>6} ms"
                      f"  {row['gather_ms']:>5} ms  {row['total_ms']:>6} ms")
            write_rows(Path(args.out), size, rows)
            print(f"ROWS {len(rows)} for procs={size} in {args.out}", flush=True)
        return 0

    n = args.size or 200
    result = multiply(comm, n, args.seed)
    if rank == 0:
        if args.json:
            print(json.dumps(result), flush=True)
        else:
            print(f"check: np.allclose(C, A @ B) = {result['verified']}"
                  f" (max |diff| {result['max_abs_diff']:.2e}) rows per rank"
                  f" {result['rows_per_rank']}")
            print(f"size={n} procs={size} seed={args.seed} checksum={result['checksum']:.6f}"
                  f" scatter_bcast_ms={result['scatter_bcast_ms']}"
                  f" compute_ms={result['compute_ms']} gather_ms={result['gather_ms']}"
                  f" total_ms={result['total_ms']}")
    return 0 if rank != 0 or result.get("verified") else 1


if __name__ == "__main__":
    sys.exit(main())
