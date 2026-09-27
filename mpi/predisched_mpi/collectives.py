"""Exp 9: rank 0 plays the scheduler with MPI collectives.

    Broadcast  comm.bcast   the scheduling config (task filter, parameters, seed) to every rank
    Scatter    comm.scatter one chunk of the batch per rank (round-robin split), and
               comm.Scatter the same split as a NumPy buffer of task sizes
    execute    each rank runs its chunk with tasks.py (the Java workers' semantics), timing each
    Gather     comm.gather  (task_id, output, exec_ms, rank) back to rank 0, and
               comm.Gather  each rank's total busy time into a NumPy array

Scheduler analogy: the broadcast is the config every worker must share, the scatter is dispatch,
the gather is result collection.

    mpiexec -n 4 python -m predisched_mpi.collectives --generate 20 --seed 42
    mpiexec -n 4 python -m predisched_mpi.collectives --trace ../workloads/mixed-steady-7.jsonl
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
from pathlib import Path

import numpy as np
from mpi4py import MPI

from predisched_mpi import tasks

REPO = Path(__file__).resolve().parents[2]
DEFAULT_OUT = REPO / "results" / "exp9-collectives.csv"


def log(comm: MPI.Comm, message: str) -> None:
    print(f"[rank {comm.Get_rank()}/{comm.Get_size()}] {message}", flush=True)


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--trace", help="workload trace (prompt 05 JSONL)")
    source.add_argument("--generate", type=int, metavar="N", help="generate N tasks")
    parser.add_argument("--seed", type=int, default=42, help="generator seed")
    parser.add_argument("--types", default=",".join(tasks.SUPPORTED),
                        help="task types to run (the broadcast filter)")
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="combined result CSV")
    return parser.parse_args(argv)


def generate(n: int, seed: int) -> list[dict]:
    """A reproducible mixed batch of the four supported types, sized to run in well under 1 s."""
    rnd = random.Random(seed)
    batch = []
    for i in range(n):
        kind = tasks.SUPPORTED[rnd.randrange(len(tasks.SUPPORTED))]
        if kind == "CPU_TASK":
            text = f"n={rnd.randrange(50_000, 2_000_000)}"
        elif kind == "HASH_TASK":
            text = f"rounds={rnd.randrange(10_000, 200_000)}"
        elif kind == "MONTE_CARLO_TASK":
            text = f"samples={rnd.randrange(10_000, 200_000)}, seed={rnd.randrange(1000)}"
        else:
            text = f"ms={rnd.randrange(10, 200)}"
        batch.append({"task_id": f"mpi-{seed}-{i:05d}", "type": kind, "input": text})
    return batch


def load_trace(path: str) -> list[dict]:
    batch = []
    with open(path, encoding="utf-8") as lines:
        for line in lines:
            if line.strip():
                entry = json.loads(line)
                batch.append({"task_id": entry["task_id"], "type": entry["type"],
                              "input": entry["input"]})
    return batch


def split(batch: list, parts: int) -> list[list]:
    """Round-robin: task i goes to rank i % parts, so chunk sizes differ by at most one."""
    return [batch[r::parts] for r in range(parts)]


def run_chunk(comm: MPI.Comm, chunk: list[dict], allowed: set[str]) -> list[tuple]:
    results = []
    for task in chunk:
        if task["type"] not in allowed:
            results.append((task["task_id"], task["type"], task["input"],
                            "SKIPPED: not in the broadcast filter", 0.0, comm.Get_rank()))
            continue
        start = time.perf_counter()
        try:
            output = tasks.execute(task["type"], task["input"])
        except tasks.TaskError as e:
            output = f"FAILED: {e}"
        exec_ms = (time.perf_counter() - start) * 1000.0
        results.append((task["task_id"], task["type"], task["input"], output, exec_ms,
                        comm.Get_rank()))
    return results


def main(argv=None) -> int:
    comm = MPI.COMM_WORLD
    rank, size = comm.Get_rank(), comm.Get_size()
    args = parse_args(argv)

    # --- Broadcast: rank 0 decides the configuration, everyone receives the same dict.
    config = None
    batch = None
    if rank == 0:
        batch = load_trace(args.trace) if args.trace else generate(args.generate or 20, args.seed)
        config = {"task_filter": sorted(t.strip() for t in args.types.split(",") if t.strip()),
                  "seed": args.seed,
                  "source": args.trace or f"generate {len(batch)} seed {args.seed}",
                  "batch_size": len(batch),
                  "split": "round_robin"}
        log(comm, f"broadcasting config {config}")
    config = comm.bcast(config, root=0)
    log(comm, f"received config: filter={config['task_filter']} batch={config['batch_size']}")

    # --- Scatter (objects): one chunk of task dicts per rank.
    chunks = split(batch, size) if rank == 0 else None
    chunk = comm.scatter(chunks, root=0)
    log(comm, f"received {len(chunk)} tasks: {[t['task_id'] for t in chunk]}")

    # --- Scatter (buffer): the same split as a padded int64 matrix of task sizes, one row each.
    width = -(-config["batch_size"] // size)
    sizes_matrix = None
    if rank == 0:
        sizes_matrix = np.full((size, width), -1, dtype=np.int64)
        for r, part in enumerate(chunks):
            sizes_matrix[r, :len(part)] = [tasks.input_size(t["type"], t["input"]) for t in part]
    my_sizes = np.empty(width, dtype=np.int64)
    comm.Scatter(sizes_matrix, my_sizes, root=0)
    my_sizes = my_sizes[my_sizes >= 0]
    assert list(my_sizes) == [tasks.input_size(t["type"], t["input"]) for t in chunk]
    log(comm, f"received sizes buffer {my_sizes.tolist()} (matches the object scatter)")

    # --- Execute the chunk.
    started = time.perf_counter()
    results = run_chunk(comm, chunk, set(config["task_filter"]))
    busy_ms = (time.perf_counter() - started) * 1000.0
    for task_id, _, _, output, exec_ms, _ in results:
        log(comm, f"returning {task_id} in {exec_ms:.1f} ms: {output}")

    # --- Gather (objects) the results, and (buffer) each rank's busy time.
    gathered = comm.gather(results, root=0)
    totals = np.empty(size, dtype=np.float64) if rank == 0 else None
    comm.Gather(np.array([busy_ms], dtype=np.float64), totals, root=0)

    if rank == 0:
        combined = sorted((row for part in gathered for row in part), key=lambda row: row[0])
        print(f"\nPer rank ({size} ranks, {len(combined)} tasks)", flush=True)
        print(f"  {'rank':>4}  {'tasks':>5}  {'size_sum':>10}  {'busy_ms':>9}")
        for r, part in enumerate(gathered):
            size_sum = int(sizes_matrix[r][sizes_matrix[r] >= 0].sum())
            print(f"  {r:>4}  {len(part):>5}  {size_sum:>10}  {totals[r]:>9.1f}")
        print(f"  makespan {totals.max():.1f} ms, total work {totals.sum():.1f} ms,"
              f" balance (max/mean) {totals.max() / max(totals.mean(), 1e-9):.2f}")
        print("\nCombined result")
        for task_id, kind, _, output, exec_ms, r in combined:
            print(f"  {task_id}  {kind:<16}  rank {r}  {exec_ms:>7.1f} ms  {output}")
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["task_id", "task_type", "input", "output", "exec_ms", "rank"])
            for task_id, kind, text, output, exec_ms, r in combined:
                writer.writerow([task_id, kind, text, output, f"{exec_ms:.3f}", r])
        print(f"\nRESULTS {len(combined)} written to {out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
