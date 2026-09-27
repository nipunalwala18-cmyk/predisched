"""Exp 10: Scatterv arithmetic, correctness under mpiexec, and the checksum shared with Java."""

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

from predisched_mpi import matmul, tasks
from test_collectives import MPI_DIR, find_mpiexec

FIXTURES = json.loads((Path(__file__).resolve().parents[2] / "testdata" / "task-outputs.json")
                      .read_text(encoding="utf-8"))


def test_scatterv_counts_for_awkward_sizes():
    assert matmul.counts_and_displacements(7, 4) == ([2, 2, 2, 1], [0, 2, 4, 6])
    assert matmul.counts_and_displacements(3, 4) == ([1, 1, 1, 0], [0, 1, 2, 3])
    counts, displacements = matmul.counts_and_displacements(801, 4)
    assert sum(counts) == 801 and displacements[-1] + counts[-1] == 801


def test_matrices_are_the_java_stream():
    a, b = tasks.matrices(3, 5)
    rnd = tasks.JavaRandom(5)
    for i in range(3):
        for j in range(3):
            assert a[i, j] == rnd.next_double()
            assert b[i, j] == rnd.next_double()


def test_numpy_checksum_agrees_with_the_java_fixture():
    for case in (c for c in FIXTURES if c["type"] == "MATRIX_TASK"):
        params = tasks.parse_input(case["input"])
        a, b = tasks.matrices(int(params["size"]), int(params.get("seed", 42)))
        java = float(case["output"].split("checksum=")[1])
        # BLAS sums in another order than Java's loop: equal to ~1e-12 relative, not bit for bit.
        assert float((a @ b).sum()) == pytest.approx(java, rel=1e-9), case


@pytest.mark.skipif(find_mpiexec() is None,
                    reason="mpiexec not found: install MS-MPI or impi_rt (Windows) or OpenMPI")
@pytest.mark.parametrize("ranks", [1, 3])
def test_c_equals_the_sequential_product(ranks):
    run = subprocess.run(
        [find_mpiexec(), "-n", str(ranks), sys.executable, "-m", "predisched_mpi.matmul",
         "--size", "50", "--seed", "3", "--json"],
        cwd=MPI_DIR, env=dict(os.environ, PYTHONPATH=str(MPI_DIR)), capture_output=True,
        text=True, timeout=180)
    assert run.returncode == 0, run.stdout + run.stderr
    result = json.loads(next(line for line in run.stdout.splitlines() if line.startswith("{")))
    assert result["verified"] is True
    assert result["procs"] == ranks and sum(result["rows_per_rank"]) == 50
    a, b = tasks.matrices(50, 3)
    assert result["checksum"] == pytest.approx(float((a @ b).sum()), rel=1e-12)
