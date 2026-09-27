"""The collectives program end to end under mpiexec (skipped when no MPI launcher is installed)."""

import csv
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from predisched_mpi import collectives

MPI_DIR = Path(__file__).resolve().parents[1]


def find_mpiexec():
    """mpiexec on PATH, or the one the impi_rt wheel puts in <venv>/Library/bin on Windows."""
    found = shutil.which("mpiexec")
    if found:
        return found
    bundled = Path(sys.prefix) / "Library" / "bin" / "mpiexec.exe"
    return str(bundled) if bundled.exists() else None


def test_round_robin_split_balances_chunks():
    parts = collectives.split(list(range(10)), 4)
    assert parts == [[0, 4, 8], [1, 5, 9], [2, 6], [3, 7]]


def test_generate_is_reproducible_and_uses_supported_types():
    a, b = collectives.generate(12, 3), collectives.generate(12, 3)
    assert a == b
    assert {t["type"] for t in a} <= set(collectives.tasks.SUPPORTED)


@pytest.mark.skipif(find_mpiexec() is None,
                    reason="mpiexec not found: install MS-MPI or impi_rt (Windows) or OpenMPI")
def test_eight_generated_tasks_come_back_through_two_ranks(tmp_path):
    out = tmp_path / "exp9.csv"
    env = dict(os.environ, PYTHONPATH=str(MPI_DIR))
    run = subprocess.run(
        [find_mpiexec(), "-n", "2", sys.executable, "-m", "predisched_mpi.collectives",
         "--generate", "8", "--seed", "1", "--out", str(out)],
        cwd=MPI_DIR, env=env, capture_output=True, text=True, timeout=180)
    assert run.returncode == 0, run.stdout + run.stderr
    assert "[rank 0/2] received config" in run.stdout
    assert "[rank 1/2] received config" in run.stdout
    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    assert len(rows) == 8
    assert {r["rank"] for r in rows} == {"0", "1"}
    for row in rows:
        assert not row["output"].startswith(("FAILED", "SKIPPED")), row
        assert row["output"] == collectives.tasks.execute(row["task_type"], row["input"])
