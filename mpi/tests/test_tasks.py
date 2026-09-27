"""tasks.py reproduces the Java executors: the shared fixtures, also asserted by the Java side."""

import json
from pathlib import Path

import pytest

from predisched_mpi import tasks

FIXTURES = json.loads((Path(__file__).resolve().parents[2] / "testdata" / "task-outputs.json")
                      .read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", FIXTURES, ids=lambda c: f"{c['type']}:{c['input']}")
def test_output_matches_the_shared_fixture(case):
    if case["success"]:
        assert tasks.execute(case["type"], case["input"]) == case["output"]
    else:
        with pytest.raises(tasks.TaskError) as failure:
            tasks.execute(case["type"], case["input"])
        assert str(failure.value) == case["output"]


def test_java_random_matches_known_java_values():
    # new java.util.Random(42).nextDouble() and new Random(0).nextDouble()
    assert tasks.JavaRandom(42).next_double() == pytest.approx(0.7275636800328681, abs=1e-16)
    assert tasks.JavaRandom(0).next_double() == pytest.approx(0.730967787376657, abs=1e-15)


def test_java_string_hash():
    assert tasks.java_string_hash("") == 0
    assert tasks.java_string_hash("hello") == 99162322
    assert -(2**31) <= tasks.java_string_hash("x" * 100) < 2**31


def test_bad_input_is_rejected_like_the_java_validator():
    with pytest.raises(tasks.TaskError, match="missing required key 'n'"):
        tasks.execute("CPU_TASK", "rounds=5")
    with pytest.raises(tasks.TaskError, match="between 2 and"):
        tasks.execute("CPU_TASK", "n=1")
    with pytest.raises(tasks.TaskError, match="not supported under MPI"):
        tasks.execute("FILE_IO_TASK", "size_mb=1")
    with pytest.raises(tasks.TaskError, match="key=value"):
        tasks.execute("CPU_TASK", "n")


def test_input_size_is_the_first_knob():
    assert tasks.input_size("CPU_TASK", "n=500") == 500
    assert tasks.input_size("MONTE_CARLO_TASK", "samples=9, seed=1") == 9
    assert tasks.input_size("GRAPH_TASK", "nodes=5") == 0
