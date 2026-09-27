import os
import sys
from pathlib import Path

import pytest

SPARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK_DIR))
FIXTURES = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession for the whole run (starting one takes seconds)."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    from predisched_spark.common import configure

    session = (configure(SparkSession.builder).master("local[2]").appName("predisched-tests")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "2")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def history_csv() -> str:
    return str(FIXTURES / "execution_history_20.csv")
