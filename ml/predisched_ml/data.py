"""Loading the dataset and splitting it the same way in training and evaluation."""

from __future__ import annotations

import dataclasses
import hashlib
from pathlib import Path

import pandas as pd
import yaml

from . import ML_DIR


def load_config(path: str | Path | None = None) -> dict:
    path = Path(path) if path else ML_DIR / "config.yaml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def dataset_path(config: dict) -> Path:
    path = Path(config["dataset"])
    return path if path.is_absolute() else ML_DIR / path


def load_dataset(config: dict) -> pd.DataFrame:
    df = pd.read_parquet(dataset_path(config))
    # Stable order: dispatch time, then id, so equal timestamps never reorder between runs.
    return df.sort_values(["dispatched_at", "id"], kind="mergesort").reset_index(drop=True)


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclasses.dataclass
class Splits:
    train: pd.DataFrame
    test: pd.DataFrame
    holdout_pattern: pd.DataFrame
    holdout_type: pd.DataFrame

    def evaluation_sets(self) -> dict[str, pd.DataFrame]:
        return {"test": self.test, "holdout_pattern": self.holdout_pattern,
                "holdout_type": self.holdout_type}


def split(df: pd.DataFrame, test_fraction: float) -> Splits:
    """Time split of the non-held-out rows (no shuffle), plus the two hold-out sets.

    The held-out pattern and task type (dataset card) never reach training. A held-out-pattern
    row of the held-out type belongs to the type set: cold start is the stricter test.
    """
    df = df.sort_values(["dispatched_at", "id"], kind="mergesort")
    held_type = df["holdout_task_type"].astype(bool)
    held_pattern = df["holdout_pattern"].astype(bool) & ~held_type
    main = df[~held_type & ~held_pattern]
    cut = int(round(len(main) * (1.0 - test_fraction)))
    return Splits(train=main.iloc[:cut].reset_index(drop=True),
                  test=main.iloc[cut:].reset_index(drop=True),
                  holdout_pattern=df[held_pattern].reset_index(drop=True),
                  holdout_type=df[held_type].reset_index(drop=True))
