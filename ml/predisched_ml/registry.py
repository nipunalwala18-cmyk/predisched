"""Versioned model files: ``ml/models/<m>/v<N>/{model.joblib, meta.json}`` and
``ml/models/registry.json`` (versions per model and which one is live)."""

from __future__ import annotations

import json
from pathlib import Path

import joblib

from . import ML_DIR

MODELS_DIR = ML_DIR / "models"


def registry_path(root: Path = MODELS_DIR) -> Path:
    return root / "registry.json"


def load_registry(root: Path = MODELS_DIR) -> dict:
    path = registry_path(root)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"models": {}}


def next_version(model: str, root: Path = MODELS_DIR) -> int:
    folder = root / model
    versions = [int(p.name[1:]) for p in folder.glob("v*") if p.name[1:].isdigit()]
    return max(versions, default=0) + 1


def save(model: str, obj, meta: dict, root: Path = MODELS_DIR, live: bool = True) -> Path:
    version = next_version(model, root)
    folder = root / model / f"v{version}"
    folder.mkdir(parents=True, exist_ok=False)
    joblib.dump(obj, folder / "model.joblib")
    meta.update(model=model, version=version)  # the caller's copy learns its version too
    (folder / "meta.json").write_text(json.dumps(meta, indent=2, default=str) + "\n",
                                      encoding="utf-8")
    registry = load_registry(root)
    entry = registry["models"].setdefault(model, {"live": None, "versions": []})
    entry["versions"].append({"version": version, "path": f"{model}/v{version}",
                              "trained_at": meta.get("trained_at"),
                              "kept": meta.get("kept", {}).get("candidate"),
                              "data_hash": meta.get("data_hash")})
    if live:
        entry["live"] = version
    registry_path(root).write_text(json.dumps(registry, indent=2) + "\n", encoding="utf-8")
    return folder


def live_folder(model: str, root: Path = MODELS_DIR, version: int | None = None) -> Path:
    registry = load_registry(root)
    entry = registry["models"].get(model)
    if not entry:
        raise FileNotFoundError(f"no {model} model in {registry_path(root)}: run"
                                f" python -m predisched_ml.train --model {model}")
    return root / model / f"v{version or entry['live']}"


def load(model: str, root: Path = MODELS_DIR, version: int | None = None) -> tuple[object, dict]:
    folder = live_folder(model, root, version)
    meta = json.loads((folder / "meta.json").read_text(encoding="utf-8"))
    return joblib.load(folder / "model.joblib"), meta
