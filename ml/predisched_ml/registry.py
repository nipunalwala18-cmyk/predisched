"""Versioned model files and their lifecycle (prompts 16 and 21).

Files: ``ml/models/<m>/v<N>/{model.joblib, meta.json}``. ``ml/models/registry.json`` lists every
version of each model with a status: ``live`` (serves the scheduler), ``shadow`` (predicts beside
live, logged, never used; F14) or ``retired``. At most one of each of live and shadow per model.

    python -m predisched_ml.registry list
    python -m predisched_ml.registry shadow <model> <version>
    python -m predisched_ml.registry promote <model> <version> [--force]

``promote`` compares the version with live on the same completed tasks (``compare_shadow``) and
refuses unless it beats live by the configured margin over at least the configured number of
tasks. ``--force`` promotes anyway; every promotion is appended to ``ml/models/promotions.log``.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

import joblib

from . import ML_DIR

MODELS_DIR = ML_DIR / "models"
STATUSES = ("live", "shadow", "retired")


def registry_path(root: Path = MODELS_DIR) -> Path:
    return root / "registry.json"


def load_registry(root: Path = MODELS_DIR) -> dict:
    path = registry_path(root)
    if not path.exists():
        return {"models": {}}
    registry = json.loads(path.read_text(encoding="utf-8"))
    # Entries written before prompt 21 have no statuses: derive them.
    for entry in registry["models"].values():
        entry.setdefault("shadow", None)
        for v in entry["versions"]:
            v.setdefault("status", "live" if v["version"] == entry.get("live")
                         else "shadow" if v["version"] == entry.get("shadow") else "retired")
    return registry


def _write(registry: dict, root: Path) -> None:
    registry_path(root).write_text(json.dumps(registry, indent=2) + "\n", encoding="utf-8")


def _set_status(entry: dict, version: int, status: str) -> None:
    """Gives ``version`` the status; the version that had it (live or shadow) is retired."""
    if status in ("live", "shadow"):
        previous = entry.get(status)
        if previous is not None and previous != version:
            for v in entry["versions"]:
                if v["version"] == previous:
                    v["status"] = "retired"
        entry[status] = version
    for other in ("live", "shadow"):
        if other != status and entry.get(other) == version:
            entry[other] = None
    for v in entry["versions"]:
        if v["version"] == version:
            v["status"] = status


def next_version(model: str, root: Path = MODELS_DIR) -> int:
    folder = root / model
    versions = [int(p.name[1:]) for p in folder.glob("v*") if p.name[1:].isdigit()]
    return max(versions, default=0) + 1


def save(model: str, obj, meta: dict, root: Path = MODELS_DIR, live: bool = True,
         status: str | None = None) -> Path:
    """Stores a new version; ``status`` (default live, or retired with live=False) says what it is."""
    status = status or ("live" if live else "retired")
    if status not in STATUSES:
        raise ValueError(f"status must be one of {STATUSES}")
    version = next_version(model, root)
    folder = root / model / f"v{version}"
    folder.mkdir(parents=True, exist_ok=False)
    joblib.dump(obj, folder / "model.joblib")
    meta.update(model=model, version=version)  # the caller's copy learns its version too
    (folder / "meta.json").write_text(json.dumps(meta, indent=2, default=str) + "\n",
                                      encoding="utf-8")
    registry = load_registry(root)
    entry = registry["models"].setdefault(model, {"live": None, "shadow": None, "versions": []})
    entry.setdefault("shadow", None)
    entry["versions"].append({"version": version, "path": f"{model}/v{version}",
                              "trained_at": meta.get("trained_at"),
                              "kept": meta.get("kept", {}).get("candidate"),
                              "data_hash": meta.get("data_hash"), "status": status})
    _set_status(entry, version, status)
    _write(registry, root)
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


def shadow_version(model: str, root: Path = MODELS_DIR) -> int | None:
    entry = load_registry(root)["models"].get(model) or {}
    return entry.get("shadow")


def set_shadow(model: str, version: int, root: Path = MODELS_DIR) -> None:
    registry = load_registry(root)
    entry = registry["models"][model]
    if not any(v["version"] == version for v in entry["versions"]):
        raise ValueError(f"{model} has no version {version}")
    if entry.get("live") == version:
        raise ValueError(f"{model} v{version} is live; a shadow must be another version")
    _set_status(entry, version, "shadow")
    _write(registry, root)


class PromotionRefused(Exception):
    pass


def promote(model: str, version: int, comparison: dict | None, margin: float, min_tasks: int,
            force: bool = False, root: Path = MODELS_DIR, log=print) -> dict:
    """Makes ``version`` live when ``comparison`` (``compare_shadow``) shows it beats live by
    ``margin`` over at least ``min_tasks`` tasks; otherwise raises PromotionRefused, unless
    ``force``. Returns the promotion record, also appended to promotions.log."""
    registry = load_registry(root)
    entry = registry["models"].get(model)
    if entry is None or not any(v["version"] == version for v in entry["versions"]):
        raise ValueError(f"{model} has no version {version}")
    live = entry.get("live")
    if live == version:
        raise ValueError(f"{model} v{version} is already live")
    reasons = []
    if comparison is None:
        reasons.append("no comparison with live on completed tasks")
    else:
        n = comparison.get("tasks", 0)
        if n < min_tasks:
            reasons.append(f"only {n} compared tasks, {min_tasks} needed")
        live_mae, cand_mae = comparison.get("live_mae"), comparison.get("shadow_mae")
        if live_mae is None or cand_mae is None:
            reasons.append("no MAE for one of the two versions")
        elif cand_mae > (1 - margin) * live_mae:
            reasons.append(f"MAE {cand_mae:.2f} does not beat live's {live_mae:.2f} by"
                           f" {margin:.0%}")
    if reasons and not force:
        raise PromotionRefused("; ".join(reasons))
    record = {"at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
              "model": model, "version": version, "previous_live": live, "forced": bool(reasons),
              "reasons_overridden": reasons, "comparison": comparison}
    if reasons:
        log(f"WARNING: promoting {model} v{version} with --force despite: {'; '.join(reasons)}")
    _set_status(entry, version, "live")
    _write(registry, root)
    with open(root / "promotions.log", "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
    log(f"{model} v{version} is live (was v{live})")
    return record


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Model registry: list, shadow, promote.")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list")
    shadow = sub.add_parser("shadow", help="run a version beside live (F14)")
    shadow.add_argument("model")
    shadow.add_argument("version", type=int)
    prom = sub.add_parser("promote", help="make a version live if it beats live")
    prom.add_argument("model")
    prom.add_argument("version", type=int)
    prom.add_argument("--force", action="store_true")
    prom.add_argument("--predictions", default=None, help="prediction log (JSONL)")
    args = parser.parse_args(argv)
    if args.command == "list":
        for model, entry in load_registry()["models"].items():
            print(f"{model}: live v{entry.get('live')}, shadow "
                  f"{'v' + str(entry['shadow']) if entry.get('shadow') else 'none'}")
            for v in entry["versions"]:
                print(f"  v{v['version']:<3} {v.get('status', '?'):<8} {v.get('kept')}"
                      f"  trained {v.get('trained_at')}")
        return 0
    if args.command == "shadow":
        set_shadow(args.model, args.version)
        print(f"{args.model} v{args.version} runs in shadow mode (reloaded by the server)")
        return 0
    from .compare_shadow import compare_from_logs
    from .data import load_config
    config = load_config().get("promotion", {})
    comparison = compare_from_logs(args.model, shadow=args.version,
                                   predictions=Path(args.predictions) if args.predictions else None)
    try:
        promote(args.model, args.version, comparison, float(config.get("margin", 0.05)),
                int(config.get("minTasks", 200)), force=args.force)
    except PromotionRefused as e:
        print(f"refused: {e} (--force to override)")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
