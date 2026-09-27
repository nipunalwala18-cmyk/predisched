"""Label builder (prompt 15): execution_history + worker_metrics -> ml/data/dataset.parquet + card.

    python ml/build_dataset.py [--config configs/campaign.yaml] [--features ml/data/features]

For each execution of the campaign (its rows are tagged with run_id / profile / pattern / rate
through ``benchmark_runs``) it adds:

    queue_len_future    the worker's queue length N seconds after dispatch: the last
                        worker_metrics sample at or before t + N, which must be at most
                        ``sampleToleranceSeconds`` older than t + N
    overloaded_future   whether any sample in (t, t + N] has cpu_pct > cpuThreshold or
                        queue_len >= queueFactor x the worker's pool size

and leak-free features (every feature timestamp <= dispatched_at):

    prior_*             mean / count of exec_time_ms of the same (task_type, worker_id), over
                        executions that had *finished* before this dispatch (prior_stats_ts)
    win_*               the Spark worker_windows row (prompt 12, 10 s windows sliding by 5 s)
                        whose window_end is the latest at or before dispatch (win_window_end)
    worker_pool_size, worker_slowdown   static worker properties from the campaign config

Rows whose labels cannot be computed are dropped, with a count per reason.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

REPO = Path(__file__).resolve().parents[1]
DSN = os.environ.get("PREDISCHED_DSN",
                     "host=localhost port=5432 dbname=predisched user=postgres password=predisched")
log = logging.getLogger("build_dataset")

FEATURE_COLUMNS = [
    "task_type", "resource_profile", "priority", "input_size", "strategy", "worker_id",
    "worker_cores", "worker_pool_size", "worker_slowdown", "concurrent_tasks_on_worker",
    "cpu_pct", "mem_pct", "active_threads", "queue_len", "arrival_rate", "avg_exec_recent",
    "prior_type_worker_mean_ms", "prior_type_worker_count", "win_arrival_rate",
    "win_queue_len_mean", "win_queue_len_max",
]
TARGET_COLUMNS = ["exec_time_ms", "wait_time_ms", "queue_len_future", "overloaded_future"]


# --- labels ---------------------------------------------------------------------------------
def _ns(values: pd.Series) -> np.ndarray:
    """Timestamps as int64 nanoseconds since the epoch (UTC), for fast searchsorted."""
    values = pd.to_datetime(values)
    if values.dt.tz is not None:
        values = values.dt.tz_convert("UTC").dt.tz_localize(None)
    return values.to_numpy(dtype="datetime64[ns]").astype(np.int64)


def compute_labels(history: pd.DataFrame, metrics: pd.DataFrame, pool_sizes: dict[str, int],
                   horizon_s: float, cpu_threshold: float, queue_factor: float,
                   tolerance_s: float) -> pd.DataFrame:
    """Adds queue_len_future and overloaded_future (NaN / NA where they cannot be computed).

    ``history`` needs worker_id and dispatched_at; ``metrics`` needs worker_id, ts, cpu_pct and
    queue_len.
    """
    out = history.copy()
    queue_future = np.full(len(out), np.nan)
    overloaded_future = np.full(len(out), np.nan)
    horizon, tolerance = int(horizon_s * 1e9), int(tolerance_s * 1e9)
    position = pd.Series(np.arange(len(out)), index=out.index)
    for worker, rows in out.groupby("worker_id"):
        samples = metrics[metrics["worker_id"] == worker].sort_values("ts")
        if samples.empty:
            continue
        ts = _ns(samples["ts"])
        queue = samples["queue_len"].to_numpy(dtype=float)
        threshold = queue_factor * pool_sizes.get(worker, np.inf)
        flag = (samples["cpu_pct"].to_numpy(dtype=float) > cpu_threshold) | (queue >= threshold)
        flag_cum = np.concatenate([[0], np.cumsum(flag)])
        start = _ns(rows["dispatched_at"])
        end = start + horizon
        at = position[rows.index].to_numpy()
        # queue_len_future: the last sample at or before t + N, if at most `tolerance` old.
        last = np.searchsorted(ts, end, side="right") - 1
        ok = (last >= 0) & (end - ts[np.clip(last, 0, None)] <= tolerance)
        queue_future[at[ok]] = queue[last[ok]]
        # overloaded_future: any flagged sample in (t, t + N].
        lo = np.searchsorted(ts, start, side="right")
        hi = np.searchsorted(ts, end, side="right")
        seen = hi > lo
        overloaded_future[at[seen]] = ((flag_cum[hi] - flag_cum[lo]) > 0)[seen]
    out["queue_len_future"] = queue_future
    out["overloaded_future"] = pd.array(
        [pd.NA if np.isnan(v) else bool(v) for v in overloaded_future], dtype="boolean")
    return out


# --- features -------------------------------------------------------------------------------
def prior_stats(history: pd.DataFrame) -> pd.DataFrame:
    """Mean and count of exec_time_ms per (task_type, worker_id) over COMPLETED executions that
    finished before each dispatch; ``prior_stats_ts`` is when the last of them finished."""
    out = history.copy()
    finished = (_ns(out["dispatched_at"]) + out["wait_time_ms"].to_numpy(dtype=np.int64)
                * 1_000_000 + out["exec_time_ms"].to_numpy(dtype=np.int64) * 1_000_000)
    finished = pd.Series(finished, index=out.index)
    dispatched = pd.Series(_ns(out["dispatched_at"]), index=out.index)
    mean = pd.Series(np.nan, index=out.index)
    count = pd.Series(0, index=out.index)
    last = pd.Series(np.iinfo(np.int64).min, index=out.index)
    for _, rows in out.groupby(["task_type", "worker_id"]):
        done = rows[rows["status"] == "COMPLETED"]
        order = np.argsort(finished[done.index].to_numpy(), kind="stable")
        done_ts = finished[done.index].to_numpy()[order]
        cum = np.concatenate([[0.0], np.cumsum(done["exec_time_ms"].to_numpy(dtype=float)[order])])
        k = np.searchsorted(done_ts, dispatched[rows.index].to_numpy(), side="right")
        mean[rows.index] = np.where(k > 0, cum[k] / np.maximum(k, 1), np.nan)
        count[rows.index] = k
        if len(done_ts):
            last[rows.index] = np.where(k > 0, done_ts[np.clip(k - 1, 0, None)],
                                        np.iinfo(np.int64).min)
    out["prior_type_worker_mean_ms"] = mean
    out["prior_type_worker_count"] = count
    stamps = pd.to_datetime(last.where(last != np.iinfo(np.int64).min), unit="ns", utc=True)
    tz = pd.to_datetime(out["dispatched_at"]).dt.tz
    out["prior_stats_ts"] = stamps.dt.tz_convert(tz) if tz is not None else stamps.dt.tz_localize(None)
    return out


def join_windows(history: pd.DataFrame, windows: pd.DataFrame | None) -> pd.DataFrame:
    """As-of join of the Spark worker windows: the latest window ending at or before dispatch."""
    out = history.copy()
    cols = {"arrival_rate": "win_arrival_rate", "queue_len_mean": "win_queue_len_mean",
            "queue_len_max": "win_queue_len_max", "window_end": "win_window_end"}
    if windows is None or windows.empty:
        for new in cols.values():
            out[new] = pd.NaT if new == "win_window_end" else np.nan
        return out
    w = windows[["worker_id", "window_end", "arrival_rate", "queue_len_mean",
                 "queue_len_max"]].copy()
    w["window_end"] = pd.to_datetime(w["window_end"])
    if w["window_end"].dt.tz is None:
        w["window_end"] = w["window_end"].dt.tz_localize("UTC")  # features.py writes UTC
    w["window_end"] = w["window_end"].dt.tz_convert(out["dispatched_at"].dt.tz)
    w = w.sort_values("window_end")
    out = out.reset_index().sort_values("dispatched_at")
    merged = pd.merge_asof(out, w.rename(columns={"window_end": "win_window_end"}),
                           left_on="dispatched_at", right_on="win_window_end", by="worker_id",
                           direction="backward", suffixes=("", "_win"))
    merged = merged.rename(columns={"arrival_rate_win": "win_arrival_rate",
                                    "queue_len_mean": "win_queue_len_mean",
                                    "queue_len_max": "win_queue_len_max"})
    return merged.set_index("index").sort_index()


# --- loading --------------------------------------------------------------------------------
def load_from_db(config: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    import psycopg
    name = config["campaign"]
    with psycopg.connect(DSN) as conn:
        runs = pd.DataFrame(conn.execute(
            "SELECT run_id, strategy, metrics FROM benchmark_runs WHERE scenario = %s",
            (f"campaign:{name}",)).fetchall(), columns=["run_id", "run_strategy", "metrics"])
        cur = conn.execute("SELECT * FROM execution_history WHERE task_id LIKE %s",
                           (f"%~{name}-%",))
        history = pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])
        if history.empty:
            return history, runs, pd.DataFrame()
        lo = history["dispatched_at"].min() - dt.timedelta(minutes=1)
        hi = history["dispatched_at"].max() + dt.timedelta(minutes=2)
        cur = conn.execute("SELECT worker_id, ts, cpu_pct, mem_pct, active_threads, queue_len"
                           " FROM worker_metrics WHERE ts BETWEEN %s AND %s", (lo, hi))
        metrics = pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])
    return history, runs, metrics


def tag_runs(history: pd.DataFrame, runs: pd.DataFrame) -> pd.DataFrame:
    """run_id, profile, pattern, rate from the benchmark_runs record whose prefix/suffix match."""
    out = history.copy()
    for col in ("run_id", "profile", "pattern"):
        out[col] = None
    out["rate"] = np.nan
    for _, run in runs.iterrows():
        m = run["metrics"]
        mask = (out["task_id"].str.startswith(m["task_prefix"])
                & out["task_id"].str.endswith(m["id_suffix"]))
        out.loc[mask, ["run_id", "profile", "pattern"]] = [run["run_id"], m["profile"],
                                                           m["pattern"]]
        out.loc[mask, "rate"] = m.get("rate", np.nan)
    return out


def build(history: pd.DataFrame, runs: pd.DataFrame, metrics: pd.DataFrame,
          windows: pd.DataFrame | None, config: dict) -> tuple[pd.DataFrame, dict]:
    labels = config["labels"]
    workers = {w["id"]: w for w in config["workers"]}
    dropped: dict[str, int] = {}
    df = tag_runs(history, runs)
    missing_run = df["run_id"].isna()
    dropped["not part of a recorded campaign run"] = int(missing_run.sum())
    df = df[~missing_run]
    for col in ("cpu_pct", "mem_pct", "arrival_rate", "avg_exec_recent"):
        df[col] = df[col].astype(float)
    metrics = metrics.copy()
    for col in ("cpu_pct",):
        metrics[col] = metrics[col].astype(float)
    df = compute_labels(df, metrics, {k: v["poolSize"] for k, v in workers.items()},
                        labels["horizonSeconds"], labels["cpuThreshold"], labels["queueFactor"],
                        labels["sampleToleranceSeconds"])
    df = prior_stats(df)
    df = join_windows(df, windows)
    df["worker_pool_size"] = df["worker_id"].map(lambda w: workers.get(w, {}).get("poolSize"))
    df["worker_slowdown"] = df["worker_id"].map(
        lambda w: float(workers.get(w, {}).get("slowdown", 1.0)))
    no_future = df["queue_len_future"].isna()
    dropped[f"no worker_metrics sample within {labels['sampleToleranceSeconds']} s of"
            f" t + {labels['horizonSeconds']} s"] = int(no_future.sum())
    df = df[~no_future]
    no_window = df["overloaded_future"].isna()
    dropped["no worker_metrics sample in (t, t + N]"] = int(no_window.sum())
    df = df[~no_window]
    df["queue_len_future"] = df["queue_len_future"].astype(int)
    df["overloaded_future"] = df["overloaded_future"].astype(bool)
    holdout = config.get("holdout", {})
    df["holdout_pattern"] = df["pattern"] == holdout.get("pattern")
    df["holdout_task_type"] = df["task_type"] == holdout.get("taskType")
    keep = (["id", "task_id", "run_id", "profile", "pattern", "rate", "attempt", "status",
             "dispatched_at"] + FEATURE_COLUMNS + TARGET_COLUMNS
            + ["prior_stats_ts", "win_window_end", "holdout_pattern", "holdout_task_type"])
    return df[keep].sort_values("dispatched_at").reset_index(drop=True), dropped


# --- card -----------------------------------------------------------------------------------
def dataset_card(df: pd.DataFrame, dropped: dict, config: dict, total_rows: int) -> str:
    labels, holdout = config["labels"], config.get("holdout", {})
    lines = [
        "# Dataset card: PrediSched execution dataset",
        "",
        f"Built {dt.datetime.now().strftime('%Y-%m-%d %H:%M')} by `ml/build_dataset.py` from"
        f" campaign `{config['campaign']}` (`configs/campaign.yaml`). Spec §12.1.",
        "",
        "## Size",
        "",
        f"- **{len(df):,} rows** (task executions; one per attempt), of {total_rows:,} campaign"
        f" rows in `execution_history`.",
        f"- {df['run_id'].nunique()} runs: {df['profile'].nunique()} profiles x"
        f" {df['pattern'].nunique()} patterns x {df['strategy'].nunique()} strategies.",
        f"- Dispatch times {df['dispatched_at'].min():%Y-%m-%d %H:%M:%S} to"
        f" {df['dispatched_at'].max():%H:%M:%S %Z}.",
        f"- Workers: " + ", ".join(f"{w['id']} (pool {w['poolSize']}, slowdown"
                                   f" {w.get('slowdown', 1.0)}x)" for w in config["workers"])
        + ". The slowdown is simulated: the worker stretches each execution.",
        "",
        "Dropped while building:",
        "",
    ]
    lines += [f"- {reason}: {count:,}" for reason, count in dropped.items()]
    positives = int(df["overloaded_future"].sum())
    lines += [
        "",
        "## Labels",
        "",
        f"- `exec_time_ms`, `wait_time_ms`: measured by the worker / scheduler.",
        f"- `queue_len_future`: the worker's queue length {labels['horizonSeconds']} s after"
        f" dispatch (last `worker_metrics` sample at or before t + {labels['horizonSeconds']} s,"
        f" at most {labels['sampleToleranceSeconds']} s old).",
        f"- `overloaded_future`: some sample in (t, t + {labels['horizonSeconds']} s] has CPU >"
        f" {labels['cpuThreshold']} % or queue length >= {labels['queueFactor']} x the worker's"
        f" pool size.",
        "",
        "Class balance of `overloaded_future`:",
        "",
        "| value | rows | share |",
        "| --- | --- | --- |",
        f"| true | {positives:,} | {positives / len(df):.1%} |",
        f"| false | {len(df) - positives:,} | {(len(df) - positives) / len(df):.1%} |",
        "",
        "By worker (share overloaded): " + ", ".join(
            f"{w} {s:.1%}" for w, s in df.groupby("worker_id")["overloaded_future"].mean().items()),
        "",
        "`queue_len_future` distribution: " + ", ".join(
            f"{int(k)}: {v:,}" for k, v in df["queue_len_future"].value_counts().sort_index()
            .items()),
        "",
        "## Execution time per task type (ms, completed executions)",
        "",
        "| task_type | rows | mean | p50 | p95 | max |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    done = df[df["status"] == "COMPLETED"]
    for task_type, g in done.groupby("task_type")["exec_time_ms"]:
        lines.append(f"| {task_type} | {len(g):,} | {g.mean():.1f} | {g.median():.0f} |"
                     f" {g.quantile(0.95):.0f} | {g.max():,} |")
    lines += [
        "",
        "Rows by status: " + ", ".join(f"{k} {v:,}" for k, v in df["status"].value_counts()
                                       .items()) + ".",
        "",
        "## Runs",
        "",
        "| strategy | rows | runs |",
        "| --- | --- | --- |",
    ]
    for strategy, g in df.groupby("strategy"):
        lines.append(f"| {strategy} | {len(g):,} | {g['run_id'].nunique()} |")
    lines += ["", "| profile | rows |", "| --- | --- |"]
    lines += [f"| {p} | {n:,} |" for p, n in df["profile"].value_counts().sort_index().items()]
    lines += ["", "| pattern | rows |", "| --- | --- |"]
    lines += [f"| {p} | {n:,} |" for p, n in df["pattern"].value_counts().sort_index().items()]
    lines += [
        "",
        "## Hold-out (prompt 16)",
        "",
        f"- Pattern `{holdout.get('pattern')}` ({int(df['holdout_pattern'].sum()):,} rows):"
        " kept out of training to test generalisation to an unseen arrival pattern.",
        f"- Task type `{holdout.get('taskType')}` ({int(df['holdout_task_type'].sum()):,} rows):"
        " kept out to test cold start on an unseen task type.",
        "- Flagged by the `holdout_pattern` / `holdout_task_type` columns; nothing is removed.",
        "",
        "## Columns",
        "",
        "Features (all known at dispatch; `prior_stats_ts` and `win_window_end` are <="
        " `dispatched_at`): " + ", ".join(f"`{c}`" for c in FEATURE_COLUMNS) + ".",
        "",
        "Targets: " + ", ".join(f"`{c}`" for c in TARGET_COLUMNS) + ".",
        "",
        "Bookkeeping: `id`, `task_id`, `run_id`, `profile`, `pattern`, `rate`, `attempt`,"
        " `status`, `dispatched_at`, `prior_stats_ts`, `win_window_end`, `holdout_pattern`,"
        " `holdout_task_type`.",
        "",
        "Not joined on purpose: Spark's `type_worker.parquet` aggregates over the whole history,"
        " so each row's own execution time would leak into its features; `prior_type_worker_*`"
        " is the leak-free equivalent.",
        "",
    ]
    return "\n".join(lines)


def main(argv=None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")  # the card has non-ASCII (the Windows console)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", default=str(REPO / "configs" / "campaign.yaml"))
    parser.add_argument("--features", default=str(REPO / "ml" / "data" / "features"))
    parser.add_argument("--out", default=str(REPO / "ml" / "data" / "dataset.parquet"))
    parser.add_argument("--card", default=str(REPO / "ml" / "data" / "dataset-card.md"))
    args = parser.parse_args(argv)
    with open(args.config, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    history, runs, metrics = load_from_db(config)
    if history.empty:
        log.error("no campaign rows in execution_history: run scripts/collect-dataset.py first")
        return 1
    log.info("loaded %d executions, %d runs, %d worker_metrics samples", len(history),
             len(runs), len(metrics))
    windows_path = Path(args.features) / "worker_windows.parquet"
    windows = pd.read_parquet(windows_path) if windows_path.exists() else None
    if windows is None:
        log.warning("no %s: win_* features left empty (run spark/predisched_spark/features.py)",
                    windows_path)
    df, dropped = build(history, runs, metrics, windows, config)
    for reason, count in dropped.items():
        log.info("dropped %d rows: %s", count, reason)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.out, index=False)
    card = dataset_card(df, dropped, config, len(history))
    Path(args.card).write_text(card, encoding="utf-8")
    log.info("wrote %d rows to %s and the card to %s", len(df), args.out, args.card)
    print(card)
    return 0


if __name__ == "__main__":
    sys.exit(main())
