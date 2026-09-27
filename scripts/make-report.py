"""Benchmark report (prompt 20, F18): HTML and PDF from a suite's results.

    python scripts/make-report.py <suite-id> [--config configs/benchmark.yaml]

Reads results/benchmark/<suite-id>/runs.csv, summary.csv and the per-run replay CSVs, and writes
report.html (charts embedded) and report.pdf next to them. The PDF is printed from the HTML by
headless Edge or Chrome (WeasyPrint needs GTK libraries that Windows does not ship); pass
--no-pdf to skip it. Headlines are generated from the numbers: a difference is only called one
when Welch's t-test gives p < 0.05.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import io
import math
import shutil
import subprocess
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import yaml  # noqa: E402
from jinja2 import Template  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
STRATEGIES = ["round_robin", "random", "least_loaded", "resource_aware", "predictive"]
# Categorical slots 1-5 of the reference palette, in fixed order (identity follows the strategy).
COLORS = dict(zip(STRATEGIES, ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]))
LABELS = {"round_robin": "Round robin", "random": "Random", "least_loaded": "Least loaded",
          "resource_aware": "Resource-aware", "predictive": "Predictive"}
METRICS = [  # column, label, unit, lower is better (None: no direction)
    ("mean_latency_ms", "Mean latency", "ms", True),
    ("p95_latency_ms", "p95 latency", "ms", True),
    ("p99_latency_ms", "p99 latency", "ms", True),
    ("throughput_per_s", "Throughput", "tasks/s", False),
    ("makespan_ms", "Makespan", "ms", True),
    ("cpu_mean_pct", "CPU mean", "%", None),
    ("cpu_var", "CPU variance", "", True),
    ("imbalance", "Imbalance (std of tasks/worker)", "", True),
    ("max_queue", "Max queue", "tasks", True),
    ("sla_violation_pct", "SLA violations", "%", True),
    ("overhead_mean_us", "Scheduling overhead (mean)", "µs", True),
    ("overhead_p95_us", "Scheduling overhead (p95)", "µs", True),
    ("mae_ms", "Live exec-time MAE", "ms", True),
]
HEADLINE_METRICS = ["mean_latency_ms", "p95_latency_ms", "throughput_per_s",
                    "sla_violation_pct"]


def style(ax, title=None):
    if title:
        ax.set_title(title, fontsize=10, loc="left")
    ax.grid(True, color="#e5e5e5", linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)


def png(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def fmt(v, digits=1):
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return "n/a"
    return f"{v:,.{digits}f}"


def p_text(p):
    return "p < 0.001" if p < 0.001 else f"p = {p:.3f}"


def fmt_p(p):
    if p is None or (isinstance(p, float) and math.isnan(p)):
        return "n/a"
    return "< 0.001" if p < 0.001 else f"{p:.3f}"


# --- data --------------------------------------------------------------------------------
def load(suite_dir: Path):
    runs = pd.read_csv(suite_dir / "runs.csv")
    summary = pd.read_csv(suite_dir / "summary.csv")
    tasks = []
    for f in sorted((suite_dir / "runs").glob("*.csv")):
        df = pd.read_csv(f)
        run_id = f.stem
        meta = runs[runs["run_id"] == run_id]
        if meta.empty:
            continue
        df["run_id"] = run_id
        df["scenario"] = meta["scenario"].iat[0]
        df["strategy"] = meta["strategy"].iat[0]
        df["rep"] = meta["rep"].iat[0]
        tasks.append(df)
    return runs, summary, pd.concat(tasks, ignore_index=True) if tasks else pd.DataFrame()


def headlines(summary: pd.DataFrame, scenarios: list[str]) -> list[dict]:
    out = []
    for scenario in scenarios:
        items = []
        for metric in HEADLINE_METRICS:
            row = summary[(summary.scenario == scenario) & (summary.strategy == "predictive")
                          & (summary.metric == metric)]
            if row.empty or pd.isna(row["p_value"].iat[0]):
                continue
            r = row.iloc[0]
            label = next(m[1] for m in METRICS if m[0] == metric)
            base = LABELS.get(r["best_baseline"], r["best_baseline"])
            diff = r["diff_pct"]
            p = r["p_value"]
            lower_better = next(m[3] for m in METRICS if m[0] == metric)
            if pd.isna(diff):
                items.append({"text": f"{label}: 0 for every strategy (no difference)",
                              "significant": False, "better": False})
                continue
            if p < 0.05:
                better = (diff < 0) == bool(lower_better)
                verdict = "better" if better else "worse"
                text = (f"{label}: predictive {diff:+.1f} % vs {base} "
                        f"({p_text(p)}, d = {r['cohens_d']:.2f}): {verdict}")
            else:
                text = (f"{label}: no significant difference vs {base} "
                        f"({diff:+.1f} %, {p_text(p)})")
            items.append({"text": text, "significant": p < 0.05,
                          "better": p < 0.05 and ((diff < 0) == bool(lower_better))})
        out.append({"scenario": scenario, "items": items})
    return out


# --- charts ------------------------------------------------------------------------------
def legend_handles():
    return [plt.Line2D([0], [0], color=COLORS[s], lw=2, label=LABELS[s]) for s in STRATEGIES]


def cdf_chart(tasks: pd.DataFrame, scenarios: list[str]) -> str:
    n = len(scenarios)
    fig, axes = plt.subplots(1, n, figsize=(3.2 * n, 3.2), sharey=True)
    axes = np.atleast_1d(axes)
    for ax, scenario in zip(axes, scenarios):
        part = tasks[(tasks.scenario == scenario) & (tasks.status == "COMPLETED")]
        for s in STRATEGIES:
            lat = np.sort(part[part.strategy == s]["latency_ms"].to_numpy())
            if len(lat):
                ax.plot(lat, np.arange(1, len(lat) + 1) / len(lat), color=COLORS[s], lw=2)
        ax.set_xscale("log")
        ax.set_xlabel("latency (ms, log)")
        style(ax, scenario)
    axes[0].set_ylabel("fraction of tasks")
    fig.legend(handles=legend_handles(), loc="lower center", ncol=5, frameon=False,
               bbox_to_anchor=(0.5, -0.08))
    fig.tight_layout()
    return png(fig)


def bar_chart(runs: pd.DataFrame, scenarios: list[str], metric: str, ylabel: str) -> str:
    fig, ax = plt.subplots(figsize=(10, 3.4))
    width = 0.16
    x = np.arange(len(scenarios))
    for i, s in enumerate(STRATEGIES):
        g = runs[runs.strategy == s].groupby("scenario")[metric]
        means = [g.mean().get(sc, np.nan) for sc in scenarios]
        stds = [g.std().get(sc, np.nan) for sc in scenarios]
        ax.bar(x + (i - 2) * width, means, width * 0.9, yerr=stds, color=COLORS[s],
               label=LABELS[s], error_kw={"lw": 1, "capsize": 2, "ecolor": "#555555"})
    ax.set_xticks(x, scenarios)
    ax.set_ylabel(ylabel)
    style(ax)
    ax.legend(frameon=False, ncol=5, fontsize=8, loc="upper center", bbox_to_anchor=(0.5, 1.15))
    fig.tight_layout()
    return png(fig)


def queue_chart(tasks: pd.DataFrame, scenarios: list[str]) -> str:
    n = len(scenarios)
    fig, axes = plt.subplots(1, n, figsize=(3.2 * n, 3.0), sharey=False)
    axes = np.atleast_1d(axes)
    for ax, scenario in zip(axes, scenarios):
        for s in STRATEGIES:
            part = tasks[(tasks.scenario == scenario) & (tasks.strategy == s) & (tasks.rep == 1)]
            if part.empty:
                continue
            t0 = part["submit_ms"].min()
            start = part["start_ms"].fillna(part["end_ms"])
            ev = sorted([(a - t0, 1) for a in part["submit_ms"]]
                        + [(b - t0, -1) for b in start])
            xs, ys, level = [0.0], [0], 0
            for t, d in ev:
                level += d
                xs.append(t / 1000.0)
                ys.append(level)
            ax.step(xs, ys, where="post", color=COLORS[s], lw=1.5)
        ax.set_xlabel("time since first submit (s)")
        style(ax, scenario)
    axes[0].set_ylabel("tasks waiting (rep 1)")
    fig.legend(handles=legend_handles(), loc="lower center", ncol=5, frameon=False,
               bbox_to_anchor=(0.5, -0.1))
    fig.tight_layout()
    return png(fig)


def imbalance_box(runs: pd.DataFrame, scenarios: list[str]) -> str:
    n = len(scenarios)
    fig, axes = plt.subplots(1, n, figsize=(3.2 * n, 3.0), sharey=True)
    axes = np.atleast_1d(axes)
    for ax, scenario in zip(axes, scenarios):
        data = [runs[(runs.scenario == scenario) & (runs.strategy == s)]["imbalance"].dropna()
                for s in STRATEGIES]
        bp = ax.boxplot(data, patch_artist=True, widths=0.6, medianprops={"color": "#222222"})
        for patch, s in zip(bp["boxes"], STRATEGIES):
            patch.set_facecolor(COLORS[s])
            patch.set_alpha(0.85)
        ax.set_xticks(range(1, len(STRATEGIES) + 1), ["RR", "Rnd", "LL", "RA", "Pred"],
                      fontsize=8)
        style(ax, scenario)
    axes[0].set_ylabel("std of tasks per worker")
    fig.tight_layout()
    return png(fig)


def radar_chart(runs: pd.DataFrame) -> str:
    axes_def = [("mean_latency_ms", "Latency", True), ("throughput_per_s", "Throughput", False),
                ("imbalance", "Balance", True), ("sla_violation_pct", "SLA", True),
                ("overhead_mean_us", "Overhead", True)]
    # Per scenario, each metric's strategy means are scaled to [0, 1] with 1 = best; the radar
    # shows the average over scenarios.
    scores = {s: [] for s in STRATEGIES}
    for col, _, lower in axes_def:
        per = {s: [] for s in STRATEGIES}
        for scenario, g in runs.groupby("scenario"):
            means = g.groupby("strategy")[col].mean()
            lo, hi = means.min(), means.max()
            for s in STRATEGIES:
                v = means.get(s, np.nan)
                if np.isnan(v) or hi == lo:
                    per[s].append(1.0 if hi == lo else np.nan)
                else:
                    x = (v - lo) / (hi - lo)
                    per[s].append(1 - x if lower else x)
        for s in STRATEGIES:
            scores[s].append(np.nanmean(per[s]) if per[s] else np.nan)
    angles = np.linspace(0, 2 * np.pi, len(axes_def), endpoint=False).tolist()
    fig, ax = plt.subplots(figsize=(4.6, 4.6), subplot_kw={"polar": True})
    for s in STRATEGIES:
        vals = scores[s] + scores[s][:1]
        ax.plot(angles + angles[:1], vals, color=COLORS[s], lw=2, label=LABELS[s])
    ax.set_xticks(angles, [a[1] for a in axes_def])
    ax.set_yticks([0.25, 0.5, 0.75, 1.0], ["", "", "", "best"], fontsize=7)
    ax.set_ylim(0, 1.05)
    ax.legend(frameon=False, fontsize=8, loc="upper center", bbox_to_anchor=(0.5, -0.06),
              ncol=3)
    fig.tight_layout()
    return png(fig)


# --- tables ------------------------------------------------------------------------------
def scenario_tables(summary: pd.DataFrame, scenarios: list[str]) -> list[dict]:
    tables = []
    shown = ["mean_latency_ms", "p95_latency_ms", "p99_latency_ms", "throughput_per_s",
             "makespan_ms", "imbalance", "max_queue", "sla_violation_pct", "overhead_mean_us",
             "cpu_mean_pct", "mae_ms"]
    heads = [next(m[1] for m in METRICS if m[0] == c) for c in shown]
    for scenario in scenarios:
        rows = []
        for s in STRATEGIES:
            cells = []
            for c in shown:
                r = summary[(summary.scenario == scenario) & (summary.strategy == s)
                            & (summary.metric == c)]
                if r.empty or pd.isna(r["mean"].iat[0]):
                    cells.append("n/a")
                    continue
                m, sd = r["mean"].iat[0], r["std"].iat[0]
                digits = 2 if c in ("imbalance", "throughput_per_s", "sla_violation_pct") else 0
                cells.append(fmt(m, digits) + ("" if pd.isna(sd) else " ± " + fmt(sd, digits)))
            rows.append({"strategy": LABELS[s], "cells": cells,
                         "predictive": s == "predictive"})
        tests = []
        for c in shown:
            r = summary[(summary.scenario == scenario) & (summary.strategy == "predictive")
                        & (summary.metric == c)]
            if r.empty or pd.isna(r["p_value"].iat[0]):
                tests.append("")
            else:
                tests.append(f"{r['diff_pct'].iat[0]:+.1f} % vs {LABELS.get(r['best_baseline'].iat[0], '')}"
                             f", p {fmt_p(r['p_value'].iat[0])}")
        tables.append({"scenario": scenario, "heads": heads, "rows": rows, "tests": tests})
    return tables


TEMPLATE = Template(r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Benchmark {{ suite_id }}</title>
<style>
:root { --text: #1f1f1d; --muted: #5f5e58; --rule: #e2e1dc; --bg: #ffffff; --good: #1baf7a; --bad: #e34948; }
body { font-family: "Segoe UI", system-ui, sans-serif; color: var(--text); background: var(--bg);
       max-width: 1100px; margin: 0 auto; padding: 24px 16px 64px; line-height: 1.45; }
h1 { font-size: 26px; margin-bottom: 4px; } h2 { font-size: 19px; margin-top: 36px;
     border-bottom: 1px solid var(--rule); padding-bottom: 4px; } h3 { font-size: 15px; }
.muted { color: var(--muted); } table { border-collapse: collapse; width: 100%; font-size: 12px;
         margin: 8px 0 16px; } th, td { border-bottom: 1px solid var(--rule); padding: 4px 6px;
         text-align: right; white-space: nowrap; } th:first-child, td:first-child { text-align: left; }
tr.pred td { font-weight: 600; } tr.tests td { color: var(--muted); font-size: 11px; }
.headline { margin: 4px 0; } .sig.better::before { content: "▲ "; color: var(--good); }
.sig.worse::before { content: "▼ "; color: var(--bad); } .nosig::before { content: "● "; color: var(--muted); }
img { max-width: 100%; } .box { border: 1px solid var(--rule); border-radius: 6px; padding: 10px 14px; }
@media print { body { max-width: none; } h2 { break-before: auto; } img { break-inside: avoid; } }
</style></head><body>
<h1>PrediSched benchmark {{ suite_id }}</h1>
<p class="muted">Generated {{ generated }} by scripts/make-report.py from results/benchmark/{{ suite_id }}/.
{{ n_runs }} runs: {{ scenarios|length }} scenarios × {{ strategies|length }} strategies × {{ reps }} repetitions.</p>

<h2>Headlines: predictive vs the best baseline</h2>
<p class="muted">Per scenario, the best baseline is the reactive strategy with the best mean for that metric.
A difference is only called one at p &lt; 0.05 (two-sided Welch t-test over the repetitions; d is Cohen's d).
▲ predictive better, ▼ worse, ● no significant difference.</p>
{% for h in headlines %}<div class="box" style="margin-bottom:8px"><strong>{{ h.scenario }}</strong>
{% for i in h["items"] %}<div class="headline {% if i.significant %}sig {{ 'better' if i.better else 'worse' }}{% else %}nosig{% endif %}">{{ i.text }}</div>{% endfor %}</div>
{% endfor %}

<h2>Setup and controls</h2>
<ul>
<li>Every run starts a fresh cluster of real processes on one laptop (Intel i5-8350U, 4 cores / 8 threads, 15 W):
one scheduler (<code>{{ node_config }}</code>, PostgreSQL history on, result cache off, speculation off) and three workers.
The prediction server and the mock HTTP target run for the whole suite.</li>
<li>Each scenario replays one saved trace from <code>{{ trace_dir }}</code>, identical for every strategy and repetition
(SHA-256 checked before every run). Trace seeds were used neither for the training dataset nor for λ tuning.</li>
<li>Repetition-major order with the strategy order rotated each repetition, so machine drift spreads over strategies.</li>
<li>The first {{ warmup_pct }} % of each trace's tasks are excluded from the latency percentiles (warm-up).
Every task carries a {{ deadline }} ms deadline; SLA violation = failed or later than that.</li>
<li>Latency is submit → completion as seen by the client (status polled every 50 ms).</li>
</ul>
<table><tr><th>Scenario</th><th>Profile</th><th>Pattern</th><th>Rate (/s)</th><th>Tasks</th><th>Seed</th><th>Workers</th><th>Fault</th></tr>
{% for s in scenario_rows %}<tr><td>{{ s.name }}</td><td>{{ s.profile }}</td><td>{{ s.pattern }}</td><td>{{ s.rate }}</td><td>{{ s.tasks }}</td><td>{{ s.seed }}</td><td>{{ s.workers }}</td><td>{{ s.fault }}</td></tr>{% endfor %}
</table>

<h2>Results per scenario</h2>
<p class="muted">Mean ± standard deviation over the repetitions. The last line gives predictive vs the best baseline for each column.</p>
{% for t in tables %}<h3>{{ t.scenario }}</h3>
<table><tr><th>Strategy</th>{% for h in t.heads %}<th>{{ h }}</th>{% endfor %}</tr>
{% for r in t.rows %}<tr class="{{ 'pred' if r.predictive else '' }}"><td>{{ r.strategy }}</td>{% for c in r.cells %}<td>{{ c }}</td>{% endfor %}</tr>{% endfor %}
<tr class="tests"><td>predictive vs best</td>{% for c in t.tests %}<td>{{ c }}</td>{% endfor %}</tr>
</table>{% endfor %}

<h2>Latency distribution</h2>
<p class="muted">Every completed task of every repetition, per strategy (CDF, log scale).</p>
<img alt="latency CDF per scenario" src="data:image/png;base64,{{ cdf }}">

<h2>Throughput</h2>
<img alt="throughput bars" src="data:image/png;base64,{{ throughput }}">
<h2>p95 latency</h2>
<img alt="p95 latency bars" src="data:image/png;base64,{{ p95 }}">

<h2>Queue over time</h2>
<p class="muted">Tasks submitted but not yet started, repetition 1 of each strategy.</p>
<img alt="queue over time" src="data:image/png;base64,{{ queue }}">

<h2>Worker imbalance</h2>
<img alt="imbalance box plots" src="data:image/png;base64,{{ imbalance }}">

<h2>Overall profile</h2>
<p class="muted">Each axis scales the strategies' means to [0, 1] within a scenario (1 = best), averaged over scenarios.
A summary, not a test: read the tables for the numbers.</p>
<img alt="radar chart" src="data:image/png;base64,{{ radar }}">

<h2>Scheduling overhead and prediction error</h2>
<table><tr><th>Strategy</th><th>Decision time mean (µs)</th><th>Decision time p95 (µs)</th></tr>
{% for o in overhead %}<tr><td>{{ o.strategy }}</td><td>{{ o.mean }}</td><td>{{ o.p95 }}</td></tr>{% endfor %}
</table>
<table><tr><th>Scenario</th><th>Live exec-time MAE of predictive (ms)</th></tr>
{% for m in mae %}<tr><td>{{ m.scenario }}</td><td>{{ m.value }}</td></tr>{% endfor %}
</table>

<h2>Limitations</h2>
<ul>
<li>Everything runs on one 4-core laptop: workers share the CPU, so "heterogeneous" hardware is simulated with
pool sizes and a slowdown factor, and the prediction server competes with the workers it predicts.</li>
<li>Five repetitions per cell: small differences can go undetected, and one test per metric and scenario is
reported without a multiple-comparison correction.</li>
<li>The models were trained on one campaign (prompt 15) on this same machine; the queue-forecast model never beat its baseline,
and the overload model's threshold did not transfer to the held-out pattern (docs/components/ml-models.md).</li>
<li>ML predictions can be wrong, and a poor model can make scheduling worse than a simple baseline; prediction calls add overhead
to every decision (see above).</li>
<li>Historical data may not capture sudden, unseen spikes; workload patterns drift, so models need retraining (prompt 21).</li>
<li>New task types face a cold start (a type-average fallback); monitoring and heartbeats add their own overhead.</li>
<li>A single active scheduler is a throughput ceiling; these results say nothing about larger clusters.</li>
</ul>
</body></html>
""")


def print_pdf(html: Path, pdf: Path) -> bool:
    candidates = [shutil.which("msedge"), shutil.which("chrome"), shutil.which("google-chrome"),
                  r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
                  r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
                  r"C:\Program Files\Google\Chrome\Application\chrome.exe"]
    browser = next((c for c in candidates if c and Path(c).exists()), None)
    if not browser:
        print("no Edge/Chrome found: report.pdf not written", file=sys.stderr)
        return False
    subprocess.run([browser, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
                    f"--print-to-pdf={pdf}", html.resolve().as_uri()],
                   check=False, capture_output=True, timeout=120)
    return pdf.exists()


def main(argv=None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("suite_id")
    parser.add_argument("--config", default=str(REPO / "configs" / "benchmark.yaml"))
    parser.add_argument("--no-pdf", action="store_true")
    args = parser.parse_args(argv)
    suite_dir = REPO / "results" / "benchmark" / args.suite_id
    config = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    runs, summary, tasks = load(suite_dir)
    scenarios = [s["name"] for s in config["scenarios"] if s["name"] in set(runs.scenario)]
    heads = headlines(summary, scenarios)
    overhead = []
    for s in STRATEGIES:
        g = runs[runs.strategy == s]
        overhead.append({"strategy": LABELS[s], "mean": fmt(g["overhead_mean_us"].mean(), 0),
                         "p95": fmt(g["overhead_p95_us"].mean(), 0)})
    mae = [{"scenario": sc, "value": fmt(runs[(runs.scenario == sc)
                                             & (runs.strategy == "predictive")]["mae_ms"].mean())}
           for sc in scenarios]
    html = TEMPLATE.render(
        suite_id=args.suite_id, generated=dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
        n_runs=len(runs), scenarios=scenarios, strategies=STRATEGIES,
        reps=int(runs["rep"].max()) if len(runs) else 0, headlines=heads,
        node_config=config["nodeConfig"], trace_dir=config["traceDir"],
        warmup_pct=int(round(100 * config["warmupFraction"])), deadline=config["deadlineMs"],
        scenario_rows=[{"name": s["name"], "profile": s["profile"], "pattern": s["pattern"],
                        "rate": s["rate"], "tasks": s["tasks"], "seed": s["seed"],
                        "workers": s["workers"],
                        "fault": (f"kill {s['killWorker']} at {s['killAtMs'] / 1000:.0f} s"
                                  if s.get("killWorker") else "")}
                       for s in config["scenarios"]],
        tables=scenario_tables(summary, scenarios),
        cdf=cdf_chart(tasks, scenarios),
        throughput=bar_chart(runs, scenarios, "throughput_per_s", "tasks/s (mean ± std)"),
        p95=bar_chart(runs, scenarios, "p95_latency_ms", "p95 latency, ms (mean ± std)"),
        queue=queue_chart(tasks, scenarios), imbalance=imbalance_box(runs, scenarios),
        radar=radar_chart(runs), overhead=overhead, mae=mae)
    html_path = suite_dir / "report.html"
    html_path.write_text(html, encoding="utf-8")
    print(f"wrote {html_path}")
    if not args.no_pdf and print_pdf(html_path, suite_dir / "report.pdf"):
        print(f"wrote {suite_dir / 'report.pdf'}")
    print("\nHeadlines (predictive vs best baseline):")
    for h in heads:
        print(f"  {h['scenario']}:")
        for i in h["items"]:
            print(f"    - {i['text']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
