"""Exp 10 speedup chart and table from results/exp10-matmul.csv (written by predisched_mpi.matmul).

    python scripts/plot-matmul.py results/exp10-matmul.csv [--out docs/img/exp10-speedup.png]

Speedup S(p) = T(1) / T(p) on the median total time (Bcast + Scatterv + compute + Gatherv);
efficiency E(p) = S(p) / p. Prints the table and draws speedup vs processes, one line per n,
with the ideal S = p dashed.
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
# Categorical slots 1-3 of the reference palette, in fixed order, plus chart chrome.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]
MARKERS = ["o", "s", "^"]
SURFACE, INK, INK_2, MUTED, GRID, AXIS = ("#fcfcfb", "#0b0b0b", "#52514e", "#898781",
                                         "#e1e0d9", "#c3c2b7")


def load(path: Path) -> dict[int, dict[int, dict]]:
    by_n: dict[int, dict[int, dict]] = defaultdict(dict)
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            by_n[int(row["n"])][int(row["procs"])] = row
    return by_n


def table(by_n) -> list[tuple]:
    rows = []
    for n in sorted(by_n):
        base = float(by_n[n][1]["total_ms"])
        for p in sorted(by_n[n]):
            r = by_n[n][p]
            total = float(r["total_ms"])
            speedup = base / total
            rows.append((n, p, float(r["scatter_bcast_ms"]), float(r["compute_ms"]),
                         float(r["gather_ms"]), total, speedup, speedup / p, r["verified"]))
    return rows


def plot(by_n, rows, out: Path) -> None:
    fig, ax = plt.subplots(figsize=(6.4, 4.2), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    procs = sorted({p for n in by_n for p in by_n[n]})
    ax.plot(procs, procs, linestyle=(0, (4, 3)), linewidth=1.5, color=MUTED, label="ideal (S = p)")
    ax.annotate("ideal", (procs[-1], procs[-1]), textcoords="offset points", xytext=(-6, 4),
                ha="right", color=MUTED, fontsize=9)
    for i, n in enumerate(sorted(by_n)):
        points = [(r[1], r[6]) for r in rows if r[0] == n]
        xs, ys = zip(*points)
        ax.plot(xs, ys, color=SERIES[i], linewidth=2, marker=MARKERS[i], markersize=7,
                markeredgecolor=SURFACE, markeredgewidth=1.5, label=f"n = {n}", zorder=3)
        ax.annotate(f"n = {n}  ({ys[-1]:.2f}x)", (xs[-1], ys[-1]), textcoords="offset points",
                    xytext=(8, 0), va="center", color=INK_2, fontsize=9)
    ax.set_xticks(procs)
    ax.set_xlim(procs[0] - 0.2, procs[-1] + 1.4)
    ax.set_ylim(0, procs[-1] + 0.3)
    ax.set_xlabel("MPI processes", color=INK_2)
    ax.set_ylabel("Speedup  T(1) / T(p)", color=INK_2)
    ax.set_title("Exp 10: parallel matrix multiplication speedup", color=INK, loc="left",
                 fontsize=11)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
    ax.tick_params(colors=MUTED)
    legend = ax.legend(loc="upper left", frameon=False, fontsize=9)
    for text in legend.get_texts():
        text.set_color(INK_2)
    fig.text(0.01, 0.01, "Median of 3 runs; total = Bcast + Scatterv + compute + Gatherv."
             " One BLAS thread per rank.", color=MUTED, fontsize=7.5)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, facecolor=SURFACE)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("csv", nargs="?", default=str(REPO / "results" / "exp10-matmul.csv"))
    parser.add_argument("--out", default=str(REPO / "docs" / "img" / "exp10-speedup.png"))
    args = parser.parse_args()
    by_n = load(Path(args.csv))
    rows = table(by_n)
    print(f"{'n':>5} {'procs':>5} {'bcast+scatter':>13} {'compute':>9} {'gather':>8} {'total':>9}"
          f" {'speedup':>8} {'efficiency':>10} {'verified':>8}")
    for n, p, sc, co, ga, to, s, e, v in rows:
        print(f"{n:>5} {p:>5} {sc:>10.3f} ms {co:>6.3f} ms {ga:>5.3f} ms {to:>6.3f} ms"
              f" {s:>7.2f}x {e:>9.0%} {v:>8}")
    plot(by_n, rows, Path(args.out))
    print(f"chart written to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
