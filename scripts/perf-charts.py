#!/usr/bin/env python3
"""Draws the performance charts in docs/images/ from the data that scripts/perf-collect.sh measured.

    ~/sdk/venv/bin/python scripts/perf-charts.py        (needs matplotlib)

Nothing is typed in: every number on every chart comes from docs/perf/data/*. Colours are the
validated categorical slots 1-3 (blue, orange, aqua) on a light surface; text always uses ink
tokens, never a series colour; every series is direct-labelled as well as in a legend (aqua is
below 3:1 contrast on this surface, which obliges visible labels).
"""
import csv
import json
import statistics
import sys
import textwrap
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "perf" / "data"
OUT = ROOT / "docs" / "images"
OUT.mkdir(parents=True, exist_ok=True)

# --- design tokens (reference palette, light mode) ---
SURFACE, INK, INK2, MUTED = "#fcfcfb", "#0b0b0b", "#52514e", "#898781"
GRID, BASELINE = "#e1e0d9", "#c3c2b7"
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": ["DejaVu Sans"], "font.size": 10,
    "axes.edgecolor": BASELINE, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "axes.spines.top": False, "axes.spines.right": False, "axes.linewidth": 1,
    "axes.grid": True, "axes.axisbelow": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "lines.solid_capstyle": "round",
})


def read_csv(name):
    with open(DATA / name) as f:
        return list(csv.DictReader(f))


def head(fig, title, subtitle):
    fig.text(0.04, 0.965, title, fontsize=14, fontweight="bold", color=INK, ha="left", va="top")
    width = int(fig.get_figwidth() * 11)          # characters that fit at this font size
    fig.text(0.04, 0.915, textwrap.fill(subtitle, width), fontsize=9.5, color=INK2, ha="left", va="top", linespacing=1.4)


def foot(fig, text):
    fig.text(0.04, 0.012, textwrap.fill(text, 135), fontsize=8, color=MUTED, ha="left", va="bottom", linespacing=1.4)


def style_axis(ax, xgrid=False):
    ax.grid(axis="x", visible=xgrid)
    ax.tick_params(length=0)


ENV = ("Docker Compose on one WSL2 machine (12 logical CPUs), Postgres 16 with durable settings "
       "(fsync on); k6, the stack and the load all share the host.")


def save(fig, name):
    p = OUT / name
    fig.savefig(p, dpi=170)
    plt.close(fig)
    print("wrote", p.relative_to(ROOT))


# ---------------------------------------------------------------- 1. drain throughput
def chart_drain():
    rows = read_csv("scale.csv")
    by = defaultdict(list)
    for r in rows:
        by[int(r["workers"])].append(float(r["jobs_per_second"]))
    ws = sorted(by)
    med = [statistics.median(by[w]) for w in ws]
    jobs = int(rows[0]["jobs"])
    runs = len(by[ws[0]])

    fig, ax = plt.subplots(figsize=(8.6, 4.9))
    fig.subplots_adjust(left=0.10, right=0.96, top=0.77, bottom=0.20)
    best = ws[med.index(max(med))]
    head(fig, "Backlog drain throughput by worker containers",
         f"{jobs:,} no-op jobs preloaded, {runs} runs per point (bar = median). Peak at {best} workers; "
         "past that more containers do not help: one Postgres commits for all of them.")
    xs = range(len(ws))
    ax.bar(xs, med, width=0.5, color=BLUE, zorder=3, linewidth=0)
    for i, w in enumerate(ws):
        n = len(by[w])
        dx = [0.36 + 0.07 * (k - (n - 1) / 2) for k in range(n)]
        ax.scatter([i + d for d in dx], sorted(by[w]), s=26, color=SURFACE, edgecolor=INK2, linewidth=1.3, zorder=4,
                   label="individual run" if i == 0 else None)
        ax.text(i, med[i] + max(med) * 0.03, f"{med[i]:,.0f}", ha="center", va="bottom",
                fontsize=11, fontweight="bold", color=INK, zorder=5)
    ax.set_xticks(list(xs), [f"{w} worker{'s' if w > 1 else ''}" for w in ws])
    ax.set_ylabel("jobs per second")
    ax.legend(frameon=False, loc="upper left", labelcolor=INK2, fontsize=9)
    ax.set_ylim(0, max(max(v) for v in by.values()) * 1.18)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    style_axis(ax)
    foot(fig, ENV + "  Source: docs/perf/data/scale.csv")
    save(fig, "perf-drain-throughput.png")


# ---------------------------------------------------------------- 2. latency vs offered load
def chart_load():
    rows = read_csv("load.csv")
    x = [float(r["offered_rps"]) for r in rows]
    series = [("p50", "p50_ms", BLUE), ("p95", "p95_ms", ORANGE), ("p99", "p99_ms", AQUA)]
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(10.4, 4.9), gridspec_kw={"width_ratios": [1.55, 1]})
    fig.subplots_adjust(left=0.075, right=0.975, top=0.80, bottom=0.17, wspace=0.28)
    head(fig, "Submit latency and throughput as offered load rises",
         "POST /jobs through the containerized API, 20 s per rate, 2 workers draining concurrently. "
         "SLO: p95 under 250 ms.")
    for label, key, color in series:
        y = [float(r[key]) for r in rows]
        ax.plot(x, y, color=color, linewidth=2, marker="o", markersize=7, markeredgecolor=SURFACE,
                markeredgewidth=2, label=label, zorder=3)
        ax.annotate(f"{label}  {y[-1]:.0f} ms", (x[-1], y[-1]), xytext=(9, 0), textcoords="offset points",
                    color=INK, fontsize=9, va="center", annotation_clip=False)
    ax.set_xlabel("offered load (requests per second)")
    ax.set_ylabel("latency (ms)")
    ax.set_xlim(min(x) - 15, max(x) + 85)
    ax.set_ylim(0, max(float(r["p99_ms"]) for r in rows) * 1.15)
    ax.set_xticks(x)
    ax.legend(frameon=False, loc="upper center", ncol=3, labelcolor=INK2, fontsize=9, bbox_to_anchor=(0.5, 1.02))
    style_axis(ax)

    ach = [float(r["achieved_rps"]) for r in rows]
    ax2.plot([min(x), max(x)], [min(x), max(x)], color=BASELINE, linewidth=1.5, linestyle=(0, (4, 3)),
             label="offered", zorder=1)
    ax2.plot(x, ach, color=BLUE, linewidth=2, marker="o", markersize=7, markeredgecolor=SURFACE,
             markeredgewidth=2, label="achieved", zorder=3)
    drops = int(float(rows[-1]["dropped_iterations"]))
    if drops:
        ax2.annotate(f"{drops} iterations dropped\nat the highest rate", (x[-1], ach[-1]), xytext=(-12, -34),
                     textcoords="offset points", color=INK, fontsize=9, ha="right")
    ax2.set_xlabel("offered load (requests per second)")
    ax2.set_ylabel("achieved (requests per second)")
    ax2.set_xticks(x)
    ax2.legend(frameon=False, loc="upper left", labelcolor=INK2, fontsize=9)
    style_axis(ax2)
    foot(fig, "One 20 s run per rate, so p99 is noisy (a single slow second moves it). " + ENV + "  Source: docs/perf/data/load.csv")
    save(fig, "perf-latency-vs-load.png")


# ---------------------------------------------------------------- 3. connection pool
def chart_pool():
    rows = read_csv("pool.csv")
    scen = ["idle", "pressure"]
    names = {"idle": "idle database", "pressure": "database under commit pressure\n(60,000-job backlog\ndraining on 8 workers)"}
    val = {(r["scenario"], int(r["pool"])): r for r in rows}
    fig, axes = plt.subplots(1, 2, figsize=(10.4, 4.9))
    fig.subplots_adjust(left=0.07, right=0.975, top=0.80, bottom=0.24, wspace=0.22)
    head(fig, "Effect of the API connection pool size",
         "Same offered load (400 req/s for 20 s) with the API pool at 10 and at 40 connections. "
         "A pod's ceiling is pool size / commit latency.")
    for ax, (key, title, fmt) in zip(axes, [("p95_ms", "submit p95 latency (ms)", "{:,.0f}"),
                                           ("achieved_rps", "achieved requests per second", "{:.0f}")]):
        w = 0.34
        for si, s in enumerate(scen):
            for pi, (pool, color) in enumerate([(10, ORANGE), (40, BLUE)]):
                if (s, pool) not in val:
                    continue
                v = float(val[(s, pool)][key])
                x = si + (pi - 0.5) * (w + 0.04)
                ax.bar(x, v, width=w, color=color, zorder=3, label=f"pool {pool}" if si == 0 else None)
                ax.text(x, v, fmt.format(v), ha="center", va="bottom", fontsize=10, fontweight="bold", color=INK)
                failed = float(val[(s, pool)]["failed_pct"])
                if failed > 0 and key == "achieved_rps":   # never hide failures behind a throughput number
                    ax.text(x, v * 0.5, f"{failed:.1f}%\nfailed", ha="center", va="center", fontsize=9, color="#ffffff",
                            fontweight="bold")
        ax.set_xticks(range(len(scen)), [names[s] for s in scen], fontsize=8.5)
        ax.set_title(title, loc="left", fontsize=10, color=INK2, pad=10)
        ax.set_ylim(0, max(float(r[key]) for r in rows) * 1.2)
        style_axis(ax)
    axes[0].legend(frameon=False, loc="upper left", labelcolor=INK2, fontsize=9)
    foot(fig, ENV + "  Source: docs/perf/data/pool.csv")
    save(fig, "perf-pool-size.png")


# ---------------------------------------------------------------- 4. chaos recovery
def chaos_inputs():
    rows = [(float(r["t"]), int(r["succeeded"]), int(r["kills"])) for r in read_csv("chaos.csv")]
    summary = json.loads((DATA / "chaos.summary.json").read_text())
    kills_at = []
    prev = 0
    for t, _, k in rows:
        if k > prev:
            kills_at += [t] * (k - prev)
        prev = k
    return rows, summary, kills_at


def chart_chaos():
    rows, summary, kills_at = chaos_inputs()
    t = [r[0] for r in rows]
    y = [r[1] for r in rows]
    fig, ax = plt.subplots(figsize=(9.6, 4.9))
    fig.subplots_adjust(left=0.09, right=0.965, top=0.80, bottom=0.17)
    head(fig, "Jobs keep completing while worker and scheduler containers are SIGKILLed",
         f"{summary['kills']} kills in the first 40 s; {summary['succeeded']:,} of {summary['jobs']:,} jobs succeeded, "
         f"{summary['recovered']} of them recovered after their worker died; "
         f"{summary['cron_slots']} cron slots, {summary['cron_duplicates']} duplicates.")
    ax.axvspan(0, 40, color=BLUE, alpha=0.06, zorder=0, linewidth=0)
    ax.plot(t, y, color=BLUE, linewidth=2.2, zorder=3, label="jobs succeeded")
    ax.scatter(kills_at, [0] * len(kills_at), marker="v", s=46, color=ORANGE, edgecolor=SURFACE, linewidth=1,
               zorder=5, clip_on=False, label="container SIGKILLed")
    ax.axhline(summary["jobs"], color=BASELINE, linewidth=1.2, linestyle=(0, (4, 3)), zorder=1)
    ax.text(t[0] + 1, summary["jobs"] * 1.012, f"all {summary['jobs']:,} jobs", color=INK2, fontsize=9, va="bottom")
    ax.text(20, summary["jobs"] * 0.5, "kill phase", color=INK2, fontsize=9, ha="center", rotation=0, va="center",
            bbox=dict(facecolor=SURFACE, edgecolor="none", pad=1.5, alpha=0.9))
    ax.annotate("recovered jobs wait out the 30 s lease,\nthen another worker's reaper re-queues them",
                (t[-1] - 6, y[-1]), xytext=(-8, -72), textcoords="offset points", color=INK2, fontsize=9, ha="right",
                arrowprops=dict(arrowstyle="-", color=MUTED, lw=1))
    ax.set_xlabel("seconds since the run started")
    ax.set_ylabel("jobs succeeded (cumulative)")
    ax.set_xlim(0, max(t) + 1)
    ax.set_ylim(0, summary["jobs"] * 1.12)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.legend(frameon=False, loc="upper left", bbox_to_anchor=(0.01, 0.9), labelcolor=INK2, fontsize=9)
    style_axis(ax)
    foot(fig, "Source: docs/perf/data/chaos.csv, chaos.summary.json (scripts/chaos-docker.sh).  " + ENV)
    save(fig, "perf-chaos-recovery.png")


# ---------------------------------------------------------------- 5. burst absorbed (the platform's own metrics)
def chart_burst():
    d = json.loads((DATA / "burst.json").read_text())
    bs, be = (float(v) for v in (DATA / "burst.window").read_text().split())
    dur = be - bs
    fig, axes = plt.subplots(4, 1, figsize=(9.6, 9.4), sharex=True)
    fig.subplots_adjust(left=0.1, right=0.945, top=0.835, bottom=0.095, hspace=0.55)
    head(fig, "A burst of 60,000 jobs arrives at once and is drained",
         f"4 workers drained it in {dur:.0f} s. The platform's own metrics from the live stack, scraped every 5 s "
         "(rates over 20 s windows). The dashed line marks the moment the jobs were inserted.")

    def line(ax, key, color, label=None):
        pts = d["series"].get(key, [])
        if pts:
            ax.plot([p[0] - bs for p in pts], [p[1] for p in pts], color=color, linewidth=2, zorder=3, label=label)
        return pts

    panels = [
        ("backlog", "jobs waiting to run (queue depth, sampled about every 10 s)", "jobs"),
        ("throughput", "completed jobs per second", "jobs/s"),
        (None, "claims per second per worker: busiest vs least busy of the 4 workers", "claims/s"),
        ("oldest_ready", "age of the oldest runnable job (sampled about every 10 s)", "seconds"),
    ]
    for ax, (key, title, unit) in zip(axes, panels):
        ax.axvline(0, color=INK2, linewidth=1, linestyle=(0, (3, 3)), zorder=2)
        if key:
            line(ax, key, BLUE)
        else:
            line(ax, "claims_max_per_worker", BLUE, "busiest worker")
            line(ax, "claims_min_per_worker", ORANGE, "least busy worker")
            ax.legend(frameon=False, loc="upper left", ncol=2, labelcolor=INK2, fontsize=9, bbox_to_anchor=(0.0, 1.0))
        ax.set_title(title, loc="left", fontsize=10, color=INK2, pad=6)
        ax.set_ylabel(unit)
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}" if v >= 10 or v == 0 else f"{v:g}"))
        style_axis(ax)
    axes[0].text(1.2, axes[0].get_ylim()[1] * 0.93, "60,000 jobs inserted", color=INK, fontsize=9, va="top")
    axes[-1].set_xlabel("seconds since the burst was inserted")
    foot(fig, "Source: docs/perf/data/burst.json (Prometheus query_range, scripts/perf-prom-range.py).  " + ENV)
    save(fig, "perf-burst-metrics.png")


if __name__ == "__main__":
    needed = ["scale.csv", "load.csv", "pool.csv", "chaos.csv", "chaos.summary.json", "burst.json", "burst.window"]
    missing = [n for n in needed if not (DATA / n).exists()]
    if missing:
        sys.exit(f"missing data (run scripts/perf-collect.sh first): {missing}")
    chart_drain()
    chart_load()
    chart_pool()
    chart_chaos()
    chart_burst()
