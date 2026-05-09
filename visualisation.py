"""
visualisation.py — Diagnostic Patent Intelligence Charts
----------------------------------------------------------
6 charts that answer diagnostic questions (not blind counts):

  1. Inventor Weight       — Top 20 inventors, weighted bar chart
  2. Gini / Lorenz Curve  — How concentrated is patent ownership?
  3. Abstract Length       — Patent quality indicator
  4. Team Collaboration    — Solo vs team inventors (pie + bar)
  5. Inventor Type         — Corporate entity vs Individual
  6. Impact Over Decades   — Filing trends per decade + growth rate

Fixes vs previous version:
  - pd.read_sql() now uses engine via a context-managed connection and
    sa_text() — fixes the immutabledict TypeError on newer SQLAlchemy
  - VIZ_CHART_5 uses POSITION() instead of ILIKE — the % wildcards in
    ILIKE '%Inc%' are misread as psycopg2 parameter placeholders
  - FORCE_RERUN flag: set False to skip charts already saved (saves 25+
    minutes when recovering from a mid-run crash)
  - np.trapz used instead of np.trapezoid for broader numpy compatibility
"""

import os
import sys
import re
import time
import platform
import logging
from datetime import datetime

import numpy as np
import pandas as pd
import psutil
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from sqlalchemy import create_engine, text as sa_text

# ─── Config ───────────────────────────────────────────────────────────────────
CONNECTION_STRING = "postgresql://postgres:root@localhost:5111/patents"
OUTPUT_DIR        = "reports/charts"
SQL_FILE          = "queries.sql"

# Set to True to regenerate all charts even if they already exist.
# Set to False to skip charts already saved — useful when recovering from
# a crash partway through (charts 1-4 done, only 5-6 need running).
FORCE_RERUN = False

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs("reports", exist_ok=True)

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("reports/visualisation.log", mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("viz")

# ─── Plot style ───────────────────────────────────────────────────────────────
plt.rcParams.update({
    "figure.facecolor": "#f9f9f9",
    "axes.facecolor":   "#f9f9f9",
    "axes.edgecolor":   "#cccccc",
    "axes.grid":        True,
    "grid.color":       "#e0e0e0",
    "grid.linestyle":   "--",
    "font.family":      "DejaVu Sans",
    "font.size":        11,
})

# ─── Startup ──────────────────────────────────────────────────────────────────
process    = psutil.Process(os.getpid())
pipeline_t = time.perf_counter()

log.info("=" * 72)
log.info("DIAGNOSTIC VISUALISATIONS — STARTING")
log.info("=" * 72)
log.info(f"Timestamp   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
log.info(f"OS          : {platform.system()} {platform.release()}")
log.info(f"CPU cores   : {psutil.cpu_count(logical=True)}")
log.info(f"RAM total   : {psutil.virtual_memory().total / 1e9:.1f} GB")
log.info(f"FORCE_RERUN : {FORCE_RERUN}")

engine = create_engine(CONNECTION_STRING)
log.info("Database connected\n")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def load_query(marker: str) -> str:
    """Extract a named SQL block from queries.sql using START/END markers."""
    with open(SQL_FILE, "r") as f:
        content = f.read()
    pattern = rf"-- {re.escape(marker)}_START\n(.*?)\n-- {re.escape(marker)}_END"
    match   = re.search(pattern, content, re.DOTALL)
    if not match:
        raise ValueError(f"Marker '{marker}' not found in {SQL_FILE}")
    sql = match.group(1).strip()
    if sql.endswith(";"):
        sql = sql[:-1]
    return sql


def run_query(marker: str) -> pd.DataFrame:
    """
    Execute a named SQL query from queries.sql and return a DataFrame.
    Uses sa_text() + context-managed connection to avoid the immutabledict
    TypeError that occurs when passing engine.connect() directly to pd.read_sql().
    """
    sql = load_query(marker)
    with engine.connect() as conn:
        return pd.read_sql(sa_text(sql), conn)


def chart_exists(filename: str) -> bool:
    """Return True and log a skip message if chart file already exists."""
    path = os.path.join(OUTPUT_DIR, filename)
    if not FORCE_RERUN and os.path.exists(path):
        log.info(f"  Skipping — already exists: {path}")
        log.info(f"  (set FORCE_RERUN=True to regenerate)")
        return True
    return False


def save_chart(fig, filename: str, chart_label: str):
    """Save figure, log timing + system metrics."""
    path    = os.path.join(OUTPUT_DIR, filename)
    t0      = time.perf_counter()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    elapsed = time.perf_counter() - t0
    cpu_pct = psutil.cpu_percent(interval=0.05)
    ram_pct = psutil.virtual_memory().percent
    mem_gb  = process.memory_info().rss / 1e9
    log.info(f"  [{chart_label}] saved → {path} | "
             f"{elapsed:.2f}s | CPU {cpu_pct:.1f}% | RAM {ram_pct:.1f}% | proc {mem_gb:.2f} GB")


# ─── Chart 1: Inventor Weight Distribution ────────────────────────────────────
log.info("[1/6] Inventor Weight Distribution (Top 20)...")

if not chart_exists("01_inventor_weight_top20.png"):
    t0  = time.perf_counter()
    df1 = run_query("VIZ_CHART_1")

    fig, ax = plt.subplots(figsize=(12, 8))
    colors  = plt.cm.RdYlGn(np.linspace(0.2, 0.85, len(df1)))
    bars    = ax.barh(range(len(df1)), df1["patent_count"].values,
                      color=colors, edgecolor="black", linewidth=0.8)
    ax.set_yticks(range(len(df1)))
    ax.set_yticklabels(
        [f"{row['name'][:28]}  ({row['country']})" for _, row in df1.iterrows()],
        fontsize=9
    )
    ax.set_xlabel("Number of Patents (Weighted by Contribution)", fontweight="bold")
    ax.set_title("Top 20 Inventors by Patent Weight\n"
                 "(Diagnostic: Who drives the most innovation globally?)",
                 fontsize=13, fontweight="bold", pad=14)
    ax.invert_yaxis()
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    for bar in bars:
        w = bar.get_width()
        ax.text(w + df1["patent_count"].max() * 0.005,
                bar.get_y() + bar.get_height() / 2,
                f"{int(w):,}", va="center", fontsize=8, fontweight="bold")
    plt.tight_layout()
    save_chart(fig, "01_inventor_weight_top20.png", "1/6")
    log.info(f"    query+render: {time.perf_counter() - t0:.2f}s | rows: {len(df1)}")


# ─── Chart 2: Invention Concentration — Lorenz Curve + Gini ──────────────────
log.info("[2/6] Invention Concentration (Lorenz Curve / Gini)...")

if not chart_exists("02_invention_concentration_gini.png"):
    t0  = time.perf_counter()
    df2 = run_query("VIZ_CHART_2_MAIN")

    if df2.empty or df2["assignee"].isna().all() or (df2["patent_count"] == 0).all():
        log.warning("    Company data empty — using inventor fallback")
        df2 = run_query("VIZ_CHART_2_FALLBACK")

    df2 = (df2.dropna(subset=["assignee"])
              .query("patent_count > 0")
              .sort_values("patent_count", ascending=True)
              .reset_index(drop=True))

    if len(df2) < 2:
        log.warning("    Not enough data for Lorenz curve — skipping chart 2")
    else:
        total = df2["patent_count"].sum()
        df2["cum_share_patents"] = df2["patent_count"].cumsum() / total * 100
        df2["pct_of_assignees"]  = (df2.index + 1) / len(df2) * 100

        x    = df2["pct_of_assignees"].values / 100
        y    = df2["cum_share_patents"].values / 100
        gini = float(1 - 2 * np.trapz(y, x))

        fig, ax = plt.subplots(figsize=(10, 7))
        ax.plot(df2["pct_of_assignees"], df2["cum_share_patents"],
                linewidth=2.5, color="#1f77b4", label="Patent distribution")
        ax.plot([0, 100], [0, 100], linestyle="--", linewidth=2,
                color="#d62728", label="Perfect equality")
        ax.fill_between(df2["pct_of_assignees"],
                        df2["cum_share_patents"],
                        df2["pct_of_assignees"],
                        alpha=0.18, color="#1f77b4")
        ax.set_xlabel("% of Assignees (Ranked by patent count)", fontweight="bold")
        ax.set_ylabel("% of Total Patents", fontweight="bold")
        ax.set_title("Invention Concentration — Lorenz Curve\n"
                     "(Diagnostic: Is ownership concentrated or democratic?)",
                     fontsize=13, fontweight="bold", pad=14)
        ax.legend(fontsize=10)
        ax.text(0.97, 0.06,
                f"Gini coefficient: {gini:.3f}\n(0 = equal,  1 = one entity owns all)",
                transform=ax.transAxes, fontsize=10, ha="right", va="bottom",
                bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.6))
        plt.tight_layout()
        save_chart(fig, "02_invention_concentration_gini.png", "2/6")
        log.info(f"    Gini: {gini:.4f} | rows: {len(df2)} | time: {time.perf_counter()-t0:.2f}s")


# ─── Chart 3: Patent Quality — Abstract Length Distribution ───────────────────
log.info("[3/6] Patent Quality — Abstract Length Distribution...")

if not chart_exists("03_patent_quality_abstract_length.png"):
    t0  = time.perf_counter()
    df3 = run_query("VIZ_CHART_3")

    if df3.empty:
        log.warning("    No abstract data — skipping chart 3")
    else:
        df3["length_bin"] = pd.cut(
            df3["abstract_length"],
            bins=[0, 200, 500, 1_000, 2_000, 5_000, 10_000_000],
            labels=["Very Short\n(<200)", "Short\n(200-500)", "Medium\n(500-1K)",
                    "Long\n(1K-2K)", "Very Long\n(2K-5K)", "Extreme\n(>5K)"],
        )
        binned   = df3.groupby("length_bin", observed=True)["count"].sum().reset_index()
        fig, ax  = plt.subplots(figsize=(11, 6))
        bar_cols = plt.cm.RdYlGn(np.linspace(0.15, 0.85, len(binned)))
        bars     = ax.bar(range(len(binned)), binned["count"],
                          color=bar_cols, edgecolor="black", linewidth=1)
        ax.set_xticks(range(len(binned)))
        ax.set_xticklabels(binned["length_bin"], fontsize=10)
        ax.set_ylabel("Number of Patents", fontweight="bold")
        ax.set_xlabel("Abstract Character Length", fontweight="bold")
        ax.set_title("Patent Quality Indicator — Abstract Length Distribution\n"
                     "(Diagnostic: Are patents well-documented or minimal?)",
                     fontsize=13, fontweight="bold", pad=14)
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{int(x/1000)}K" if x >= 1000 else str(int(x)))
        )
        for bar in bars:
            h = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, h,
                    f"{int(h):,}", ha="center", va="bottom", fontsize=9, fontweight="bold")
        plt.tight_layout()
        save_chart(fig, "03_patent_quality_abstract_length.png", "3/6")
        log.info(f"    rows: {len(df3)} | time: {time.perf_counter()-t0:.2f}s")


# ─── Chart 4: Team Collaboration Patterns ─────────────────────────────────────
log.info("[4/6] Team Collaboration Patterns...")

if not chart_exists("04_team_collaboration_patterns.png"):
    t0  = time.perf_counter()
    df4 = run_query("VIZ_CHART_4")

    df4["team_size"] = pd.cut(
        df4["inventor_count"],
        bins=[0, 1, 3, 5, 10, 1_000_000],
        labels=["Solo (1)", "Small (2-3)", "Medium (4-5)", "Large (6-10)", "Massive (10+)"],
    )
    team_counts = df4.groupby("team_size", observed=True).size().reset_index(name="patent_count")

    fig, axes  = plt.subplots(1, 2, figsize=(14, 6))
    colors_pie = ["#2ca02c", "#1f77b4", "#ff7f0e", "#d62728", "#9467bd"]
    axes[0].pie(
        team_counts["patent_count"],
        labels=team_counts["team_size"],
        autopct="%1.1f%%",
        colors=colors_pie,
        startangle=90,
        textprops={"fontsize": 10, "fontweight": "bold"},
        wedgeprops={"linewidth": 1.2, "edgecolor": "white"},
    )
    axes[0].set_title("Team Size Distribution\n(Share of all patents)", fontweight="bold")

    axes[1].bar(team_counts["team_size"], team_counts["patent_count"],
                color=colors_pie, edgecolor="black", linewidth=0.8)
    axes[1].set_ylabel("Number of Patents")
    axes[1].yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{int(x/1000)}K" if x >= 1000 else str(int(x)))
    )
    for bar in axes[1].patches:
        h = bar.get_height()
        axes[1].text(bar.get_x() + bar.get_width() / 2, h,
                     f"{int(h):,}", ha="center", va="bottom", fontsize=9)
    axes[1].set_title("Team Size — Absolute Counts", fontweight="bold")
    axes[1].tick_params(axis="x", rotation=20)

    fig.suptitle("Patent Collaboration Patterns\n"
                 "(Diagnostic: Solo genius vs collaborative innovation?)",
                 fontsize=13, fontweight="bold", y=1.01)
    plt.tight_layout()
    save_chart(fig, "04_team_collaboration_patterns.png", "4/6")
    log.info(f"    rows: {len(df4)} | time: {time.perf_counter()-t0:.2f}s")


# ─── Chart 5: Inventor Type — Corporate Entity vs Individual ──────────────────
# NOTE: Query uses POSITION() not ILIKE to avoid psycopg2 treating
# the % in ILIKE '%Inc%' as a parameter placeholder (immutabledict crash).
log.info("[5/6] Inventor Type — Corporate vs Individual...")

if not chart_exists("05_inventor_type_distribution.png"):
    t0  = time.perf_counter()
    df5 = run_query("VIZ_CHART_5")

    if df5.empty or len(df5) < 2:
        log.warning("    Insufficient data for inventor type chart — skipping")
    else:
        fig, ax     = plt.subplots(figsize=(9, 6))
        type_colors = {"Corporate Entity": "#1f77b4", "Individual": "#ff7f0e"}
        bar_colors  = [type_colors.get(t, "#aaa") for t in df5["inventor_type"]]
        bars = ax.bar(df5["inventor_type"], df5["patent_count"],
                      color=bar_colors, edgecolor="black", linewidth=1.2, width=0.5)
        ax.set_ylabel("Number of Patents", fontweight="bold")
        ax.set_xlabel("Inventor Type", fontweight="bold")
        ax.set_title("Corporate Entity vs Individual Inventors\n"
                     "(Diagnostic: Who drives patent filings — companies or people?)",
                     fontsize=13, fontweight="bold", pad=14)
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{int(x/1_000_000)}M" if x >= 1e6
                                  else f"{int(x/1000)}K" if x >= 1000 else str(int(x)))
        )
        for bar in bars:
            h = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, h,
                    f"{int(h):,}", ha="center", va="bottom", fontsize=11, fontweight="bold")
        plt.tight_layout()
        save_chart(fig, "05_inventor_type_distribution.png", "5/6")
        log.info(f"    rows: {len(df5)} | time: {time.perf_counter()-t0:.2f}s")


# ─── Chart 6: Impact Trends — Patents per Decade ──────────────────────────────
log.info("[6/6] Impact Trends — Patents per Decade...")

if not chart_exists("06_impact_trends_over_decades.png"):
    t0  = time.perf_counter()
    df6 = run_query("VIZ_CHART_6")

    if df6.empty:
        log.warning("    No year data — skipping chart 6")
    else:
        df6["decade_label"] = df6["decade"].astype(int).astype(str) + "s"
        df6["yoy_growth"]   = df6["patent_count"].pct_change() * 100

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

        bar_cols = plt.cm.viridis(np.linspace(0.2, 0.9, len(df6)))
        bars     = ax1.bar(df6["decade_label"], df6["patent_count"],
                           color=bar_cols, edgecolor="black", linewidth=0.8)
        ax1.set_ylabel("Patent Count", fontweight="bold")
        ax1.set_title("Impact Trends — Patents Filed Per Decade\n"
                      "(Diagnostic: When was innovation accelerating fastest?)",
                      fontsize=13, fontweight="bold", pad=12)
        ax1.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{int(x/1000)}K" if x >= 1000 else str(int(x)))
        )
        for bar in bars:
            h = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width() / 2, h,
                     f"{int(h):,}", ha="center", va="bottom", fontsize=9)

        growth_valid = df6.dropna(subset=["yoy_growth"])
        g_colors     = ["#2ecc71" if v >= 0 else "#e74c3c" for v in growth_valid["yoy_growth"]]
        ax2.bar(growth_valid["decade_label"], growth_valid["yoy_growth"],
                color=g_colors, edgecolor="black", linewidth=0.8)
        ax2.axhline(0, color="black", linewidth=0.8)
        ax2.set_ylabel("Growth vs Previous Decade (%)", fontweight="bold")
        ax2.set_xlabel("Decade", fontweight="bold")
        ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
        plt.xticks(rotation=45, ha="right")

        plt.tight_layout()
        save_chart(fig, "06_impact_trends_over_decades.png", "6/6")
        log.info(f"    rows: {len(df6)} | time: {time.perf_counter()-t0:.2f}s")


# ─── Done ─────────────────────────────────────────────────────────────────────
total_elapsed = time.perf_counter() - pipeline_t
ram_end       = psutil.virtual_memory().percent
cpu_end       = psutil.cpu_percent(interval=0.1)
mem_proc      = process.memory_info().rss / 1e9

log.info("=" * 72)
log.info("VISUALISATIONS COMPLETE")
log.info("=" * 72)
log.info(f"Total time  : {total_elapsed:.1f}s")
log.info(f"CPU at end  : {cpu_end:.1f}%")
log.info(f"RAM at end  : {ram_end:.1f}%")
log.info(f"Process mem : {mem_proc:.2f} GB")
log.info(f"Charts saved → {OUTPUT_DIR}/")
log.info("=" * 72)

print(f"\n  All 6 diagnostic charts saved → {OUTPUT_DIR}/")
print(f"  Total time : {total_elapsed:.1f}s")
print(f"  Log        : reports/visualisation.log")