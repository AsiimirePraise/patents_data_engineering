"""
visualizations.py - Diagnostic Patent Intelligence Charts
-----------------------------------------------------------
Creates meaningful diagnostic visualizations 
  - Inventor weight distribution (weighted by contribution)
  - Patent concentration metrics (Gini coefficient)
  - Quality indicators (abstract length)
  - Team collaboration patterns
  - Impact trends (temporal analysis)

Logging: Tracks execution time and system metrics
"""

import os
import sys
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from sqlalchemy import create_engine
import time
import psutil
import logging
from datetime import datetime
import re


# Config

CONNECTION_STRING = "postgresql://postgres:root@localhost:5111/patents"
OUTPUT_DIR = "reports/charts"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("reports/visualisation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Track system metrics
process = psutil.Process(os.getpid())
start_time = time.time()
start_memory = process.memory_info().rss / (1024 ** 3)  # GB

# Consistent style
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


logger.info("="*80)
logger.info("DIAGNOSTIC VISUALIZATIONS — STARTING")
logger.info("="*80)
logger.info(f"Start Time: {datetime.now().isoformat()}")
logger.info(f"Memory at start: {start_memory:.2f} GB")
logger.info(f"CPU cores available: {psutil.cpu_count()}")

print("  DIAGNOSTIC PATENT VISUALIZATIONS")

print(f"  Connecting to database...")
sys.stdout.flush()

engine = create_engine(CONNECTION_STRING)
print("  Connected.\n")
logger.info("Database connected successfully")


# Helper: Load SQL queries from file

def load_query(query_name):
    """
    Load a named SQL query from queries.sql file.
    Queries are marked with comments like -- VIZ_CHART_1_START and -- VIZ_CHART_1_END
    """
    sql_file = "queries.sql"
    if not os.path.exists(sql_file):
        logger.error(f"SQL file not found: {sql_file}")
        return None
    
    with open(sql_file, 'r') as f:
        content = f.read()
    
    # Extract query between markers
    pattern = f"-- {query_name}_START\n(.*?)\n-- {query_name}_END"
    match = re.search(pattern, content, re.DOTALL)
    
    if match:
        query = match.group(1).strip()
        return query
    else:
        logger.warning(f"Query not found: {query_name}")
        return None


# Helper: save figure

def save(fig, filename, chart_num):
    """Save figure and log timing."""
    path = os.path.join(OUTPUT_DIR, filename)
    
    save_start = time.time()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    save_duration = time.time() - save_start
    
    plt.close(fig)
    
    current_memory = process.memory_info().rss / (1024 ** 3)
    cpu_percent = process.cpu_percent(interval=0.1)
    
    logger.info(f"[{chart_num}] Saved -> {path}")
    logger.info(f"    Save time: {save_duration:.2f}s | Memory: {current_memory:.2f} GB | CPU: {cpu_percent:.1f}%")
    print(f"  [{chart_num}] Saved -> {path}")



# ============================================================================
# DIAGNOSTIC VISUALIZATIONS 
# ============================================================================

# 1. Inventor Weight Distribution — Top 20 by Patent Count

print("\n[1/6] Inventor Weight Distribution (Top 20)...")
logger.info("[1/6] Starting - Inventor Weight Distribution")
chart_start = time.time()

query_1 = load_query("VIZ_CHART_1")
df1 = pd.read_sql(query_1, engine)

fig, ax = plt.subplots(figsize=(12, 8))
colors = plt.cm.RdYlGn(np.linspace(0.2, 0.8, len(df1)))
bars = ax.barh(range(len(df1)), df1["patent_count"].values, color=colors, edgecolor='black', linewidth=1)

ax.set_yticks(range(len(df1)))
ax.set_yticklabels([f"{row['name'][:30]}" for _, row in df1.iterrows()], fontsize=9)
ax.set_xlabel("Number of Patents (Weighted by Contribution)", fontsize=11, fontweight='bold')
ax.set_title("Top 20 Inventors by Patent Weight", fontsize=13, fontweight='bold', pad=15)
ax.invert_yaxis()

# Add value labels
for i, bar in enumerate(bars):
    width = bar.get_width()
    ax.text(width, bar.get_y() + bar.get_height()/2., f' {int(width):,}',
            ha='left', va='center', fontweight='bold', fontsize=8)

ax.grid(axis='x', alpha=0.3, linestyle='--')
save(fig, "01_inventor_weight_top20.png", 1)
logger.info(f"    Duration: {time.time() - chart_start:.2f}s | Rows: {len(df1)}")



# 2. Invention Concentration — Gini Coefficient (Lorenz Curve)

print("[2/6] Invention Concentration (Gini Coefficient)...")
logger.info("[2/6] Starting - Invention Concentration")
chart_start = time.time()

query_2_main = load_query("VIZ_CHART_2_MAIN")
df2 = pd.read_sql(query_2_main, engine)

# If assignee is empty, use inventor name instead
if df2.empty or df2['assignee'].isna().all():
    query_2_fallback = load_query("VIZ_CHART_2_FALLBACK")
    df2 = pd.read_sql(query_2_fallback, engine)

df2 = df2.dropna(subset=['assignee']).sort_values('patent_count', ascending=False).reset_index(drop=True)

# Calculate Lorenz curve
df2['cumulative_patents'] = df2['patent_count'].cumsum()
df2['cumulative_share'] = df2['cumulative_patents'] / df2['patent_count'].sum() * 100
df2['inventor_rank'] = range(1, len(df2) + 1)
df2['inventor_share'] = df2['inventor_rank'] / len(df2) * 100

fig, ax = plt.subplots(figsize=(10, 7))
ax.plot(df2['inventor_share'], df2['cumulative_share'], linewidth=2.5, color='#1f77b4', label='Actual')
ax.plot([0, 100], [0, 100], linestyle='--', linewidth=2, color='red', label='Perfect Equality')
ax.fill_between(df2['inventor_share'], df2['cumulative_share'], df2['inventor_share'], alpha=0.2, color='#1f77b4')

ax.set_xlabel('% of Inventors (Ranked)', fontsize=11, fontweight='bold')
ax.set_ylabel('% of Patents', fontsize=11, fontweight='bold')
ax.set_title('Invention Concentration: Lorenz Curve (Gini Coefficient)', fontsize=13, fontweight='bold', pad=15)
ax.legend(fontsize=10, loc='upper left')
ax.grid(alpha=0.3, linestyle='--')

# Calculate Gini
x = df2['inventor_share'].values / 100
y = df2['cumulative_share'].values / 100
gini = 1 - 2 * np.trapezoid(y, x)

ax.text(0.98, 0.05, f'Gini: {gini:.3f}\n(0=Equal, 1=Concentrated)',
        transform=ax.transAxes, fontsize=10, verticalalignment='bottom',
        horizontalalignment='right', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

save(fig, "02_invention_concentration_gini.png", 2)
logger.info(f"    Duration: {time.time() - chart_start:.2f}s | Gini Coefficient: {gini:.3f}")



# 3. Patent Quality Indicator — Abstract Length Distribution

print("[3/6] Patent Quality Metrics (Abstract Length)...")
logger.info("[3/6] Starting - Patent Quality Metrics")
chart_start = time.time()

query_3 = load_query("VIZ_CHART_3")
df3 = pd.read_sql(query_3, engine)

if df3.empty:
    logger.warning("    No abstract data found - skipping quality chart")
else:
    # Bin abstract lengths
    df3['length_bin'] = pd.cut(df3['abstract_length'], 
                               bins=[0, 200, 500, 1000, 2000, 5000, 100000],
                               labels=['Very Short\n(<200)', 'Short\n(200-500)', 'Medium\n(500-1K)', 
                                      'Long\n(1-2K)', 'Very Long\n(2-5K)', 'Extreme\n(>5K)'])
    
    binned_df = df3.groupby('length_bin', observed=True)['count'].sum().reset_index()
    
    fig, ax = plt.subplots(figsize=(11, 6))
    colors = plt.cm.RdYlGn(np.linspace(0.2, 0.8, len(binned_df)))
    bars = ax.bar(range(len(binned_df)), binned_df['count'], color=colors, edgecolor='black', linewidth=1.2)
    
    ax.set_xticks(range(len(binned_df)))
    ax.set_xticklabels(binned_df['length_bin'], fontsize=10)
    ax.set_ylabel('Patent Count', fontsize=11, fontweight='bold')
    ax.set_xlabel('Abstract Length', fontsize=11, fontweight='bold')
    ax.set_title('Patent Quality Indicator: Abstract Length Distribution', fontsize=13, fontweight='bold', pad=15)
    
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}',
                ha='center', va='bottom', fontweight='bold', fontsize=9)
    
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'{int(x/1000)}K' if x >= 1000 else int(x)))
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    save(fig, "03_patent_quality_abstract_length.png", 3)
    logger.info(f"    Duration: {time.time() - chart_start:.2f}s | Abstracts analyzed: {len(df3)}")



# 4. Team Collaboration Patterns — Inventor Count Distribution

print("[4/6] Team Collaboration Patterns...")
logger.info("[4/6] Starting - Team Collaboration Patterns")
chart_start = time.time()

query_4 = load_query("VIZ_CHART_4")
df4 = pd.read_sql(query_4, engine)

df4['team_size'] = pd.cut(df4['inventor_count'],
                           bins=[0, 1, 3, 5, 10, 1000],
                           labels=['Solo\n(1)', 'Small\n(2-3)', 'Medium\n(4-5)', 'Large\n(6-10)', 'Massive\n(10+)'])

team_stats = df4.groupby('team_size', observed=True).size().reset_index(name='patent_count')

fig, ax = plt.subplots(figsize=(11, 6))
colors = ['#2ca02c', '#1f77b4', '#ff7f0e', '#d62728', '#9467bd']
wedges, texts, autotexts = ax.pie(team_stats['patent_count'],
                                    labels=team_stats['team_size'],
                                    autopct='%1.1f%%',
                                    colors=colors,
                                    startangle=90,
                                    textprops={'fontsize': 10, 'fontweight': 'bold'})

ax.set_title('Patent Invention Team Composition (Collaboration Patterns)', fontsize=13, fontweight='bold', pad=15)

save(fig, "04_team_collaboration_patterns.png", 4)
logger.info(f"    Duration: {time.time() - chart_start:.2f}s | Patents: {len(df4)}")



# 5. Inventor Type Analysis — Corporate vs. Individual

print("[5/6] Inventor Type Distribution...")
logger.info("[5/6] Starting - Inventor Type Distribution")
chart_start = time.time()

try:
    query_5 = load_query("VIZ_CHART_5")
    df5 = pd.read_sql(query_5, engine)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    colors = ['#1f77b4', '#ff7f0e']
    bars = ax.bar(df5['inventor_type'], df5['patent_count'], color=colors, edgecolor='black', linewidth=1.5)
    
    ax.set_ylabel('Number of Patents', fontsize=12, fontweight='bold')
    ax.set_xlabel('Inventor Type', fontsize=12, fontweight='bold')
    ax.set_title('Patent Distribution: Corporate vs. Individual Inventors', 
                 fontsize=13, fontweight='bold', pad=15)
    
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}',
                ha='center', va='bottom', fontweight='bold', fontsize=11)
    
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'{int(x/1000)}K' if x >= 1000 else int(x)))
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    save(fig, "05_inventor_type_distribution.png", 5)
    logger.info(f"    Duration: {time.time() - chart_start:.2f}s")
except Exception as e:
    logger.warning(f"    Could not create inventor type chart: {e}")



# 6. Impact Trends — Patents per Year

print("[6/6] Impact Trends (Patents per Year)...")
logger.info("[6/6] Starting - Impact Trends")
chart_start = time.time()

query_6 = load_query("VIZ_CHART_6")
df6 = pd.read_sql(query_6, engine)

if df6.empty:
    logger.warning("    No patent date data found")
else:
    df6['decade_label'] = df6['decade'].astype(int).astype(str) + '0s'
    
    fig, ax = plt.subplots(figsize=(12, 6))
    colors = plt.cm.viridis(np.linspace(0.2, 0.9, len(df6)))
    bars = ax.bar(df6['decade_label'], df6['patent_count'], color=colors, edgecolor='black', linewidth=1.2)
    
    ax.set_ylabel('Patent Count', fontsize=11, fontweight='bold')
    ax.set_xlabel('Decade Filed', fontsize=11, fontweight='bold')
    ax.set_title('Impact Trends: Patent Filing Over Decades', fontsize=13, fontweight='bold', pad=15)
    
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height):,}',
                ha='center', va='bottom', fontweight='bold', fontsize=9)
    
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'{int(x/1000)}K' if x >= 1000 else int(x)))
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    plt.xticks(rotation=45)
    
    save(fig, "06_impact_trends_over_decades.png", 6)
    logger.info(f"    Duration: {time.time() - chart_start:.2f}s")


# Final logging summary
end_time = time.time()
end_memory = process.memory_info().rss / (1024 ** 3)
total_duration = end_time - start_time
memory_delta = end_memory - start_memory

logger.info("="*80)
logger.info("DIAGNOSTIC VISUALIZATIONS — COMPLETED")
logger.info("="*80)
logger.info(f"Total execution time: {total_duration:.2f}s")
logger.info(f"Memory at end: {end_memory:.2f} GB (Δ {memory_delta:+.2f} GB)")
logger.info(f"All 6 diagnostic charts saved to -> {OUTPUT_DIR}/")
logger.info("="*80)

print(f"\n  [OK] All 6 diagnostic charts saved!")
print(f"  [TIME] Total time: {total_duration:.2f}s")
print(f"  [MEMORY] Memory used: {memory_delta:+.2f} GB")
print(f"  Charts location: {OUTPUT_DIR}/")