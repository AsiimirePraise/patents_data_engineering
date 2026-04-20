"""
visualizations.py — Fetches data from PostgreSQL and saves 8 PNG charts
to the reports/charts/ folder.
"""

import os
import sys
import pandas as pd
import matplotlib
matplotlib.use('Agg')  
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from sqlalchemy import create_engine


# Config

CONNECTION_STRING = "postgresql://postgres:root@localhost:5111/patents"
OUTPUT_DIR = "reports/charts"
os.makedirs(OUTPUT_DIR, exist_ok=True)

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


print("  PATENT VISUALIZATIONS")

print(f"  Connecting to database...")
sys.stdout.flush()

engine = create_engine(CONNECTION_STRING)
print("  Connected.\n")



# Helper: save figure

def save(fig, filename):
    path = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved → {path}")



# 1. Top 20 Countries by Patent Count

print("[1/8] Top 20 Countries by Patent Count...")
df1 = pd.read_sql("""
    SELECT i.country, COUNT(DISTINCT r.patent_id) AS patent_count
    FROM inventors i
    JOIN relationships r ON i.inventor_id = r.inventor_id
    WHERE i.country IS NOT NULL AND i.country != 'Unknown'
    GROUP BY i.country
    ORDER BY patent_count DESC
    LIMIT 20
""", engine)

fig, ax = plt.subplots(figsize=(12, 7))
bars = ax.barh(df1["country"][::-1], df1["patent_count"][::-1], color="#4C72B0")
ax.set_xlabel("Number of Patents")
ax.set_title("Top 20 Countries by Patent Count", fontsize=14, fontweight="bold", pad=15)
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
for bar in bars:
    w = bar.get_width()
    ax.text(w + max(df1["patent_count"]) * 0.005, bar.get_y() + bar.get_height() / 2,
            f"{int(w):,}", va="center", fontsize=9)
plt.tight_layout()
save(fig, "01_top20_countries.png")



# 2. Top 20 Inventors by Patent Count

print("[2/8] Top 20 Inventors by Patent Count...")
df2 = pd.read_sql("""
    SELECT i.name, i.country, COUNT(DISTINCT r.patent_id) AS patent_count
    FROM inventors i
    JOIN relationships r ON i.inventor_id = r.inventor_id
    GROUP BY i.inventor_id, i.name, i.country
    ORDER BY patent_count DESC
    LIMIT 20
""", engine)

fig, ax = plt.subplots(figsize=(12, 7))
colors = plt.cm.tab20.colors
bars = ax.barh(df2["name"][::-1], df2["patent_count"][::-1],
               color=[colors[i % 20] for i in range(len(df2))])
ax.set_xlabel("Number of Patents")
ax.set_title("Top 20 Inventors by Patent Count", fontsize=14, fontweight="bold", pad=15)
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
for bar in bars:
    w = bar.get_width()
    ax.text(w + max(df2["patent_count"]) * 0.005, bar.get_y() + bar.get_height() / 2,
            f"{int(w):,}", va="center", fontsize=9)
plt.tight_layout()
save(fig, "02_top20_inventors.png")



# 3. Top 20 Companies by Patent Count

print("[3/8] Top 20 Companies by Patent Count...")
df3 = pd.read_sql("""
    SELECT c.name, COUNT(DISTINCT r.patent_id) AS patent_count
    FROM companies c
    JOIN relationships r ON c.company_id = r.company_id
    GROUP BY c.company_id, c.name
    ORDER BY patent_count DESC
    LIMIT 20
""", engine)

fig, ax = plt.subplots(figsize=(12, 7))
bars = ax.barh(df3["name"][::-1], df3["patent_count"][::-1], color="#DD8452")
ax.set_xlabel("Number of Patents")
ax.set_title("Top 20 Companies by Patent Count", fontsize=14, fontweight="bold", pad=15)
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
for bar in bars:
    w = bar.get_width()
    ax.text(w + max(df3["patent_count"]) * 0.005, bar.get_y() + bar.get_height() / 2,
            f"{int(w):,}", va="center", fontsize=9)
plt.tight_layout()
save(fig, "03_top20_companies.png")



# 4. Patents Created Per Year (Trend)

print("[4/8] Patents Per Year Trend...")
df4 = pd.read_sql("""
    SELECT year, COUNT(*) AS patent_count
    FROM patents
    WHERE year IS NOT NULL
    GROUP BY year
    ORDER BY year ASC
""", engine)

fig, ax = plt.subplots(figsize=(14, 6))
ax.plot(df4["year"], df4["patent_count"], color="#4C72B0", linewidth=2.5, marker="o", markersize=3)
ax.fill_between(df4["year"], df4["patent_count"], alpha=0.15, color="#4C72B0")
ax.set_xlabel("Year")
ax.set_ylabel("Number of Patents")
ax.set_title("Patents Granted Per Year (Trend Over Time)", fontsize=14, fontweight="bold", pad=15)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
plt.tight_layout()
save(fig, "04_patents_per_year.png")



# 5. Top 50 Inventors — Ranked with Window Function (colored by country)

print("[5/8] Top 50 Inventors Ranked (Window Function)...")
df5 = pd.read_sql("""
    SELECT inventor_id, name, country, patent_count,
           RANK() OVER (ORDER BY patent_count DESC) AS global_rank
    FROM (
        SELECT i.inventor_id, i.name, i.country,
               COUNT(DISTINCT r.patent_id) AS patent_count
        FROM inventors i
        JOIN relationships r ON i.inventor_id = r.inventor_id
        GROUP BY i.inventor_id, i.name, i.country
    ) sub
    ORDER BY global_rank
    LIMIT 50
""", engine)

# Assign a color per country
unique_countries = df5["country"].unique()
color_map = {c: plt.cm.tab20.colors[i % 20] for i, c in enumerate(unique_countries)}
bar_colors = df5["country"].map(color_map)

fig, ax = plt.subplots(figsize=(14, 14))
ax.barh(df5["name"][::-1], df5["patent_count"][::-1],
        color=bar_colors[::-1].values)
ax.set_xlabel("Number of Patents")
ax.set_title("Top 50 Inventors — Global Rank (Colored by Country)",
             fontsize=14, fontweight="bold", pad=15)
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))

# Legend for countries
from matplotlib.patches import Patch
legend_handles = [Patch(color=color_map[c], label=c) for c in unique_countries]
ax.legend(handles=legend_handles, title="Country", loc="lower right",
          fontsize=8, title_fontsize=9)
plt.tight_layout()
save(fig, "05_top50_inventors_ranked.png")



# 6. Patent Growth Rate — Year over Year %

print("[6/8] Year-over-Year Patent Growth Rate...")
df6 = df4.copy()
df6["growth_rate"] = df6["patent_count"].pct_change() * 100
df6 = df6.dropna()

fig, ax = plt.subplots(figsize=(14, 6))
colors_bar = ["#2ecc71" if v >= 0 else "#e74c3c" for v in df6["growth_rate"]]
ax.bar(df6["year"], df6["growth_rate"], color=colors_bar, width=0.8)
ax.axhline(0, color="black", linewidth=0.8)
ax.set_xlabel("Year")
ax.set_ylabel("Growth Rate (%)")
ax.set_title("Year-over-Year Patent Growth Rate (%)", fontsize=14, fontweight="bold", pad=15)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.1f}%"))
plt.tight_layout()
save(fig, "06_yoy_growth_rate.png")



# 7. Top 10 Companies Share of Total Patents (Donut)

print("[7/8] Top 10 Companies Share of Total Patents (Donut)...")
df7 = pd.read_sql("""
    SELECT c.name, COUNT(DISTINCT r.patent_id) AS patent_count
    FROM companies c
    JOIN relationships r ON c.company_id = r.company_id
    GROUP BY c.company_id, c.name
    ORDER BY patent_count DESC
    LIMIT 10
""", engine)

total = pd.read_sql("SELECT COUNT(*) AS total FROM patents", engine)["total"][0]
other = max(0, total - df7["patent_count"].sum())
labels = df7["name"].tolist() + (["All Others"] if other > 0 else [])
sizes  = df7["patent_count"].tolist() + ([other] if other > 0 else [])

fig, ax = plt.subplots(figsize=(12, 8))
wedges, texts, autotexts = ax.pie(
    sizes, labels=None, autopct="%1.1f%%",
    startangle=140, pctdistance=0.82,
    colors=list(plt.cm.tab20.colors[:len(labels)]),
    wedgeprops=dict(width=0.5)
)
for at in autotexts:
    at.set_fontsize(8)
ax.legend(wedges, labels, title="Company", loc="center left",
          bbox_to_anchor=(1, 0, 0.5, 1), fontsize=9)
ax.set_title("Top 10 Companies Share of Total Patents",
             fontsize=14, fontweight="bold", pad=15)
plt.tight_layout()
save(fig, "07_top10_companies_share.png")



# 8. Top 10 Countries Share of Total Patents (Donut)

print("[8/8] Top 10 Countries Share of Total Patents (Donut)...")
df8 = pd.read_sql("""
    SELECT i.country, COUNT(DISTINCT r.patent_id) AS patent_count
    FROM inventors i
    JOIN relationships r ON i.inventor_id = r.inventor_id
    WHERE i.country IS NOT NULL AND i.country != 'Unknown'
    GROUP BY i.country
    ORDER BY patent_count DESC
    LIMIT 10
""", engine)

other_c  = max(0, total - df8["patent_count"].sum())
labels_c = df8["country"].tolist() + (["All Others"] if other_c > 0 else [])
sizes_c  = df8["patent_count"].tolist() + ([other_c] if other_c > 0 else [])

fig, ax = plt.subplots(figsize=(12, 8))
wedges, texts, autotexts = ax.pie(
    sizes_c, labels=None, autopct="%1.1f%%",
    startangle=140, pctdistance=0.82,
    colors=list(plt.cm.Set3.colors[:len(labels_c)]),
    wedgeprops=dict(width=0.5)
)
for at in autotexts:
    at.set_fontsize(8)
ax.legend(wedges, labels_c, title="Country", loc="center left",
          bbox_to_anchor=(1, 0, 0.5, 1), fontsize=9)
ax.set_title("Top 10 Countries Share of Total Patents",
             fontsize=14, fontweight="bold", pad=15)
plt.tight_layout()
save(fig, "08_top10_countries_share.png")

print(f"  All 8 charts saved to → {OUTPUT_DIR}/")