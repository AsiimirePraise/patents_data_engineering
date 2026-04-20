"""
analysis.py — Advanced Patent Analysis
----------------------------------------

Analyses:
  1. Most active decades
  2. Patent growth by country over time (top 5 countries)
  3. Top 5 inventors per country
  4. Peak invention year per country
  5. Company dominance — concentration of patent ownership
"""

import os
import sys
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from sqlalchemy import create_engine


# Config

CONNECTION_STRING = "postgresql://postgres:root@localhost:5111/patents"
OUTPUT_DIR        = "reports/charts"
REPORTS_DIR       = "reports"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

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


print("  ADVANCED PATENT ANALYSIS")

print("  Connecting to database...")
sys.stdout.flush()

engine = create_engine(CONNECTION_STRING)
print("  Connected.\n")


def save(fig, filename):
    path = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved → {path}")



# Analysis 1: Most Active Decades

print("[1/5] Most Active Decades...")
df1 = pd.read_sql("""
    SELECT
        (year / 10) * 10  AS decade,
        COUNT(*)          AS patent_count
    FROM patents
    WHERE year IS NOT NULL
    GROUP BY decade
    ORDER BY decade ASC
""", engine)

df1["decade_label"] = df1["decade"].astype(str) + "s"

fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.bar(df1["decade_label"], df1["patent_count"], color="#4C72B0", width=0.6)
ax.set_xlabel("Decade")
ax.set_ylabel("Number of Patents")
ax.set_title("Most Active Patent Decades", fontsize=14, fontweight="bold", pad=15)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
for bar in bars:
    h = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, h + max(df1["patent_count"]) * 0.01,
            f"{int(h):,}", ha="center", fontsize=9)
plt.xticks(rotation=45)
plt.tight_layout()
save(fig, "09_most_active_decades.png")

df1.to_csv(os.path.join(REPORTS_DIR, "analysis_decades.csv"), index=False)
print(df1[["decade_label", "patent_count"]].to_string(index=False))



# Analysis 2: Patent Growth by Country Over Time (Top 5)

print("\n[2/5] Patent Growth by Country Over Time (Top 5)...")

# Get top 5 countries first
top5 = pd.read_sql("""
    SELECT i.country, COUNT(DISTINCT r.patent_id) AS total
    FROM inventors i
    JOIN relationships r ON i.inventor_id = r.inventor_id
    WHERE i.country IS NOT NULL AND i.country != 'Unknown'
    GROUP BY i.country
    ORDER BY total DESC
    LIMIT 5
""", engine)
top5_list = top5["country"].tolist()
top5_str  = ", ".join([f"'{c}'" for c in top5_list])

df2 = pd.read_sql(f"""
    SELECT p.year, i.country, COUNT(DISTINCT r.patent_id) AS patent_count
    FROM patents p
    JOIN relationships r  ON p.patent_id   = r.patent_id
    JOIN inventors     i  ON r.inventor_id = i.inventor_id
    WHERE i.country IN ({top5_str})
      AND p.year IS NOT NULL
    GROUP BY p.year, i.country
    ORDER BY p.year ASC
""", engine)

fig, ax = plt.subplots(figsize=(14, 7))
colors = plt.cm.tab10.colors
for idx, country in enumerate(top5_list):
    subset = df2[df2["country"] == country]
    ax.plot(subset["year"], subset["patent_count"],
            label=country, color=colors[idx], linewidth=2.5)

ax.set_xlabel("Year")
ax.set_ylabel("Number of Patents")
ax.set_title("Patent Growth Over Time — Top 5 Countries",
             fontsize=14, fontweight="bold", pad=15)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
ax.legend(title="Country", fontsize=10)
plt.tight_layout()
save(fig, "10_country_growth_over_time.png")

df2.to_csv(os.path.join(REPORTS_DIR, "analysis_country_growth.csv"), index=False)
print(f"  Top 5 countries: {top5_list}")



# Analysis 3: Top 5 Inventors Per Country (Top 10 countries)

print("\n[3/5] Top 5 Inventors Per Country...")
df3 = pd.read_sql("""
    WITH ranked AS (
        SELECT
            i.inventor_id,
            i.name,
            i.country,
            COUNT(DISTINCT r.patent_id) AS patent_count,
            RANK() OVER (PARTITION BY i.country ORDER BY COUNT(DISTINCT r.patent_id) DESC) AS rnk
        FROM inventors i
        JOIN relationships r ON i.inventor_id = r.inventor_id
        WHERE i.country IS NOT NULL AND i.country != 'Unknown'
        GROUP BY i.inventor_id, i.name, i.country
    )
    SELECT country, name, patent_count, rnk
    FROM ranked
    WHERE rnk <= 5
      AND country IN (
          SELECT country FROM (
              SELECT i.country, COUNT(DISTINCT r.patent_id) AS total
              FROM inventors i
              JOIN relationships r ON i.inventor_id = r.inventor_id
              WHERE i.country IS NOT NULL AND i.country != 'Unknown'
              GROUP BY i.country
              ORDER BY total DESC
              LIMIT 10
          ) top10
      )
    ORDER BY country, rnk
""", engine)

# Plot: one grouped bar chart per country (top 5 countries only for readability)
plot_countries = df3["country"].unique()[:5]
fig, axes = plt.subplots(1, len(plot_countries), figsize=(18, 7), sharey=False)

for ax, country in zip(axes, plot_countries):
    subset = df3[df3["country"] == country].head(5)
    short_names = subset["name"].apply(lambda n: n.split()[-1] if pd.notna(n) else "N/A")
    ax.barh(short_names[::-1], subset["patent_count"][::-1], color="#4C72B0")
    ax.set_title(country, fontsize=11, fontweight="bold")
    ax.set_xlabel("Patents")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))

fig.suptitle("Top 5 Inventors Per Country (Top 5 Countries)",
             fontsize=14, fontweight="bold", y=1.02)
plt.tight_layout()
save(fig, "11_top_inventors_per_country.png")

df3.to_csv(os.path.join(REPORTS_DIR, "analysis_inventors_per_country.csv"), index=False)
print(df3.head(20).to_string(index=False))



# Analysis 4: Peak Invention Year Per Country (Top 15)

print("\n[4/5] Peak Invention Year Per Country...")
df4 = pd.read_sql("""
    WITH yearly AS (
        SELECT
            i.country,
            p.year,
            COUNT(DISTINCT r.patent_id) AS patent_count
        FROM patents p
        JOIN relationships r ON p.patent_id   = r.patent_id
        JOIN inventors     i ON r.inventor_id = i.inventor_id
        WHERE i.country IS NOT NULL
          AND i.country != 'Unknown'
          AND p.year IS NOT NULL
        GROUP BY i.country, p.year
    ),
    peaked AS (
        SELECT country, year AS peak_year, patent_count AS peak_count,
               RANK() OVER (PARTITION BY country ORDER BY patent_count DESC) AS rnk
        FROM yearly
    )
    SELECT country, peak_year, peak_count
    FROM peaked
    WHERE rnk = 1
      AND country IN (
          SELECT country FROM (
              SELECT i.country, COUNT(DISTINCT r.patent_id) AS total
              FROM inventors i
              JOIN relationships r ON i.inventor_id = r.inventor_id
              WHERE i.country IS NOT NULL AND i.country != 'Unknown'
              GROUP BY i.country
              ORDER BY total DESC
              LIMIT 15
          ) top15
      )
    ORDER BY peak_count DESC
""", engine)

fig, ax = plt.subplots(figsize=(13, 7))
bars = ax.barh(df4["country"][::-1], df4["peak_count"][::-1], color="#55A868")
ax.set_xlabel("Patents in Peak Year")
ax.set_title("Peak Patent Year Per Country (Top 15 Countries)",
             fontsize=14, fontweight="bold", pad=15)
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))

for bar, (_, row) in zip(bars, df4[::-1].iterrows()):
    w = bar.get_width()
    ax.text(w + max(df4["peak_count"]) * 0.005,
            bar.get_y() + bar.get_height() / 2,
            f"{int(row['peak_year'])}  ({int(w):,})",
            va="center", fontsize=9)
plt.tight_layout()
save(fig, "12_peak_year_per_country.png")

df4.to_csv(os.path.join(REPORTS_DIR, "analysis_peak_year.csv"), index=False)
print(df4.to_string(index=False))



# Analysis 5: Company Dominance — Patent Concentration

print("\n[5/5] Company Patent Concentration...")
df5_raw = pd.read_sql("""
    SELECT c.company_id, COUNT(DISTINCT r.patent_id) AS patent_count
    FROM companies c
    JOIN relationships r ON c.company_id = r.company_id
    GROUP BY c.company_id
    ORDER BY patent_count DESC
""", engine)

total_patents    = df5_raw["patent_count"].sum()
total_companies  = len(df5_raw)

# Build concentration brackets
brackets = [0.01, 0.05, 0.10, 0.25, 0.50, 1.00]
rows = []
for pct in brackets:
    n        = max(1, int(total_companies * pct))
    top_sum  = df5_raw.head(n)["patent_count"].sum()
    share    = round(100.0 * top_sum / total_patents, 2)
    rows.append({
        "top_pct_companies": f"Top {int(pct*100)}%",
        "num_companies":     n,
        "patents_held":      int(top_sum),
        "share_of_total":    share,
    })

df5 = pd.DataFrame(rows)

fig, ax = plt.subplots(figsize=(10, 6))
ax.bar(df5["top_pct_companies"], df5["share_of_total"], color="#C44E52", width=0.5)
ax.set_xlabel("Top % of Companies")
ax.set_ylabel("Share of Total Patents (%)")
ax.set_title("Patent Concentration — How Much Do Top Companies Own?",
             fontsize=14, fontweight="bold", pad=15)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
for bar, val in zip(ax.patches, df5["share_of_total"]):
    ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.5,
            f"{val}%", ha="center", fontsize=10, fontweight="bold")
plt.tight_layout()
save(fig, "13_company_concentration.png")

df5.to_csv(os.path.join(REPORTS_DIR, "analysis_concentration.csv"), index=False)
print(df5.to_string(index=False))



# Done

print(f"\n{'=' * 60}")
print("  Advanced analysis complete!")
print(f"  Charts saved → {OUTPUT_DIR}/")
print(f"  CSVs saved   → {REPORTS_DIR}/")
