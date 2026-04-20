"""
dashboard.py — Streamlit Patent Intelligence Dashboard
--------------------------------------------------------
Run with:  streamlit run dashboard.py

Requires: streamlit, pandas, sqlalchemy, matplotlib, pillow

Tabs:
  1. Overview      — key stats + top 5 summaries
  2. Query Results — all 7 queries as interactive tables
  3. Visualisations — all 8 charts from visualisation.py
  4. Advanced Analysis — 5 extra analyses + charts
"""

import os
import json
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.patches import Patch
from sqlalchemy import create_engine, text
from PIL import Image
import streamlit as st


# Config

CONNECTION_STRING = "postgresql://postgres:root@localhost:5111/patents"
CHARTS_DIR        = "reports/charts"
REPORTS_DIR       = "reports"
SQL_FILE          = "queries.sql"

st.set_page_config(
    page_title="Patent Intelligence Dashboard",
    page_icon="🔬",
    layout="wide",
)


# DB connection (cached)

@st.cache_resource
def get_engine():
    return create_engine(CONNECTION_STRING)

engine = get_engine()



# Load queries.sql

@st.cache_data
def load_named_queries(filepath):
    with open(filepath, "r") as f:
        content = f.read()
    queries = {}
    for i in range(1, 8):
        start_marker = f"-- Q{i}_START"
        end_marker   = f"-- Q{i}_END"
        start = content.find(start_marker)
        end   = content.find(end_marker)
        if start != -1 and end != -1:
            sql = content[start + len(start_marker):end].strip()
            if sql.endswith(";"):
                sql = sql[:-1].strip()
            queries[i] = sql
    return queries

QUERIES = load_named_queries(SQL_FILE)



# Query runner (cached per query number)

@st.cache_data
def run_query(q_number):
    sql = QUERIES.get(q_number)
    if not sql:
        return pd.DataFrame()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)

@st.cache_data
def run_sql(sql):
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)



# Header

st.title("🔬 Global Patent Intelligence Dashboard")
st.markdown("Exploring **9.4 million patents** from PatentsView — inventors, companies, countries & trends.")
st.divider()



# Tabs

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Overview",
    "🔍 Query Results",
    "📈 Visualisations",
    "🧪 Advanced Analysis",
])



# TAB 1: OVERVIEW

with tab1:
    st.header("Overview")

    # Key metrics
    total_patents   = run_sql("SELECT COUNT(*) AS n FROM patents")["n"][0]
    total_inventors = run_sql("SELECT COUNT(*) AS n FROM inventors")["n"][0]
    total_companies = run_sql("SELECT COUNT(*) AS n FROM companies")["n"][0]
    year_range      = run_sql("SELECT MIN(year) AS mn, MAX(year) AS mx FROM patents WHERE year IS NOT NULL")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Patents",   f"{int(total_patents):,}")
    col2.metric("Total Inventors", f"{int(total_inventors):,}")
    col3.metric("Total Companies", f"{int(total_companies):,}")
    col4.metric("Year Range",
                f"{int(year_range['mn'][0])} – {int(year_range['mx'][0])}")

    st.divider()

    col_a, col_b, col_c = st.columns(3)

    with col_a:
        st.subheader("🏆 Top 5 Inventors")
        df_inv = run_query(1).head(5)[["name", "country", "patent_count"]]
        df_inv.index = range(1, len(df_inv) + 1)
        st.dataframe(df_inv, use_container_width=True)

    with col_b:
        st.subheader("🏢 Top 5 Companies")
        df_com = run_query(2).head(5)[["name", "patent_count"]]
        df_com.index = range(1, len(df_com) + 1)
        st.dataframe(df_com, use_container_width=True)

    with col_c:
        st.subheader("🌍 Top 5 Countries")
        df_cou = run_query(3).head(5)[["country", "patent_count"]]
        df_cou.index = range(1, len(df_cou) + 1)
        st.dataframe(df_cou, use_container_width=True)

    st.divider()
    st.subheader("📅 Patents Per Year")
    df_trend = run_query(4)
    st.line_chart(df_trend.set_index("year")["patent_count"])



# TAB 2: QUERY RESULTS

with tab2:
    st.header("Query Results")
    st.markdown("Select a query to run and explore the results.")

    query_options = {
        "Q1 — Top Inventors: Who has the most patents?":           1,
        "Q2 — Top Companies: Which companies own the most?":       2,
        "Q3 — Countries: Which produce the most patents?":         3,
        "Q4 — Trends Over Time: Patents created each year?":       4,
        "Q5 — JOIN Query: Patents with inventors and companies":    5,
        "Q6 — CTE Query: Top 3 companies per country":             6,
        "Q7 — Ranking Query: Rank inventors (window functions)":    7,
    }

    selected = st.selectbox("Choose a query:", list(query_options.keys()))
    q_num    = query_options[selected]

    df_result = run_query(q_num)

    st.markdown(f"**{len(df_result)} rows returned**")
    st.dataframe(df_result, use_container_width=True, height=450)

    csv = df_result.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇ Download as CSV",
        data=csv,
        file_name=f"query_{q_num}_results.csv",
        mime="text/csv",
    )



# TAB 3: VISUALISATIONS

with tab3:
    st.header("Visualisations")
    st.markdown("8 charts generated from the patent database.")

    charts = [
        ("01_top20_countries.png",      "Top 20 Countries by Patent Count"),
        ("02_top20_inventors.png",      "Top 20 Inventors by Patent Count"),
        ("03_top20_companies.png",      "Top 20 Companies by Patent Count"),
        ("04_patents_per_year.png",     "Patents Granted Per Year"),
        ("05_top50_inventors_ranked.png","Top 50 Inventors — Ranked by Country"),
        ("06_yoy_growth_rate.png",      "Year-over-Year Patent Growth Rate"),
        ("07_top10_companies_share.png","Top 10 Companies Share of Patents"),
        ("08_top10_countries_share.png","Top 10 Countries Share of Patents"),
    ]

    for i in range(0, len(charts), 2):
        cols = st.columns(2)
        for j, col in enumerate(cols):
            if i + j < len(charts):
                filename, title = charts[i + j]
                path = os.path.join(CHARTS_DIR, filename)
                if os.path.exists(path):
                    col.subheader(title)
                    col.image(Image.open(path), use_container_width=True)
                else:
                    col.warning(f"Chart not found: {filename}\nRun visualisation.py first.")



# TAB 4: ADVANCED ANALYSIS

with tab4:
    st.header("Advanced Analysis")
    st.markdown("Deeper insights beyond the 7 required queries.")

    # ── Analysis 1: Most Active Decades ──
    st.subheader("1. Most Active Patent Decades")
    path = os.path.join(CHARTS_DIR, "09_most_active_decades.png")
    if os.path.exists(path):
        st.image(Image.open(path), use_container_width=True)
    else:
        df_dec = run_sql("""
            SELECT (year / 10) * 10 AS decade, COUNT(*) AS patent_count
            FROM patents WHERE year IS NOT NULL
            GROUP BY decade ORDER BY decade
        """)
        df_dec["decade_label"] = df_dec["decade"].astype(str) + "s"
        st.bar_chart(df_dec.set_index("decade_label")["patent_count"])

    st.divider()

    # ── Analysis 2: Country Growth Over Time ──
    st.subheader("2. Patent Growth Over Time — Top 5 Countries")
    path = os.path.join(CHARTS_DIR, "10_country_growth_over_time.png")
    if os.path.exists(path):
        st.image(Image.open(path), use_container_width=True)
    else:
        st.info("Run analysis.py to generate this chart.")

    st.divider()

    # ── Analysis 3: Top Inventors Per Country ──
    st.subheader("3. Top 5 Inventors Per Country")
    path = os.path.join(CHARTS_DIR, "11_top_inventors_per_country.png")
    if os.path.exists(path):
        st.image(Image.open(path), use_container_width=True)

    csv_path = os.path.join(REPORTS_DIR, "analysis_inventors_per_country.csv")
    if os.path.exists(csv_path):
        df_inv_country = pd.read_csv(csv_path)
        countries      = df_inv_country["country"].unique().tolist()
        selected_country = st.selectbox("Filter by country:", ["All"] + countries)
        if selected_country != "All":
            df_inv_country = df_inv_country[df_inv_country["country"] == selected_country]
        st.dataframe(df_inv_country, use_container_width=True)

    st.divider()

    # ── Analysis 4: Peak Year Per Country ──
    st.subheader("4. Peak Patent Year Per Country")
    path = os.path.join(CHARTS_DIR, "12_peak_year_per_country.png")
    if os.path.exists(path):
        st.image(Image.open(path), use_container_width=True)

    csv_path = os.path.join(REPORTS_DIR, "analysis_peak_year.csv")
    if os.path.exists(csv_path):
        st.dataframe(pd.read_csv(csv_path), use_container_width=True)

    st.divider()

    # ── Analysis 5: Company Concentration ──
    st.subheader("5. Company Patent Concentration")
    st.markdown("""
    How concentrated is patent ownership?
    Do a small number of companies hold most of the patents?
    """)
    path = os.path.join(CHARTS_DIR, "13_company_concentration.png")
    if os.path.exists(path):
        st.image(Image.open(path), use_container_width=True)

    csv_path = os.path.join(REPORTS_DIR, "analysis_concentration.csv")
    if os.path.exists(csv_path):
        df_conc = pd.read_csv(csv_path)
        st.dataframe(df_conc, use_container_width=True)

    st.divider()

    # ── JSON Report Viewer ──
    st.subheader("📄 JSON Report Viewer")
    json_path = os.path.join(REPORTS_DIR, "report.json")
    if os.path.exists(json_path):
        with open(json_path, "r") as f:
            report_data = json.load(f)
        st.markdown(f"**Generated at:** {report_data.get('generated_at', 'N/A')}")
        st.markdown(f"**Queries run:** {len(report_data.get('queries_run', []))}")
        if report_data.get("queries_run"):
            for entry in report_data["queries_run"]:
                with st.expander(f"{entry['query']}  —  {entry['run_at']}"):
                    st.dataframe(pd.DataFrame(entry["results"]), use_container_width=True)
    else:
        st.info("No report.json found yet. Run reports.py and execute some queries first.")