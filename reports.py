"""
reports.py — Interactive Patent Intelligence Report
-----------------------------------------------------

Features:
  - Numbered menu for all 7 queries (loops until you exit)
  - Prints results to console after each selection
  - Appends each result to reports/report.json
  - Option to export top_inventors.csv, top_companies.csv, country_trends.csv
  - Option to print a full console summary report
  - Auto-exports CSVs and prints summary on exit
"""

import os
import sys
import json
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime


# Config

CONNECTION_STRING = "postgresql://postgres:root@localhost:5111/patents"
REPORTS_DIR       = "reports"
JSON_FILE         = os.path.join(REPORTS_DIR, "report.json")
SQL_FILE          = "queries.sql"

os.makedirs(REPORTS_DIR, exist_ok=True)


# Connect


print("  PATENT INTELLIGENCE REPORT SYSTEM")

print("  Connecting to database...")
sys.stdout.flush()

engine = create_engine(CONNECTION_STRING)
print("  Connected.\n")



# Load & parse queries.sql into named blocks

def load_named_queries(filepath):
    """Extract each Q1..Q7 block from queries.sql using marker comments."""
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



# JSON helpers

def load_json_report():
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, "r") as f:
            return json.load(f)
    return {"generated_at": str(datetime.now()), "queries_run": []}

def save_json_report(data):
    with open(JSON_FILE, "w") as f:
        json.dump(data, f, indent=2, default=str)

def append_to_json(report, query_label, df):
    entry = {
        "query":     query_label,
        "run_at":    str(datetime.now()),
        "row_count": len(df),
        "results":   df.head(20).to_dict(orient="records"),
    }
    report["queries_run"].append(entry)
    report["generated_at"] = str(datetime.now())
    save_json_report(report)



# Run a query → DataFrame

def run_query(q_number):
    sql = QUERIES.get(q_number)
    if not sql:
        print(f"  Query Q{q_number} not found in {SQL_FILE}")
        return None
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    return df



# Display result

def display_result(label, df):
    
    print(f"  {label}")
    
    if df is None or df.empty:
        print("  No results returned.")
    else:
        print(df.to_string(index=False))
        print(f"\n  → {len(df)} rows returned")



# Export CSVs

def export_csvs():
    
    print("  EXPORTING CSV FILES")
    

    exports = [
        (1, "top_inventors.csv"),
        (2, "top_companies.csv"),
        (3, "country_trends.csv"),
    ]
    for q_num, filename in exports:
        df = run_query(q_num)
        if df is not None:
            path = os.path.join(REPORTS_DIR, filename)
            df.to_csv(path, index=False)
            print(f"  Saved → {path}  ({len(df)} rows)")

    print("  All CSVs exported.")



# Console summary report

def print_console_report():
    
    print("  ================== PATENT REPORT ===================")
    

    with engine.connect() as conn:
        total = pd.read_sql("SELECT COUNT(*) AS total FROM patents", conn)["total"][0]
    print(f"\n  Total Patents: {int(total):,}")

    df_inv = run_query(1)
    if df_inv is not None and not df_inv.empty:
        print("\n  Top Inventors:")
        for idx, row in df_inv.head(5).iterrows():
            print(f"    {idx + 1}. {row['name']} ({row.get('country', 'N/A')}) "
                  f"— {int(row['patent_count']):,} patents")

    df_com = run_query(2)
    if df_com is not None and not df_com.empty:
        print("\n  Top Companies:")
        for idx, row in df_com.head(5).iterrows():
            print(f"    {idx + 1}. {row['name']} — {int(row['patent_count']):,} patents")

    df_cou = run_query(3)
    if df_cou is not None and not df_cou.empty:
        print("\n  Top Countries:")
        for idx, row in df_cou.head(5).iterrows():
            print(f"    {idx + 1}. {row['country']} — {int(row['patent_count']):,} patents")

    print(f"\n  Report generated at : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  JSON report saved   → {JSON_FILE}")
    print(f"  CSVs saved          → {REPORTS_DIR}/")
    



# Menu

MENU_ITEMS = {
    "1": ("Q1 — Top Inventors: Who has the most patents?",            1),
    "2": ("Q2 — Top Companies: Which companies own the most?",        2),
    "3": ("Q3 — Countries: Which produce the most patents?",          3),
    "4": ("Q4 — Trends Over Time: Patents created each year?",        4),
    "5": ("Q5 — JOIN Query: Patents with inventors and companies",     5),
    "6": ("Q6 — CTE Query: Top 3 companies per country",              6),
    "7": ("Q7 — Ranking Query: Rank inventors (window functions)",     7),
    "8": ("Export CSVs (top_inventors, top_companies, country_trends)", None),
    "9": ("Print Full Console Summary Report",                         None),
    "0": ("Exit",                                                      None),
}

def print_menu():
    print(f"\n{'─' * 60}")
    print("  PATENT INTELLIGENCE — SELECT A QUERY")
    print("─" * 60)
    for key, (label, _) in MENU_ITEMS.items():
        print(f"  [{key}]  {label}")
    print("─" * 60)



# Main loop

report = load_json_report()

while True:
    print_menu()
    choice = input("  Enter your choice: ").strip()

    if choice not in MENU_ITEMS:
        print("  Invalid choice. Please enter a number from the menu.")
        continue

    label, q_number = MENU_ITEMS[choice]

    if choice == "0":
        export_csvs()
        print_console_report()
        print("\n  Goodbye!\n")
        break

    elif choice == "8":
        export_csvs()

    elif choice == "9":
        print_console_report()

    else:
        df = run_query(q_number)
        display_result(label, df)
        if df is not None and not df.empty:
            append_to_json(report, label, df)
            print(f"   Result appended to {JSON_FILE}")