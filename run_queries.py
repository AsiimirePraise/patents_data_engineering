"""
run_queries.py — Runs each .sql query file against PostgreSQL
and prints the results as a table.
"""

import pandas as pd
from sqlalchemy import create_engine, text
import sys

# Config 
CONNECTION_STRING = "postgresql://postgres:root@localhost:5111/patents"

SQL_FILES = [
    ("Q1 + Q7 : Inventors",  "query_inventors.sql"),
    ("Q2      : Companies",  "query_companies.sql"),
    ("Q3      : Countries",  "query_countries.sql"),
    ("Q4+Q5+Q6: Patents",    "query_patents.sql"),
]

print(f"{'='*60}")
print("  STARTING QUERIES AND DATA LOAD")
print(f"{'='*60}")
print(f"Connecting to database: {CONNECTION_STRING.split('@')[1]}")
sys.stdout.flush()

# Connect 
engine = create_engine(CONNECTION_STRING)
print(" Database connection established")


# Helper: split a .sql file into individual statements 
def load_sql_file(filepath):
    with open(filepath, "r") as f:
        content = f.read()

    # Remove comment lines, split on semicolons
    statements = []
    for stmt in content.split(";"):
        cleaned = stmt.strip()
        # Skip empty blocks or comment-only blocks
        lines = [l for l in cleaned.splitlines() if not l.strip().startswith("--")]
        cleaned = "\n".join(lines).strip()
        if cleaned:
            statements.append(cleaned)
    return statements


#  Run Each SQL File
import sys

with engine.connect() as conn:
    for label, sql_file in SQL_FILES:
        print(f"\n{'='*60}")
        print(f"  {label}  |  {sql_file}")
        print(f"{'='*60}")

        print(f"  Reading SQL file: {sql_file}")
        statements = load_sql_file(sql_file)
        print(f"  Found {len(statements)} SQL statement(s)")

        for i, stmt in enumerate(statements, start=1):
            try:
                print(f"  Executing statement {i}/{len(statements)}...")
                sys.stdout.flush()
                result = conn.execute(text(stmt))

                # If the statement returns rows, show them
                if result.returns_rows:
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    print(f"\n--- Statement {i} result ({len(df)} rows) ---")
                    print(df.to_string(index=False))
                else:
                    conn.commit()
                    print(f"  Statement {i} executed successfully")

            except Exception as e:
                print(f"\n Error in statement {i}: {e}")
                sys.stdout.flush()

print("\n All queries complete.")



# Load cleaned CSV files into PostgreSQL tables
import os
import sys

data_dir = "data/clean"

print(f"\n{'='*60}")
print("  LOADING CSV DATA INTO DATABASE")
print(f"{'='*60}")

# Load patents
print(f"\n[1/4] Reading clean_patents.csv...")
sys.stdout.flush()
patents_df = pd.read_csv(os.path.join(data_dir, "clean_patents.csv"))
print(f"      Loaded {len(patents_df)} patent rows from CSV")
print(f"      Columns: {list(patents_df.columns)}")
# Select only the columns that match the schema
patents_df = patents_df[["patent_id", "title", "abstract", "filing_date", "year"]]
print(f"      Inserting into 'patents' table...")
sys.stdout.flush()
patents_df.to_sql("patents", engine, if_exists="replace", index=False, chunksize=1000)
print(f"       Successfully inserted {len(patents_df)} patents")

# Load inventors
print(f"\n[2/4] Reading clean_inventors.csv...")
sys.stdout.flush()
inventors_df = pd.read_csv(os.path.join(data_dir, "clean_inventors.csv"))
print(f"      Loaded {len(inventors_df)} inventor rows from CSV")
print(f"      Columns: {list(inventors_df.columns)}")
# Select only the columns that match the schema
inventors_df = inventors_df[["inventor_id", "name", "country"]]
print(f"      Inserting into 'inventors' table...")
sys.stdout.flush()
inventors_df.to_sql("inventors", engine, if_exists="replace", index=False, chunksize=1000)
print(f"       Successfully inserted {len(inventors_df)} inventors")

# Load companies
print(f"\n[3/4] Reading clean_companies.csv...")
sys.stdout.flush()
companies_df = pd.read_csv(os.path.join(data_dir, "clean_companies.csv"))
print(f"      Loaded {len(companies_df)} company rows from CSV")
print(f"      Columns: {list(companies_df.columns)}")
# Select only the columns that match the schema
companies_df = companies_df[["company_id", "name"]]
print(f"      Inserting into 'companies' table...")
sys.stdout.flush()
companies_df.to_sql("companies", engine, if_exists="replace", index=False, chunksize=1000)
print(f"       Successfully inserted {len(companies_df)} companies")

# Load relationships
print(f"\n[4/4] Reading clean_relationships.csv...")
sys.stdout.flush()
relationships_df = pd.read_csv(os.path.join(data_dir, "clean_relationships.csv"))
print(f"      Loaded {len(relationships_df)} relationship rows from CSV")
print(f"      Columns: {list(relationships_df.columns)}")
# Select only the columns that match the schema
relationships_df = relationships_df[["patent_id", "inventor_id", "company_id"]]
print(f"      Inserting into 'relationships' table...")
sys.stdout.flush()
relationships_df.to_sql("relationships", engine, if_exists="replace", index=False, chunksize=1000)
print(f"       Successfully inserted {len(relationships_df)} relationships")

print(f"\n{'='*60}")
print(" All data loaded successfully!")
print(f"{'='*60}")
