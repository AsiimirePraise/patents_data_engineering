"""
run_queries.py — Loads cleaned CSVs into PostgreSQL.
Run this FIRST before reports.py or visualisation.py.
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine

CONNECTION_STRING = "postgresql://postgres:root@localhost:5111/patents"
DATA_DIR          = "data/clean"

print("  PATENT DATA PIPELINE — LOADING DATA INTO DATABASE")

print(f"  Target: {CONNECTION_STRING.split('@')[1]}")
sys.stdout.flush()

engine = create_engine(CONNECTION_STRING)
print("  Database connection established\n")


# Load patents

print("[1/4] Reading clean_patents.csv...")
sys.stdout.flush()
patents_df = pd.read_csv(os.path.join(DATA_DIR, "clean_patents.csv"))
print(f"      Loaded {len(patents_df):,} rows  |  Columns: {list(patents_df.columns)}")
patents_df = patents_df[["patent_id", "title", "abstract", "filing_date", "year"]]
print("      Inserting into 'patents' table...")
sys.stdout.flush()
patents_df.to_sql("patents", engine, if_exists="replace", index=False, chunksize=1000)
print(f"      Done — {len(patents_df):,} patents inserted\n")


# Load inventors

print("[2/4] Reading clean_inventors.csv...")
sys.stdout.flush()
inventors_df = pd.read_csv(os.path.join(DATA_DIR, "clean_inventors.csv"))
print(f"      Loaded {len(inventors_df):,} rows  |  Columns: {list(inventors_df.columns)}")
inventors_df = inventors_df[["inventor_id", "name", "country"]]
print("      Inserting into 'inventors' table...")
sys.stdout.flush()
inventors_df.to_sql("inventors", engine, if_exists="replace", index=False, chunksize=1000)
print(f"      Done — {len(inventors_df):,} inventors inserted\n")


# Load companies

print("[3/4] Reading clean_companies.csv...")
sys.stdout.flush()
companies_df = pd.read_csv(os.path.join(DATA_DIR, "clean_companies.csv"))
print(f"      Loaded {len(companies_df):,} rows  |  Columns: {list(companies_df.columns)}")
companies_df = companies_df[["company_id", "name"]]
print("      Inserting into 'companies' table...")
sys.stdout.flush()
companies_df.to_sql("companies", engine, if_exists="replace", index=False, chunksize=1000)
print(f"      Done — {len(companies_df):,} companies inserted\n")


# Load relationships

print("[4/4] Reading clean_relationships.csv...")
sys.stdout.flush()
relationships_df = pd.read_csv(os.path.join(DATA_DIR, "clean_relationships.csv"))
print(f"      Loaded {len(relationships_df):,} rows  |  Columns: {list(relationships_df.columns)}")
relationships_df = relationships_df[["patent_id", "inventor_id", "company_id"]]
print("      Inserting into 'relationships' table...")
sys.stdout.flush()
relationships_df.to_sql("relationships", engine, if_exists="replace", index=False, chunksize=1000)
print(f"      Done — {len(relationships_df):,} relationships inserted\n")


print("  All data loaded successfully!")
