"""
run_queries.py — Loads cleaned CSVs into PostgreSQL with Logging.
Run this FIRST before reports.py or visualisation.py.

Logs: Data loading performance and system metrics
"""

import os
import sys
import pandas as pd
import time
import psutil
import logging
from datetime import datetime
from sqlalchemy import create_engine

CONNECTION_STRING = "postgresql://postgres:root@localhost:5111/patents"
DATA_DIR          = "data/clean"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("reports/data_loading.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Track metrics
process = psutil.Process(os.getpid())
start_time = time.time()
start_memory = process.memory_info().rss / (1024 ** 3)
start_cpu = psutil.cpu_percent(interval=0.1)

logger.info("="*80)
logger.info("DATA LOADING PIPELINE — STARTING")
logger.info("="*80)
logger.info(f"Start time: {datetime.now().isoformat()}")
logger.info(f"Memory: {start_memory:.2f} GB | CPU cores: {psutil.cpu_count()}")

print("  PATENT DATA PIPELINE — LOADING DATA INTO DATABASE")

print(f"  Target: {CONNECTION_STRING.split('@')[1]}")
sys.stdout.flush()

engine = create_engine(CONNECTION_STRING)
print("  Database connection established\n")
logger.info(f"Database connected: {CONNECTION_STRING.split('@')[1]}")


# Load patents

print("[1/4] Reading clean_patents.csv...")
sys.stdout.flush()
load_start = time.time()

patents_df = pd.read_csv(os.path.join(DATA_DIR, "clean_patents.csv"))
read_duration = time.time() - load_start

print(f"      Loaded {len(patents_df):,} rows  |  Columns: {list(patents_df.columns)}")
print(f"      Read time: {read_duration:.2f}s | Throughput: {len(patents_df)/read_duration:,.0f} rows/sec")
logger.info(f"[1/4] Patents: {len(patents_df):,} rows | Read: {read_duration:.2f}s | Throughput: {len(patents_df)/read_duration:,.0f} rows/sec")

patents_df = patents_df[["patent_id", "title", "abstract", "filing_date", "year"]]
print("      Inserting into 'patents' table...")
sys.stdout.flush()

insert_start = time.time()
patents_df.to_sql("patents", engine, if_exists="replace", index=False, chunksize=1000)
insert_duration = time.time() - insert_start

print(f"      ✓ Done — {len(patents_df):,} patents inserted ({insert_duration:.2f}s)\n")
logger.info(f"      Insert: {insert_duration:.2f}s | Throughput: {len(patents_df)/insert_duration:,.0f} rows/sec")


# Load inventors

print("[2/4] Reading clean_inventors.csv...")
sys.stdout.flush()
load_start = time.time()

inventors_df = pd.read_csv(os.path.join(DATA_DIR, "clean_inventors.csv"))
read_duration = time.time() - load_start

print(f"      Loaded {len(inventors_df):,} rows  |  Columns: {list(inventors_df.columns)}")
print(f"      Read time: {read_duration:.2f}s | Throughput: {len(inventors_df)/read_duration:,.0f} rows/sec")
logger.info(f"[2/4] Inventors: {len(inventors_df):,} rows | Read: {read_duration:.2f}s | Throughput: {len(inventors_df)/read_duration:,.0f} rows/sec")

inventors_df = inventors_df[["inventor_id", "name", "country"]]
print("      Inserting into 'inventors' table...")
sys.stdout.flush()

insert_start = time.time()
inventors_df.to_sql("inventors", engine, if_exists="replace", index=False, chunksize=1000)
insert_duration = time.time() - insert_start

print(f"      ✓ Done — {len(inventors_df):,} inventors inserted ({insert_duration:.2f}s)\n")
logger.info(f"      Insert: {insert_duration:.2f}s | Throughput: {len(inventors_df)/insert_duration:,.0f} rows/sec")


# Load companies

print("[3/4] Reading clean_companies.csv...")
sys.stdout.flush()
load_start = time.time()

companies_df = pd.read_csv(os.path.join(DATA_DIR, "clean_companies.csv"))
read_duration = time.time() - load_start

print(f"      Loaded {len(companies_df):,} rows  |  Columns: {list(companies_df.columns)}")
print(f"      Read time: {read_duration:.2f}s | Throughput: {len(companies_df)/read_duration:,.0f} rows/sec")
logger.info(f"[3/4] Companies: {len(companies_df):,} rows | Read: {read_duration:.2f}s | Throughput: {len(companies_df)/read_duration:,.0f} rows/sec")

companies_df = companies_df[["company_id", "name"]]
print("      Inserting into 'companies' table...")
sys.stdout.flush()

insert_start = time.time()
companies_df.to_sql("companies", engine, if_exists="replace", index=False, chunksize=1000)
insert_duration = time.time() - insert_start

print(f"      ✓ Done — {len(companies_df):,} companies inserted ({insert_duration:.2f}s)\n")
logger.info(f"      Insert: {insert_duration:.2f}s | Throughput: {len(companies_df)/insert_duration:,.0f} rows/sec")


# Load relationships

print("[4/4] Reading clean_relationships.csv...")
sys.stdout.flush()
load_start = time.time()

relationships_df = pd.read_csv(os.path.join(DATA_DIR, "clean_relationships.csv"))
read_duration = time.time() - load_start

print(f"      Loaded {len(relationships_df):,} rows  |  Columns: {list(relationships_df.columns)}")
print(f"      Read time: {read_duration:.2f}s | Throughput: {len(relationships_df)/read_duration:,.0f} rows/sec")
logger.info(f"[4/4] Relationships: {len(relationships_df):,} rows | Read: {read_duration:.2f}s | Throughput: {len(relationships_df)/read_duration:,.0f} rows/sec")

relationships_df = relationships_df[["patent_id", "inventor_id", "company_id"]]
print("      Inserting into 'relationships' table...")
sys.stdout.flush()

insert_start = time.time()
relationships_df.to_sql("relationships", engine, if_exists="replace", index=False, chunksize=1000)
insert_duration = time.time() - insert_start

print(f"      ✓ Done — {len(relationships_df):,} relationships inserted ({insert_duration:.2f}s)\n")
logger.info(f"      Insert: {insert_duration:.2f}s | Throughput: {len(relationships_df)/insert_duration:,.0f} rows/sec")


# Final summary
end_time = time.time()
end_memory = process.memory_info().rss / (1024 ** 3)
total_duration = end_time - start_time
memory_delta = end_memory - start_memory
end_cpu = psutil.cpu_percent(interval=0.1)

print("\n  ✅ All data loaded successfully!")
logger.info("="*80)
logger.info("DATA LOADING — COMPLETED")
logger.info("="*80)
logger.info(f"Total execution time: {total_duration:.2f}s")
logger.info(f"Memory: {start_memory:.2f} GB → {end_memory:.2f} GB (Δ {memory_delta:+.2f} GB)")
logger.info(f"CPU: Start {start_cpu:.1f}% → End {end_cpu:.1f}%")
logger.info(f"Total records loaded: {len(patents_df) + len(inventors_df) + len(companies_df) + len(relationships_df):,}")
logger.info("="*80)

print(f"  ⏱️  Total time: {total_duration:.2f}s")
print(f"  💾 Memory: {memory_delta:+.2f} GB")
print(f"  Records: {len(patents_df) + len(inventors_df) + len(companies_df) + len(relationships_df):,}")
print(f"  Logs: reports/data_loading.log")
