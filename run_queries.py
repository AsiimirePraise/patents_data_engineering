"""
run_queries.py — Loads cleaned CSVs into PostgreSQL.
Run this FIRST before reports.py, visualisation.py, or dashboard.py.

Schema strategy — single source of truth:
  - This script reads and executes schema.sql automatically at startup.
  - schema.sql uses DROP TABLE IF EXISTS + CREATE TABLE, so every run
    wipes old data and recreates tables with the correct types.
  - No duplicates on rerun. No DDL duplicated in this file.
  - patent_id is TEXT in schema.sql — this prevents the bigint crash
    on design patent IDs like 'D1000000' that appears at chunk 853.

Logging features:
  - Machine specs logged at startup (CPU model, cores, total RAM, OS)
  - Per 10,000-row chunk: elapsed time, CPU%, RAM%
  - Per-table summary: total rows, total time, rows/sec
  - Final pipeline summary
  - All output goes to both console and reports/data_loading.log
"""

import os
import sys
import time
import platform
import logging
from datetime import datetime

import pandas as pd
import psutil
from sqlalchemy import create_engine, text as sa_text

# ─── Config ───────────────────────────────────────────────────────────────────
CONNECTION_STRING = "postgresql://postgres:root@localhost:5111/patents"
DATA_DIR          = "data/clean"
SCHEMA_FILE       = "schema.sql"
CHUNK_SIZE        = 10_000

os.makedirs("reports", exist_ok=True)

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("reports/data_loading.log", mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("pipeline")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def log_machine_specs():
    vm = psutil.virtual_memory()
    log.info("=" * 72)
    log.info("PATENT DATA PIPELINE — STARTUP")
    log.info("=" * 72)
    log.info(f"Timestamp   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"OS          : {platform.system()} {platform.release()} ({platform.version()})")
    log.info(f"Machine     : {platform.machine()}")
    log.info(f"CPU         : {platform.processor() or 'N/A'}")
    log.info(f"CPU Cores   : {psutil.cpu_count(logical=False)} physical / "
             f"{psutil.cpu_count(logical=True)} logical")
    log.info(f"Total RAM   : {vm.total / 1e9:.2f} GB")
    log.info(f"Available   : {vm.available / 1e9:.2f} GB")
    log.info(f"Python      : {platform.python_version()}")
    log.info("=" * 72)


def snapshot():
    return psutil.cpu_percent(interval=0.05), psutil.virtual_memory().percent


def apply_schema(engine):
    """
    Read schema.sql and execute it against the database.
    schema.sql uses DROP TABLE IF EXISTS so this is safe to run on every
    startup — old data is wiped and tables are recreated with correct types.
    This is the single source of truth for the schema; no DDL lives here.
    """
    if not os.path.exists(SCHEMA_FILE):
        log.error(f"schema.sql not found at '{SCHEMA_FILE}' — cannot continue.")
        sys.exit(1)

    with open(SCHEMA_FILE, "r") as f:
        sql = f.read()

    # Split on ; and execute each statement individually
    # (psycopg2 does not support multiple statements in one execute call)
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(sa_text(stmt))

    log.info(f"  ✓ schema.sql applied — all tables dropped and recreated\n")


def load_table(df: pd.DataFrame, table_name: str, engine, columns: list[str]):
    """
    Insert a DataFrame into PostgreSQL in CHUNK_SIZE-row batches.
    Uses if_exists='append' — tables already exist from schema.sql so
    pandas only inserts data and never touches or infers the schema.
    Logs timing + system metrics for every chunk.
    Returns (total_rows, total_seconds).
    """
    df = df[columns].copy()

    # Cast every _id column to str — belt-and-suspenders against any
    # remaining numeric inference (e.g. patent_id 'D1000000' design patents)
    for col in df.columns:
        if col.endswith("_id"):
            df[col] = df[col].astype(str)

    total_rows = len(df)
    n_chunks   = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
    table_t0   = time.perf_counter()

    log.info(f"  Loading '{table_name}': {total_rows:,} rows | "
             f"{n_chunks} chunks × {CHUNK_SIZE:,}")

    for chunk_idx, start in enumerate(range(0, total_rows, CHUNK_SIZE), start=1):
        chunk   = df.iloc[start : start + CHUNK_SIZE]
        end_row = min(start + CHUNK_SIZE, total_rows)
        t0      = time.perf_counter()

        # append only — schema already created correctly by schema.sql
        chunk.to_sql(table_name, engine, if_exists="append", index=False)

        elapsed      = time.perf_counter() - t0
        cpu_pct, ram = snapshot()

        log.info(
            f"    [{table_name}] chunk {chunk_idx:>4}/{n_chunks} | "
            f"rows {start+1:>9,}–{end_row:>9,} | "
            f"{elapsed:>5.2f}s | "
            f"CPU {cpu_pct:>5.1f}% | RAM {ram:>5.1f}%"
        )

    total_secs = time.perf_counter() - table_t0
    rps        = total_rows / total_secs if total_secs > 0 else 0
    log.info(f"  ✓ '{table_name}' done — {total_rows:,} rows in "
             f"{total_secs:.1f}s ({rps:,.0f} rows/sec)\n")
    return total_rows, total_secs


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    log_machine_specs()

    log.info(f"Connecting to: {CONNECTION_STRING.split('@')[1]}")
    engine = create_engine(CONNECTION_STRING)
    log.info("Database connection established")

    # Apply schema.sql — drops + recreates all tables with correct types.
    # This is the only place DDL runs; no schema defined anywhere else.
    log.info(f"Applying {SCHEMA_FILE} ...")
    apply_schema(engine)

    pipeline_t0 = time.perf_counter()
    summary     = {}

    # ── 1. Patents ─────────────────────────────────────────────────────────────
    log.info("[1/4] Reading clean_patents.csv ...")
    # dtype on patent_id prevents pandas inferring bigint from the first
    # 8M numeric-looking rows — design patents have IDs like 'D1000000'
    df = pd.read_csv(os.path.join(DATA_DIR, "clean_patents.csv"),
                     dtype={"patent_id": str})
    log.info(f"      Read {len(df):,} rows | columns: {list(df.columns)}")
    rows, secs = load_table(df, "patents", engine,
                            ["patent_id", "title", "abstract", "filing_date", "year"])
    summary["patents"] = (rows, secs)

    # ── 2. Inventors ───────────────────────────────────────────────────────────
    log.info("[2/4] Reading clean_inventors.csv ...")
    df = pd.read_csv(os.path.join(DATA_DIR, "clean_inventors.csv"),
                     dtype={"inventor_id": str})
    log.info(f"      Read {len(df):,} rows | columns: {list(df.columns)}")
    rows, secs = load_table(df, "inventors", engine,
                            ["inventor_id", "name", "country"])
    summary["inventors"] = (rows, secs)

    # ── 3. Companies ───────────────────────────────────────────────────────────
    log.info("[3/4] Reading clean_companies.csv ...")
    df = pd.read_csv(os.path.join(DATA_DIR, "clean_companies.csv"),
                     dtype={"company_id": str})
    log.info(f"      Read {len(df):,} rows | columns: {list(df.columns)}")
    rows, secs = load_table(df, "companies", engine,
                            ["company_id", "name"])
    summary["companies"] = (rows, secs)

    # ── 4. Relationships ───────────────────────────────────────────────────────
    log.info("[4/4] Reading clean_relationships.csv ...")
    df = pd.read_csv(os.path.join(DATA_DIR, "clean_relationships.csv"),
                     dtype={"patent_id": str, "inventor_id": str, "company_id": str})
    log.info(f"      Read {len(df):,} rows | columns: {list(df.columns)}")
    rows, secs = load_table(df, "relationships", engine,
                            ["patent_id", "inventor_id", "company_id"])
    summary["relationships"] = (rows, secs)

    # ── Pipeline summary ───────────────────────────────────────────────────────
    total_elapsed = time.perf_counter() - pipeline_t0
    total_rows    = sum(r for r, _ in summary.values())
    cpu_end, ram_end = snapshot()

    log.info("=" * 72)
    log.info("PIPELINE COMPLETE — SUMMARY")
    log.info("=" * 72)
    for tbl, (r, s) in summary.items():
        rps = r / s if s > 0 else 0
        log.info(f"  {tbl:<20}: {r:>10,} rows | {s:>7.1f}s | {rps:>10,.0f} rows/sec")
    log.info(f"  {'TOTAL':<20}: {total_rows:>10,} rows | {total_elapsed:>7.1f}s")
    log.info(f"  Final CPU : {cpu_end:.1f}%  |  Final RAM: {ram_end:.1f}%")
    log.info(f"  Log saved → reports/data_loading.log")
    log.info("=" * 72)

    print(f"\n  All data loaded successfully!")
    print(f"  Total time : {total_elapsed:.1f}s")
    print(f"  Total rows : {total_rows:,}")
    print(f"  Log        : reports/data_loading.log")


if __name__ == "__main__":
    main()