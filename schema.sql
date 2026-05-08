-- =============================================================================
-- Global Patent Intelligence Data Pipeline
-- schema.sql — Table Definitions + Indexes + Full-Text Search
-- =============================================================================
-- Run order: schema.sql → run_queries.py → visualisation.py → reports.py
-- =============================================================================

-- Drop tables in reverse dependency order (safe re-run)
DROP TABLE IF EXISTS relationships;
DROP TABLE IF EXISTS inventors;
DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS patents;

-- -----------------------------------------------------------------------------
-- patents table
-- abstract_tsv: auto-generated tsvector for NLP full-text search in dashboard.
-- Combines title + abstract so keyword searches hit both fields simultaneously.
-- -----------------------------------------------------------------------------
CREATE TABLE patents (
    patent_id    TEXT PRIMARY KEY,
    title        TEXT,
    abstract     TEXT,
    filing_date  DATE,
    year         INTEGER,
    abstract_tsv tsvector GENERATED ALWAYS AS (
        to_tsvector(
            'english',
            COALESCE(title, '') || ' ' || COALESCE(abstract, '')
        )
    ) STORED
);

-- GIN index: makes plainto_tsquery searches fast on 9M+ rows
CREATE INDEX idx_patents_fts  ON patents USING GIN(abstract_tsv);
-- B-tree index on year: speeds up trend/decade/YoY queries significantly
CREATE INDEX idx_patents_year ON patents(year);

-- -----------------------------------------------------------------------------
-- inventors table
-- -----------------------------------------------------------------------------
CREATE TABLE inventors (
    inventor_id TEXT PRIMARY KEY,
    name        TEXT,
    country     TEXT
);

CREATE INDEX idx_inventors_country ON inventors(country);

-- -----------------------------------------------------------------------------
-- companies (assignees) table
-- -----------------------------------------------------------------------------
CREATE TABLE companies (
    company_id TEXT PRIMARY KEY,
    name       TEXT
);

-- -----------------------------------------------------------------------------
-- relationships table  (patent <-> inventor <-> company)
-- All three indexes needed — analytical queries JOIN on each column separately
-- -----------------------------------------------------------------------------
CREATE TABLE relationships (
    patent_id   TEXT,
    inventor_id TEXT,
    company_id  TEXT
);

CREATE INDEX idx_rel_patent   ON relationships(patent_id);
CREATE INDEX idx_rel_inventor ON relationships(inventor_id);
CREATE INDEX idx_rel_company  ON relationships(company_id);