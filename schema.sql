-- =============================================================================
-- Global Patent Intelligence Data Pipeline
-- schema.sql — Table Definitions ONLY
-- =============================================================================

-- Drop tables in reverse dependency order (safe re-run)
DROP TABLE IF EXISTS relationships;
DROP TABLE IF EXISTS inventors;
DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS patents;


-- -----------------------------------------------------------------------------
-- patents table
-- -----------------------------------------------------------------------------
CREATE TABLE patents (
    patent_id   TEXT    PRIMARY KEY,
    title       TEXT,
    abstract    TEXT,
    filing_date DATE,
    year        INTEGER
);


-- -----------------------------------------------------------------------------
-- inventors table
-- -----------------------------------------------------------------------------
CREATE TABLE inventors (
    inventor_id TEXT    PRIMARY KEY,
    name        TEXT,
    country     TEXT
);


-- -----------------------------------------------------------------------------
-- companies (assignees) table
-- -----------------------------------------------------------------------------
CREATE TABLE companies (
    company_id  TEXT    PRIMARY KEY,
    name        TEXT
);


-- -----------------------------------------------------------------------------
-- relationships table  (patent <-> inventor <-> company)
-- -----------------------------------------------------------------------------
CREATE TABLE relationships (
    patent_id   TEXT,
    inventor_id TEXT,
    company_id  TEXT
);