-- =============================================================================
-- Global Patent Intelligence Data Pipeline
-- queries.sql — 7 Required Queries + Visualisation Queries
-- =============================================================================


-- ----------------------------------------------------------------------------
-- Q1: Top Inventors — Who has the most patents?
-- ----------------------------------------------------------------------------
-- Q1_START
SELECT
    i.inventor_id,
    i.name,
    i.country,
    COUNT(DISTINCT r.patent_id) AS patent_count
FROM inventors i
JOIN relationships r ON i.inventor_id = r.inventor_id
GROUP BY i.inventor_id, i.name, i.country
ORDER BY patent_count DESC
LIMIT 20;
-- Q1_END


-- ----------------------------------------------------------------------------
-- Q2: Top Companies — Which companies own the most patents?
-- ----------------------------------------------------------------------------
-- Q2_START
SELECT
    c.company_id,
    c.name,
    COUNT(DISTINCT r.patent_id) AS patent_count
FROM companies c
JOIN relationships r ON c.company_id = r.company_id
GROUP BY c.company_id, c.name
ORDER BY patent_count DESC
LIMIT 20;
-- Q2_END


-- ----------------------------------------------------------------------------
-- Q3: Countries — Which countries produce the most patents?
-- ----------------------------------------------------------------------------
-- Q3_START
SELECT
    i.country,
    COUNT(DISTINCT r.patent_id) AS patent_count
FROM inventors i
JOIN relationships r ON i.inventor_id = r.inventor_id
WHERE i.country IS NOT NULL
  AND i.country != 'Unknown'
GROUP BY i.country
ORDER BY patent_count DESC
LIMIT 20;
-- Q3_END


-- ----------------------------------------------------------------------------
-- Q4: Trends Over Time — How many patents are created each year?
-- ----------------------------------------------------------------------------
-- Q4_START
SELECT
    year,
    COUNT(*) AS patent_count
FROM patents
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year ASC;
-- Q4_END


-- ----------------------------------------------------------------------------
-- Q5: JOIN Query — Combine patents with inventors and companies
-- ----------------------------------------------------------------------------
-- Q5_START
SELECT
    p.patent_id,
    p.title,
    p.filing_date,
    p.year,
    i.name       AS inventor_name,
    i.country    AS inventor_country,
    c.name       AS company_name
FROM patents p
JOIN relationships  r ON p.patent_id   = r.patent_id
JOIN inventors      i ON r.inventor_id = i.inventor_id
LEFT JOIN companies c ON r.company_id  = c.company_id
ORDER BY p.year DESC, p.patent_id
LIMIT 100;
-- Q5_END


-- ----------------------------------------------------------------------------
-- Q6: CTE Query — Top 3 companies per country (WITH statement)
-- ----------------------------------------------------------------------------
-- Q6_START
WITH company_country_counts AS (
    SELECT
        c.name  AS company_name,
        i.country,
        COUNT(DISTINCT r.patent_id) AS patent_count
    FROM companies c
    JOIN relationships  r ON c.company_id  = r.company_id
    JOIN inventors      i ON r.inventor_id = i.inventor_id
    WHERE i.country IS NOT NULL
      AND i.country != 'Unknown'
    GROUP BY c.name, i.country
),
ranked AS (
    SELECT
        company_name,
        country,
        patent_count,
        RANK() OVER (PARTITION BY country ORDER BY patent_count DESC) AS rnk
    FROM company_country_counts
)
SELECT
    country,
    company_name,
    patent_count,
    rnk
FROM ranked
WHERE rnk <= 3
ORDER BY country, rnk;
-- Q6_END


-- ----------------------------------------------------------------------------
-- Q7: Ranking Query — Rank inventors using window functions
-- ----------------------------------------------------------------------------
-- Q7_START
SELECT
    inventor_id,
    name,
    country,
    patent_count,
    RANK() OVER (ORDER BY patent_count DESC)                       AS global_rank,
    RANK() OVER (PARTITION BY country ORDER BY patent_count DESC)  AS country_rank,
    ROUND(
        100.0 * patent_count / SUM(patent_count) OVER (),
        4
    )                                                              AS pct_of_total
FROM (
    SELECT
        i.inventor_id,
        i.name,
        i.country,
        COUNT(DISTINCT r.patent_id) AS patent_count
    FROM inventors i
    JOIN relationships r ON i.inventor_id = r.inventor_id
    GROUP BY i.inventor_id, i.name, i.country
) ranked_inventors
ORDER BY global_rank
LIMIT 50;
-- Q7_END


-- =============================================================================
-- VISUALISATION QUERIES
-- Used by visualisation.py — DO NOT rename the markers
-- =============================================================================

-- Chart 1: Top 20 Inventors by Patent Weight
-- VIZ_CHART_1_START
SELECT
    i.name,
    i.country,
    COUNT(DISTINCT r.patent_id) AS patent_count
FROM inventors i
JOIN relationships r ON i.inventor_id = r.inventor_id
GROUP BY i.inventor_id, i.name, i.country
ORDER BY patent_count DESC
LIMIT 20;
-- VIZ_CHART_1_END


-- Chart 2 Main: Lorenz curve data — patents per company
-- VIZ_CHART_2_MAIN_START
SELECT
    c.name                      AS assignee,
    COUNT(DISTINCT r.patent_id) AS patent_count
FROM companies c
JOIN relationships r ON c.company_id = r.company_id
WHERE c.name IS NOT NULL
GROUP BY c.company_id, c.name
ORDER BY patent_count DESC;
-- VIZ_CHART_2_MAIN_END


-- Chart 2 Fallback: Lorenz curve — patents per inventor (if companies empty)
-- VIZ_CHART_2_FALLBACK_START
SELECT
    i.name                      AS assignee,
    COUNT(DISTINCT r.patent_id) AS patent_count
FROM inventors i
JOIN relationships r ON i.inventor_id = r.inventor_id
WHERE i.name IS NOT NULL
GROUP BY i.inventor_id, i.name
ORDER BY patent_count DESC;
-- VIZ_CHART_2_FALLBACK_END


-- Chart 3: Patent Quality — Abstract Length Distribution
-- VIZ_CHART_3_START
SELECT
    LENGTH(abstract) AS abstract_length,
    COUNT(*)         AS count
FROM patents
WHERE abstract IS NOT NULL
  AND abstract != ''
GROUP BY LENGTH(abstract)
ORDER BY abstract_length;
-- VIZ_CHART_3_END


-- Chart 4: Team Collaboration — Inventor Count per Patent
-- VIZ_CHART_4_START
SELECT
    patent_id,
    COUNT(DISTINCT inventor_id) AS inventor_count
FROM relationships
GROUP BY patent_id;
-- VIZ_CHART_4_END


-- Chart 5: Inventor Type — Corporate vs Individual
-- VIZ_CHART_5_START
SELECT
    CASE
        WHEN i.name ILIKE '%Inc%'
          OR i.name ILIKE '%LLC%'
          OR i.name ILIKE '%Corp%'
          OR i.name ILIKE '%Ltd%'
          OR i.name ILIKE '%Company%'
          OR i.name ILIKE '%Co.%'
          OR i.name ILIKE '%Technologies%'
          OR i.name ILIKE '%Systems%'
          OR i.name ILIKE '%Solutions%'
          THEN 'Corporate Entity'
        ELSE 'Individual'
    END                         AS inventor_type,
    COUNT(DISTINCT r.patent_id) AS patent_count
FROM inventors i
JOIN relationships r ON i.inventor_id = r.inventor_id
GROUP BY inventor_type;
-- VIZ_CHART_5_END


-- Chart 6: Impact Trends — Patents per Decade (using year column)
-- VIZ_CHART_6_START
SELECT
    (year / 10) * 10 AS decade,
    COUNT(*)         AS patent_count
FROM patents
WHERE year IS NOT NULL
GROUP BY (year / 10) * 10
ORDER BY decade;
-- VIZ_CHART_6_END