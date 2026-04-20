-- ----------------------------------------------------------------------------
-- Q4: Trends Over Time — How many patents are created each year?
-- ----------------------------------------------------------------------------
SELECT
    year,
    COUNT(*) AS patent_count
FROM patents
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year ASC;


-- ----------------------------------------------------------------------------
-- Q5: JOIN Query — Combine patents with inventors and companies
-- ----------------------------------------------------------------------------
SELECT
    p.patent_id,
    p.title,
    p.filing_date,
    p.year,
    i.name  AS inventor_name,
    i.country AS inventor_country,
    c.name  AS company_name
FROM patents p
JOIN relationships  r ON p.patent_id   = r.patent_id
JOIN inventors      i ON r.inventor_id = i.inventor_id
LEFT JOIN companies c ON r.company_id  = c.company_id
ORDER BY p.year DESC, p.patent_id
LIMIT 100;


-- ----------------------------------------------------------------------------
-- Q6: CTE Query — Top companies per country (WITH statement)
-- ----------------------------------------------------------------------------
WITH company_country_counts AS (
    -- Step 1: count patents per company per country
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
    -- Step 2: rank companies within each country
    SELECT
        company_name,
        country,
        patent_count,
        RANK() OVER (PARTITION BY country ORDER BY patent_count DESC) AS rnk
    FROM company_country_counts
)
-- Step 3: keep only top 3 companies per country
SELECT
    country,
    company_name,
    patent_count,
    rnk
FROM ranked
WHERE rnk <= 3
ORDER BY country, rnk;
