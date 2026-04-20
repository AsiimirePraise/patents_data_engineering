-- ----------------------------------------------------------------------------
-- Q1: Top Inventors — Who has the most patents?
-- ----------------------------------------------------------------------------
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


-- ----------------------------------------------------------------------------
-- Q7: Ranking Query — Rank inventors using window functions
-- ----------------------------------------------------------------------------
SELECT
    inventor_id,
    name,
    country,
    patent_count,
    RANK() OVER (ORDER BY patent_count DESC)                      AS global_rank,
    RANK() OVER (PARTITION BY country ORDER BY patent_count DESC) AS country_rank,
    ROUND(
        100.0 * patent_count / SUM(patent_count) OVER (),
        4
    )                                                             AS pct_of_total
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
