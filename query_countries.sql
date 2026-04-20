-- ----------------------------------------------------------------------------
-- Q3: Countries — Which countries produce the most patents?
-- ----------------------------------------------------------------------------
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
