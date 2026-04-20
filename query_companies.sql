-- ----------------------------------------------------------------------------
-- Q2: Top Companies — Which companies own the most patents?
-- ----------------------------------------------------------------------------
SELECT
    c.company_id,
    c.name,
    COUNT(DISTINCT r.patent_id) AS patent_count
FROM companies c
JOIN relationships r ON c.company_id = r.company_id
GROUP BY c.company_id, c.name
ORDER BY patent_count DESC
LIMIT 20;
