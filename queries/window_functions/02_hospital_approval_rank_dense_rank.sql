-- ============================================================
-- Query  : Rank Hospitals by Claim Approval Rate
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : Window Functions — DENSE_RANK, NTILE(4)
-- Purpose: Ranks all hospitals with 50+ claims by approval rate.
--          DENSE_RANK gives overall rank (no gaps).
--          NTILE(4) splits into performance quartiles (Q1=best).
-- ============================================================

WITH hospital_performance AS (
    SELECT
        dp.provider_id, dp.hospital_name, dp.tier, dp.accreditation,
        COUNT(*) AS total_claims,
        SUM(CASE WHEN fc.status = 'Approved' THEN 1 ELSE 0 END) AS approved_claims,
        ROUND(SUM(CASE WHEN fc.status = 'Approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS approval_rate_pct
    FROM fact_claims fc
    JOIN dim_providers dp ON fc.provider_id = dp.provider_id
    GROUP BY dp.provider_id, dp.hospital_name, dp.tier, dp.accreditation
    HAVING COUNT(*) >= 50
)
SELECT *,
    DENSE_RANK() OVER (ORDER BY approval_rate_pct DESC) AS approval_rank,
    NTILE(4) OVER (ORDER BY approval_rate_pct DESC) AS performance_quartile
FROM hospital_performance
ORDER BY approval_rank LIMIT 20;
