-- ============================================================
-- Query  : 3-Month Moving Average of Claim Amounts by Insurer
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : Window Function — AVG OVER (ROWS BETWEEN 2 PRECEDING)
-- Purpose: Smooths monthly claim volatility using a 3-month
--          rolling average per insurer. Useful for trend analysis
--          and identifying genuine growth vs. seasonal spikes.
-- ============================================================

WITH monthly_claims AS (
    SELECT pol.insurer, d.year, d.month,
        SUM(fc.claim_amount) AS total_claim_amount,
        COUNT(*) AS claim_count,
        AVG(fc.claim_amount) AS avg_claim_amount
    FROM fact_claims fc
    JOIN dim_date d ON fc.date_key = d.date_key
    JOIN dim_policies pol ON fc.policy_id = pol.policy_id
    GROUP BY pol.insurer, d.year, d.month
)
SELECT insurer, year, month, total_claim_amount,
    ROUND(AVG(total_claim_amount) OVER (
        PARTITION BY insurer ORDER BY year, month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )) AS moving_avg_3month,
    ROUND(AVG(avg_claim_amount) OVER (
        PARTITION BY insurer ORDER BY year, month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )) AS moving_avg_per_claim
FROM monthly_claims
ORDER BY insurer, year, month;
