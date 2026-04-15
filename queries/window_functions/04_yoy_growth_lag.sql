-- ============================================================
-- Query  : Year-over-Year Claims Growth Using LAG()
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : Window Function — LAG() OVER (PARTITION BY insurer)
-- Purpose: Compares each insurer's yearly claim volume and value
--          to the prior year using LAG(). Calculates YoY growth %
--          for both claim count and total amount.
-- ============================================================

WITH yearly_metrics AS (
    SELECT pol.insurer, d.year,
        COUNT(*) AS total_claims,
        SUM(fc.claim_amount) AS total_amount,
        SUM(fc.approved_amount) AS total_paid,
        AVG(fc.settlement_days) AS avg_settlement_days
    FROM fact_claims fc
    JOIN dim_date d ON fc.date_key = d.date_key
    JOIN dim_policies pol ON fc.policy_id = pol.policy_id
    GROUP BY pol.insurer, d.year
)
SELECT insurer, year, total_claims, total_amount,
    LAG(total_claims) OVER (PARTITION BY insurer ORDER BY year) AS prev_year_claims,
    LAG(total_amount)  OVER (PARTITION BY insurer ORDER BY year) AS prev_year_amount,
    ROUND(
        (total_claims - LAG(total_claims) OVER (PARTITION BY insurer ORDER BY year)) * 100.0
        / NULLIF(LAG(total_claims) OVER (PARTITION BY insurer ORDER BY year), 0),
        2
    ) AS yoy_claims_growth_pct,
    ROUND(
        (total_amount - LAG(total_amount) OVER (PARTITION BY insurer ORDER BY year)) * 100.0
        / NULLIF(LAG(total_amount) OVER (PARTITION BY insurer ORDER BY year), 0),
        2
    ) AS yoy_amount_growth_pct
FROM yearly_metrics
ORDER BY insurer, year;
