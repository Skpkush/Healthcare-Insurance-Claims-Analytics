-- ============================================================
-- Query  : Multi-Level CTE — Hospital + Insurer Cross Performance
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : 3-level CTE chain, HAVING, Performance grading
-- Purpose: Scores each hospital-insurer pair on rejection rate,
--          approval value %, and settlement speed.
--          Only includes pairs with 10+ claims (statistically valid).
--          Final grade: EXCELLENT / AVERAGE / BELOW AVERAGE / POOR
-- ============================================================

WITH hospital_insurer_metrics AS (
    SELECT dp.provider_id, dp.hospital_name, dp.tier, pol.insurer,
        COUNT(*) AS total_claims,
        SUM(fc.claim_amount) AS total_claimed,
        SUM(fc.approved_amount) AS total_approved,
        SUM(CASE WHEN fc.status = 'Rejected' THEN 1 ELSE 0 END) AS rejected_count,
        AVG(fc.settlement_days) AS avg_settlement_days
    FROM fact_claims fc
    JOIN dim_providers dp ON fc.provider_id = dp.provider_id
    JOIN dim_policies pol ON fc.policy_id = pol.policy_id
    GROUP BY dp.provider_id, dp.hospital_name, dp.tier, pol.insurer
    HAVING COUNT(*) >= 10
),
performance_scored AS (
    SELECT *,
        ROUND(rejected_count * 100.0 / total_claims, 2) AS rejection_rate,
        ROUND(total_approved * 100.0 / NULLIF(total_claimed, 0), 2) AS approval_value_pct,
        ROUND(total_approved * 1.0 / total_claims) AS avg_payout_per_claim
    FROM hospital_insurer_metrics
),
final_ranking AS (
    SELECT *,
        CASE WHEN rejection_rate > 25 AND avg_settlement_days > 45 THEN 'POOR'
             WHEN rejection_rate > 15 OR avg_settlement_days > 30  THEN 'BELOW AVERAGE'
             WHEN rejection_rate <= 10 AND avg_settlement_days <= 15 THEN 'EXCELLENT'
             ELSE 'AVERAGE' END AS performance_grade
    FROM performance_scored
)
SELECT * FROM final_ranking ORDER BY rejection_rate DESC, avg_settlement_days DESC LIMIT 30;
