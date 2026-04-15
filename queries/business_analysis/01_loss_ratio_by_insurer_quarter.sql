-- ============================================================
-- Query  : Loss Ratio Analysis by Insurer & Quarter
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : CTE, Aggregation, CASE classification
-- Purpose: Measures insurer profitability per quarter.
--          Loss Ratio = Net Claims Paid / Estimated Premium
--          < 60% = Profitable | 60-80% = Moderate | >100% = Loss Making
-- ============================================================

WITH quarterly_claims AS (
    SELECT pol.insurer, d.year, d.quarter,
        COUNT(DISTINCT fc.patient_id) AS unique_claimants,
        COUNT(*) AS total_claims,
        SUM(fc.claim_amount) AS gross_claims,
        SUM(fc.approved_amount) AS net_claims_paid,
        SUM(CASE WHEN fc.status = 'Rejected' THEN 1 ELSE 0 END) AS rejected_claims
    FROM fact_claims fc
    JOIN dim_date d ON fc.date_key = d.date_key
    JOIN dim_policies pol ON fc.policy_id = pol.policy_id
    GROUP BY pol.insurer, d.year, d.quarter
),
premium_estimates AS (
    SELECT insurer, SUM(annual_premium) * 50 / 4 AS estimated_quarterly_premium
    FROM dim_policies GROUP BY insurer
)
SELECT qc.insurer, qc.year, qc.quarter, qc.total_claims,
    qc.gross_claims, qc.net_claims_paid, pe.estimated_quarterly_premium,
    ROUND(qc.net_claims_paid * 100.0 / NULLIF(pe.estimated_quarterly_premium, 0), 2) AS loss_ratio_pct,
    CASE WHEN qc.net_claims_paid * 100.0 / NULLIF(pe.estimated_quarterly_premium, 0) > 100 THEN 'LOSS MAKING'
         WHEN qc.net_claims_paid * 100.0 / NULLIF(pe.estimated_quarterly_premium, 0) > 80  THEN 'HIGH RISK'
         WHEN qc.net_claims_paid * 100.0 / NULLIF(pe.estimated_quarterly_premium, 0) > 60  THEN 'MODERATE'
         ELSE 'PROFITABLE' END AS profitability_status,
    ROUND(qc.rejected_claims * 100.0 / qc.total_claims, 2) AS rejection_rate_pct
FROM quarterly_claims qc
JOIN premium_estimates pe ON qc.insurer = pe.insurer
ORDER BY qc.insurer, qc.year, qc.quarter;
