-- ============================================================
-- Query  : State-Level Healthcare Access & Cost Analysis
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : Multi-table JOIN, Aggregation, ROUND
-- Purpose: Compares claim volume, cost, approval rate, and
--          settlement speed across Indian states.
--          Key finding: Gujarat = highest claiming state at ₹140.99 Crore.
-- ============================================================

SELECT p.state,
    COUNT(DISTINCT p.patient_id) AS total_patients,
    COUNT(DISTINCT dp.provider_id) AS hospitals_used,
    COUNT(*) AS total_claims,
    SUM(fc.claim_amount) AS total_claimed,
    SUM(fc.approved_amount) AS total_approved,
    ROUND(AVG(fc.claim_amount)::NUMERIC) AS avg_claim_amount,
    ROUND(AVG(fc.settlement_days)::NUMERIC, 1) AS avg_settlement_days,
    ROUND(SUM(fc.is_emergency) * 100.0 / COUNT(*), 2) AS emergency_pct,
    ROUND(SUM(fc.is_readmission) * 100.0 / COUNT(*), 2) AS readmission_pct,
    ROUND(SUM(CASE WHEN fc.status = 'Approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS approval_rate_pct,
    ROUND(SUM(fc.claim_amount) * 1.0 / COUNT(DISTINCT p.patient_id)) AS claim_per_patient
FROM fact_claims fc
JOIN dim_patients p ON fc.patient_id = p.patient_id
JOIN dim_providers dp ON fc.provider_id = dp.provider_id
GROUP BY p.state ORDER BY total_claimed DESC;
