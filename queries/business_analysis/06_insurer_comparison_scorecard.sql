-- ============================================================
-- Query  : Insurer Comparison Scorecard
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : Aggregation, CASE grading, Multi-metric comparison
-- Purpose: Grades each insurer A-D based on approval rate and
--          average settlement speed. Covers payout ratio,
--          emergency %, readmission % and rejection rate.
-- ============================================================

SELECT pol.insurer,
    COUNT(*) AS total_claims,
    COUNT(DISTINCT fc.patient_id) AS unique_patients,
    SUM(fc.claim_amount) AS gross_claims_value,
    SUM(fc.approved_amount) AS total_paid_out,
    ROUND(SUM(fc.approved_amount) * 100.0 / NULLIF(SUM(fc.claim_amount), 0), 2) AS payout_ratio_pct,
    ROUND(AVG(fc.settlement_days)::NUMERIC, 1) AS avg_settlement_days,
    ROUND(SUM(CASE WHEN fc.status = 'Approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS approval_rate,
    ROUND(SUM(CASE WHEN fc.status = 'Rejected' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS rejection_rate,
    ROUND(SUM(fc.is_emergency) * 100.0 / COUNT(*), 2) AS emergency_claim_pct,
    ROUND(SUM(fc.is_readmission) * 100.0 / COUNT(*), 2) AS readmission_pct,
    CASE WHEN SUM(CASE WHEN fc.status='Approved' THEN 1 ELSE 0 END)*100.0/COUNT(*) > 60
              AND AVG(fc.settlement_days) < 30 THEN 'A - EXCELLENT'
         WHEN SUM(CASE WHEN fc.status='Approved' THEN 1 ELSE 0 END)*100.0/COUNT(*) > 50
              AND AVG(fc.settlement_days) < 45 THEN 'B - GOOD'
         WHEN SUM(CASE WHEN fc.status='Rejected' THEN 1 ELSE 0 END)*100.0/COUNT(*) > 20 THEN 'D - POOR'
         ELSE 'C - AVERAGE' END AS overall_grade
FROM fact_claims fc
JOIN dim_policies pol ON fc.policy_id = pol.policy_id
GROUP BY pol.insurer ORDER BY approval_rate DESC;
