-- ============================================================
-- Query  : Monthly Trend Dashboard Query (Power BI Source)
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : Multi-table JOIN, GROUP BY, Aggregation
-- Purpose: Granular monthly summary across insurer, hospital tier,
--          treatment type, diagnosis, and state. Used as the
--          base query feeding the Power BI trend visuals.
-- ============================================================

SELECT d.year, d.month, d.month_name, pol.insurer, dp.tier AS hospital_tier,
    fc.treatment_type, fc.diagnosis_name, p.state,
    COUNT(*) AS claim_count,
    COUNT(DISTINCT fc.patient_id) AS unique_patients,
    SUM(fc.claim_amount) AS gross_claims,
    SUM(fc.approved_amount) AS net_paid,
    AVG(fc.claim_amount) AS avg_claim,
    SUM(CASE WHEN fc.status = 'Approved' THEN 1 ELSE 0 END) AS approved_count,
    SUM(CASE WHEN fc.status = 'Rejected' THEN 1 ELSE 0 END) AS rejected_count,
    AVG(fc.settlement_days) AS avg_settlement_days,
    AVG(fc.length_of_stay) AS avg_los,
    SUM(fc.is_emergency) AS emergency_count,
    SUM(fc.is_readmission) AS readmission_count
FROM fact_claims fc
JOIN dim_date d ON fc.date_key = d.date_key
JOIN dim_policies pol ON fc.policy_id = pol.policy_id
JOIN dim_providers dp ON fc.provider_id = dp.provider_id
JOIN dim_patients p ON fc.patient_id = p.patient_id
GROUP BY d.year, d.month, d.month_name, pol.insurer, dp.tier,
    fc.treatment_type, fc.diagnosis_name, p.state
ORDER BY d.year, d.month;
