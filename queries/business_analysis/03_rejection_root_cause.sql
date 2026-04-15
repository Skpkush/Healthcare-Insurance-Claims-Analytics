-- ============================================================
-- Query  : Claim Rejection Root Cause Analysis
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : Aggregation, STRING_AGG, GROUP BY
-- Purpose: Breaks down rejected claims by insurer and actual
--          rejection_reason. Shows diagnoses and states affected.
--          Key finding: Incomplete Documents = largest recoverable bucket.
-- ============================================================

SELECT pol.insurer, fc.rejection_reason,
    COUNT(*) AS total_rejections,
    SUM(fc.claim_amount) AS total_value_rejected,
    ROUND(AVG(fc.claim_amount)::NUMERIC) AS avg_amount_per_rejection,
    STRING_AGG(DISTINCT fc.diagnosis_name, ', ' ORDER BY fc.diagnosis_name) AS diagnoses_affected,
    STRING_AGG(DISTINCT p.state, ', ' ORDER BY p.state) AS states_affected
FROM fact_claims fc
JOIN dim_policies pol ON fc.policy_id = pol.policy_id
JOIN dim_patients p ON fc.patient_id = p.patient_id
WHERE fc.status = 'Rejected'
GROUP BY pol.insurer, fc.rejection_reason
ORDER BY total_rejections DESC LIMIT 30;
