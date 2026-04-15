-- ============================================================
-- Query  : Fraud Detection — Patients with 3+ Claims in 90 Days
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : CTE, Window Function — RANGE BETWEEN INTERVAL
-- Purpose: Flags patients with 3+ claims in any rolling 90-day
--          window as potential fraud or over-utilisation cases.
--          HIGH RISK = 5+ claims | MEDIUM RISK = 3-4 claims
-- ============================================================

WITH claim_timeline AS (
    SELECT fc.patient_id, fc.claim_id, d.full_date AS claim_date,
        fc.claim_amount, fc.diagnosis_name, fc.provider_id,
        COUNT(*) OVER (
            PARTITION BY fc.patient_id ORDER BY d.full_date
            RANGE BETWEEN INTERVAL '90 days' PRECEDING AND CURRENT ROW
        ) AS claims_in_90_days
    FROM fact_claims fc
    JOIN dim_date d ON fc.date_key = d.date_key
),
fraud_flags AS (
    SELECT *,
        CASE WHEN claims_in_90_days >= 5 THEN 'HIGH RISK'
             WHEN claims_in_90_days >= 3 THEN 'MEDIUM RISK'
             ELSE 'LOW RISK' END AS fraud_risk_level
    FROM claim_timeline WHERE claims_in_90_days >= 3
)
SELECT ff.*, p.age, p.city, p.chronic_condition, dp.hospital_name
FROM fraud_flags ff
JOIN dim_patients p ON ff.patient_id = p.patient_id
JOIN dim_providers dp ON ff.provider_id = dp.provider_id
ORDER BY claims_in_90_days DESC, claim_amount DESC LIMIT 100;
