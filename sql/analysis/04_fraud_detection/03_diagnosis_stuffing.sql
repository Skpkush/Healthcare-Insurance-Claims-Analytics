-- Diagnosis Code Stuffing
-- Claims with all 10 diagnosis slots filled may indicate "stuffing" to maximize reimbursement
-- Compare diagnosis density between fraud-flagged and non-flagged providers

WITH claim_diagnosis_counts AS (
    SELECT
        bcd.claim_id,
        COUNT(*) AS diagnosis_count
    FROM bridge_claim_diagnosis bcd
    GROUP BY bcd.claim_id
),
claim_with_provider AS (
    SELECT
        c.claim_id,
        c.diagnosis_count,
        COALESCE(fi.provider_id, fo.provider_id) AS provider_id
    FROM claim_diagnosis_counts c
    LEFT JOIN fact_inpatient_claims fi ON fi.claim_id = c.claim_id
    LEFT JOIN fact_outpatient_claims fo ON fo.claim_id = c.claim_id
)
SELECT
    p.is_potentially_fraudulent,
    cwp.diagnosis_count,
    COUNT(*) AS claim_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY p.is_potentially_fraudulent), 2) AS pct_of_fraud_status
FROM claim_with_provider cwp
JOIN dim_provider p ON p.provider_id = cwp.provider_id
GROUP BY p.is_potentially_fraudulent, cwp.diagnosis_count
ORDER BY p.is_potentially_fraudulent DESC, cwp.diagnosis_count;
