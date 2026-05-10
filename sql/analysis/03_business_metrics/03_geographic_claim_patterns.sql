-- Geographic Claim Patterns
-- Top US states by claim volume. Are some states over-represented in fraud-flagged claims?

WITH state_metrics AS (
    SELECT
        b.state_code,
        COUNT(DISTINCT b.beneficiary_id) AS patient_count,
        COUNT(*) AS claim_count,
        SUM(ac.claim_amount_reimbursed) AS total_billed,
        SUM(CASE WHEN p.is_potentially_fraudulent THEN ac.claim_amount_reimbursed ELSE 0 END) AS fraud_flagged_billed
    FROM dim_beneficiary b
    JOIN (
        SELECT beneficiary_id, provider_id, claim_amount_reimbursed FROM fact_inpatient_claims
        UNION ALL
        SELECT beneficiary_id, provider_id, claim_amount_reimbursed FROM fact_outpatient_claims
    ) ac ON ac.beneficiary_id = b.beneficiary_id
    JOIN dim_provider p ON p.provider_id = ac.provider_id
    WHERE b.state_code IS NOT NULL
    GROUP BY b.state_code
)
SELECT
    state_code,
    patient_count,
    claim_count,
    ROUND(total_billed::numeric, 2) AS total_billed,
    ROUND(fraud_flagged_billed::numeric, 2) AS fraud_billed,
    ROUND(100.0 * fraud_flagged_billed / NULLIF(total_billed, 0), 2) AS fraud_pct_of_billing
FROM state_metrics
ORDER BY total_billed DESC
LIMIT 15;
