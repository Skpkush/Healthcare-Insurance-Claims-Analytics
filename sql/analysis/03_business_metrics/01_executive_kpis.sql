-- Executive KPI Dashboard
-- The numbers a CFO or risk officer would ask for first

WITH all_claims AS (
    SELECT provider_id, beneficiary_id, claim_amount_reimbursed, claim_start_date_key,
           'inpatient' AS claim_type FROM fact_inpatient_claims
    UNION ALL
    SELECT provider_id, beneficiary_id, claim_amount_reimbursed, claim_start_date_key,
           'outpatient' AS claim_type FROM fact_outpatient_claims
)
SELECT
    'Total claims' AS metric, COUNT(*)::text AS value FROM all_claims
UNION ALL SELECT 'Total reimbursed ($)', '$' || ROUND(SUM(claim_amount_reimbursed)::numeric, 0)::text FROM all_claims
UNION ALL SELECT 'Avg claim ($)', '$' || ROUND(AVG(claim_amount_reimbursed)::numeric, 2)::text FROM all_claims
UNION ALL SELECT 'Median inpatient ($)', '$' || PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY claim_amount_reimbursed)::text
                FROM all_claims WHERE claim_type = 'inpatient'
UNION ALL SELECT 'Median outpatient ($)', '$' || PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY claim_amount_reimbursed)::text
                FROM all_claims WHERE claim_type = 'outpatient'
UNION ALL SELECT 'Unique patients', COUNT(DISTINCT beneficiary_id)::text FROM all_claims
UNION ALL SELECT 'Unique providers', COUNT(DISTINCT provider_id)::text FROM all_claims
UNION ALL SELECT 'Fraud-flagged providers', SUM(CASE WHEN is_potentially_fraudulent THEN 1 ELSE 0 END)::text FROM dim_provider
UNION ALL SELECT 'Provider fraud rate (%)',
                ROUND(100.0 * SUM(CASE WHEN is_potentially_fraudulent THEN 1 ELSE 0 END) / COUNT(*), 2)::text || '%'
                FROM dim_provider
UNION ALL SELECT '$ from fraud-flagged providers',
                '$' || ROUND(SUM(ac.claim_amount_reimbursed)::numeric, 0)::text
                FROM all_claims ac JOIN dim_provider p ON p.provider_id = ac.provider_id
                WHERE p.is_potentially_fraudulent = TRUE;
