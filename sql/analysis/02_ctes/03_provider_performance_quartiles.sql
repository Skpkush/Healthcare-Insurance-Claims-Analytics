-- Provider Performance Quartiles
-- Bucket providers into quartiles by efficiency (claims processed per patient)
-- Compare cost patterns across quartiles

WITH provider_efficiency AS (
    SELECT
        provider_id,
        COUNT(*) AS total_claims,
        COUNT(DISTINCT beneficiary_id) AS unique_patients,
        SUM(claim_amount_reimbursed) AS total_billed
    FROM (
        SELECT provider_id, beneficiary_id, claim_amount_reimbursed FROM fact_inpatient_claims
        UNION ALL
        SELECT provider_id, beneficiary_id, claim_amount_reimbursed FROM fact_outpatient_claims
    ) all_claims
    GROUP BY provider_id
),
quartiles AS (
    SELECT
        provider_id,
        total_claims,
        unique_patients,
        total_billed,
        NTILE(4) OVER (ORDER BY total_billed) AS billing_quartile
    FROM provider_efficiency
)
SELECT
    q.billing_quartile,
    COUNT(*) AS provider_count,
    SUM(CASE WHEN p.is_potentially_fraudulent THEN 1 ELSE 0 END) AS fraud_flagged_count,
    ROUND(100.0 * SUM(CASE WHEN p.is_potentially_fraudulent THEN 1 ELSE 0 END) / COUNT(*), 2) AS fraud_pct,
    ROUND(AVG(q.total_billed)::numeric, 2) AS avg_total_billed,
    ROUND(SUM(q.total_billed)::numeric, 2) AS quartile_total_billing
FROM quartiles q
JOIN dim_provider p ON p.provider_id = q.provider_id
GROUP BY q.billing_quartile
ORDER BY q.billing_quartile;
