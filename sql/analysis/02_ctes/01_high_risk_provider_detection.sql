-- High-Risk Provider Detection
-- Multi-step CTE combining 3 risk signals into a composite score

WITH provider_volume AS (
    SELECT
        provider_id,
        COUNT(*) AS total_claims,
        COUNT(DISTINCT beneficiary_id) AS unique_patients,
        SUM(claim_amount_reimbursed) AS total_reimbursed,
        AVG(claim_amount_reimbursed) AS avg_claim
    FROM (
        SELECT provider_id, beneficiary_id, claim_amount_reimbursed
        FROM fact_inpatient_claims
        UNION ALL
        SELECT provider_id, beneficiary_id, claim_amount_reimbursed
        FROM fact_outpatient_claims
    ) all_claims
    GROUP BY provider_id
),
provider_percentiles AS (
    SELECT
        provider_id,
        total_claims,
        unique_patients,
        total_reimbursed,
        avg_claim,
        PERCENT_RANK() OVER (ORDER BY total_claims) AS pct_volume,
        PERCENT_RANK() OVER (ORDER BY avg_claim) AS pct_avg_amount,
        PERCENT_RANK() OVER (ORDER BY total_claims::float / NULLIF(unique_patients, 0)) AS pct_claims_per_patient
    FROM provider_volume
),
risk_scored AS (
    SELECT
        pp.*,
        p.is_potentially_fraudulent,
        ROUND((pct_volume * 0.4 + pct_avg_amount * 0.3 + pct_claims_per_patient * 0.3)::numeric, 4) AS composite_risk_score
    FROM provider_percentiles pp
    JOIN dim_provider p ON p.provider_id = pp.provider_id
)
SELECT
    provider_id,
    is_potentially_fraudulent,
    total_claims,
    unique_patients,
    ROUND((total_claims::float / NULLIF(unique_patients, 0))::numeric, 2) AS claims_per_patient,
    ROUND(avg_claim::numeric, 2) AS avg_claim,
    composite_risk_score
FROM risk_scored
-- Top 20 by composite score. Threshold-based filtering removed:
-- composite_risk_score is a weighted sum of three PERCENT_RANKs, not a percentile,
-- so values of 0.95+ require all three signals to be near-top simultaneously
-- (which they almost never are -- volume and avg-claim are anti-correlated).
ORDER BY composite_risk_score DESC
LIMIT 20;
