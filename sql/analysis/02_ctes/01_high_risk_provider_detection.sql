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
),
percentile_filtered AS (
    -- Rank what's already ranked: PERCENT_RANK over the composite score itself.
    -- Necessary because composite_risk_score is a weighted sum of three percentiles,
    -- not a percentile -- its distribution caps below 1.0 (the three signals are
    -- anti-correlated, so no provider scores top across all three at once).
    SELECT *,
        PERCENT_RANK() OVER (ORDER BY composite_risk_score) AS composite_percentile
    FROM risk_scored
)
SELECT
    provider_id,
    is_potentially_fraudulent,
    total_claims,
    unique_patients,
    ROUND((total_claims::float / NULLIF(unique_patients, 0))::numeric, 2) AS claims_per_patient,
    ROUND(avg_claim::numeric, 2) AS avg_claim,
    composite_risk_score,
    ROUND(composite_percentile::numeric, 4) AS composite_percentile
FROM percentile_filtered
WHERE composite_percentile >= 0.95   -- Now correctly = top 5% of composite scores
ORDER BY composite_risk_score DESC
LIMIT 20;
