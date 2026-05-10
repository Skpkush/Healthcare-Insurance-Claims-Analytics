-- Amount Outliers vs Provider Median
-- Claims that are 5x the provider's typical amount -- fraud signal

WITH provider_baselines AS (
    SELECT
        provider_id,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY claim_amount_reimbursed) AS median_claim
    FROM (
        SELECT provider_id, claim_amount_reimbursed FROM fact_inpatient_claims
        UNION ALL
        SELECT provider_id, claim_amount_reimbursed FROM fact_outpatient_claims
    ) all_claims
    GROUP BY provider_id
),
flagged_claims AS (
    SELECT
        ac.provider_id,
        ac.claim_id,
        ac.beneficiary_id,
        ac.claim_amount_reimbursed,
        pb.median_claim,
        ROUND((ac.claim_amount_reimbursed::float / NULLIF(pb.median_claim, 0))::numeric, 2) AS amount_ratio_vs_median
    FROM (
        SELECT provider_id, claim_id, beneficiary_id, claim_amount_reimbursed FROM fact_inpatient_claims
        UNION ALL
        SELECT provider_id, claim_id, beneficiary_id, claim_amount_reimbursed FROM fact_outpatient_claims
    ) ac
    JOIN provider_baselines pb ON pb.provider_id = ac.provider_id
    WHERE pb.median_claim > 0
      AND ac.claim_amount_reimbursed > 5 * pb.median_claim
      AND ac.claim_amount_reimbursed > 1000
)
SELECT
    fc.provider_id,
    p.is_potentially_fraudulent,
    fc.claim_id,
    ROUND(fc.claim_amount_reimbursed::numeric, 2) AS claim_amount,
    ROUND(fc.median_claim::numeric, 2) AS provider_median,
    fc.amount_ratio_vs_median
FROM flagged_claims fc
JOIN dim_provider p ON p.provider_id = fc.provider_id
ORDER BY fc.amount_ratio_vs_median DESC
LIMIT 25;
