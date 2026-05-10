-- Provider Fraud Risk Ranking (FIXED: now shows top 10 from each group)
WITH provider_metrics AS (
    SELECT
        p.provider_id,
        p.is_potentially_fraudulent,
        COUNT(*) AS total_claims,
        SUM(claim_amount_reimbursed) AS total_reimbursed,
        AVG(claim_amount_reimbursed) AS avg_claim_amount
    FROM (
        SELECT provider_id, claim_amount_reimbursed FROM fact_inpatient_claims
        UNION ALL
        SELECT provider_id, claim_amount_reimbursed FROM fact_outpatient_claims
    ) all_claims
    JOIN dim_provider p ON p.provider_id = all_claims.provider_id
    GROUP BY p.provider_id, p.is_potentially_fraudulent
),
ranked_metrics AS (
    SELECT
        provider_id,
        is_potentially_fraudulent,
        total_claims,
        ROUND(total_reimbursed::numeric, 2) AS total_reimbursed,
        ROUND(avg_claim_amount::numeric, 2) AS avg_claim_amount,
        RANK() OVER (
            PARTITION BY is_potentially_fraudulent
            ORDER BY total_reimbursed DESC
        ) AS rank_within_fraud_status,
        PERCENT_RANK() OVER (
            PARTITION BY is_potentially_fraudulent
            ORDER BY total_reimbursed
        )::numeric(5,4) AS percentile_within_status
    FROM provider_metrics
)
SELECT *
FROM ranked_metrics
WHERE rank_within_fraud_status <= 10  -- Top 10 within each group
ORDER BY is_potentially_fraudulent DESC, rank_within_fraud_status;
