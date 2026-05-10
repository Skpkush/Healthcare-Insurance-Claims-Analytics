-- Running Claim Totals by Provider (FIXED: deterministic provider selection)
WITH all_claims AS (
    SELECT provider_id, claim_start_date_key, claim_amount_reimbursed
    FROM fact_inpatient_claims
    UNION ALL
    SELECT provider_id, claim_start_date_key, claim_amount_reimbursed
    FROM fact_outpatient_claims
),
top_3_providers AS (
    -- Deterministic: top 3 by total claim count
    SELECT provider_id
    FROM all_claims
    GROUP BY provider_id
    ORDER BY COUNT(*) DESC
    LIMIT 3
),
daily_provider_claims AS (
    SELECT
        ac.provider_id,
        ac.claim_start_date_key AS claim_date,
        COUNT(*) AS claims_today,
        SUM(ac.claim_amount_reimbursed) AS dollars_today
    FROM all_claims ac
    INNER JOIN top_3_providers t ON t.provider_id = ac.provider_id
    GROUP BY ac.provider_id, ac.claim_start_date_key
)
SELECT
    provider_id,
    claim_date,
    claims_today,
    SUM(claims_today) OVER (
        PARTITION BY provider_id
        ORDER BY claim_date
        ROWS UNBOUNDED PRECEDING
    ) AS running_claim_count,
    ROUND(dollars_today::numeric, 2) AS dollars_today,
    ROUND(SUM(dollars_today) OVER (
        PARTITION BY provider_id
        ORDER BY claim_date
        ROWS UNBOUNDED PRECEDING
    )::numeric, 2) AS running_dollars,
    ROUND(AVG(claims_today) OVER (
        PARTITION BY provider_id
        ORDER BY claim_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )::numeric, 2) AS rolling_7day_avg_claims
FROM daily_provider_claims
ORDER BY provider_id, claim_date
LIMIT 50;
