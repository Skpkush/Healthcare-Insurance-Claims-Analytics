-- Running Claim Totals by Provider
-- Window function: SUM() OVER ORDER BY date with ROWS UNBOUNDED PRECEDING
-- Identifies providers with sudden claim volume spikes

WITH daily_provider_claims AS (
    SELECT
        provider_id,
        claim_start_date_key AS claim_date,
        COUNT(*) AS claims_today,
        SUM(claim_amount_reimbursed) AS dollars_today
    FROM (
        SELECT provider_id, claim_start_date_key, claim_amount_reimbursed
        FROM fact_inpatient_claims
        UNION ALL
        SELECT provider_id, claim_start_date_key, claim_amount_reimbursed
        FROM fact_outpatient_claims
    ) all_claims
    GROUP BY provider_id, claim_start_date_key
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
WHERE provider_id IN (
    -- Sample: pick 3 providers — one fraud-flagged, two not
    -- (each arm wrapped so its ORDER BY/LIMIT applies before UNION)
    (SELECT provider_id FROM dim_provider
     WHERE is_potentially_fraudulent = TRUE
     ORDER BY RANDOM() LIMIT 1)
    UNION
    (SELECT provider_id FROM dim_provider
     WHERE is_potentially_fraudulent = FALSE
     ORDER BY RANDOM() LIMIT 2)
)
ORDER BY provider_id, claim_date
LIMIT 50;
