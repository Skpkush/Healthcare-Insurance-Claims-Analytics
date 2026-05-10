-- Velocity Outliers
-- Providers submitting >10 claims/day are statistical outliers
-- This is a classic fraud signal

WITH daily_provider_volume AS (
    SELECT
        provider_id,
        claim_start_date_key AS claim_date,
        COUNT(*) AS claims_that_day
    FROM (
        SELECT provider_id, claim_start_date_key FROM fact_inpatient_claims
        UNION ALL
        SELECT provider_id, claim_start_date_key FROM fact_outpatient_claims
    ) all_claims
    GROUP BY provider_id, claim_start_date_key
),
provider_velocity AS (
    SELECT
        provider_id,
        MAX(claims_that_day) AS peak_daily_claims,
        ROUND(AVG(claims_that_day)::numeric, 2) AS avg_daily_claims,
        COUNT(DISTINCT claim_date) AS active_days
    FROM daily_provider_volume
    GROUP BY provider_id
)
SELECT
    pv.provider_id,
    p.is_potentially_fraudulent,
    pv.peak_daily_claims,
    pv.avg_daily_claims,
    pv.active_days
FROM provider_velocity pv
JOIN dim_provider p ON p.provider_id = pv.provider_id
WHERE pv.peak_daily_claims > 10
ORDER BY pv.peak_daily_claims DESC
LIMIT 25;
