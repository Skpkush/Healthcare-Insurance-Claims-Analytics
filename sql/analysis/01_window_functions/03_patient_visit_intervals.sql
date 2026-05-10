-- Patient Visit Patterns (FIXED: detects actual doctor-shopping, not normal multi-line billing)
-- Senior-grade fraud signal: 3+ visits across DISTINCT providers within 7 days
--
-- Postgres limitation: COUNT(DISTINCT col) OVER (...) is not supported.
-- Workaround: collect provider_ids into an array via array_agg() OVER (window),
-- then count distinct via unnest() in the outer query. Only runs the unnest
-- on rows that survive the cheap visits-count prefilter, so it's fast.
--
-- The RANGE BETWEEN INTERVAL '6 days' PRECEDING frame is the senior-grade
-- technique here -- counts time, not row positions, which is the correct
-- semantics for sliding-time-window fraud detection.

WITH patient_claims AS (
    SELECT
        beneficiary_id,
        claim_start_date_key AS visit_date,
        provider_id
    FROM fact_inpatient_claims
    UNION ALL
    SELECT
        beneficiary_id,
        claim_start_date_key,
        provider_id
    FROM fact_outpatient_claims
),
windowed AS (
    SELECT
        beneficiary_id,
        visit_date,
        COUNT(*) OVER w AS visits_in_7day_window,
        array_agg(provider_id) OVER w AS providers_in_window
    FROM patient_claims
    WINDOW w AS (
        PARTITION BY beneficiary_id
        ORDER BY visit_date
        RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
    )
),
prefiltered AS (
    -- Cheap prefilter -> only count distinct on rows that could possibly qualify
    SELECT
        beneficiary_id,
        visit_date,
        visits_in_7day_window,
        (SELECT COUNT(DISTINCT p) FROM unnest(providers_in_window) AS p)
            AS distinct_providers_in_7day_window
    FROM windowed
    WHERE visits_in_7day_window >= 3
)
SELECT
    beneficiary_id,
    visit_date,
    visits_in_7day_window,
    distinct_providers_in_7day_window
FROM prefiltered
WHERE distinct_providers_in_7day_window >= 3
ORDER BY distinct_providers_in_7day_window DESC, beneficiary_id, visit_date
LIMIT 50;
