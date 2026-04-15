-- ============================================================
-- Query  : Percentile Ranking of Settlement Days by Insurer
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : Window Function — PERCENT_RANK() OVER (PARTITION BY)
-- Purpose: Ranks each claim's settlement speed as a percentile
--          within its insurer. Shows min/max/avg settlement days
--          per insurer for benchmarking comparison.
-- ============================================================

SELECT pol.insurer, fc.claim_id, fc.settlement_days,
    ROUND(PERCENT_RANK() OVER (
        PARTITION BY pol.insurer ORDER BY fc.settlement_days
    )::NUMERIC, 4) AS percentile_rank,
    ROUND(AVG(fc.settlement_days) OVER (PARTITION BY pol.insurer)::NUMERIC, 1) AS insurer_avg_days,
    MIN(fc.settlement_days) OVER (PARTITION BY pol.insurer) AS insurer_min_days,
    MAX(fc.settlement_days) OVER (PARTITION BY pol.insurer) AS insurer_max_days
FROM fact_claims fc
JOIN dim_policies pol ON fc.policy_id = pol.policy_id
WHERE fc.settlement_days IS NOT NULL
ORDER BY pol.insurer, fc.settlement_days;
