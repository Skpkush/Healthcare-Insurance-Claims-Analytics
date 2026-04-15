-- ============================================================
-- Query  : Treatment Cost Benchmarking by Diagnosis
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : PERCENTILE_CONT, STDDEV, Statistical distribution
-- Purpose: Full cost distribution per diagnosis — avg, median,
--          p25/p75/p95, stddev, and average approval %.
--          Shows Cancer Treatment = 5% of claims, 16.8% of costs.
-- ============================================================

SELECT fc.diagnosis_code, fc.diagnosis_name,
    COUNT(*) AS total_cases,
    ROUND(AVG(fc.claim_amount)::NUMERIC) AS avg_cost,
    MIN(fc.claim_amount) AS min_cost,
    MAX(fc.claim_amount) AS max_cost,
    ROUND(STDDEV(fc.claim_amount)::NUMERIC) AS stddev_cost,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY fc.claim_amount)::NUMERIC) AS p25_cost,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY fc.claim_amount)::NUMERIC) AS median_cost,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY fc.claim_amount)::NUMERIC) AS p75_cost,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fc.claim_amount)::NUMERIC) AS p95_cost,
    ROUND(AVG(fc.approved_amount)::NUMERIC) AS avg_approved,
    ROUND(AVG(fc.approved_amount) * 100.0 / NULLIF(AVG(fc.claim_amount), 0), 2) AS avg_approval_pct,
    ROUND(AVG(fc.length_of_stay)::NUMERIC, 1) AS avg_los
FROM fact_claims fc
GROUP BY fc.diagnosis_code, fc.diagnosis_name ORDER BY total_cases DESC;
