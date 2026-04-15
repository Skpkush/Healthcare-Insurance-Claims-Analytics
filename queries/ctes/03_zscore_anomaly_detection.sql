-- ============================================================
-- Query  : Claim Amount Anomaly Detection (Z-Score Method)
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : CTE, STDDEV, Z-Score statistical outlier detection
-- Purpose: Calculates Z-score per claim relative to its diagnosis
--          group average. Z > 3 = Extreme Outlier, Z > 2 = High Outlier.
--          Used to detect fraudulent or billing-error claims.
-- ============================================================

WITH diagnosis_stats AS (
    SELECT diagnosis_code, diagnosis_name,
        AVG(claim_amount) AS mean_amount,
        STDDEV(claim_amount) AS stddev_amount,
        COUNT(*) AS total_claims
    FROM fact_claims GROUP BY diagnosis_code, diagnosis_name
),
claim_zscore AS (
    SELECT fc.claim_id, fc.patient_id, fc.diagnosis_code, fc.diagnosis_name,
        fc.claim_amount, ds.mean_amount, ds.stddev_amount,
        ROUND(((fc.claim_amount - ds.mean_amount) / NULLIF(ds.stddev_amount, 0))::NUMERIC, 2) AS z_score
    FROM fact_claims fc
    JOIN diagnosis_stats ds ON fc.diagnosis_code = ds.diagnosis_code
)
SELECT claim_id, patient_id, diagnosis_name, claim_amount,
    ROUND(mean_amount::NUMERIC) AS diagnosis_avg, z_score,
    CASE WHEN z_score > 3  THEN 'EXTREME OUTLIER - Investigate'
         WHEN z_score > 2  THEN 'HIGH OUTLIER - Review'
         WHEN z_score < -2 THEN 'UNUSUALLY LOW - Verify'
         ELSE 'NORMAL' END AS anomaly_flag
FROM claim_zscore WHERE ABS(z_score) > 2
ORDER BY ABS(z_score) DESC LIMIT 50;
