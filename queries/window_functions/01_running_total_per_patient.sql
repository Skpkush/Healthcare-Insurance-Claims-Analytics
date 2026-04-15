-- ============================================================
-- Query  : Running Total of Claims Per Patient Over Time
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : Window Function — SUM OVER (ROWS UNBOUNDED PRECEDING)
-- Purpose: Calculates cumulative claim amount and count per patient
--          ordered chronologically. Useful for tracking patient
--          spend trajectory over their claim history.
-- ============================================================

SELECT
    fc.claim_id, fc.patient_id, d.full_date, fc.claim_amount,
    SUM(fc.claim_amount) OVER (
        PARTITION BY fc.patient_id ORDER BY d.full_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_claims,
    COUNT(*) OVER (PARTITION BY fc.patient_id ORDER BY d.full_date) AS cumulative_claim_count
FROM fact_claims fc
JOIN dim_date d ON fc.date_key = d.date_key
WHERE fc.patient_id IN ('P00001', 'P00002', 'P00003')
ORDER BY fc.patient_id, d.full_date;
