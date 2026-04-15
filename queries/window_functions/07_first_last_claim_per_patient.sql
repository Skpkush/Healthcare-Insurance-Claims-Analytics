-- ============================================================
-- Query  : First & Last Claim Per Patient
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : Window Functions — FIRST_VALUE, LAST_VALUE, WINDOW alias
-- Purpose: Retrieves each patient's very first and most recent claim
--          in a single query using FIRST_VALUE / LAST_VALUE with
--          explicit ROWS BETWEEN frame. Shows claim journey start
--          to current state with total claim count.
-- ============================================================

SELECT DISTINCT fc.patient_id,
    FIRST_VALUE(fc.claim_id) OVER w AS first_claim_id,
    FIRST_VALUE(d.full_date) OVER w AS first_claim_date,
    FIRST_VALUE(fc.diagnosis_name) OVER w AS first_diagnosis,
    FIRST_VALUE(fc.claim_amount) OVER w AS first_claim_amount,
    LAST_VALUE(fc.claim_id) OVER (
        PARTITION BY fc.patient_id ORDER BY d.full_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS latest_claim_id,
    LAST_VALUE(d.full_date) OVER (
        PARTITION BY fc.patient_id ORDER BY d.full_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS latest_claim_date,
    COUNT(*) OVER (PARTITION BY fc.patient_id) AS total_claims
FROM fact_claims fc
JOIN dim_date d ON fc.date_key = d.date_key
WINDOW w AS (PARTITION BY fc.patient_id ORDER BY d.full_date)
ORDER BY total_claims DESC LIMIT 50;
