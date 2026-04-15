-- ============================================================
-- Query  : Cohort Analysis — Patient Retention by Signup Quarter
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : Multi-level CTE, EXTRACT, Date cohort grouping
-- Purpose: Groups patients by registration quarter and tracks
--          their claim activity over time. Shows revenue per
--          active patient per cohort — useful for retention analysis.
-- ============================================================

WITH patient_cohorts AS (
    SELECT p.patient_id,
        EXTRACT(YEAR FROM p.registration_date)::TEXT || '-Q' ||
        EXTRACT(QUARTER FROM p.registration_date)::TEXT AS signup_cohort
    FROM dim_patients p
),
cohort_activity AS (
    SELECT pc.signup_cohort, d.year AS activity_year, d.quarter AS activity_quarter,
        COUNT(DISTINCT pc.patient_id) AS active_patients,
        COUNT(*) AS total_claims,
        SUM(fc.claim_amount) AS total_claimed,
        AVG(fc.claim_amount) AS avg_claim_amount
    FROM patient_cohorts pc
    JOIN fact_claims fc ON pc.patient_id = fc.patient_id
    JOIN dim_date d ON fc.date_key = d.date_key
    GROUP BY pc.signup_cohort, d.year, d.quarter
)
SELECT signup_cohort, activity_year, activity_quarter, active_patients, total_claims,
    total_claimed, ROUND(avg_claim_amount) AS avg_claim,
    ROUND(total_claimed * 1.0 / active_patients) AS revenue_per_active_patient
FROM cohort_activity ORDER BY signup_cohort, activity_year, activity_quarter;
