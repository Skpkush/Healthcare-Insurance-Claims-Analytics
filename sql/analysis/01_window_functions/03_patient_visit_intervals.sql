-- Patient Visit Intervals
-- Window function: LAG() to access previous row's date
-- Identifies suspicious visit frequency patterns

WITH patient_claims AS (
    SELECT
        beneficiary_id,
        claim_start_date_key AS visit_date,
        claim_amount_reimbursed
    FROM (
        SELECT beneficiary_id, claim_start_date_key, claim_amount_reimbursed
        FROM fact_inpatient_claims
        UNION ALL
        SELECT beneficiary_id, claim_start_date_key, claim_amount_reimbursed
        FROM fact_outpatient_claims
    ) all_claims
),
visits_with_lag AS (
    SELECT
        beneficiary_id,
        visit_date,
        LAG(visit_date) OVER (
            PARTITION BY beneficiary_id
            ORDER BY visit_date
        ) AS previous_visit_date,
        claim_amount_reimbursed
    FROM patient_claims
)
SELECT
    beneficiary_id,
    visit_date,
    previous_visit_date,
    visit_date - previous_visit_date AS days_since_last_visit,
    ROUND(claim_amount_reimbursed::numeric, 2) AS claim_amount
FROM visits_with_lag
WHERE previous_visit_date IS NOT NULL
  AND visit_date - previous_visit_date <= 1  -- same day or next day visits
ORDER BY days_since_last_visit, beneficiary_id
LIMIT 50;
