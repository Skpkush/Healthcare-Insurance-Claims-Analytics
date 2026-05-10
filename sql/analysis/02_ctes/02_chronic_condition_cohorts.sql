-- Chronic Condition Cohort Analysis
-- Multi-step CTE: count conditions per patient, bucket into cohorts, aggregate

WITH patient_conditions AS (
    SELECT
        beneficiary_id,
        (CASE WHEN chronic_alzheimer THEN 1 ELSE 0 END +
         CASE WHEN chronic_heart_failure THEN 1 ELSE 0 END +
         CASE WHEN chronic_kidney_disease THEN 1 ELSE 0 END +
         CASE WHEN chronic_cancer THEN 1 ELSE 0 END +
         CASE WHEN chronic_obstr_pulmonary THEN 1 ELSE 0 END +
         CASE WHEN chronic_depression THEN 1 ELSE 0 END +
         CASE WHEN chronic_diabetes THEN 1 ELSE 0 END +
         CASE WHEN chronic_ischemic_heart THEN 1 ELSE 0 END +
         CASE WHEN chronic_osteoporosis THEN 1 ELSE 0 END +
         CASE WHEN chronic_rheumatoid_arthritis THEN 1 ELSE 0 END +
         CASE WHEN chronic_stroke THEN 1 ELSE 0 END
        ) AS condition_count
    FROM dim_beneficiary
),
cohorts AS (
    SELECT
        beneficiary_id,
        condition_count,
        CASE
            WHEN condition_count = 0 THEN 'Healthy (0)'
            WHEN condition_count BETWEEN 1 AND 2 THEN 'Mild (1-2)'
            WHEN condition_count BETWEEN 3 AND 5 THEN 'Moderate (3-5)'
            WHEN condition_count BETWEEN 6 AND 8 THEN 'High (6-8)'
            ELSE 'Very High (9-11)'
        END AS comorbidity_cohort
    FROM patient_conditions
),
patient_costs AS (
    SELECT
        beneficiary_id,
        SUM(claim_amount_reimbursed) AS total_claim_cost,
        COUNT(*) AS total_claims
    FROM (
        SELECT beneficiary_id, claim_amount_reimbursed FROM fact_inpatient_claims
        UNION ALL
        SELECT beneficiary_id, claim_amount_reimbursed FROM fact_outpatient_claims
    ) all_claims
    GROUP BY beneficiary_id
)
SELECT
    c.comorbidity_cohort,
    COUNT(*) AS patient_count,
    ROUND(AVG(pc.total_claims)::numeric, 1) AS avg_claims_per_patient,
    ROUND(AVG(pc.total_claim_cost)::numeric, 2) AS avg_total_cost_per_patient,
    ROUND(SUM(pc.total_claim_cost)::numeric, 2) AS cohort_total_spend
FROM cohorts c
LEFT JOIN patient_costs pc ON pc.beneficiary_id = c.beneficiary_id
GROUP BY c.comorbidity_cohort
ORDER BY
    CASE c.comorbidity_cohort
        WHEN 'Healthy (0)' THEN 1
        WHEN 'Mild (1-2)' THEN 2
        WHEN 'Moderate (3-5)' THEN 3
        WHEN 'High (6-8)' THEN 4
        ELSE 5
    END;
