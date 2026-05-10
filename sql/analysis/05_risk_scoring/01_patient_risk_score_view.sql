-- ============================================================
-- File: 01_patient_risk_score_view.sql
-- Purpose: Materialized view computing 5-factor patient risk score
-- Created Day 6, validated in Hour 3
-- ============================================================

-- Drop if exists for idempotency
DROP MATERIALIZED VIEW IF EXISTS mv_patient_risk_score;

CREATE MATERIALIZED VIEW mv_patient_risk_score AS
WITH patient_claims_summary AS (
    -- Step 1: Aggregate claim activity per patient
    SELECT
        beneficiary_id,
        COUNT(*) AS claim_count,
        SUM(claim_amount_reimbursed) AS total_claim_amount
    FROM (
        SELECT beneficiary_id, claim_amount_reimbursed FROM fact_inpatient_claims
        UNION ALL
        SELECT beneficiary_id, claim_amount_reimbursed FROM fact_outpatient_claims
    ) all_claims
    GROUP BY beneficiary_id
),
patient_features AS (
    -- Step 2: Combine demographics + chronic count + claim activity
    SELECT
        b.beneficiary_id,
        -- Age in years (calculated against latest data date -- 2010-12-31 used as anchor)
        EXTRACT(YEAR FROM AGE(DATE '2010-12-31', b.date_of_birth)) AS age_years,
        -- Chronic condition count (0 to 11)
        (CASE WHEN b.chronic_alzheimer THEN 1 ELSE 0 END +
         CASE WHEN b.chronic_heart_failure THEN 1 ELSE 0 END +
         CASE WHEN b.chronic_kidney_disease THEN 1 ELSE 0 END +
         CASE WHEN b.chronic_cancer THEN 1 ELSE 0 END +
         CASE WHEN b.chronic_obstr_pulmonary THEN 1 ELSE 0 END +
         CASE WHEN b.chronic_depression THEN 1 ELSE 0 END +
         CASE WHEN b.chronic_diabetes THEN 1 ELSE 0 END +
         CASE WHEN b.chronic_ischemic_heart THEN 1 ELSE 0 END +
         CASE WHEN b.chronic_osteoporosis THEN 1 ELSE 0 END +
         CASE WHEN b.chronic_rheumatoid_arthritis THEN 1 ELSE 0 END +
         CASE WHEN b.chronic_stroke THEN 1 ELSE 0 END
        ) AS chronic_count,
        b.has_renal_disease,
        COALESCE(pcs.claim_count, 0) AS claim_count,
        COALESCE(pcs.total_claim_amount, 0) AS total_claim_amount
    FROM dim_beneficiary b
    LEFT JOIN patient_claims_summary pcs ON pcs.beneficiary_id = b.beneficiary_id
),
normalized_features AS (
    -- Step 3: Normalize each feature to [0,1] using percentile rank
    -- This avoids being skewed by outliers (claim amount has 99th percentile cliff)
    SELECT
        beneficiary_id,
        age_years,
        chronic_count,
        has_renal_disease,
        claim_count,
        total_claim_amount,
        -- Percentile-based normalization (more robust than min-max)
        PERCENT_RANK() OVER (ORDER BY age_years) AS norm_age,
        PERCENT_RANK() OVER (ORDER BY chronic_count) AS norm_chronic,
        PERCENT_RANK() OVER (ORDER BY claim_count) AS norm_frequency,
        PERCENT_RANK() OVER (ORDER BY total_claim_amount) AS norm_amount,
        CASE WHEN has_renal_disease THEN 1.0 ELSE 0.0 END AS norm_renal
    FROM patient_features
),
scored AS (
    -- Step 4: Apply weights and compute composite score
    SELECT
        beneficiary_id,
        age_years,
        chronic_count,
        has_renal_disease,
        claim_count,
        total_claim_amount,
        ROUND((norm_age      * 0.35
             + norm_chronic   * 0.25
             + norm_frequency * 0.20
             + norm_amount    * 0.15
             + norm_renal     * 0.05)::numeric, 4) AS risk_score
    FROM normalized_features
)
-- Step 5: Categorize into risk tiers
SELECT
    beneficiary_id,
    age_years,
    chronic_count,
    has_renal_disease,
    claim_count,
    total_claim_amount,
    risk_score,
    CASE
        WHEN risk_score <= 0.33 THEN 'Low'
        WHEN risk_score <= 0.66 THEN 'Medium'
        ELSE 'High'
    END AS risk_tier
FROM scored;

-- Index on the materialized view for fast queries
CREATE INDEX idx_mv_risk_tier ON mv_patient_risk_score (risk_tier);
CREATE INDEX idx_mv_risk_score ON mv_patient_risk_score (risk_score);

-- Summary stats on the view
COMMENT ON MATERIALIZED VIEW mv_patient_risk_score IS
    'Patient Risk Score V2 -- 5-factor composite (age 35%, chronic 25%, frequency 20%, amount 15%, renal 5%).
     Refresh quarterly or after data load. Validated against actual claim costs on Day 6.';
