-- ============================================================
-- File: 01_provider_features.sql
-- Purpose: Per-provider feature matrix for fraud detection ML
-- Output: One row per provider with engineered features
-- ============================================================

DROP MATERIALIZED VIEW IF EXISTS mv_provider_features;

CREATE MATERIALIZED VIEW mv_provider_features AS
WITH all_claims AS (
    SELECT
        provider_id, beneficiary_id, claim_id,
        claim_amount_reimbursed, deductible_amt_paid,
        claim_start_date_key, claim_end_date_key,
        attending_physician_id, operating_physician_id, other_physician_id,
        'inpatient' AS claim_type
    FROM fact_inpatient_claims
    UNION ALL
    SELECT
        provider_id, beneficiary_id, claim_id,
        claim_amount_reimbursed, deductible_amt_paid,
        claim_start_date_key, claim_end_date_key,
        attending_physician_id, operating_physician_id, other_physician_id,
        'outpatient'
    FROM fact_outpatient_claims
),
provider_volume AS (
    SELECT
        provider_id,
        COUNT(*)                                         AS total_claims,
        COUNT(DISTINCT beneficiary_id)                   AS unique_patients,
        COUNT(DISTINCT claim_start_date_key)             AS active_days,
        SUM(CASE WHEN claim_type = 'inpatient' THEN 1 ELSE 0 END)  AS inpatient_claims,
        SUM(CASE WHEN claim_type = 'outpatient' THEN 1 ELSE 0 END) AS outpatient_claims
    FROM all_claims
    GROUP BY provider_id
),
provider_amounts AS (
    SELECT
        provider_id,
        SUM(claim_amount_reimbursed)                                    AS total_reimbursed,
        AVG(claim_amount_reimbursed)                                    AS avg_claim_amount,
        STDDEV(claim_amount_reimbursed)                                 AS stddev_claim_amount,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY claim_amount_reimbursed) AS median_claim,
        MAX(claim_amount_reimbursed)                                    AS max_claim_amount,
        SUM(deductible_amt_paid)                                        AS total_deductible
    FROM all_claims
    GROUP BY provider_id
),
provider_velocity AS (
    SELECT
        provider_id,
        MAX(claims_per_day) AS peak_daily_claims,
        AVG(claims_per_day) AS avg_daily_claims
    FROM (
        SELECT provider_id, claim_start_date_key, COUNT(*) AS claims_per_day
        FROM all_claims
        GROUP BY provider_id, claim_start_date_key
    ) daily
    GROUP BY provider_id
),
provider_diagnoses AS (
    -- Use the bridge to count diagnoses per claim, then aggregate
    SELECT
        ac.provider_id,
        AVG(diag_count)::numeric(5,2) AS avg_diagnoses_per_claim,
        MAX(diag_count) AS max_diagnoses_per_claim,
        SUM(CASE WHEN diag_count >= 9 THEN 1 ELSE 0 END) AS claims_with_9plus_diagnoses
    FROM all_claims ac
    LEFT JOIN (
        SELECT claim_id, COUNT(*) AS diag_count
        FROM bridge_claim_diagnosis
        GROUP BY claim_id
    ) cd ON cd.claim_id = ac.claim_id
    GROUP BY ac.provider_id
),
provider_patient_risk AS (
    -- Aggregate risk scores of provider's patients
    SELECT
        ac.provider_id,
        AVG(prs.risk_score) AS avg_patient_risk_score,
        SUM(CASE WHEN prs.risk_tier = 'High' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0) AS pct_high_risk_patients,
        AVG(prs.chronic_count) AS avg_patient_chronic_count
    FROM all_claims ac
    JOIN mv_patient_risk_score prs ON prs.beneficiary_id = ac.beneficiary_id
    GROUP BY ac.provider_id
),
provider_physician_diversity AS (
    -- How concentrated is the physician network?
    SELECT
        provider_id,
        COUNT(DISTINCT attending_physician_id) AS distinct_attending_physicians,
        COUNT(DISTINCT operating_physician_id) AS distinct_operating_physicians
    FROM all_claims
    GROUP BY provider_id
)
-- Final assembly
SELECT
    p.provider_id,
    p.is_potentially_fraudulent::int  AS label,
    -- Volume
    pv.total_claims,
    pv.unique_patients,
    pv.active_days,
    (pv.total_claims::float / NULLIF(pv.unique_patients, 0))::numeric(8,2) AS claims_per_patient,
    (pv.total_claims::float / NULLIF(pv.active_days, 0))::numeric(8,2)     AS claims_per_active_day,
    pv.inpatient_claims,
    pv.outpatient_claims,
    (pv.inpatient_claims::float / NULLIF(pv.total_claims, 0))::numeric(5,4) AS inpatient_ratio,
    -- Amounts
    pa.total_reimbursed::numeric(14,2),
    pa.avg_claim_amount::numeric(12,2),
    pa.stddev_claim_amount::numeric(12,2),
    pa.median_claim::numeric(12,2),
    pa.max_claim_amount::numeric(12,2),
    (pa.stddev_claim_amount / NULLIF(pa.avg_claim_amount, 0))::numeric(6,4) AS coefficient_of_variation,
    -- Velocity
    pvel.peak_daily_claims,
    pvel.avg_daily_claims::numeric(6,2),
    -- Diagnosis density
    pd.avg_diagnoses_per_claim,
    pd.max_diagnoses_per_claim,
    pd.claims_with_9plus_diagnoses,
    -- Patient risk
    ppr.avg_patient_risk_score::numeric(5,4),
    ppr.pct_high_risk_patients::numeric(5,4),
    ppr.avg_patient_chronic_count::numeric(5,2),
    -- Physician diversity
    ppd.distinct_attending_physicians,
    ppd.distinct_operating_physicians
FROM dim_provider p
LEFT JOIN provider_volume pv ON pv.provider_id = p.provider_id
LEFT JOIN provider_amounts pa ON pa.provider_id = p.provider_id
LEFT JOIN provider_velocity pvel ON pvel.provider_id = p.provider_id
LEFT JOIN provider_diagnoses pd ON pd.provider_id = p.provider_id
LEFT JOIN provider_patient_risk ppr ON ppr.provider_id = p.provider_id
LEFT JOIN provider_physician_diversity ppd ON ppd.provider_id = p.provider_id;

CREATE INDEX idx_mv_provider_features_label ON mv_provider_features (label);

COMMENT ON MATERIALIZED VIEW mv_provider_features IS
    'Per-provider feature matrix for fraud detection ML. 24 features + label.';
