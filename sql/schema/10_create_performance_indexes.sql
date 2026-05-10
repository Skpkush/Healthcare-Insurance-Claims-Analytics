-- ============================================================
-- File: 10_create_performance_indexes.sql
-- Purpose: Performance indexes added based on Day 5 EXPLAIN ANALYZE evidence
-- These supplement the FK/fraud-flag indexes already in dims/facts DDL
-- ============================================================

-- Bridge claim_id lookups (used in joins from facts to bridges)
-- Already covered by composite PK (claim_id, diagnosis_position) but add explicit
-- index on claim_id alone since many queries don't filter on position
CREATE INDEX IF NOT EXISTS idx_bridge_diag_claim_id
    ON bridge_claim_diagnosis (claim_id);

CREATE INDEX IF NOT EXISTS idx_bridge_proc_claim_id
    ON bridge_claim_procedure (claim_id);

-- Composite index for chronic-condition queries
-- These are common analytical access patterns: "patients with X+Y conditions"
CREATE INDEX IF NOT EXISTS idx_beneficiary_diabetes_ischemic
    ON dim_beneficiary (chronic_diabetes, chronic_ischemic_heart);

CREATE INDEX IF NOT EXISTS idx_beneficiary_heart_failure
    ON dim_beneficiary (chronic_heart_failure)
    WHERE chronic_heart_failure = TRUE;  -- Partial index -- smaller, faster

-- Date range queries on facts (Day 5 velocity analysis hits these)
CREATE INDEX IF NOT EXISTS idx_inp_provider_date
    ON fact_inpatient_claims (provider_id, claim_start_date_key);

CREATE INDEX IF NOT EXISTS idx_out_provider_date
    ON fact_outpatient_claims (provider_id, claim_start_date_key);

-- Beneficiary date queries (Day 7 will need these for ML feature engineering)
CREATE INDEX IF NOT EXISTS idx_inp_beneficiary_date
    ON fact_inpatient_claims (beneficiary_id, claim_start_date_key);

CREATE INDEX IF NOT EXISTS idx_out_beneficiary_date
    ON fact_outpatient_claims (beneficiary_id, claim_start_date_key);

-- Update table statistics so query planner uses new indexes
ANALYZE dim_beneficiary;
ANALYZE dim_provider;
ANALYZE fact_inpatient_claims;
ANALYZE fact_outpatient_claims;
ANALYZE bridge_claim_diagnosis;
ANALYZE bridge_claim_procedure;
