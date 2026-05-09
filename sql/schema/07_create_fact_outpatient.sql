-- ============================================================
-- File: 07_create_fact_outpatient.sql
-- Purpose: Outpatient claim transactions (clinic visits, no hospitalization)
-- Source: Train_Outpatientdata.csv (517,737 rows)
-- Grain: One row per outpatient claim
-- ============================================================

CREATE TABLE fact_outpatient_claims (
    claim_id                  VARCHAR(20)   PRIMARY KEY,     -- Source: ClaimID

    -- Foreign keys to dimensions
    beneficiary_id            VARCHAR(20)   NOT NULL REFERENCES dim_beneficiary(beneficiary_id),
    provider_id               VARCHAR(20)   NOT NULL REFERENCES dim_provider(provider_id),
    claim_start_date_key      DATE          NOT NULL REFERENCES dim_date(date_key),
    claim_end_date_key        DATE          NOT NULL REFERENCES dim_date(date_key),

    -- Measures ($)
    claim_amount_reimbursed   NUMERIC(12,2) NOT NULL DEFAULT 0,
    deductible_amt_paid       NUMERIC(12,2)          DEFAULT 0,

    -- Physician IDs
    attending_physician_id    VARCHAR(20),
    operating_physician_id    VARCHAR(20),
    other_physician_id        VARCHAR(20),

    -- Diagnosis
    admit_diagnosis_code      VARCHAR(10)
);

-- Indexes for common query patterns
CREATE INDEX idx_out_provider     ON fact_outpatient_claims (provider_id);
CREATE INDEX idx_out_beneficiary  ON fact_outpatient_claims (beneficiary_id);
CREATE INDEX idx_out_start_date   ON fact_outpatient_claims (claim_start_date_key);

COMMENT ON TABLE fact_outpatient_claims IS 'Outpatient claim transactions; one row per outpatient claim';
