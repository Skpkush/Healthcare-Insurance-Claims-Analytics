-- ============================================================
-- File: 06_create_fact_inpatient.sql
-- Purpose: Inpatient claim transactions (hospital admissions)
-- Source: Train_Inpatientdata.csv (40,474 rows)
-- Grain: One row per inpatient claim
-- ============================================================

CREATE TABLE fact_inpatient_claims (
    claim_id                  VARCHAR(20)   PRIMARY KEY,     -- Source: ClaimID

    -- Foreign keys to dimensions
    beneficiary_id            VARCHAR(20)   NOT NULL REFERENCES dim_beneficiary(beneficiary_id),  -- Source: BeneID
    provider_id               VARCHAR(20)   NOT NULL REFERENCES dim_provider(provider_id),         -- Source: Provider
    claim_start_date_key      DATE          NOT NULL REFERENCES dim_date(date_key),                -- Source: ClaimStartDt
    claim_end_date_key        DATE          NOT NULL REFERENCES dim_date(date_key),                -- Source: ClaimEndDt
    admission_date_key        DATE                   REFERENCES dim_date(date_key),                -- Source: AdmissionDt (inpatient-only)
    discharge_date_key        DATE                   REFERENCES dim_date(date_key),                -- Source: DischargeDt (inpatient-only)

    -- Measures ($)
    claim_amount_reimbursed   NUMERIC(12,2) NOT NULL DEFAULT 0,  -- Source: InscClaimAmtReimbursed
    deductible_amt_paid       NUMERIC(12,2)          DEFAULT 0,  -- Source: DeductibleAmtPaid

    -- Physician IDs (3 roles, no FK because no dim_physician table — see design doc)
    attending_physician_id    VARCHAR(20),                       -- Source: AttendingPhysician
    operating_physician_id    VARCHAR(20),                       -- Source: OperatingPhysician
    other_physician_id        VARCHAR(20),                       -- Source: OtherPhysician

    -- Diagnosis info
    admit_diagnosis_code      VARCHAR(10),                       -- Source: ClmAdmitDiagnosisCode
    diagnosis_group_code      VARCHAR(10)                        -- Source: DiagnosisGroupCode (DRG, inpatient-only)
);

-- Indexes for common query patterns
CREATE INDEX idx_inp_provider     ON fact_inpatient_claims (provider_id);
CREATE INDEX idx_inp_beneficiary  ON fact_inpatient_claims (beneficiary_id);
CREATE INDEX idx_inp_start_date   ON fact_inpatient_claims (claim_start_date_key);

COMMENT ON TABLE fact_inpatient_claims IS 'Inpatient claim transactions; one row per inpatient claim';
COMMENT ON COLUMN fact_inpatient_claims.diagnosis_group_code IS 'DRG (Diagnosis Related Group) — inpatient-only Medicare reimbursement code';
