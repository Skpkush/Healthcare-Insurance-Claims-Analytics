-- ============================================================
-- File: 04_create_dim_diagnosis.sql
-- Purpose: Distinct ICD diagnosis codes (built from claim data on Day 3)
-- Source: Distinct codes from ClmDiagnosisCode_1..10 across all claims
-- ============================================================

CREATE TABLE dim_diagnosis_code (
    diagnosis_code  VARCHAR(10)  PRIMARY KEY,    -- ICD-9 or ICD-10 code
    icd_version     SMALLINT,                     -- 9 or 10 (most are ICD-9 in this 2008-2010 era)
    description     VARCHAR(500)                  -- Optional: from external ICD lookup table (Day 7)
);

COMMENT ON TABLE dim_diagnosis_code IS 'Distinct diagnosis codes referenced by claims';
COMMENT ON COLUMN dim_diagnosis_code.description IS 'Populated optionally from CMS ICD lookup; NULL if not available';
