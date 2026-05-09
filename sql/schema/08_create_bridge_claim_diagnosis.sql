-- ============================================================
-- File: 08_create_bridge_claim_diagnosis.sql
-- Purpose: Bridge table — each claim → up to 10 diagnoses with positional context
-- Source: ClmDiagnosisCode_1 through ClmDiagnosisCode_10 (unpivoted on Day 4)
-- Grain: One row per claim+diagnosis_position pair
-- Estimated rows: ~3-5 million (avg ~6 diagnoses per claim × 558K claims)
-- ============================================================

CREATE TABLE bridge_claim_diagnosis (
    claim_id            VARCHAR(20)  NOT NULL,
    diagnosis_code      VARCHAR(10)  NOT NULL REFERENCES dim_diagnosis_code(diagnosis_code),
    diagnosis_position  SMALLINT     NOT NULL CHECK (diagnosis_position BETWEEN 1 AND 10),

    PRIMARY KEY (claim_id, diagnosis_position)
);

-- Critical index: filtering by diagnosis_code is the most common access path
CREATE INDEX idx_bridge_diag_code ON bridge_claim_diagnosis (diagnosis_code);

COMMENT ON TABLE bridge_claim_diagnosis IS 'Bridge table linking claims to multiple diagnosis codes';
COMMENT ON COLUMN bridge_claim_diagnosis.diagnosis_position IS '1=primary diagnosis (principal admission reason); 2-10=secondary/comorbid';
COMMENT ON COLUMN bridge_claim_diagnosis.claim_id IS 'No FK constraint — claim_id may exist in either fact_inpatient_claims OR fact_outpatient_claims; enforced at load layer';
