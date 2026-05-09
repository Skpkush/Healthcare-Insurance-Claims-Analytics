-- ============================================================
-- File: 02_create_dim_beneficiary.sql
-- Purpose: Patient demographics + chronic conditions + annual coverage
-- Source: Train_Beneficiarydata.csv (138,556 rows)
-- ============================================================

CREATE TABLE dim_beneficiary (
    beneficiary_id              VARCHAR(20)   PRIMARY KEY,        -- Source: BeneID (e.g., 'BENE11001')
    date_of_birth               DATE          NOT NULL,           -- Source: DOB
    date_of_death               DATE,                              -- Source: DOD (99% NULL — most patients alive)
    gender                      SMALLINT      NOT NULL,           -- Source: Gender (1=male, 2=female per data)
    race                        SMALLINT      NOT NULL,           -- Source: Race (categorical code)
    state_code                  SMALLINT,                          -- Source: State (US state FIPS)
    county_code                 INTEGER,                           -- Source: County
    has_renal_disease           BOOLEAN       NOT NULL DEFAULT FALSE,  -- Source: RenalDiseaseIndicator (Y/0)

    -- Chronic conditions (Source: ChronicCond_* columns; convert 1/2 → TRUE/FALSE)
    -- IMPORTANT: in this dataset 1=Yes, 2=No (counter-intuitive — verify on load)
    chronic_alzheimer            BOOLEAN     NOT NULL DEFAULT FALSE,  -- ChronicCond_Alzheimer
    chronic_heart_failure        BOOLEAN     NOT NULL DEFAULT FALSE,  -- ChronicCond_Heartfailure
    chronic_kidney_disease       BOOLEAN     NOT NULL DEFAULT FALSE,  -- ChronicCond_KidneyDisease
    chronic_cancer               BOOLEAN     NOT NULL DEFAULT FALSE,  -- ChronicCond_Cancer
    chronic_obstr_pulmonary      BOOLEAN     NOT NULL DEFAULT FALSE,  -- ChronicCond_ObstrPulmonary (COPD)
    chronic_depression           BOOLEAN     NOT NULL DEFAULT FALSE,  -- ChronicCond_Depression
    chronic_diabetes             BOOLEAN     NOT NULL DEFAULT FALSE,  -- ChronicCond_Diabetes
    chronic_ischemic_heart       BOOLEAN     NOT NULL DEFAULT FALSE,  -- ChronicCond_IschemicHeart (67.6% prevalence!)
    chronic_osteoporosis         BOOLEAN     NOT NULL DEFAULT FALSE,  -- ChronicCond_Osteoporasis (sic)
    chronic_rheumatoid_arthritis BOOLEAN     NOT NULL DEFAULT FALSE,  -- ChronicCond_rheumatoidarthritis
    chronic_stroke               BOOLEAN     NOT NULL DEFAULT FALSE,  -- ChronicCond_stroke

    -- Coverage months
    months_part_a_coverage      SMALLINT,                              -- Source: NoOfMonths_PartACov (0-12)
    months_part_b_coverage      SMALLINT,                              -- Source: NoOfMonths_PartBCov (0-12)

    -- Annual amounts ($)
    ip_annual_reimbursement_amt NUMERIC(12,2) DEFAULT 0,               -- Source: IPAnnualReimbursementAmt
    ip_annual_deductible_amt    NUMERIC(12,2) DEFAULT 0,               -- Source: IPAnnualDeductibleAmt
    op_annual_reimbursement_amt NUMERIC(12,2) DEFAULT 0,               -- Source: OPAnnualReimbursementAmt
    op_annual_deductible_amt    NUMERIC(12,2) DEFAULT 0                -- Source: OPAnnualDeductibleAmt
);

COMMENT ON TABLE dim_beneficiary IS 'Patient/beneficiary dimension with chronic condition flags';
