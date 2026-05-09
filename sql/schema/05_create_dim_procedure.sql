-- ============================================================
-- File: 05_create_dim_procedure.sql
-- Purpose: Distinct procedure codes
-- Source: Distinct codes from ClmProcedureCode_1..6 across all claims
-- ============================================================

CREATE TABLE dim_procedure_code (
    procedure_code  VARCHAR(10)  PRIMARY KEY,
    description     VARCHAR(500)            -- Optional: from external CPT/HCPCS lookup
);

COMMENT ON TABLE dim_procedure_code IS 'Distinct procedure codes referenced by claims';
