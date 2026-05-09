-- ============================================================
-- File: 09_create_bridge_claim_procedure.sql
-- Purpose: Bridge table — each claim → up to 6 procedures with positional context
-- Source: ClmProcedureCode_1 through ClmProcedureCode_6 (unpivoted on Day 4)
-- Estimated rows: ~500K-1M (most claims have 0-2 procedures)
-- ============================================================

CREATE TABLE bridge_claim_procedure (
    claim_id            VARCHAR(20)  NOT NULL,
    procedure_code      VARCHAR(10)  NOT NULL REFERENCES dim_procedure_code(procedure_code),
    procedure_position  SMALLINT     NOT NULL CHECK (procedure_position BETWEEN 1 AND 6),

    PRIMARY KEY (claim_id, procedure_position)
);

CREATE INDEX idx_bridge_proc_code ON bridge_claim_procedure (procedure_code);

COMMENT ON TABLE bridge_claim_procedure IS 'Bridge table linking claims to multiple procedure codes';
COMMENT ON COLUMN bridge_claim_procedure.procedure_position IS '1=primary procedure; 2-6=additional procedures';
