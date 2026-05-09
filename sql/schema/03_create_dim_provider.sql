-- ============================================================
-- File: 03_create_dim_provider.sql
-- Purpose: Healthcare providers with potential-fraud labels
-- Source: Train.csv (5,410 providers, 9.35% fraud-flagged)
-- ============================================================

CREATE TABLE dim_provider (
    provider_id               VARCHAR(20)   PRIMARY KEY,           -- Source: Provider (e.g., 'PRV51001')
    is_potentially_fraudulent BOOLEAN       NOT NULL DEFAULT FALSE -- Source: PotentialFraud (Yes/No)
);

-- Index for fraud-filtered queries (very common access pattern)
CREATE INDEX idx_provider_fraud ON dim_provider (is_potentially_fraudulent);

COMMENT ON TABLE dim_provider IS 'Provider dimension with fraud labels from training set';
COMMENT ON COLUMN dim_provider.is_potentially_fraudulent IS 'TRUE for providers flagged as potentially fraudulent (~9.35% in training data)';
