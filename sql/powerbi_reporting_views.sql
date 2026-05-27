-- ============================================================
-- File: powerbi_reporting_views.sql
-- Purpose: Plain views wrapping the materialized views.
--
-- Power BI's PostgreSQL Navigator enumerates objects via
-- information_schema, which (per the SQL standard) does NOT list
-- PostgreSQL materialized views. Wrapping each mv in a regular VIEW
-- makes it visible to Power BI. The view is a pass-through -- it
-- reads the already-computed mv, so there is no performance cost.
--
-- Refresh order after a data reload:
--   REFRESH MATERIALIZED VIEW mv_patient_risk_score;
--   REFRESH MATERIALIZED VIEW mv_provider_features;
-- (the views below need no refresh -- they always reflect the mv)
-- ============================================================

CREATE OR REPLACE VIEW v_patient_risk_score AS
    SELECT * FROM mv_patient_risk_score;

CREATE OR REPLACE VIEW v_provider_features AS
    SELECT * FROM mv_provider_features;
