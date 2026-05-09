-- ============================================================
-- File: sql/setup_db.sql
-- Purpose: Bootstrap the healthcare_claims_v2 database and project user.
-- Run once as the postgres superuser. The healthcare_user password is passed
-- as a psql variable to keep it out of source control:
--
--   psql -U postgres -h localhost \
--        -v healthcare_password='<choose a strong password>' \
--        -f sql/setup_db.sql
--
-- PowerShell (Windows) one-liner:
--   psql -U postgres -h localhost -v healthcare_password='YourStrongPw' -f sql/setup_db.sql
-- ============================================================

CREATE DATABASE healthcare_claims_v2;
CREATE USER healthcare_user WITH PASSWORD :'healthcare_password';
ALTER DATABASE healthcare_claims_v2 OWNER TO healthcare_user;

-- Switch into the new database to grant schema-level access (Postgres 15+
-- no longer grants the public schema to non-owners by default).
\c healthcare_claims_v2
ALTER SCHEMA public OWNER TO healthcare_user;

\echo 'Bootstrap complete: database healthcare_claims_v2, user healthcare_user, schema public owned by healthcare_user.'
