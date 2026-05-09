-- ============================================================
-- File: 01_create_dim_date.sql
-- Purpose: Calendar dimension covering claim date range (~2008-2010)
-- Generated programmatically on Day 3 from claim start/end dates
-- ============================================================

CREATE TABLE dim_date (
    date_key        DATE        PRIMARY KEY,
    day_of_week     SMALLINT    NOT NULL,    -- 0=Sunday ... 6=Saturday
    day_name        VARCHAR(10) NOT NULL,    -- 'Monday', 'Tuesday', etc.
    day_of_month    SMALLINT    NOT NULL,    -- 1-31
    day_of_year     SMALLINT    NOT NULL,    -- 1-366
    week_of_year    SMALLINT    NOT NULL,    -- 1-53
    month_number    SMALLINT    NOT NULL,    -- 1-12
    month_name      VARCHAR(10) NOT NULL,    -- 'January', 'February', etc.
    quarter         SMALLINT    NOT NULL,    -- 1-4
    year            SMALLINT    NOT NULL,
    is_weekend      BOOLEAN     NOT NULL
);

COMMENT ON TABLE dim_date IS 'Calendar dimension for claim date analysis';
COMMENT ON COLUMN dim_date.date_key IS 'Natural key — actual date, used in fact table FKs';
