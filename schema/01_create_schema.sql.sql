

DROP TABLE IF EXISTS fact_claims CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_patients CASCADE;
DROP TABLE IF EXISTS dim_providers CASCADE;
DROP TABLE IF EXISTS dim_policies CASCADE;

-- ============================================================
-- dim_date (1,096 rows)
-- CSV columns: date_key,full_date,day,month,month_name,quarter,year,day_of_week,is_weekend
-- ============================================================
CREATE TABLE dim_date (
    date_key        INT PRIMARY KEY,
    full_date       DATE NOT NULL,
    day             INT,
    month           INT,
    month_name      VARCHAR(20),
    quarter         INT,
    year            INT,
    day_of_week     VARCHAR(15),
    is_weekend      INT
);

-- ============================================================
-- dim_patients (5,000 rows)
-- CSV columns: patient_id,age,gender,city,state,chronic_condition,blood_group,registration_date
-- ============================================================
CREATE TABLE dim_patients (
    patient_id          VARCHAR(10) PRIMARY KEY,
    age                 INT,
    gender              VARCHAR(10),
    city                VARCHAR(50),
    state               VARCHAR(50),
    chronic_condition   VARCHAR(50),
    blood_group         VARCHAR(5),
    registration_date   DATE
);

-- ============================================================
-- dim_providers (200 rows)
-- CSV columns: provider_id,hospital_name,speciality,city,state,tier,bed_capacity,accreditation
-- ============================================================
CREATE TABLE dim_providers (
    provider_id     VARCHAR(10) PRIMARY KEY,
    hospital_name   VARCHAR(100),
    speciality      VARCHAR(50),
    city            VARCHAR(50),
    state           VARCHAR(50),
    tier            VARCHAR(10),
    bed_capacity    INT,
    accreditation   VARCHAR(10)
);

-- ============================================================
-- dim_policies (40 rows)
-- CSV columns: policy_id,insurer,plan_type,annual_premium,coverage_limit,deductible,copay_pct,waiting_period_days,claim_settlement_ratio
-- ============================================================
CREATE TABLE dim_policies (
    policy_id               VARCHAR(10) PRIMARY KEY,
    insurer                 VARCHAR(50),
    plan_type               VARCHAR(30),
    annual_premium          INT,
    coverage_limit          BIGINT,
    deductible              INT,
    copay_pct               INT,
    waiting_period_days     INT,
    claim_settlement_ratio  DECIMAL(4,2)
);

-- ============================================================
-- fact_claims (50,000 rows)
-- CSV columns: claim_id,patient_id,provider_id,policy_id,date_key,diagnosis_code,diagnosis_name,treatment_type,claim_amount,approved_amount,status,rejection_reason,settlement_days,length_of_stay,is_emergency,is_readmission
-- ============================================================
CREATE TABLE fact_claims (
    claim_id            VARCHAR(15) PRIMARY KEY,
    patient_id          VARCHAR(10) REFERENCES dim_patients(patient_id),
    provider_id         VARCHAR(10) REFERENCES dim_providers(provider_id),
    policy_id           VARCHAR(10) REFERENCES dim_policies(policy_id),
    date_key            INT REFERENCES dim_date(date_key),
    diagnosis_code      VARCHAR(10),
    diagnosis_name      VARCHAR(50),
    treatment_type      VARCHAR(20),
    claim_amount        BIGINT,
    approved_amount     BIGINT,
    status              VARCHAR(25),
    rejection_reason    VARCHAR(50),
    settlement_days     INT,
    length_of_stay      INT,
    is_emergency        INT,
    is_readmission      INT
);


select * from fact_claims
select * from dim_date
select * from dim_policies
select * from dim_providers
select * from dim_patients



-- ============================================================
-- INDEXES for Performance Optimization
-- ============================================================
CREATE INDEX idx_claims_patient ON fact_claims(patient_id);
CREATE INDEX idx_claims_provider ON fact_claims(provider_id);
CREATE INDEX idx_claims_policy ON fact_claims(policy_id);
CREATE INDEX idx_claims_date ON fact_claims(date_key);
CREATE INDEX idx_claims_status ON fact_claims(status);
CREATE INDEX idx_claims_diagnosis ON fact_claims(diagnosis_code);
CREATE INDEX idx_date_year_month ON dim_date(year, month);
CREATE INDEX idx_patients_state ON dim_patients(state);
CREATE INDEX idx_providers_tier ON dim_providers(tier);

show db