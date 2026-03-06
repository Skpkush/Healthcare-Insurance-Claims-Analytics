-- ============================================================
-- BUSINESS ANALYSIS — Advanced SQL Queries (PostgreSQL)
-- Healthcare & Insurance Claims Analytics
-- ============================================================

select * from fact_claims
select * from dim_date
select * from dim_policies
select * from dim_providers
select * from dim_patients



-- Q1: Loss Ratio Analysis by Insurer & Quarter
WITH quarterly_claims AS (
    SELECT pol.insurer, d.year, d.quarter,
        COUNT(DISTINCT fc.patient_id) AS unique_claimants,
        COUNT(*) AS total_claims,
        SUM(fc.claim_amount) AS gross_claims,
        SUM(fc.approved_amount) AS net_claims_paid,
        SUM(CASE WHEN fc.status = 'Rejected' THEN 1 ELSE 0 END) AS rejected_claims
    FROM fact_claims fc
    JOIN dim_date d ON fc.date_key = d.date_key
    JOIN dim_policies pol ON fc.policy_id = pol.policy_id
    GROUP BY pol.insurer, d.year, d.quarter
),
premium_estimates AS (
    SELECT insurer, SUM(annual_premium) * 50 / 4 AS estimated_quarterly_premium
    FROM dim_policies GROUP BY insurer
)
SELECT qc.insurer, qc.year, qc.quarter, qc.total_claims,
    qc.gross_claims, qc.net_claims_paid, pe.estimated_quarterly_premium,
    ROUND(qc.net_claims_paid * 100.0 / NULLIF(pe.estimated_quarterly_premium, 0), 2) AS loss_ratio_pct,
    CASE WHEN qc.net_claims_paid * 100.0 / NULLIF(pe.estimated_quarterly_premium, 0) > 100 THEN 'LOSS MAKING'
         WHEN qc.net_claims_paid * 100.0 / NULLIF(pe.estimated_quarterly_premium, 0) > 80 THEN 'HIGH RISK'
         WHEN qc.net_claims_paid * 100.0 / NULLIF(pe.estimated_quarterly_premium, 0) > 60 THEN 'MODERATE'
         ELSE 'PROFITABLE' END AS profitability_status,
    ROUND(qc.rejected_claims * 100.0 / qc.total_claims, 2) AS rejection_rate_pct
FROM quarterly_claims qc
JOIN premium_estimates pe ON qc.insurer = pe.insurer
ORDER BY qc.insurer, qc.year, qc.quarter;


-- Q2: Patient Risk Scoring Model
WITH patient_metrics AS (
    SELECT p.patient_id, p.age, p.gender, p.state, p.chronic_condition,
        COUNT(*) AS total_claims, SUM(fc.claim_amount) AS total_claimed,
        SUM(fc.approved_amount) AS total_approved, AVG(fc.claim_amount) AS avg_claim,
        MAX(fc.claim_amount) AS max_claim,
        SUM(fc.is_emergency) AS emergency_count,
        SUM(fc.is_readmission) AS readmission_count,
        AVG(fc.length_of_stay) AS avg_los
    FROM dim_patients p
    JOIN fact_claims fc ON p.patient_id = fc.patient_id
    GROUP BY p.patient_id, p.age, p.gender, p.state, p.chronic_condition
)
SELECT patient_id, age, gender, state, chronic_condition,
    total_claims, total_claimed, emergency_count, readmission_count,
    ROUND(avg_los::NUMERIC, 1) AS avg_length_of_stay,
    -- RISK SCORE (0-100)
    (CASE WHEN age > 60 THEN 20 ELSE ROUND(age * 20.0 / 60) END
    + CASE WHEN chronic_condition IN ('Diabetes+Hypertension','Heart Disease','COPD') THEN 25
           WHEN chronic_condition IN ('Diabetes','Hypertension','Obesity') THEN 15
           WHEN chronic_condition = 'None' THEN 0 ELSE 10 END
    + CASE WHEN total_claims > 15 THEN 20 WHEN total_claims > 10 THEN 15
           WHEN total_claims > 5 THEN 10 ELSE 5 END
    + CASE WHEN emergency_count > 3 THEN 15 WHEN emergency_count > 1 THEN 10 ELSE 0 END
    + CASE WHEN readmission_count > 2 THEN 20 WHEN readmission_count > 0 THEN 10 ELSE 0 END
    ) AS risk_score,
    CASE WHEN (CASE WHEN age > 60 THEN 20 ELSE ROUND(age * 20.0 / 60) END
        + CASE WHEN chronic_condition IN ('Diabetes+Hypertension','Heart Disease','COPD') THEN 25
               WHEN chronic_condition IN ('Diabetes','Hypertension','Obesity') THEN 15
               WHEN chronic_condition = 'None' THEN 0 ELSE 10 END
        + CASE WHEN total_claims > 15 THEN 20 WHEN total_claims > 10 THEN 15
               WHEN total_claims > 5 THEN 10 ELSE 5 END
        + CASE WHEN emergency_count > 3 THEN 15 WHEN emergency_count > 1 THEN 10 ELSE 0 END
        + CASE WHEN readmission_count > 2 THEN 20 WHEN readmission_count > 0 THEN 10 ELSE 0 END
    ) >= 65 THEN 'HIGH RISK'
    WHEN (CASE WHEN age > 60 THEN 20 ELSE ROUND(age * 20.0 / 60) END
        + CASE WHEN chronic_condition IN ('Diabetes+Hypertension','Heart Disease','COPD') THEN 25
               WHEN chronic_condition IN ('Diabetes','Hypertension','Obesity') THEN 15
               WHEN chronic_condition = 'None' THEN 0 ELSE 10 END
        + CASE WHEN total_claims > 15 THEN 20 WHEN total_claims > 10 THEN 15
               WHEN total_claims > 5 THEN 10 ELSE 5 END
        + CASE WHEN emergency_count > 3 THEN 15 WHEN emergency_count > 1 THEN 10 ELSE 0 END
        + CASE WHEN readmission_count > 2 THEN 20 WHEN readmission_count > 0 THEN 10 ELSE 0 END
    ) >= 40 THEN 'MEDIUM RISK'
    ELSE 'LOW RISK' END AS risk_category
FROM patient_metrics ORDER BY risk_score DESC LIMIT 100;


-- Q3: Claim Rejection Root Cause Analysis
SELECT pol.insurer, fc.rejection_reason,
    COUNT(*) AS total_rejections,
    SUM(fc.claim_amount) AS total_value_rejected,
    ROUND(AVG(fc.claim_amount)::NUMERIC) AS avg_amount_per_rejection,
    STRING_AGG(DISTINCT fc.diagnosis_name, ', ' ORDER BY fc.diagnosis_name) AS diagnoses_affected,
    STRING_AGG(DISTINCT p.state, ', ' ORDER BY p.state) AS states_affected
FROM fact_claims fc
JOIN dim_policies pol ON fc.policy_id = pol.policy_id
JOIN dim_patients p ON fc.patient_id = p.patient_id
WHERE fc.status = 'Rejected'
GROUP BY pol.insurer, fc.rejection_reason
ORDER BY total_rejections DESC LIMIT 30;


-- Q4: State-Level Healthcare Access & Cost Analysis
SELECT p.state,
    COUNT(DISTINCT p.patient_id) AS total_patients,
    COUNT(DISTINCT dp.provider_id) AS hospitals_used,
    COUNT(*) AS total_claims,
    SUM(fc.claim_amount) AS total_claimed,
    SUM(fc.approved_amount) AS total_approved,
    ROUND(AVG(fc.claim_amount)::NUMERIC) AS avg_claim_amount,
    ROUND(AVG(fc.settlement_days)::NUMERIC, 1) AS avg_settlement_days,
    ROUND(SUM(fc.is_emergency) * 100.0 / COUNT(*), 2) AS emergency_pct,
    ROUND(SUM(fc.is_readmission) * 100.0 / COUNT(*), 2) AS readmission_pct,
    ROUND(SUM(CASE WHEN fc.status = 'Approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS approval_rate_pct,
    ROUND(SUM(fc.claim_amount) * 1.0 / COUNT(DISTINCT p.patient_id)) AS claim_per_patient
FROM fact_claims fc
JOIN dim_patients p ON fc.patient_id = p.patient_id
JOIN dim_providers dp ON fc.provider_id = dp.provider_id
GROUP BY p.state ORDER BY total_claimed DESC;


-- Q5: Treatment Cost Benchmarking by Diagnosis (with PERCENTILE_CONT)
SELECT fc.diagnosis_code, fc.diagnosis_name,
    COUNT(*) AS total_cases,
    ROUND(AVG(fc.claim_amount)::NUMERIC) AS avg_cost,
    MIN(fc.claim_amount) AS min_cost,
    MAX(fc.claim_amount) AS max_cost,
    ROUND(STDDEV(fc.claim_amount)::NUMERIC) AS stddev_cost,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY fc.claim_amount)::NUMERIC) AS p25_cost,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY fc.claim_amount)::NUMERIC) AS median_cost,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY fc.claim_amount)::NUMERIC) AS p75_cost,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fc.claim_amount)::NUMERIC) AS p95_cost,
    ROUND(AVG(fc.approved_amount)::NUMERIC) AS avg_approved,
    ROUND(AVG(fc.approved_amount) * 100.0 / NULLIF(AVG(fc.claim_amount), 0), 2) AS avg_approval_pct,
    ROUND(AVG(fc.length_of_stay)::NUMERIC, 1) AS avg_los
FROM fact_claims fc
GROUP BY fc.diagnosis_code, fc.diagnosis_name ORDER BY total_cases DESC;


-- Q6: Insurer Comparison Scorecard
SELECT pol.insurer,
    COUNT(*) AS total_claims,
    COUNT(DISTINCT fc.patient_id) AS unique_patients,
    SUM(fc.claim_amount) AS gross_claims_value,
    SUM(fc.approved_amount) AS total_paid_out,
    ROUND(SUM(fc.approved_amount) * 100.0 / NULLIF(SUM(fc.claim_amount), 0), 2) AS payout_ratio_pct,
    ROUND(AVG(fc.settlement_days)::NUMERIC, 1) AS avg_settlement_days,
    ROUND(SUM(CASE WHEN fc.status = 'Approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS approval_rate,
    ROUND(SUM(CASE WHEN fc.status = 'Rejected' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS rejection_rate,
    ROUND(SUM(fc.is_emergency) * 100.0 / COUNT(*), 2) AS emergency_claim_pct,
    ROUND(SUM(fc.is_readmission) * 100.0 / COUNT(*), 2) AS readmission_pct,
    CASE WHEN SUM(CASE WHEN fc.status='Approved' THEN 1 ELSE 0 END)*100.0/COUNT(*) > 60 
              AND AVG(fc.settlement_days) < 30 THEN 'A - EXCELLENT'
         WHEN SUM(CASE WHEN fc.status='Approved' THEN 1 ELSE 0 END)*100.0/COUNT(*) > 50 
              AND AVG(fc.settlement_days) < 45 THEN 'B - GOOD'
         WHEN SUM(CASE WHEN fc.status='Rejected' THEN 1 ELSE 0 END)*100.0/COUNT(*) > 20 THEN 'D - POOR'
         ELSE 'C - AVERAGE' END AS overall_grade
FROM fact_claims fc
JOIN dim_policies pol ON fc.policy_id = pol.policy_id
GROUP BY pol.insurer ORDER BY approval_rate DESC;


-- Q7: Monthly Trend Dashboard Query (for Power BI)
SELECT d.year, d.month, d.month_name, pol.insurer, dp.tier AS hospital_tier,
    fc.treatment_type, fc.diagnosis_name, p.state,
    COUNT(*) AS claim_count,
    COUNT(DISTINCT fc.patient_id) AS unique_patients,
    SUM(fc.claim_amount) AS gross_claims,
    SUM(fc.approved_amount) AS net_paid,
    AVG(fc.claim_amount) AS avg_claim,
    SUM(CASE WHEN fc.status = 'Approved' THEN 1 ELSE 0 END) AS approved_count,
    SUM(CASE WHEN fc.status = 'Rejected' THEN 1 ELSE 0 END) AS rejected_count,
    AVG(fc.settlement_days) AS avg_settlement_days,
    AVG(fc.length_of_stay) AS avg_los,
    SUM(fc.is_emergency) AS emergency_count,
    SUM(fc.is_readmission) AS readmission_count
FROM fact_claims fc
JOIN dim_date d ON fc.date_key = d.date_key
JOIN dim_policies pol ON fc.policy_id = pol.policy_id
JOIN dim_providers dp ON fc.provider_id = dp.provider_id
JOIN dim_patients p ON fc.patient_id = p.patient_id
GROUP BY d.year, d.month, d.month_name, pol.insurer, dp.tier,
    fc.treatment_type, fc.diagnosis_name, p.state
ORDER BY d.year, d.month;
