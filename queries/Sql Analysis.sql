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

-- ============================================================
-- CTEs & SUBQUERIES — Advanced SQL Queries (PostgreSQL)
-- Healthcare & Insurance Claims Analytics
-- ============================================================

-- Q1: Fraud Detection — Patients with 3+ Claims in 90 Days
WITH claim_timeline AS (
    SELECT fc.patient_id, fc.claim_id, d.full_date AS claim_date,
        fc.claim_amount, fc.diagnosis_name, fc.provider_id,
        COUNT(*) OVER (
            PARTITION BY fc.patient_id ORDER BY d.full_date
            RANGE BETWEEN INTERVAL '90 days' PRECEDING AND CURRENT ROW
        ) AS claims_in_90_days
    FROM fact_claims fc
    JOIN dim_date d ON fc.date_key = d.date_key
),
fraud_flags AS (
    SELECT *, 
        CASE WHEN claims_in_90_days >= 5 THEN 'HIGH RISK'
             WHEN claims_in_90_days >= 3 THEN 'MEDIUM RISK'
             ELSE 'LOW RISK' END AS fraud_risk_level
    FROM claim_timeline WHERE claims_in_90_days >= 3
)
SELECT ff.*, p.age, p.city, p.chronic_condition, dp.hospital_name
FROM fraud_flags ff
JOIN dim_patients p ON ff.patient_id = p.patient_id
JOIN dim_providers dp ON ff.provider_id = dp.provider_id
ORDER BY claims_in_90_days DESC, claim_amount DESC LIMIT 100;


-- Q2: Cohort Analysis — Retention by Signup Quarter
WITH patient_cohorts AS (
    SELECT p.patient_id,
        EXTRACT(YEAR FROM p.registration_date)::TEXT || '-Q' || EXTRACT(QUARTER FROM p.registration_date)::TEXT AS signup_cohort
    FROM dim_patients p
),
cohort_activity AS (
    SELECT pc.signup_cohort, d.year AS activity_year, d.quarter AS activity_quarter,
        COUNT(DISTINCT pc.patient_id) AS active_patients,
        COUNT(*) AS total_claims,
        SUM(fc.claim_amount) AS total_claimed,
        AVG(fc.claim_amount) AS avg_claim_amount
    FROM patient_cohorts pc
    JOIN fact_claims fc ON pc.patient_id = fc.patient_id
    JOIN dim_date d ON fc.date_key = d.date_key
    GROUP BY pc.signup_cohort, d.year, d.quarter
)
SELECT signup_cohort, activity_year, activity_quarter, active_patients, total_claims,
    total_claimed, ROUND(avg_claim_amount) AS avg_claim,
    ROUND(total_claimed * 1.0 / active_patients) AS revenue_per_active_patient
FROM cohort_activity ORDER BY signup_cohort, activity_year, activity_quarter;


-- Q3: Claim Amount Anomaly Detection (Z-Score Method)
WITH diagnosis_stats AS (
    SELECT diagnosis_code, diagnosis_name,
        AVG(claim_amount) AS mean_amount,
        STDDEV(claim_amount) AS stddev_amount,
        COUNT(*) AS total_claims
    FROM fact_claims GROUP BY diagnosis_code, diagnosis_name
),
claim_zscore AS (
    SELECT fc.claim_id, fc.patient_id, fc.diagnosis_code, fc.diagnosis_name,
        fc.claim_amount, ds.mean_amount, ds.stddev_amount,
        ROUND(((fc.claim_amount - ds.mean_amount) / NULLIF(ds.stddev_amount, 0))::NUMERIC, 2) AS z_score
    FROM fact_claims fc
    JOIN diagnosis_stats ds ON fc.diagnosis_code = ds.diagnosis_code
)
SELECT claim_id, patient_id, diagnosis_name, claim_amount,
    ROUND(mean_amount::NUMERIC) AS diagnosis_avg, z_score,
    CASE WHEN z_score > 3 THEN 'EXTREME OUTLIER - Investigate'
         WHEN z_score > 2 THEN 'HIGH OUTLIER - Review'
         WHEN z_score < -2 THEN 'UNUSUALLY LOW - Verify'
         ELSE 'NORMAL' END AS anomaly_flag
FROM claim_zscore WHERE ABS(z_score) > 2
ORDER BY ABS(z_score) DESC LIMIT 50;


-- Q4: Multi-Level CTE — Hospital + Insurer Cross Performance
WITH hospital_insurer_metrics AS (
    SELECT dp.provider_id, dp.hospital_name, dp.tier, pol.insurer,
        COUNT(*) AS total_claims,
        SUM(fc.claim_amount) AS total_claimed,
        SUM(fc.approved_amount) AS total_approved,
        SUM(CASE WHEN fc.status = 'Rejected' THEN 1 ELSE 0 END) AS rejected_count,
        AVG(fc.settlement_days) AS avg_settlement_days
    FROM fact_claims fc
    JOIN dim_providers dp ON fc.provider_id = dp.provider_id
    JOIN dim_policies pol ON fc.policy_id = pol.policy_id
    GROUP BY dp.provider_id, dp.hospital_name, dp.tier, pol.insurer
    HAVING COUNT(*) >= 10
),
performance_scored AS (
    SELECT *,
        ROUND(rejected_count * 100.0 / total_claims, 2) AS rejection_rate,
        ROUND(total_approved * 100.0 / NULLIF(total_claimed, 0), 2) AS approval_value_pct,
        ROUND(total_approved * 1.0 / total_claims) AS avg_payout_per_claim
    FROM hospital_insurer_metrics
),
final_ranking AS (
    SELECT *,
        CASE WHEN rejection_rate > 25 AND avg_settlement_days > 45 THEN 'POOR'
             WHEN rejection_rate > 15 OR avg_settlement_days > 30 THEN 'BELOW AVERAGE'
             WHEN rejection_rate <= 10 AND avg_settlement_days <= 15 THEN 'EXCELLENT'
             ELSE 'AVERAGE' END AS performance_grade
    FROM performance_scored
)
SELECT * FROM final_ranking ORDER BY rejection_rate DESC, avg_settlement_days DESC LIMIT 30;


-- Q5: Patient Claim Escalation Pattern Detection
WITH numbered_claims AS (
    SELECT fc.patient_id, fc.claim_id, d.full_date, fc.claim_amount, fc.diagnosis_name,
        ROW_NUMBER() OVER (PARTITION BY fc.patient_id ORDER BY d.full_date) AS claim_seq
    FROM fact_claims fc JOIN dim_date d ON fc.date_key = d.date_key
),
escalation_check AS (
    SELECT c1.patient_id, c1.claim_seq,
        c1.claim_amount AS current_amount, c2.claim_amount AS next_amount, c3.claim_amount AS third_amount,
        CASE WHEN c2.claim_amount > c1.claim_amount AND c3.claim_amount > c2.claim_amount 
             THEN 'ESCALATING' ELSE 'STABLE' END AS pattern
    FROM numbered_claims c1
    JOIN numbered_claims c2 ON c1.patient_id = c2.patient_id AND c2.claim_seq = c1.claim_seq + 1
    JOIN numbered_claims c3 ON c1.patient_id = c3.patient_id AND c3.claim_seq = c1.claim_seq + 2
)
SELECT ec.patient_id, p.age, p.chronic_condition, p.city,
    COUNT(*) AS escalation_count,
    MAX(ec.third_amount) AS max_claim_in_escalation,
    ROUND(AVG(ec.third_amount - ec.current_amount)) AS avg_escalation_amount
FROM escalation_check ec
JOIN dim_patients p ON ec.patient_id = p.patient_id
WHERE ec.pattern = 'ESCALATING'
GROUP BY ec.patient_id, p.age, p.chronic_condition, p.city
HAVING COUNT(*) >= 2
ORDER BY escalation_count DESC, max_claim_in_escalation DESC LIMIT 30;


-- Q6: Correlated Subquery — Claims Above Provider Average
SELECT fc.claim_id, fc.patient_id, fc.provider_id, dp.hospital_name,
    fc.diagnosis_name, fc.claim_amount,
    (SELECT ROUND(AVG(fc2.claim_amount)::NUMERIC) FROM fact_claims fc2 
     WHERE fc2.provider_id = fc.provider_id) AS provider_avg_claim,
    fc.claim_amount - (SELECT AVG(fc2.claim_amount) FROM fact_claims fc2 
     WHERE fc2.provider_id = fc.provider_id) AS deviation_from_avg,
    fc.status
FROM fact_claims fc
JOIN dim_providers dp ON fc.provider_id = dp.provider_id
WHERE fc.claim_amount > (SELECT AVG(fc2.claim_amount) * 2.5 FROM fact_claims fc2 
      WHERE fc2.provider_id = fc.provider_id)
ORDER BY deviation_from_avg DESC LIMIT 50;





-- ============================================================
-- WINDOW FUNCTIONS — Advanced SQL Queries (PostgreSQL)
-- Healthcare & Insurance Claims Analytics
-- ============================================================

-- Q1: Running Total of Claims Per Patient Over Time
SELECT 
    fc.claim_id, fc.patient_id, d.full_date, fc.claim_amount,
    SUM(fc.claim_amount) OVER (
        PARTITION BY fc.patient_id ORDER BY d.full_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_claims,
    COUNT(*) OVER (PARTITION BY fc.patient_id ORDER BY d.full_date) AS cumulative_claim_count
FROM fact_claims fc
JOIN dim_date d ON fc.date_key = d.date_key
WHERE fc.patient_id IN ('P00001', 'P00002', 'P00003')
ORDER BY fc.patient_id, d.full_date;


-- Q2: Rank Hospitals by Claim Approval Rate (DENSE_RANK)
WITH hospital_performance AS (
    SELECT 
        dp.provider_id, dp.hospital_name, dp.tier, dp.accreditation,
        COUNT(*) AS total_claims,
        SUM(CASE WHEN fc.status = 'Approved' THEN 1 ELSE 0 END) AS approved_claims,
        ROUND(SUM(CASE WHEN fc.status = 'Approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS approval_rate_pct
    FROM fact_claims fc
    JOIN dim_providers dp ON fc.provider_id = dp.provider_id
    GROUP BY dp.provider_id, dp.hospital_name, dp.tier, dp.accreditation
    HAVING COUNT(*) >= 50
)
SELECT *,
    DENSE_RANK() OVER (ORDER BY approval_rate_pct DESC) AS approval_rank,
    NTILE(4) OVER (ORDER BY approval_rate_pct DESC) AS performance_quartile
FROM hospital_performance
ORDER BY approval_rank LIMIT 20;


-- Q3: 3-Month Moving Average of Claim Amounts by Insurer
WITH monthly_claims AS (
    SELECT pol.insurer, d.year, d.month,
        SUM(fc.claim_amount) AS total_claim_amount,
        COUNT(*) AS claim_count,
        AVG(fc.claim_amount) AS avg_claim_amount
    FROM fact_claims fc
    JOIN dim_date d ON fc.date_key = d.date_key
    JOIN dim_policies pol ON fc.policy_id = pol.policy_id
    GROUP BY pol.insurer, d.year, d.month
)
SELECT insurer, year, month, total_claim_amount,
    ROUND(AVG(total_claim_amount) OVER (
        PARTITION BY insurer ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )) AS moving_avg_3month,
    ROUND(AVG(avg_claim_amount) OVER (
        PARTITION BY insurer ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )) AS moving_avg_per_claim
FROM monthly_claims
ORDER BY insurer, year, month;


-- Q4: Year-over-Year Growth Using LAG()
WITH yearly_metrics AS (
    SELECT pol.insurer, d.year,
        COUNT(*) AS total_claims,
        SUM(fc.claim_amount) AS total_amount,
        SUM(fc.approved_amount) AS total_paid,
        AVG(fc.settlement_days) AS avg_settlement_days
    FROM fact_claims fc
    JOIN dim_date d ON fc.date_key = d.date_key
    JOIN dim_policies pol ON fc.policy_id = pol.policy_id
    GROUP BY pol.insurer, d.year
)
SELECT insurer, year, total_claims, total_amount,
    LAG(total_claims) OVER (PARTITION BY insurer ORDER BY year) AS prev_year_claims,
    LAG(total_amount) OVER (PARTITION BY insurer ORDER BY year) AS prev_year_amount,
    ROUND((total_claims - LAG(total_claims) OVER (PARTITION BY insurer ORDER BY year)) * 100.0 
        / NULLIF(LAG(total_claims) OVER (PARTITION BY insurer ORDER BY year), 0), 2) AS yoy_claims_growth_pct,
    ROUND((total_amount - LAG(total_amount) OVER (PARTITION BY insurer ORDER BY year)) * 100.0 
        / NULLIF(LAG(total_amount) OVER (PARTITION BY insurer ORDER BY year), 0), 2) AS yoy_amount_growth_pct
FROM yearly_metrics
ORDER BY insurer, year;


-- Q5: Patient Claim Ranking Within Each State (ROW_NUMBER + % of Total)
WITH patient_totals AS (
    SELECT p.patient_id, p.age, p.gender, p.state, p.chronic_condition,
        COUNT(*) AS claim_count,
        SUM(fc.claim_amount) AS total_claimed,
        SUM(fc.approved_amount) AS total_approved
    FROM fact_claims fc
    JOIN dim_patients p ON fc.patient_id = p.patient_id
    GROUP BY p.patient_id, p.age, p.gender, p.state, p.chronic_condition
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY state ORDER BY total_claimed DESC) AS state_rank,
        ROUND(total_claimed * 100.0 / SUM(total_claimed) OVER (PARTITION BY state), 2) AS pct_of_state_claims,
        ROUND(AVG(total_claimed) OVER (PARTITION BY state)) AS state_avg_claim
    FROM patient_totals
)
SELECT * FROM ranked WHERE state_rank <= 5 ORDER BY state, state_rank;


-- Q6: Percentile Ranking of Settlement Days by Insurer
SELECT pol.insurer, fc.claim_id, fc.settlement_days,
    ROUND(PERCENT_RANK() OVER (PARTITION BY pol.insurer ORDER BY fc.settlement_days)::NUMERIC, 4) AS percentile_rank,
    ROUND(AVG(fc.settlement_days) OVER (PARTITION BY pol.insurer)::NUMERIC, 1) AS insurer_avg_days,
    MIN(fc.settlement_days) OVER (PARTITION BY pol.insurer) AS insurer_min_days,
    MAX(fc.settlement_days) OVER (PARTITION BY pol.insurer) AS insurer_max_days
FROM fact_claims fc
JOIN dim_policies pol ON fc.policy_id = pol.policy_id
WHERE fc.settlement_days IS NOT NULL
ORDER BY pol.insurer, fc.settlement_days;


-- Q7: First & Last Claim Per Patient (FIRST_VALUE / LAST_VALUE)
SELECT DISTINCT fc.patient_id,
    FIRST_VALUE(fc.claim_id) OVER w AS first_claim_id,
    FIRST_VALUE(d.full_date) OVER w AS first_claim_date,
    FIRST_VALUE(fc.diagnosis_name) OVER w AS first_diagnosis,
    FIRST_VALUE(fc.claim_amount) OVER w AS first_claim_amount,
    LAST_VALUE(fc.claim_id) OVER (PARTITION BY fc.patient_id ORDER BY d.full_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_claim_id,
    LAST_VALUE(d.full_date) OVER (PARTITION BY fc.patient_id ORDER BY d.full_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_claim_date,
    COUNT(*) OVER (PARTITION BY fc.patient_id) AS total_claims
FROM fact_claims fc
JOIN dim_date d ON fc.date_key = d.date_key
WINDOW w AS (PARTITION BY fc.patient_id ORDER BY d.full_date)
ORDER BY total_claims DESC LIMIT 50;


