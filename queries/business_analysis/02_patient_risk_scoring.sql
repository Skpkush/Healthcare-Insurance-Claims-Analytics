-- ============================================================
-- Query  : Patient Risk Scoring Model (5-Factor Weighted)
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : CTE, Multi-factor CASE scoring (0-100 scale)
-- Purpose: Assigns each patient a risk score based on age,
--          chronic condition, claim frequency, emergency visits,
--          and readmissions. Classifies as HIGH / MEDIUM / LOW RISK.
-- Score breakdown: Age(20) + Chronic(25) + Frequency(20) + Emergency(15) + Readmission(20)
-- ============================================================

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
           WHEN total_claims > 5  THEN 10 ELSE 5 END
    + CASE WHEN emergency_count > 3 THEN 15 WHEN emergency_count > 1 THEN 10 ELSE 0 END
    + CASE WHEN readmission_count > 2 THEN 20 WHEN readmission_count > 0 THEN 10 ELSE 0 END
    ) AS risk_score,
    CASE WHEN (CASE WHEN age > 60 THEN 20 ELSE ROUND(age * 20.0 / 60) END
        + CASE WHEN chronic_condition IN ('Diabetes+Hypertension','Heart Disease','COPD') THEN 25
               WHEN chronic_condition IN ('Diabetes','Hypertension','Obesity') THEN 15
               WHEN chronic_condition = 'None' THEN 0 ELSE 10 END
        + CASE WHEN total_claims > 15 THEN 20 WHEN total_claims > 10 THEN 15
               WHEN total_claims > 5  THEN 10 ELSE 5 END
        + CASE WHEN emergency_count > 3 THEN 15 WHEN emergency_count > 1 THEN 10 ELSE 0 END
        + CASE WHEN readmission_count > 2 THEN 20 WHEN readmission_count > 0 THEN 10 ELSE 0 END
    ) >= 65 THEN 'HIGH RISK'
    WHEN (CASE WHEN age > 60 THEN 20 ELSE ROUND(age * 20.0 / 60) END
        + CASE WHEN chronic_condition IN ('Diabetes+Hypertension','Heart Disease','COPD') THEN 25
               WHEN chronic_condition IN ('Diabetes','Hypertension','Obesity') THEN 15
               WHEN chronic_condition = 'None' THEN 0 ELSE 10 END
        + CASE WHEN total_claims > 15 THEN 20 WHEN total_claims > 10 THEN 15
               WHEN total_claims > 5  THEN 10 ELSE 5 END
        + CASE WHEN emergency_count > 3 THEN 15 WHEN emergency_count > 1 THEN 10 ELSE 0 END
        + CASE WHEN readmission_count > 2 THEN 20 WHEN readmission_count > 0 THEN 10 ELSE 0 END
    ) >= 40 THEN 'MEDIUM RISK'
    ELSE 'LOW RISK' END AS risk_category
FROM patient_metrics ORDER BY risk_score DESC LIMIT 100;
