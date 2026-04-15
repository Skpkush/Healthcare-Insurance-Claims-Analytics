-- ============================================================
-- Query  : Patient Claim Ranking Within Each State
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : Window Functions — ROW_NUMBER, SUM OVER (PARTITION),
--          AVG OVER (PARTITION)
-- Purpose: Ranks top 5 highest-claiming patients within each state.
--          Shows each patient's % share of their state's total claims
--          and compares them against the state average.
-- ============================================================

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
