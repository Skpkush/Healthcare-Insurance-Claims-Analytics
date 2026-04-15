-- ============================================================
-- Query  : Patient Claim Escalation Pattern Detection
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : CTE, ROW_NUMBER, Self-JOIN on sequential claims
-- Purpose: Detects patients whose consecutive claim amounts are
--          strictly increasing (3 in a row) — potential indicator
--          of staged fraud or worsening chronic conditions.
--          Returns patients with 2+ escalation sequences.
-- ============================================================

WITH numbered_claims AS (
    SELECT fc.patient_id, fc.claim_id, d.full_date, fc.claim_amount, fc.diagnosis_name,
        ROW_NUMBER() OVER (PARTITION BY fc.patient_id ORDER BY d.full_date) AS claim_seq
    FROM fact_claims fc JOIN dim_date d ON fc.date_key = d.date_key
),
escalation_check AS (
    SELECT c1.patient_id, c1.claim_seq,
        c1.claim_amount AS current_amount,
        c2.claim_amount AS next_amount,
        c3.claim_amount AS third_amount,
        CASE WHEN c2.claim_amount > c1.claim_amount
              AND c3.claim_amount > c2.claim_amount
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
