-- ============================================================
-- Query  : Correlated Subquery — Claims Above 2.5x Provider Average
-- Author : Sumit Prajapat
-- Project: Healthcare Insurance Claims Analytics
-- Tech   : Correlated Subquery (scalar), deviation calculation
-- Purpose: Finds claims where the amount exceeds 2.5x the average
--          for that specific hospital. Flags potential overbilling
--          or unusually expensive procedures at a given provider.
-- ============================================================

SELECT fc.claim_id, fc.patient_id, fc.provider_id, dp.hospital_name,
    fc.diagnosis_name, fc.claim_amount,
    (SELECT ROUND(AVG(fc2.claim_amount)::NUMERIC)
     FROM fact_claims fc2
     WHERE fc2.provider_id = fc.provider_id) AS provider_avg_claim,
    fc.claim_amount - (SELECT AVG(fc2.claim_amount)
     FROM fact_claims fc2
     WHERE fc2.provider_id = fc.provider_id) AS deviation_from_avg,
    fc.status
FROM fact_claims fc
JOIN dim_providers dp ON fc.provider_id = dp.provider_id
WHERE fc.claim_amount > (
    SELECT AVG(fc2.claim_amount) * 2.5
    FROM fact_claims fc2
    WHERE fc2.provider_id = fc.provider_id
)
ORDER BY deviation_from_avg DESC LIMIT 50;
