-- Top Diagnoses by Volume (with fraud overlay)
-- What conditions drive the most claims? Are fraud-flagged providers concentrated on specific diagnoses?

SELECT
    bcd.diagnosis_code,
    COUNT(*) AS claim_appearances,
    COUNT(DISTINCT bcd.claim_id) AS unique_claims,
    SUM(CASE WHEN p.is_potentially_fraudulent THEN 1 ELSE 0 END) AS appearances_in_fraud_flagged_claims,
    ROUND(100.0 * SUM(CASE WHEN p.is_potentially_fraudulent THEN 1 ELSE 0 END) / COUNT(*), 2) AS fraud_concentration_pct
FROM bridge_claim_diagnosis bcd
LEFT JOIN fact_inpatient_claims fi ON fi.claim_id = bcd.claim_id
LEFT JOIN fact_outpatient_claims fo ON fo.claim_id = bcd.claim_id
LEFT JOIN dim_provider p ON p.provider_id = COALESCE(fi.provider_id, fo.provider_id)
GROUP BY bcd.diagnosis_code
ORDER BY claim_appearances DESC
LIMIT 20;
