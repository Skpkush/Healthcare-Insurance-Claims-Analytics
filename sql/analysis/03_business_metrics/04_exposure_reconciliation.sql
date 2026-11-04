-- Exposure Reconciliation: why the dashboard showed 440 providers next to $295.68M
-- 440 counts flagged providers with an inpatient claim. $295.68M covers all 506.
-- Each row pairs a provider count with the dollars from the same claim scope.

WITH all_claims AS (
    SELECT provider_id, claim_amount_reimbursed, 'Inpatient' AS claim_scope FROM fact_inpatient_claims
    UNION ALL
    SELECT provider_id, claim_amount_reimbursed, 'Outpatient' AS claim_scope FROM fact_outpatient_claims
),
flagged AS (
    SELECT ac.*
    FROM all_claims ac
    JOIN dim_provider p ON p.provider_id = ac.provider_id
    WHERE p.is_potentially_fraudulent
)
SELECT
    claim_scope,
    COUNT(DISTINCT provider_id) AS flagged_providers,
    ROUND(SUM(claim_amount_reimbursed)::numeric, 0) AS reimbursed_usd
FROM flagged
GROUP BY claim_scope
UNION ALL
SELECT
    'All claims (reported figure)',
    COUNT(DISTINCT provider_id),
    ROUND(SUM(claim_amount_reimbursed)::numeric, 0)
FROM flagged
ORDER BY reimbursed_usd;
