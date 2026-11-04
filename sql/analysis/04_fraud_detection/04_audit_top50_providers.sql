-- Audit Target List: top 50 providers by reimbursed dollars
-- Business question: if the audit team can only open 50 files,
-- which 50, and how much of the flagged dollars do they cover?
-- Ranks on billing alone, so the fraud label is an outcome here, not an input.

WITH all_claims AS (
    SELECT provider_id, claim_amount_reimbursed FROM fact_inpatient_claims
    UNION ALL
    SELECT provider_id, claim_amount_reimbursed FROM fact_outpatient_claims
),
provider_billing AS (
    SELECT
        ac.provider_id,
        p.is_potentially_fraudulent,
        COUNT(*) AS total_claims,
        SUM(ac.claim_amount_reimbursed) AS total_reimbursed
    FROM all_claims ac
    JOIN dim_provider p ON p.provider_id = ac.provider_id
    GROUP BY ac.provider_id, p.is_potentially_fraudulent
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY total_reimbursed DESC, provider_id) AS audit_rank,
        SUM(CASE WHEN is_potentially_fraudulent THEN total_reimbursed ELSE 0 END)
            OVER () AS flagged_usd,
        SUM(CASE WHEN is_potentially_fraudulent THEN total_reimbursed ELSE 0 END)
            OVER (ORDER BY total_reimbursed DESC, provider_id) AS cumulative_flagged_usd
    FROM provider_billing
)
SELECT
    audit_rank,
    provider_id,
    is_potentially_fraudulent,
    total_claims,
    ROUND(total_reimbursed::numeric, 0) AS total_reimbursed,
    ROUND(100.0 * cumulative_flagged_usd::numeric / flagged_usd::numeric, 2) AS cumulative_pct_of_flagged_usd
FROM ranked
WHERE audit_rank <= 50
ORDER BY audit_rank;
