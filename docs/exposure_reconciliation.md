# Fraud Exposure Reconciliation

## The discrepancy

The Power BI Executive page showed **440 fraud providers (8.13%)** beside **$295.68M exposure**. The SQL layer and README showed **506 providers (9.35%)** for the same $295.68M. Those figures came from two different claim scopes.

| Claim scope | Flagged providers | Reimbursed |
|---|---:|---:|
| Inpatient only | 440 | $241,288,510 |
| Outpatient only | 462 | $54,392,610 |
| **All claims (reported figure)** | **506** | **$295,681,120** |

Inpatient and outpatient provider counts overlap, so they do not add up to 506. Dollars do add up, because each claim belongs to one table.

## Root cause

`Fraud Provider Count` counted distinct providers from `fact_inpatient_claims` only, while `Fraud Dollar Amount` summed both fact tables. The 66 flagged providers with outpatient claims only were left out of the count but kept in the dollars. `Fraud Rate %` inherited the same error.

## Fix

`Fraud Provider Count` now counts distinct flagged providers across both fact tables and still responds to slicers. Queried from the model after the change:

| Measure | Before | After |
|---|---:|---:|
| Fraud Provider Count | 440 | **506** |
| Fraud Rate % | 8.13% | **9.35%** |
| Fraud Dollar Amount | $295.68M | $295.68M |
| Fraud Dollar % of Total | 53.1% | 53.1% |

The dollar figure was always correct. The provider count was wrong. A check is in [`04_exposure_reconciliation.sql`](../sql/analysis/03_business_metrics/04_exposure_reconciliation.sql), and all figures were recomputed from the raw Kaggle CSVs.

## What $295.68M means

$295.68M is **all money reimbursed to providers carrying the `PotentialFraud` label**. It is gross exposure, not proven fraud loss. The dataset does not say which claims at those providers were fraudulent, so it should be quoted as "dollars at flagged providers".

## Still open in the dashboard

These sit in the report layer, so they need a manual edit in Power BI Desktop:

- The saved screenshots still show 440 and 8.13% until they are retaken.
- The Fraud Detection card titled **High Risk Providers** shows 20K, which is a count of high-risk *patients*. There are only 5,410 providers.
- The velocity note says 49 total. The correct reading is 49 flagged out of 51 outliers, which is the 96.08%.
- The **Fraud Rate vs Target** gauge now reads 9.35% against the 8.00% target.
