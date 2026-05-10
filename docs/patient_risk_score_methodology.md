# Patient Risk Score Model — V2 (Real Data)

## Purpose
Stratify Medicare beneficiaries by likely future cost and complexity,
using observable features available at point-of-care.

## Methodology — 5 Factors

| Factor | Weight | Rationale |
|---|---|---|
| Age | 35% | Strongest single predictor of healthcare cost in actuarial literature |
| Chronic Condition Count | 25% | Each additional chronic condition adds ~$1,500 average annual cost (per Medicare data) |
| Claim Frequency (annual) | 20% | High utilizers consume disproportionate resources |
| Total Annual Claim Amount | 15% | Past spend predicts future spend (regression to mean considered but persistent) |
| Renal Disease Indicator | 5% | Specific high-cost driver; CMS has separate ESRD-related coverage |

## Score Range
0.0 (lowest risk) to 1.0 (highest risk), each factor normalized to [0,1] before weighting.

## Risk Tiers
- **Low**: 0.00 - 0.33 — routine monitoring
- **Medium**: 0.34 - 0.66 — quarterly review
- **High**: 0.67 - 1.00 — case management, care coordination

## V1 → V2 Differences
- V1 used synthetic data with arbitrary chronic condition flags
- V2 uses real Kaggle Medicare data with 11 clinically meaningful chronic conditions
- V2 will be VALIDATED against actual claim costs (V1 was not)
