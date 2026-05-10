# Healthcare Provider Fraud Detection — Model Card

## Models trained

Four variants on `mv_provider_features` (5,410 providers, 9.35% fraud-flagged), 80/20 stratified split, random_state=42.

| Model | AUC-ROC | Avg Precision | Recall (fraud) | Precision (fraud) | F1 (fraud) |
|---|---:|---:|---:|---:|---:|
| **LR (balanced)** | **0.9573** | 0.7542 | **0.9010** | 0.4044 | 0.5583 |
| LR (SMOTE) | 0.9570 | **0.7576** | 0.9010 | 0.4027 | 0.5566 |
| Random Forest | 0.9538 | 0.7342 | 0.7822 | 0.5374 | **0.6371** |
| XGBoost | 0.9404 | 0.7122 | 0.6535 | 0.5690 | 0.6083 |

## Production recommendation: **LR (balanced)**

For fraud-detection use cases where the cost of a missed fraudster ≫ cost of a false positive (typical in healthcare), **recall on the fraud class is the metric that matters**. Logistic Regression with `class_weight='balanced'` catches **91 of 101 fraud-flagged providers (90.1% recall)** at the default 0.5 threshold, vs Random Forest 78% and XGBoost 65%. It also has the highest AUC-ROC (0.957).

The 40% precision means about 60% of LR's positive predictions are false alarms — but that's a workload of 225 providers/year flagged for review out of 5,410, of which 91 are actual fraud. A human review team can absorb 134 false positives to catch 91 true fraudsters. A fraud detector that misses 35% of fraud (XGBoost) cannot be made up for downstream.

If precision is the binding constraint (e.g., automated punitive action, no human review), Random Forest at 54% precision / 78% recall is the better balance.

## Two artifacts saved

| File | Model | Use case |
|---|---|---|
| [`ml/models/lr_balanced_fraud_detector_v1.joblib`](../models/lr_balanced_fraud_detector_v1.joblib) | LogisticRegression (balanced) + StandardScaler | **Recommended.** Recall-optimized; pair with human review queue |
| [`ml/models/xgb_fraud_detector_v1.joblib`](../models/xgb_fraud_detector_v1.joblib) | XGBClassifier (`scale_pos_weight=9.69`) | Higher precision; SHAP-explainable |

The LR artifact bundles the `StandardScaler` because LR requires scaled inputs. XGBoost does not.

## Top features by SHAP (XGBoost, mean |SHAP value|)

| Rank | Feature | Mean |SHAP| |
|---|---|---:|
| 1 | total_reimbursed | 2.572 |
| 2 | claims_per_patient | 0.654 |
| 3 | median_claim | 0.418 |
| 4 | max_claim_amount | 0.330 |
| 5 | avg_patient_risk_score | 0.315 |
| 6 | stddev_claim_amount | 0.306 |
| 7 | avg_claim_amount | 0.303 |
| 8 | avg_diagnoses_per_claim | 0.280 |
| 9 | pct_high_risk_patients | 0.272 |
| 10 | claims_with_9plus_diagnoses | 0.258 |

`total_reimbursed` dominates — fraud is concentrated in providers who bill a lot, consistent with Day 5 finding (53% of all dollars come from 9.35% of providers).

Plots: [`ml/reports/shap_global_importance.png`](shap_global_importance.png), [`ml/reports/shap_beeswarm.png`](shap_beeswarm.png), [`ml/reports/roc_curves.png`](roc_curves.png).

## Falsifiable hypothesis test — Day 6 risk features

Hypothesis: the patient-level risk-score features (`avg_patient_risk_score`, `pct_high_risk_patients`) add lift to fraud detection.

| Variant | AUC-ROC |
|---|---:|
| XGBoost with risk features | 0.9404 |
| XGBoost without risk features | 0.9431 |
| **Lift** | **−0.0027** |

**HYPOTHESIS REJECTED.** Risk features slightly *hurt* fraud detection AUC. The Day 6 risk score captures patient-level cost/complexity, which is orthogonal to (and slightly noisier than) provider-level fraud signal. SHAP shows the risk features have non-zero importance — they're not random — but they're correlated with stronger features (`total_reimbursed`, `claims_per_patient`) that the model picks up directly. Including them adds variance without adding signal.

This is exactly the kind of result the Day 6 V3 note anticipated: the risk score is a useful patient-stratification tool, not a fraud-detection feature. Day 6 conclusion stands.

## Honest limitations

1. **Provider-level prediction.** Can't catch within-provider fraud (a single rogue physician at an otherwise legitimate provider). Fraud must be visible in aggregated provider-level statistics.
2. **Labels of unknown provenance.** The Kaggle `PotentialFraud` flag is anonymized; we don't know exactly how it was derived. Models may be learning to reproduce whatever rule generated those labels.
3. **9.35% base rate.** May not generalize to other provider populations. CMS-wide actual fraud rate is estimated at 3-10%; our test set matches the upper bound.
4. **No temporal validation.** Train/test split is random, not time-ordered. A production model should be validated on a held-out time window.
5. **SHAP importance is single-model.** XGBoost's tree-split importance differs from LR's coefficient ordering. Both agree `total_reimbursed` dominates; minor features may swap orders between models.
6. **`total_reimbursed` is dominant — close to circular.** If "fraud-flagged" was operationalized partly via billing volume (likely), the model is partly learning the labeling rule, not independent signal. The remaining features still add lift, but headline AUC is partly mechanical.

## Intended use

- Portfolio demonstration of end-to-end ML workflow.
- **NOT for production fraud detection without further validation** — specifically: temporal hold-out test, calibration curve check, threshold tuning to operational cost ratios, and re-derivation of labels from primary source.
- Decent starting point for a production v2: keep LR-balanced as the screen, layer human review, monitor drift in feature distributions monthly.
