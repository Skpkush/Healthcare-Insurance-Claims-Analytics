"""Build the audit target list and the threshold analysis.

Outputs (ml/reports/):
  audit_top50_providers.csv   ranked list for the audit team
  audit_coverage_curve.csv    list size vs share of flagged dollars
  threshold_analysis.csv      recall/precision/queue at each operating point
  metrics.json                every figure quoted in the README

Run: python ml/scripts/audit_targeting.py
"""
import json
import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import StratifiedKFold, train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from provider_features import AMT, ROOT, build_features, load_claims

warnings.filterwarnings("ignore")
OUT = ROOT / "ml" / "reports"
MODELS = ROOT / "ml" / "models"
SEED = 42
TARGET_RECALL = 0.90


def make_xgb(y):
    # Same hyperparameters as the v1 notebook.
    return xgb.XGBClassifier(
        n_estimators=200, max_depth=6, learning_rate=0.1,
        scale_pos_weight=(y == 0).sum() / (y == 1).sum(),
        eval_metric="logloss", random_state=SEED, n_jobs=-1,
    )


def make_lr(_y=None):
    return make_pipeline(
        StandardScaler(),
        LogisticRegression(class_weight="balanced", max_iter=1000, random_state=SEED),
    )


def out_of_fold(make_model, X, y, folds=5):
    """Score each row with a model that never saw it."""
    scores = pd.Series(0.0, index=X.index)
    for tr, te in StratifiedKFold(folds, shuffle=True, random_state=SEED).split(X, y):
        model = make_model(y.iloc[tr]).fit(X.iloc[tr], y.iloc[tr])
        scores.iloc[te] = model.predict_proba(X.iloc[te])[:, 1]
    return scores


def threshold_for_recall(scores, y, target):
    """Highest threshold that still reaches the target recall."""
    fraud = np.sort(scores[y == 1].to_numpy())[::-1]
    return float(fraud[int(np.ceil(target * len(fraud))) - 1])


def operating_point(name, scores, y, threshold):
    flagged = scores >= threshold
    tp = int((flagged & (y == 1)).sum())
    return {
        "operating_point": name, "threshold": round(threshold, 4), "queue": int(flagged.sum()),
        "true_positives": tp, "false_positives": int(flagged.sum()) - tp,
        "recall": round(tp / int((y == 1).sum()), 4),
        "precision": round(tp / max(int(flagged.sum()), 1), 4),
    }


def reconcile(claims):
    flagged = claims[claims.label == 1]
    by_type = flagged.groupby("claim_type").agg(usd=(AMT, "sum"), providers=("Provider", "nunique"))
    return {
        "total_reimbursed": int(claims[AMT].sum()),
        "flagged_reimbursed": int(flagged[AMT].sum()),
        "flagged_share": round(flagged[AMT].sum() / claims[AMT].sum(), 4),
        "flagged_providers_all_claims": int(flagged.Provider.nunique()),
        "flagged_providers_inpatient": int(by_type.loc["inpatient", "providers"]),
        "flagged_providers_outpatient": int(by_type.loc["outpatient", "providers"]),
        "flagged_reimbursed_inpatient": int(by_type.loc["inpatient", "usd"]),
        "flagged_reimbursed_outpatient": int(by_type.loc["outpatient", "usd"]),
        "total_providers": int(claims.Provider.nunique()),
        "claim_date_min": str(pd.to_datetime(claims.ClaimStartDt).min().date()),
        "claim_date_max": str(pd.to_datetime(claims.ClaimStartDt).max().date()),
    }


def threshold_analysis(X, y, cols):
    """Thresholds come from train only; the test split just grades them."""
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, stratify=y, random_state=SEED)
    xgb_v1 = joblib.load(MODELS / "xgb_fraud_detector_v1.joblib")["model"]
    lr_art = joblib.load(MODELS / "lr_balanced_fraud_detector_v1.joblib")
    scaled = pd.DataFrame(lr_art["scaler"].transform(X_te), columns=cols, index=X_te.index)
    xgb_te = pd.Series(xgb_v1.predict_proba(X_te)[:, 1], index=X_te.index)
    lr_te = pd.Series(lr_art["model"].predict_proba(scaled)[:, 1], index=X_te.index)
    xgb_cv = out_of_fold(make_xgb, X_tr, y_tr)
    lr_cv = out_of_fold(make_lr, X_tr, y_tr)
    t_xgb = threshold_for_recall(xgb_cv, y_tr, TARGET_RECALL)
    t_lr = threshold_for_recall(lr_cv, y_tr, TARGET_RECALL)

    tuned = f"tuned for {TARGET_RECALL:.0%} recall on train CV"
    rows = [
        {"model": "LR balanced", **operating_point("default 0.5", lr_te, y_te, 0.5)},
        {"model": "XGBoost", **operating_point("default 0.5", xgb_te, y_te, 0.5)},
        {"model": "LR balanced", **operating_point(tuned, lr_te, y_te, t_lr)},
        {"model": "XGBoost", **operating_point(tuned, xgb_te, y_te, t_xgb)},
    ]
    # Same queue as LR at 0.5, so both models get the same audit workload.
    queue = rows[0]["queue"]
    for name, s in (("LR balanced", lr_te), ("XGBoost", xgb_te)):
        cut = float(s.nlargest(queue).min())
        rows.append({"model": name, **operating_point(f"fixed queue of {queue}", s, y_te, cut)})

    missed = xgb_te[(y_te == 1) & (xgb_te < 0.5)]
    diagnosis = {
        "xgb_auc_train": round(roc_auc_score(y_tr, xgb_v1.predict_proba(X_tr)[:, 1]), 4),
        "xgb_auc_test": round(roc_auc_score(y_te, xgb_te), 4),
        "xgb_auc_train_cv": round(roc_auc_score(y_tr, xgb_cv), 4),
        "lr_auc_test": round(roc_auc_score(y_te, lr_te), 4),
        "xgb_ap_test": round(average_precision_score(y_te, xgb_te), 4),
        "lr_ap_test": round(average_precision_score(y_te, lr_te), 4),
        "xgb_missed_at_05": len(missed),
        "xgb_missed_median_score": round(float(missed.median()), 4),
        "xgb_missed_above_tuned_threshold": int((missed >= t_xgb).sum()),
        "xgb_fraud_median_score": round(float(xgb_te[y_te == 1].median()), 4),
        "lr_fraud_median_score": round(float(lr_te[y_te == 1].median()), 4),
        "xgb_threshold_tuned": round(t_xgb, 4),
        "lr_threshold_tuned": round(t_lr, 4),
    }
    return pd.DataFrame(rows), diagnosis


def audit_list(df, X, y):
    """Every provider gets a score from a model that never saw that provider."""
    t = df.assign(score_lr=out_of_fold(make_lr, X, y), score_xgb=out_of_fold(make_xgb, X, y))
    t["expected_usd"] = t.score_lr * t.total_reimbursed
    flagged_usd = t.loc[t.label == 1, "total_reimbursed"].sum()

    def coverage(frame):
        return round(frame.loc[frame.label == 1, "total_reimbursed"].sum() / flagged_usd, 4)

    strategies = {}
    for name, col in (("billing_only", "total_reimbursed"), ("lr_score", "score_lr"),
                      ("xgb_score", "score_xgb"), ("lr_score_x_billing", "expected_usd")):
        top = t.nlargest(50, col)
        strategies[name] = {"flagged_in_top50": int(top.label.sum()),
                            "coverage_of_flagged_usd": coverage(top)}

    ranked = t.sort_values("total_reimbursed", ascending=False)
    curve = []
    for n in (10, 25, 50, 100, 200, 506):
        top = ranked.head(n)
        curve.append({
            "list_size": n, "pct_of_providers": round(n / len(t), 4),
            "flagged_in_list": int(top.label.sum()), "reimbursed_usd": int(top.total_reimbursed.sum()),
            "coverage_of_flagged_usd": coverage(top),
        })

    top50 = ranked.head(50).reset_index().rename(columns={"Provider": "provider_id"})
    top50.insert(0, "audit_rank", range(1, 51))
    top50["share_of_flagged_usd"] = (top50.total_reimbursed / flagged_usd).round(4)
    top50["cumulative_share"] = (top50.total_reimbursed.cumsum() / flagged_usd).round(4)
    top50["flagged_potential_fraud"] = top50.label.map({1: "Yes", 0: "No"})
    top50["velocity_flag"] = np.where(top50.peak_daily_claims > 10, "Yes", "No")
    top50 = top50[[
        "audit_rank", "provider_id", "total_reimbursed", "share_of_flagged_usd", "cumulative_share",
        "flagged_potential_fraud", "score_lr", "score_xgb", "total_claims", "unique_patients",
        "claims_per_patient", "inpatient_ratio", "peak_daily_claims", "velocity_flag",
        "claims_with_9plus_diagnoses",
    ]].round({"score_lr": 4, "score_xgb": 4})
    return top50, curve, strategies, operating_point("LR 0.5, all providers", t.score_lr, y, 0.5)


def main():
    claims, _ = load_claims()
    df = build_features(claims)
    cols = joblib.load(MODELS / "xgb_fraud_detector_v1.joblib")["feature_cols"]
    X, y = df[cols], df["label"]

    thresholds, diagnosis = threshold_analysis(X, y, cols)
    top50, curve, strategies, full_queue = audit_list(df, X, y)

    thresholds.to_csv(OUT / "threshold_analysis.csv", index=False)
    top50.to_csv(OUT / "audit_top50_providers.csv", index=False)
    pd.DataFrame(curve).to_csv(OUT / "audit_coverage_curve.csv", index=False)
    metrics = {
        "reconciliation": reconcile(claims), "threshold_diagnosis": diagnosis,
        "top50_strategies": strategies, "coverage_curve": curve, "full_portfolio_queue": full_queue,
    }
    (OUT / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
    print(json.dumps(metrics, indent=2))
    print(thresholds.to_string(index=False))


if __name__ == "__main__":
    main()
