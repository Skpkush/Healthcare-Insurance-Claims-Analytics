"""Rebuild the provider feature matrix from the raw Kaggle CSVs, no database needed.

Pandas port of sql/analysis/05_risk_scoring/01_patient_risk_score_view.sql and
ml/features/01_provider_features.sql. Column names, order and rounding match
mv_provider_features so the saved v1 models can score this output directly.
"""
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
# data/raw also holds the old synthetic V1 CSVs, so look for the Kaggle train file.
RAW = next(d for d in (ROOT / "data" / "kaggle_raw", ROOT / "data" / "raw") if any(d.glob("Train-*.csv")))
AMT = "InscClaimAmtReimbursed"
CHRONIC = [
    "ChronicCond_Alzheimer", "ChronicCond_Heartfailure", "ChronicCond_KidneyDisease",
    "ChronicCond_Cancer", "ChronicCond_ObstrPulmonary", "ChronicCond_Depression",
    "ChronicCond_Diabetes", "ChronicCond_IschemicHeart", "ChronicCond_Osteoporasis",
    "ChronicCond_rheumatoidarthritis", "ChronicCond_stroke",
]
DIAG = [f"ClmDiagnosisCode_{i}" for i in range(1, 11)]


def _csv(prefix):
    return pd.read_csv(next(RAW.glob(f"{prefix}-*.csv")))


def load_claims():
    """All train claims with claim_type and the provider fraud label attached."""
    labels = _csv("Train")
    ip, op = _csv("Train_Inpatientdata"), _csv("Train_Outpatientdata")
    ip["claim_type"], op["claim_type"] = "inpatient", "outpatient"
    claims = pd.concat([ip, op], ignore_index=True).merge(labels, on="Provider", how="left")
    claims["label"] = (claims["PotentialFraud"] == "Yes").astype(int)
    return claims, labels


def _percent_rank(s):
    return (s.rank(method="min") - 1) / (len(s) - 1)


def patient_risk(claims):
    b = _csv("Train_Beneficiarydata")
    dob = pd.to_datetime(b["DOB"])
    anchor = pd.Timestamp("2010-12-31")
    b["age_years"] = anchor.year - dob.dt.year - (
        (dob.dt.month > anchor.month) | ((dob.dt.month == anchor.month) & (dob.dt.day > anchor.day))
    ).astype(int)
    b["chronic_count"] = (b[CHRONIC] == 1).sum(axis=1)
    agg = claims.groupby("BeneID")[AMT].agg(claim_count="size", total_claim_amount="sum")
    b = b.merge(agg, left_on="BeneID", right_index=True, how="left").fillna(
        {"claim_count": 0, "total_claim_amount": 0}
    )
    b["risk_score"] = (
        _percent_rank(b["age_years"]) * 0.35
        + _percent_rank(b["chronic_count"]) * 0.25
        + _percent_rank(b["claim_count"]) * 0.20
        + _percent_rank(b["total_claim_amount"]) * 0.15
        + (b["RenalDiseaseIndicator"] == "Y") * 0.05
    ).round(4)
    b["high_risk"] = (b["risk_score"] > 0.66).astype(int)
    return b[["BeneID", "risk_score", "high_risk", "chronic_count"]]


def build_features(claims=None):
    if claims is None:
        claims, _ = load_claims()
    labels = claims.groupby("Provider")["label"].first()
    g = claims.groupby("Provider")
    f = pd.DataFrame(index=labels.index.sort_values())
    f["label"] = labels
    f["total_claims"] = g.size()
    f["unique_patients"] = g["BeneID"].nunique()
    f["active_days"] = g["ClaimStartDt"].nunique()
    f["claims_per_patient"] = (f.total_claims / f.unique_patients).round(2)
    f["claims_per_active_day"] = (f.total_claims / f.active_days).round(2)
    f["inpatient_claims"] = claims[claims.claim_type == "inpatient"].groupby("Provider").size()
    f["inpatient_claims"] = f["inpatient_claims"].fillna(0)
    f["outpatient_claims"] = f.total_claims - f.inpatient_claims
    f["inpatient_ratio"] = (f.inpatient_claims / f.total_claims).round(4)
    f["total_reimbursed"] = g[AMT].sum()
    f["avg_claim_amount"] = g[AMT].mean().round(2)
    std = g[AMT].std()
    f["stddev_claim_amount"] = std.round(2)
    f["median_claim"] = g[AMT].median()
    f["max_claim_amount"] = g[AMT].max()
    f["coefficient_of_variation"] = (std / g[AMT].mean()).round(4)
    daily = claims.groupby(["Provider", "ClaimStartDt"]).size().groupby("Provider")
    f["peak_daily_claims"] = daily.max()
    f["avg_daily_claims"] = daily.mean().round(2)

    diag_count = claims[DIAG].notna().sum(axis=1).replace(0, np.nan)
    diag = pd.DataFrame({"Provider": claims["Provider"], "d": diag_count}).groupby("Provider")["d"]
    f["avg_diagnoses_per_claim"] = diag.mean().round(2)
    f["max_diagnoses_per_claim"] = diag.max()
    f["claims_with_9plus_diagnoses"] = diag.apply(lambda s: (s >= 9).sum())

    risk = claims[["Provider", "BeneID"]].merge(patient_risk(claims), on="BeneID").groupby("Provider")
    f["avg_patient_risk_score"] = risk["risk_score"].mean().round(4)
    f["pct_high_risk_patients"] = risk["high_risk"].mean().round(4)
    f["avg_patient_chronic_count"] = risk["chronic_count"].mean().round(2)
    f["distinct_attending_physicians"] = g["AttendingPhysician"].nunique()
    f["distinct_operating_physicians"] = g["OperatingPhysician"].nunique()

    # Same imputation as the training notebook.
    return f.astype(float).assign(label=lambda d: d.label.astype(int)).pipe(
        lambda d: d.fillna(d.median(numeric_only=True))
    )
