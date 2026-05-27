# 🏥 Healthcare Provider Fraud Detection V2

<div align="center">

![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Azure](https://img.shields.io/badge/Azure%20ADF-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-F7931E?style=for-the-badge&logo=scikit-learn&logoColor=white)

**An end-to-end fraud detection analytics solution on real Medicare claims data**

[📊 Live Dashboard](https://app.powerbi.com/links/wxcRR41LJv?ctid=ed9d315f-9e92-4ac9-a8e7-1d8fe09d45dd&pbi_source=linkShare&bookmarkGuid=4e718836-a58d-4911-a649-6d1e03ea7ee4) · [🗄️ SQL Schema](#database-schema) · [🤖 ML Model](#ml-model) · [📁 Repository](https://github.com/Skpkush/Healthcare-Insurance-Claims-Analytics)

</div>

---

## 📌 Project Summary

This project builds a **production-grade fraud detection pipeline** on the [Kaggle Healthcare Provider Fraud Detection dataset](https://www.kaggle.com/datasets/rohitrox/healthcare-provider-fraud-detection-analysis), covering **558,211 Medicare claims** across **5,410 providers** from 2008–2010.

> **Core finding: Q4 billing providers (top 25% by total billing) are 226× more likely to be fraudulent than Q1 providers — 34.02% fraud rate vs 0.15%.**

The pipeline spans data ingestion → PostgreSQL schema → Azure ADF orchestration → Python ML → Power BI 4-page interactive dashboard.

---

## 🔑 Key Findings

| Finding | Metric | Insight |
|---|---|---|
| **226× Billing Concentration** | Q1: 0.15% → Q4: 34.02% | Top billing quartile drives nearly all fraud |
| **Velocity Signal** | 96.08% accuracy | 96% of high-velocity providers (>10 claims/day) are fraud-flagged |
| **Diagnosis Stuffing** | 1.66× ratio | Fraud providers file 9.92% vs 5.99% claims with 9+ diagnosis codes |
| **ML Model Recall** | 90.1% | Logistic Regression catches 9 out of 10 fraud providers |
| **Cohort Cost Gradient** | 18.8× | Very High cohort costs $12,306 vs Healthy cohort $690 avg |
| **Fraud Exposure** | $295.68M | 53.1% of total $556.54M reimbursed linked to fraud providers |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA PIPELINE                                │
│                                                                     │
│  Kaggle CSV  →  Azure Blob  →  Azure ADF  →  PostgreSQL  →  Views  │
│  (558K rows)    (Raw zone)     (Pipeline)    (9 tables)   (ML feats)│
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│                      ANALYTICS LAYER                                │
│                                                                     │
│   Python (scikit-learn)    →    Power BI (4 pages)                  │
│   LR + XGBoost + RF             Executive | Operations |            │
│   AUC 0.9573 | Recall 90.1%     Fraud Detection | Provider Detail   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Dashboard — 4 Pages

### [🔗 View Live Dashboard](https://app.powerbi.com/links/wxcRR41LJv?ctid=ed9d315f-9e92-4ac9-a8e7-1d8fe09d45dd&pbi_source=linkShare&bookmarkGuid=4e718836-a58d-4911-a649-6d1e03ea7ee4)

| Page | Purpose | Key Visuals |
|---|---|---|
| **Page 1 — Executive** | C-suite portfolio overview | 6 KPI cards, combo trend chart, decomposition tree, key influencers, gauge |
| **Page 2 — Operations** | State/cohort operational view | US fraud map, provider scatter, cohort heatmap, waterfall, top 10 diagnosis |
| **Page 3 — Fraud Detection** | 226× hero analysis + ML results | Quartile bar chart, ML cards, velocity scatter, diagnosis stuffing, investigation queue |
| **Page 4 — Provider Detail** | Single-provider drill-through | Billing trend, risk flags, vs portfolio comparison, investigation verdict |

---

## 🗄️ Database Schema

**PostgreSQL — 9 tables, 2.43M total rows**

```
dim_beneficiary          (138,556 rows)  — Patient demographics + chronic conditions
dim_provider             (  5,410 rows)  — Provider fraud labels
dim_date                 (  1,096 rows)  — Date dimension
dim_diagnosis_code       (  2,213 rows)  — ICD-9 codes
fact_inpatient_claims    ( 40,474 rows)  — Inpatient claim records
fact_outpatient_claims   (517,737 rows)  — Outpatient claim records
bridge_claim_diagnosis   (1,710,000 rows) — Claim ↔ diagnosis mapping
v_provider_features      (  5,410 rows)  — ML feature view (28 engineered features)
v_patient_risk_score     (138,556 rows)  — Patient risk scoring view
```

**Key engineered features (v_provider_features):**
- `avg_daily_claims` — velocity signal
- `claims_with_9plus_diagnoses` — stuffing detection
- `avg_patient_chronic_count` — patient complexity proxy
- `coefficient_of_variation` — billing inconsistency
- `peak_daily_claims` — outlier velocity

---

## 🤖 ML Model

**Task**: Binary classification — predict `is_potentially_fraudulent` (provider level)

**Training set**: 4,328 providers | **Test set**: 1,082 providers | **Base fraud rate**: 9.35%

| Model | AUC | Recall (Fraud) | Precision (Fraud) | F1 |
|---|---|---|---|---|
| **Logistic Regression (balanced)** | **0.9573** | **90.1%** | **40.4%** | **0.558** |
| XGBoost | 0.9089 | 65.4% | 58.1% | 0.615 |
| Random Forest | 0.8934 | 61.2% | 55.3% | 0.581 |

> **Why LR won**: In fraud detection, recall is priority — missing a fraudulent provider costs more than a false positive. LR with `class_weight='balanced'` achieves 90.1% recall vs XGBoost's 65.4%, catching 26 more fraud providers per review cycle.

**Investigation Queue**: 225 providers flagged (91 TP + 134 FP) per review cycle.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Data Ingestion** | Azure Data Factory (ADF) pipeline |
| **Storage** | Azure Blob Storage (raw zone) |
| **Database** | PostgreSQL 15 (Aiven cloud) |
| **Feature Engineering** | PostgreSQL views + window functions |
| **ML** | Python — scikit-learn, pandas, numpy, matplotlib |
| **Visualization** | Microsoft Power BI Desktop + Service |
| **Version Control** | Git + GitHub (branch: v2-real-data) |

---

## 📁 Repository Structure

```
Healthcare-Insurance-Claims-Analytics/
│
├── data/
│   └── raw/                    # Kaggle source CSVs (gitignored)
│
├── sql/
│   ├── schema/                 # CREATE TABLE statements
│   ├── views/                  # v_provider_features, v_patient_risk_score
│   └── analysis/               # 226x finding queries
│
├── python/
│   ├── 01_eda.ipynb            # Exploratory analysis
│   ├── 02_feature_engineering.ipynb
│   ├── 03_ml_models.ipynb      # LR + XGBoost + RF comparison
│   └── 04_shap_analysis.ipynb  # Feature importance
│
├── adf/
│   └── pipeline/               # Azure ADF pipeline JSON
│
├── dashboard/
│   └── healthcare_v2_main.pbix # Power BI dashboard (4 pages)
│
└── README.md
```

---

## 🚀 How to Run

### Prerequisites
```bash
pip install pandas numpy scikit-learn matplotlib seaborn psycopg2
```

### 1. Database Setup
```sql
-- Run in order:
-- sql/schema/01_create_tables.sql
-- sql/schema/02_create_views.sql
```

### 2. Load Data
```bash
# Via Azure ADF pipeline or direct psql:
psql -h <host> -U <user> -d healthcare_v2 -f sql/schema/01_create_tables.sql
```

### 3. Run ML Pipeline
```bash
jupyter notebook python/03_ml_models.ipynb
```

### 4. Open Dashboard
```
Open dashboard/healthcare_v2_main.pbix in Power BI Desktop
OR view live: https://app.powerbi.com/links/wxcRR41LJv?ctid=ed9d315f-9e92-4ac9-a8e7-1d8fe09d45dd&pbi_source=linkShare&bookmarkGuid=4e718836-a58d-4911-a649-6d1e03ea7ee4
```

---

## 💡 DAX Highlights

```dax
-- 226x Fraud Concentration
Quartile Fraud Rate =
DIVIDE(
    CALCULATE(DISTINCTCOUNT('public v_provider_features'[provider_id]),
        'public v_provider_features'[label] = 1),
    DISTINCTCOUNT('public v_provider_features'[provider_id]), 0)

-- Composite Risk Flag (6-level)
Risk Flag =
VAR IsQ4 = 'public v_provider_features'[Billing Quartile] = "Q4 (Highest)"
VAR IsVelocity = 'public v_provider_features'[peak_daily_claims] > 10
VAR IsFraud = 'public v_provider_features'[label] = 1
RETURN
SWITCH(TRUE(),
    IsFraud && IsQ4 && IsVelocity, "CRITICAL",
    IsFraud && IsQ4,               "HIGH RISK",
    IsFraud && IsVelocity,         "FRAUD + VELOCITY",
    IsFraud,                       "FRAUD FLAGGED",
    IsQ4 && IsVelocity,            "MONITOR CLOSELY",
    IsQ4,                          "ELEVATED",
                                   "NORMAL")
```

---

## 📬 Contact

**Sumit Kumar Prajapat**
- 🔗 GitHub: [github.com/Skpkush](https://github.com/Skpkush)
- 💼 LinkedIn: [linkedin.com/in/sumit-kumar-prajapat](https://linkedin.com/in/sumit-kumar-prajapat)
- 📧 Certifications: PL-300 | AZ-900 | DP-900 | AWS Cloud Practitioner

---

<div align="center">

**⭐ Star this repo if you found it useful!**

*Built on real Medicare data | End-to-end pipeline | Production-grade dashboard*

</div>