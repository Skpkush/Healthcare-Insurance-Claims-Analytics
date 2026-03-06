# Healthcare & Insurance Claims Analytics

## End-to-End Data Analytics Platform with Azure Cloud Pipeline

![Python](https://img.shields.io/badge/Python-3.12-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791)
![Power BI](https://img.shields.io/badge/Power%20BI-PL--300%20Certified-F2C811)
![Azure](https://img.shields.io/badge/Azure-Cloud%20Pipeline-0078D4)
![License](https://img.shields.io/badge/License-MIT-green)

<img width="1919" height="931" alt="Pipeline success" src="https://github.com/user-attachments/assets/0f618c0a-0dbf-4d04-b821-3bb5c7864e50" />

<img width="1902" height="764" alt="PIpeline building" src="https://github.com/user-attachments/assets/e3035e9e-f591-4663-85be-319c92e9924a" />

<img width="1362" height="783" alt="Executive Dashboard 1" src="https://github.com/user-attachments/assets/79e5f8ba-2029-46e7-acad-e152279203eb" />

<img width="1369" height="773" alt="Operartion Dashboard" src="https://github.com/user-attachments/assets/a74e4bf3-f955-41ed-8c2a-3bb9f38e13ba" />

<img width="1339" height="768" alt="Risk dasboard" src="https://github.com/user-attachments/assets/97eb0e6b-e76f-4ff3-b4e4-75c75a844175" />



---

## Project Overview

A complete data analytics platform analyzing **50,000 insurance claims** across **5,000 patients**, **200 hospitals**, and **8 insurers** over 3 years (2022-2024). Built with Python, PostgreSQL, Power BI, and deployed on **Microsoft Azure** with automated ETL pipeline.

### Key Findings
- **Loss Ratio: 55.76%** — profitable portfolio (under 60% threshold)
- **Cancer Treatment**: 5% of claims but **16.8% of total costs** — highest cost diagnosis
- **24 high-risk patients** flagged responsible for **₹9.34 Crore** in claims
- **₹19.4 Crore** in avoidable rejections due to Incomplete Documents — recoverable through process fix
- **Gujarat** is the highest claiming state at ₹140.99 Crore

---

## Architecture

```
LOCAL ENVIRONMENT                    AZURE CLOUD
================                    ===========

Python Script                       Azure Blob Storage
(generate_data.py)                  (raw-data container)
  ↓ generates                            ↓
6 CSV files ──── upload ────→    Azure Data Factory
(50K+ records)                   (pl_load_claims_data pipeline)
                                 Runs daily at 6:00 AM IST
  ↓ load locally                         ↓
                                 Azure PostgreSQL
PostgreSQL (local)               (claims-analytics-sumit)
  ↓                                      ↓
20 SQL Queries                   Power BI Dashboard
  ↓                              (3 pages + 2 hidden)
Power BI Dashboard
(healthcare_claims.pbix)
```

---

## Azure Cloud Pipeline

### Resource Group: `rg-claims-analytics`

| Azure Service | Resource Name | Purpose |
|---|---|---|
| Azure Database for PostgreSQL | claims-analytics-sumit | Cloud data warehouse |
| Azure Blob Storage | claimsanalyticssumit | Data lake for CSV files |
| Azure Data Factory | claims-analytics-adf | Automated ETL orchestration |

### Pipeline: `pl_load_claims_data`

5 sequential Copy Data activities loading dimension tables first, then fact table:

```
Copy_dim_date → Copy_dim_patients → Copy_dim_providers → Copy_dim_policies → Copy_fact_claims
```

**Pipeline Status: ✅ Succeeded**
<img width="1902" height="764" alt="PIpeline building" src="https://github.com/user-attachments/assets/9fb6cb25-63b4-40e7-b462-e461bd419fbe" />

<img width="1919" height="931" alt="Pipeline success" src="https://github.com/user-attachments/assets/240ca2d9-5619-478a-ab6f-59990eb26377" />



### Azure PostgreSQL Server

<img width="1912" height="933" alt="Azure Postgres" src="https://github.com/user-attachments/assets/7ccc11a6-7995-4f69-9c5b-3b576ac7fda5" />

### Azure Blob Storage Overview
<img width="1898" height="855" alt="Azure Blob storage" src="https://github.com/user-attachments/assets/2ea656aa-43cb-4743-9925-82c6bd6852d6" />

---

## Data Model — Star Schema

```
                    ┌──────────────┐
                    │   dim_date   │
                    │──────────────│
                    │ date_key (PK)│
                    │ full_date    │
                    │ year, month  │
                    │ quarter      │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────┴───────┐    ┌──────────────┐
│ dim_patients │    │ fact_claims  │    │dim_providers │
│──────────────│    │──────────────│    │──────────────│
│patient_id(PK)├────┤claim_id (PK) ├────┤provider_id(PK)│
│ age, gender  │    │ patient_id   │    │hospital_name │
│ city, state  │    │ provider_id  │    │ speciality   │
│chronic_cond  │    │ policy_id    │    │ tier, city   │
└──────────────┘    │ date_key     │    └──────────────┘
                    │ claim_amount │
┌──────────────┐    │approved_amt  │
│ dim_policies │    │ status       │
│──────────────│    │diagnosis_name│
│policy_id(PK) ├────┤settlement_day│
│ insurer      │    │is_emergency  │
│ plan_type    │    │is_readmission│
│ premium      │    └──────────────┘
└──────────────┘
```

| Table | Rows | Description |
|---|---|---|
| fact_claims | 50,000 | Insurance claim transactions |
| dim_date | 1,096 | Calendar dimension (2022-2024) |
| dim_patients | 5,000 | Patient demographics |
| dim_providers | 200 | Hospital information |
| dim_policies | 40 | Insurance policy details |

---

## SQL Analysis Highlights

### 20 Advanced Queries Covering:

**Window Functions:** ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, PERCENT_RANK, NTILE, Running Totals

**CTEs:** Multi-level Common Table Expressions for fraud detection and complex business logic

**Fraud Detection:** 90-day rolling window claim frequency analysis using `RANGE BETWEEN INTERVAL '90 days' PRECEDING AND CURRENT ROW`

**Z-Score Anomaly Detection:** Statistical outlier identification — claims with Z-score > 3 flagged as extreme anomalies

**Business Analysis:** Loss ratio trends, insurer benchmarking, hospital performance quartiles, rejection root cause analysis

### Sample Query — Fraud Detection
```sql
WITH claim_timeline AS (
    SELECT patient_id, claim_id, claim_date,
           COUNT(*) OVER (
               PARTITION BY patient_id 
               ORDER BY claim_date 
               RANGE BETWEEN INTERVAL '90 days' PRECEDING AND CURRENT ROW
           ) AS claims_90d
    FROM fact_claims
)
SELECT * FROM claim_timeline WHERE claims_90d >= 3;
```

---

## Power BI Dashboard

### 30+ DAX Measures across 6 folders:
- `_01_Base`: Total Claims, Total Claimed (Cr), Approved Value, Avg Claim
- `_02_Rates`: Approval Rate, Rejection Rate, Loss Ratio, Emergency Rate
- `_03_Time`: YoY Growth %, YTD Claims, Moving Average, Running Total
- `_04_Advanced`: % of Total (ALL), % Within Insurer (ALLEXCEPT), RANKX
- `_05_Risk`: Patient Risk Score (5-factor model), Risk Category
- `_06_Dynamic`: Dynamic Titles, YoY Arrow Indicators, Status Labels

### Dashboard Pages:

**Page 1 — Executive Command Center**
- 6 KPI Cards with conditional formatting
- Insurer Performance Scorecard (Clustered Bar)
- Monthly Claims Trend (Combo Chart with forecast)
- Decomposition Tree (AI Visual)
- Geographic Map

**Page 2 — Operational Deep Dive**
- Claim Size Distribution (Histogram)
- Rejection Root Cause (100% Stacked Bar)
- Hospital Performance Matrix (Heatmap)
- Key Influencers (AI Visual — auto-detects rejection causes)
- Treatment Type Trends (Stacked Area)

**Page 3 — Fraud & Risk Intelligence**
- Risk Distribution (3 Gauge Charts)
- Claim Anomaly Scatter Plot (Fraud Radar)
- High-Frequency Claimants Table (Data Bars + Color Scale)
- Age vs Chronic Condition Risk Heatmap
- Emergency Funnel by Hospital Tier

**Hidden Pages:**
- Patient Detail (Drill-Through) — right-click any patient for full profile
- Custom Tooltip — mini-dashboard on hover

**Advanced Features:**
- Page Navigation Buttons on all pages
- Synced Slicers across pages
- 8 Conditional Formatting rules

---

## Patient Risk Scoring Model

5-factor weighted scoring (max 100 points):

| Factor | Max Points | Logic |
|---|---|---|
| Age | 20 | 60+ gets full points |
| Chronic Condition | 25 | Heart Disease/COPD = 25, Diabetes = 15, None = 0 |
| Claim Frequency | 20 | 15+ claims = 20 points |
| Emergency Visits | 15 | 3+ emergencies = 15 points |
| Readmissions | 20 | 2+ readmissions = 20 points |

**Classification:** HIGH RISK (65+) | MEDIUM RISK (40-64) | LOW RISK (<40)

---

## Tech Stack

| Category | Technologies |
|---|---|
| **Languages** | Python, SQL, DAX |
| **Database** | PostgreSQL 16, Azure Database for PostgreSQL |
| **Cloud** | Azure Blob Storage, Azure Data Factory, Azure PostgreSQL Flexible Server |
| **BI Tools** | Power BI Desktop, Power BI Service |
| **Python Libraries** | pandas, NumPy, psycopg2 |
| **Version Control** | Git, GitHub |

---

## Project Structure

```
healthcare-insurance-claims-analytics/
├── data/
│   ├── raw/                    # 6 CSV files (generated)
│   ├── generate_data.py        # Python data generator
│   └── load_to_postgres.py     # Database loader (local + Azure)
├── schema/
│   └── 01_create_schema.sql    # Star schema DDL
├── queries/
│   ├── window_functions/       # Window function queries
│   ├── ctes/                   # CTE-based analysis
│   ├── business_analysis/      # Business metrics queries
│   └── fraud_detection/        # Fraud & anomaly queries
├── dashboard/
│   ├── healthcare_claims.pbix  # Power BI dashboard file
│   ├── page1_executive.png     # Dashboard screenshots
│   ├── page2_operations.png
│   └── page3_fraud_risk.png
├── images/
│   ├── azure_postgresql.png    # Azure deployment screenshots
│   ├── azure_blob_storage.png
│   ├── azure_resource_group.png
│   ├── adf_pipeline_success.png
│   └── adf_studio_overview.png
├── requirements.txt
└── README.md
```

---

## Certifications

- **PL-300** — Microsoft Power BI Data Analyst
- **AWS Cloud Practitioner** — Amazon Web Services
- **CFA Investment Foundations** — CFA Institute

---

## Author

**Sumit Prajapat**  
Data Analyst | Power BI | SQL | Python | Azure

---

*Built as a portfolio project demonstrating end-to-end data analytics capabilities from data generation to cloud deployment.*
