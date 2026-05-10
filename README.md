# Healthcare & Insurance Claims Analytics

## End-to-End Data Analytics Platform with Azure Cloud Pipeline

## Note on Data
Data is synthetically generated using Python to simulate realistic insurance 
claim patterns. The schema design, SQL queries, DAX measures, and analytical 
methodology are production-grade.

![Python](https://img.shields.io/badge/Python-3.12-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791)
![Power BI](https://img.shields.io/badge/Power%20BI-PL--300%20Certified-F2C811)
![License](https://img.shields.io/badge/License-MIT-green)

<img width="1362" height="783" alt="Executive Dashboard 1" src="https://github.com/user-attachments/assets/79e5f8ba-2029-46e7-acad-e152279203eb" />

<img width="1369" height="773" alt="Operartion Dashboard" src="https://github.com/user-attachments/assets/a74e4bf3-f955-41ed-8c2a-3bb9f38e13ba" />

<img width="1339" height="768" alt="Risk dasboard" src="https://github.com/user-attachments/assets/97eb0e6b-e76f-4ff3-b4e4-75c75a844175" />

---

## Project Overview

A complete data analytics platform analyzing **50,000 insurance claims** across **5,000 patients**, **200 hospitals**, and **8 insurers** over 3 years (2022-2024). Built with Python, PostgreSQL, and Power BI.

### Key Findings
- **Loss Ratio: 55.76%** — profitable portfolio (under 60% threshold)
- **Cancer Treatment**: 5% of claims but **16.8% of total costs** — highest cost diagnosis
- **24 high-risk patients** flagged responsible for **₹9.34 Crore** in claims
- **₹19.4 Crore** in avoidable rejections due to Incomplete Documents — recoverable through process fix
- **Gujarat** is the highest claiming state at ₹140.99 Crore

---

## Architecture
```
Python Script
(generate_data.py)
  ↓ generates
6 CSV files
(50K+ records)
  ↓ load locally
PostgreSQL
  ↓
20 SQL Queries
  ↓
Power BI Dashboard
(healthcare_claims.pbix)
```

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
| **Database** | PostgreSQL 16 |
| **BI Tools** | Power BI Desktop, Power BI Service |
| **Python Libraries** | pandas, NumPy, psycopg2 |
| **Version Control** | Git, GitHub |

---

## How to Run
```bash
# 1. Clone the repository
git clone https://github.com/Skpkush/Healthcare-Insurance-Claims-Analytics.git
cd Healthcare-Insurance-Claims-Analytics

# 2. Install dependencies
pip install -r requirements.txt

# 3. Generate the dataset
python data/generate_data.py

# 4. Load data into PostgreSQL
python data/load_to_postgres.py
```

> To open the dashboard — download `Claim analytics.pbix` from the `dashboard/` folder and open with Power BI Desktop.

---

## Project Structure
```
healthcare-insurance-claims-analytics/
├── data/
│   ├── generate_data.py        # Python data generator
│   └── load_to_postgres.py     # Database loader
├── schema/
│   └── 01_create_schema.sql
└── queries/
|   ├── business_analysis/     (7 files — loss ratio, risk scoring, rejection, state analysis, benchmarking, scorecard, monthly trend)
|   ├── ctes/                  (4 real CTEs + 2 redirect stubs pointing to fraud_detection/)
|   └── window_functions/      (7 files — running total, rank, moving avg, LAG, state rank, percentile, first/last)
├── dashboard/
│   ├── Claim analytics.pbix        # Power BI dashboard file
│   ├── Executive Dashboard 1.png   # Dashboard screenshots
│   ├── Operartion Dashboard.png
│   └── Risk dasboard.png
├── requirements.txt
└── README.md
```

---

## Certifications

- **PL-300** — Microsoft Power BI Data Analyst
- **AWS Cloud Practitioner** — Amazon Web Services
- **CFA Investment Foundations** — CFA Institute
- **AZ900** -  Microsoft Azure Fundamental
- **DP900** -  Microsoft Azure Data Fundamental

---

## Author

**Sumit Prajapat**
Data Analyst | Power BI | SQL | Python | Azure
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5)](https://www.linkedin.com/in/sumit-k-prajapat/)
[![GitHub](https://img.shields.io/badge/GitHub-Skpkush-181717)](https://github.com/Skpkush)
[![Email](https://img.shields.io/badge/Email-Contact-D14836)](mailto:sumitkprajapat@gmail.com)

---

*Built as a portfolio project demonstrating end-to-end data analytics capabilities from data generation to cloud deployment.*

