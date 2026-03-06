"""
Healthcare & Insurance Claims Analytics — Synthetic Data Generator
Author: Sumit | Data Analyst Portfolio Project
Description: Generates realistic Indian healthcare insurance claims data
             with 5 tables following a star schema design.

Usage:
    python data/generate_data.py

Output:
    - data/raw/dim_date.csv        (1,096 rows)
    - data/raw/dim_patients.csv    (5,000 rows)
    - data/raw/dim_providers.csv   (200 rows)
    - data/raw/dim_policies.csv    (40 rows)
    - data/raw/fact_claims.csv     (50,000 rows)
    - data/raw/diagnosis_reference.csv (20 rows)
"""

import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

# ============================================================
# SET SEED FOR REPRODUCIBILITY
# ============================================================
np.random.seed(42)
random.seed(42)

# Create output directory if it doesn't exist
os.makedirs('data/raw', exist_ok=True)

print("=" * 60)
print("  HEALTHCARE & INSURANCE CLAIMS — DATA GENERATOR")
print("=" * 60)
print()

# ============================================================
# DIMENSION TABLE 1: dim_date (2022-01-01 to 2024-12-31)
# ============================================================
print("[1/6] Generating dim_date...")
dates = pd.date_range('2022-01-01', '2024-12-31', freq='D')
dim_date = pd.DataFrame({
    'date_key': range(1, len(dates) + 1),
    'full_date': dates,
    'day': dates.day,
    'month': dates.month,
    'month_name': dates.strftime('%B'),
    'quarter': dates.quarter,
    'year': dates.year,
    'day_of_week': dates.strftime('%A'),
    'is_weekend': dates.dayofweek.isin([5, 6]).astype(int)
})
print(f"   ✅ dim_date: {len(dim_date)} rows generated")

# ============================================================
# DIMENSION TABLE 2: dim_patients (5,000 patients)
# ============================================================
print("[2/6] Generating dim_patients...")
n_patients = 5000

indian_cities = [
    'Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Hyderabad',
    'Pune', 'Kolkata', 'Ahmedabad', 'Jaipur', 'Lucknow',
    'Chandigarh', 'Indore', 'Nagpur', 'Bhopal', 'Patna',
    'Coimbatore', 'Kochi', 'Surat', 'Vadodara', 'Visakhapatnam'
]

city_to_state = {
    'Mumbai': 'Maharashtra', 'Delhi': 'Delhi',
    'Bangalore': 'Karnataka', 'Chennai': 'Tamil Nadu',
    'Hyderabad': 'Telangana', 'Pune': 'Maharashtra',
    'Kolkata': 'West Bengal', 'Ahmedabad': 'Gujarat',
    'Jaipur': 'Rajasthan', 'Lucknow': 'Uttar Pradesh',
    'Chandigarh': 'Punjab', 'Indore': 'Madhya Pradesh',
    'Nagpur': 'Maharashtra', 'Bhopal': 'Madhya Pradesh',
    'Patna': 'Bihar', 'Coimbatore': 'Tamil Nadu',
    'Kochi': 'Kerala', 'Surat': 'Gujarat',
    'Vadodara': 'Gujarat', 'Visakhapatnam': 'Andhra Pradesh'
}

chronic_conditions = [
    'None', 'Diabetes', 'Hypertension', 'Diabetes+Hypertension',
    'Heart Disease', 'Asthma', 'Thyroid', 'Obesity', 'Arthritis', 'COPD'
]
chronic_probs = [0.25, 0.15, 0.15, 0.10, 0.08, 0.07, 0.06, 0.05, 0.05, 0.04]

cities_list = [random.choice(indian_cities) for _ in range(n_patients)]
ages = np.clip(np.random.normal(45, 18, n_patients).astype(int), 1, 90)

dim_patients = pd.DataFrame({
    'patient_id': [f'P{str(i).zfill(5)}' for i in range(1, n_patients + 1)],
    'age': ages,
    'gender': np.random.choice(['Male', 'Female', 'Other'], n_patients, p=[0.48, 0.50, 0.02]),
    'city': cities_list,
    'state': [city_to_state[c] for c in cities_list],
    'chronic_condition': np.random.choice(chronic_conditions, n_patients, p=chronic_probs),
    'blood_group': np.random.choice(
        ['A+', 'A-', 'B+', 'B-', 'O+', 'O-', 'AB+', 'AB-'],
        n_patients,
        p=[0.22, 0.06, 0.30, 0.04, 0.25, 0.04, 0.06, 0.03]
    ),
    'registration_date': [
        datetime(2021, 1, 1) + timedelta(days=random.randint(0, 365))
        for _ in range(n_patients)
    ]
})
print(f"   ✅ dim_patients: {len(dim_patients)} rows generated")

# ============================================================
# DIMENSION TABLE 3: dim_providers (200 hospitals)
# ============================================================
print("[3/6] Generating dim_providers...")
n_providers = 200

hospital_prefixes = [
    'Apollo', 'Fortis', 'Max', 'Medanta', 'AIIMS',
    'Narayana', 'Manipal', 'Columbia Asia', 'Lilavati', 'Kokilaben',
    'Ruby Hall', 'Breach Candy', 'Sir Ganga Ram', 'Hinduja', 'Wockhardt',
    'Global', 'Sahyadri', 'Yashoda', 'KIMS', 'Care'
]

specialities = [
    'General Medicine', 'Cardiology', 'Orthopedics', 'Neurology',
    'Oncology', 'Gastroenterology', 'Pulmonology', 'Nephrology',
    'Dermatology', 'ENT', 'Ophthalmology', 'Pediatrics',
    'Gynecology', 'Urology'
]

provider_cities = np.random.choice(indian_cities, n_providers)

dim_providers = pd.DataFrame({
    'provider_id': [f'H{str(i).zfill(4)}' for i in range(1, n_providers + 1)],
    'hospital_name': [
        f'{random.choice(hospital_prefixes)} Hospital {random.choice(indian_cities)}'
        for _ in range(n_providers)
    ],
    'speciality': np.random.choice(specialities, n_providers),
    'city': provider_cities,
    'state': [city_to_state[c] for c in provider_cities],
    'tier': np.random.choice(['Tier 1', 'Tier 2', 'Tier 3'], n_providers, p=[0.30, 0.45, 0.25]),
    'bed_capacity': np.random.choice(
        [50, 100, 150, 200, 300, 500, 750, 1000],
        n_providers,
        p=[0.10, 0.15, 0.20, 0.20, 0.15, 0.10, 0.05, 0.05]
    ),
    'accreditation': np.random.choice(
        ['NABH', 'NABL', 'JCI', 'ISO', 'None'],
        n_providers,
        p=[0.35, 0.15, 0.10, 0.15, 0.25]
    )
})
print(f"   ✅ dim_providers: {len(dim_providers)} rows generated")

# ============================================================
# DIMENSION TABLE 4: dim_policies (8 insurers x 5 plan types = 40)
# ============================================================
print("[4/6] Generating dim_policies...")

insurers = [
    'Star Health', 'ICICI Lombard', 'Bajaj Allianz', 'Niva Bupa',
    'HDFC ERGO', 'New India Assurance', 'Care Health', 'Tata AIG'
]
plan_types = ['Individual', 'Family Floater', 'Senior Citizen', 'Critical Illness', 'Top-Up']

policies = []
pid = 1
for insurer in insurers:
    for plan in plan_types:
        policies.append({
            'policy_id': f'POL{str(pid).zfill(4)}',
            'insurer': insurer,
            'plan_type': plan,
            'annual_premium': random.randint(5000, 25000),
            'coverage_limit': random.choice([300000, 500000, 1000000, 1500000, 2000000, 5000000]),
            'deductible': random.choice([0, 5000, 10000, 15000, 25000]),
            'copay_pct': random.choice([0, 10, 15, 20]),
            'waiting_period_days': random.choice([30, 60, 90, 180, 365]),
            'claim_settlement_ratio': round(random.uniform(0.85, 0.98), 2)
        })
        pid += 1

dim_policies = pd.DataFrame(policies)
print(f"   ✅ dim_policies: {len(dim_policies)} rows generated")

# ============================================================
# FACT TABLE: fact_claims (50,000 claims over 3 years)
# ============================================================
print("[5/6] Generating fact_claims (50,000 rows)... This may take a few seconds...")
n_claims = 50000

# Diagnosis codes with realistic Indian medical costs (INR)
diagnosis_codes = {
    'D001': 'Type 2 Diabetes',
    'D002': 'Hypertension',
    'D003': 'Acute Myocardial Infarction',
    'D004': 'Pneumonia',
    'D005': 'Dengue Fever',
    'D006': 'Appendicitis',
    'D007': 'Fracture - Femur',
    'D008': 'Kidney Stone',
    'D009': 'Cataract Surgery',
    'D010': 'Malaria',
    'D011': 'Thyroid Disorder',
    'D012': 'Gastroenteritis',
    'D013': 'Cesarean Delivery',
    'D014': 'Knee Replacement',
    'D015': 'Coronary Bypass',
    'D016': 'Liver Cirrhosis',
    'D017': 'Asthma Attack',
    'D018': 'Spinal Surgery',
    'D019': 'Cancer Treatment',
    'D020': 'COVID-19 Treatment'
}

# Average cost per diagnosis in INR
avg_costs = {
    'D001': 25000, 'D002': 15000, 'D003': 350000, 'D004': 80000,
    'D005': 45000, 'D006': 120000, 'D007': 200000, 'D008': 90000,
    'D009': 60000, 'D010': 35000, 'D011': 20000, 'D012': 15000,
    'D013': 150000, 'D014': 400000, 'D015': 500000, 'D016': 250000,
    'D017': 40000, 'D018': 450000, 'D019': 600000, 'D020': 200000
}

claim_statuses = ['Approved', 'Rejected', 'Pending', 'Partially Approved', 'Under Review']
status_probs = [0.55, 0.15, 0.10, 0.12, 0.08]

rejection_reasons = [
    'Incomplete Documents', 'Pre-existing Condition', 'Waiting Period Not Over',
    'Policy Lapsed', 'Claim Exceeds Coverage', 'Non-Covered Treatment', 'Fraud Suspected'
]

treatment_types = ['IPD', 'OPD', 'Day Care', 'Emergency']
treatment_probs = [0.45, 0.25, 0.15, 0.15]

# Pre-fetch arrays for faster generation
date_keys = dim_date['date_key'].values
patient_ids = dim_patients['patient_id'].values
provider_ids = dim_providers['provider_id'].values
policy_ids = dim_policies['policy_id'].values
diag_codes = list(diagnosis_codes.keys())

claims = []
for i in range(1, n_claims + 1):
    # Pick random diagnosis and generate realistic cost
    diag = random.choice(diag_codes)
    base_cost = avg_costs[diag]
    claim_amt = max(5000, int(np.random.normal(base_cost, base_cost * 0.3)))

    # Determine claim status
    status = np.random.choice(claim_statuses, p=status_probs)

    # Calculate approved amount based on status
    if status == 'Approved':
        paid_amt = int(claim_amt * random.uniform(0.80, 1.0))
        rej_reason = None
    elif status == 'Partially Approved':
        paid_amt = int(claim_amt * random.uniform(0.30, 0.70))
        rej_reason = None
    elif status == 'Rejected':
        paid_amt = 0
        rej_reason = random.choice(rejection_reasons)
    else:  # Pending or Under Review
        paid_amt = 0
        rej_reason = None

    # Settlement days (NULL for pending claims)
    if status in ['Approved', 'Partially Approved', 'Rejected']:
        settlement_days = random.randint(1, 90)
    else:
        settlement_days = None

    # Length of stay (0 for OPD)
    los = max(0, int(np.random.exponential(4))) if random.random() > 0.3 else 0

    claims.append({
        'claim_id': f'CLM{str(i).zfill(6)}',
        'patient_id': random.choice(patient_ids),
        'provider_id': random.choice(provider_ids),
        'policy_id': random.choice(policy_ids),
        'date_key': int(random.choice(date_keys)),
        'diagnosis_code': diag,
        'diagnosis_name': diagnosis_codes[diag],
        'treatment_type': np.random.choice(treatment_types, p=treatment_probs),
        'claim_amount': claim_amt,
        'approved_amount': paid_amt,
        'status': status,
        'rejection_reason': rej_reason,
        'settlement_days': settlement_days,
        'length_of_stay': los,
        'is_emergency': 1 if random.random() < 0.2 else 0,
        'is_readmission': 1 if random.random() < 0.12 else 0
    })

    # Progress indicator
    if i % 10000 == 0:
        print(f"   ... {i}/{n_claims} claims generated")

fact_claims = pd.DataFrame(claims)
print(f"   ✅ fact_claims: {len(fact_claims)} rows generated")

# ============================================================
# REFERENCE TABLE: diagnosis_reference
# ============================================================
print("[6/6] Generating diagnosis_reference...")
diag_ref = pd.DataFrame([
    {'diagnosis_code': k, 'diagnosis_name': v, 'avg_cost_inr': avg_costs[k]}
    for k, v in diagnosis_codes.items()
])
print(f"   ✅ diagnosis_reference: {len(diag_ref)} rows generated")

# ============================================================
# SAVE ALL CSVs TO data/raw/
# ============================================================
print()
print("Saving CSV files to data/raw/ ...")
dim_date.to_csv('data/raw/dim_date.csv', index=False)
dim_patients.to_csv('data/raw/dim_patients.csv', index=False)
dim_providers.to_csv('data/raw/dim_providers.csv', index=False)
dim_policies.to_csv('data/raw/dim_policies.csv', index=False)
fact_claims.to_csv('data/raw/fact_claims.csv', index=False)
diag_ref.to_csv('data/raw/diagnosis_reference.csv', index=False)

# ============================================================
# PRINT SUMMARY
# ============================================================
print()
print("=" * 60)
print("  ✅ ALL DATA GENERATED SUCCESSFULLY!")
print("=" * 60)
print()
print(f"  Files saved to: data/raw/")
print(f"  ─────────────────────────────────────────")
print(f"  dim_date.csv           : {len(dim_date):>6,} rows")
print(f"  dim_patients.csv       : {len(dim_patients):>6,} rows")
print(f"  dim_providers.csv      : {len(dim_providers):>6,} rows")
print(f"  dim_policies.csv       : {len(dim_policies):>6,} rows")
print(f"  fact_claims.csv        : {len(fact_claims):>6,} rows")
print(f"  diagnosis_reference.csv: {len(diag_ref):>6,} rows")
print(f"  ─────────────────────────────────────────")
print(f"  Total records          : {len(dim_date)+len(dim_patients)+len(dim_providers)+len(dim_policies)+len(fact_claims)+len(diag_ref):>6,}")
print()
print("  Next step: Load data into MySQL using data/load_to_mysql.py")
print()
