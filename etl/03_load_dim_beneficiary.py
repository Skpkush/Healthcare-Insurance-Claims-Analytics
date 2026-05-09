"""
Load dim_beneficiary from Train_Beneficiarydata.csv.

Critical transformations:
  - Chronic conditions: 1=Yes, 2=No  → BOOLEAN
  - RenalDiseaseIndicator:    Y=Yes, 0=No → BOOLEAN
  - Dates: parse with errors='coerce' (DOD is 99% NULL)

Usage: python etl/03_load_dim_beneficiary.py
"""
import os
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD'))}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

DATA_DIR = Path(__file__).parent.parent / 'data' / 'raw'


def find_csv(prefix):
    """Tolerate Kaggle's timestamped filenames."""
    for cand in DATA_DIR.glob(f'{prefix}.csv'):
        return cand
    matches = sorted(DATA_DIR.glob(f'{prefix}-*.csv'))
    if matches:
        return matches[0]
    raise FileNotFoundError(f"No CSV found for prefix: {prefix}")


SOURCE_FILE = find_csv('Train_Beneficiarydata')

CHRONIC_MAP = {
    'ChronicCond_Alzheimer':           'chronic_alzheimer',
    'ChronicCond_Heartfailure':        'chronic_heart_failure',
    'ChronicCond_KidneyDisease':       'chronic_kidney_disease',
    'ChronicCond_Cancer':              'chronic_cancer',
    'ChronicCond_ObstrPulmonary':      'chronic_obstr_pulmonary',
    'ChronicCond_Depression':          'chronic_depression',
    'ChronicCond_Diabetes':            'chronic_diabetes',
    'ChronicCond_IschemicHeart':       'chronic_ischemic_heart',
    'ChronicCond_Osteoporasis':        'chronic_osteoporosis',
    'ChronicCond_rheumatoidarthritis': 'chronic_rheumatoid_arthritis',
    'ChronicCond_stroke':              'chronic_stroke',
}


def load_and_transform():
    print(f"Reading {SOURCE_FILE.name}...")
    df = pd.read_csv(SOURCE_FILE)
    print(f"Source rows: {len(df):,}")

    out = pd.DataFrame()
    out['beneficiary_id'] = df['BeneID']

    out['date_of_birth'] = pd.to_datetime(df['DOB'], errors='coerce').dt.date
    out['date_of_death'] = pd.to_datetime(df['DOD'], errors='coerce').dt.date

    out['gender']      = df['Gender'].astype('Int8')
    out['race']        = df['Race'].astype('Int8')
    out['state_code']  = df['State'].astype('Int16')
    out['county_code'] = df['County'].astype('Int32')

    out['has_renal_disease'] = df['RenalDiseaseIndicator'] == 'Y'

    out['months_part_a_coverage'] = df['NoOfMonths_PartACov'].astype('Int8')
    out['months_part_b_coverage'] = df['NoOfMonths_PartBCov'].astype('Int8')

    for src_col, dst_col in CHRONIC_MAP.items():
        out[dst_col] = df[src_col] == 1

    out['ip_annual_reimbursement_amt'] = df['IPAnnualReimbursementAmt'].fillna(0)
    out['ip_annual_deductible_amt']    = df['IPAnnualDeductibleAmt'].fillna(0)
    out['op_annual_reimbursement_amt'] = df['OPAnnualReimbursementAmt'].fillna(0)
    out['op_annual_deductible_amt']    = df['OPAnnualDeductibleAmt'].fillna(0)

    print(f"\nValidation:")
    print(f"  Patients with DOD (deceased):         {out['date_of_death'].notna().sum():>7,} "
          f"({out['date_of_death'].notna().mean() * 100:.2f}%)")
    print(f"  Patients with renal disease:          {out['has_renal_disease'].sum():>7,} "
          f"({out['has_renal_disease'].mean() * 100:.2f}%)")
    print(f"  Patients with diabetes:               {out['chronic_diabetes'].sum():>7,} "
          f"({out['chronic_diabetes'].mean() * 100:.2f}%)")
    print(f"  Patients with ischemic heart disease: {out['chronic_ischemic_heart'].sum():>7,} "
          f"({out['chronic_ischemic_heart'].mean() * 100:.2f}%)")

    return out


def load_to_postgres(df):
    engine = create_engine(DB_URL)

    print("\nTruncating dim_beneficiary...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dim_beneficiary CASCADE;"))

    print(f"Loading {len(df):,} rows in chunks...")
    chunk_size = 5000
    for i in tqdm(range(0, len(df), chunk_size), desc="Loading"):
        chunk = df.iloc[i:i + chunk_size]
        chunk.to_sql('dim_beneficiary', engine, if_exists='append',
                     index=False, method='multi')

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dim_beneficiary;"))
        print(f"\nFinal row count in dim_beneficiary: {result.scalar():,}")


def main():
    df = load_and_transform()
    load_to_postgres(df)
    print("\ndim_beneficiary loaded successfully.")


if __name__ == '__main__':
    main()
