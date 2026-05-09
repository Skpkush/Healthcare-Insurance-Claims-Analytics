"""
Build dim_diagnosis_code by extracting distinct codes from claim files.

Source: ClmDiagnosisCode_1..10 from inpatient and outpatient CSVs,
plus ClmAdmitDiagnosisCode (admission diagnosis).

Usage: python etl/04_build_dim_diagnosis.py
"""
import os
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD'))}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

DATA_DIR = Path(__file__).parent.parent / 'data' / 'raw'
DIAG_COLS = [f'ClmDiagnosisCode_{i}' for i in range(1, 11)]


def find_csv(prefix):
    for cand in DATA_DIR.glob(f'{prefix}.csv'):
        return cand
    matches = sorted(DATA_DIR.glob(f'{prefix}-*.csv'))
    if matches:
        return matches[0]
    raise FileNotFoundError(prefix)


def extract_distinct_codes():
    print("Extracting diagnosis codes from inpatient claims...")
    inp = pd.read_csv(find_csv('Train_Inpatientdata'),
                      usecols=DIAG_COLS + ['ClmAdmitDiagnosisCode'])
    print("Extracting diagnosis codes from outpatient claims...")
    out = pd.read_csv(find_csv('Train_Outpatientdata'),
                      usecols=DIAG_COLS + ['ClmAdmitDiagnosisCode'])

    all_codes = []
    for df in [inp, out]:
        for col in DIAG_COLS + ['ClmAdmitDiagnosisCode']:
            all_codes.append(df[col].dropna().astype(str).str.strip())

    distinct = pd.concat(all_codes).drop_duplicates().sort_values()
    print(f"Found {len(distinct):,} distinct diagnosis codes")

    return pd.DataFrame({
        'diagnosis_code': distinct.values,
        'icd_version': 9,
        'description': None,
    })


def load_to_postgres(df):
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dim_diagnosis_code CASCADE;"))
    df.to_sql('dim_diagnosis_code', engine, if_exists='append',
              index=False, method='multi', chunksize=2000)
    with engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM dim_diagnosis_code;")).scalar()
        print(f"Loaded {n:,} distinct diagnosis codes")


def main():
    df = extract_distinct_codes()
    load_to_postgres(df)


if __name__ == '__main__':
    main()
