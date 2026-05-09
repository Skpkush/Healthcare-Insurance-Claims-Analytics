"""Load fact_outpatient_claims."""
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
    for cand in DATA_DIR.glob(f'{prefix}.csv'):
        return cand
    matches = sorted(DATA_DIR.glob(f'{prefix}-*.csv'))
    if matches:
        return matches[0]
    raise FileNotFoundError(prefix)


def load_and_transform():
    print("Reading outpatient claims (this is the big file ~518K rows)...")
    df = pd.read_csv(find_csv('Train_Outpatientdata'))
    print(f"Source rows: {len(df):,}")

    out = pd.DataFrame()
    out['claim_id']                = df['ClaimID']
    out['beneficiary_id']          = df['BeneID']
    out['provider_id']             = df['Provider']
    out['claim_start_date_key']    = pd.to_datetime(df['ClaimStartDt']).dt.date
    out['claim_end_date_key']      = pd.to_datetime(df['ClaimEndDt']).dt.date
    out['claim_amount_reimbursed'] = df['InscClaimAmtReimbursed'].fillna(0)
    out['deductible_amt_paid']     = df['DeductibleAmtPaid'].fillna(0)
    out['attending_physician_id']  = df['AttendingPhysician']
    out['operating_physician_id']  = df['OperatingPhysician']
    out['other_physician_id']      = df['OtherPhysician']
    out['admit_diagnosis_code']    = df['ClmAdmitDiagnosisCode']

    print(f"Total reimbursed: ${out['claim_amount_reimbursed'].sum():,.0f}")
    return out


def load_to_postgres(df):
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE fact_outpatient_claims CASCADE;"))

    chunk_size = 5000
    for i in tqdm(range(0, len(df), chunk_size), desc="Loading outpatient"):
        df.iloc[i:i + chunk_size].to_sql(
            'fact_outpatient_claims', engine,
            if_exists='append', index=False, method='multi'
        )

    with engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM fact_outpatient_claims;")).scalar()
        print(f"\nLoaded {n:,} outpatient claims")


def main():
    df = load_and_transform()
    load_to_postgres(df)


if __name__ == '__main__':
    main()
