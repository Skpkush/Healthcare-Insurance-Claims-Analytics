"""
Load bridge_claim_diagnosis by unpivoting ClmDiagnosisCode_1..10.

Each claim becomes up to 10 rows in the bridge — one per non-NULL diagnosis code,
with diagnosis_position preserved (1 = primary, 2-10 = secondary).
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
DIAG_COLS = [f'ClmDiagnosisCode_{i}' for i in range(1, 11)]


def find_csv(prefix):
    for cand in DATA_DIR.glob(f'{prefix}.csv'):
        return cand
    matches = sorted(DATA_DIR.glob(f'{prefix}-*.csv'))
    if matches:
        return matches[0]
    raise FileNotFoundError(prefix)


def unpivot_diagnoses(claims_df, source_label):
    """Convert wide diagnosis columns to long bridge format."""
    print(f"Unpivoting {source_label}...")

    bridge = claims_df[['ClaimID'] + DIAG_COLS].melt(
        id_vars=['ClaimID'],
        value_vars=DIAG_COLS,
        var_name='diag_col_name',
        value_name='diagnosis_code',
    )

    bridge = bridge.dropna(subset=['diagnosis_code'])

    bridge['diagnosis_position'] = (
        bridge['diag_col_name'].str.extract(r'(\d+)$').astype(int)
    )

    bridge = bridge[['ClaimID', 'diagnosis_code', 'diagnosis_position']]
    bridge.columns = ['claim_id', 'diagnosis_code', 'diagnosis_position']

    # Match dim_diagnosis_code's normalization (strip whitespace; no .0 suffixes
    # observed for diagnoses but harmless to apply for safety).
    bridge['diagnosis_code'] = bridge['diagnosis_code'].astype(str).str.strip()

    print(f"  {len(claims_df):,} claims -> {len(bridge):,} bridge rows")
    return bridge


def main():
    print("Loading inpatient claim data...")
    inp = pd.read_csv(find_csv('Train_Inpatientdata'),
                      usecols=['ClaimID'] + DIAG_COLS)

    print("Loading outpatient claim data...")
    out = pd.read_csv(find_csv('Train_Outpatientdata'),
                      usecols=['ClaimID'] + DIAG_COLS)

    bridge_inp = unpivot_diagnoses(inp, "inpatient")
    bridge_out = unpivot_diagnoses(out, "outpatient")

    bridge = pd.concat([bridge_inp, bridge_out], ignore_index=True)
    print(f"\nTotal bridge rows: {len(bridge):,}")

    print("\nDiagnosis position distribution:")
    print(bridge['diagnosis_position'].value_counts().sort_index().to_string())

    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE bridge_claim_diagnosis CASCADE;"))

    chunk_size = 10000
    print(f"\nLoading to bridge_claim_diagnosis...")
    for i in tqdm(range(0, len(bridge), chunk_size), desc="Bridge load"):
        bridge.iloc[i:i + chunk_size].to_sql(
            'bridge_claim_diagnosis', engine,
            if_exists='append', index=False, method='multi'
        )

    with engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM bridge_claim_diagnosis;")).scalar()
        print(f"\nFinal bridge_claim_diagnosis row count: {n:,}")


if __name__ == '__main__':
    main()
