"""Load bridge_claim_procedure by unpivoting ClmProcedureCode_1..6."""
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
PROC_COLS = [f'ClmProcedureCode_{i}' for i in range(1, 7)]


def find_csv(prefix):
    for cand in DATA_DIR.glob(f'{prefix}.csv'):
        return cand
    matches = sorted(DATA_DIR.glob(f'{prefix}-*.csv'))
    if matches:
        return matches[0]
    raise FileNotFoundError(prefix)


def unpivot_procedures(df, label):
    bridge = df[['ClaimID'] + PROC_COLS].melt(
        id_vars=['ClaimID'],
        value_vars=PROC_COLS,
        var_name='proc_col_name',
        value_name='procedure_code',
    )
    bridge = bridge.dropna(subset=['procedure_code'])
    bridge['procedure_position'] = bridge['proc_col_name'].str.extract(r'(\d+)$').astype(int)
    bridge = bridge[['ClaimID', 'procedure_code', 'procedure_position']]
    bridge.columns = ['claim_id', 'procedure_code', 'procedure_position']

    # Same normalization as dim_procedure_code (float-read codes get a .0 suffix).
    bridge['procedure_code'] = (
        bridge['procedure_code']
        .astype(str)
        .str.strip()
        .str.replace(r'\.0$', '', regex=True)
    )

    print(f"  {label}: {len(df):,} claims -> {len(bridge):,} procedure bridge rows")
    return bridge


def main():
    inp = pd.read_csv(find_csv('Train_Inpatientdata'),  usecols=['ClaimID'] + PROC_COLS)
    out = pd.read_csv(find_csv('Train_Outpatientdata'), usecols=['ClaimID'] + PROC_COLS)

    bridge_inp = unpivot_procedures(inp, "inpatient")
    bridge_out = unpivot_procedures(out, "outpatient")
    bridge = pd.concat([bridge_inp, bridge_out], ignore_index=True)
    print(f"\nTotal procedure bridge rows: {len(bridge):,}")

    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE bridge_claim_procedure CASCADE;"))

    chunk_size = 10000
    for i in tqdm(range(0, len(bridge), chunk_size), desc="Procedure bridge"):
        bridge.iloc[i:i + chunk_size].to_sql(
            'bridge_claim_procedure', engine,
            if_exists='append', index=False, method='multi'
        )

    with engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM bridge_claim_procedure;")).scalar()
        print(f"\nFinal bridge_claim_procedure row count: {n:,}")


if __name__ == '__main__':
    main()
