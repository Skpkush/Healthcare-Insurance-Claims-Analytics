"""Build dim_procedure_code from claim files."""
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
PROC_COLS = [f'ClmProcedureCode_{i}' for i in range(1, 7)]


def find_csv(prefix):
    for cand in DATA_DIR.glob(f'{prefix}.csv'):
        return cand
    matches = sorted(DATA_DIR.glob(f'{prefix}-*.csv'))
    if matches:
        return matches[0]
    raise FileNotFoundError(prefix)


def extract_distinct_codes():
    print("Extracting procedure codes from claims...")
    inp = pd.read_csv(find_csv('Train_Inpatientdata'),  usecols=PROC_COLS)
    out = pd.read_csv(find_csv('Train_Outpatientdata'), usecols=PROC_COLS)

    all_codes = []
    for df in [inp, out]:
        for col in PROC_COLS:
            # Procedure codes can be read as floats — normalize to clean string ints.
            codes = df[col].dropna().astype(str).str.strip()
            codes = codes.str.replace(r'\.0$', '', regex=True)
            all_codes.append(codes)

    distinct = pd.concat(all_codes).drop_duplicates().sort_values()
    print(f"Found {len(distinct):,} distinct procedure codes")

    return pd.DataFrame({
        'procedure_code': distinct.values,
        'description': None,
    })


def load_to_postgres(df):
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dim_procedure_code CASCADE;"))
    df.to_sql('dim_procedure_code', engine, if_exists='append',
              index=False, method='multi', chunksize=2000)
    with engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM dim_procedure_code;")).scalar()
        print(f"Loaded {n:,} distinct procedure codes")


def main():
    df = extract_distinct_codes()
    load_to_postgres(df)


if __name__ == '__main__':
    main()
