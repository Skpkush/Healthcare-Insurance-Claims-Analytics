"""
Load dim_provider from Train.csv.
Includes the PotentialFraud label.

Usage: python etl/02_load_dim_provider.py
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


def find_csv(prefix):
    """Tolerate Kaggle's timestamped filenames."""
    for cand in DATA_DIR.glob(f'{prefix}.csv'):
        return cand
    matches = sorted(DATA_DIR.glob(f'{prefix}-*.csv'))
    if matches:
        return matches[0]
    raise FileNotFoundError(f"No CSV found for prefix: {prefix}")


SOURCE_FILE = find_csv('Train')


def load_and_transform():
    df = pd.read_csv(SOURCE_FILE)

    print(f"Source row count: {len(df):,}")
    print(f"Source columns: {df.columns.tolist()}")
    print(f"PotentialFraud distribution:\n{df['PotentialFraud'].value_counts()}")

    out = pd.DataFrame({
        'provider_id': df['Provider'],
        'is_potentially_fraudulent': df['PotentialFraud'] == 'Yes',
    })

    print(f"Transformed row count: {len(out):,}")
    print(f"Fraud-flagged providers: {out['is_potentially_fraudulent'].sum():,} "
          f"({out['is_potentially_fraudulent'].mean() * 100:.2f}%)")

    return out


def load_to_postgres(df):
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dim_provider CASCADE;"))

    df.to_sql('dim_provider', engine, if_exists='append', index=False,
              method='multi', chunksize=1000)

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*), SUM(CASE WHEN is_potentially_fraudulent THEN 1 ELSE 0 END) "
            "FROM dim_provider;"
        ))
        total, fraud_count = result.fetchone()
        print(f"Loaded {total} providers. {fraud_count} flagged as potentially fraudulent.")


def main():
    print("Loading and transforming Train.csv...")
    df = load_and_transform()
    print("\nLoading to PostgreSQL...")
    load_to_postgres(df)
    print("\ndim_provider loaded successfully.")


if __name__ == '__main__':
    main()
