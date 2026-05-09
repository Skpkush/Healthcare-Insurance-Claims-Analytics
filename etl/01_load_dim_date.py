"""
Generate and load dim_date.
Calendar dimension covering all claim date ranges (with safety padding).

Usage: python etl/01_load_dim_date.py
"""
import os
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# URL-encode the password so special chars (@, #, etc.) don't break the URL parser.
DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{quote_plus(os.getenv('DB_PASSWORD'))}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# Date range — covers all claim activity in this dataset (actual: 2008-11-27..2009-12-31).
# Padded to whole years on both sides for safety against late-arriving data.
START_DATE = '2008-01-01'
END_DATE   = '2010-12-31'


def generate_dim_date():
    """Generate calendar dimension as a DataFrame."""
    date_range = pd.date_range(START_DATE, END_DATE, freq='D')
    df = pd.DataFrame({'date_key': date_range})

    df['day_of_week']  = df['date_key'].dt.dayofweek
    df['day_name']     = df['date_key'].dt.day_name()
    df['day_of_month'] = df['date_key'].dt.day
    df['day_of_year']  = df['date_key'].dt.dayofyear
    df['week_of_year'] = df['date_key'].dt.isocalendar().week
    df['month_number'] = df['date_key'].dt.month
    df['month_name']   = df['date_key'].dt.month_name()
    df['quarter']      = df['date_key'].dt.quarter
    df['year']         = df['date_key'].dt.year
    df['is_weekend']   = df['day_of_week'].isin([5, 6])

    # Match DDL's DATE column type (not TIMESTAMP).
    df['date_key'] = df['date_key'].dt.date
    return df


def load_to_postgres(df):
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dim_date CASCADE;"))

    df.to_sql('dim_date', engine, if_exists='append', index=False,
              method='multi', chunksize=1000)

    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*), MIN(date_key), MAX(date_key) FROM dim_date;"
        ))
        row = result.fetchone()
        print(f"Loaded {row[0]} rows. Date range: {row[1]} to {row[2]}")


def main():
    print("Generating dim_date...")
    df = generate_dim_date()
    print(f"Generated {len(df)} rows")

    print("Loading to PostgreSQL...")
    load_to_postgres(df)
    print("dim_date loaded successfully.")


if __name__ == '__main__':
    main()
