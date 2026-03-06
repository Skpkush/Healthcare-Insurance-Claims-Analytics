
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import sys
import os

# ============================================================
# DATABASE CONNECTION — UPDATE YOUR PASSWORD HERE
# ============================================================
DB_CONFIG = {
    'host': 'claims-analytics-sumit.postgres.database.azure.com',
    'port': 5432,
    'user': 'pgadmin',
    'password': 'P@ssw0rd#2998',
    'database': 'claims_analytics'
}


def connect_to_postgres():
    """Connect to PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        print("✅ Connected to PostgreSQL successfully!")
        return conn
    except psycopg2.Error as err:
        print(f"❌ PostgreSQL Connection Error: {err}")
        print()
        print("TROUBLESHOOTING:")
        sys.exit(1)


def load_csv_to_table(cursor, conn, table_name, csv_path):
    """Load a CSV file into a PostgreSQL table using fast COPY method."""
    if not os.path.exists(csv_path):
        print(f"  ❌ File not found: {csv_path}")
        print(f"     Run 'python data/generate_data.py' first!")
        return False

    df = pd.read_csv(csv_path)

    # Fix data types: convert float columns that should be INT
    # (pandas reads INT columns with NaN as float64)
    for col in df.columns:
        if df[col].dtype == 'float64':
            # Convert non-null floats to int, keep NaN as None
            df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)

    # Replace remaining NaN with None (PostgreSQL NULL)
    df = df.where(pd.notnull(df), None)

    # Build INSERT query with execute_values (fast batch insert)
    cols = list(df.columns)
    cols_str = ', '.join(cols)
    insert_sql = f"INSERT INTO {table_name} ({cols_str}) VALUES %s"

    # Convert to list of tuples with native Python types (not numpy)
    data = []
    for row in df.values:
        converted = []
        for val in row:
            if val is None:
                converted.append(None)
            elif isinstance(val, float) and pd.isna(val):
                converted.append(None)
            elif isinstance(val, (int, float)):
                converted.append(int(val))
            else:
                converted.append(str(val) if not isinstance(val, str) else val)
        data.append(tuple(converted))

    # Insert using execute_values (much faster than executemany)
    batch_size = 5000
    total = len(data)
    for i in range(0, total, batch_size):
        batch = data[i:i + batch_size]
        execute_values(cursor, insert_sql, batch, page_size=batch_size)
        loaded = min(i + batch_size, total)
        print(f"  ... {loaded}/{total} rows loaded", end='\r')

    conn.commit()
    print(f"  ✅ {table_name}: {total} rows loaded successfully       ")
    return True


def main():
    print("=" * 60)
    print("  LOADING DATA INTO PostgreSQL — claims_analytics")
    print("=" * 60)
    print()

    # Connect
    conn = connect_to_postgres()
    cursor = conn.cursor()

    # IMPORTANT: Load dimension tables FIRST, then fact table
    # (because fact table has foreign key references)
    tables_to_load = [
        ('dim_date', 'data/raw/dim_date.csv'),
        ('dim_patients', 'data/raw/dim_patients.csv'),
        ('dim_providers', 'data/raw/dim_providers.csv'),
        ('dim_policies', 'data/raw/dim_policies.csv'),
        ('fact_claims', 'data/raw/fact_claims.csv'),       # LAST!
    ]

    print("Loading tables (dimensions first, then fact):")
    print()

    for table_name, csv_path in tables_to_load:
        # Clear existing data using TRUNCATE (faster than DELETE, resets sequences)
        if table_name == 'fact_claims':
            cursor.execute("TRUNCATE TABLE fact_claims")
        else:
            cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
        conn.commit()

        load_csv_to_table(cursor, conn, table_name, csv_path)

    # Verify
    print()
    print("─" * 40)
    print("VERIFICATION:")
    for table_name, _ in tables_to_load:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"  {table_name}: {count:,} rows")

    # Run ANALYZE to update statistics for query optimizer
    print()
    print("Running ANALYZE on all tables...")
    cursor.execute("ANALYZE")
    conn.commit()
    print("  ✅ Table statistics updated")

    cursor.close()
    conn.close()

    print()
    print("=" * 60)
    print("  ✅ ALL DATA LOADED SUCCESSFULLY!")
    print("=" * 60)
    print()


if __name__ == '__main__':
    main()