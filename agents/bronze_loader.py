"""
Bronze Loader: Ingests raw CSV data into DuckDB Bronze tables.
Bronze = raw, unchanged data. No cleaning yet.
"""

import duckdb
import os
from config import DB_PATH, WAREHOUSE_DIR, BRONZE_TABLES
from agents.extractor import extract_all


def load_to_bronze():
    """
    Load all source CSVs into DuckDB Bronze tables.
    Overwrites existing Bronze tables each run (idempotent).
    """
    # Make sure warehouse folder exists
    os.makedirs(WAREHOUSE_DIR, exist_ok=True)

    # Connect to DuckDB (creates file if not exists)
    conn = duckdb.connect(DB_PATH)
    print(f"\n🗄️  Connected to DuckDB: {DB_PATH}\n")

    # Extract all sources
    data = extract_all()

    # Load each into Bronze
    print("\n📦 Loading into Bronze layer...\n")
    for source_name, df in data.items():
        table_name = BRONZE_TABLES[source_name]

        # Register DataFrame temporarily so DuckDB can see it
        conn.register("temp_df", df)
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
        conn.unregister("temp_df")  # ← clean up the temp reference

        # Verify
        row_count = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]

        print(f"   ✅ {table_name}: {row_count:,} rows loaded")

    # Show all bronze tables
    print("\n📋 Bronze tables in warehouse:")
    tables = conn.execute("SHOW TABLES").fetchall()
    for t in tables:
        print(f"   • {t[0]}")

    conn.close()
    print("\n✅ Bronze layer build complete!\n")


if __name__ == "__main__":
    load_to_bronze()