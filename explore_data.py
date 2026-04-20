"""
Explore the insurance dataset before building the ETL pipeline.
This helps us understand:
  - What columns exist
  - What data types they are
  - How many nulls
  - How many duplicates
  - Sample data
"""

import pandas as pd
import os

DATA_FOLDER = "data"

def explore_file(filename):
    print(f"\n{'='*70}")
    print(f"📁 FILE: {filename}")
    print('='*70)

    filepath = os.path.join(DATA_FOLDER, filename)
    df = pd.read_csv(filepath)

    # Basic info
    print(f"\n📊 Shape: {df.shape[0]} rows × {df.shape[1]} columns")

    # Columns and dtypes
    print("\n📋 Columns & Data Types:")
    for col, dtype in df.dtypes.items():
        print(f"   • {col:<30} {dtype}")

    # Null counts
    print("\n🕳  Null Values:")
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]
    if len(nulls) == 0:
        print("   ✅ No nulls!")
    else:
        for col, count in nulls.items():
            pct = (count / len(df)) * 100
            print(f"   • {col:<30} {count} nulls ({pct:.1f}%)")

    # Duplicates
    dup_count = df.duplicated().sum()
    print(f"\n🔁 Duplicate Rows: {dup_count}")

    # Sample data
    print("\n👀 Sample (first 3 rows):")
    print(df.head(3).to_string())


if __name__ == "__main__":
    # Get all CSV files in data folder
    csv_files = [f for f in os.listdir(DATA_FOLDER) if f.endswith(".csv")]

    print(f"\n🔍 Found {len(csv_files)} CSV file(s) in {DATA_FOLDER}/")
    for f in csv_files:
        print(f"   • {f}")

    # Explore each file
    for f in csv_files:
        explore_file(f)

    print(f"\n{'='*70}")
    print("✅ Exploration complete!")
    print('='*70)