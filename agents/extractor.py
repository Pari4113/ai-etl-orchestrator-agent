"""
Extractor: Reads source CSV files into pandas DataFrames.
Handles missing files gracefully and logs what it's doing.
"""

import pandas as pd
import os
from config import SOURCE_FILES


def extract_csv(source_name: str) -> pd.DataFrame:
    """
    Read a single source CSV into a DataFrame.

    Args:
        source_name: Key from SOURCE_FILES (e.g., 'insurance')

    Returns:
        pandas DataFrame with raw CSV contents
    """
    if source_name not in SOURCE_FILES:
        raise ValueError(
            f"Unknown source '{source_name}'. "
            f"Available: {list(SOURCE_FILES.keys())}"
        )

    file_path = SOURCE_FILES[source_name]

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV not found: {file_path}")

    print(f"📥 Extracting: {source_name} from {file_path}")
    df = pd.read_csv(file_path)
    print(f"   ✅ Loaded {len(df):,} rows × {len(df.columns)} cols")
    return df


def extract_all() -> dict:
    """
    Extract all source CSVs at once.

    Returns:
        Dict mapping source name → DataFrame
    """
    return {name: extract_csv(name) for name in SOURCE_FILES}


if __name__ == "__main__":
    # Quick test when run directly
    print("\n🧪 Testing extractor...\n")
    data = extract_all()
    print(f"\n✅ Extracted {len(data)} sources successfully!")