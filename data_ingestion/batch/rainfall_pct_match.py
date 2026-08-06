"""
Reports the percentage of training disasters that have matched
rainfall exposure records.
"""

import pandas as pd

from storage.config import PROCESSED_DATA_DIR

features = pd.read_csv(PROCESSED_DATA_DIR / "features.csv")
rain = pd.read_parquet(PROCESSED_DATA_DIR / "rainfall_aggregated.parquet")

matched_ids = set(rain["disasternumber"])

rate = features["disasternumber"].isin(matched_ids).mean() * 100

print(f"Rainfall match rate among training disasters: {rate:.1f}%")