import pandas as pd
features = pd.read_csv("direct_data_streaming/data/processed/features.csv")
rain = pd.read_parquet("direct_data_streaming/data/processed/rainfall_aggregated.parquet")
matched_ids = set(rain["disasternumber"])
rate = features["disasternumber"].isin(matched_ids).mean() * 100
print(f"Rainfall match rate among training disasters: {rate:.1f}%")