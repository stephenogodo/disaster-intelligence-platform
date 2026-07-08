import pandas as pd
features = pd.read_csv("direct_data_streaming/data/processed/features.csv")
climate = pd.read_parquet("direct_data_streaming/data/processed/climate_aggregated.parquet")

matched_ids = set(climate["disasternumber"])
features["has_climate_match"] = features["disasternumber"].isin(matched_ids)

print(f"Match rate among the {len(features)} disasters actually used for training: "
      f"{features['has_climate_match'].sum()}/{len(features)} "
      f"({features['has_climate_match'].mean()*100:.1f}%)")