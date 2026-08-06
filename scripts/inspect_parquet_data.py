from storage.config import RAW_DATA_DIR
import pandas as pd

for name in [
    "declarations",
    "public_assistance",
    "disaster_summaries",
]:
    path = RAW_DATA_DIR / f"{name}.parquet"
    df = pd.read_parquet(path)

    print("=" * 70)
    print(name)
    print(df.shape)
    print(df.head(3))
    print(df.dtypes)