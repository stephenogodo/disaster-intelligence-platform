import pandas as pd

for name in [
    "declarations",
    "public_assistance",
    "disaster_summaries",
]:
    path = f"direct_data_streaming/data/raw/{name}.parquet"
    df = pd.read_parquet(path)

    print("=" * 70)
    print(name)
    print(df.shape)
    print(df.head(3))
    print(df.dtypes)