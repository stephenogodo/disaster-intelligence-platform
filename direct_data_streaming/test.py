import pandas as pd
df = pd.read_csv("direct_data_streaming/data/processed/features.csv")
for col in ["incident_severity_score", "incident_duration_days", "days_to_declaration", "state_disaster_frequency"]:
    print(col, "->", df[col].