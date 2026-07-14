import requests
import pandas as pd

ENDPOINTS = {
    "declarations": {
        "url": "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
    },
    "public_assistance": {
        "url": "https://www.fema.gov/api/open/v2/PublicAssistanceFundedProjectsDetails"
    },
    "disaster_summaries": {
        "url": "https://www.fema.gov/api/open/v1/FemaWebDisasterSummaries"
    },
}

for dataset_name, endpoint in ENDPOINTS.items():

    print("\n" + "=" * 80)
    print(f"DATASET: {dataset_name}")
    print("=" * 80)

    response = requests.get(
        endpoint["url"],
        params={"$top": 1},
        timeout=30,
    )

    response.raise_for_status()

    payload = response.json()

    # Extract FEMA dataset
    records = None

    for value in payload.values():
        if isinstance(value, list):
            records = value
            break

    if not records:
        print("No records returned.")
        continue

    df = pd.DataFrame(records)

    print("\nColumns:\n")
    for col in sorted(df.columns):
        print(col)

    print("\nTotal columns:", len(df.columns))