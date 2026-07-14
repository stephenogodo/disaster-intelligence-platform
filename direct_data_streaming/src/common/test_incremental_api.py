import requests
import pandas as pd

URL = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"

# 1. Get one record and inspect all available fields
print("=" * 80)
print("TEST 1: Discover available fields")
print("=" * 80)

r = requests.get(URL, params={"$top": 1})
r.raise_for_status()

data = r.json()["DisasterDeclarationsSummaries"]

df = pd.DataFrame(data)

print(df.columns.tolist())

# 2. Try sorting by lastRefresh
print("\n" + "=" * 80)
print("TEST 2: Test ordering by lastRefresh")
print("=" * 80)

params = {
    "$top": 5,
    "$orderby": "lastRefresh desc"
}

r = requests.get(URL, params=params)

print("Status:", r.status_code)

if r.status_code == 200:
    df = pd.DataFrame(r.json()["DisasterDeclarationsSummaries"])
    print(df[["disasterNumber", "lastRefresh"]])
else:
    print(r.text)

# 3. Try filtering
print("\n" + "=" * 80)
print("TEST 3: Test filtering")
print("=" * 80)

params = {
    "$top": 5,
    "$filter": "lastRefresh gt '2025-01-01T00:00:00.000Z'"
}

r = requests.get(URL, params=params)

print("Status:", r.status_code)

if r.status_code == 200:
    print(pd.DataFrame(r.json()["DisasterDeclarationsSummaries"]).head())
else:
    print(r.text)