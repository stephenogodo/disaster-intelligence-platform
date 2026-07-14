import requests
import pandas as pd

URL = "https://www.fema.gov/api/open/v2/PublicAssistanceFundedProjectsDetails"

r = requests.get(
    URL,
    params={"$top": 1},
    timeout=30,
)

r.raise_for_status()

df = pd.DataFrame(r.json()["PublicAssistanceFundedProjectsDetails"])

for c in sorted(df.columns):
    if "category" in c.lower():
        print(c)