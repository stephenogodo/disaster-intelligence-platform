import requests

URL = "https://www.fema.gov/api/open/v2/PublicAssistanceFundedProjectsDetails"

tests = [
    ["gmProjectId"],
    ["gmProjectId", "disasterNumber"],
    ["gmProjectId", "disasterNumber", "projectAmount"],
    ["gmProjectId", "disasterNumber", "projectAmount", "projectCategory"],
    ["gmProjectId", "disasterNumber", "projectAmount", "projectCategory",
     "federalShareObligated"],
    ["gmProjectId", "disasterNumber", "projectAmount", "projectCategory",
     "federalShareObligated", "stateAbbreviation"],
    ["gmProjectId", "disasterNumber", "projectAmount", "projectCategory",
     "federalShareObligated", "stateAbbreviation", "lastRefresh"],
]

for fields in tests:

    params = {
        "$top": 1,
        "$select": ",".join(fields)
    }

    r = requests.get(URL, params=params)

    print("-" * 70)
    print(fields)
    print("Status:", r.status_code)

    if r.status_code != 200:
        print(r.text[:500])