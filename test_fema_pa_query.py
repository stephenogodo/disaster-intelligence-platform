import requests

url = "https://www.fema.gov/api/open/v2/PublicAssistanceFundedProjectsDetails"

tests = [
    (
        "A — lastRefresh only",
        {
            "$top": 5,
            "$select": "gmProjectId,lastRefresh",
            "$orderby": "lastRefresh asc,gmProjectId asc",
            "$filter": "lastRefresh gt '2026-08-12T15:21:33.184Z'",
        },
    ),
    (
        "B — exact timestamp + numeric key",
        {
            "$top": 5,
            "$select": "gmProjectId,lastRefresh",
            "$orderby": "lastRefresh asc,gmProjectId asc",
            "$filter": (
                "lastRefresh eq '2026-08-12T15:21:33.184Z' "
                "and gmProjectId gt 164946"
            ),
        },
    ),
    (
        "C — numeric key only",
        {
            "$top": 5,
            "$select": "gmProjectId,lastRefresh",
            "$orderby": "gmProjectId asc",
            "$filter": "gmProjectId gt 164946",
        },
    ),
]

for name, params in tests:
    print("\n" + "=" * 70)
    print(name)
    print("=" * 70)

    response = requests.get(
        url,
        params=params,
        timeout=60,
    )

    print("STATUS:", response.status_code)

    payload = response.json()

    print("METADATA COUNT:", payload.get("metadata", {}).get("count"))

    records = payload.get(
        "PublicAssistanceFundedProjectsDetails",
        [],
    )

    print("RETURNED:", len(records))

    for record in records:
        print(
            record.get("gmProjectId"),
            record.get("lastRefresh"),
        )