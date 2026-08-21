import requests

url = "https://www.fema.gov/api/open/v1/FemaWebDisasterSummaries"

tests = [
    (
        "A — lastRefresh only",
        {
            "$top": 5,
            "$select": "disasterNumber,id,lastRefresh",
            "$orderby": "lastRefresh asc",
            "$filter": "lastRefresh gt '2026-08-13T00:02:48.135Z'",
        },
    ),
    (
        "B — exact timestamp + id",
        {
            "$top": 5,
            "$select": "disasterNumber,id,lastRefresh",
            "$orderby": "lastRefresh asc,id asc",
            "$filter": (
                "lastRefresh eq '2026-08-13T00:02:48.135Z' "
                "and id gt '4740'"
            ),
        },
    ),
    (
        "C — disasterNumber only",
        {
            "$top": 5,
            "$select": "disasterNumber,id,lastRefresh",
            "$orderby": "disasterNumber asc",
            "$filter": "disasterNumber gt 4740",
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

    print(
        "METADATA COUNT:",
        payload.get("metadata", {}).get("count"),
    )

    records = payload.get(
        "FemaWebDisasterSummaries",
        [],
    )

    print("RETURNED:", len(records))

    for record in records:
        print(
            record.get("disasterNumber"),
            record.get("id"),
            record.get("lastRefresh"),
        )