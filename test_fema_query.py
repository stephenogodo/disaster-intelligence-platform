import requests

url = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"

params = {
    "$top": 10,
    "$skip": 0,
    "$orderby": "lastRefresh asc",
    "$filter": "lastRefresh gt '2024-10-29T13:01:43.933Z'",
    "$select": "id,hash,disasterNumber,state,incidentType,declarationType,fipsStateCode,fipsCountyCode,lastRefresh",
}

response = requests.get(url, params=params, timeout=60)

print("STATUS:", response.status_code)
print("URL:", response.url)
print()
print(response.text[:10000])