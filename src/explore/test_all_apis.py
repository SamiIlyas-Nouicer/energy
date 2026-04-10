import requests
from test_rte import get_rte_token

ENDPOINTS = {
    "actual_generation": "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_production_type",
    "actual_consumption": "https://digital.iservices.rte-france.com/open_api/consumption/v1/short_term",
    "physical_flows":     "https://digital.iservices.rte-france.com/open_api/physical_flow/v1/physical_flows",
}

def test_endpoint(name, url, headers):
    print(f"\n── {name} {'─' * (40 - len(name))}")
    res = requests.get(url, headers=headers)  # no params → API returns today automatically

    if res.status_code != 200:
        print(f"  ❌ Failed ({res.status_code}): {res.text[:300]}")
        return None

    data = res.json()
    top_key = list(data.keys())[0]
    records = data[top_key]

    if records:
        first = records[0]
        print(f"  ✅ {len(records)} record(s) — top key: '{top_key}'")
        print(f"  First record keys : {list(first.keys())}")
        if "values" in first:
            print(f"  values[0]         : {first['values'][0]}")
    return data

def test_all():
    token = get_rte_token()
    headers = {"Authorization": f"Bearer {token}"}

    results = {name: test_endpoint(name, url, headers) for name, url in ENDPOINTS.items()}
    passed = sum(1 for v in results.values() if v is not None)
    print(f"\n{'='*50}\n  {passed}/{len(ENDPOINTS)} endpoints working\n")

     # ── Check consumption types ──────────────────────────────────────────
    data = results["actual_consumption"]
    if data:
        print("Consumption record types:")
        for record in data["short_term"]:
            print(f"  type={record['type']} → {len(record['values'])} values")

if __name__ == "__main__":
    test_all()