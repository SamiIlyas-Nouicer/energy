import requests
from test_rte import get_rte_token

def test_all():
    token = get_rte_token()
    headers = {"Authorization": f"Bearer {token}"}
    
    endpoints = {
        "Generation": "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_production_type",
        "Consumption_Short": "https://digital.iservices.rte-france.com/open_api/consumption/v1/short_term",
        "Consumption_Weekly": "https://digital.iservices.rte-france.com/open_api/consumption/v1/weekly_forecasts",
        "Consumption_Annual": "https://digital.iservices.rte-france.com/open_api/consumption/v1/annual_forecasts",
        "Flows": "https://digital.iservices.rte-france.com/open_api/physical_flow/v1/physical_flows"
            }           
    for name, url in endpoints.items():
        print(f"🧪 Testing {name} API...")
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            print(f"✅ {name}: Success!")
        else:
            print(f"❌ {name}: Failed ({res.status_code}) - {res.text}")

if __name__ == "__main__":
    test_all()

    