import os
import base64
import requests
from dotenv import load_dotenv

# Load your CLIENT_ID and CLIENT_SECRET from .env
load_dotenv()

def get_rte_token():
    """
    Handles the OAuth2 Client Credentials flow for RTE.
    Returns a valid access_token string.
    """
    client_id = os.getenv("RTE_CLIENT_ID")
    client_secret = os.getenv("RTE_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        print("❌ Error: Credentials missing in .env file.")
        return None

    # RTE requires: base64(client_id:client_secret)
    credential = f"{client_id}:{client_secret}"
    encoded_creds = base64.b64encode(credential.encode()).decode()
    
    url = "https://digital.iservices.rte-france.com/token/oauth/"
    headers = {
        "Authorization": f"Basic {encoded_creds}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    try:
        response = requests.post(url, headers=headers, data={"grant_type": "client_credentials"})
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        print(f"❌ Auth Failed: {e}")
        return None

if __name__ == "__main__":
    # Test it independently
    token = get_rte_token()
    if token:
        print(f"✅ Auth Success! Token: {token[:15]}...")