import os
import requests
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
OBJECT_TYPES = ["contacts", "companies", "deals"]

headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

for obj in OBJECT_TYPES:
    url = f"https://api.hubapi.com/crm/v3/properties/{obj}?archived=false"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        print(f"{obj} properties:")
        for prop in resp.json().get("results", []):
            print(f"- {prop['name']}: {prop.get('label', '')}")
    else:
        print(f"Error fetching {obj}: {resp.text}")
