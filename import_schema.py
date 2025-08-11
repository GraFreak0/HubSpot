import os
import requests

ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

url = "https://api.hubapi.com/crm-object-schemas/v3/schemas"
resp = requests.get(url, headers=headers)

if resp.ok:
    schemas = resp.json()
    print("Retrieved schemas for objects:")
    for schema in schemas.get("results", []):
        print(f"– {schema.get('name')} ({schema.get('objectTypeId')}) with {len(schema.get('properties', []))} properties")
else:
    print("Failed to fetch schemas:", resp.status_code, resp.text)
