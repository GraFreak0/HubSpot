import os
import requests
import pandas as pd
from dotenv import load_dotenv

# =========================
# CONFIGURATION
# =========================
# Load Access Token from environment variable
load_dotenv()
ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN") # "pat-na1-d8e7e9bb-9358-46ab-b459-d49521c0cd03" 
# print("Access Token:", ACCESS_TOKEN)  # Debugging line to check if token is loaded
if not ACCESS_TOKEN:
    raise ValueError("Please set the HUBSPOT_ACCESS_TOKEN environment variable.")

OUTPUT_DIR = "hubspot_data"
MAX_WORKERS = 8  # Number of parallel threads

# List of HubSpot objects to fetch
OBJECTS = {
    "carts": "crm/v3/objects/carts",
    "companies": "crm/v3/objects/companies",
    "contacts": "crm/v3/objects/contacts",
    "deals": "crm/v3/objects/deals",
    "discounts": "crm/v3/objects/discounts",
    "fees": "crm/v3/objects/fees",
    # "goals": "crm/v3/objects/goals",
    "invoices": "crm/v3/objects/invoices",
    # "leads": "crm/v3/objects/leads",
    "line_items": "crm/v3/objects/line_items",
    "orders": "crm/v3/objects/orders",
    # "partner_clients": "crm/v3/objects/partner_clients",
    # "partner_services": "crm/v3/objects/partner_services",
    # "payments": "crm/v3/objects/payments",
    "products": "crm/v3/objects/products",
    "quotes": "crm/v3/objects/quotes",
    "taxes": "crm/v3/objects/taxes",    
    # "tickets": "crm/v3/objects/tickets"
}

# =========================
# FETCH FUNCTION
# =========================
def fetch_all_objects(object_name, endpoint):
    """
    Fetch all records for a given HubSpot object type in batches of 100.
    """
    url = f"https://api.hubapi.com/{endpoint}"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "accept": "application/json"
    }
    params = {
        "limit": 100,  # MAX ALLOWED by HubSpot
        "archived": "false"
    }

    all_results = []
    page_count = 0

    while True:
        response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        all_results.extend(results)

        page_count += 1
        print(f"📄 {object_name}: Batch {page_count} - {len(results)} records fetched")

        paging = data.get("paging", {}).get("next", {})
        if "after" in paging:
            params["after"] = paging["after"]
        else:
            break

    print(f"✅ {object_name}: {len(all_results)} total records fetched")
    return all_results

# =========================
# MAIN SCRIPT
# =========================
if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for obj, endpoint in OBJECTS.items():
        try:
            records = fetch_all_objects(obj, endpoint)

            if records:
                df = pd.json_normalize(records)
                output_path = os.path.join(OUTPUT_DIR, f"{obj}.csv")
                df.to_csv(output_path, index=False, encoding="utf-8")
                print(f"💾 Saved: {output_path}")
            else:
                print(f"⚠️ No records found for {obj}")

        except requests.exceptions.HTTPError as e:
            print(f"❌ Error fetching {obj}: {e}")

    print("🎯 All objects fetched successfully.")